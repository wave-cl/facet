//! The store: a directory of projection files bound to one chron
//! database.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use redb::{Database, ReadableDatabase};
use uuid::Uuid;

use crate::data::{ProjectionData, ProjectionInner};
use crate::error::{Error, Result};
use crate::meta::{self, Checkpoint, FORMAT};
use crate::schema::ProjectionDecl;

/// Whether every batch is flushed to disk before it is acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fsync {
    /// Every batch commit is durable (one fsync per batch per projection).
    Always,
    /// A batch survives a process crash but not a power loss until the
    /// next durable write ([`Store::flush`], a declaration write, or the
    /// next batch under `Always`). Rows and checkpoint still move together.
    Never,
}

/// What to do with a projection file built from another chron database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnDbMismatch {
    /// Refuse with [`Error::WrongDatabase`].
    Refuse,
    /// Rebuild the projection from nothing under the new id.
    Rebuild,
}

/// Store options.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Options {
    /// Batch durability. Default `Always`.
    pub fsync: Fsync,
    /// redb's cache per projection file. Default 64 MiB.
    pub cache_bytes: usize,
    /// Default `Refuse`.
    pub on_db_mismatch: OnDbMismatch,
    /// Most changed keys a batch summary lists per collection before it
    /// says `All`. Default 10 000.
    pub summary_max_keys: usize,
    /// How many batch summaries a slow watcher may fall behind before it
    /// is told to re-read. Default 64.
    pub summary_capacity: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            fsync: Fsync::Always,
            cache_bytes: 64 * 1024 * 1024,
            on_db_mismatch: OnDbMismatch::Refuse,
            summary_max_keys: 10_000,
            summary_capacity: 64,
        }
    }
}

impl Options {
    /// The defaults.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets [`Options::fsync`].
    pub fn fsync(mut self, f: Fsync) -> Self {
        self.fsync = f;
        self
    }
    /// Sets [`Options::cache_bytes`].
    pub fn cache_bytes(mut self, n: usize) -> Self {
        self.cache_bytes = n;
        self
    }
    /// Sets [`Options::on_db_mismatch`].
    pub fn on_db_mismatch(mut self, o: OnDbMismatch) -> Self {
        self.on_db_mismatch = o;
        self
    }
    /// Sets [`Options::summary_max_keys`].
    pub fn summary_max_keys(mut self, n: usize) -> Self {
        self.summary_max_keys = n;
        self
    }
    /// Sets [`Options::summary_capacity`].
    pub fn summary_capacity(mut self, n: usize) -> Self {
        self.summary_capacity = n.max(1);
        self
    }
}

/// Why a projection was rebuilt at registration, or that it was not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rebuilt {
    /// The file held the same declaration and version; nothing changed.
    No,
    /// The file did not exist.
    Fresh,
    /// The declaration changed.
    DeclChanged,
    /// Only the version string changed.
    VersionChanged,
    /// The file was written by another engine format.
    FormatChanged,
    /// The file was bound to another chron database and the options said
    /// to rebuild.
    DatabaseChanged,
}

/// What [`Store::register`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Registered {
    /// Whether and why the projection starts from nothing.
    pub rebuilt: Rebuilt,
    /// The checkpoint to resume from (zero after a rebuild).
    pub checkpoint: Checkpoint,
}

/// A projection file as [`Store::list`] sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionInfo {
    /// The projection.
    pub name: String,
    /// Its version string.
    pub version: String,
    /// Its checkpoint.
    pub checkpoint: Checkpoint,
    /// The file's size.
    pub bytes: u64,
    /// Whether a handle is open on it in this process.
    pub open: bool,
}

pub(crate) struct StoreInner {
    pub(crate) dir: PathBuf,
    pub(crate) db_id: Uuid,
    pub(crate) options: Options,
    pub(crate) open: Mutex<HashMap<String, Weak<ProjectionInner>>>,
}

/// A directory of projection files, one per projection, all bound to one
/// chron database. Cheap to clone.
#[derive(Clone)]
pub struct Store(pub(crate) Arc<StoreInner>);

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("dir", &self.0.dir)
            .field("db_id", &self.0.db_id)
            .finish()
    }
}

impl Store {
    /// Opens (creating it if needed) the directory for projections over
    /// the chron database `db_id`.
    pub fn open(dir: impl AsRef<Path>, db_id: Uuid, options: Options) -> Result<Store> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Store(Arc::new(StoreInner {
            dir,
            db_id,
            options,
            open: Mutex::new(HashMap::new()),
        })))
    }

    /// The directory.
    pub fn dir(&self) -> &Path {
        &self.0.dir
    }

    /// The chron database id the store is bound to.
    pub fn db_id(&self) -> Uuid {
        self.0.db_id
    }

    /// The options.
    pub fn options(&self) -> &Options {
        &self.0.options
    }

    fn path_of(&self, name: &str) -> PathBuf {
        self.0.dir.join(format!("{name}.redb"))
    }

    /// Opens the projection's file, creating it when absent, and applies
    /// the rebuild rule: a file whose declaration, version or format
    /// differs from `decl` loses every collection and its checkpoint in
    /// one durable transaction. A file bound to another chron database is
    /// refused unless the options say to rebuild. A projection with a live
    /// handle cannot be registered again (`InUse`).
    pub fn register(&self, decl: &ProjectionDecl) -> Result<(ProjectionData, Registered)> {
        decl.validate()?;
        let mut open = self.0.open.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(w) = open.get(&decl.name)
            && w.upgrade().is_some()
        {
            return Err(Error::InUse(decl.name.clone()));
        }
        let path = self.path_of(&decl.name);
        let file = path.display().to_string();
        let db = Database::builder()
            .set_cache_size(self.0.options.cache_bytes)
            .create(&path)?;
        let decl_bytes = decl.canonical_bytes();
        let existing = {
            let txn = db.begin_read()?;
            meta::read_meta(&txn, &file)?
        };
        let (reason, checkpoint) = match existing {
            None => (Some(Rebuilt::Fresh), Checkpoint::default()),
            Some(m) => {
                if m.projection != decl.name {
                    return Err(Error::Corrupt {
                        file,
                        reason: format!("holds projection {:?}", m.projection),
                    });
                }
                if m.db_id != self.0.db_id {
                    match self.0.options.on_db_mismatch {
                        OnDbMismatch::Refuse => {
                            return Err(Error::WrongDatabase {
                                file,
                                bound: m.db_id,
                                expected: self.0.db_id,
                            });
                        }
                        OnDbMismatch::Rebuild => (Some(Rebuilt::DatabaseChanged), m.checkpoint),
                    }
                } else if m.format != FORMAT {
                    (Some(Rebuilt::FormatChanged), m.checkpoint)
                } else if m.decl != decl_bytes {
                    (Some(Rebuilt::DeclChanged), m.checkpoint)
                } else if m.version != decl.version {
                    (Some(Rebuilt::VersionChanged), m.checkpoint)
                } else {
                    (None, m.checkpoint)
                }
            }
        };
        let checkpoint = match reason {
            None => checkpoint,
            Some(_) => {
                let mut txn = db.begin_write()?;
                txn.set_durability(redb::Durability::Immediate)?;
                meta::drop_collections(&txn)?;
                meta::write_meta(&txn, self.0.db_id, &decl.name, &decl.version, &decl_bytes)?;
                txn.commit()?;
                Checkpoint::default()
            }
        };
        let inner = Arc::new(ProjectionInner::new(
            self.0.clone(),
            decl.clone(),
            db,
            path,
            checkpoint,
        ));
        open.insert(decl.name.clone(), Arc::downgrade(&inner));
        Ok((
            ProjectionData(inner),
            Registered {
                rebuilt: reason.unwrap_or(Rebuilt::No),
                checkpoint,
            },
        ))
    }

    /// Deletes a projection's file. `Ok(false)` when there was none;
    /// `InUse` while a handle is alive.
    pub fn drop_projection(&self, name: &str) -> Result<bool> {
        let mut open = self.0.open.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(w) = open.get(name) {
            if w.upgrade().is_some() {
                return Err(Error::InUse(name.to_owned()));
            }
            open.remove(name);
        }
        let path = self.path_of(name);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Every projection file in the directory, with its checkpoint.
    pub fn list(&self) -> Result<Vec<ProjectionInfo>> {
        let open = self.0.open.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::new();
        let mut entries: Vec<PathBuf> = std::fs::read_dir(&self.0.dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "redb"))
            .collect();
        entries.sort();
        for path in entries {
            let name = path.file_stem().unwrap().to_string_lossy().into_owned();
            let bytes = std::fs::metadata(&path)?.len();
            if let Some(p) = open.get(&name).and_then(Weak::upgrade) {
                out.push(ProjectionInfo {
                    name,
                    version: p.decl.version.clone(),
                    checkpoint: p.checkpoint(),
                    bytes,
                    open: true,
                });
                continue;
            }
            let db = Database::builder()
                .set_cache_size(1024 * 1024)
                .open(&path)?;
            let txn = db.begin_read()?;
            let Some(m) = meta::read_meta(&txn, &path.display().to_string())? else {
                continue;
            };
            out.push(ProjectionInfo {
                name,
                version: m.version,
                checkpoint: m.checkpoint,
                bytes,
                open: false,
            });
        }
        Ok(out)
    }

    /// Makes every open projection's batches durable (a no-op under
    /// `Fsync::Always`).
    pub fn flush(&self) -> Result<()> {
        let handles: Vec<Arc<ProjectionInner>> = {
            let open = self.0.open.lock().unwrap_or_else(|e| e.into_inner());
            open.values().filter_map(Weak::upgrade).collect()
        };
        for p in handles {
            p.flush()?;
        }
        Ok(())
    }
}

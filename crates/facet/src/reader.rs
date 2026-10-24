//! The read side: a snapshot of one projection and paged scans.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use redb::{ReadOnlyTable, ReadTransaction, ReadableTable, TableDefinition};

use crate::error::{Error, Result};
use crate::keyenc::prefix_end;
use crate::meta::Checkpoint;
use crate::prim;
use crate::schema::ProjectionDecl;

pub(crate) type RawReadTable = ReadOnlyTable<&'static [u8], &'static [u8]>;

/// One consistent read of a projection, at the checkpoint it reports.
pub struct Snapshot {
    txn: ReadTransaction,
    pub(crate) decl: Arc<ProjectionDecl>,
    checkpoint: Checkpoint,
    tables: RefCell<HashMap<String, Option<Arc<RawReadTable>>>>,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("projection", &self.decl.name)
            .field("checkpoint", &self.checkpoint)
            .finish()
    }
}

impl Snapshot {
    pub(crate) fn new(
        txn: ReadTransaction,
        decl: Arc<ProjectionDecl>,
        checkpoint: Checkpoint,
    ) -> Self {
        Snapshot {
            txn,
            decl,
            checkpoint,
            tables: RefCell::new(HashMap::new()),
        }
    }

    /// The checkpoint this snapshot reflects.
    pub fn checkpoint(&self) -> Checkpoint {
        self.checkpoint
    }

    /// The declaration.
    pub fn decl(&self) -> &ProjectionDecl {
        &self.decl
    }

    /// The table, or `None` when nothing was ever written to it.
    pub(crate) fn table(&self, name: &str) -> Result<Option<Arc<RawReadTable>>> {
        if let Some(t) = self.tables.borrow().get(name) {
            return Ok(t.clone());
        }
        let t = match self
            .txn
            .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
        {
            Ok(t) => Some(Arc::new(t)),
            Err(redb::TableError::TableDoesNotExist(_)) => None,
            Err(e) => return Err(e.into()),
        };
        self.tables.borrow_mut().insert(name.to_owned(), t.clone());
        Ok(t)
    }

    /// The register collection `name`.
    pub fn register<V>(&self, name: &str) -> Result<prim::register::RegisterR<'_, V>> {
        prim::register::RegisterR::open(self, name)
    }

    /// The map collection `name` keyed by `K`.
    pub fn map<K: crate::KeyType, V>(&self, name: &str) -> Result<prim::map::MapR<'_, K, V>> {
        prim::map::MapR::open(self, name)
    }
}

/// Where a scan left off: the encoded key last returned. Opaque; carry
/// it as bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cursor(pub(crate) Vec<u8>);

impl Cursor {
    /// The cursor's bytes, for the wire.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    /// A cursor from its bytes.
    pub fn from_bytes(b: impl Into<Vec<u8>>) -> Cursor {
        Cursor(b.into())
    }
}

/// One page of a scan.
#[derive(Debug, Clone, PartialEq)]
pub struct Page<T> {
    /// The items, in scan order.
    pub items: Vec<T>,
    /// Where to continue, `None` when the scan is exhausted.
    pub next: Option<Cursor>,
    /// The snapshot's checkpoint.
    pub checkpoint: Checkpoint,
}

impl<T> Page<T> {
    /// Maps the items.
    pub fn map<U>(self, f: impl FnMut(T) -> U) -> Page<U> {
        Page {
            items: self.items.into_iter().map(f).collect(),
            next: self.next,
            checkpoint: self.checkpoint,
        }
    }
}

/// Encoded `(key, value)` pairs and where to continue.
pub(crate) type RawPage = (Vec<(Vec<u8>, Vec<u8>)>, Option<Cursor>);

/// A scan over encoded keys: a byte prefix, optional inclusive bounds
/// within it, a cursor to continue from, a direction and a limit (0 =
/// no limit).
#[derive(Debug, Clone, Default)]
pub(crate) struct RawScan {
    pub prefix: Vec<u8>,
    pub from: Option<Vec<u8>>,
    pub to: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    pub reverse: bool,
    pub limit: usize,
}

impl RawScan {
    /// Runs the scan over a table (`None` reads as empty).
    pub(crate) fn run<T: ReadableTable<&'static [u8], &'static [u8]>>(
        &self,
        table: Option<&T>,
    ) -> Result<RawPage> {
        let Some(table) = table else {
            return Ok((Vec::new(), None));
        };
        let mut lower: Bound<Vec<u8>> = Bound::Included(self.prefix.clone());
        let mut upper: Bound<Vec<u8>> = match prefix_end(&self.prefix) {
            Some(end) => Bound::Excluded(end),
            None => Bound::Unbounded,
        };
        if let Some(from) = &self.from
            && from.as_slice() > self.prefix.as_slice()
        {
            lower = Bound::Included(from.clone());
        }
        if let Some(to) = &self.to {
            let tighter = match &upper {
                Bound::Excluded(end) => to < end,
                Bound::Unbounded => true,
                Bound::Included(_) => unreachable!(),
            };
            if tighter {
                upper = Bound::Included(to.clone());
            }
        }
        if let Some(after) = &self.after {
            let inside = after.starts_with(&self.prefix)
                && match &lower {
                    Bound::Included(l) => after >= l,
                    _ => true,
                }
                && match &upper {
                    Bound::Included(u) => after <= u,
                    Bound::Excluded(u) => after < u,
                    Bound::Unbounded => true,
                };
            if !inside {
                return Err(Error::InvalidCursor(
                    "the cursor lies outside this scan's range".into(),
                ));
            }
            if self.reverse {
                upper = Bound::Excluded(after.clone());
            } else {
                lower = Bound::Excluded(after.clone());
            }
        }
        let want = if self.limit == 0 {
            usize::MAX
        } else {
            self.limit
        };
        let range = table.range::<&[u8]>((
            lower.as_ref().map(|v| v.as_slice()),
            upper.as_ref().map(|v| v.as_slice()),
        ))?;
        let mut items = Vec::new();
        let mut more = false;
        let mut take = |entry: std::result::Result<_, redb::StorageError>| -> Result<bool> {
            let (k, v): (redb::AccessGuard<&[u8]>, redb::AccessGuard<&[u8]>) = entry?;
            if items.len() == want {
                more = true;
                return Ok(false);
            }
            items.push((k.value().to_vec(), v.value().to_vec()));
            Ok(true)
        };
        if self.reverse {
            for entry in range.rev() {
                if !take(entry)? {
                    break;
                }
            }
        } else {
            for entry in range {
                if !take(entry)? {
                    break;
                }
            }
        }
        let next = if more {
            items.last().map(|(k, _)| Cursor(k.clone()))
        } else {
            None
        };
        Ok((items, next))
    }
}

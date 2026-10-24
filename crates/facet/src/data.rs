//! A projection's file: batches, snapshots, rebuilds and the watch feeds.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use redb::{Database, Durability, ReadableDatabase};
use tokio::sync::{broadcast, watch};

use crate::error::{ApplyError, Error, Result};
use crate::meta::{self, Checkpoint};
use crate::reader::Snapshot;
use crate::schema::ProjectionDecl;
use crate::store::{Fsync, StoreInner};
use crate::watch::{BatchSummary, ChangeTracker};
use crate::writer::Writer;

pub(crate) struct ProjectionInner {
    pub(crate) store: Arc<StoreInner>,
    pub(crate) decl: Arc<ProjectionDecl>,
    pub(crate) db: Database,
    pub(crate) path: PathBuf,
    apply: Mutex<()>,
    checkpoint: RwLock<Checkpoint>,
    cp_tx: watch::Sender<Checkpoint>,
    summary_tx: broadcast::Sender<Arc<BatchSummary>>,
}

impl ProjectionInner {
    pub(crate) fn new(
        store: Arc<StoreInner>,
        decl: ProjectionDecl,
        db: Database,
        path: PathBuf,
        checkpoint: Checkpoint,
    ) -> Self {
        let (cp_tx, _) = watch::channel(checkpoint);
        let (summary_tx, _) = broadcast::channel(store.options.summary_capacity);
        ProjectionInner {
            store,
            decl: Arc::new(decl),
            db,
            path,
            apply: Mutex::new(()),
            checkpoint: RwLock::new(checkpoint),
            cp_tx,
            summary_tx,
        }
    }

    pub(crate) fn checkpoint(&self) -> Checkpoint {
        *self.checkpoint.read().unwrap_or_else(|e| e.into_inner())
    }

    fn durability(&self) -> Durability {
        match self.store.options.fsync {
            Fsync::Always => Durability::Immediate,
            Fsync::Never => Durability::None,
        }
    }

    pub(crate) fn flush(&self) -> Result<()> {
        let _guard = self.apply.lock().unwrap_or_else(|e| e.into_inner());
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        txn.commit()?;
        Ok(())
    }

    fn publish(&self, checkpoint: Checkpoint, summary: BatchSummary) {
        *self.checkpoint.write().unwrap_or_else(|e| e.into_inner()) = checkpoint;
        self.cp_tx.send_replace(checkpoint);
        let _ = self.summary_tx.send(Arc::new(summary));
    }
}

/// A registered projection: the handle batches, reads and rebuilds go
/// through. Cheap to clone; the file closes when the last clone drops.
#[derive(Clone)]
pub struct ProjectionData(pub(crate) Arc<ProjectionInner>);

impl std::fmt::Debug for ProjectionData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectionData")
            .field("name", &self.0.decl.name)
            .field("checkpoint", &self.0.checkpoint())
            .finish()
    }
}

impl ProjectionData {
    /// The projection's name.
    pub fn name(&self) -> &str {
        &self.0.decl.name
    }

    /// The declaration it was registered with.
    pub fn decl(&self) -> &ProjectionDecl {
        &self.0.decl
    }

    /// The file.
    pub fn path(&self) -> &std::path::Path {
        &self.0.path
    }

    /// The last committed checkpoint.
    pub fn checkpoint(&self) -> Checkpoint {
        self.0.checkpoint()
    }

    /// Runs one batch: `f` writes through the [`Writer`] inside a write
    /// transaction that also carries `checkpoint`, and the whole thing
    /// commits or nothing does. An error or panic in `f` leaves the file
    /// as it was; an engine error the closure passes up with `?` comes
    /// back as `Handler` too. The checkpoint must advance past the
    /// committed one. Batches on one projection run one at a time.
    pub fn apply<R, E>(
        &self,
        checkpoint: Checkpoint,
        f: impl FnOnce(&mut Writer<'_>) -> Result<R, E>,
    ) -> Result<R, ApplyError<E>> {
        let inner = &self.0;
        let _guard = inner.apply.lock().unwrap_or_else(|e| e.into_inner());
        let current = inner.checkpoint();
        if checkpoint.next <= current.next {
            return Err(Error::CheckpointNotAdvancing {
                current: current.next,
                proposed: checkpoint.next,
            }
            .into());
        }
        let mut txn = inner.db.begin_write().map_err(Error::from)?;
        txn.set_durability(inner.durability())
            .map_err(Error::from)?;
        let outcome = {
            let tracker = ChangeTracker::new(inner.store.options.summary_max_keys);
            let mut w = Writer::new(&txn, &inner.decl, tracker);
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut w)));
            match r {
                Ok(Ok(r)) => Ok((r, w.finish())),
                Ok(Err(e)) => Err(ApplyError::Handler(e)),
                Err(p) => Err(ApplyError::Store(Error::Panicked(panic_message(&p)))),
            }
        };
        let (result, tracker) = match outcome {
            Ok(v) => v,
            Err(e) => {
                let _ = txn.abort();
                return Err(e);
            }
        };
        if let Err(e) = meta::write_checkpoint(&txn, checkpoint) {
            let _ = txn.abort();
            return Err(e.into());
        }
        txn.commit().map_err(Error::from)?;
        inner.publish(checkpoint, tracker.finish(checkpoint));
        Ok(result)
    }

    /// Drops every collection and zeroes the checkpoint, durably, keeping
    /// the declaration. The next batch starts from nothing.
    pub fn rebuild(&self) -> Result<()> {
        let inner = &self.0;
        let _guard = inner.apply.lock().unwrap_or_else(|e| e.into_inner());
        let mut txn = inner.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        meta::drop_collections(&txn)?;
        meta::write_checkpoint(&txn, Checkpoint::default())?;
        txn.commit()?;
        let mut tracker = ChangeTracker::new(0);
        for c in &inner.decl.collections {
            tracker.touch_all(&c.name);
        }
        inner.publish(Checkpoint::default(), tracker.finish(Checkpoint::default()));
        Ok(())
    }

    /// A consistent read of the whole projection at one checkpoint.
    pub fn snapshot(&self) -> Result<Snapshot> {
        // The read transaction is taken under the apply lock so the cached
        // checkpoint and the transaction agree.
        let inner = &self.0;
        let _guard = inner.apply.lock().unwrap_or_else(|e| e.into_inner());
        let txn = inner.db.begin_read()?;
        let checkpoint = inner.checkpoint();
        Ok(Snapshot::new(txn, inner.decl.clone(), checkpoint))
    }

    /// Follows the committed checkpoint.
    pub fn watch_checkpoint(&self) -> watch::Receiver<Checkpoint> {
        self.0.cp_tx.subscribe()
    }

    /// Receives every batch's summary from now on, in commit order. A
    /// receiver that falls more than the store's `summary_capacity`
    /// behind gets `Lagged` and should re-read from a snapshot.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<BatchSummary>> {
        self.0.summary_tx.subscribe()
    }

    /// Makes earlier batches durable (a no-op under `Fsync::Always`).
    pub fn flush(&self) -> Result<()> {
        self.0.flush()
    }
}

fn panic_message(p: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic".to_owned()
    }
}

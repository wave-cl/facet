//! In-process reads of a running runtime's projections.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, watch};

use super::error::RuntimeError;
use super::rt::Shared;
use super::status::Status;
use crate::data::ProjectionData;
use crate::meta::Checkpoint;
use crate::reader::Snapshot;
use crate::watch::BatchSummary;

/// One source's subscription as the runtime sees it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SourceStatus {
    /// The source.
    pub source: chron_client::Source,
    /// Whether a subscription is open.
    pub connected: bool,
    /// Times it was opened again after an error.
    pub reconnects: u64,
    /// The last error, or why the tail ended.
    pub error: Option<String>,
    /// Events held in the ring.
    pub buffered: usize,
}

/// Reads a runtime's projections. Cheap to clone; stays usable until the
/// runtime shuts down.
#[derive(Clone)]
pub struct Handle {
    pub(crate) shared: Arc<Shared>,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("projections", &self.names())
            .finish()
    }
}

impl Handle {
    /// The projection names, in registration order.
    pub fn names(&self) -> Vec<String> {
        self.shared.order.clone()
    }

    /// The projection as the application defined it.
    pub fn definition(&self, name: &str) -> Option<Arc<super::Projection>> {
        self.shared.runners.get(name).map(|r| r.def.clone())
    }

    /// The projection's stored data (its file).
    pub fn projection(&self, name: &str) -> Option<ProjectionData> {
        self.shared.runners.get(name).map(|r| r.data.clone())
    }

    /// A consistent snapshot of the projection.
    pub fn snapshot(&self, name: &str) -> Result<Snapshot, RuntimeError> {
        let data = self
            .projection(name)
            .ok_or_else(|| RuntimeError::NoProjection(name.to_owned()))?;
        Ok(data.snapshot()?)
    }

    /// The projection's status.
    pub fn status(&self, name: &str) -> Option<Status> {
        self.shared
            .runners
            .get(name)
            .map(|r| r.status.borrow().clone())
    }

    /// Every projection's status, in registration order.
    pub fn statuses(&self) -> Vec<Status> {
        self.shared
            .order
            .iter()
            .filter_map(|n| self.status(n))
            .collect()
    }

    /// Follows the projection's status.
    pub fn watch_status(&self, name: &str) -> Option<watch::Receiver<Status>> {
        self.shared.runners.get(name).map(|r| r.status.clone())
    }

    /// Follows the projection's committed checkpoint.
    pub fn watch_checkpoint(&self, name: &str) -> Option<watch::Receiver<Checkpoint>> {
        self.projection(name).map(|d| d.watch_checkpoint())
    }

    /// Receives every batch's summary from now on.
    pub fn watch(
        &self,
        name: &str,
    ) -> Result<broadcast::Receiver<Arc<BatchSummary>>, RuntimeError> {
        self.projection(name)
            .map(|d| d.subscribe())
            .ok_or_else(|| RuntimeError::NoProjection(name.to_owned()))
    }

    /// Waits until the projection's checkpoint reaches `at_least`
    /// (read your writes: a chron commit's `next`), at most `timeout`.
    pub async fn wait_for(
        &self,
        name: &str,
        at_least: u64,
        timeout: Duration,
    ) -> Result<Checkpoint, RuntimeError> {
        let mut rx = self
            .watch_checkpoint(name)
            .ok_or_else(|| RuntimeError::NoProjection(name.to_owned()))?;
        let outcome = tokio::time::timeout(timeout, rx.wait_for(|cp| cp.next >= at_least))
            .await
            .map(|r| r.map(|cp| *cp));
        match outcome {
            Ok(Ok(cp)) => Ok(cp),
            Ok(Err(_)) => Err(RuntimeError::Stopping),
            Err(_) => Err(RuntimeError::BoundTimeout {
                projection: name.to_owned(),
                checkpoint: rx.borrow().next,
                at_least,
            }),
        }
    }

    /// The sources the runtime follows, one tail each.
    pub fn sources(&self) -> Vec<SourceStatus> {
        self.shared
            .tails
            .iter()
            .map(|t| {
                let s = t.state.borrow();
                SourceStatus {
                    source: t.source.clone(),
                    connected: s.connected,
                    reconnects: s.reconnects,
                    error: s.error.clone().or_else(|| s.ended.clone()),
                    buffered: t.ring.len(),
                }
            })
            .collect()
    }

    /// chron's confirmed head as the runtime last saw it.
    pub fn chron_head(&self) -> u64 {
        *self.shared.head.borrow()
    }

    /// The chron database id the store is bound to.
    pub fn db_id(&self) -> uuid::Uuid {
        self.shared.db_id
    }
}

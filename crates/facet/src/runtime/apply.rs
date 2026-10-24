//! Applying a batch of events to a projection: every handler, inside one
//! transaction with the checkpoint.

use std::collections::BTreeMap;

use chron_client::Event;

use super::cx::Cx;
use super::deadletter::{DEAD_LETTERS, DeadLetter};
use super::projection::Projection;
use crate::data::ProjectionData;
use crate::error::{ApplyError, Error};
use crate::meta::Checkpoint;
use crate::value::Timestamp;

/// A crc32 over what identifies an event: its commit time, aggregate,
/// index, tags and payload. Kept in the checkpoint so a log that was
/// replaced under the projection is noticed.
pub fn fingerprint(ev: &Event) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&ev.recorded_at.to_be_bytes());
    match ev.aggregate {
        Some(a) => h.update(a.as_bytes()),
        None => h.update(&[0u8; 16]),
    }
    h.update(&ev.index.to_be_bytes());
    for t in &ev.tags {
        h.update(&(t.len() as u32).to_be_bytes());
        h.update(t);
    }
    h.update(&(ev.payload.len() as u32).to_be_bytes());
    h.update(&ev.payload);
    h.finalize()
}

/// A handler failed on the event at `position`.
#[derive(Debug, Clone)]
pub(crate) struct BatchFailure {
    pub position: u64,
    pub error: String,
}

/// Why a batch did not commit.
pub(crate) enum BatchError {
    /// A handler failed; the batch may be retried with the position
    /// skipped.
    Handler(BatchFailure),
    /// The engine failed or refused.
    Store(Error),
}

/// Applies `events` (in order, all of them) with `skipped` recorded as
/// dead letters instead of handled. Returns how many dead letters were
/// written.
pub(crate) fn apply_batch(
    def: &Projection,
    data: &ProjectionData,
    events: &[Event],
    skipped: &BTreeMap<u64, String>,
) -> Result<u64, BatchError> {
    let last = events.last().expect("a batch holds at least one event");
    let checkpoint = Checkpoint::after(last.position, fingerprint(last));
    let now = Timestamp(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    );
    let codec = def.codec();
    let r = data.apply(checkpoint, |w| {
        let mut dead = 0u64;
        for ev in events {
            let at = |e: Error| BatchFailure {
                position: ev.position,
                error: e.to_string(),
            };
            if let Some(error) = skipped.get(&ev.position) {
                w.table::<u64, DeadLetter>(DEAD_LETTERS)
                    .map_err(at)?
                    .upsert(&DeadLetter {
                        position: ev.position,
                        aggregate: ev.aggregate,
                        index: ev.index,
                        tags: ev.tags.clone(),
                        error: error.clone(),
                        at: now,
                    })
                    .map_err(at)?;
                dead += 1;
                continue;
            }
            for h in def.handlers_for(ev) {
                let mut cx = Cx::new(w, ev, codec);
                (h.f)(&mut cx, ev).map_err(|e| BatchFailure {
                    position: ev.position,
                    error: e.to_string(),
                })?;
            }
        }
        Ok(dead)
    });
    match r {
        Ok(dead) => Ok(dead),
        Err(ApplyError::Handler(f)) => Err(BatchError::Handler(f)),
        Err(ApplyError::Store(e)) => Err(BatchError::Store(e)),
    }
}

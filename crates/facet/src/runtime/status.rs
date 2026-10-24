//! What a projection is doing.

use std::time::SystemTime;

use crate::meta::Checkpoint;
use crate::store::Rebuilt;

/// A projection's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Checking where to resume.
    Starting,
    /// Applying what chron already holds.
    CatchingUp,
    /// Applying events as they are confirmed.
    Live,
    /// Dropped its collections and starting over.
    Rebuilding,
    /// Stopped on an error; the checkpoint stands. `resume` retries.
    Failed,
    /// Paused by an operator.
    Paused,
    /// The runtime stopped.
    Stopped,
}

impl State {
    /// The name on the wire.
    pub fn name(self) -> &'static str {
        match self {
            State::Starting => "starting",
            State::CatchingUp => "catching_up",
            State::Live => "live",
            State::Rebuilding => "rebuilding",
            State::Failed => "failed",
            State::Paused => "paused",
            State::Stopped => "stopped",
        }
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A projection's status.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Status {
    /// The projection.
    pub name: String,
    /// Its state.
    pub state: State,
    /// Its committed checkpoint.
    pub checkpoint: Checkpoint,
    /// chron's confirmed head as the runtime last saw it.
    pub chron_head: u64,
    /// The error it stopped on, or the last reconnect error.
    pub error: Option<String>,
    /// Dead letters recorded.
    pub dead_letters: u64,
    /// Batches committed since the runtime started.
    pub batches: u64,
    /// Events applied since the runtime started.
    pub events: u64,
    /// When the last batch committed.
    pub last_batch_at: Option<SystemTime>,
    /// Whether and why the projection was rebuilt at start.
    pub rebuilt: Rebuilt,
    /// Times the source was resubscribed after an error.
    pub reconnects: u64,
}

impl Status {
    pub(crate) fn new(name: &str, checkpoint: Checkpoint, rebuilt: Rebuilt) -> Status {
        Status {
            name: name.to_owned(),
            state: State::Starting,
            checkpoint,
            chron_head: 0,
            error: None,
            dead_letters: 0,
            batches: 0,
            events: 0,
            last_batch_at: None,
            rebuilt,
            reconnects: 0,
        }
    }

    /// Events chron has confirmed that the projection has not applied.
    pub fn lag(&self) -> u64 {
        self.chron_head.saturating_sub(self.checkpoint.next)
    }
}

/// What an operator can ask a running projection.
#[derive(Debug)]
pub(crate) enum Control {
    Pause,
    Resume,
    Rebuild(tokio::sync::oneshot::Sender<crate::Result<()>>),
}

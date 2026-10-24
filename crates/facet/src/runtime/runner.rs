//! One projection's task: resume, take batches from its source's tail
//! (or read what the ring no longer holds), apply; fail, pause, resume
//! and rebuild on request.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chron_client::{Client, ClientError, Consistency, Event, ReadOptions, Source};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::apply::{self, BatchError, apply_batch};
use super::deadletter::{DEAD_LETTERS, DeadLetter};
use super::options::RuntimeOptions;
use super::projection::{OnError, Projection};
use super::ring::Take;
use super::status::{Control, State, Status};
use super::tail::{Tail, TailState};
use crate::data::ProjectionData;

pub(crate) struct RunnerCtx {
    pub def: Arc<Projection>,
    pub data: ProjectionData,
    pub client: Client,
    pub opts: Arc<RuntimeOptions>,
    pub db_id: Uuid,
    pub status: watch::Sender<Status>,
    pub control: mpsc::Receiver<Control>,
    pub head: watch::Receiver<u64>,
    pub fatal: watch::Receiver<Option<String>>,
    pub tail: Arc<Tail>,
    pub tail_state: watch::Receiver<TailState>,
    pub cancel: CancellationToken,
}

/// What the resume check decided.
enum Resume {
    Continue,
    Rebuild(String),
}

/// Why the loop could not go on.
enum Stop {
    /// Wait for an operator.
    Failed(String),
    /// Try again after a delay.
    Transient(String),
}

/// How long a runner waits for a signal before looking again anyway.
const IDLE_WAIT: Duration = Duration::from_secs(2);

fn confirmed() -> ReadOptions {
    ReadOptions {
        consistency: Some(Consistency::Confirmed),
        ..Default::default()
    }
}

impl RunnerCtx {
    fn set(&self, f: impl FnOnce(&mut Status)) {
        self.status.send_modify(f);
    }

    fn set_state(&self, state: State) {
        self.set(|s| s.state = state);
    }

    fn fail(&self, msg: String) {
        tracing::warn!(projection = %self.def.name(), %msg, "failed");
        self.set(|s| {
            s.state = State::Failed;
            s.error = Some(msg);
        });
    }

    fn dead_letters(&self) -> u64 {
        self.data
            .snapshot()
            .and_then(|s| s.table::<u64, DeadLetter>(DEAD_LETTERS)?.len())
            .unwrap_or(0)
    }

    /// Checks that the checkpoint still names a point in this log.
    async fn resume_check(&self) -> Result<Resume, Stop> {
        let info = self
            .client
            .info()
            .await
            .map_err(|e| Stop::Transient(e.to_string()))?;
        if info.db_id != self.db_id {
            return Err(Stop::Failed(format!(
                "chron now serves database {} instead of {}; restart the runtime (with rebind to start over)",
                info.db_id, self.db_id
            )));
        }
        let cp = self.data.checkpoint();
        if cp.next < info.floor && cp.last.is_some() {
            return Err(Stop::Failed(floor_message(info.floor, cp.next)));
        }
        let head = self
            .client
            .head_with(confirmed())
            .await
            .map_err(|e| Stop::Transient(e.to_string()))?;
        if cp.next > head.head {
            return Ok(Resume::Rebuild(format!(
                "the confirmed head is {} but the checkpoint is at {}: the log is shorter than what was applied",
                head.head, cp.next
            )));
        }
        let Some(last) = cp.last else {
            return Ok(Resume::Continue);
        };
        match self
            .client
            .read_all_with(last.position, 1, confirmed())
            .await
        {
            Ok(events) => match events.first() {
                Some(ev)
                    if ev.position == last.position
                        && apply::fingerprint(ev) == last.fingerprint =>
                {
                    Ok(Resume::Continue)
                }
                Some(ev) => Ok(Resume::Rebuild(format!(
                    "the event at {} is not the one applied (fingerprint {:08x}, applied {:08x}): the log was replaced",
                    last.position,
                    apply::fingerprint(ev),
                    last.fingerprint
                ))),
                None => Ok(Resume::Rebuild(format!(
                    "no event at {}: the log is shorter than what was applied",
                    last.position
                ))),
            },
            Err(ClientError::BelowFloor { .. }) => Ok(Resume::Continue),
            Err(ClientError::PositionOutOfRange { .. }) => Ok(Resume::Rebuild(format!(
                "no event at {}: the log is shorter than what was applied",
                last.position
            ))),
            Err(e) => Err(Stop::Transient(e.to_string())),
        }
    }

    /// Reads the next page straight from chron (the ring does not reach
    /// back to the checkpoint).
    async fn catch_up(&self, from: u64) -> Result<Vec<Event>, Stop> {
        let limit = self.opts.batch_events;
        let r = match &self.tail.source {
            Source::All => self.client.read_all_with(from, limit, confirmed()).await,
            Source::Tag(t) => {
                self.client
                    .read_tag_with(t.clone(), from, limit, confirmed())
                    .await
            }
            Source::Stream(n) => {
                self.client
                    .read_stream_with(n, from, limit, confirmed())
                    .await
            }
        };
        match r {
            Ok(events) => Ok(events),
            Err(ClientError::PositionOutOfRange { .. }) => Ok(Vec::new()),
            Err(ClientError::BelowFloor { floor, position }) => {
                Err(Stop::Failed(floor_message(floor, position)))
            }
            // Whether a missing stream is gone or not yet defined is the
            // tail's call (it ends, or defines it and connects).
            Err(ClientError::NotFound(m)) => {
                Err(Stop::Transient(format!("waiting for the source: {m}")))
            }
            Err(e) => Err(Stop::Transient(e.to_string())),
        }
    }

    /// Runs the batch on the blocking pool; on `Skip`, reruns with the
    /// failing positions recorded as dead letters.
    async fn apply(&self, events: Vec<Event>) -> Result<(), String> {
        let mut skipped: BTreeMap<u64, String> = BTreeMap::new();
        let events = Arc::new(events);
        for _ in 0..=events.len() {
            let def = self.def.clone();
            let data = self.data.clone();
            let evs = events.clone();
            let skip = skipped.clone();
            let r = tokio::task::spawn_blocking(move || apply_batch(&def, &data, &evs, &skip))
                .await
                .map_err(|e| format!("the batch task panicked: {e}"))?;
            match r {
                Ok(dead) => {
                    let n = events.len() as u64;
                    let now = SystemTime::now();
                    let cp = self.data.checkpoint();
                    let dead_total = if dead > 0 {
                        Some(self.dead_letters())
                    } else {
                        None
                    };
                    self.set(|s| {
                        s.batches += 1;
                        s.events += n;
                        s.checkpoint = cp;
                        s.last_batch_at = Some(now);
                        s.error = None;
                        if let Some(d) = dead_total {
                            s.dead_letters = d;
                        }
                    });
                    return Ok(());
                }
                Err(BatchError::Store(e)) => {
                    return Err(format!("the store refused the batch: {e}"));
                }
                Err(BatchError::Handler(f)) => match self.def.on_error() {
                    OnError::Fail => {
                        return Err(format!("event {}: {}", f.position, f.error));
                    }
                    OnError::Skip => {
                        tracing::warn!(projection = %self.def.name(), position = f.position, error = %f.error, "skipped");
                        skipped.insert(f.position, f.error);
                    }
                },
            }
        }
        Err("the batch kept failing after every event was skipped".into())
    }

    /// Handles one control message while running; `true` when the loop
    /// must restart from the top.
    fn on_control(&self, c: Control) -> bool {
        match c {
            Control::Pause => {
                self.set_state(State::Paused);
                true
            }
            Control::Resume => {
                self.set(|s| {
                    s.state = State::Starting;
                    s.error = None;
                });
                true
            }
            Control::Rebuild(reply) => {
                self.set_state(State::Rebuilding);
                let r = self.data.rebuild();
                let cp = self.data.checkpoint();
                self.set(|s| {
                    s.checkpoint = cp;
                    s.dead_letters = 0;
                    s.error = None;
                    s.state = State::Starting;
                });
                let _ = reply.send(r);
                true
            }
        }
    }

    /// Waits for an operator while failed or paused; `None` when the
    /// runtime stops.
    async fn wait_control(&mut self) -> Option<()> {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return None,
                c = self.control.recv() => match c {
                    None => return None,
                    Some(Control::Pause) => { self.set_state(State::Paused); }
                    Some(c) => { self.on_control(c); return Some(()); }
                }
            }
        }
    }

    /// What stops the loop right now, if anything: the runtime's fatal
    /// error, the tail's end, a cut-back past the checkpoint, retention
    /// past it.
    fn blocked(&self) -> Option<String> {
        if let Some(f) = self.fatal.borrow().as_ref() {
            return Some(f.clone());
        }
        let ts = self.tail_state.borrow();
        if let Some(e) = &ts.ended {
            return Some(e.clone());
        }
        let next = self.data.checkpoint().next;
        if let Some(p) = ts.cut_back
            && next > p
        {
            return Some(format!(
                "confirmed position {next} was cut back to {p}; rebuild to start over"
            ));
        }
        None
    }

    /// Sleeps until something changes: an event in the ring, the tail's
    /// state, a control message, the runtime stopping, or the idle wait.
    /// `Some(true)` to restart the outer loop, `None` to stop.
    async fn wait_signal(&mut self) -> Option<bool> {
        tokio::select! {
            _ = self.cancel.cancelled() => None,
            c = self.control.recv() => c.map(|c| self.on_control(c)),
            _ = self.tail_state.changed() => Some(false),
            _ = self.fatal.changed() => Some(false),
            _ = tokio::time::sleep(IDLE_WAIT) => Some(false),
        }
    }
}

fn floor_message(floor: u64, next: u64) -> String {
    format!(
        "retention passed the checkpoint: the floor is {floor} and the next position to apply {next}; rebuild to start from the floor"
    )
}

pub(crate) async fn run(mut ctx: RunnerCtx) {
    let max_events = ctx.opts.batch_events;
    let max_bytes = ctx.opts.batch_bytes;
    let mut backoff = ctx.opts.reconnect_min;
    'outer: loop {
        if ctx.cancel.is_cancelled() {
            ctx.set_state(State::Stopped);
            return;
        }
        let state = ctx.status.borrow().state;
        if matches!(state, State::Failed | State::Paused) {
            if ctx.wait_control().await.is_none() {
                ctx.set_state(State::Stopped);
                return;
            }
            continue;
        }
        // Mark the tail's state as seen so the next change wakes us.
        ctx.tail_state.borrow_and_update();
        if let Some(msg) = ctx.blocked() {
            ctx.fail(msg);
            continue;
        }
        match ctx.resume_check().await {
            Ok(Resume::Continue) => {}
            Ok(Resume::Rebuild(reason)) => {
                tracing::warn!(projection = %ctx.def.name(), %reason, "rebuilding");
                ctx.set_state(State::Rebuilding);
                if let Err(e) = ctx.data.rebuild() {
                    ctx.fail(format!("rebuild: {e}"));
                    continue;
                }
                let cp = ctx.data.checkpoint();
                ctx.set(|s| {
                    s.checkpoint = cp;
                    s.dead_letters = 0;
                });
            }
            Err(Stop::Failed(m)) => {
                ctx.fail(m);
                continue;
            }
            Err(Stop::Transient(m)) => {
                ctx.set(|s| s.error = Some(m));
                tokio::select! {
                    _ = ctx.cancel.cancelled() => { ctx.set_state(State::Stopped); return; }
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(ctx.opts.reconnect_max);
                continue;
            }
        }
        backoff = ctx.opts.reconnect_min;
        ctx.set(|s| {
            s.state = State::CatchingUp;
            s.error = None;
        });
        loop {
            if let Some(msg) = ctx.blocked() {
                ctx.fail(msg);
                continue 'outer;
            }
            {
                let ts = ctx.tail_state.borrow_and_update();
                let reconnects = ts.reconnects;
                let err = if ts.connected { None } else { ts.error.clone() };
                drop(ts);
                ctx.set(|s| {
                    s.reconnects = reconnects;
                    if let Some(e) = err {
                        s.error = Some(e);
                    }
                });
            }
            let cp = ctx.data.checkpoint();
            // Nothing applied yet: start where the log starts.
            let next = if cp.last.is_none() {
                cp.next.max(ctx.tail_state.borrow().floor)
            } else {
                cp.next
            };
            let took = ctx.tail.ring.take_from(next, max_events, max_bytes);
            let (batch, from_ring) = match took {
                Take::Events(b) => (b, true),
                Take::Empty => (Vec::new(), true),
                Take::Behind => match ctx.catch_up(next).await {
                    Ok(b) => (b, false),
                    Err(Stop::Failed(m)) => {
                        ctx.fail(m);
                        continue 'outer;
                    }
                    Err(Stop::Transient(m)) => {
                        ctx.set(|s| s.error = Some(m));
                        tokio::select! {
                            _ = ctx.cancel.cancelled() => { ctx.set_state(State::Stopped); return; }
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(ctx.opts.reconnect_max);
                        continue;
                    }
                },
            };
            backoff = ctx.opts.reconnect_min;
            if batch.is_empty() {
                // Caught up with what the ring holds (or with chron).
                let head = *ctx.head.borrow();
                let connected = ctx.tail_state.borrow().connected;
                ctx.set(|s| {
                    s.chron_head = head.max(s.checkpoint.next);
                    if connected && from_ring {
                        s.state = State::Live;
                    }
                });
                match ctx.wait_signal().await {
                    None => {
                        ctx.set_state(State::Stopped);
                        return;
                    }
                    Some(true) => continue 'outer,
                    Some(false) => continue,
                }
            }
            let full = batch.len() >= max_events;
            if let Err(m) = ctx.apply(batch).await {
                ctx.fail(m);
                continue 'outer;
            }
            let head = *ctx.head.borrow();
            ctx.set(|s| {
                s.chron_head = head.max(s.checkpoint.next);
                s.state = if full || !from_ring {
                    State::CatchingUp
                } else {
                    State::Live
                };
            });
        }
    }
}

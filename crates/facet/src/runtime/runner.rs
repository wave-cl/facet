//! One projection's task: resume, subscribe, batch, apply; reconnect on
//! errors; pause, resume and rebuild on request.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chron_client::{Client, ClientError, Consistency, EventStream, ReadOptions, Source};
use futures::{FutureExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::apply::{self, BatchError, apply_batch};
use super::deadletter::{DEAD_LETTERS, DeadLetter};
use super::options::RuntimeOptions;
use super::projection::{OnError, Projection};
use super::status::{Control, State, Status};
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
    pub cancel: CancellationToken,
}

/// What the resume check decided.
enum Resume {
    /// Carry on from the checkpoint.
    Continue,
    /// Drop everything and start over, for this reason.
    Rebuild(String),
}

/// Why the loop could not go on.
enum Stop {
    /// Wait for an operator.
    Failed(String),
    /// Try again after a delay (and a rediscovery).
    Transient(String),
}

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
        if cp.next < info.floor {
            return Err(Stop::Failed(format!(
                "retention passed the checkpoint: the floor is {} and the next position to apply {}; rebuild to start from the floor",
                info.floor, cp.next
            )));
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

    async fn subscribe(&self, source: &Source) -> Result<EventStream, Stop> {
        let from = self.data.checkpoint().next;
        match self
            .client
            .subscribe_source(source.clone(), from, Consistency::Confirmed)
            .await
        {
            Ok(s) => Ok(s),
            Err(ClientError::NotFound(m)) => Err(Stop::Failed(format!(
                "the source does not exist: {m} (a stream facet defines itself comes in a later version)"
            ))),
            Err(ClientError::BelowFloor { floor, .. }) => Err(Stop::Failed(format!(
                "retention passed the checkpoint: the floor is {floor} and the next position to apply {from}; rebuild to start from the floor"
            ))),
            Err(e) => Err(Stop::Transient(e.to_string())),
        }
    }

    fn classify_stream_error(&self, e: ClientError) -> Stop {
        match e {
            ClientError::CutBack { position, .. } => {
                if self.data.checkpoint().next > position {
                    Stop::Failed(format!(
                        "confirmed position {} was cut back to {position}; rebuild to start over",
                        self.data.checkpoint().next
                    ))
                } else {
                    Stop::Transient(format!(
                        "the node was cut back to {position}; resubscribing"
                    ))
                }
            }
            ClientError::BelowFloor { floor, position } => Stop::Failed(format!(
                "retention passed the checkpoint: the floor is {floor} and the next position to apply {position}; rebuild to start from the floor"
            )),
            ClientError::NotFound(m) => Stop::Failed(format!("the source is gone: {m}")),
            other => Stop::Transient(other.to_string()),
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

    /// Handles one control message while running; `true` when the
    /// current subscription must be dropped and the loop restarted.
    async fn on_control(&self, c: Control) -> bool {
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

    /// Waits for an operator while failed or paused.
    async fn wait_control(&mut self) -> Option<()> {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return None,
                c = self.control.recv() => match c {
                    None => return None,
                    Some(Control::Pause) => { self.set_state(State::Paused); }
                    Some(c) => { self.on_control(c).await; return Some(()); }
                }
            }
        }
    }
}

use chron_client::Event;

pub(crate) async fn run(mut ctx: RunnerCtx) {
    let source = ctx.def.resolved_source();
    let max_events = ctx.opts.batch_events;
    let max_bytes = ctx.opts.batch_bytes;
    let mut backoff = ctx.opts.reconnect_min;
    let mut first = true;
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
        if !first {
            ctx.set(|s| s.reconnects += 1);
        }
        first = false;
        // Resume check.
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
                if !pause_and_rediscover(&mut ctx, &mut backoff).await {
                    return;
                }
                continue;
            }
        }
        let mut stream = match ctx.subscribe(&source).await {
            Ok(s) => s,
            Err(Stop::Failed(m)) => {
                ctx.fail(m);
                continue;
            }
            Err(Stop::Transient(m)) => {
                ctx.set(|s| s.error = Some(m));
                if !pause_and_rediscover(&mut ctx, &mut backoff).await {
                    return;
                }
                continue;
            }
        };
        backoff = ctx.opts.reconnect_min;
        ctx.set(|s| {
            s.state = State::CatchingUp;
            s.error = None;
        });
        loop {
            let first_event = tokio::select! {
                _ = ctx.cancel.cancelled() => {
                    ctx.set_state(State::Stopped);
                    return;
                }
                c = ctx.control.recv() => {
                    match c {
                        None => { ctx.set_state(State::Stopped); return; }
                        Some(c) => {
                            if ctx.on_control(c).await { continue 'outer; }
                            continue;
                        }
                    }
                }
                ev = stream.next() => ev,
            };
            let mut batch = Vec::with_capacity(max_events.min(1024));
            let mut bytes = 0usize;
            let mut pending_error = None;
            match first_event {
                None => {
                    pending_error = Some(ClientError::Unavailable("the subscription ended".into()));
                }
                Some(Err(e)) => pending_error = Some(e),
                Some(Ok(ev)) => {
                    bytes += ev.payload.len();
                    batch.push(ev);
                    while batch.len() < max_events && bytes < max_bytes {
                        match stream.next().now_or_never() {
                            None => break,
                            Some(None) => {
                                pending_error =
                                    Some(ClientError::Unavailable("the subscription ended".into()));
                                break;
                            }
                            Some(Some(Err(e))) => {
                                pending_error = Some(e);
                                break;
                            }
                            Some(Some(Ok(ev))) => {
                                bytes += ev.payload.len();
                                batch.push(ev);
                            }
                        }
                    }
                }
            }
            if !batch.is_empty() {
                let full = batch.len() >= max_events || bytes >= max_bytes;
                if let Err(m) = ctx.apply(batch).await {
                    ctx.fail(m);
                    continue 'outer;
                }
                let head = *ctx.head.borrow();
                ctx.set(|s| {
                    s.chron_head = head.max(s.checkpoint.next);
                    s.state = if full { State::CatchingUp } else { State::Live };
                });
            }
            if let Some(e) = pending_error {
                match ctx.classify_stream_error(e) {
                    Stop::Failed(m) => {
                        ctx.fail(m);
                        continue 'outer;
                    }
                    Stop::Transient(m) => {
                        ctx.set(|s| s.error = Some(m));
                        drop(stream);
                        if !pause_and_rediscover(&mut ctx, &mut backoff).await {
                            return;
                        }
                        continue 'outer;
                    }
                }
            }
        }
    }
}

/// Sleeps the backoff (doubling it) and looks for the leader again;
/// `false` when the runtime is stopping.
async fn pause_and_rediscover(ctx: &mut RunnerCtx, backoff: &mut Duration) -> bool {
    tokio::select! {
        _ = ctx.cancel.cancelled() => {
            ctx.set_state(State::Stopped);
            return false;
        }
        _ = tokio::time::sleep(*backoff) => {}
    }
    *backoff = (*backoff * 2).min(ctx.opts.reconnect_max);
    if let Err(e) = ctx.client.rediscover().await {
        tracing::debug!(projection = %ctx.def.name(), error = %e, "rediscover");
    }
    true
}

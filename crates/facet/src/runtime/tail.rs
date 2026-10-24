//! One subscription per source, shared by every projection on it.

use std::sync::Arc;
use std::time::Duration;

use chron_client::{Client, ClientError, Consistency, Source};
use futures::StreamExt;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::options::RuntimeOptions;
use super::projection::StreamPolicy;
use super::ring::Ring;

/// A stream facet defines itself for an `Auto` source over several
/// event types.
#[derive(Debug, Clone)]
pub(crate) struct AutoStream {
    pub name: String,
    pub tags: Vec<bytes::Bytes>,
    pub policy: StreamPolicy,
}

/// What the tail knows about its source.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct TailState {
    /// Whether a subscription is open.
    pub connected: bool,
    /// Bumped on every event pushed and every state change, so waiters
    /// wake.
    pub tick: u64,
    /// Times the subscription was opened again after an error.
    pub reconnects: u64,
    /// The last error, kept while reconnecting.
    pub error: Option<String>,
    /// The node was cut back to this position; runners past it fail.
    pub cut_back: Option<u64>,
    /// The tail stopped for good, for this reason.
    pub ended: Option<String>,
    /// chron's floor as the tail last saw it: where a projection with
    /// nothing applied yet starts.
    pub floor: u64,
}

pub(crate) struct Tail {
    pub source: Source,
    pub ring: Arc<Ring>,
    pub state: watch::Sender<TailState>,
}

pub(crate) struct TailCtx {
    pub source: Source,
    pub auto: Option<AutoStream>,
    pub client: Client,
    pub opts: Arc<RuntimeOptions>,
    pub db_id: Uuid,
    pub ring: Arc<Ring>,
    pub state: watch::Sender<TailState>,
    /// The lowest checkpoint among the runners on this source: where a
    /// fresh subscription starts.
    pub floor: Arc<dyn Fn() -> u64 + Send + Sync>,
    pub cancel: CancellationToken,
}

impl TailCtx {
    fn set(&self, f: impl FnOnce(&mut TailState)) {
        self.state.send_modify(|s| {
            f(s);
            s.tick += 1;
        });
    }

    /// Defines the Auto stream, or checks the one that exists.
    async fn ensure_stream(&self) -> Result<(), String> {
        let Some(auto) = &self.auto else {
            return Ok(());
        };
        let streams = self
            .client
            .streams()
            .await
            .map_err(|e| format!("listing streams: {e}"))?;
        if let Some(existing) = streams.iter().find(|s| s.name == auto.name) {
            if existing.tags == auto.tags {
                return Ok(());
            }
            let show = |tags: &[bytes::Bytes]| {
                tags.iter()
                    .map(|t| String::from_utf8_lossy(t).into_owned())
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            match auto.policy {
                StreamPolicy::Refuse => {
                    return Err(format!(
                        "the chron stream {} exists over tags [{}], not the declared [{}]; delete it or set StreamPolicy::Recreate",
                        auto.name,
                        show(&existing.tags),
                        show(&auto.tags)
                    ));
                }
                StreamPolicy::Recreate => {
                    tracing::warn!(stream = %auto.name, "recreating the stream with other tags");
                    self.client
                        .delete_stream(&auto.name)
                        .await
                        .map_err(|e| format!("deleting stream {}: {e}", auto.name))?;
                }
            }
        }
        self.client
            .create_stream(&auto.name, auto.tags.clone())
            .await
            .map(|_| ())
            .or_else(|e| match e {
                ClientError::AlreadyExists(_) => Ok(()),
                e => Err(format!("defining stream {}: {e}", auto.name)),
            })
    }
}

/// Runs until cancelled: subscribes, fills the ring, reconnects.
pub(crate) async fn run(ctx: TailCtx) {
    let mut backoff = ctx.opts.reconnect_min;
    let mut first = true;
    loop {
        if ctx.cancel.is_cancelled() {
            return;
        }
        if !first {
            ctx.set(|s| s.reconnects += 1);
        }
        first = false;
        // Where does this log stand?
        let info = match ctx.client.info().await {
            Ok(i) => i,
            Err(e) => {
                ctx.set(|s| s.error = Some(e.to_string()));
                if !pause(&ctx, &mut backoff).await {
                    return;
                }
                continue;
            }
        };
        ctx.set(|s| s.floor = info.floor);
        if info.db_id != ctx.db_id {
            ctx.set(|s| {
                s.connected = false;
                s.ended = Some(format!(
                    "chron now serves database {} instead of {}; restart the runtime (with rebind to start over)",
                    info.db_id, ctx.db_id
                ));
            });
            ctx.cancel.cancelled().await;
            return;
        }
        if let Err(msg) = ctx.ensure_stream().await {
            // A refusal is final; a chron error is retried.
            let final_ = msg.contains("not the declared");
            ctx.set(|s| {
                s.connected = false;
                if final_ {
                    s.ended = Some(msg.clone());
                } else {
                    s.error = Some(msg.clone());
                }
            });
            if final_ {
                ctx.cancel.cancelled().await;
                return;
            }
            if !pause(&ctx, &mut backoff).await {
                return;
            }
            continue;
        }
        let from = (ctx.floor)().max(info.floor);
        let mut stream = match ctx
            .client
            .subscribe_source(ctx.source.clone(), from, Consistency::Confirmed)
            .await
        {
            Ok(s) => s,
            Err(ClientError::NotFound(m)) if ctx.auto.is_some() => {
                // The stream went away since the check: define it again.
                ctx.set(|s| s.error = Some(format!("the stream is gone ({m}); defining it again")));
                if !pause(&ctx, &mut backoff).await {
                    return;
                }
                continue;
            }
            Err(ClientError::NotFound(m)) => {
                ctx.set(|s| {
                    s.connected = false;
                    s.ended = Some(format!("the source does not exist: {m}"));
                });
                ctx.cancel.cancelled().await;
                return;
            }
            Err(e) => {
                ctx.set(|s| s.error = Some(e.to_string()));
                if !pause(&ctx, &mut backoff).await {
                    return;
                }
                continue;
            }
        };
        ctx.ring.reset(from);
        ctx.set(|s| {
            s.connected = true;
            s.error = None;
        });
        backoff = ctx.opts.reconnect_min;
        let err = loop {
            let item = tokio::select! {
                _ = ctx.cancel.cancelled() => return,
                item = stream.next() => item,
            };
            match item {
                None => break ClientError::Unavailable("the subscription ended".into()),
                Some(Err(e)) => break e,
                Some(Ok(ev)) => {
                    if ctx.ring.push(ev) {
                        ctx.set(|_| {});
                    }
                }
            }
        };
        drop(stream);
        ctx.ring.clear();
        match err {
            ClientError::CutBack { position, .. } => {
                ctx.set(|s| {
                    s.connected = false;
                    s.cut_back = Some(position);
                    s.error = Some(format!("the node was cut back to {position}"));
                });
            }
            ClientError::NotFound(m) | ClientError::Unavailable(m)
                if ctx.auto.is_none() && m.contains("deleted") =>
            {
                ctx.set(|s| {
                    s.connected = false;
                    s.ended = Some(format!("the source is gone: {m}"));
                });
                ctx.cancel.cancelled().await;
                return;
            }
            other => {
                ctx.set(|s| {
                    s.connected = false;
                    s.error = Some(other.to_string());
                });
            }
        }
        if !pause(&ctx, &mut backoff).await {
            return;
        }
    }
}

/// Sleeps the backoff (doubling it) and looks for the leader again;
/// `false` when the runtime is stopping.
async fn pause(ctx: &TailCtx, backoff: &mut Duration) -> bool {
    tokio::select! {
        _ = ctx.cancel.cancelled() => return false,
        _ = tokio::time::sleep(*backoff) => {}
    }
    *backoff = (*backoff * 2).min(ctx.opts.reconnect_max);
    if let Err(e) = ctx.client.rediscover().await {
        tracing::debug!(error = %e, "rediscover");
    }
    true
}

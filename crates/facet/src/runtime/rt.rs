//! The runtime: connects, registers, runs one tail per source and one
//! runner per projection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chron_client::{Client, Consistency, ReadOptions, Source};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::app::App;
use super::error::RuntimeError;
use super::handle::Handle;
use super::options::RuntimeOptions;
use super::projection::{self as def, Projection};
use super::ring::Ring;
use super::runner::{self, RunnerCtx};
use super::status::{Control, Status};
use super::tail::{self, AutoStream, Tail, TailCtx, TailState};
use crate::data::ProjectionData;
use crate::store::{OnDbMismatch, Store};

pub(crate) struct RunnerLink {
    pub def: Arc<Projection>,
    pub data: ProjectionData,
    pub status: watch::Receiver<Status>,
    pub control: mpsc::Sender<Control>,
}

pub(crate) struct Shared {
    pub app: App,
    pub store: Store,
    pub client: Client,
    pub db_id: Uuid,
    pub opts: Arc<RuntimeOptions>,
    pub order: Vec<String>,
    pub runners: HashMap<String, RunnerLink>,
    pub tails: Vec<Arc<Tail>>,
    /// chron's confirmed head, refreshed by the status task.
    pub head: watch::Sender<u64>,
    /// Set when the runtime as a whole cannot go on (the chron database
    /// changed under it).
    pub fatal: watch::Sender<Option<String>>,
    pub cancel: CancellationToken,
}

/// A running application: the store, the chron connection, one tail
/// per source and one task per projection.
pub struct Runtime {
    shared: Arc<Shared>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("db_id", &self.shared.db_id)
            .field("projections", &self.shared.order)
            .finish()
    }
}

fn source_key(s: &Source) -> String {
    match s {
        Source::All => "all".into(),
        Source::Tag(t) => format!("tag:{}", crate::value::base64_encode(t)),
        Source::Stream(n) => format!("stream:{n}"),
    }
}

impl Runtime {
    /// Connects to chron, opens the store bound to its database id,
    /// registers every projection (rebuilding what its declaration or
    /// version changed), defines the streams `Auto` sources need, and
    /// starts the tails and runners.
    pub async fn start(app: App, opts: RuntimeOptions) -> Result<Runtime, RuntimeError> {
        app.validate()?;
        if opts.chron.is_empty() {
            return Err(RuntimeError::Config("no chron address".into()));
        }
        let opts = Arc::new(opts);
        let client = Client::connect_any(opts.chron.clone(), opts.chron_config.clone()).await?;
        let info = client.info().await?;
        let db_id = info.db_id;
        let mut store_opts = opts.store.clone();
        if opts.rebind {
            store_opts.on_db_mismatch = OnDbMismatch::Rebuild;
        }
        let store = Store::open(&opts.data_dir, db_id, store_opts)?;
        let head = client
            .head_with(ReadOptions {
                consistency: Some(Consistency::Confirmed),
                ..Default::default()
            })
            .await
            .map(|h| h.head)
            .unwrap_or(info.head);
        let (head_tx, _) = watch::channel(head);
        let (fatal_tx, _) = watch::channel(None);
        let cancel = CancellationToken::new();

        // Register every projection first: a refused file stops the
        // start before anything runs.
        let mut registered = Vec::new();
        for def in app.projections() {
            let (data, reg) = store.register(&def.decl())?;
            registered.push((def.clone(), data, reg));
        }

        // One tail per distinct source.
        struct TailPlan {
            source: Source,
            auto: Option<AutoStream>,
            members: Vec<ProjectionData>,
            ring: Arc<Ring>,
            state: watch::Sender<TailState>,
        }
        let mut plans: Vec<TailPlan> = Vec::new();
        let mut tail_of: HashMap<String, usize> = HashMap::new();
        for (def, data, _) in &registered {
            let source = def.resolved_source();
            let key = source_key(&source);
            let idx = *tail_of.entry(key).or_insert_with(|| {
                let auto = match (&source, def.source()) {
                    (Source::Stream(name), def::Source::Auto) => Some(AutoStream {
                        name: name.clone(),
                        tags: def.event_types(),
                        policy: def.stream_policy(),
                    }),
                    _ => None,
                };
                let (state, _) = watch::channel(TailState::default());
                plans.push(TailPlan {
                    source: source.clone(),
                    auto,
                    members: Vec::new(),
                    ring: Arc::new(Ring::new(opts.batch_events.max(4096), 64 * 1024 * 1024)),
                    state,
                });
                plans.len() - 1
            });
            plans[idx].members.push(data.clone());
        }

        let mut runners = HashMap::new();
        let mut order = Vec::new();
        let mut starts = Vec::new();
        for (def, data, reg) in registered {
            let (status_tx, status_rx) =
                watch::channel(Status::new(def.name(), reg.checkpoint, reg.rebuilt));
            let (control_tx, control_rx) = mpsc::channel(8);
            order.push(def.name().to_owned());
            let idx = tail_of[&source_key(&def.resolved_source())];
            runners.insert(
                def.name().to_owned(),
                RunnerLink {
                    def: def.clone(),
                    data: data.clone(),
                    status: status_rx,
                    control: control_tx,
                },
            );
            starts.push((def, data, status_tx, control_rx, idx));
        }
        let mut tails = Vec::new();
        let mut tail_ctxs = Vec::new();
        for plan in plans {
            let members = plan.members.clone();
            let floor: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(move || {
                members
                    .iter()
                    .map(|d| d.checkpoint().next)
                    .min()
                    .unwrap_or(0)
            });
            tails.push(Arc::new(Tail {
                source: plan.source.clone(),
                ring: plan.ring.clone(),
                state: plan.state.clone(),
            }));
            tail_ctxs.push(TailCtx {
                source: plan.source,
                auto: plan.auto,
                client: client.clone(),
                opts: opts.clone(),
                db_id,
                ring: plan.ring,
                state: plan.state,
                floor,
                cancel: cancel.clone(),
            });
        }
        let shared = Arc::new(Shared {
            app,
            store,
            client,
            db_id,
            opts: opts.clone(),
            order,
            runners,
            tails: tails.clone(),
            head: head_tx,
            fatal: fatal_tx,
            cancel: cancel.clone(),
        });
        let mut tasks = Vec::new();
        for ctx in tail_ctxs {
            tasks.push(tokio::spawn(tail::run(ctx)));
        }
        for (def, data, status_tx, control_rx, idx) in starts {
            let ctx = RunnerCtx {
                def,
                data,
                client: shared.client.clone(),
                opts: opts.clone(),
                db_id,
                status: status_tx,
                control: control_rx,
                head: shared.head.subscribe(),
                fatal: shared.fatal.subscribe(),
                tail: tails[idx].clone(),
                tail_state: tails[idx].state.subscribe(),
                cancel: cancel.clone(),
            };
            tasks.push(tokio::spawn(runner::run(ctx)));
        }
        tasks.push(tokio::spawn(status_task(shared.clone())));
        Ok(Runtime {
            shared,
            tasks: Mutex::new(tasks),
        })
    }

    /// A handle for reads.
    pub fn handle(&self) -> Handle {
        Handle {
            shared: self.shared.clone(),
        }
    }

    /// The chron client.
    pub fn client(&self) -> &Client {
        &self.shared.client
    }

    /// The store.
    pub fn store(&self) -> &Store {
        &self.shared.store
    }

    /// The chron database id the store is bound to.
    pub fn db_id(&self) -> Uuid {
        self.shared.db_id
    }

    /// The application.
    pub fn app(&self) -> &App {
        &self.shared.app
    }

    /// A projection's status.
    pub fn status(&self, name: &str) -> Option<Status> {
        self.handle().status(name)
    }

    /// Every projection's status.
    pub fn statuses(&self) -> Vec<Status> {
        self.handle().statuses()
    }

    /// Why the runtime as a whole cannot go on, if so (the chron
    /// database changed under it).
    pub fn fatal(&self) -> Option<String> {
        self.shared.fatal.borrow().clone()
    }

    fn link(&self, name: &str) -> Result<&RunnerLink, RuntimeError> {
        self.shared
            .runners
            .get(name)
            .ok_or_else(|| RuntimeError::NoProjection(name.to_owned()))
    }

    /// Pauses a projection: it stops applying until resumed.
    pub async fn pause(&self, name: &str) -> Result<(), RuntimeError> {
        self.link(name)?
            .control
            .send(Control::Pause)
            .await
            .map_err(|_| RuntimeError::Stopping)
    }

    /// Resumes a paused or failed projection from its checkpoint.
    pub async fn resume(&self, name: &str) -> Result<(), RuntimeError> {
        self.link(name)?
            .control
            .send(Control::Resume)
            .await
            .map_err(|_| RuntimeError::Stopping)
    }

    /// Drops a projection's collections and starts it over from
    /// nothing; returns once the rebuild began.
    pub async fn rebuild(&self, name: &str) -> Result<(), RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.link(name)?
            .control
            .send(Control::Rebuild(tx))
            .await
            .map_err(|_| RuntimeError::Stopping)?;
        match rx.await {
            Ok(r) => Ok(r?),
            Err(_) => Err(RuntimeError::Stopping),
        }
    }

    /// Stops every task, waits for them and flushes the store.
    pub async fn shutdown(self) -> Result<(), RuntimeError> {
        self.shared.cancel.cancel();
        let tasks: Vec<JoinHandle<()>> =
            std::mem::take(&mut *self.tasks.lock().unwrap_or_else(|e| e.into_inner()));
        for t in tasks {
            let _ = t.await;
        }
        self.shared.store.flush()?;
        Ok(())
    }
}

/// Refreshes chron's confirmed head for the statuses and notices a
/// changed database.
async fn status_task(shared: Arc<Shared>) {
    let mut tick = tokio::time::interval(shared.opts.status_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shared.cancel.cancelled() => return,
            _ = tick.tick() => {}
        }
        if let Ok(h) = shared
            .client
            .head_with(ReadOptions {
                consistency: Some(Consistency::Confirmed),
                ..Default::default()
            })
            .await
        {
            shared.head.send_if_modified(|cur| {
                if *cur != h.head {
                    *cur = h.head;
                    true
                } else {
                    false
                }
            });
        }
        if let Ok(info) = shared.client.info().await
            && info.db_id != shared.db_id
        {
            let msg = format!(
                "chron now serves database {} instead of {}; restart the runtime (with rebind to start over)",
                info.db_id, shared.db_id
            );
            shared.fatal.send_if_modified(|f| {
                if f.is_none() {
                    *f = Some(msg);
                    true
                } else {
                    false
                }
            });
        }
    }
}

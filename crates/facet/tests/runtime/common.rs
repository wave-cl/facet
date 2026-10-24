#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use chron_client::{Client, ClientConfig, Expected, NewEvent, Transaction, Uuid};
use facet::{
    App, CounterHandle, Fsync, HandlerError, OnError, Options, Projection, RegisterHandle, Runtime,
    RuntimeOptions, Source, State,
};

pub fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// An in-process chron daemon.
pub struct Chron {
    pub dir: tempfile::TempDir,
    pub running: Option<chrond::Running>,
    pub addr: SocketAddr,
}

impl Chron {
    pub async fn start() -> Chron {
        Self::start_in(tmp(), "127.0.0.1:0".parse().unwrap()).await
    }

    /// Starts on `dir/data` listening at `addr` (port 0 picks one).
    pub async fn start_in(dir: tempfile::TempDir, addr: SocketAddr) -> Chron {
        let mut opts = chrond::Options::new(dir.path().join("data"), addr);
        opts.fsync = false;
        let running = chrond::start(opts).await.expect("chrond starts");
        let addr = running.local_addr;
        Chron {
            dir,
            running: Some(running),
            addr,
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub async fn client(&self) -> Client {
        Client::connect(self.url(), ClientConfig::default())
            .await
            .expect("connect")
    }

    /// Commits one transaction with these payloads under `tag` on one
    /// aggregate; returns the committed `next`.
    pub async fn commit(&self, tag: &str, payloads: &[&str]) -> u64 {
        commit(&self.client().await, tag, payloads).await
    }

    pub async fn shutdown(&mut self) {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("shutdown");
        }
    }

    pub async fn restart(&mut self) {
        self.shutdown().await;
        let mut opts = chrond::Options::new(self.dir.path().join("data"), self.addr);
        opts.fsync = false;
        let running = chrond::start(opts).await.expect("chrond restarts");
        self.running = Some(running);
    }
}

pub async fn commit(client: &Client, tag: &str, payloads: &[&str]) -> u64 {
    let mut t = Transaction::new();
    for p in payloads {
        t.append_one(
            Uuid::from_u128(1),
            Expected::Any,
            NewEvent::new(p.to_string()).tag(tag.to_string()),
        );
    }
    client.commit(t).await.expect("commit").next
}

/// A counter projection `c` over tag `t`: `n` counts the events, `sum`
/// (a register) keeps a running total of the numbers in the payloads,
/// read back inside the batch. A payload `bad` fails the
/// handler; `panic` panics.
pub fn counter_app(
    version: &str,
    on_error: OnError,
) -> (App, CounterHandle<(), i64>, RegisterHandle<i64>) {
    let (b, (n, sum)) = Projection::new("c")
        .version(version)
        .on_error(on_error)
        .source(Source::tag("t"))
        .collections(|c| {
            c.register::<u64>("last");
            (c.counter::<(), i64>("n"), c.register::<i64>("sum"))
        });
    let (n2, sum2) = (n.clone(), sum.clone());
    let p = b
        .on_raw("t", move |cx, ev| {
            let payload = std::str::from_utf8(&ev.payload).unwrap_or("");
            if payload == "bad" {
                return Err(HandlerError::Handler("bad payload".into()));
            }
            if payload == "panic" {
                panic!("a handler panic");
            }
            let value: i64 = payload.parse().unwrap_or(0);
            // Every position must be new: a repeat is a bug, named here.
            let position = cx.position();
            let last = cx.writer().register::<u64>("last")?.get()?;
            if let Some(last) = last
                && position <= last
            {
                return Err(HandlerError::Handler(format!(
                    "position {position} applied after {last}"
                )));
            }
            cx.writer().register::<u64>("last")?.set(&position)?;
            cx.counter(&n2)?.add(&(), 1)?;
            let so_far = cx.register(&sum2)?.get()?.unwrap_or(0);
            cx.register(&sum2)?.set(&(so_far + value))?;
            Ok(())
        })
        .build()
        .unwrap();
    (App::new().projection(p), n, sum)
}

pub fn rt_options(dir: &Path, chron: &Chron) -> RuntimeOptions {
    RuntimeOptions::new(dir.join("facet"), [chron.url()])
        .store(Options::new().fsync(Fsync::Never))
        .status_interval(Duration::from_millis(200))
        .reconnect(Duration::from_millis(50), Duration::from_millis(300))
}

pub async fn start_rt(app: App, dir: &Path, chron: &Chron) -> Runtime {
    Runtime::start(app, rt_options(dir, chron))
        .await
        .expect("the runtime starts")
}

/// Waits until the projection is live with its checkpoint at `next`.
pub async fn wait_applied(rt: &Runtime, name: &str, next: u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let s = rt.status(name).expect("known projection");
        if s.checkpoint.next >= next && matches!(s.state, State::Live | State::CatchingUp) {
            return;
        }
        if s.state == State::Failed {
            panic!("{name} failed: {:?}", s.error);
        }
        assert!(
            Instant::now() < deadline,
            "{name} stuck at {s:?}, want {next}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub async fn wait_state(rt: &Runtime, name: &str, state: State) -> facet::Status {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let s = rt.status(name).expect("known projection");
        if s.state == state {
            return s;
        }
        assert!(
            Instant::now() < deadline,
            "{name} stuck at {s:?}, want {state}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub fn counter_value(rt: &Runtime, n: &CounterHandle<(), i64>) -> i64 {
    rt.handle()
        .snapshot("c")
        .unwrap()
        .counter::<(), i64>(n.name())
        .unwrap()
        .get(&())
        .unwrap()
        .unwrap_or(0)
}

pub fn sum_value(rt: &Runtime, sum: &RegisterHandle<i64>) -> i64 {
    rt.handle()
        .snapshot("c")
        .unwrap()
        .register::<i64>(sum.name())
        .unwrap()
        .get()
        .unwrap()
        .unwrap_or(0)
}

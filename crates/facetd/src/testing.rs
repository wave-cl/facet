//! An in-process chrond and facetd for tests (feature `test-util`).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use chron_client::{Client, ClientConfig, Expected, NewEvent, Transaction, Uuid};
use facet::App;

pub use chrond::testing::{Pki, free_port, pki};

use crate::{Options, Running, serve};

/// A chrond and a facetd over it, each in a temporary directory.
pub struct Fixture {
    chron_dir: tempfile::TempDir,
    chron: Option<chrond::Running>,
    dir: tempfile::TempDir,
    running: Option<Running>,
}

impl Fixture {
    /// Starts both with the defaults (fsync off, loopback, no tokens).
    pub async fn start(app: App) -> Fixture {
        Self::start_with(app, |_| {}).await
    }

    /// Starts both, with `f` adjusting the daemon's options.
    pub async fn start_with(app: App, f: impl FnOnce(&mut Options)) -> Fixture {
        let chron_dir = tempfile::tempdir().expect("tempdir");
        let mut copts = chrond::Options::new(
            chron_dir.path().join("data"),
            "127.0.0.1:0".parse().unwrap(),
        );
        copts.fsync = false;
        let chron = chrond::start(copts).await.expect("chrond starts");
        let dir = tempfile::tempdir().expect("tempdir");
        let mut fx = Fixture {
            chron_dir,
            chron: Some(chron),
            dir,
            running: None,
        };
        fx.start_facetd(app, f).await;
        fx
    }

    async fn start_facetd(&mut self, app: App, f: impl FnOnce(&mut Options)) {
        let mut opts = Options::new(
            self.dir.path().join("facet"),
            [self.chron_addr()],
            "127.0.0.1:0".parse().unwrap(),
        );
        opts.runtime = opts
            .runtime
            .store(facet::Options::new().fsync(facet::Fsync::Never))
            .status_interval(Duration::from_millis(200))
            .reconnect(Duration::from_millis(50), Duration::from_millis(300));
        f(&mut opts);
        self.running = Some(serve(app, opts).await.expect("facetd starts"));
    }

    /// Stops facetd and starts it again over the same store and chron,
    /// with another application.
    pub async fn restart_with(mut self, app: App, f: impl FnOnce(&mut Options)) -> Fixture {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("facetd stops");
        }
        self.start_facetd(app, f).await;
        self
    }

    /// The facetd address, `http://127.0.0.1:port`.
    pub fn addr(&self) -> String {
        format!(
            "http://{}",
            self.running.as_ref().expect("running").local_addr
        )
    }

    /// Where `/metrics` is served, when enabled.
    pub fn metrics_addr(&self) -> SocketAddr {
        self.running
            .as_ref()
            .expect("running")
            .metrics_addr
            .expect("metrics enabled")
    }

    /// Where the HTTP gateway is served, when enabled.
    pub fn http_addr(&self) -> SocketAddr {
        self.running
            .as_ref()
            .expect("running")
            .http_addr
            .expect("http enabled")
    }

    /// The chron address.
    pub fn chron_addr(&self) -> String {
        format!(
            "http://{}",
            self.chron.as_ref().expect("chron running").local_addr
        )
    }

    /// The store directory.
    pub fn data_dir(&self) -> PathBuf {
        self.dir.path().join("facet")
    }

    /// The in-process handle.
    pub fn handle(&self) -> facet::Handle {
        self.running.as_ref().expect("running").handle.clone()
    }

    /// A chron client.
    pub async fn chron_client(&self) -> Client {
        Client::connect(self.chron_addr(), ClientConfig::default())
            .await
            .expect("connect to chron")
    }

    /// Commits one transaction of these payloads under `tag` on one
    /// aggregate; returns the committed `next`.
    pub async fn commit(&self, tag: &str, payloads: &[&str]) -> u64 {
        let client = self.chron_client().await;
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

    /// Waits until the projection applied up to `next`.
    pub async fn wait_applied(&self, name: &str, next: u64, within: Duration) {
        let h = self.handle();
        let deadline = Instant::now() + within;
        loop {
            let s = h.status(name).expect("known projection");
            if s.checkpoint.next >= next
                && matches!(s.state, facet::State::Live | facet::State::CatchingUp)
            {
                return;
            }
            if s.state == facet::State::Failed {
                panic!("{name} failed: {:?}", s.error);
            }
            assert!(
                Instant::now() < deadline,
                "{name} stuck at {s:?}, want {next}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until the projection is in `state`.
    pub async fn wait_state(
        &self,
        name: &str,
        state: facet::State,
        within: Duration,
    ) -> facet::Status {
        let h = self.handle();
        let deadline = Instant::now() + within;
        loop {
            let s = h.status(name).expect("known projection");
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

    /// Stops facetd, then chrond.
    pub async fn shutdown(mut self) {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("facetd stops");
        }
        if let Some(c) = self.chron.take() {
            c.shutdown().await.expect("chrond stops");
        }
        let _ = (&self.chron_dir, &self.dir);
    }
}

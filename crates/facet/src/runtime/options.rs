//! Runtime options.

use std::path::PathBuf;
use std::time::Duration;

use chron_client::ClientConfig;

/// How a [`super::Runtime`] runs.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RuntimeOptions {
    /// The store directory.
    pub data_dir: PathBuf,
    /// Store options (durability, caches).
    pub store: crate::Options,
    /// chron addresses to find the leader through.
    pub chron: Vec<String>,
    /// How to connect to chron (tokens, TLS, database).
    pub chron_config: ClientConfig,
    /// Rebuild projection files bound to another chron database instead
    /// of refusing to start.
    pub rebind: bool,
    /// Most events per batch. Default 256.
    pub batch_events: usize,
    /// Most payload bytes per batch. Default 4 MiB.
    pub batch_bytes: usize,
    /// How often chron's head is read for the status. Default 10 s.
    pub status_interval: Duration,
    /// The first reconnect delay. Default 100 ms.
    pub reconnect_min: Duration,
    /// The longest reconnect delay. Default 5 s.
    pub reconnect_max: Duration,
}

impl RuntimeOptions {
    /// Options for a store at `data_dir` over the chron reached through
    /// `seeds`.
    pub fn new(
        data_dir: impl Into<PathBuf>,
        seeds: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        RuntimeOptions {
            data_dir: data_dir.into(),
            store: crate::Options::new(),
            chron: seeds.into_iter().map(Into::into).collect(),
            chron_config: ClientConfig::default(),
            rebind: false,
            batch_events: 256,
            batch_bytes: 4 * 1024 * 1024,
            status_interval: Duration::from_secs(10),
            reconnect_min: Duration::from_millis(100),
            reconnect_max: Duration::from_secs(5),
        }
    }

    /// Sets [`RuntimeOptions::store`].
    pub fn store(mut self, o: crate::Options) -> Self {
        self.store = o;
        self
    }
    /// Sets [`RuntimeOptions::chron_config`].
    pub fn chron_config(mut self, c: ClientConfig) -> Self {
        self.chron_config = c;
        self
    }
    /// Sets [`RuntimeOptions::rebind`].
    pub fn rebind(mut self, yes: bool) -> Self {
        self.rebind = yes;
        self
    }
    /// Sets [`RuntimeOptions::batch_events`].
    pub fn batch_events(mut self, n: usize) -> Self {
        self.batch_events = n.max(1);
        self
    }
    /// Sets [`RuntimeOptions::batch_bytes`].
    pub fn batch_bytes(mut self, n: usize) -> Self {
        self.batch_bytes = n.max(1);
        self
    }
    /// Sets [`RuntimeOptions::status_interval`].
    pub fn status_interval(mut self, d: Duration) -> Self {
        self.status_interval = d;
        self
    }
    /// Sets the reconnect delays.
    pub fn reconnect(mut self, min: Duration, max: Duration) -> Self {
        self.reconnect_min = min;
        self.reconnect_max = max.max(min);
        self
    }
}

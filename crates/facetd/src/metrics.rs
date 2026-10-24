//! Prometheus metrics: calls counted as they happen, and a collector
//! reading the runtime's statuses on each scrape.

use std::sync::Arc;

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};

/// Seconds, from a millisecond to half a minute: the gRPC call buckets.
pub const BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Every `facet.v1` method and whether it streams.
pub const METHODS: &[(&str, bool)] = &[
    ("/facet.v1.Query/Get", false),
    ("/facet.v1.Query/Scan", false),
    ("/facet.v1.Query/Lookup", false),
    ("/facet.v1.Query/Range", false),
    ("/facet.v1.Query/Buckets", false),
    ("/facet.v1.Query/Watch", true),
    ("/facet.v1.Admin/Status", false),
    ("/facet.v1.Admin/ListProjections", false),
    ("/facet.v1.Admin/GetProjection", false),
    ("/facet.v1.Admin/DeadLetters", false),
    ("/facet.v1.Admin/Rebuild", false),
    ("/facet.v1.Admin/Pause", false),
    ("/facet.v1.Admin/Resume", false),
    ("/facet.v1.Admin/DropProjection", false),
    ("/grpc.health.v1.Health/Check", false),
    ("/grpc.health.v1.Health/Watch", true),
];

/// The method label for a request path: the path itself when it is a
/// known method, else `unknown`.
pub fn method_label(path: &str) -> (&'static str, bool) {
    METHODS
        .iter()
        .find(|(m, _)| *m == path)
        .map(|(m, s)| (*m, *s))
        .unwrap_or(("unknown", false))
}

/// gRPC status codes spelled as go-grpc-prometheus does.
pub fn code_label(code: i32) -> &'static str {
    match code {
        0 => "OK",
        1 => "Canceled",
        2 => "Unknown",
        3 => "InvalidArgument",
        4 => "DeadlineExceeded",
        5 => "NotFound",
        6 => "AlreadyExists",
        7 => "PermissionDenied",
        8 => "ResourceExhausted",
        9 => "FailedPrecondition",
        10 => "Aborted",
        11 => "OutOfRange",
        12 => "Unimplemented",
        13 => "Internal",
        14 => "Unavailable",
        15 => "DataLoss",
        16 => "Unauthenticated",
        _ => "Unknown",
    }
}

/// The registry and the families updated as calls happen.
pub struct Metrics {
    registry: Registry,
    /// Calls by method and status code.
    pub rpc_calls: IntCounterVec,
    /// Call durations by method.
    pub rpc_duration: HistogramVec,
    /// Streams open by method.
    pub streams_open: IntGaugeVec,
    /// Watchers attached.
    pub watchers: IntGauge,
    start_time: IntGauge,
}

impl Metrics {
    /// A registry with the call families and the runtime collector.
    pub fn new(handle: facet::Handle) -> Metrics {
        let registry = Registry::new();
        let rpc_calls = IntCounterVec::new(
            Opts::new(
                "facet_rpc_calls_total",
                "gRPC calls by method and status code",
            ),
            &["method", "code"],
        )
        .expect("valid opts");
        let rpc_duration = HistogramVec::new(
            HistogramOpts::new("facet_rpc_duration_seconds", "gRPC call durations")
                .buckets(BUCKETS.to_vec()),
            &["method"],
        )
        .expect("valid opts");
        let streams_open = IntGaugeVec::new(
            Opts::new("facet_streams_open", "gRPC streams open by method"),
            &["method"],
        )
        .expect("valid opts");
        let watchers =
            IntGauge::new("facet_watchers", "Watch streams attached").expect("valid opts");
        let start_time = IntGauge::new(
            "facet_start_time_seconds",
            "When the daemon started, unix seconds",
        )
        .expect("valid opts");
        start_time.set(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        );
        for c in [
            Box::new(rpc_calls.clone()) as Box<dyn Collector>,
            Box::new(rpc_duration.clone()),
            Box::new(streams_open.clone()),
            Box::new(watchers.clone()),
            Box::new(start_time.clone()),
            Box::new(RuntimeCollector::new(handle)),
        ] {
            registry.register(c).expect("fresh registry");
        }
        Metrics {
            registry,
            rpc_calls,
            rpc_duration,
            streams_open,
            watchers,
            start_time,
        }
    }

    /// The Prometheus text exposition.
    pub fn render(&self) -> String {
        let families = self.registry.gather();
        TextEncoder::new()
            .encode_to_string(&families)
            .unwrap_or_default()
    }

    /// When the daemon started, unix seconds.
    pub fn started_at(&self) -> i64 {
        self.start_time.get()
    }
}

/// Reads the runtime on each scrape.
struct RuntimeCollector {
    handle: facet::Handle,
    checkpoint: IntGaugeVec,
    lag: IntGaugeVec,
    state: IntGaugeVec,
    dead_letters: IntGaugeVec,
    batches: IntGaugeVec,
    events: IntGaugeVec,
    reconnects: IntGaugeVec,
    chron_head: IntGauge,
    fatal: IntGauge,
    source_connected: IntGaugeVec,
    source_buffered: IntGaugeVec,
    source_reconnects: IntGaugeVec,
}

const STATES: [facet::State; 7] = [
    facet::State::Starting,
    facet::State::CatchingUp,
    facet::State::Live,
    facet::State::Rebuilding,
    facet::State::Failed,
    facet::State::Paused,
    facet::State::Stopped,
];

impl RuntimeCollector {
    fn new(handle: facet::Handle) -> Self {
        let g = |name: &str, help: &str, labels: &[&str]| {
            IntGaugeVec::new(Opts::new(name, help), labels).expect("valid opts")
        };
        RuntimeCollector {
            handle,
            checkpoint: g(
                "facet_projection_checkpoint",
                "The next position the projection applies",
                &["projection"],
            ),
            lag: g(
                "facet_projection_lag_events",
                "Confirmed events not yet applied",
                &["projection"],
            ),
            state: g(
                "facet_projection_state",
                "1 for the projection's state",
                &["projection", "state"],
            ),
            dead_letters: g(
                "facet_projection_dead_letters",
                "Dead letters recorded",
                &["projection"],
            ),
            batches: g(
                "facet_projection_batches_total",
                "Batches committed since start",
                &["projection"],
            ),
            events: g(
                "facet_projection_events_total",
                "Events applied since start",
                &["projection"],
            ),
            reconnects: g(
                "facet_projection_reconnects_total",
                "Times the source was resubscribed",
                &["projection"],
            ),
            chron_head: IntGauge::new(
                "facet_chron_head",
                "chron's confirmed head as the daemon knows it",
            )
            .expect("valid opts"),
            fatal: IntGauge::new(
                "facet_fatal",
                "1 when the runtime cannot go on (the chron database changed)",
            )
            .expect("valid opts"),
            source_connected: g(
                "facet_source_connected",
                "1 while the source's subscription is open",
                &["source"],
            ),
            source_buffered: g(
                "facet_source_buffered",
                "Events the source's ring holds",
                &["source"],
            ),
            source_reconnects: g(
                "facet_source_reconnects_total",
                "Times the source was resubscribed",
                &["source"],
            ),
        }
    }

    fn update(&self) {
        for v in [
            &self.checkpoint,
            &self.lag,
            &self.state,
            &self.dead_letters,
            &self.batches,
            &self.events,
            &self.reconnects,
            &self.source_connected,
            &self.source_buffered,
            &self.source_reconnects,
        ] {
            v.reset();
        }
        let head = self.handle.chron_head();
        self.chron_head.set(head as i64);
        for s in self.handle.statuses() {
            let p = s.name.as_str();
            self.checkpoint
                .with_label_values(&[p])
                .set(s.checkpoint.next as i64);
            self.lag.with_label_values(&[p]).set(
                head.max(s.checkpoint.next)
                    .saturating_sub(s.checkpoint.next) as i64,
            );
            for st in STATES {
                self.state
                    .with_label_values(&[p, st.name()])
                    .set(i64::from(s.state == st));
            }
            self.dead_letters
                .with_label_values(&[p])
                .set(s.dead_letters as i64);
            self.batches.with_label_values(&[p]).set(s.batches as i64);
            self.events.with_label_values(&[p]).set(s.events as i64);
            self.reconnects
                .with_label_values(&[p])
                .set(s.reconnects as i64);
        }
        for src in self.handle.sources() {
            let label = crate::admin_svc::source_label(&src.source);
            self.source_connected
                .with_label_values(&[&label])
                .set(i64::from(src.connected));
            self.source_buffered
                .with_label_values(&[&label])
                .set(src.buffered as i64);
            self.source_reconnects
                .with_label_values(&[&label])
                .set(src.reconnects as i64);
        }
        self.fatal.set(0);
    }

    fn all(&self) -> Vec<&dyn Collector> {
        vec![
            &self.checkpoint,
            &self.lag,
            &self.state,
            &self.dead_letters,
            &self.batches,
            &self.events,
            &self.reconnects,
            &self.chron_head,
            &self.fatal,
            &self.source_connected,
            &self.source_buffered,
            &self.source_reconnects,
        ]
    }
}

impl Collector for RuntimeCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.all().into_iter().flat_map(|c| c.desc()).collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.update();
        self.all().into_iter().flat_map(|c| c.collect()).collect()
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Metrics")
    }
}

/// Shared by the layer and the services.
pub type Shared = Arc<Metrics>;

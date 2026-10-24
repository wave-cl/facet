//! The facet daemon: serves an application's projections over gRPC
//! (`facet.v1.Query` and `facet.v1.Admin`), with bearer tokens, TLS,
//! health and Prometheus metrics. An application binary embeds it:
//!
//! ```no_run
//! # fn app() -> facet::App { facet::App::new() }
//! #[derive(clap::Parser)]
//! struct Args {
//!     #[command(flatten)]
//!     daemon: facetd::Cli,
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let args = <Args as clap::Parser>::parse();
//!     facetd::run(app(), args.daemon).await
//! }
//! ```

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod admin_svc;
pub mod auth;
mod cli;
mod codec;
mod http;
mod http_json;
pub mod keys;
pub mod logging;
pub mod metrics;
mod metrics_http;
mod query_svc;
mod rpc_metrics;
#[cfg(feature = "test-util")]
pub mod testing;
mod trace;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use facet::{App, Runtime, RuntimeOptions};
use facet_proto::v1::admin_server::AdminServer;
use facet_proto::v1::query_server::QueryServer;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic_health::ServingStatus;

pub use cli::Cli;
pub use metrics::Metrics;

/// Server TLS: a certificate, its key, and optionally a CA that client
/// certificates must chain to (mTLS).
#[derive(Debug, Clone)]
pub struct Tls {
    /// The certificate chain, PEM.
    pub cert_pem: Vec<u8>,
    /// The private key, PEM.
    pub key_pem: Vec<u8>,
    /// Require client certificates signed by this CA, PEM.
    pub client_ca_pem: Option<Vec<u8>>,
}

impl Tls {
    fn server_config(&self) -> ServerTlsConfig {
        let mut cfg =
            ServerTlsConfig::new().identity(Identity::from_pem(&self.cert_pem, &self.key_pem));
        if let Some(ca) = &self.client_ca_pem {
            cfg = cfg.client_ca_root(Certificate::from_pem(ca));
        }
        cfg
    }
}

/// How the daemon runs.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Options {
    /// The runtime: store directory, chron seeds, batch sizes.
    pub runtime: RuntimeOptions,
    /// Address to serve gRPC on; port 0 picks a free port.
    pub listen: SocketAddr,
    /// Serve Prometheus metrics at `GET /metrics` on this address (plain
    /// HTTP, no token).
    pub metrics_listen: Option<SocketAddr>,
    /// Serve the HTTP/JSON gateway on this address (same tokens and TLS
    /// as gRPC).
    pub http_listen: Option<SocketAddr>,
    /// TLS for the gRPC listener.
    pub tls: Option<Tls>,
    /// A bearer token every call must carry.
    pub token: Option<String>,
    /// A separate token the admin calls must carry (`token` does not
    /// suffice for them when this is set).
    pub admin_token: Option<String>,
    /// Bound on each call; `None` for none.
    pub request_timeout: Option<Duration>,
    /// Largest gRPC message in either direction. Default 64 MiB.
    pub max_message_bytes: usize,
    /// Most concurrent calls per connection. Default 256.
    pub concurrency_per_connection: usize,
    /// HTTP/2 keepalive ping interval. Default 30 s.
    pub keepalive_interval: Duration,
    /// How long a keepalive ping may go unanswered. Default 10 s.
    pub keepalive_timeout: Duration,
    /// Entries a scan returns when the request says 0. Default 100.
    pub default_page: u32,
    /// Most entries a scan may return. Default 10 000.
    pub max_page: u32,
}

impl Options {
    /// Options for a store at `data_dir` over chron at `seeds`, serving
    /// on `listen`.
    pub fn new(
        data_dir: impl Into<PathBuf>,
        seeds: impl IntoIterator<Item = impl Into<String>>,
        listen: SocketAddr,
    ) -> Options {
        Options {
            runtime: RuntimeOptions::new(data_dir, seeds),
            listen,
            metrics_listen: None,
            http_listen: None,
            tls: None,
            token: None,
            admin_token: None,
            request_timeout: None,
            max_message_bytes: 64 * 1024 * 1024,
            concurrency_per_connection: 256,
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            default_page: 100,
            max_page: 10_000,
        }
    }
}

/// What the services share.
pub(crate) struct Ctx {
    pub runtime: Arc<Runtime>,
    pub handle: facet::Handle,
    pub metrics: Arc<Metrics>,
    pub started: Instant,
    pub opts: Arc<Options>,
    pub admin_token: Option<Vec<u8>>,
    pub cancel: CancellationToken,
}

impl Ctx {
    /// The admin calls: open when no admin token is configured.
    pub fn require_admin(&self, meta: &tonic::metadata::MetadataMap) -> Result<(), tonic::Status> {
        let Some(want) = &self.admin_token else {
            return Ok(());
        };
        match auth::bearer(meta) {
            Some(t) if auth::matches(t, want) => Ok(()),
            _ => Err(tonic::Status::permission_denied(
                "this call needs the admin token (--admin-token on the daemon)",
            )),
        }
    }
}

/// A running daemon.
pub struct Running {
    /// Where gRPC is served.
    pub local_addr: SocketAddr,
    /// Where `/metrics` is served, when [`Options::metrics_listen`] was
    /// set.
    pub metrics_addr: Option<SocketAddr>,
    /// Where the HTTP/JSON gateway is served, when
    /// [`Options::http_listen`] was set.
    pub http_addr: Option<SocketAddr>,
    /// Reads the projections in process.
    pub handle: facet::Handle,
    /// The runtime.
    pub runtime: Arc<Runtime>,
    cancel: CancellationToken,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
    metrics_server: Option<JoinHandle<()>>,
    http_server: Option<JoinHandle<()>>,
    live: JoinHandle<()>,
}

impl Running {
    /// Stops serving, then the runtime.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.cancel.cancel();
        let _ = self.server.await;
        if let Some(m) = self.metrics_server {
            let _ = m.await;
        }
        if let Some(h) = self.http_server {
            let _ = h.await;
        }
        self.live.abort();
        self.runtime.shutdown().await?;
        Ok(())
    }
}

/// Starts the runtime for `app` and serves it until [`Running::shutdown`].
pub async fn serve(app: App, opts: Options) -> anyhow::Result<Running> {
    if opts.token.as_deref() == Some("") || opts.admin_token.as_deref() == Some("") {
        anyhow::bail!("the token is empty");
    }
    if opts.request_timeout == Some(Duration::ZERO) {
        anyhow::bail!(
            "a request timeout of 0 would end every call at once; leave it unset for no bound"
        );
    }
    if !opts.listen.ip().is_loopback() && opts.token.is_none() && opts.tls.is_none() {
        tracing::warn!(listen = %opts.listen, "listening beyond loopback with no token and no TLS: anyone who can reach this address can read the projections");
    }
    let opts = Arc::new(opts);
    let runtime = Arc::new(
        Runtime::start(app, opts.runtime.clone())
            .await
            .context("starting the runtime")?,
    );
    let handle = runtime.handle();
    let metrics = Arc::new(Metrics::new(handle.clone()));
    let cancel = CancellationToken::new();
    let ctx = Arc::new(Ctx {
        runtime: runtime.clone(),
        handle: handle.clone(),
        metrics: metrics.clone(),
        started: Instant::now(),
        opts: opts.clone(),
        admin_token: opts.admin_token.as_ref().map(|t| t.as_bytes().to_vec()),
        cancel: cancel.clone(),
    });
    let listener = TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("cannot listen on {}", opts.listen))?;
    let local_addr = listener.local_addr()?;
    let bearer = auth::BearerAuth::new(
        &[opts.token.as_deref(), opts.admin_token.as_deref()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
    );
    let n = opts.max_message_bytes;
    let query_svc = Arc::new(query_svc::QueryService::new(ctx.clone()));
    let admin_svc = Arc::new(admin_svc::AdminService::new(ctx.clone()));
    let query = InterceptedService::new(
        QueryServer::from_arc(query_svc.clone())
            .max_decoding_message_size(n)
            .max_encoding_message_size(n),
        bearer.clone(),
    );
    let admin = InterceptedService::new(
        AdminServer::from_arc(admin_svc.clone())
            .max_decoding_message_size(n)
            .max_encoding_message_size(n),
        bearer.clone(),
    );
    let (http_addr, http_server) = match opts.http_listen {
        Some(addr) => {
            if !addr.ip().is_loopback() && opts.token.is_none() && opts.tls.is_none() {
                tracing::warn!(%addr, "the HTTP gateway listens beyond loopback with no token and no TLS");
            }
            let tls = match &opts.tls {
                Some(t) => Some(http::rustls_config(t).context("http tls")?),
                None => None,
            };
            let l = TcpListener::bind(addr)
                .await
                .with_context(|| format!("cannot listen on {addr} for the HTTP gateway"))?;
            let local = l.local_addr()?;
            let router = http::router(
                query_svc.clone(),
                admin_svc.clone(),
                metrics.clone(),
                bearer.clone(),
                opts.request_timeout,
            );
            let task = tokio::spawn(http::serve(l, router, tls, cancel.clone()));
            (Some(local), Some(task))
        }
        None => (None, None),
    };
    let (health, health_svc) = tonic_health::server::health_reporter();
    health
        .set_serving::<QueryServer<query_svc::QueryService>>()
        .await;
    health
        .set_serving::<AdminServer<admin_svc::AdminService>>()
        .await;
    health.set_service_status("", ServingStatus::Serving).await;
    let live = spawn_live_health(health, handle.clone(), cancel.clone());
    let mut builder = tonic::transport::Server::builder()
        .tcp_nodelay(true)
        .http2_keepalive_interval(Some(opts.keepalive_interval))
        .http2_keepalive_timeout(Some(opts.keepalive_timeout))
        .concurrency_limit_per_connection(opts.concurrency_per_connection)
        .layer(trace::layer())
        .layer(rpc_metrics::RpcMetricsLayer::new(metrics.clone()));
    let (metrics_addr, metrics_server) = match opts.metrics_listen {
        Some(addr) => {
            if !addr.ip().is_loopback() {
                tracing::warn!(%addr, "metrics listen beyond loopback with no token: checkpoints, lag and version are visible to anyone who can reach it");
            }
            let l = TcpListener::bind(addr)
                .await
                .with_context(|| format!("cannot listen on {addr} for metrics"))?;
            let local = l.local_addr()?;
            let task = tokio::spawn(metrics_http::serve(l, metrics.clone(), cancel.clone()));
            (Some(local), Some(task))
        }
        None => (None, None),
    };
    if let Some(t) = opts.request_timeout {
        builder = builder.timeout(t);
    }
    if let Some(tls) = &opts.tls {
        builder = builder.tls_config(tls.server_config()).context("tls")?;
    }
    let shutdown = cancel.clone().cancelled_owned();
    let server = tokio::spawn(
        builder
            .add_service(health_svc)
            .add_service(query)
            .add_service(admin)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown),
    );
    tracing::info!(%local_addr, projections = ?handle.names(), "serving");
    Ok(Running {
        local_addr,
        metrics_addr,
        http_addr,
        handle,
        runtime,
        cancel,
        server,
        metrics_server,
        http_server,
        live,
    })
}

/// `facet.v1.Query.live` serves only while every projection is live or
/// catching up with no lag.
fn spawn_live_health(
    health: tonic_health::server::HealthReporter,
    handle: facet::Handle,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut was: Option<bool> = None;
        loop {
            let ok = handle.statuses().iter().all(|s| {
                matches!(s.state, facet::State::Live)
                    || (matches!(s.state, facet::State::CatchingUp) && s.lag() == 0)
            });
            if was != Some(ok) {
                health
                    .set_service_status(
                        "facet.v1.Query.live",
                        if ok {
                            ServingStatus::Serving
                        } else {
                            ServingStatus::NotServing
                        },
                    )
                    .await;
                was = Some(ok);
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
        }
    })
}

/// Runs the daemon for `app` as a process: logging per the CLI, serve,
/// stop on SIGINT or SIGTERM.
pub async fn run(app: App, cli: Cli) -> anyhow::Result<()> {
    let format = cli.log_format;
    let filter = tracing_subscriber::EnvFilter::try_from_env("FACETD_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing::subscriber::set_global_default(logging::subscriber(
        format,
        filter,
        std::io::stderr,
    ));
    let opts = cli.into_options()?;
    let running = serve(app, opts).await?;
    tracing::info!(addr = %running.local_addr, "facetd started");
    wait_for_signal().await;
    tracing::info!("stopping");
    running.shutdown().await
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

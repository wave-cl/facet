//! The command-line flags an application binary flattens into its own.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;

use crate::logging::LogFormat;
use crate::{Options, Tls};

/// The daemon's flags. Flatten into the application's `clap::Parser`
/// and hand to [`crate::run`].
#[derive(Debug, Clone, clap::Args)]
pub struct Cli {
    /// The store directory; created if missing.
    #[arg(long, env = "FACET_DATA_DIR")]
    pub data_dir: PathBuf,
    /// chron addresses (`http://host:port`), comma separated; the leader
    /// is found among them.
    #[arg(long, env = "FACET_CHRON", value_delimiter = ',', required = true)]
    pub chron: Vec<String>,
    /// The chron database to follow (the daemon's `default` otherwise).
    #[arg(long, env = "FACET_CHRON_DB")]
    pub chron_db: Option<String>,
    /// Bearer token sent to chron.
    #[arg(long, env = "FACET_CHRON_TOKEN", conflicts_with = "chron_token_file")]
    pub chron_token: Option<String>,
    /// A file holding the token sent to chron.
    #[arg(long, env = "FACET_CHRON_TOKEN_FILE")]
    pub chron_token_file: Option<PathBuf>,
    /// chron's admin token (to define streams); `--chron-token` otherwise.
    #[arg(
        long,
        env = "FACET_CHRON_ADMIN_TOKEN",
        conflicts_with = "chron_admin_token_file"
    )]
    pub chron_admin_token: Option<String>,
    /// A file holding chron's admin token.
    #[arg(long, env = "FACET_CHRON_ADMIN_TOKEN_FILE")]
    pub chron_admin_token_file: Option<PathBuf>,
    /// The CA that chron's certificate chains to (an `https://` chron).
    #[arg(long, env = "FACET_CHRON_CA")]
    pub chron_ca: Option<PathBuf>,
    /// A client certificate to present to chron (mTLS), with its key.
    #[arg(long, env = "FACET_CHRON_CERT", requires = "chron_key")]
    pub chron_cert: Option<PathBuf>,
    /// The key for `--chron-cert`.
    #[arg(long, env = "FACET_CHRON_KEY", requires = "chron_cert")]
    pub chron_key: Option<PathBuf>,
    /// The name chron's certificate is checked against, when it is not
    /// the address's host.
    #[arg(long, env = "FACET_CHRON_DOMAIN")]
    pub chron_domain: Option<String>,
    /// Address to serve gRPC on.
    #[arg(long, env = "FACET_LISTEN", default_value = "127.0.0.1:4343")]
    pub listen: SocketAddr,
    /// Serve Prometheus metrics at `GET /metrics` here (plain HTTP).
    #[arg(long, env = "FACET_METRICS_LISTEN")]
    pub metrics_listen: Option<SocketAddr>,
    /// Serve the HTTP/JSON gateway here (same tokens and TLS as gRPC).
    #[arg(long, env = "FACET_HTTP_LISTEN")]
    pub http_listen: Option<SocketAddr>,
    /// Serve TLS with this certificate (PEM), with `--tls-key`.
    #[arg(long, env = "FACET_TLS_CERT", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,
    /// The key for `--tls-cert`.
    #[arg(long, env = "FACET_TLS_KEY", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
    /// Require client certificates chaining to this CA (mTLS).
    #[arg(long, env = "FACET_TLS_CLIENT_CA", requires = "tls_cert")]
    pub tls_client_ca: Option<PathBuf>,
    /// A bearer token every call must carry.
    #[arg(long, env = "FACET_TOKEN", conflicts_with = "token_file")]
    pub token: Option<String>,
    /// A file holding that token.
    #[arg(long, env = "FACET_TOKEN_FILE")]
    pub token_file: Option<PathBuf>,
    /// A separate token the admin calls (rebuild, pause, resume, drop)
    /// must carry.
    #[arg(long, env = "FACET_ADMIN_TOKEN", conflicts_with = "admin_token_file")]
    pub admin_token: Option<String>,
    /// A file holding the admin token.
    #[arg(long, env = "FACET_ADMIN_TOKEN_FILE")]
    pub admin_token_file: Option<PathBuf>,
    /// Do not fsync each batch: a batch survives a process crash but not
    /// power loss until the next durable write.
    #[arg(long)]
    pub no_fsync: bool,
    /// Rebuild projection files bound to another chron database instead
    /// of refusing to start.
    #[arg(long)]
    pub rebind: bool,
    /// Most events per batch.
    #[arg(long, env = "FACET_BATCH_EVENTS", default_value_t = 256, value_parser = clap::value_parser!(u32).range(1..))]
    pub batch_events: u32,
    /// Most payload bytes per batch.
    #[arg(long, env = "FACET_BATCH_BYTES", default_value_t = 4 * 1024 * 1024, value_parser = clap::value_parser!(u64).range(1..))]
    pub batch_bytes: u64,
    /// Bound on each gRPC call, in seconds; none by default.
    #[arg(long, env = "FACET_REQUEST_TIMEOUT")]
    pub request_timeout: Option<f64>,
    /// Largest gRPC message in either direction, in bytes.
    #[arg(long, env = "FACET_MAX_MESSAGE_BYTES", default_value_t = 64 * 1024 * 1024)]
    pub max_message_bytes: usize,
    /// How log lines are written.
    #[arg(long, env = "FACETD_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    pub log_format: LogFormat,
}

fn read_pem(what: &str, p: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    std::fs::read(p).with_context(|| format!("reading the {what} {}", p.display()))
}

fn read_token(
    inline: Option<String>,
    file: Option<&PathBuf>,
    what: &str,
) -> anyhow::Result<Option<String>> {
    match (inline, file) {
        (Some(t), _) => Ok(Some(t)),
        (None, Some(p)) => {
            let t = std::fs::read_to_string(p)
                .with_context(|| format!("reading the {what} file {}", p.display()))?;
            let t = t.trim().to_string();
            if t.is_empty() {
                anyhow::bail!("the {what} file {} is empty", p.display());
            }
            Ok(Some(t))
        }
        (None, None) => Ok(None),
    }
}

impl Cli {
    /// Resolves the flags (reading token and certificate files).
    pub fn into_options(self) -> anyhow::Result<Options> {
        let mut opts = Options::new(&self.data_dir, self.chron.clone(), self.listen);
        let mut cc = chron_client::ClientConfig {
            token: read_token(
                self.chron_token,
                self.chron_token_file.as_ref(),
                "chron token",
            )?,
            admin_token: read_token(
                self.chron_admin_token,
                self.chron_admin_token_file.as_ref(),
                "chron admin token",
            )?,
            database: self.chron_db,
            ..Default::default()
        };
        if self.chron_ca.is_some() || self.chron_cert.is_some() || self.chron_domain.is_some() {
            cc.tls = Some(chron_client::TlsConfig {
                ca_pem: self
                    .chron_ca
                    .as_ref()
                    .map(|p| read_pem("chron CA", p))
                    .transpose()?,
                identity: match (&self.chron_cert, &self.chron_key) {
                    (Some(c), Some(k)) => Some(chron_client::Identity {
                        cert_pem: read_pem("chron client certificate", c)?,
                        key_pem: read_pem("chron client key", k)?,
                    }),
                    _ => None,
                },
                domain: self.chron_domain,
            });
        }
        opts.runtime = opts
            .runtime
            .chron_config(cc)
            .rebind(self.rebind)
            .batch_events(self.batch_events as usize)
            .batch_bytes(usize::try_from(self.batch_bytes).unwrap_or(usize::MAX))
            .store(facet::Options::new().fsync(if self.no_fsync {
                facet::Fsync::Never
            } else {
                facet::Fsync::Always
            }));
        opts.metrics_listen = self.metrics_listen;
        opts.http_listen = self.http_listen;
        opts.tls = match (&self.tls_cert, &self.tls_key) {
            (Some(c), Some(k)) => Some(Tls {
                cert_pem: read_pem("TLS certificate", c)?,
                key_pem: read_pem("TLS key", k)?,
                client_ca_pem: self
                    .tls_client_ca
                    .as_ref()
                    .map(|p| read_pem("client CA", p))
                    .transpose()?,
            }),
            _ => None,
        };
        opts.token = read_token(self.token, self.token_file.as_ref(), "token")?;
        opts.admin_token = read_token(
            self.admin_token,
            self.admin_token_file.as_ref(),
            "admin token",
        )?;
        opts.request_timeout = match self.request_timeout {
            Some(s) if s > 0.0 => Some(Duration::from_secs_f64(s)),
            Some(_) => anyhow::bail!("--request-timeout must be more than 0"),
            None => None,
        };
        opts.max_message_bytes = self.max_message_bytes;
        Ok(opts)
    }
}

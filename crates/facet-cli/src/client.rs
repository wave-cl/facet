//! The connection flags and the client they make.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use facet_client::{Client, ClientConfig, Identity, TlsConfig};

#[derive(clap::Args, Debug, Clone)]
pub struct Conn {
    /// The daemon (http://host:port or https://host:port).
    #[arg(
        long,
        env = "FACET_ADDR",
        default_value = "http://127.0.0.1:4343",
        global = true
    )]
    pub addr: String,
    /// Bearer token for every call.
    #[arg(long, env = "FACET_TOKEN", global = true)]
    pub token: Option<String>,
    /// Bearer token for the admin calls (rebuild, pause, resume, drop);
    /// --token otherwise.
    #[arg(long, env = "FACET_ADMIN_TOKEN", global = true)]
    pub admin_token: Option<String>,
    /// The CA the daemon's certificate chains to (an https:// address).
    #[arg(long, env = "FACET_TLS_CA", global = true)]
    pub tls_ca: Option<PathBuf>,
    /// A client certificate to present (mTLS), with --tls-key.
    #[arg(long, env = "FACET_TLS_CERT", global = true, requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,
    /// The key for --tls-cert.
    #[arg(long, env = "FACET_TLS_KEY", global = true, requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
    /// The name the daemon's certificate is checked against.
    #[arg(long, env = "FACET_TLS_DOMAIN", global = true)]
    pub tls_domain: Option<String>,
    /// Seconds to wait for the connection.
    #[arg(
        long,
        env = "FACET_CONNECT_TIMEOUT",
        default_value_t = 3.0,
        global = true
    )]
    pub connect_timeout: f64,
}

impl Conn {
    pub async fn connect(&self) -> anyhow::Result<Client> {
        let mut config = ClientConfig {
            token: self.token.clone(),
            admin_token: self.admin_token.clone(),
            connect_timeout: Duration::from_secs_f64(self.connect_timeout.max(0.1)),
            ..Default::default()
        };
        if self.tls_ca.is_some() || self.tls_cert.is_some() || self.tls_domain.is_some() {
            let read =
                |p: &PathBuf| std::fs::read(p).with_context(|| format!("reading {}", p.display()));
            config.tls = Some(TlsConfig {
                ca_pem: self.tls_ca.as_ref().map(read).transpose()?,
                identity: match (&self.tls_cert, &self.tls_key) {
                    (Some(c), Some(k)) => Some(Identity {
                        cert_pem: read(c)?,
                        key_pem: read(k)?,
                    }),
                    _ => None,
                },
                domain: self.tls_domain.clone(),
            });
        }
        Ok(Client::connect(&self.addr, config).await?)
    }
}

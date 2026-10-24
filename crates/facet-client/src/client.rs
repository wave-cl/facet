//! The client.

use std::pin::Pin;
use std::time::Duration;

use facet_proto::v1 as pb;
use facet_proto::v1::admin_client::AdminClient;
use facet_proto::v1::query_client::QueryClient;
use futures::{Stream, StreamExt};
use serde_json::Value as Json;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::error::ClientError;

/// A client certificate and its key, both PEM (mTLS).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The certificate chain.
    pub cert_pem: Vec<u8>,
    /// The private key.
    pub key_pem: Vec<u8>,
}

/// How to reach a daemon over TLS. Only an `https://` address uses it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsConfig {
    /// A private CA; without it the system roots are used.
    pub ca_pem: Option<Vec<u8>>,
    /// A certificate to present to the daemon (mTLS).
    pub identity: Option<Identity>,
    /// The name to verify the server's certificate against, when it is
    /// not the address's host.
    pub domain: Option<String>,
}

/// How to connect: transport, credentials and limits.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// TLS settings for an `https://` address.
    pub tls: Option<TlsConfig>,
    /// Sent as `authorization: Bearer <token>` on every call.
    pub token: Option<String>,
    /// Sent instead of `token` on the admin calls (rebuild, pause,
    /// resume, drop) when the daemon requires a separate admin token.
    pub admin_token: Option<String>,
    /// Bound on establishing the connection.
    pub connect_timeout: Duration,
    /// Bound on each call's response head; streams keep running after
    /// it. `None` (the default) waits as long as the daemon takes.
    pub request_timeout: Option<Duration>,
    /// Largest message accepted from, or sent to, the daemon; default
    /// 64 MiB.
    pub max_message_bytes: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            tls: None,
            token: None,
            admin_token: None,
            connect_timeout: Duration::from_secs(3),
            request_timeout: None,
            max_message_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Read your writes: wait until the projection applied `at_least`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadAt {
    /// A chron commit's `next`.
    pub at_least: Option<u64>,
    /// How long to wait for it; `None` is the daemon's default (5 s),
    /// at most 30 s.
    pub wait: Option<Duration>,
}

impl ReadAt {
    /// Wait for `at_least` with the daemon's default timeout.
    pub fn at_least(position: u64) -> ReadAt {
        ReadAt {
            at_least: Some(position),
            wait: None,
        }
    }

    fn wire(self) -> Option<pb::ReadAt> {
        self.at_least.map(|p| pb::ReadAt {
            at_least: Some(p),
            wait_ms: self
                .wait
                .map(|d| d.as_millis().min(u32::MAX as u128) as u32),
        })
    }
}

/// What a read saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seen {
    /// The projection's checkpoint: the next position it will apply.
    pub checkpoint: u64,
    /// Its state.
    pub state: pb::State,
    /// chron's confirmed head as the daemon knows it.
    pub chron_head: u64,
}

impl Seen {
    fn from_wire(s: Option<pb::Seen>) -> Seen {
        let s = s.unwrap_or_default();
        Seen {
            checkpoint: s.checkpoint,
            state: s.state(),
            chron_head: s.chron_head,
        }
    }
}

/// One entry of a scan.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    /// The key's parts.
    pub key: Json,
    /// The value.
    pub value: Json,
}

/// A page of entries.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    /// The entries, in scan order.
    pub entries: Vec<Entry>,
    /// Where to continue, `None` when exhausted.
    pub next: Option<Vec<u8>>,
    /// What the read saw.
    pub seen: Seen,
}

fn json(text: &str) -> Result<Json, ClientError> {
    if text.is_empty() {
        return Ok(Json::Null);
    }
    serde_json::from_str(text).map_err(|e| ClientError::Decode(format!("{e}: {text}")))
}

fn text(j: Option<&Json>) -> String {
    j.map(|j| j.to_string()).unwrap_or_default()
}

fn page(r: pb::ScanResponse) -> Result<Page, ClientError> {
    let mut entries = Vec::with_capacity(r.entries.len());
    for e in r.entries {
        entries.push(Entry {
            key: json(&e.key)?,
            value: json(&e.value)?,
        });
    }
    Ok(Page {
        entries,
        next: (!r.next.is_empty()).then_some(r.next),
        seen: Seen::from_wire(r.seen),
    })
}

/// A scan's parameters, for [`Client::scan`] and [`Client::lookup`].
#[derive(Debug, Clone, Default)]
pub struct Scan {
    /// Leading key parts every entry shares.
    pub prefix: Option<Json>,
    /// Entries at or above these leading parts.
    pub from: Option<Json>,
    /// Entries at or below these leading parts.
    pub to: Option<Json>,
    /// The previous page's `next`.
    pub after: Option<Vec<u8>>,
    /// At most this many (0 = the daemon's default).
    pub limit: u32,
    /// Descending.
    pub reverse: bool,
    /// Read your writes.
    pub at: ReadAt,
}

/// One batch's changes to a watched collection.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    /// The checkpoint the batch committed.
    pub checkpoint: u64,
    /// The collection.
    pub collection: String,
    /// The keys that changed (empty with `all`).
    pub keys: Vec<Json>,
    /// Everything may have changed: re-read.
    pub all: bool,
}

/// A stream of changes.
pub type ChangeStream = Pin<Box<dyn Stream<Item = Result<Change, ClientError>> + Send + 'static>>;

/// A source's subscription as the daemon reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceStatus {
    /// The source.
    pub source: String,
    /// Whether a subscription is open.
    pub connected: bool,
    /// Times it was opened again after an error.
    pub reconnects: u64,
    /// The last error, if any.
    pub error: Option<String>,
    /// Events buffered.
    pub buffered: u64,
}

/// The daemon's status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonStatus {
    /// The daemon's version.
    pub version: String,
    /// Seconds since it started.
    pub uptime_secs: u64,
    /// The store directory.
    pub store_path: String,
    /// The chron database id the store is bound to.
    pub chron_db_id: String,
    /// chron's confirmed head as the daemon knows it.
    pub chron_head: u64,
    /// Why the runtime as a whole cannot go on, if so.
    pub fatal: Option<String>,
    /// Projections per state: starting, catching up, live, rebuilding,
    /// failed, paused.
    pub counts: [u32; 6],
    /// The sources followed.
    pub sources: Vec<SourceStatus>,
}

/// One collection as the daemon describes it.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionInfo {
    /// The name.
    pub name: String,
    /// The kind.
    pub kind: String,
    /// The key shape (`kinds`, `nullable`, `names`).
    pub key_shape: Json,
    /// The declaration.
    pub decl: Json,
    /// The secondary indexes (tables).
    pub indexes: Vec<String>,
}

/// A projection as the daemon reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionStatus {
    /// The name.
    pub name: String,
    /// The version string.
    pub version: String,
    /// The state.
    pub state: pb::State,
    /// The checkpoint.
    pub checkpoint: u64,
    /// chron's confirmed head.
    pub chron_head: u64,
    /// Events not yet applied.
    pub lag: u64,
    /// The error it stopped on, if any.
    pub error: Option<String>,
    /// The source.
    pub source: String,
    /// The event types handled.
    pub event_types: Vec<String>,
    /// The collections.
    pub collections: Vec<CollectionInfo>,
    /// Dead letters recorded.
    pub dead_letters: u64,
    /// Batches committed since the daemon started.
    pub batches: u64,
    /// Events applied since the daemon started.
    pub events: u64,
    /// When the last batch committed, unix nanoseconds.
    pub last_batch_unix_nanos: Option<i64>,
    /// The error policy.
    pub on_error: String,
    /// Whether and why it was rebuilt at start.
    pub rebuilt: String,
    /// Times its source was resubscribed.
    pub reconnects: u64,
}

impl ProjectionStatus {
    fn from_wire(p: pb::ProjectionStatus) -> Result<ProjectionStatus, ClientError> {
        let state = p.state();
        let mut collections = Vec::with_capacity(p.collections.len());
        for c in p.collections {
            collections.push(CollectionInfo {
                name: c.name,
                kind: c.kind,
                key_shape: json(&c.key_shape)?,
                decl: json(&c.decl)?,
                indexes: c.indexes,
            });
        }
        Ok(ProjectionStatus {
            name: p.name,
            version: p.version,
            state,
            checkpoint: p.checkpoint,
            chron_head: p.chron_head,
            lag: p.lag,
            error: (!p.error.is_empty()).then_some(p.error),
            source: p.source,
            event_types: p.event_types,
            collections,
            dead_letters: p.dead_letters,
            batches: p.batches,
            events: p.events,
            last_batch_unix_nanos: p.last_batch_unix_nanos,
            on_error: p.on_error,
            rebuilt: p.rebuilt,
            reconnects: p.reconnects,
        })
    }
}

/// An event a projection skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    /// The event's global position.
    pub position: u64,
    /// Its aggregate, if any.
    pub aggregate: Option<Vec<u8>>,
    /// Its index in the aggregate.
    pub index: u64,
    /// Its tags.
    pub tags: Vec<Vec<u8>>,
    /// The handler's error.
    pub error: String,
    /// When it was skipped, unix nanoseconds.
    pub at_unix_nanos: i64,
}

#[derive(Clone)]
struct Auth(Option<MetadataValue<tonic::metadata::Ascii>>);

impl Interceptor for Auth {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(t) = &self.0 {
            req.metadata_mut().insert("authorization", t.clone());
        }
        Ok(req)
    }
}

fn auth(token: Option<&str>) -> Result<Auth, ClientError> {
    match token {
        None => Ok(Auth(None)),
        Some(t) => format!("Bearer {t}")
            .parse()
            .map(|v| Auth(Some(v)))
            .map_err(|_| ClientError::InvalidArgument("the token is not ASCII".into())),
    }
}

/// A connection to one facetd. Cheap to clone.
#[derive(Clone)]
pub struct Client {
    channel: Channel,
    addr: String,
    config: ClientConfig,
    token: Auth,
    admin_token: Auth,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").field("addr", &self.addr).finish()
    }
}

type Q = QueryClient<InterceptedService<Channel, Auth>>;
type A = AdminClient<InterceptedService<Channel, Auth>>;

impl Client {
    /// Connects to `addr` (`http://host:port` or `https://host:port`).
    pub async fn connect(
        addr: impl Into<String>,
        config: ClientConfig,
    ) -> Result<Client, ClientError> {
        let addr = addr.into();
        let mut endpoint = Endpoint::from_shared(addr.clone())
            .map_err(|e| ClientError::InvalidArgument(format!("{addr}: {e}")))?
            .connect_timeout(config.connect_timeout)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .tcp_keepalive(Some(Duration::from_secs(30)));
        if let Some(t) = config.request_timeout {
            endpoint = endpoint.timeout(t);
        }
        if config.tls.is_some() && !addr.starts_with("https://") {
            return Err(ClientError::InvalidArgument(format!(
                "{addr} is not an https:// address, but TLS was configured"
            )));
        }
        if addr.starts_with("https://") {
            let tls = config.tls.clone().unwrap_or_default();
            let mut cfg = ClientTlsConfig::new();
            match &tls.ca_pem {
                Some(ca) => cfg = cfg.ca_certificate(Certificate::from_pem(ca)),
                None => cfg = cfg.with_native_roots(),
            }
            if let Some(id) = &tls.identity {
                cfg = cfg.identity(tonic::transport::Identity::from_pem(
                    &id.cert_pem,
                    &id.key_pem,
                ));
            }
            if let Some(d) = &tls.domain {
                cfg = cfg.domain_name(d.clone());
            }
            endpoint = endpoint.tls_config(cfg)?;
        }
        let channel = endpoint.connect().await?;
        let token = auth(config.token.as_deref())?;
        let admin_token = auth(config.admin_token.as_deref().or(config.token.as_deref()))?;
        Ok(Client {
            channel,
            addr,
            config,
            token,
            admin_token,
        })
    }

    /// The address.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    fn query(&self) -> Q {
        QueryClient::with_interceptor(self.channel.clone(), self.token.clone())
            .max_decoding_message_size(self.config.max_message_bytes)
            .max_encoding_message_size(self.config.max_message_bytes)
    }

    fn admin(&self) -> A {
        AdminClient::with_interceptor(self.channel.clone(), self.token.clone())
            .max_decoding_message_size(self.config.max_message_bytes)
    }

    fn admin_privileged(&self) -> A {
        AdminClient::with_interceptor(self.channel.clone(), self.admin_token.clone())
            .max_decoding_message_size(self.config.max_message_bytes)
    }

    /// One entry by its whole key; `None` for a register.
    pub async fn get(
        &self,
        projection: &str,
        collection: &str,
        key: Option<&Json>,
        at: ReadAt,
    ) -> Result<(Option<Json>, Seen), ClientError> {
        let r = self
            .query()
            .get(pb::GetRequest {
                projection: projection.into(),
                collection: collection.into(),
                key: text(key),
                at: at.wire(),
            })
            .await?
            .into_inner();
        let value = if r.found { Some(json(&r.value)?) } else { None };
        Ok((value, Seen::from_wire(r.seen)))
    }

    /// Entries in key order.
    pub async fn scan(
        &self,
        projection: &str,
        collection: &str,
        scan: Scan,
    ) -> Result<Page, ClientError> {
        let r = self
            .query()
            .scan(pb::ScanRequest {
                projection: projection.into(),
                collection: collection.into(),
                prefix: text(scan.prefix.as_ref()),
                from: text(scan.from.as_ref()),
                to: text(scan.to.as_ref()),
                after: scan.after.unwrap_or_default(),
                limit: scan.limit,
                reverse: scan.reverse,
                at: scan.at.wire(),
            })
            .await?
            .into_inner();
        page(r)
    }

    /// A table's rows through a secondary index; keys are the index's.
    pub async fn lookup(
        &self,
        projection: &str,
        table: &str,
        index: &str,
        scan: Scan,
    ) -> Result<Page, ClientError> {
        let r = self
            .query()
            .lookup(pb::LookupRequest {
                projection: projection.into(),
                table: table.into(),
                index: index.into(),
                prefix: text(scan.prefix.as_ref()),
                from: text(scan.from.as_ref()),
                to: text(scan.to.as_ref()),
                after: scan.after.unwrap_or_default(),
                limit: scan.limit,
                reverse: scan.reverse,
                at: scan.at.wire(),
            })
            .await?
            .into_inner();
        page(r)
    }

    /// A sorted set's partition in score order; `reverse` for the top.
    /// Keys are members, values scores.
    #[allow(clippy::too_many_arguments)]
    pub async fn range(
        &self,
        projection: &str,
        collection: &str,
        partition: Option<&Json>,
        min: Option<&Json>,
        max: Option<&Json>,
        after: Option<Vec<u8>>,
        limit: u32,
        reverse: bool,
        at: ReadAt,
    ) -> Result<Page, ClientError> {
        let r = self
            .query()
            .range(pb::RangeRequest {
                projection: projection.into(),
                collection: collection.into(),
                partition: text(partition),
                min: text(min),
                max: text(max),
                after: after.unwrap_or_default(),
                limit,
                reverse,
                at: at.wire(),
            })
            .await?
            .into_inner();
        page(r)
    }

    /// The `n` highest-scored members of a partition.
    pub async fn top(
        &self,
        projection: &str,
        collection: &str,
        partition: Option<&Json>,
        n: u32,
    ) -> Result<Page, ClientError> {
        self.range(
            projection,
            collection,
            partition,
            None,
            None,
            None,
            n,
            true,
            ReadAt::default(),
        )
        .await
    }

    /// Buckets of one key and width in time order; keys are window
    /// starts, values `{start, count, sum, min, max}`.
    #[allow(clippy::too_many_arguments)]
    pub async fn buckets(
        &self,
        projection: &str,
        collection: &str,
        key: Option<&Json>,
        width: &str,
        from: Option<i64>,
        to: Option<i64>,
        after: Option<Vec<u8>>,
        limit: u32,
        reverse: bool,
        at: ReadAt,
    ) -> Result<Page, ClientError> {
        let r = self
            .query()
            .buckets(pb::BucketsRequest {
                projection: projection.into(),
                collection: collection.into(),
                key: text(key),
                width: width.into(),
                from,
                to,
                after: after.unwrap_or_default(),
                limit,
                reverse,
                at: at.wire(),
            })
            .await?
            .into_inner();
        page(r)
    }

    /// Every batch's changes to a collection from now on, narrowed to a
    /// key or a prefix. Ends with [`ClientError::WatchLagged`] when the
    /// daemon could not keep the watcher up.
    pub async fn watch(
        &self,
        projection: &str,
        collection: &str,
        key: Option<&Json>,
        prefix: Option<&Json>,
    ) -> Result<ChangeStream, ClientError> {
        let stream = self
            .query()
            .watch(pb::WatchRequest {
                projection: projection.into(),
                collection: collection.into(),
                key: text(key),
                prefix: text(prefix),
            })
            .await?
            .into_inner();
        Ok(Box::pin(stream.map(|item| {
            let c = item?;
            let mut keys = Vec::with_capacity(c.keys.len());
            for k in &c.keys {
                keys.push(json(k)?);
            }
            Ok(Change {
                checkpoint: c.checkpoint,
                collection: c.collection,
                keys,
                all: c.all,
            })
        })))
    }

    /// The daemon's status.
    pub async fn status(&self) -> Result<DaemonStatus, ClientError> {
        let s = self
            .admin()
            .status(pb::StatusRequest {})
            .await?
            .into_inner();
        Ok(DaemonStatus {
            version: s.version,
            uptime_secs: s.uptime_secs,
            store_path: s.store_path,
            chron_db_id: s.chron_db_id,
            chron_head: s.chron_head,
            fatal: (!s.fatal.is_empty()).then_some(s.fatal),
            counts: [
                s.starting,
                s.catching_up,
                s.live,
                s.rebuilding,
                s.failed,
                s.paused,
            ],
            sources: s
                .sources
                .into_iter()
                .map(|x| SourceStatus {
                    source: x.source,
                    connected: x.connected,
                    reconnects: x.reconnects,
                    error: (!x.error.is_empty()).then_some(x.error),
                    buffered: x.buffered,
                })
                .collect(),
        })
    }

    /// Every projection, registered or orphaned.
    pub async fn projections(&self) -> Result<Vec<ProjectionStatus>, ClientError> {
        let r = self
            .admin()
            .list_projections(pb::ListProjectionsRequest {})
            .await?
            .into_inner();
        r.projections
            .into_iter()
            .map(ProjectionStatus::from_wire)
            .collect()
    }

    /// One projection.
    pub async fn projection(&self, name: &str) -> Result<ProjectionStatus, ClientError> {
        let r = self
            .admin()
            .get_projection(pb::ProjectionRequest { name: name.into() })
            .await?
            .into_inner();
        ProjectionStatus::from_wire(r)
    }

    /// A projection's dead letters after `after`.
    pub async fn dead_letters(
        &self,
        name: &str,
        after: u64,
        limit: u32,
    ) -> Result<(Vec<DeadLetter>, u64), ClientError> {
        let r = self
            .admin()
            .dead_letters(pb::DeadLettersRequest {
                name: name.into(),
                after,
                limit,
            })
            .await?
            .into_inner();
        Ok((
            r.dead_letters
                .into_iter()
                .map(|d| DeadLetter {
                    position: d.position,
                    aggregate: d.aggregate,
                    index: d.index,
                    tags: d.tags,
                    error: d.error,
                    at_unix_nanos: d.at_unix_nanos,
                })
                .collect(),
            r.next,
        ))
    }

    /// Drops the projection's collections and starts it over. An admin
    /// call.
    pub async fn rebuild(&self, name: &str) -> Result<ProjectionStatus, ClientError> {
        let r = self
            .admin_privileged()
            .rebuild(pb::ProjectionRequest { name: name.into() })
            .await?
            .into_inner();
        ProjectionStatus::from_wire(r)
    }

    /// Pauses the projection. An admin call.
    pub async fn pause(&self, name: &str) -> Result<ProjectionStatus, ClientError> {
        let r = self
            .admin_privileged()
            .pause(pb::ProjectionRequest { name: name.into() })
            .await?
            .into_inner();
        ProjectionStatus::from_wire(r)
    }

    /// Resumes a paused or failed projection. An admin call.
    pub async fn resume(&self, name: &str) -> Result<ProjectionStatus, ClientError> {
        let r = self
            .admin_privileged()
            .resume(pb::ProjectionRequest { name: name.into() })
            .await?
            .into_inner();
        ProjectionStatus::from_wire(r)
    }

    /// Deletes an orphaned projection's file. An admin call.
    pub async fn drop_projection(&self, name: &str) -> Result<bool, ClientError> {
        let r = self
            .admin_privileged()
            .drop_projection(pb::ProjectionRequest { name: name.into() })
            .await?
            .into_inner();
        Ok(r.existed)
    }

    /// Whether the daemon reports `service` as serving (`""` for the
    /// daemon, `facet.v1.Query` for reads, `facet.v1.Query.live` once
    /// every projection is caught up).
    pub async fn health(&self, service: &str) -> Result<bool, ClientError> {
        let mut h = tonic_health::pb::health_client::HealthClient::new(self.channel.clone());
        let r = h
            .check(tonic_health::pb::HealthCheckRequest {
                service: service.into(),
            })
            .await?
            .into_inner();
        Ok(r.status() == tonic_health::pb::health_check_response::ServingStatus::Serving)
    }
}

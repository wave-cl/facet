//! The HTTP/JSON gateway: the same calls as the gRPC services for clients
//! without gRPC, on `--http-listen`, with the same tokens and TLS.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, MatchedPath, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use facet_proto::v1 as pb;
use facet_proto::v1::admin_server::Admin;
use facet_proto::v1::query_server::Query as QueryTrait;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tower::ServiceExt;

use crate::admin_svc::AdminService;
use crate::auth::BearerAuth;
use crate::http_json::{
    ApiError, change_json, dead_letters_json, decode_base64, error_json, get_json, projection_json,
    scan_json, status_json,
};
use crate::metrics::Metrics;
use crate::query_svc::QueryService;

/// Every route the gateway serves; `openapi.json` must list exactly these
/// (a unit test checks both directions).
#[cfg_attr(not(test), allow(dead_code))]
pub const ROUTES: &[(&str, &str)] = &[
    ("GET", "/v1/status"),
    ("GET", "/v1/projections"),
    ("GET", "/v1/projections/{projection}"),
    ("DELETE", "/v1/projections/{projection}"),
    ("POST", "/v1/projections/{projection}/rebuild"),
    ("POST", "/v1/projections/{projection}/pause"),
    ("POST", "/v1/projections/{projection}/resume"),
    ("GET", "/v1/projections/{projection}/dead-letters"),
    ("GET", "/v1/projections/{projection}/{collection}/get"),
    ("GET", "/v1/projections/{projection}/{collection}/scan"),
    ("GET", "/v1/projections/{projection}/{collection}/lookup"),
    ("GET", "/v1/projections/{projection}/{collection}/range"),
    ("GET", "/v1/projections/{projection}/{collection}/top"),
    ("GET", "/v1/projections/{projection}/{collection}/buckets"),
    ("GET", "/v1/projections/{projection}/{collection}/watch"),
    ("GET", "/openapi.json"),
    ("GET", "/"),
];

/// The OpenAPI document, served at `/openapi.json`.
pub const OPENAPI: &str = include_str!("../openapi.json");

#[derive(Clone)]
struct HttpState {
    query: Arc<QueryService>,
    admin: Arc<AdminService>,
    metrics: Arc<Metrics>,
    auth: BearerAuth,
}

/// The connection's peer address, for the log line.
#[derive(Clone, Copy)]
struct Peer(SocketAddr);

type Result<T> = std::result::Result<T, ApiError>;

/// Builds the router: `/v1/...` behind the bearer token, `/openapi.json`
/// and `/` open.
pub fn router(
    query: Arc<QueryService>,
    admin: Arc<AdminService>,
    metrics: Arc<Metrics>,
    auth: BearerAuth,
    request_timeout: Option<Duration>,
) -> Router {
    let state = HttpState {
        query,
        admin,
        metrics,
        auth,
    };
    let mut v1 = Router::new()
        .route("/status", get(status))
        .route("/projections", get(list_projections))
        .route(
            "/projections/{projection}",
            get(get_projection).delete(drop_projection),
        )
        .route("/projections/{projection}/rebuild", post(rebuild))
        .route("/projections/{projection}/pause", post(pause))
        .route("/projections/{projection}/resume", post(resume))
        .route("/projections/{projection}/dead-letters", get(dead_letters))
        .route("/projections/{projection}/{collection}/get", get(get_entry))
        .route("/projections/{projection}/{collection}/scan", get(scan))
        .route("/projections/{projection}/{collection}/lookup", get(lookup))
        .route("/projections/{projection}/{collection}/range", get(range))
        .route("/projections/{projection}/{collection}/top", get(top))
        .route(
            "/projections/{projection}/{collection}/buckets",
            get(buckets),
        )
        .route("/projections/{projection}/{collection}/watch", get(watch))
        .layer(middleware::from_fn_with_state(state.clone(), require_token));
    if let Some(t) = request_timeout {
        v1 = v1.layer(tower_http::timeout::TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            t,
        ));
    }
    Router::new()
        .nest("/v1", v1)
        .route("/openapi.json", get(openapi))
        .route("/", get(root))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(middleware::from_fn_with_state(state.clone(), log_and_count))
        .with_state(state)
}

async fn log_and_count(State(st): State<HttpState>, req: Request, next: Next) -> Response {
    let start = Instant::now();
    let method = req.method().clone();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());
    let peer = req
        .extensions()
        .get::<Peer>()
        .map(|p| p.0.to_string())
        .unwrap_or_default();
    let res = next.run(req).await;
    let status = res.status();
    st.metrics
        .http_requests
        .with_label_values(&[&route, status.as_str()])
        .inc();
    tracing::info!(
        target: "facetd::http",
        %method,
        route = %route,
        status = status.as_u16(),
        duration_ms = start.elapsed().as_millis() as u64,
        %peer,
        "http"
    );
    res
}

async fn require_token(State(st): State<HttpState>, req: Request, next: Next) -> Response {
    if let Err(s) = st.auth.check(req.headers()) {
        return ApiError(s).into_response();
    }
    next.run(req).await
}

async fn not_found() -> ApiError {
    ApiError(Status::not_found("no such route; see /openapi.json"))
}

async fn method_not_allowed() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({ "error": "method not allowed for this route; see /openapi.json", "code": "Unimplemented" })),
    )
        .into_response()
}

async fn openapi() -> Response {
    ([(header::CONTENT_TYPE, "application/json")], OPENAPI).into_response()
}

async fn root() -> &'static str {
    "facetd HTTP gateway: see /openapi.json\n"
}

fn bad(e: impl std::fmt::Display) -> ApiError {
    ApiError(Status::invalid_argument(e.to_string()))
}

/// A gRPC request carrying the HTTP call's bearer token, so the admin
/// gate sees it.
fn grpc<T>(headers: &HeaderMap, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    if let Some(v) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        && let Ok(mv) = v.parse()
    {
        req.metadata_mut().insert("authorization", mv);
    }
    req
}

#[derive(Deserialize, Default)]
struct ReadQuery {
    key: Option<String>,
    prefix: Option<String>,
    from: Option<String>,
    to: Option<String>,
    after: Option<String>,
    limit: Option<u32>,
    #[serde(default)]
    reverse: bool,
    at_least: Option<u64>,
    wait_ms: Option<u32>,
    index: Option<String>,
    partition: Option<String>,
    min: Option<String>,
    max: Option<String>,
    width: Option<String>,
}

impl ReadQuery {
    fn at(&self) -> Option<pb::ReadAt> {
        self.at_least.map(|p| pb::ReadAt {
            at_least: Some(p),
            wait_ms: self.wait_ms,
        })
    }

    fn after(&self) -> Result<Vec<u8>> {
        match &self.after {
            Some(a) if !a.is_empty() => Ok(decode_base64(a, "after").map_err(ApiError)?),
            _ => Ok(Vec::new()),
        }
    }
}

type Q = std::result::Result<Query<ReadQuery>, axum::extract::rejection::QueryRejection>;

async fn get_entry(
    State(st): State<HttpState>,
    Path((projection, collection)): Path<(String, String)>,
    q: Q,
) -> Result<Json<Value>> {
    let Query(q) = q.map_err(bad)?;
    let at = q.at();
    let r = st
        .query
        .get(tonic::Request::new(pb::GetRequest {
            projection,
            collection,
            key: q.key.unwrap_or_default(),
            at,
        }))
        .await?
        .into_inner();
    Ok(Json(get_json(&r)))
}

async fn scan(
    State(st): State<HttpState>,
    Path((projection, collection)): Path<(String, String)>,
    q: Q,
) -> Result<Json<Value>> {
    let Query(q) = q.map_err(bad)?;
    let at = q.at();
    let after = q.after()?;
    let r = st
        .query
        .scan(tonic::Request::new(pb::ScanRequest {
            projection,
            collection,
            prefix: q.prefix.unwrap_or_default(),
            from: q.from.unwrap_or_default(),
            to: q.to.unwrap_or_default(),
            after,
            limit: q.limit.unwrap_or(0),
            reverse: q.reverse,
            at,
        }))
        .await?
        .into_inner();
    Ok(Json(scan_json(&r)))
}

async fn lookup(
    State(st): State<HttpState>,
    Path((projection, table)): Path<(String, String)>,
    q: Q,
) -> Result<Json<Value>> {
    let Query(q) = q.map_err(bad)?;
    let at = q.at();
    let after = q.after()?;
    let index = q.index.ok_or_else(|| bad("lookup needs ?index=<name>"))?;
    let r = st
        .query
        .lookup(tonic::Request::new(pb::LookupRequest {
            projection,
            table,
            index,
            prefix: q.prefix.unwrap_or_default(),
            from: q.from.unwrap_or_default(),
            to: q.to.unwrap_or_default(),
            after,
            limit: q.limit.unwrap_or(0),
            reverse: q.reverse,
            at,
        }))
        .await?
        .into_inner();
    Ok(Json(scan_json(&r)))
}

async fn range_inner(
    st: &HttpState,
    projection: String,
    collection: String,
    q: ReadQuery,
    reverse: bool,
) -> Result<Json<Value>> {
    let at = q.at();
    let after = q.after()?;
    let r = st
        .query
        .range(tonic::Request::new(pb::RangeRequest {
            projection,
            collection,
            partition: q.partition.unwrap_or_default(),
            min: q.min.unwrap_or_default(),
            max: q.max.unwrap_or_default(),
            after,
            limit: q.limit.unwrap_or(0),
            reverse,
            at,
        }))
        .await?
        .into_inner();
    Ok(Json(scan_json(&r)))
}

async fn range(
    State(st): State<HttpState>,
    Path((projection, collection)): Path<(String, String)>,
    q: Q,
) -> Result<Json<Value>> {
    let Query(q) = q.map_err(bad)?;
    let reverse = q.reverse;
    range_inner(&st, projection, collection, q, reverse).await
}

async fn top(
    State(st): State<HttpState>,
    Path((projection, collection)): Path<(String, String)>,
    q: Q,
) -> Result<Json<Value>> {
    let Query(mut q) = q.map_err(bad)?;
    if q.limit.is_none() {
        q.limit = Some(10);
    }
    range_inner(&st, projection, collection, q, true).await
}

async fn buckets(
    State(st): State<HttpState>,
    Path((projection, collection)): Path<(String, String)>,
    q: Q,
) -> Result<Json<Value>> {
    let Query(q) = q.map_err(bad)?;
    let at = q.at();
    let after = q.after()?;
    let width = q
        .width
        .ok_or_else(|| bad("buckets need ?width=minute|hour|day|<nanos>"))?;
    let parse = |s: &Option<String>, what: &str| -> Result<Option<i64>> {
        match s {
            None => Ok(None),
            Some(v) => v
                .parse::<i64>()
                .map(Some)
                .map_err(|_| bad(format!("{what}: not a number"))),
        }
    };
    let r = st
        .query
        .buckets(tonic::Request::new(pb::BucketsRequest {
            projection,
            collection,
            key: q.key.clone().unwrap_or_default(),
            width,
            from: parse(&q.from, "from")?,
            to: parse(&q.to, "to")?,
            after,
            limit: q.limit.unwrap_or(0),
            reverse: q.reverse,
            at,
        }))
        .await?
        .into_inner();
    Ok(Json(scan_json(&r)))
}

async fn watch(
    State(st): State<HttpState>,
    Path((projection, collection)): Path<(String, String)>,
    q: Q,
) -> Result<Sse<impl tokio_stream::Stream<Item = std::result::Result<SseEvent, Infallible>>>> {
    let Query(q) = q.map_err(bad)?;
    let stream = st
        .query
        .watch(tonic::Request::new(pb::WatchRequest {
            projection,
            collection,
            key: q.key.unwrap_or_default(),
            prefix: q.prefix.unwrap_or_default(),
        }))
        .await?
        .into_inner();
    let stream = stream.map(|item| {
        Ok(match item {
            Ok(c) => SseEvent::default().data(change_json(&c).to_string()),
            Err(s) => SseEvent::default()
                .event("error")
                .data(error_json(&s).to_string()),
        })
    });
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

async fn status(State(st): State<HttpState>) -> Result<Json<Value>> {
    let r = st
        .admin
        .status(tonic::Request::new(pb::StatusRequest {}))
        .await?
        .into_inner();
    Ok(Json(status_json(&r)))
}

async fn list_projections(State(st): State<HttpState>) -> Result<Json<Value>> {
    let r = st
        .admin
        .list_projections(tonic::Request::new(pb::ListProjectionsRequest {}))
        .await?
        .into_inner();
    Ok(Json(
        json!({ "projections": r.projections.iter().map(projection_json).collect::<Vec<_>>() }),
    ))
}

async fn get_projection(
    State(st): State<HttpState>,
    Path(name): Path<String>,
) -> Result<Json<Value>> {
    let r = st
        .admin
        .get_projection(tonic::Request::new(pb::ProjectionRequest { name }))
        .await?
        .into_inner();
    Ok(Json(projection_json(&r)))
}

#[derive(Deserialize, Default)]
struct DeadQuery {
    after: Option<u64>,
    limit: Option<u32>,
}

async fn dead_letters(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    q: std::result::Result<Query<DeadQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<Value>> {
    let Query(q) = q.map_err(bad)?;
    let r = st
        .admin
        .dead_letters(tonic::Request::new(pb::DeadLettersRequest {
            name,
            after: q.after.unwrap_or(0),
            limit: q.limit.unwrap_or(0),
        }))
        .await?
        .into_inner();
    Ok(Json(dead_letters_json(&r)))
}

async fn rebuild(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let r = st
        .admin
        .rebuild(grpc(&headers, pb::ProjectionRequest { name }))
        .await?
        .into_inner();
    Ok(Json(projection_json(&r)))
}

async fn pause(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let r = st
        .admin
        .pause(grpc(&headers, pb::ProjectionRequest { name }))
        .await?
        .into_inner();
    Ok(Json(projection_json(&r)))
}

async fn resume(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let r = st
        .admin
        .resume(grpc(&headers, pb::ProjectionRequest { name }))
        .await?
        .into_inner();
    Ok(Json(projection_json(&r)))
}

async fn drop_projection(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let r = st
        .admin
        .drop_projection(grpc(&headers, pb::ProjectionRequest { name }))
        .await?
        .into_inner();
    Ok(Json(json!({ "existed": r.existed })))
}

/// A rustls server config from the daemon's TLS settings.
pub fn rustls_config(tls: &crate::Tls) -> anyhow::Result<Arc<tokio_rustls::rustls::ServerConfig>> {
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&tls.cert_pem)
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("TLS certificate: {e}"))?;
    let key =
        PrivateKeyDer::from_pem_slice(&tls.key_pem).map_err(|e| anyhow::anyhow!("TLS key: {e}"))?;
    let builder = ServerConfig::builder();
    let mut cfg = match &tls.client_ca_pem {
        Some(ca) => {
            let mut roots = RootCertStore::empty();
            for c in CertificateDer::pem_slice_iter(ca) {
                roots
                    .add(c.map_err(|e| anyhow::anyhow!("client CA: {e}"))?)
                    .map_err(|e| anyhow::anyhow!("client CA: {e}"))?;
            }
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| anyhow::anyhow!("client CA: {e}"))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)?
        }
        None => builder.with_no_client_auth().with_single_cert(certs, key)?,
    };
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
}

/// Accepts connections until `cancel` fires, each served on its own task
/// over HTTP/1 (with TLS when configured).
pub async fn serve(
    listener: TcpListener,
    router: Router,
    tls: Option<Arc<tokio_rustls::rustls::ServerConfig>>,
    cancel: CancellationToken,
) {
    let mut tasks = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            _ = cancel.cancelled() => break,
            a = listener.accept() => a,
        };
        let (stream, peer) = match accepted {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(error = %e, "http accept");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let router = router.clone();
        let tls = tls.clone();
        tasks.spawn(async move {
            let svc = hyper::service::service_fn(
                move |mut req: hyper::Request<hyper::body::Incoming>| {
                    req.extensions_mut().insert(Peer(peer));
                    let router = router.clone();
                    async move {
                        let req = req.map(Body::new);
                        router.oneshot(req).await
                    }
                },
            );
            match tls {
                Some(cfg) => {
                    let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
                    match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(stream))
                        .await
                    {
                        Ok(Ok(tls_stream)) => {
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(TokioIo::new(tls_stream), svc)
                                .await;
                        }
                        Ok(Err(e)) => tracing::debug!(%peer, error = %e, "http tls handshake"),
                        Err(_) => tracing::debug!(%peer, "http tls handshake timed out"),
                    }
                }
                None => {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                }
            }
        });
    }
    tasks.abort_all();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_json::http_status;

    /// The document lists exactly the routes the router serves.
    #[test]
    fn openapi_matches_the_route_table() {
        let doc: Value = serde_json::from_str(OPENAPI).expect("openapi.json parses");
        let paths = doc["paths"].as_object().expect("paths");
        for (method, path) in ROUTES {
            let ops = paths
                .get(*path)
                .unwrap_or_else(|| panic!("{path} missing from openapi.json"));
            assert!(
                ops.get(method.to_lowercase()).is_some(),
                "{method} {path} missing from openapi.json"
            );
        }
        for (path, ops) in paths {
            for method in ops.as_object().unwrap().keys() {
                let m = method.to_uppercase();
                assert!(
                    ROUTES.iter().any(|(rm, rp)| *rm == m && rp == path),
                    "{m} {path} is in openapi.json but not served"
                );
            }
        }
        assert_eq!(doc["info"]["version"], env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn statuses_map_to_http() {
        assert_eq!(http_status(&Status::not_found("x")), StatusCode::NOT_FOUND);
        assert_eq!(
            http_status(&Status::invalid_argument("x")),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            http_status(&Status::failed_precondition("x")),
            StatusCode::CONFLICT
        );
        assert_eq!(
            http_status(&Status::permission_denied("x")),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            http_status(&Status::unauthenticated("x")),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            http_status(&Status::unavailable("x")),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let mut s = Status::unavailable("late");
        crate::codec::insert(&mut s, facet_proto::CHECKPOINT_HEADER, 7);
        assert_eq!(http_status(&s), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(error_json(&s)["checkpoint"], 7);
    }
}

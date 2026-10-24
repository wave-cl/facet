//! JSON shapes and errors of the HTTP gateway.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use facet_proto::v1 as pb;
use serde_json::{Value, json};
use tonic::{Code, Status};

/// A gRPC status as an HTTP response: the code's HTTP equivalent and a
/// JSON body `{"error", "code", ...}` with the status metadata the
/// daemon sets (`checkpoint`, `chron_head`, `at_least`, `position`).
#[derive(Debug)]
pub struct ApiError(pub Status);

impl From<Status> for ApiError {
    fn from(s: Status) -> Self {
        ApiError(s)
    }
}

/// The HTTP status for a gRPC code.
pub fn http_status(s: &Status) -> StatusCode {
    match s.code() {
        Code::Ok => StatusCode::OK,
        Code::InvalidArgument | Code::OutOfRange => StatusCode::BAD_REQUEST,
        Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        Code::PermissionDenied => StatusCode::FORBIDDEN,
        Code::NotFound => StatusCode::NOT_FOUND,
        Code::AlreadyExists | Code::FailedPrecondition | Code::Aborted => StatusCode::CONFLICT,
        Code::ResourceExhausted => StatusCode::PAYLOAD_TOO_LARGE,
        Code::Unavailable if s.metadata().contains_key(facet_proto::CHECKPOINT_HEADER) => {
            StatusCode::GATEWAY_TIMEOUT
        }
        Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// The code's name, as go-grpc spells it.
pub fn code_name(c: Code) -> &'static str {
    crate::metrics::code_label(c as i32)
}

/// The error body.
pub fn error_json(s: &Status) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("error".into(), json!(s.message()));
    obj.insert("code".into(), json!(code_name(s.code())));
    for (key, name) in [
        (facet_proto::CHECKPOINT_HEADER, "checkpoint"),
        (facet_proto::CHRON_HEAD_HEADER, "chron_head"),
        ("facet-at-least", "at_least"),
        (facet_proto::POSITION_HEADER, "position"),
    ] {
        if let Some(v) = s
            .metadata()
            .get(key)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
        {
            obj.insert(name.into(), json!(v));
        }
    }
    Value::Object(obj)
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (http_status(&self.0), Json(error_json(&self.0))).into_response()
    }
}

fn b64(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// Decodes a base64 query parameter.
pub fn decode_base64(s: &str, what: &str) -> Result<Vec<u8>, Status> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| Status::invalid_argument(format!("{what}: not base64: {e}")))
}

fn text_json(s: &str) -> Value {
    if s.is_empty() {
        return Value::Null;
    }
    serde_json::from_str(s).unwrap_or(Value::String(s.to_owned()))
}

pub fn state_name(s: i32) -> &'static str {
    match pb::State::try_from(s).unwrap_or(pb::State::Unspecified) {
        pb::State::Unspecified => "unknown",
        pb::State::Starting => "starting",
        pb::State::CatchingUp => "catching_up",
        pb::State::Live => "live",
        pb::State::Rebuilding => "rebuilding",
        pb::State::Failed => "failed",
        pb::State::Paused => "paused",
        pb::State::Stopped => "stopped",
        pb::State::Orphaned => "orphaned",
    }
}

pub fn seen_json(s: &Option<pb::Seen>) -> Value {
    let s = (*s).unwrap_or_default();
    json!({ "checkpoint": s.checkpoint, "state": state_name(s.state), "chron_head": s.chron_head })
}

pub fn get_json(r: &pb::GetResponse) -> Value {
    json!({
        "found": r.found,
        "value": if r.found { text_json(&r.value) } else { Value::Null },
        "seen": seen_json(&r.seen),
    })
}

pub fn scan_json(r: &pb::ScanResponse) -> Value {
    json!({
        "entries": r.entries.iter().map(|e| json!({ "key": text_json(&e.key), "value": text_json(&e.value) })).collect::<Vec<_>>(),
        "next": if r.next.is_empty() { Value::Null } else { json!(b64(&r.next)) },
        "seen": seen_json(&r.seen),
    })
}

pub fn change_json(c: &pb::Change) -> Value {
    json!({
        "checkpoint": c.checkpoint,
        "collection": c.collection,
        "keys": c.keys.iter().map(|k| text_json(k)).collect::<Vec<_>>(),
        "all": c.all,
    })
}

pub fn projection_json(p: &pb::ProjectionStatus) -> Value {
    json!({
        "name": p.name,
        "version": p.version,
        "state": state_name(p.state),
        "checkpoint": p.checkpoint,
        "chron_head": p.chron_head,
        "lag": p.lag,
        "error": if p.error.is_empty() { Value::Null } else { json!(p.error) },
        "source": p.source,
        "event_types": p.event_types,
        "collections": p.collections.iter().map(|c| json!({
            "name": c.name,
            "kind": c.kind,
            "key_shape": text_json(&c.key_shape),
            "decl": text_json(&c.decl),
            "indexes": c.indexes,
        })).collect::<Vec<_>>(),
        "dead_letters": p.dead_letters,
        "batches": p.batches,
        "events": p.events,
        "last_batch_unix_nanos": p.last_batch_unix_nanos,
        "on_error": p.on_error,
        "rebuilt": p.rebuilt,
        "reconnects": p.reconnects,
    })
}

pub fn status_json(s: &pb::StatusResponse) -> Value {
    json!({
        "version": s.version,
        "uptime_secs": s.uptime_secs,
        "store_path": s.store_path,
        "chron_db_id": s.chron_db_id,
        "chron_head": s.chron_head,
        "fatal": if s.fatal.is_empty() { Value::Null } else { json!(s.fatal) },
        "projections": {
            "starting": s.starting, "catching_up": s.catching_up, "live": s.live,
            "rebuilding": s.rebuilding, "failed": s.failed, "paused": s.paused,
        },
        "sources": s.sources.iter().map(|x| json!({
            "source": x.source, "connected": x.connected, "reconnects": x.reconnects,
            "error": if x.error.is_empty() { Value::Null } else { json!(x.error) },
            "buffered": x.buffered,
        })).collect::<Vec<_>>(),
    })
}

pub fn dead_letters_json(r: &pb::DeadLettersResponse) -> Value {
    json!({
        "dead_letters": r.dead_letters.iter().map(|d| {
            let (text, binary): (Vec<&Vec<u8>>, Vec<&Vec<u8>>) = d.tags.iter().partition(|t| std::str::from_utf8(t).is_ok());
            let mut o = json!({
                "position": d.position,
                "aggregate": d.aggregate.as_ref().and_then(|a| uuid::Uuid::from_slice(a).ok()).map(|u| u.to_string()),
                "index": d.index,
                "tags": text.iter().map(|t| String::from_utf8_lossy(t).into_owned()).collect::<Vec<_>>(),
                "error": d.error,
                "at_unix_nanos": d.at_unix_nanos,
            });
            if !binary.is_empty() {
                o["tags_base64"] = json!(binary.iter().map(|t| b64(t)).collect::<Vec<_>>());
            }
            o
        }).collect::<Vec<_>>(),
        "next": r.next,
    })
}

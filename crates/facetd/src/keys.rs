//! Keys and values as JSON text, the way the wire carries them.

use base64::Engine;
use facet::keyenc::{KeyKind, KeyPart, KeyTuple};
use facet::{Decimal, KeyShape, Timestamp, Uuid, Value};
use serde_json::Value as Json;
use tonic::Status;

/// Parses one key part of `kind` from JSON.
pub fn part(kind: KeyKind, nullable: bool, j: &Json, what: &str) -> Result<KeyPart, Status> {
    let bad =
        |expected: &str| Status::invalid_argument(format!("{what}: expected {expected}, got {j}"));
    if j.is_null() {
        if nullable {
            return Ok(KeyPart::Null(kind));
        }
        return Err(bad(kind.name()));
    }
    Ok(match kind {
        KeyKind::Bool => KeyPart::Bool(j.as_bool().ok_or_else(|| bad("a boolean"))?),
        KeyKind::Int => KeyPart::Int(match j {
            Json::Number(n) => n.as_i64().ok_or_else(|| bad("an integer"))?,
            Json::String(s) => s.parse().map_err(|_| bad("an integer"))?,
            _ => return Err(bad("an integer")),
        }),
        KeyKind::Uint => KeyPart::Uint(match j {
            Json::Number(n) => n.as_u64().ok_or_else(|| bad("an unsigned integer"))?,
            Json::String(s) => s.parse().map_err(|_| bad("an unsigned integer"))?,
            _ => return Err(bad("an unsigned integer")),
        }),
        KeyKind::Decimal => KeyPart::Decimal(match j {
            Json::Number(n) => n
                .to_string()
                .parse::<Decimal>()
                .map_err(|_| bad("a decimal"))?,
            Json::String(s) => s.parse::<Decimal>().map_err(|_| bad("a decimal"))?,
            _ => return Err(bad("a decimal")),
        }),
        KeyKind::Str => KeyPart::Str(j.as_str().ok_or_else(|| bad("a string"))?.to_owned()),
        KeyKind::Bytes => KeyPart::Bytes(match j {
            Json::String(s) => base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(|_| bad("base64 bytes"))?
                .into(),
            Json::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for i in items {
                    out.push(
                        u8::try_from(i.as_u64().ok_or_else(|| bad("bytes"))?)
                            .map_err(|_| bad("bytes"))?,
                    );
                }
                out.into()
            }
            _ => return Err(bad("base64 bytes")),
        }),
        KeyKind::Uuid => KeyPart::Uuid(
            Uuid::parse_str(j.as_str().ok_or_else(|| bad("a uuid"))?).map_err(|_| bad("a uuid"))?,
        ),
        KeyKind::Timestamp => KeyPart::Timestamp(match j {
            Json::Number(n) => Timestamp(n.as_i64().ok_or_else(|| bad("a timestamp"))?),
            Json::String(s) => {
                Timestamp::parse_rfc3339(s).map_err(|_| bad("an RFC 3339 timestamp"))?
            }
            _ => return Err(bad("a timestamp")),
        }),
    })
}

/// Parses a key against the leading parts of a shape: a JSON array, or
/// a bare scalar for one part; empty text is the empty tuple. With
/// `whole`, every part must be given.
pub fn parse(shape: &KeyShape, text: &str, whole: bool, what: &str) -> Result<KeyTuple, Status> {
    let parts: Vec<Json> = if text.trim().is_empty() {
        Vec::new()
    } else {
        let j: Json = serde_json::from_str(text)
            .map_err(|e| Status::invalid_argument(format!("{what}: not JSON: {e}")))?;
        match j {
            Json::Array(a) => a,
            other => vec![other],
        }
    };
    if parts.len() > shape.kinds.len() {
        return Err(Status::invalid_argument(format!(
            "{what}: {} parts given, the key has {}",
            parts.len(),
            shape.kinds.len()
        )));
    }
    if whole && parts.len() != shape.kinds.len() {
        return Err(Status::invalid_argument(format!(
            "{what}: a whole key has {} parts ({}), {} given",
            shape.kinds.len(),
            shape.names.join(", "),
            parts.len()
        )));
    }
    let mut out = Vec::with_capacity(parts.len());
    for (i, j) in parts.iter().enumerate() {
        out.push(part(
            shape.kinds[i],
            shape.nullable[i],
            j,
            &format!("{what} part {} ({})", i, shape.names[i]),
        )?);
    }
    Ok(KeyTuple(out))
}

/// Parses an optional key (empty text is `None`).
pub fn parse_opt(shape: &KeyShape, text: &str, what: &str) -> Result<Option<KeyTuple>, Status> {
    if text.trim().is_empty() {
        return Ok(None);
    }
    parse(shape, text, false, what).map(Some)
}

/// A key as JSON text: an array of its parts.
pub fn key_json(t: &KeyTuple) -> String {
    Json::Array(t.0.iter().map(|p| p.to_value().to_json()).collect()).to_string()
}

/// A value as JSON text.
pub fn value_json(v: &Value) -> String {
    v.to_json().to_string()
}

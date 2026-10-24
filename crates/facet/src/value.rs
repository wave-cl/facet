//! The value model: what a register, map value, list element or table
//! row is made of, with serde bridges to the caller's own types and a
//! JSON rendering for the wire.

use std::collections::BTreeMap;

use bytes::Bytes;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::{Error, Result};

mod de;
mod ser;

/// Unix nanoseconds. A key kind and a value type of its own, so that a
/// column declared `timestamp` is kept apart from a plain integer.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// RFC 3339 in UTC with nanoseconds.
    pub fn to_rfc3339(self) -> String {
        match jiff::Timestamp::from_nanosecond(i128::from(self.0)) {
            Ok(t) => t.to_string(),
            Err(_) => self.0.to_string(),
        }
    }

    /// Parses RFC 3339 (any offset) to unix nanoseconds.
    pub fn parse_rfc3339(s: &str) -> Result<Timestamp> {
        let t: jiff::Timestamp = s
            .parse()
            .map_err(|e| Error::Value(format!("timestamp {s:?}: {e}")))?;
        i64::try_from(t.as_nanosecond())
            .map(Timestamp)
            .map_err(|_| Error::Value(format!("timestamp {s:?} is out of range")))
    }
}

/// A stored value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// No value.
    Null,
    /// A boolean.
    Bool(bool),
    /// A signed integer.
    Int(i64),
    /// An unsigned integer.
    Uint(u64),
    /// A float. Never a key or a score.
    Float(f64),
    /// A decimal.
    Decimal(Decimal),
    /// A string.
    Str(String),
    /// Bytes.
    Bytes(Bytes),
    /// A UUID.
    Uuid(Uuid),
    /// Unix nanoseconds.
    Timestamp(Timestamp),
    /// A list.
    List(Vec<Value>),
    /// A map with string keys, sorted.
    Map(BTreeMap<String, Value>),
}

impl Value {
    /// The name of the variant, for messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::Uint(_) => "uint",
            Value::Float(_) => "float",
            Value::Decimal(_) => "decimal",
            Value::Str(_) => "string",
            Value::Bytes(_) => "bytes",
            Value::Uuid(_) => "uuid",
            Value::Timestamp(_) => "timestamp",
            Value::List(_) => "list",
            Value::Map(_) => "map",
        }
    }

    /// The JSON rendering: decimals as strings, UUIDs hyphenated,
    /// timestamps as RFC 3339, bytes as base64, integers as numbers.
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::Value as J;
        match self {
            Value::Null => J::Null,
            Value::Bool(b) => J::Bool(*b),
            Value::Int(i) => J::from(*i),
            Value::Uint(u) => J::from(*u),
            Value::Float(f) => serde_json::Number::from_f64(*f).map_or(J::Null, J::Number),
            Value::Decimal(d) => J::String(d.to_string()),
            Value::Str(s) => J::String(s.clone()),
            Value::Bytes(b) => J::String(base64_encode(b)),
            Value::Uuid(u) => J::String(u.hyphenated().to_string()),
            Value::Timestamp(t) => J::String(t.to_rfc3339()),
            Value::List(l) => J::Array(l.iter().map(Value::to_json).collect()),
            Value::Map(m) => J::Object(m.iter().map(|(k, v)| (k.clone(), v.to_json())).collect()),
        }
    }

    /// The reverse of [`Value::to_json`] as far as JSON allows: numbers
    /// become `Int`, `Uint` or `Float`, strings stay strings. Columns
    /// declared with a type turn strings back into decimals, UUIDs and
    /// timestamps when validated.
    pub fn from_json(j: &serde_json::Value) -> Value {
        use serde_json::Value as J;
        match j {
            J::Null => Value::Null,
            J::Bool(b) => Value::Bool(*b),
            J::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(u) = n.as_u64() {
                    Value::Uint(u)
                } else {
                    Value::Float(n.as_f64().unwrap_or(f64::NAN))
                }
            }
            J::String(s) => Value::Str(s.clone()),
            J::Array(a) => Value::List(a.iter().map(Value::from_json).collect()),
            J::Object(o) => Value::Map(
                o.iter()
                    .map(|(k, v)| (k.clone(), Value::from_json(v)))
                    .collect(),
            ),
        }
    }
}

/// Converts any `Serialize` into a [`Value`].
pub fn to_value<T: serde::Serialize + ?Sized>(t: &T) -> Result<Value> {
    t.serialize(ser::ValueSerializer)
}

/// Converts a [`Value`] into any `Deserialize`.
pub fn from_value<T: serde::de::DeserializeOwned>(v: Value) -> Result<T> {
    T::deserialize(v)
}

pub(crate) fn base64_encode(b: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for chunk in b.chunks(3) {
        let n =
            chunk.iter().fold(0u32, |acc, &x| (acc << 8) | u32::from(x)) << (8 * (3 - chunk.len()));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

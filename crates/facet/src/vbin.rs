//! The stored encoding of a [`Value`]: a tag byte, then the payload.
//!
//! | tag | value | payload |
//! |---|---|---|
//! | `00` | null | |
//! | `01`, `02` | false, true | |
//! | `03` | int | zigzag varint |
//! | `04` | uint | varint |
//! | `05` | float | 8 big-endian |
//! | `06` | decimal | the 16 bytes of `Decimal::serialize` |
//! | `07` | string | varint length, UTF-8 |
//! | `08` | bytes | varint length, bytes |
//! | `09` | uuid | 16 bytes |
//! | `0a` | timestamp | 8 big-endian i64 |
//! | `0b` | list | varint count, items |
//! | `0c` | map | varint count, (string key, value) pairs |
//!
//! There is no version byte per value: the file format governs, and a
//! format change is a rebuild.

use std::collections::BTreeMap;

use bytes::Bytes;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::value::{Timestamp, Value};

/// Encodes a value.
pub fn encode(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write(v, &mut out);
    out
}

fn varint(mut n: u64, out: &mut Vec<u8>) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn write(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Null => out.push(0x00),
        Value::Bool(false) => out.push(0x01),
        Value::Bool(true) => out.push(0x02),
        Value::Int(i) => {
            out.push(0x03);
            varint(((i << 1) ^ (i >> 63)) as u64, out);
        }
        Value::Uint(u) => {
            out.push(0x04);
            varint(*u, out);
        }
        Value::Float(f) => {
            out.push(0x05);
            out.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Value::Decimal(d) => {
            out.push(0x06);
            out.extend_from_slice(&d.serialize());
        }
        Value::Str(s) => {
            out.push(0x07);
            varint(s.len() as u64, out);
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(b) => {
            out.push(0x08);
            varint(b.len() as u64, out);
            out.extend_from_slice(b);
        }
        Value::Uuid(u) => {
            out.push(0x09);
            out.extend_from_slice(u.as_bytes());
        }
        Value::Timestamp(t) => {
            out.push(0x0a);
            out.extend_from_slice(&t.0.to_be_bytes());
        }
        Value::List(l) => {
            out.push(0x0b);
            varint(l.len() as u64, out);
            for item in l {
                write(item, out);
            }
        }
        Value::Map(m) => {
            out.push(0x0c);
            varint(m.len() as u64, out);
            for (k, item) in m {
                varint(k.len() as u64, out);
                out.extend_from_slice(k.as_bytes());
                write(item, out);
            }
        }
    }
}

/// Decodes a value; trailing bytes are an error.
pub fn decode(bytes: &[u8]) -> Result<Value> {
    let mut r = Reader { bytes, at: 0 };
    let v = r.value(0)?;
    if r.at != bytes.len() {
        return Err(Error::Value(format!(
            "{} trailing bytes after the value",
            bytes.len() - r.at
        )));
    }
    Ok(v)
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

const MAX_DEPTH: usize = 64;

impl Reader<'_> {
    fn short() -> Error {
        Error::Value("truncated value".into())
    }

    fn take(&mut self, n: usize) -> Result<&[u8]> {
        let s = self
            .bytes
            .get(self.at..self.at + n)
            .ok_or_else(Self::short)?;
        self.at += n;
        Ok(s)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn varint(&mut self) -> Result<u64> {
        let mut n: u64 = 0;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            n |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(n);
            }
        }
        Err(Error::Value("varint is too long".into()))
    }

    fn len(&mut self) -> Result<usize> {
        let n = self.varint()?;
        usize::try_from(n)
            .ok()
            .filter(|&n| n <= self.bytes.len() - self.at)
            .ok_or_else(|| Error::Value(format!("length {n} is past the end")))
    }

    fn string(&mut self) -> Result<String> {
        let n = self.len()?;
        String::from_utf8(self.take(n)?.to_vec())
            .map_err(|e| Error::Value(format!("string is not UTF-8: {e}")))
    }

    fn value(&mut self, depth: usize) -> Result<Value> {
        if depth > MAX_DEPTH {
            return Err(Error::Value("value nests deeper than 64".into()));
        }
        Ok(match self.byte()? {
            0x00 => Value::Null,
            0x01 => Value::Bool(false),
            0x02 => Value::Bool(true),
            0x03 => {
                let z = self.varint()?;
                Value::Int(((z >> 1) as i64) ^ -((z & 1) as i64))
            }
            0x04 => Value::Uint(self.varint()?),
            0x05 => Value::Float(f64::from_bits(u64::from_be_bytes(
                self.take(8)?.try_into().unwrap(),
            ))),
            0x06 => {
                let raw: [u8; 16] = self.take(16)?.try_into().unwrap();
                Value::Decimal(Decimal::deserialize(raw))
            }
            0x07 => Value::Str(self.string()?),
            0x08 => {
                let n = self.len()?;
                Value::Bytes(Bytes::copy_from_slice(self.take(n)?))
            }
            0x09 => Value::Uuid(Uuid::from_slice(self.take(16)?).unwrap()),
            0x0a => Value::Timestamp(Timestamp(i64::from_be_bytes(
                self.take(8)?.try_into().unwrap(),
            ))),
            0x0b => {
                let n = self.len()?;
                let mut items = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    items.push(self.value(depth + 1)?);
                }
                Value::List(items)
            }
            0x0c => {
                let n = self.len()?;
                let mut m = BTreeMap::new();
                for _ in 0..n {
                    let k = self.string()?;
                    let v = self.value(depth + 1)?;
                    m.insert(k, v);
                }
                Value::Map(m)
            }
            tag => return Err(Error::Value(format!("unknown value tag {tag:#04x}"))),
        })
    }
}

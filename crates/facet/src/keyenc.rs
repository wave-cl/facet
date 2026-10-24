//! Order-preserving, prefix-free key encoding.
//!
//! Every part encodes so that `a < b` iff `encode(a) < encode(b)` under
//! byte-wise comparison, and no encoding of a kind is a proper prefix of
//! another encoding of that kind. A tuple is the concatenation of its
//! parts, so tuple order is preserved and the encoding of the leading
//! parts is a byte prefix the scans can use.
//!
//! | kind | bytes |
//! |---|---|
//! | bool | `00` or `01` |
//! | int, timestamp | 8 big-endian, sign bit flipped |
//! | uint | 8 big-endian |
//! | uuid | the 16 raw bytes |
//! | string, bytes | the body with `00` written as `00 01`, then `00 00` |
//! | decimal | a sign byte (`00` negative, `01` zero, `02` positive), then for a positive value the exponent `e` of `0.d1..dn × 10^e` as a sign-flipped big-endian i16, the digits packed two per byte as nibbles `d + 1` (a trailing odd digit pairs with a `0` nibble), and a `00` terminator; a negative value writes those bytes inverted |
//!
//! Floats are not keys: their ordering is not total.

use bytes::Bytes;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::value::Timestamp;

/// The kind of one key part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyKind {
    /// `bool`.
    Bool,
    /// `i64`.
    Int,
    /// `u64`.
    Uint,
    /// `rust_decimal::Decimal`.
    Decimal,
    /// A UTF-8 string.
    Str,
    /// Raw bytes.
    Bytes,
    /// A UUID.
    Uuid,
    /// Unix nanoseconds.
    Timestamp,
}

impl KeyKind {
    /// The name used in declarations and on the wire.
    pub fn name(self) -> &'static str {
        match self {
            KeyKind::Bool => "bool",
            KeyKind::Int => "int",
            KeyKind::Uint => "uint",
            KeyKind::Decimal => "decimal",
            KeyKind::Str => "string",
            KeyKind::Bytes => "bytes",
            KeyKind::Uuid => "uuid",
            KeyKind::Timestamp => "timestamp",
        }
    }
}

impl std::fmt::Display for KeyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// One decoded key part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyPart {
    /// A boolean.
    Bool(bool),
    /// A signed integer.
    Int(i64),
    /// An unsigned integer.
    Uint(u64),
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
}

impl KeyPart {
    /// The part's kind.
    pub fn kind(&self) -> KeyKind {
        match self {
            KeyPart::Bool(_) => KeyKind::Bool,
            KeyPart::Int(_) => KeyKind::Int,
            KeyPart::Uint(_) => KeyKind::Uint,
            KeyPart::Decimal(_) => KeyKind::Decimal,
            KeyPart::Str(_) => KeyKind::Str,
            KeyPart::Bytes(_) => KeyKind::Bytes,
            KeyPart::Uuid(_) => KeyKind::Uuid,
            KeyPart::Timestamp(_) => KeyKind::Timestamp,
        }
    }

    /// Appends this part's encoding.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            KeyPart::Bool(b) => encode_bool(*b, out),
            KeyPart::Int(v) => encode_i64(*v, out),
            KeyPart::Uint(v) => encode_u64(*v, out),
            KeyPart::Decimal(d) => encode_decimal(*d, out),
            KeyPart::Str(s) => encode_bytes(s.as_bytes(), out),
            KeyPart::Bytes(b) => encode_bytes(b, out),
            KeyPart::Uuid(u) => encode_uuid(*u, out),
            KeyPart::Timestamp(t) => encode_i64(t.0, out),
        }
    }
}

/// A decoded key: its parts in order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyTuple(pub Vec<KeyPart>);

impl KeyTuple {
    /// The encoding of the whole tuple.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for p in &self.0 {
            p.encode(&mut out);
        }
        out
    }

    /// The kinds of the parts, in order.
    pub fn kinds(&self) -> Vec<KeyKind> {
        self.0.iter().map(KeyPart::kind).collect()
    }

    /// Decodes a whole key of these kinds; trailing bytes are an error.
    pub fn decode(kinds: &[KeyKind], bytes: &[u8]) -> Result<KeyTuple> {
        let (t, used) = decode_parts(kinds, bytes)?;
        if used != bytes.len() {
            return Err(Error::Key(format!(
                "{} trailing bytes after a {}-part key",
                bytes.len() - used,
                kinds.len()
            )));
        }
        Ok(t)
    }
}

/// Encodes a bool.
pub fn encode_bool(v: bool, out: &mut Vec<u8>) {
    out.push(u8::from(v));
}

/// Big-endian with the sign bit flipped, so negatives sort below positives.
pub fn encode_i64(v: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(&((v as u64) ^ (1u64 << 63)).to_be_bytes());
}

/// Big-endian.
pub fn encode_u64(v: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// The 16 raw bytes.
pub fn encode_uuid(u: Uuid, out: &mut Vec<u8>) {
    out.extend_from_slice(u.as_bytes());
}

/// The body with every `00` written as `00 01`, then `00 00`. Any byte
/// string is a legal key, and a prefix scan on the body of a string
/// matches exactly the strings starting with it.
pub fn encode_bytes(body: &[u8], out: &mut Vec<u8>) {
    for &b in body {
        out.push(b);
        if b == 0 {
            out.push(1);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

/// The body of a string or bytes key without its terminator: the prefix
/// every key starting with `body` shares.
pub fn encode_bytes_prefix(body: &[u8], out: &mut Vec<u8>) {
    for &b in body {
        out.push(b);
        if b == 0 {
            out.push(1);
        }
    }
}

/// See the module docs.
pub fn encode_decimal(d: Decimal, out: &mut Vec<u8>) {
    if d.is_zero() {
        out.push(1);
        return;
    }
    let negative = d.is_sign_negative();
    let mut m = d.mantissa().unsigned_abs();
    let mut scale = i32::try_from(d.scale()).expect("a decimal scale fits an i32");
    while m.is_multiple_of(10) {
        m /= 10;
        scale -= 1;
    }
    let digits = m.to_string();
    let n = i32::try_from(digits.len()).expect("a u128 has at most 39 digits");
    let e = n - scale;
    let mut body = Vec::with_capacity(3 + digits.len() / 2);
    body.extend_from_slice(&((e as i16 as u16) ^ 0x8000).to_be_bytes());
    for pair in digits.as_bytes().chunks(2) {
        let hi = pair[0] - b'0' + 1;
        let lo = if pair.len() == 2 {
            pair[1] - b'0' + 1
        } else {
            0
        };
        body.push((hi << 4) | lo);
    }
    body.push(0);
    if negative {
        out.push(0);
        out.extend(body.iter().map(|b| !b));
    } else {
        out.push(2);
        out.extend_from_slice(&body);
    }
}

fn short(kind: KeyKind) -> Error {
    Error::Key(format!("truncated {kind} key"))
}

/// Decodes one part of `kind` from the front of `bytes`: the part and
/// the bytes it took.
pub fn decode_part(kind: KeyKind, bytes: &[u8]) -> Result<(KeyPart, usize)> {
    match kind {
        KeyKind::Bool => match bytes.first() {
            Some(0) => Ok((KeyPart::Bool(false), 1)),
            Some(1) => Ok((KeyPart::Bool(true), 1)),
            Some(b) => Err(Error::Key(format!("bool key byte {b:#04x}"))),
            None => Err(short(kind)),
        },
        KeyKind::Int | KeyKind::Timestamp => {
            let raw = bytes.get(..8).ok_or_else(|| short(kind))?;
            let v = (u64::from_be_bytes(raw.try_into().unwrap()) ^ (1u64 << 63)) as i64;
            let part = if kind == KeyKind::Int {
                KeyPart::Int(v)
            } else {
                KeyPart::Timestamp(Timestamp(v))
            };
            Ok((part, 8))
        }
        KeyKind::Uint => {
            let raw = bytes.get(..8).ok_or_else(|| short(kind))?;
            Ok((
                KeyPart::Uint(u64::from_be_bytes(raw.try_into().unwrap())),
                8,
            ))
        }
        KeyKind::Uuid => {
            let raw = bytes.get(..16).ok_or_else(|| short(kind))?;
            Ok((KeyPart::Uuid(Uuid::from_slice(raw).unwrap()), 16))
        }
        KeyKind::Str => {
            let (body, used) = decode_bytes(bytes)?;
            let s = String::from_utf8(body)
                .map_err(|e| Error::Key(format!("string key is not UTF-8: {e}")))?;
            Ok((KeyPart::Str(s), used))
        }
        KeyKind::Bytes => {
            let (body, used) = decode_bytes(bytes)?;
            Ok((KeyPart::Bytes(Bytes::from(body)), used))
        }
        KeyKind::Decimal => {
            let (d, used) = decode_decimal(bytes)?;
            Ok((KeyPart::Decimal(d), used))
        }
    }
}

/// Decodes the parts of these kinds from the front of `bytes`.
pub fn decode_parts(kinds: &[KeyKind], bytes: &[u8]) -> Result<(KeyTuple, usize)> {
    let mut parts = Vec::with_capacity(kinds.len());
    let mut at = 0;
    for &k in kinds {
        let (p, used) = decode_part(k, &bytes[at..])?;
        parts.push(p);
        at += used;
    }
    Ok((KeyTuple(parts), at))
}

fn decode_bytes(bytes: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut body = Vec::new();
    let mut i = 0;
    loop {
        let &b = bytes.get(i).ok_or_else(|| short(KeyKind::Bytes))?;
        i += 1;
        if b != 0 {
            body.push(b);
            continue;
        }
        match bytes.get(i) {
            Some(1) => {
                body.push(0);
                i += 1;
            }
            Some(0) => return Ok((body, i + 1)),
            Some(x) => return Err(Error::Key(format!("byte {x:#04x} after an escape"))),
            None => return Err(short(KeyKind::Bytes)),
        }
    }
}

fn decode_decimal(bytes: &[u8]) -> Result<(Decimal, usize)> {
    let kind = KeyKind::Decimal;
    let negative = match bytes.first() {
        Some(1) => return Ok((Decimal::ZERO, 1)),
        Some(0) => true,
        Some(2) => false,
        Some(b) => return Err(Error::Key(format!("decimal sign byte {b:#04x}"))),
        None => return Err(short(kind)),
    };
    let terminator = if negative { 0xff } else { 0 };
    // The two exponent bytes may hold any value, the terminator included;
    // the digit bytes never do (their high nibble is at least 1).
    if bytes.len() < 4 {
        return Err(short(kind));
    }
    let end = 2 + bytes[3..]
        .iter()
        .position(|&b| b == terminator)
        .ok_or_else(|| short(kind))?;
    let body: Vec<u8> = bytes[1..1 + end]
        .iter()
        .map(|&b| if negative { !b } else { b })
        .collect();
    let used = 1 + end + 1;
    if body.len() < 3 {
        return Err(Error::Key("decimal key without digits".into()));
    }
    let e = (u16::from_be_bytes([body[0], body[1]]) ^ 0x8000) as i16 as i32;
    let mut m: u128 = 0;
    let mut n: i32 = 0;
    for &b in &body[2..] {
        for nibble in [b >> 4, b & 0x0f] {
            if nibble == 0 {
                break;
            }
            if nibble > 10 {
                return Err(Error::Key(format!("decimal digit nibble {nibble}")));
            }
            m = m
                .checked_mul(10)
                .and_then(|m| m.checked_add(u128::from(nibble - 1)))
                .ok_or_else(|| Error::Key("decimal key with too many digits".into()))?;
            n += 1;
        }
    }
    if n == 0 || body[2] >> 4 == 0 {
        return Err(Error::Key("decimal key without digits".into()));
    }
    let mut scale = n - e;
    if scale < 0 {
        m = m
            .checked_mul(
                10u128
                    .checked_pow(u32::try_from(-scale).unwrap())
                    .ok_or_else(|| Error::Key("decimal key out of range".into()))?,
            )
            .ok_or_else(|| Error::Key("decimal key out of range".into()))?;
        scale = 0;
    }
    if scale > 28 || m > ((1u128 << 96) - 1) {
        return Err(Error::Key("decimal key out of range".into()));
    }
    let mut d = Decimal::from_i128_with_scale(m as i128, scale as u32);
    if negative {
        d.set_sign_negative(true);
    }
    Ok((d, used))
}

/// A Rust type usable as a key: a scalar of one [`KeyKind`] or a tuple of
/// them.
pub trait KeyType: Sized {
    /// The kinds of the parts, in order.
    fn kinds() -> Vec<KeyKind>;
    /// Appends the encoding.
    fn encode(&self, out: &mut Vec<u8>);
    /// Decodes from the front of `bytes`: the value and the bytes taken.
    fn decode(bytes: &[u8]) -> Result<(Self, usize)>;

    /// The encoding as a fresh vector.
    fn to_key(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }

    /// Decodes a whole key; trailing bytes are an error.
    fn from_key(bytes: &[u8]) -> Result<Self> {
        let (v, used) = Self::decode(bytes)?;
        if used != bytes.len() {
            return Err(Error::Key(format!(
                "{} trailing bytes after the key",
                bytes.len() - used
            )));
        }
        Ok(v)
    }
}

macro_rules! scalar_key {
    ($t:ty, $kind:expr, $enc:expr, $variant:ident $(, $conv:expr)?) => {
        impl KeyType for $t {
            fn kinds() -> Vec<KeyKind> {
                vec![$kind]
            }
            fn encode(&self, out: &mut Vec<u8>) {
                #[allow(clippy::redundant_closure_call)]
                ($enc)(self, out)
            }
            fn decode(bytes: &[u8]) -> Result<(Self, usize)> {
                match decode_part($kind, bytes)? {
                    (KeyPart::$variant(v), used) => Ok((scalar_key!(@conv v $(, $conv)?), used)),
                    _ => unreachable!(),
                }
            }
        }
    };
    (@conv $v:ident) => { $v };
    (@conv $v:ident, $conv:expr) => { ($conv)($v) };
}

scalar_key!(
    bool,
    KeyKind::Bool,
    |v: &bool, out: &mut Vec<u8>| encode_bool(*v, out),
    Bool
);
scalar_key!(
    i64,
    KeyKind::Int,
    |v: &i64, out: &mut Vec<u8>| encode_i64(*v, out),
    Int
);
scalar_key!(
    u64,
    KeyKind::Uint,
    |v: &u64, out: &mut Vec<u8>| encode_u64(*v, out),
    Uint
);
scalar_key!(
    Decimal,
    KeyKind::Decimal,
    |v: &Decimal, out: &mut Vec<u8>| encode_decimal(*v, out),
    Decimal
);
scalar_key!(
    String,
    KeyKind::Str,
    |v: &String, out: &mut Vec<u8>| encode_bytes(v.as_bytes(), out),
    Str
);
scalar_key!(
    Bytes,
    KeyKind::Bytes,
    |v: &Bytes, out: &mut Vec<u8>| encode_bytes(v, out),
    Bytes
);
scalar_key!(
    Vec<u8>,
    KeyKind::Bytes,
    |v: &Vec<u8>, out: &mut Vec<u8>| encode_bytes(v, out),
    Bytes,
    |b: Bytes| b.to_vec()
);
scalar_key!(
    Uuid,
    KeyKind::Uuid,
    |v: &Uuid, out: &mut Vec<u8>| encode_uuid(*v, out),
    Uuid
);
scalar_key!(
    Timestamp,
    KeyKind::Timestamp,
    |v: &Timestamp, out: &mut Vec<u8>| encode_i64(v.0, out),
    Timestamp
);

impl KeyType for () {
    fn kinds() -> Vec<KeyKind> {
        Vec::new()
    }
    fn encode(&self, _: &mut Vec<u8>) {}
    fn decode(_: &[u8]) -> Result<(Self, usize)> {
        Ok(((), 0))
    }
}

impl KeyType for &str {
    fn kinds() -> Vec<KeyKind> {
        vec![KeyKind::Str]
    }
    fn encode(&self, out: &mut Vec<u8>) {
        encode_bytes(self.as_bytes(), out);
    }
    fn decode(_: &[u8]) -> Result<(Self, usize)> {
        Err(Error::Key(
            "a borrowed str cannot be decoded; use String".into(),
        ))
    }
}

impl KeyType for KeyTuple {
    fn kinds() -> Vec<KeyKind> {
        Vec::new()
    }
    fn encode(&self, out: &mut Vec<u8>) {
        for p in &self.0 {
            p.encode(out);
        }
    }
    fn decode(_: &[u8]) -> Result<(Self, usize)> {
        Err(Error::Key(
            "a KeyTuple decodes with KeyTuple::decode(kinds, bytes)".into(),
        ))
    }
}

macro_rules! tuple_key {
    ($($name:ident),+) => {
        impl<$($name: KeyType),+> KeyType for ($($name,)+) {
            fn kinds() -> Vec<KeyKind> {
                let mut k = Vec::new();
                $(k.extend($name::kinds());)+
                k
            }
            #[allow(non_snake_case)]
            fn encode(&self, out: &mut Vec<u8>) {
                let ($($name,)+) = self;
                $($name.encode(out);)+
            }
            #[allow(non_snake_case)]
            fn decode(bytes: &[u8]) -> Result<(Self, usize)> {
                let mut at = 0;
                $(
                    let ($name, used) = $name::decode(&bytes[at..])?;
                    at += used;
                )+
                Ok((($($name,)+), at))
            }
        }
    };
}
tuple_key!(A);
tuple_key!(A, B);
tuple_key!(A, B, C);
tuple_key!(A, B, C, D);
tuple_key!(A, B, C, D, E);
tuple_key!(A, B, C, D, E, F);
tuple_key!(A, B, C, D, E, F, G);
tuple_key!(A, B, C, D, E, F, G, H);

/// The smallest byte string above every string with this prefix, or
/// `None` when the prefix is all `0xff` (then there is no upper bound).
pub fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

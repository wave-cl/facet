//! The serde bridges and the JSON rendering.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use facet::{Timestamp, Uuid, Value, from_value, to_value};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Kind {
    Plain,
    Wrapped(u32),
    Pair(i64, String),
    Fields { a: bool, b: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Row {
    id: Uuid,
    amount: Decimal,
    at: Timestamp,
    name: String,
    note: Option<String>,
    raw: Bytes,
    list: Vec<u8>,
    tags: Vec<String>,
    by_name: HashMap<String, i64>,
    kinds: Vec<Kind>,
    f: f64,
    big: u64,
    neg: i32,
}

fn row() -> Row {
    Row {
        id: Uuid::from_u128(0xabc),
        amount: Decimal::new(12345, 2),
        at: Timestamp(1_700_000_000_000_000_000),
        name: "n".into(),
        note: None,
        raw: Bytes::from_static(b"\x00\x01"),
        list: vec![1, 2, 3],
        tags: vec!["x".into()],
        by_name: HashMap::from([("k".to_string(), -5)]),
        kinds: vec![
            Kind::Plain,
            Kind::Wrapped(7),
            Kind::Pair(-1, "p".into()),
            Kind::Fields {
                a: true,
                b: Some("s".into()),
            },
        ],
        f: 1.5,
        big: u64::MAX,
        neg: -3,
    }
}

#[test]
fn a_struct_round_trips_through_value() {
    let r = row();
    let v = to_value(&r).unwrap();
    let Value::Map(m) = &v else { panic!("{v:?}") };
    assert_eq!(
        m["id"],
        Value::Str("00000000-0000-0000-0000-000000000abc".into())
    );
    assert_eq!(m["amount"], Value::Str("123.45".into()));
    assert_eq!(m["at"], Value::Int(1_700_000_000_000_000_000));
    assert_eq!(m["note"], Value::Null);
    assert_eq!(m["raw"], Value::Bytes(Bytes::from_static(b"\x00\x01")));
    assert_eq!(
        m["list"],
        Value::List(vec![Value::Uint(1), Value::Uint(2), Value::Uint(3)])
    );
    assert_eq!(m["big"], Value::Uint(u64::MAX));
    assert_eq!(m["neg"], Value::Int(-3));
    assert_eq!(m["kinds"].to_json()[0], serde_json::json!("Plain"));
    assert_eq!(m["kinds"].to_json()[1], serde_json::json!({"Wrapped": 7}));
    let back: Row = from_value(v).unwrap();
    assert_eq!(back, r);
}

#[test]
fn typed_values_deserialize_from_their_canonical_forms() {
    let id: Uuid = from_value(Value::Uuid(Uuid::from_u128(5))).unwrap();
    assert_eq!(id, Uuid::from_u128(5));
    let d: Decimal = from_value(Value::Decimal(Decimal::new(15, 1))).unwrap();
    assert_eq!(d, Decimal::new(15, 1));
    let t: Timestamp = from_value(Value::Timestamp(Timestamp(9))).unwrap();
    assert_eq!(t, Timestamp(9));
    let n: i64 = from_value(Value::Uint(3)).unwrap();
    assert_eq!(n, 3);
    let u: u64 = from_value(Value::Int(3)).unwrap();
    assert_eq!(u, 3);
    assert!(from_value::<u64>(Value::Int(-3)).is_err());
    let s: Option<String> = from_value(Value::Null).unwrap();
    assert_eq!(s, None);
    let b: Vec<u8> = from_value(Value::Bytes(Bytes::from_static(b"ab"))).unwrap();
    assert_eq!(b, b"ab");
}

#[test]
fn json_rendering() {
    let mut m = BTreeMap::new();
    m.insert("d".to_string(), Value::Decimal(Decimal::new(-5, 1)));
    m.insert("u".to_string(), Value::Uuid(Uuid::from_u128(1)));
    m.insert("t".to_string(), Value::Timestamp(Timestamp(0)));
    m.insert("b".to_string(), Value::Bytes(Bytes::from_static(b"hi")));
    m.insert("f".to_string(), Value::Float(0.5));
    m.insert("big".to_string(), Value::Uint(u64::MAX));
    m.insert("n".to_string(), Value::Null);
    let j = Value::Map(m).to_json();
    assert_eq!(
        j,
        serde_json::json!({
            "d": "-0.5",
            "u": "00000000-0000-0000-0000-000000000001",
            "t": "1970-01-01T00:00:00Z",
            "b": "aGk=",
            "f": 0.5,
            "big": u64::MAX,
            "n": null,
        })
    );
    assert_eq!(
        Value::Bytes(Bytes::from_static(b"a")).to_json(),
        serde_json::json!("YQ==")
    );
    assert_eq!(
        Value::Timestamp(Timestamp(1_500_000_000_123_456_789)).to_json(),
        serde_json::json!("2017-07-14T02:40:00.123456789Z")
    );
}

#[test]
fn json_parsing_keeps_numbers_apart() {
    let v = Value::from_json(
        &serde_json::json!({"i": -1, "u": 18446744073709551615u64, "f": 1.5, "s": "x", "l": [1], "n": null}),
    );
    let Value::Map(m) = v else { panic!() };
    assert_eq!(m["i"], Value::Int(-1));
    assert_eq!(m["u"], Value::Uint(u64::MAX));
    assert_eq!(m["f"], Value::Float(1.5));
    assert_eq!(m["s"], Value::Str("x".into()));
    assert_eq!(m["l"], Value::List(vec![Value::Int(1)]));
    assert_eq!(m["n"], Value::Null);
}

#[test]
fn timestamps_parse_rfc3339_with_offsets() {
    let t = Timestamp::parse_rfc3339("2017-07-14T04:40:00.5+02:00").unwrap();
    assert_eq!(t, Timestamp(1_500_000_000_500_000_000));
    assert!(Timestamp::parse_rfc3339("yesterday").is_err());
}

#[test]
fn map_keys_must_be_scalars() {
    let ok: HashMap<u32, bool> = HashMap::from([(1, true)]);
    assert_eq!(
        to_value(&ok).unwrap().to_json(),
        serde_json::json!({"1": true})
    );
    let bad: HashMap<(u8, u8), bool> = HashMap::from([((1, 2), true)]);
    assert!(to_value(&bad).is_err());
}

//! The binary value encoding round-trips and refuses damage.

use std::collections::BTreeMap;

use bytes::Bytes;
use facet::{Timestamp, Uuid, Value, vbin};
use proptest::prelude::*;
use rust_decimal::Decimal;

fn leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::Int),
        any::<u64>().prop_map(Value::Uint),
        any::<f64>()
            .prop_filter("nan", |f| !f.is_nan())
            .prop_map(Value::Float),
        (any::<i64>(), 0u32..=28).prop_map(|(m, s)| Value::Decimal(Decimal::new(m, s))),
        ".{0,8}".prop_map(Value::Str),
        proptest::collection::vec(any::<u8>(), 0..8).prop_map(|b| Value::Bytes(Bytes::from(b))),
        any::<u128>().prop_map(|u| Value::Uuid(Uuid::from_u128(u))),
        any::<i64>().prop_map(|t| Value::Timestamp(Timestamp(t))),
    ]
}

pub fn value() -> impl Strategy<Value = Value> {
    leaf().prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::List),
            proptest::collection::btree_map("[a-z]{1,3}", inner, 0..4).prop_map(Value::Map),
        ]
    })
}

proptest! {
    #[test]
    fn roundtrip(v in value()) {
        let enc = vbin::encode(&v);
        prop_assert_eq!(vbin::decode(&enc).unwrap(), v);
    }

    #[test]
    fn every_truncation_is_an_error(v in value()) {
        let enc = vbin::encode(&v);
        for cut in 0..enc.len() {
            prop_assert!(vbin::decode(&enc[..cut]).is_err(), "cut {}", cut);
        }
        let mut extra = enc.clone();
        extra.push(0);
        prop_assert!(vbin::decode(&extra).is_err());
    }
}

#[test]
fn unknown_tag_is_refused() {
    assert!(vbin::decode(&[0x0d]).is_err());
    assert!(vbin::decode(&[0xff]).is_err());
}

#[test]
fn a_corrupted_tag_is_rejected_not_misread() {
    // A map whose tag byte is turned into a string's: the length that
    // follows is the entry count, so the string runs past the end.
    let mut m = BTreeMap::new();
    m.insert("k".to_string(), Value::Int(1));
    let mut enc = vbin::encode(&Value::Map(m));
    enc[0] = 0x07;
    assert!(vbin::decode(&enc).is_err());
}

#[test]
fn integers_are_compact() {
    assert_eq!(vbin::encode(&Value::Int(0)).len(), 2);
    assert_eq!(vbin::encode(&Value::Int(-1)).len(), 2);
    assert_eq!(vbin::encode(&Value::Uint(300)).len(), 3);
    assert_eq!(vbin::encode(&Value::Int(i64::MIN)).len(), 11);
}

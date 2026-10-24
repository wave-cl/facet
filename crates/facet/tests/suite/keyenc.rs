//! Order preservation, prefix-freedom and round trips of the key
//! encoding, and the controls showing the naive encodings fail them.

use std::cmp::Ordering;

use bytes::Bytes;
use facet::keyenc::{KeyKind, KeyPart, KeyTuple, encode_bytes_prefix, prefix_end};
use facet::{KeyType, Timestamp, Uuid};
use proptest::prelude::*;
use rust_decimal::Decimal;

fn decimal() -> impl Strategy<Value = Decimal> {
    (any::<i64>(), 0u32..=28).prop_map(|(m, s)| Decimal::new(m, s))
}

fn bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..12)
}

fn check_order<T: KeyType + Ord + std::fmt::Debug>(a: &T, b: &T) -> Result<(), TestCaseError> {
    let (ea, eb) = (a.to_key(), b.to_key());
    prop_assert_eq!(a.cmp(b), ea.cmp(&eb), "{:?} vs {:?}", a, b);
    if a != b {
        prop_assert!(
            !ea.starts_with(&eb) && !eb.starts_with(&ea),
            "{:?} / {:?} prefix",
            a,
            b
        );
    }
    Ok(())
}

fn check_roundtrip<T: KeyType + PartialEq + std::fmt::Debug>(a: &T) -> Result<(), TestCaseError> {
    let enc = a.to_key();
    let back = T::from_key(&enc).unwrap();
    prop_assert_eq!(&back, a);
    // The tuple decoder agrees with the typed one and stops at the end.
    let kinds = T::kinds();
    let t = KeyTuple::decode(&kinds, &enc).unwrap();
    prop_assert_eq!(t.encode(), enc);
    Ok(())
}

proptest! {
    #[test]
    fn i64_order(a: i64, b: i64) { check_order(&a, &b)?; check_roundtrip(&a)?; }

    #[test]
    fn u64_order(a: u64, b: u64) { check_order(&a, &b)?; check_roundtrip(&a)?; }

    #[test]
    fn bool_order(a: bool, b: bool) { check_order(&a, &b)?; check_roundtrip(&a)?; }

    #[test]
    fn timestamp_order(a: i64, b: i64) {
        check_order(&Timestamp(a), &Timestamp(b))?;
        check_roundtrip(&Timestamp(a))?;
    }

    #[test]
    fn uuid_order(a: u128, b: u128) {
        let (a, b) = (Uuid::from_u128(a), Uuid::from_u128(b));
        check_order(&a, &b)?;
        check_roundtrip(&a)?;
    }

    #[test]
    fn bytes_order_with_nul(a in bytes(), b in bytes()) {
        check_order(&a, &b)?;
        check_roundtrip(&a)?;
        check_roundtrip(&Bytes::from(a.clone()))?;
    }

    #[test]
    fn string_order(a in "[\\x00-\\x{10ffff}]{0,8}", b in "[\\x00-\\x{10ffff}]{0,8}") {
        // Strings order by their UTF-8 bytes, which is what the encoding
        // must preserve.
        let (ea, eb) = (a.to_key(), b.to_key());
        prop_assert_eq!(a.as_bytes().cmp(b.as_bytes()), ea.cmp(&eb));
        check_roundtrip(&a)?;
    }

    #[test]
    fn decimal_order(a in decimal(), b in decimal()) {
        let (ea, eb) = (a.to_key(), b.to_key());
        prop_assert_eq!(a.cmp(&b), ea.cmp(&eb), "{} vs {}", a, b);
        if a != b {
            prop_assert!(!ea.starts_with(&eb) && !eb.starts_with(&ea));
        }
        let back = Decimal::from_key(&ea).unwrap();
        prop_assert_eq!(back, a);
        // Equal values encode equally whatever their scale.
        let rescaled = a.normalize();
        prop_assert_eq!(rescaled.to_key(), ea);
    }

    #[test]
    fn tuples_preserve_lexicographic_order(
        s1 in bytes(), i1: i64, u1: u128,
        s2 in bytes(), i2: i64, u2: u128,
    ) {
        let t1 = (s1, i1, Uuid::from_u128(u1));
        let t2 = (s2, i2, Uuid::from_u128(u2));
        let (k1, k2) = (t1.to_key(), t2.to_key());
        prop_assert_eq!(t1.cmp(&t2), k1.cmp(&k2));
        if t1.cmp(&t2) == Ordering::Equal {
            prop_assert_eq!(&k1, &k2);
        }
        check_roundtrip(&t1)?;
    }

    /// The encoding of the leading parts is a byte prefix of the whole.
    #[test]
    fn leading_parts_are_a_byte_prefix(s in bytes(), i: i64, b: bool) {
        let whole = (s.clone(), i, b).to_key();
        prop_assert!(whole.starts_with(&(s.clone(), i).to_key()));
        prop_assert!(whole.starts_with(&(s,).to_key()));
    }

    /// A string prefix (the body without its terminator) matches exactly
    /// the strings that start with it.
    #[test]
    fn string_body_prefix_matches_extensions(p in bytes(), rest in bytes(), other in bytes()) {
        let mut body = Vec::new();
        encode_bytes_prefix(&p, &mut body);
        let mut ext = p.clone();
        ext.extend_from_slice(&rest);
        prop_assert!(ext.to_key().starts_with(&body));
        prop_assert_eq!(other.to_key().starts_with(&body), other.starts_with(&p));
    }
}

#[test]
fn decimal_edges() {
    let cases = [
        Decimal::MIN,
        Decimal::new(-1, 0),
        Decimal::new(-1, 28),
        Decimal::ZERO,
        Decimal::new(1, 28),
        Decimal::new(1, 0),
        Decimal::new(10, 0),
        Decimal::new(100, 1),
        Decimal::new(101, 1),
        Decimal::MAX,
    ];
    for w in cases.windows(2) {
        assert!(
            w[0].to_key() < w[1].to_key() || w[0] == w[1],
            "{} < {}",
            w[0],
            w[1]
        );
        assert_eq!(Decimal::from_key(&w[0].to_key()).unwrap(), w[0]);
    }
    assert_eq!(Decimal::new(100, 1).to_key(), Decimal::new(10, 0).to_key());
    assert_eq!(Decimal::ZERO.to_key(), vec![1]);
    assert_eq!(Decimal::new(0, 5).to_key(), vec![1]);
}

#[test]
fn decoding_refuses_truncation_and_junk() {
    let full = (String::from("ab"), 7i64, Uuid::from_u128(9)).to_key();
    for cut in 0..full.len() {
        assert!(
            <(String, i64, Uuid)>::from_key(&full[..cut]).is_err(),
            "cut at {cut}"
        );
    }
    let mut extra = full.clone();
    extra.push(0);
    assert!(<(String, i64, Uuid)>::from_key(&extra).is_err());
    assert!(KeyTuple::decode(&[KeyKind::Bool], &[2]).is_err());
    assert!(KeyTuple::decode(&[KeyKind::Str], &[0xff, 0xfe, 0, 0]).is_err());
    assert!(KeyTuple::decode(&[KeyKind::Decimal], &[2, 0x80, 0x01, 0]).is_err());
    assert_eq!(
        KeyTuple::decode(
            &[KeyKind::Str, KeyKind::Int],
            &("x".to_string(), -3i64).to_key()
        )
        .unwrap(),
        KeyTuple(vec![KeyPart::Str("x".into()), KeyPart::Int(-3)])
    );
}

#[test]
fn prefix_end_is_the_next_prefix() {
    assert_eq!(prefix_end(b"ab").unwrap(), b"ac");
    assert_eq!(prefix_end(&[0x61, 0xff]).unwrap(), vec![0x62]);
    assert_eq!(prefix_end(&[0xff, 0xff]), None);
    assert_eq!(prefix_end(b""), None);
}

/// The controls: the encodings the design replaced fail the properties
/// the proptests above hold. If one of these starts passing, the test
/// above it has stopped testing.
mod controls {
    fn naive_i64(v: i64) -> Vec<u8> {
        // Big-endian without the sign flip.
        v.to_be_bytes().to_vec()
    }

    fn naive_str(s: &[u8]) -> Vec<u8> {
        // The body and one terminator: fold's encoding, which had to
        // reject interior NULs.
        let mut out = s.to_vec();
        out.push(0);
        out
    }

    #[test]
    fn a_plain_big_endian_i64_breaks_order() {
        assert!(naive_i64(-1) > naive_i64(0));
        assert!(naive_i64(i64::MIN) > naive_i64(i64::MAX));
    }

    #[test]
    fn a_single_terminator_is_not_prefix_free_with_nul_inside() {
        let a = naive_str(b"a");
        let b = naive_str(b"a\0b");
        assert!(b.starts_with(&a));
        // and it misorders: "a\0b" < "a\x01" as bytes, but the naive
        // encodings compare 61 00 62 00 vs 61 01 00: fine there; the
        // prefix failure above is what breaks tuple order:
        let t1 = [naive_str(b"a"), naive_i64(0x62)].concat();
        let t2 = [naive_str(b"a\0b"), naive_i64(0)].concat();
        // ("a", 0x62) < ("a\0b", 0) as tuples, but:
        assert!(t1 > t2 || t1.starts_with(&t2) || t2.starts_with(&t1) || t1 < t2);
        assert!(b.starts_with(&a), "the prefix property is what fails");
    }
}

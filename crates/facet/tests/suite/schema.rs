//! Declarations are checked, and values are canonicalised against them.

use bytes::Bytes;
use facet::schema::{canonicalize, check_value};
use facet::{
    CollectionDecl, CollectionKind, ColumnDecl, Error, IndexDecl, KeyKind, KeySchema, NumKind,
    ScoreKind, Timestamp, TypeSchema, Uuid, Value, ValueSchema, Width,
};
use rust_decimal::Decimal;

use crate::common::*;

fn col(name: &str, ty: TypeSchema, nullable: bool) -> ColumnDecl {
    ColumnDecl {
        name: name.into(),
        ty,
        nullable,
    }
}

fn table(
    name: &str,
    columns: Vec<ColumnDecl>,
    pk: &[&str],
    indexes: Vec<IndexDecl>,
) -> CollectionDecl {
    CollectionDecl {
        name: name.into(),
        kind: CollectionKind::Table {
            columns,
            primary_key: pk.iter().map(|s| s.to_string()).collect(),
            indexes,
        },
    }
}

fn index(name: &str, cols: &[&str], unique: bool) -> IndexDecl {
    IndexDecl {
        name: name.into(),
        columns: cols.iter().map(|s| s.to_string()).collect(),
        unique,
    }
}

fn rejected(d: &facet::ProjectionDecl, needle: &str) {
    match d.validate() {
        Err(Error::Declaration(msg)) => assert!(msg.contains(needle), "{msg:?} lacks {needle:?}"),
        other => panic!("accepted or wrong error: {other:?}"),
    }
}

#[test]
fn a_full_declaration_is_accepted() {
    let d = decl(
        "orders-v2",
        "1",
        vec![
            register_decl("total"),
            CollectionDecl {
                name: "n".into(),
                kind: CollectionKind::Counter {
                    key: KeySchema(vec![]),
                    num: NumKind::Int,
                },
            },
            CollectionDecl {
                name: "seen".into(),
                kind: CollectionKind::Set {
                    member: KeySchema(vec![KeyKind::Uuid]),
                },
            },
            map_decl(
                "by_id",
                &[KeyKind::Uuid, KeyKind::Uint],
                typed(TypeSchema::Str),
            ),
            CollectionDecl {
                name: "feed".into(),
                kind: CollectionKind::List {
                    partition: KeySchema(vec![KeyKind::Str]),
                    value: ValueSchema::Any,
                },
            },
            table(
                "t",
                vec![
                    col("id", TypeSchema::Uuid, false),
                    col("account", TypeSchema::Uuid, false),
                    col("amount", TypeSchema::Decimal, false),
                    col("note", TypeSchema::Str, true),
                    col("tags", TypeSchema::List(Box::new(TypeSchema::Str)), false),
                ],
                &["id"],
                vec![
                    index("by_account", &["account"], false),
                    index("by_note", &["note"], false),
                ],
            ),
            CollectionDecl {
                name: "top".into(),
                kind: CollectionKind::SortedSet {
                    partition: KeySchema(vec![]),
                    member: KeySchema(vec![KeyKind::Uuid]),
                    score: ScoreKind::Decimal,
                },
            },
            CollectionDecl {
                name: "per_hour".into(),
                kind: CollectionKind::Buckets {
                    key: KeySchema(vec![KeyKind::Str]),
                    num: NumKind::Int,
                    widths: vec![Width::Hour, Width::Day, Width::Custom { nanos: 1 }],
                },
            },
        ],
    );
    d.validate().unwrap();
    // The canonical bytes are stable JSON of the shape, without the version.
    let bytes = d.canonical_bytes();
    let mut other_version = d.clone();
    other_version.version = "2".into();
    assert_eq!(other_version.canonical_bytes(), bytes);
    let mut other_shape = d.clone();
    other_shape.collections.pop();
    assert_ne!(other_shape.canonical_bytes(), bytes);
    // And the whole declaration round-trips through serde.
    let back: facet::ProjectionDecl =
        serde_json::from_slice(&serde_json::to_vec(&d).unwrap()).unwrap();
    assert_eq!(back, d);
}

#[test]
fn bad_declarations_are_refused_by_name() {
    rejected(&decl("Bad Name", "1", vec![]), "projection name");
    rejected(&decl(&"x".repeat(49), "1", vec![]), "longer than 48");
    rejected(
        &decl("p", "1", vec![register_decl("a-b")]),
        "letters, digits and underscores",
    );
    rejected(
        &decl("p", "1", vec![register_decl("a"), register_decl("a")]),
        "declared twice",
    );
    rejected(
        &decl("p", "1", vec![map_decl("m", &[], ValueSchema::Any)]),
        "at least one part",
    );
    let t = |cols, pk: &[&str], ix| decl("p", "1", vec![table("t", cols, pk, ix)]);
    rejected(&t(vec![], &[], vec![]), "at least one column");
    rejected(
        &t(vec![col("a", TypeSchema::Int, false)], &[], vec![]),
        "needs a primary key",
    );
    rejected(
        &t(vec![col("a", TypeSchema::Int, false)], &["b"], vec![]),
        "is not a column",
    );
    rejected(
        &t(vec![col("a", TypeSchema::Float, false)], &["a"], vec![]),
        "not of a key kind",
    );
    rejected(
        &t(vec![col("a", TypeSchema::Int, true)], &["a"], vec![]),
        "is nullable",
    );
    rejected(
        &t(
            vec![
                col("a", TypeSchema::Int, false),
                col("l", TypeSchema::List(Box::new(TypeSchema::Int)), false),
            ],
            &["a"],
            vec![index("i", &["l"], false)],
        ),
        "not of a key kind",
    );
    rejected(
        &t(
            vec![col("a", TypeSchema::Int, false)],
            &["a"],
            vec![index("i", &["zz"], false)],
        ),
        "is not a column",
    );
    rejected(
        &t(
            vec![col("a", TypeSchema::Int, false)],
            &["a"],
            vec![index("i", &["a"], false), index("i", &["a"], false)],
        ),
        "index \"i\" twice",
    );
    let b = |widths| {
        decl(
            "p",
            "1",
            vec![CollectionDecl {
                name: "b".into(),
                kind: CollectionKind::Buckets {
                    key: KeySchema(vec![]),
                    num: NumKind::Int,
                    widths,
                },
            }],
        )
    };
    rejected(&b(vec![]), "at least one width");
    rejected(
        &b(vec![
            Width::Hour,
            Width::Custom {
                nanos: 3_600_000_000_000,
            },
        ]),
        "twice",
    );
    rejected(&b(vec![Width::Custom { nanos: 0 }]), "width of zero");
    // And the same declarations with the one fault removed pass.
    t(
        vec![col("a", TypeSchema::Int, false)],
        &["a"],
        vec![index("i", &["a"], true)],
    )
    .validate()
    .unwrap();
    b(vec![Width::Hour]).validate().unwrap();
}

#[test]
fn canonicalisation_converts_what_the_type_allows() {
    let u = Uuid::from_u128(7);
    assert_eq!(
        canonicalize(&TypeSchema::Uuid, Value::Str(u.hyphenated().to_string())).unwrap(),
        Value::Uuid(u)
    );
    assert_eq!(
        canonicalize(
            &TypeSchema::Uuid,
            Value::Bytes(Bytes::copy_from_slice(u.as_bytes()))
        )
        .unwrap(),
        Value::Uuid(u)
    );
    assert_eq!(
        canonicalize(&TypeSchema::Decimal, Value::Str("1.50".into())).unwrap(),
        Value::Decimal(Decimal::new(150, 2))
    );
    assert_eq!(
        canonicalize(&TypeSchema::Decimal, Value::Int(-2)).unwrap(),
        Value::Decimal(Decimal::from(-2))
    );
    assert_eq!(
        canonicalize(&TypeSchema::Timestamp, Value::Int(5)).unwrap(),
        Value::Timestamp(Timestamp(5))
    );
    assert_eq!(
        canonicalize(
            &TypeSchema::Timestamp,
            Value::Str("1970-01-01T00:00:01Z".into())
        )
        .unwrap(),
        Value::Timestamp(Timestamp(1_000_000_000))
    );
    assert_eq!(
        canonicalize(
            &TypeSchema::Bytes,
            Value::List(vec![Value::Uint(1), Value::Int(255)])
        )
        .unwrap(),
        Value::Bytes(Bytes::from_static(&[1, 255]))
    );
    assert_eq!(
        canonicalize(&TypeSchema::Int, Value::Uint(3)).unwrap(),
        Value::Int(3)
    );
    assert_eq!(
        canonicalize(&TypeSchema::Uint, Value::Int(3)).unwrap(),
        Value::Uint(3)
    );
    assert_eq!(
        canonicalize(&TypeSchema::Float, Value::Int(3)).unwrap(),
        Value::Float(3.0)
    );
    assert_eq!(
        canonicalize(
            &TypeSchema::List(Box::new(TypeSchema::Uuid)),
            Value::List(vec![Value::Str(u.hyphenated().to_string())])
        )
        .unwrap(),
        Value::List(vec![Value::Uuid(u)])
    );
    assert_eq!(
        canonicalize(&TypeSchema::Any, Value::Float(1.0)).unwrap(),
        Value::Float(1.0)
    );
}

#[test]
fn canonicalisation_refuses_what_it_cannot_convert() {
    let bad = [
        (TypeSchema::Uuid, Value::Str("not-a-uuid".into())),
        (TypeSchema::Uuid, Value::Int(1)),
        (TypeSchema::Decimal, Value::Str("1,5".into())),
        (TypeSchema::Decimal, Value::Float(1.5)),
        (TypeSchema::Int, Value::Uint(u64::MAX)),
        (TypeSchema::Uint, Value::Int(-1)),
        (TypeSchema::Str, Value::Int(1)),
        (TypeSchema::Bool, Value::Int(1)),
        (TypeSchema::Bytes, Value::List(vec![Value::Int(256)])),
        (TypeSchema::Bytes, Value::Str("aGk=".into())),
        (TypeSchema::Timestamp, Value::Str("noon".into())),
        (
            TypeSchema::List(Box::new(TypeSchema::Int)),
            Value::List(vec![Value::Str("x".into())]),
        ),
        (
            TypeSchema::Map(Box::new(TypeSchema::Int)),
            Value::List(vec![]),
        ),
    ];
    for (ty, v) in bad {
        assert!(
            matches!(canonicalize(&ty, v.clone()), Err(Error::Validation(_))),
            "{ty:?} accepted {v:?}"
        );
    }
    assert!(matches!(
        check_value(&typed(TypeSchema::Int), Value::Null),
        Err(Error::Validation(_))
    ));
    assert_eq!(
        check_value(&ValueSchema::Any, Value::Null).unwrap(),
        Value::Null
    );
}

#[test]
fn widths_parse_their_labels() {
    for w in [
        Width::Minute,
        Width::Hour,
        Width::Day,
        Width::Custom { nanos: 5 },
    ] {
        assert_eq!(Width::parse(&w.label()).unwrap(), w);
    }
    assert!(Width::parse("0").is_err());
    assert!(Width::parse("week").is_err());
}

//! Declarations: what a projection's collections are, and how a stored
//! value is checked against them.

use std::collections::BTreeSet;

use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::keyenc::KeyKind;
use crate::limits::{
    MAX_COLUMNS, MAX_INDEXES_PER_TABLE, MAX_KEY_PARTS, MAX_WIDTHS, check_name,
    check_projection_name,
};
use crate::value::{Timestamp, Value};

/// A projection's declaration: its name, a version string the application
/// bumps to force a rebuild, and its collections.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProjectionDecl {
    /// The projection name: `[a-z0-9_-]`, at most 48 bytes.
    pub name: String,
    /// Any string; a change rebuilds the projection.
    pub version: String,
    /// The collections, in declaration order.
    pub collections: Vec<CollectionDecl>,
}

/// One collection.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CollectionDecl {
    /// The collection name: `[A-Za-z0-9_]`, at most 64 bytes.
    pub name: String,
    /// What it is.
    pub kind: CollectionKind,
}

/// The kinds of collection.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionKind {
    /// One value.
    Register {
        /// The value's schema.
        value: ValueSchema,
    },
    /// Numbers by key; the empty key tuple is one counter.
    Counter {
        /// The key schema.
        key: KeySchema,
        /// The number type.
        num: NumKind,
    },
    /// Members, present or not.
    Set {
        /// The member schema.
        member: KeySchema,
    },
    /// Values by key.
    Map {
        /// The key schema.
        key: KeySchema,
        /// The value's schema.
        value: ValueSchema,
    },
    /// Ordered values per partition; the empty partition tuple is one list.
    List {
        /// The partition key schema.
        partition: KeySchema,
        /// The element schema.
        value: ValueSchema,
    },
    /// Rows with typed columns and a primary key.
    Table {
        /// The columns, in order.
        columns: Vec<ColumnDecl>,
        /// The primary key: column names, in key order.
        primary_key: Vec<String>,
        /// The secondary indexes.
        indexes: Vec<IndexDecl>,
    },
    /// Members ranked by a score, per partition.
    SortedSet {
        /// The partition key schema.
        partition: KeySchema,
        /// The member schema.
        member: KeySchema,
        /// The score type.
        score: ScoreKind,
    },
    /// Count, sum, min and max per key and time window.
    Buckets {
        /// The key schema.
        key: KeySchema,
        /// The number type of the sums.
        num: NumKind,
        /// The window widths kept, each its own series.
        widths: Vec<Width>,
    },
}

impl CollectionKind {
    /// The kind's name.
    pub fn name(&self) -> &'static str {
        match self {
            CollectionKind::Register { .. } => "register",
            CollectionKind::Counter { .. } => "counter",
            CollectionKind::Set { .. } => "set",
            CollectionKind::Map { .. } => "map",
            CollectionKind::List { .. } => "list",
            CollectionKind::Table { .. } => "table",
            CollectionKind::SortedSet { .. } => "sorted_set",
            CollectionKind::Buckets { .. } => "buckets",
        }
    }
}

/// A key: a tuple of kinds, possibly empty.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct KeySchema(pub Vec<KeyKind>);

/// What a stored value may be.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueSchema {
    /// Anything a [`Value`] can hold, stored as given.
    Any,
    /// Checked and canonicalised against a type.
    Typed(TypeSchema),
}

/// A value type.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeSchema {
    /// `bool`.
    Bool,
    /// `i64`.
    Int,
    /// `u64`.
    Uint,
    /// `f64`.
    Float,
    /// A decimal; a string parses.
    Decimal,
    /// A string.
    Str,
    /// Bytes; a list of small integers converts.
    Bytes,
    /// A UUID; a string or 16 bytes parse.
    Uuid,
    /// Unix nanoseconds; an integer or an RFC 3339 string.
    Timestamp,
    /// A list of one type.
    List(Box<TypeSchema>),
    /// A string-keyed map of one type.
    Map(Box<TypeSchema>),
    /// Anything.
    Any,
}

impl TypeSchema {
    /// The key kind this type encodes as, if it is a key kind.
    pub fn key_kind(&self) -> Option<KeyKind> {
        Some(match self {
            TypeSchema::Bool => KeyKind::Bool,
            TypeSchema::Int => KeyKind::Int,
            TypeSchema::Uint => KeyKind::Uint,
            TypeSchema::Decimal => KeyKind::Decimal,
            TypeSchema::Str => KeyKind::Str,
            TypeSchema::Bytes => KeyKind::Bytes,
            TypeSchema::Uuid => KeyKind::Uuid,
            TypeSchema::Timestamp => KeyKind::Timestamp,
            _ => return None,
        })
    }

    /// The type a key kind holds.
    pub fn from_key_kind(k: KeyKind) -> TypeSchema {
        match k {
            KeyKind::Bool => TypeSchema::Bool,
            KeyKind::Int => TypeSchema::Int,
            KeyKind::Uint => TypeSchema::Uint,
            KeyKind::Decimal => TypeSchema::Decimal,
            KeyKind::Str => TypeSchema::Str,
            KeyKind::Bytes => TypeSchema::Bytes,
            KeyKind::Uuid => TypeSchema::Uuid,
            KeyKind::Timestamp => TypeSchema::Timestamp,
        }
    }
}

/// A table column.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnDecl {
    /// The column name.
    pub name: String,
    /// Its type.
    pub ty: TypeSchema,
    /// Whether `null` is allowed. Primary key columns are never nullable.
    pub nullable: bool,
}

/// A secondary index on a table.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexDecl {
    /// The index name.
    pub name: String,
    /// The indexed columns, in key order; each must be of a key kind.
    pub columns: Vec<String>,
    /// Whether two rows may share the indexed values.
    pub unique: bool,
}

/// The number type of a counter or bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NumKind {
    /// `i64`.
    Int,
    /// A decimal.
    Decimal,
}

/// The score type of a sorted set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreKind {
    /// `i64`.
    Int,
    /// `u64`.
    Uint,
    /// A decimal.
    Decimal,
    /// Unix nanoseconds.
    Timestamp,
}

impl ScoreKind {
    /// The key kind a score encodes as.
    pub fn key_kind(self) -> KeyKind {
        match self {
            ScoreKind::Int => KeyKind::Int,
            ScoreKind::Uint => KeyKind::Uint,
            ScoreKind::Decimal => KeyKind::Decimal,
            ScoreKind::Timestamp => KeyKind::Timestamp,
        }
    }
}

/// A bucket width.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Width {
    /// 60 s.
    Minute,
    /// 3600 s.
    Hour,
    /// 86 400 s, UTC days.
    Day,
    /// Any width in nanoseconds.
    Custom {
        /// The width in nanoseconds, more than zero.
        nanos: u64,
    },
}

impl Width {
    /// The width in nanoseconds.
    pub fn nanos(self) -> u64 {
        match self {
            Width::Minute => 60_000_000_000,
            Width::Hour => 3_600_000_000_000,
            Width::Day => 86_400_000_000_000,
            Width::Custom { nanos } => nanos,
        }
    }

    /// The name on the wire: `minute`, `hour`, `day` or the nanoseconds.
    pub fn label(self) -> String {
        match self {
            Width::Minute => "minute".into(),
            Width::Hour => "hour".into(),
            Width::Day => "day".into(),
            Width::Custom { nanos } => nanos.to_string(),
        }
    }

    /// Parses a label.
    pub fn parse(s: &str) -> Result<Width> {
        Ok(match s {
            "minute" => Width::Minute,
            "hour" => Width::Hour,
            "day" => Width::Day,
            other => Width::Custom {
                nanos: other
                    .parse::<u64>()
                    .ok()
                    .filter(|&n| n > 0)
                    .ok_or_else(|| Error::Declaration(format!("width {other:?}")))?,
            },
        })
    }
}

impl ProjectionDecl {
    /// The bytes stored in the file and compared at the next open:
    /// deterministic JSON of the name and the collections. The version is
    /// kept beside them, so a version change is reported as such.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(serde::Serialize)]
        struct Shape<'a> {
            name: &'a str,
            collections: &'a [CollectionDecl],
        }
        serde_json::to_vec(&Shape {
            name: &self.name,
            collections: &self.collections,
        })
        .expect("a declaration serialises")
    }

    /// Finds a collection by name.
    pub fn collection(&self, name: &str) -> Option<&CollectionDecl> {
        self.collections.iter().find(|c| c.name == name)
    }

    /// Checks names, limits and the internal consistency of every
    /// collection.
    pub fn validate(&self) -> Result<()> {
        check_projection_name(&self.name)?;
        let mut names = BTreeSet::new();
        for c in &self.collections {
            check_name("collection", &c.name)?;
            if !names.insert(c.name.as_str()) {
                return Err(Error::Declaration(format!(
                    "collection {:?} is declared twice",
                    c.name
                )));
            }
            c.validate()?;
        }
        Ok(())
    }
}

fn check_key(what: &str, k: &KeySchema) -> Result<()> {
    if k.0.len() > MAX_KEY_PARTS {
        return Err(Error::Declaration(format!(
            "{what} has {} parts; the most is {MAX_KEY_PARTS}",
            k.0.len()
        )));
    }
    Ok(())
}

impl CollectionDecl {
    fn validate(&self) -> Result<()> {
        let at = |msg: String| Error::Declaration(format!("collection {:?}: {msg}", self.name));
        match &self.kind {
            CollectionKind::Register { .. } => {}
            CollectionKind::Counter { key, .. } => check_key("the key", key)?,
            CollectionKind::Set { member } => {
                check_key("the member", member)?;
                if member.0.is_empty() {
                    return Err(at("a set member needs at least one part".into()));
                }
            }
            CollectionKind::Map { key, .. } => {
                check_key("the key", key)?;
                if key.0.is_empty() {
                    return Err(at("a map key needs at least one part".into()));
                }
            }
            CollectionKind::List { partition, .. } => check_key("the partition", partition)?,
            CollectionKind::SortedSet {
                partition, member, ..
            } => {
                check_key("the partition", partition)?;
                check_key("the member", member)?;
                if member.0.is_empty() {
                    return Err(at("a sorted set member needs at least one part".into()));
                }
            }
            CollectionKind::Buckets { key, widths, .. } => {
                check_key("the key", key)?;
                if widths.is_empty() {
                    return Err(at("buckets need at least one width".into()));
                }
                if widths.len() > MAX_WIDTHS {
                    return Err(at(format!(
                        "{} widths; the most is {MAX_WIDTHS}",
                        widths.len()
                    )));
                }
                let mut seen = BTreeSet::new();
                for w in widths {
                    if w.nanos() == 0 {
                        return Err(at("a width of zero".into()));
                    }
                    if !seen.insert(w.nanos()) {
                        return Err(at(format!("width {} twice", w.label())));
                    }
                }
            }
            CollectionKind::Table {
                columns,
                primary_key,
                indexes,
            } => {
                if columns.is_empty() {
                    return Err(at("a table needs at least one column".into()));
                }
                if columns.len() > MAX_COLUMNS {
                    return Err(at(format!(
                        "{} columns; the most is {MAX_COLUMNS}",
                        columns.len()
                    )));
                }
                let mut seen = BTreeSet::new();
                for c in columns {
                    check_name("column", &c.name)?;
                    if !seen.insert(c.name.as_str()) {
                        return Err(at(format!("column {:?} twice", c.name)));
                    }
                }
                if primary_key.is_empty() {
                    return Err(at("a table needs a primary key".into()));
                }
                if primary_key.len() > MAX_KEY_PARTS {
                    return Err(at(format!(
                        "a primary key of {} columns; the most is {MAX_KEY_PARTS}",
                        primary_key.len()
                    )));
                }
                let mut pk = BTreeSet::new();
                for name in primary_key {
                    let col = columns.iter().find(|c| &c.name == name).ok_or_else(|| {
                        at(format!("primary key column {name:?} is not a column"))
                    })?;
                    if col.ty.key_kind().is_none() {
                        return Err(at(format!(
                            "primary key column {name:?} is not of a key kind"
                        )));
                    }
                    if col.nullable {
                        return Err(at(format!("primary key column {name:?} is nullable")));
                    }
                    if !pk.insert(name.as_str()) {
                        return Err(at(format!("primary key column {name:?} twice")));
                    }
                }
                if indexes.len() > MAX_INDEXES_PER_TABLE {
                    return Err(at(format!(
                        "{} indexes; the most is {MAX_INDEXES_PER_TABLE}",
                        indexes.len()
                    )));
                }
                let mut inames = BTreeSet::new();
                for ix in indexes {
                    check_name("index", &ix.name)?;
                    if !inames.insert(ix.name.as_str()) {
                        return Err(at(format!("index {:?} twice", ix.name)));
                    }
                    if ix.columns.is_empty() {
                        return Err(at(format!("index {:?} has no columns", ix.name)));
                    }
                    if ix.columns.len() > MAX_KEY_PARTS {
                        return Err(at(format!(
                            "index {:?} has {} columns; the most is {MAX_KEY_PARTS}",
                            ix.name,
                            ix.columns.len()
                        )));
                    }
                    let mut icols = BTreeSet::new();
                    for name in &ix.columns {
                        let col = columns.iter().find(|c| &c.name == name).ok_or_else(|| {
                            at(format!(
                                "index {:?} column {name:?} is not a column",
                                ix.name
                            ))
                        })?;
                        if col.ty.key_kind().is_none() {
                            return Err(at(format!(
                                "index {:?} column {name:?} is not of a key kind",
                                ix.name
                            )));
                        }
                        if !icols.insert(name.as_str()) {
                            return Err(at(format!("index {:?} column {name:?} twice", ix.name)));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Checks a value against a schema and returns its canonical form: a
/// string becomes a decimal, UUID or timestamp where the type says so, an
/// integer widens to the declared integer kind or to a float or decimal,
/// a list of small integers becomes bytes.
pub fn canonicalize(ty: &TypeSchema, v: Value) -> Result<Value> {
    let bad = |v: &Value| Error::Validation(format!("expected {ty:?}, got {}", v.type_name()));
    Ok(match (ty, v) {
        (TypeSchema::Any, v) => v,
        (TypeSchema::Bool, v @ Value::Bool(_)) => v,
        (TypeSchema::Int, v @ Value::Int(_)) => v,
        (TypeSchema::Int, Value::Uint(u)) => Value::Int(
            i64::try_from(u).map_err(|_| Error::Validation(format!("{u} does not fit an int")))?,
        ),
        (TypeSchema::Uint, v @ Value::Uint(_)) => v,
        (TypeSchema::Uint, Value::Int(i)) => Value::Uint(
            u64::try_from(i).map_err(|_| Error::Validation(format!("{i} does not fit a uint")))?,
        ),
        (TypeSchema::Float, v @ Value::Float(_)) => v,
        (TypeSchema::Float, Value::Int(i)) => Value::Float(i as f64),
        (TypeSchema::Float, Value::Uint(u)) => Value::Float(u as f64),
        (TypeSchema::Decimal, v @ Value::Decimal(_)) => v,
        (TypeSchema::Decimal, Value::Int(i)) => Value::Decimal(Decimal::from(i)),
        (TypeSchema::Decimal, Value::Uint(u)) => Value::Decimal(Decimal::from(u)),
        (TypeSchema::Decimal, Value::Str(s)) => Value::Decimal(
            s.parse::<Decimal>()
                .map_err(|e| Error::Validation(format!("decimal {s:?}: {e}")))?,
        ),
        (TypeSchema::Str, v @ Value::Str(_)) => v,
        (TypeSchema::Bytes, v @ Value::Bytes(_)) => v,
        (TypeSchema::Bytes, Value::List(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let b = match item {
                    Value::Uint(u) if u < 256 => u as u8,
                    Value::Int(i) if (0..256).contains(&i) => i as u8,
                    other => return Err(bad(&Value::List(vec![other]))),
                };
                out.push(b);
            }
            Value::Bytes(out.into())
        }
        (TypeSchema::Uuid, v @ Value::Uuid(_)) => v,
        (TypeSchema::Uuid, Value::Str(s)) => Value::Uuid(
            Uuid::parse_str(&s).map_err(|e| Error::Validation(format!("uuid {s:?}: {e}")))?,
        ),
        (TypeSchema::Uuid, Value::Bytes(b)) if b.len() == 16 => {
            Value::Uuid(Uuid::from_slice(&b).unwrap())
        }
        (TypeSchema::Timestamp, v @ Value::Timestamp(_)) => v,
        (TypeSchema::Timestamp, Value::Int(i)) => Value::Timestamp(Timestamp(i)),
        (TypeSchema::Timestamp, Value::Uint(u)) => {
            Value::Timestamp(Timestamp(i64::try_from(u).map_err(|_| {
                Error::Validation(format!("{u} does not fit a timestamp"))
            })?))
        }
        (TypeSchema::Timestamp, Value::Str(s)) => Value::Timestamp(
            Timestamp::parse_rfc3339(&s).map_err(|e| Error::Validation(e.to_string()))?,
        ),
        (TypeSchema::List(inner), Value::List(items)) => Value::List(
            items
                .into_iter()
                .map(|i| canonicalize(inner, i))
                .collect::<Result<_>>()?,
        ),
        (TypeSchema::Map(inner), Value::Map(m)) => Value::Map(
            m.into_iter()
                .map(|(k, v)| canonicalize(inner, v).map(|v| (k, v)))
                .collect::<Result<_>>()?,
        ),
        (_, v) => return Err(bad(&v)),
    })
}

/// Checks a value against a value schema.
pub fn check_value(schema: &ValueSchema, v: Value) -> Result<Value> {
    match schema {
        ValueSchema::Any => Ok(v),
        ValueSchema::Typed(ty) => {
            if matches!(v, Value::Null) {
                return Err(Error::Validation(format!(
                    "null where a {ty:?} is declared"
                )));
            }
            canonicalize(ty, v)
        }
    }
}

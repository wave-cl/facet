//! The collections. Each has a write handle (on a [`crate::Writer`]) and
//! a read handle (on a [`crate::Snapshot`]); both are checked against the
//! projection's declaration when opened.

pub mod buckets;
pub mod counter;
pub mod dynr;
pub mod list;
pub mod map;
pub mod register;
mod scan;
pub mod set;
pub mod sorted_set;
pub mod table;

pub use counter::Num;
pub use dynr::{DynR, DynScan, KeyShape};
pub use scan::Scan;
pub use sorted_set::Score;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};
use crate::keyenc::{KeyKind, KeyType};
use crate::limits::{check_key_len, check_value_len};
use crate::schema::{CollectionDecl, ProjectionDecl, ValueSchema, check_value};
use crate::value::{from_value, to_value};
use crate::vbin;

pub(crate) fn find<'d>(decl: &'d ProjectionDecl, name: &str) -> Result<&'d CollectionDecl> {
    decl.collection(name)
        .ok_or_else(|| Error::NotDeclared(name.to_owned()))
}

pub(crate) fn mismatch(
    name: &str,
    declared: impl std::fmt::Display,
    asked: impl std::fmt::Display,
) -> Error {
    Error::KindMismatch {
        name: name.to_owned(),
        declared: declared.to_string(),
        asked: asked.to_string(),
    }
}

pub(crate) fn table_name(collection: &str) -> String {
    format!("c:{collection}")
}

pub(crate) fn kinds_str(kinds: &[KeyKind]) -> String {
    let parts: Vec<&str> = kinds.iter().map(|k| k.name()).collect();
    format!("({})", parts.join(", "))
}

/// Checks a handle's key type against the declared kinds of one key
/// (`what` names it: "key", "member", "partition").
pub(crate) fn check_kinds(
    name: &str,
    kind: &str,
    what: &str,
    declared: &[KeyKind],
    asked: &[KeyKind],
) -> Result<()> {
    if declared != asked {
        return Err(mismatch(
            name,
            format!("{kind} with {what} {}", kinds_str(declared)),
            format!("{kind} with {what} {}", kinds_str(asked)),
        ));
    }
    Ok(())
}

/// `P` must be a leading part of `K`.
pub(crate) fn check_prefix<K: KeyType, P: KeyType>() -> Result<()> {
    let k = K::kinds();
    let p = P::kinds();
    if p.len() > k.len() || k[..p.len()] != p[..] {
        return Err(Error::Key(format!(
            "{} is not a prefix of the key {}",
            kinds_str(&p),
            kinds_str(&k)
        )));
    }
    Ok(())
}

pub(crate) fn encode_key<K: KeyType>(k: &K) -> Result<Vec<u8>> {
    let bytes = k.to_key();
    check_key_len(bytes.len())?;
    Ok(bytes)
}

pub(crate) fn encode_value<V: Serialize + ?Sized>(schema: &ValueSchema, v: &V) -> Result<Vec<u8>> {
    let bytes = vbin::encode(&check_value(schema, to_value(v)?)?);
    check_value_len(bytes.len())?;
    Ok(bytes)
}

pub(crate) fn decode_value<V: DeserializeOwned>(bytes: &[u8]) -> Result<V> {
    from_value(vbin::decode(bytes)?)
}

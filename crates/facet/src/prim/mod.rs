//! The collections.

pub mod map;
pub mod register;

use crate::error::{Error, Result};
use crate::schema::{CollectionDecl, ProjectionDecl};

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

pub(crate) fn kinds_str(kinds: &[crate::KeyKind]) -> String {
    let parts: Vec<&str> = kinds.iter().map(|k| k.name()).collect();
    format!("({})", parts.join(", "))
}

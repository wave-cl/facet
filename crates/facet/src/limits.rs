//! Sizes and counts the engine refuses to exceed. Every limit is checked
//! before anything reaches storage; a violation fails the batch and leaves
//! the previous batch's state in place.

use crate::error::{Error, Result};

/// Longest encoded key, in bytes.
pub const MAX_KEY_BYTES: usize = 1024;
/// Largest encoded value, element or row, in bytes.
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
/// Most parts in one key tuple.
pub const MAX_KEY_PARTS: usize = 16;
/// Most columns in a table.
pub const MAX_COLUMNS: usize = 256;
/// Most secondary indexes on one table.
pub const MAX_INDEXES_PER_TABLE: usize = 16;
/// Most elements one bulk operation takes.
pub const MAX_ELEMENTS_PER_OP: usize = 10_000;
/// Longest collection, column or index name, in bytes.
pub const MAX_NAME_BYTES: usize = 64;
/// Longest projection name, in bytes. A projection name is also a file
/// name and, prefixed with `facet-`, a chron stream name (64 bytes).
pub const MAX_PROJECTION_NAME_BYTES: usize = 48;
/// Most bucket widths on one buckets collection.
pub const MAX_WIDTHS: usize = 8;

/// A collection, column or index name: `[A-Za-z0-9_]`, 1 to
/// [`MAX_NAME_BYTES`] bytes.
pub fn check_name(what: &str, name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Declaration(format!("the {what} name is empty")));
    }
    if name.len() > MAX_NAME_BYTES {
        return Err(Error::Declaration(format!(
            "the {what} name {name:?} is longer than {MAX_NAME_BYTES} bytes"
        )));
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(Error::Declaration(format!(
            "the {what} name {name:?} may only hold letters, digits and underscores"
        )));
    }
    Ok(())
}

/// A projection name: `[a-z0-9_-]`, 1 to [`MAX_PROJECTION_NAME_BYTES`]
/// bytes.
pub fn check_projection_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Declaration("the projection name is empty".into()));
    }
    if name.len() > MAX_PROJECTION_NAME_BYTES {
        return Err(Error::Declaration(format!(
            "the projection name {name:?} is longer than {MAX_PROJECTION_NAME_BYTES} bytes"
        )));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
    {
        return Err(Error::Declaration(format!(
            "the projection name {name:?} may only hold lowercase letters, digits, '_' and '-'"
        )));
    }
    Ok(())
}

pub(crate) fn check_key_len(len: usize) -> Result<()> {
    if len > MAX_KEY_BYTES {
        return Err(Error::Limit(format!(
            "an encoded key is {len} bytes; the most is {MAX_KEY_BYTES}"
        )));
    }
    Ok(())
}

pub(crate) fn check_value_len(len: usize) -> Result<()> {
    if len > MAX_VALUE_BYTES {
        return Err(Error::Limit(format!(
            "an encoded value is {len} bytes; the most is {MAX_VALUE_BYTES}"
        )));
    }
    Ok(())
}

pub(crate) fn check_elements(n: usize) -> Result<()> {
    if n > MAX_ELEMENTS_PER_OP {
        return Err(Error::Limit(format!(
            "{n} elements in one operation; the most is {MAX_ELEMENTS_PER_OP}"
        )));
    }
    Ok(())
}

//! The scan builder every collection's read handle hands out.

use std::marker::PhantomData;

use crate::error::{Error, Result};
use crate::keyenc::{KeyType, prefix_end};
use crate::prim::check_prefix;
use crate::reader::{Cursor, Page, RawScan, Snapshot};

/// Decodes one raw entry into an item.
pub(crate) type Decode<'s, T> = Box<dyn Fn(&[u8], &[u8]) -> Result<T> + 's>;

/// A scan over a collection (or one partition of it), built then run.
/// `K` is the key the bounds are typed by; items come out as `T`.
pub struct Scan<'s, K, T> {
    s: &'s Snapshot,
    table: String,
    /// The fixed leading bytes (a partition, a width); user bounds go
    /// after them.
    base: Vec<u8>,
    raw: RawScan,
    decode: Decode<'s, T>,
    err: Option<Error>,
    _k: PhantomData<fn() -> K>,
}

impl<'s, K: KeyType, T> Scan<'s, K, T> {
    pub(crate) fn new(
        s: &'s Snapshot,
        table: String,
        base: Vec<u8>,
        decode: Decode<'s, T>,
    ) -> Self {
        Scan {
            s,
            table,
            base,
            raw: RawScan::default(),
            decode,
            err: None,
            _k: PhantomData,
        }
    }

    fn with_base(&self, bytes: &[u8]) -> Vec<u8> {
        let mut v = self.base.clone();
        v.extend_from_slice(bytes);
        v
    }

    /// Only keys whose leading parts equal `prefix`.
    pub fn prefix<P: KeyType>(mut self, prefix: &P) -> Self {
        if let Err(e) = check_prefix::<K, P>() {
            self.err = Some(e);
        }
        self.raw.prefix = self.with_base(&prefix.to_key());
        self
    }

    /// Only keys at or above `k`.
    pub fn from(mut self, k: &K) -> Self {
        self.raw.from = Some(self.with_base(&k.to_key()));
        self
    }

    /// Only keys at or below `k`.
    pub fn to(mut self, k: &K) -> Self {
        self.raw.to = Some(self.with_base(&k.to_key()));
        self
    }

    /// Only keys whose leading parts are at or below `p` (every key
    /// starting with `p` is included).
    pub fn to_prefix<P: KeyType>(mut self, p: &P) -> Self {
        if let Err(e) = check_prefix::<K, P>() {
            self.err = Some(e);
        }
        self.raw.to_exclusive = prefix_end(&self.with_base(&p.to_key()));
        if self.raw.to_exclusive.is_none() {
            // Every key with this prefix is at the end of the keyspace.
            self.raw.to = None;
        }
        self
    }

    /// Only keys whose leading parts are at or above `p`.
    pub fn from_prefix<P: KeyType>(mut self, p: &P) -> Self {
        if let Err(e) = check_prefix::<K, P>() {
            self.err = Some(e);
        }
        self.raw.from = Some(self.with_base(&p.to_key()));
        self
    }

    /// Continues after the key a previous page ended with.
    pub fn after(mut self, cursor: &Cursor) -> Self {
        self.raw.after = Some(cursor.0.clone());
        self
    }

    /// Descending order.
    pub fn reverse(mut self) -> Self {
        self.raw.reverse = true;
        self
    }

    /// At most `n` items (0 = all).
    pub fn limit(mut self, n: usize) -> Self {
        self.raw.limit = n;
        self
    }

    fn prepared(mut self) -> Result<Self> {
        if let Some(e) = self.err.take() {
            return Err(e);
        }
        if self.raw.prefix.is_empty() {
            self.raw.prefix = self.base.clone();
        }
        Ok(self)
    }

    /// Runs the scan.
    pub fn run(self) -> Result<Page<T>> {
        let me = self.prepared()?;
        let (items, next) = me.raw.run(me.s.raw_table(&me.table)?.as_deref())?;
        let mut out = Vec::with_capacity(items.len());
        for (k, v) in items {
            out.push((me.decode)(&k, &v)?);
        }
        Ok(Page {
            items: out,
            next,
            checkpoint: me.s.checkpoint(),
        })
    }

    /// Counts the matching keys, stopping at `max` (the flag says
    /// whether it stopped). Decodes nothing.
    pub fn count(self, max: u64) -> Result<(u64, bool)> {
        let mut me = self.prepared()?;
        me.raw.limit = usize::try_from(max).unwrap_or(usize::MAX);
        let (items, next) = me.raw.run(me.s.raw_table(&me.table)?.as_deref())?;
        Ok((items.len() as u64, next.is_some()))
    }

    /// The raw encoded entries, for generic tooling.
    pub fn run_raw(self) -> Result<Page<(Vec<u8>, Vec<u8>)>> {
        let me = self.prepared()?;
        let (items, next) = me.raw.run(me.s.raw_table(&me.table)?.as_deref())?;
        Ok(Page {
            items,
            next,
            checkpoint: me.s.checkpoint(),
        })
    }
}

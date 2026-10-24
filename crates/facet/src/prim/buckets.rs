//! Buckets: count, sum, min and max per key and time window, kept for
//! every declared width.
//!
//! Table `c:<name>`: `enc(key) ‖ width index u8 ‖ window start (i64
//! nanos, sign-flipped) -> count u64 ‖ sum ‖ min ‖ max` (each number in
//! the counter's encoding).

use std::marker::PhantomData;

use redb::ReadableTable;

use crate::error::{Error, Result};
use crate::keyenc::{KeyType, encode_i64};
use crate::prim::counter::Num;
use crate::prim::{Scan, check_kinds, encode_key, find, mismatch, table_name};
use crate::reader::Snapshot;
use crate::schema::{CollectionKind, Width};
use crate::writer::Writer;

/// One window's aggregate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket<N> {
    /// The window's start, unix nanoseconds.
    pub start: i64,
    /// How many values fell in it.
    pub count: u64,
    /// Their sum.
    pub sum: N,
    /// The smallest.
    pub min: N,
    /// The largest.
    pub max: N,
}

impl<N: Num> Bucket<N> {
    fn encode(&self) -> Vec<u8> {
        let mut out = self.count.to_be_bytes().to_vec();
        out.extend(self.sum.encode());
        out.extend(self.min.encode());
        out.extend(self.max.encode());
        out
    }

    /// Decodes a stored bucket.
    pub fn decode_bytes(start: i64, b: &[u8]) -> Result<Bucket<N>> {
        Self::decode(start, b)
    }

    fn decode(start: i64, b: &[u8]) -> Result<Bucket<N>> {
        if b.len() < 8 || !(b.len() - 8).is_multiple_of(3) {
            return Err(Error::Value("bucket bytes".into()));
        }
        let n = (b.len() - 8) / 3;
        Ok(Bucket {
            start,
            count: u64::from_be_bytes(b[..8].try_into().unwrap()),
            sum: N::decode(&b[8..8 + n])?,
            min: N::decode(&b[8 + n..8 + 2 * n])?,
            max: N::decode(&b[8 + 2 * n..])?,
        })
    }
}

fn open<K: KeyType, N: Num>(
    decl: &crate::schema::ProjectionDecl,
    name: &str,
) -> Result<Vec<Width>> {
    let c = find(decl, name)?;
    match &c.kind {
        CollectionKind::Buckets { key, num, widths } => {
            check_kinds(name, "buckets", "key", &key.0, &K::kinds())?;
            if *num != N::KIND {
                return Err(mismatch(
                    name,
                    format!("buckets of {num:?}"),
                    format!("buckets of {:?}", N::KIND),
                ));
            }
            Ok(widths.clone())
        }
        other => Err(mismatch(name, other.name(), "buckets")),
    }
}

fn width_index(widths: &[Width], w: Width) -> Result<u8> {
    widths
        .iter()
        .position(|x| x.nanos() == w.nanos())
        .map(|i| i as u8)
        .ok_or_else(|| Error::Validation(format!("width {} is not declared", w.label())))
}

/// The start of the window of `width` holding `at`, rounding towards
/// negative infinity.
pub fn window_start(at: i64, width: Width) -> i64 {
    let w = i128::from(width.nanos());
    let at = i128::from(at);
    let start = at.div_euclid(w) * w;
    i64::try_from(start).unwrap_or(i64::MIN)
}

fn bucket_key(key: &[u8], width_index: u8, start: i64) -> Vec<u8> {
    let mut k = key.to_vec();
    k.push(width_index);
    encode_i64(start, &mut k);
    k
}

/// Buckets inside a batch.
pub struct BucketsW<'w, 't, K, N> {
    w: &'w mut Writer<'t>,
    name: String,
    table: String,
    widths: Vec<Width>,
    _k: PhantomData<(K, N)>,
}

impl<'w, 't, K: KeyType, N: Num> BucketsW<'w, 't, K, N> {
    pub(crate) fn open(w: &'w mut Writer<'t>, name: &str) -> Result<Self> {
        let widths = open::<K, N>(w.decl, name)?;
        Ok(BucketsW {
            w,
            name: name.to_owned(),
            table: table_name(name),
            widths,
            _k: PhantomData,
        })
    }

    /// Adds `by` to the window of every width that holds `at`.
    pub fn add(&mut self, k: &K, at: i64, by: N) -> Result<()> {
        let key = encode_key(k)?;
        for (i, w) in self.widths.clone().iter().enumerate() {
            let start = window_start(at, *w);
            let bk = bucket_key(&key, i as u8, start);
            let t = self.w.raw_table(&self.table)?;
            let mut b = match t.get(bk.as_slice())? {
                Some(g) => Bucket::<N>::decode(start, g.value())?,
                None => Bucket {
                    start,
                    count: 0,
                    sum: N::zero(),
                    min: by,
                    max: by,
                },
            };
            b.count += 1;
            b.sum = b.sum.checked_add(by).ok_or_else(|| {
                Error::Overflow(format!("buckets {}: {:?} + {by:?}", self.name, b.sum))
            })?;
            if by < b.min {
                b.min = by;
            }
            if by > b.max {
                b.max = by;
            }
            t.insert(bk.as_slice(), b.encode().as_slice())?;
            self.w.changes.touch(&self.name, &bk);
        }
        self.w.changes.touch(&self.name, &key);
        Ok(())
    }

    /// The window of `width` starting at `start`, as the batch sees it.
    pub fn get(&mut self, k: &K, width: Width, start: i64) -> Result<Option<Bucket<N>>> {
        let bk = bucket_key(&encode_key(k)?, width_index(&self.widths, width)?, start);
        let t = self.w.raw_table(&self.table)?;
        match t.get(bk.as_slice())? {
            Some(g) => Ok(Some(Bucket::decode(start, g.value())?)),
            None => Ok(None),
        }
    }
}

/// Buckets in a snapshot.
pub struct BucketsR<'s, K, N> {
    s: &'s Snapshot,
    table: String,
    widths: Vec<Width>,
    _k: PhantomData<(K, N)>,
}

impl<'s, K: KeyType, N: Num> BucketsR<'s, K, N> {
    pub(crate) fn open(s: &'s Snapshot, name: &str) -> Result<Self> {
        let widths = open::<K, N>(&s.decl, name)?;
        Ok(BucketsR {
            s,
            table: table_name(name),
            widths,
            _k: PhantomData,
        })
    }

    /// The declared widths.
    pub fn widths(&self) -> &[Width] {
        &self.widths
    }

    /// The window of `width` holding `at`.
    pub fn at(&self, k: &K, width: Width, at: i64) -> Result<Option<Bucket<N>>> {
        self.get(k, width, window_start(at, width))
    }

    /// The window of `width` starting at `start`.
    pub fn get(&self, k: &K, width: Width, start: i64) -> Result<Option<Bucket<N>>> {
        let bk = bucket_key(&encode_key(k)?, width_index(&self.widths, width)?, start);
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(None);
        };
        match t.get(bk.as_slice())? {
            Some(g) => Ok(Some(Bucket::decode(start, g.value())?)),
            None => Ok(None),
        }
    }

    /// The windows of one key and width in time order. Bound them with
    /// `from(&(start,))` and `to(&(start,))`.
    pub fn range(&self, k: &K, width: Width) -> Result<Scan<'s, (i64,), Bucket<N>>> {
        let mut base = encode_key(k)?;
        base.push(width_index(&self.widths, width)?);
        let base_len = base.len();
        Ok(Scan::new(
            self.s,
            self.table.clone(),
            base,
            Box::new(move |key, v| {
                let (start,) = <(i64,)>::from_key(&key[base_len..])?;
                Bucket::decode(start, v)
            }),
        ))
    }
}

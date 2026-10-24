//! Sorted sets: members ranked by a score, per partition.
//!
//! Table `c:<name>`: `enc(partition) ‖ enc(member) -> enc(score)`; table
//! `c:<name>:s`: `enc(partition) ‖ enc(score) ‖ enc(member) -> ()`.

use std::marker::PhantomData;

use redb::{ReadableTable, ReadableTableMetadata};
use rust_decimal::Decimal;

use crate::error::Result;
use crate::keyenc::KeyType;
use crate::prim::{Scan, check_kinds, encode_key, find, mismatch, table_name};
use crate::reader::{Page, Snapshot};
use crate::schema::{CollectionKind, ScoreKind};
use crate::value::Timestamp;
use crate::writer::Writer;

/// A score: a key kind that is a number or a time.
pub trait Score: KeyType + Copy + PartialOrd + std::fmt::Debug + Send + Sync + 'static {
    /// The declared kind this type matches.
    const KIND: ScoreKind;
}

impl Score for i64 {
    const KIND: ScoreKind = ScoreKind::Int;
}
impl Score for u64 {
    const KIND: ScoreKind = ScoreKind::Uint;
}
impl Score for Decimal {
    const KIND: ScoreKind = ScoreKind::Decimal;
}
impl Score for Timestamp {
    const KIND: ScoreKind = ScoreKind::Timestamp;
}

fn open<P: KeyType, M: KeyType, S: Score>(
    decl: &crate::schema::ProjectionDecl,
    name: &str,
) -> Result<()> {
    let c = find(decl, name)?;
    match &c.kind {
        CollectionKind::SortedSet {
            partition,
            member,
            score,
        } => {
            check_kinds(name, "sorted set", "partition", &partition.0, &P::kinds())?;
            check_kinds(name, "sorted set", "member", &member.0, &M::kinds())?;
            if *score != S::KIND {
                return Err(mismatch(
                    name,
                    format!("sorted set scored by {score:?}"),
                    format!("sorted set scored by {:?}", S::KIND),
                ));
            }
            Ok(())
        }
        other => Err(mismatch(name, other.name(), "sorted set")),
    }
}

fn score_table(collection: &str) -> String {
    format!("c:{collection}:s")
}

fn join(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(a.len() + b.len());
    v.extend_from_slice(a);
    v.extend_from_slice(b);
    v
}

/// A sorted set inside a batch.
pub struct SortedSetW<'w, 't, P, M, S> {
    w: &'w mut Writer<'t>,
    name: String,
    table: String,
    scores: String,
    _p: PhantomData<(P, M, S)>,
}

impl<'w, 't, P: KeyType, M: KeyType, S: Score> SortedSetW<'w, 't, P, M, S> {
    pub(crate) fn open(w: &'w mut Writer<'t>, name: &str) -> Result<Self> {
        open::<P, M, S>(w.decl, name)?;
        Ok(SortedSetW {
            w,
            name: name.to_owned(),
            table: table_name(name),
            scores: score_table(name),
            _p: PhantomData,
        })
    }

    /// Sets the member's score (adding the member); returns the old
    /// score.
    pub fn put(&mut self, p: &P, m: &M, score: S) -> Result<Option<S>> {
        let partition = encode_key(p)?;
        let member = encode_key(m)?;
        let key = join(&partition, &member);
        let new_score = score.to_key();
        let old = {
            let t = self.w.raw_table(&self.table)?;
            let old = t.get(key.as_slice())?.map(|g| g.value().to_vec());
            t.insert(key.as_slice(), new_score.as_slice())?;
            old
        };
        let st = self.w.raw_table(&self.scores)?;
        if let Some(old) = &old {
            let old_key = join(&join(&partition, old), &member);
            st.remove(old_key.as_slice())?;
        }
        let new_key = join(&join(&partition, &new_score), &member);
        st.insert(new_key.as_slice(), &b""[..])?;
        self.w.changes.touch(&self.name, &partition);
        self.w.changes.touch(&self.name, &key);
        old.map(|b| S::from_key(&b)).transpose()
    }

    /// Removes the member; returns its score if it was there.
    pub fn remove(&mut self, p: &P, m: &M) -> Result<Option<S>> {
        let partition = encode_key(p)?;
        let member = encode_key(m)?;
        let key = join(&partition, &member);
        let old = self
            .w
            .raw_table(&self.table)?
            .remove(key.as_slice())?
            .map(|g| g.value().to_vec());
        if let Some(old) = &old {
            let old_key = join(&join(&partition, old), &member);
            self.w.raw_table(&self.scores)?.remove(old_key.as_slice())?;
            self.w.changes.touch(&self.name, &partition);
            self.w.changes.touch(&self.name, &key);
        }
        old.map(|b| S::from_key(&b)).transpose()
    }

    /// The member's score, as the batch sees it.
    pub fn score(&mut self, p: &P, m: &M) -> Result<Option<S>> {
        let key = join(&encode_key(p)?, &encode_key(m)?);
        let t = self.w.raw_table(&self.table)?;
        match t.get(key.as_slice())? {
            Some(g) => Ok(Some(S::from_key(g.value())?)),
            None => Ok(None),
        }
    }
}

/// A sorted set in a snapshot.
pub struct SortedSetR<'s, P, M, S> {
    s: &'s Snapshot,
    table: String,
    scores: String,
    _p: PhantomData<(P, M, S)>,
}

impl<'s, P: KeyType, M: KeyType, S: Score> SortedSetR<'s, P, M, S> {
    pub(crate) fn open(s: &'s Snapshot, name: &str) -> Result<Self> {
        open::<P, M, S>(&s.decl, name)?;
        Ok(SortedSetR {
            s,
            table: table_name(name),
            scores: score_table(name),
            _p: PhantomData,
        })
    }

    /// The member's score.
    pub fn score(&self, p: &P, m: &M) -> Result<Option<S>> {
        let key = join(&encode_key(p)?, &encode_key(m)?);
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(None);
        };
        match t.get(key.as_slice())? {
            Some(g) => Ok(Some(S::from_key(g.value())?)),
            None => Ok(None),
        }
    }

    /// The number of members over every partition.
    pub fn len(&self) -> Result<u64> {
        Ok(match self.s.raw_table(&self.table)? {
            Some(t) => t.len()?,
            None => 0,
        })
    }

    /// Whether there are no members.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// The members of one partition in member order, with their scores.
    pub fn members(&self, p: &P) -> Result<Scan<'s, M, (M, S)>> {
        let partition = encode_key(p)?;
        let base_len = partition.len();
        Ok(Scan::new(
            self.s,
            self.table.clone(),
            partition,
            Box::new(move |k, v| Ok((M::from_key(&k[base_len..])?, S::from_key(v)?))),
        ))
    }

    /// The members of one partition in score order (ties by member),
    /// as `(member, score)`. Bound it with `from_prefix(&(min,))` and
    /// `to_prefix(&(max,))`; the top `n` are `.reverse().limit(n)`.
    pub fn by_score(&self, p: &P) -> Result<Scan<'s, (S, M), (M, S)>> {
        let partition = encode_key(p)?;
        let base_len = partition.len();
        Ok(Scan::new(
            self.s,
            self.scores.clone(),
            partition,
            Box::new(move |k, _| {
                let (s, m) = <(S, M)>::from_key(&k[base_len..])?;
                Ok((m, s))
            }),
        ))
    }

    /// The `n` highest-scored members, highest first.
    pub fn top(&self, p: &P, n: usize) -> Result<Page<(M, S)>> {
        self.by_score(p)?.reverse().limit(n).run()
    }

    /// The member's rank in ascending score order (0 = lowest), walking
    /// at most `max_scan` entries; `None` when absent or past the cap.
    pub fn rank(&self, p: &P, m: &M, max_scan: u64) -> Result<Option<u64>> {
        let Some(score) = self.score(p, m)? else {
            return Ok(None);
        };
        let target = join(&score.to_key(), &encode_key(m)?);
        let page = self
            .by_score(p)?
            .limit(usize::try_from(max_scan).unwrap_or(usize::MAX))
            .run_raw()?;
        let base_len = encode_key(p)?.len();
        Ok(page
            .items
            .iter()
            .position(|(k, _)| k[base_len..] == target[..])
            .map(|i| i as u64))
    }
}

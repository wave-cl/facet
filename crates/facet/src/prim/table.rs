//! Tables: rows with typed columns, a primary key and secondary indexes.
//!
//! Table `c:<name>`: `enc(primary key columns) -> vbin(list of the other
//! columns in declared order)`; table `c:<name>:i:<index>`: `enc(index
//! columns, nullable ones wrapped) ‖ enc(primary key) -> ()`, or for a
//! unique index `enc(index columns) -> enc(primary key)`.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use redb::{ReadableTable, ReadableTableMetadata};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};
use crate::keyenc::{KeyKind, KeyPart, KeyType, decode_parts, decode_parts_nullable};
use crate::limits::{check_key_len, check_value_len};
use crate::prim::{Scan, check_kinds, find, kinds_str, mismatch, table_name};
use crate::reader::Snapshot;
use crate::schema::{CollectionDecl, CollectionKind, ColumnDecl, canonicalize};
use crate::value::{Value, from_value, to_value};
use crate::vbin;
use crate::writer::Writer;

struct IndexMeta {
    name: String,
    table: String,
    cols: Vec<usize>,
    kinds: Vec<KeyKind>,
    nullable: Vec<bool>,
    unique: bool,
}

/// The declaration, resolved once per handle.
pub(crate) struct Meta {
    name: String,
    table: String,
    pub(crate) columns: Vec<ColumnDecl>,
    pk: Vec<usize>,
    pk_kinds: Vec<KeyKind>,
    non_pk: Vec<usize>,
    indexes: Vec<IndexMeta>,
}

fn open<K: KeyType>(decl: &crate::schema::ProjectionDecl, name: &str) -> Result<Arc<Meta>> {
    let c = find(decl, name)?;
    let meta = open_meta(c)?;
    check_kinds(name, "table", "primary key", &meta.pk_kinds, &K::kinds())?;
    Ok(meta)
}

/// Resolves a table declaration.
pub(crate) fn open_meta(c: &CollectionDecl) -> Result<Arc<Meta>> {
    let name = c.name.as_str();
    let CollectionKind::Table {
        columns,
        primary_key,
        indexes,
    } = &c.kind
    else {
        return Err(mismatch(name, c.kind.name(), "table"));
    };
    let col_index = |n: &str| columns.iter().position(|c| c.name == n).expect("validated");
    let pk: Vec<usize> = primary_key.iter().map(|n| col_index(n)).collect();
    let pk_kinds: Vec<KeyKind> = pk
        .iter()
        .map(|&i| columns[i].ty.key_kind().expect("validated"))
        .collect();
    let non_pk: Vec<usize> = (0..columns.len()).filter(|i| !pk.contains(i)).collect();
    let indexes = indexes
        .iter()
        .map(|ix| {
            let cols: Vec<usize> = ix.columns.iter().map(|n| col_index(n)).collect();
            IndexMeta {
                name: ix.name.clone(),
                table: format!("c:{name}:i:{}", ix.name),
                kinds: cols
                    .iter()
                    .map(|&i| columns[i].ty.key_kind().expect("validated"))
                    .collect(),
                nullable: cols.iter().map(|&i| columns[i].nullable).collect(),
                cols,
                unique: ix.unique,
            }
        })
        .collect();
    Ok(Arc::new(Meta {
        name: name.to_owned(),
        table: table_name(name),
        columns: columns.clone(),
        pk,
        pk_kinds,
        non_pk,
        indexes,
    }))
}

impl Meta {
    /// A row's column values in declared order, validated and canonical.
    fn values_of<R: Serialize>(&self, row: &R) -> Result<Vec<Value>> {
        let Value::Map(mut m) = to_value(row)? else {
            return Err(Error::Validation(format!(
                "table {}: a row must serialise as a map",
                self.name
            )));
        };
        let mut values = Vec::with_capacity(self.columns.len());
        for c in &self.columns {
            let v = m.remove(&c.name).unwrap_or(Value::Null);
            if matches!(v, Value::Null) {
                if !c.nullable {
                    return Err(Error::Validation(format!(
                        "table {}: column {} is null",
                        self.name, c.name
                    )));
                }
                values.push(Value::Null);
            } else {
                values.push(canonicalize(&c.ty, v).map_err(|e| {
                    Error::Validation(format!("table {}: column {}: {e}", self.name, c.name))
                })?);
            }
        }
        if let Some((extra, _)) = m.into_iter().next() {
            return Err(Error::Validation(format!(
                "table {}: {extra:?} is not a column",
                self.name
            )));
        }
        Ok(values)
    }

    fn pk_bytes(&self, values: &[Value]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        for (&i, &kind) in self.pk.iter().zip(&self.pk_kinds) {
            KeyPart::from_value(kind, &values[i])?.encode(&mut out);
        }
        check_key_len(out.len())?;
        Ok(out)
    }

    fn non_pk_bytes(&self, values: &[Value]) -> Result<Vec<u8>> {
        let list: Vec<Value> = self.non_pk.iter().map(|&i| values[i].clone()).collect();
        let bytes = vbin::encode(&Value::List(list));
        check_value_len(bytes.len())?;
        Ok(bytes)
    }

    fn index_prefix(&self, ix: &IndexMeta, values: &[Value]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        for ((&i, &kind), &nullable) in ix.cols.iter().zip(&ix.kinds).zip(&ix.nullable) {
            let part = KeyPart::from_value(kind, &values[i])?;
            if nullable {
                part.encode_nullable(&mut out);
            } else {
                part.encode(&mut out);
            }
        }
        Ok(out)
    }

    /// The column values from a stored entry.
    pub(crate) fn values_from(&self, pk: &[u8], non_pk: &[u8]) -> Result<Vec<Value>> {
        let (parts, used) = decode_parts(&self.pk_kinds, pk)?;
        if used != pk.len() {
            return Err(Error::Key("trailing bytes after the primary key".into()));
        }
        let Value::List(rest) = vbin::decode(non_pk)? else {
            return Err(Error::Value("a row must decode as a list".into()));
        };
        if rest.len() != self.non_pk.len() {
            return Err(Error::Value(format!(
                "table {}: a row has {} columns, the declaration {}",
                self.name,
                rest.len(),
                self.non_pk.len()
            )));
        }
        let mut values = vec![Value::Null; self.columns.len()];
        for (&i, p) in self.pk.iter().zip(parts.0) {
            values[i] = p.to_value();
        }
        for (&i, v) in self.non_pk.iter().zip(rest) {
            values[i] = v;
        }
        Ok(values)
    }

    fn row_from<R: DeserializeOwned>(&self, values: Vec<Value>) -> Result<R> {
        let m: BTreeMap<String, Value> = self
            .columns
            .iter()
            .zip(values)
            .map(|(c, v)| (c.name.clone(), v))
            .collect();
        from_value(Value::Map(m))
    }

    fn index(&self, name: &str) -> Result<&IndexMeta> {
        self.indexes
            .iter()
            .find(|ix| ix.name == name)
            .ok_or_else(|| Error::NotDeclared(format!("{}.{name}", self.name)))
    }
}

/// A table inside a batch.
pub struct TableW<'w, 't, K, R> {
    w: &'w mut Writer<'t>,
    meta: Arc<Meta>,
    _k: PhantomData<(K, R)>,
}

impl<'w, 't, K: KeyType, R> TableW<'w, 't, K, R> {
    pub(crate) fn open(w: &'w mut Writer<'t>, name: &str) -> Result<Self> {
        let meta = open::<K>(w.decl, name)?;
        Ok(TableW {
            w,
            meta,
            _k: PhantomData,
        })
    }

    fn stored_values(&mut self, pk: &[u8]) -> Result<Option<Vec<Value>>> {
        let meta = self.meta.clone();
        let t = self.w.raw_table(&meta.table)?;
        match t.get(pk)? {
            Some(g) => Ok(Some(meta.values_from(pk, g.value())?)),
            None => Ok(None),
        }
    }

    fn drop_index_entries(&mut self, pk: &[u8], values: &[Value]) -> Result<()> {
        let meta = self.meta.clone();
        for ix in &meta.indexes {
            let mut key = meta.index_prefix(ix, values)?;
            if !ix.unique {
                key.extend_from_slice(pk);
            }
            self.w.raw_table(&ix.table)?.remove(key.as_slice())?;
        }
        Ok(())
    }

    fn add_index_entries(&mut self, pk: &[u8], values: &[Value]) -> Result<()> {
        let meta = self.meta.clone();
        for ix in &meta.indexes {
            let mut key = meta.index_prefix(ix, values)?;
            if ix.unique {
                let t = self.w.raw_table(&ix.table)?;
                if let Some(g) = t.get(key.as_slice())?
                    && g.value() != pk
                {
                    let shown: Vec<String> = ix
                        .cols
                        .iter()
                        .map(|&i| values[i].to_json().to_string())
                        .collect();
                    return Err(Error::UniqueViolation {
                        index: format!("{}.{}", meta.name, ix.name),
                        key: shown.join(", "),
                    });
                }
                t.insert(key.as_slice(), pk)?;
            } else {
                key.extend_from_slice(pk);
                check_key_len(key.len())?;
                self.w
                    .raw_table(&ix.table)?
                    .insert(key.as_slice(), &b""[..])?;
            }
        }
        Ok(())
    }

    /// Inserts or replaces the row, maintaining the indexes.
    pub fn upsert(&mut self, row: &R) -> Result<()>
    where
        R: Serialize,
    {
        let meta = self.meta.clone();
        let values = meta.values_of(row)?;
        let pk = meta.pk_bytes(&values)?;
        let body = meta.non_pk_bytes(&values)?;
        if let Some(old) = self.stored_values(&pk)? {
            self.drop_index_entries(&pk, &old)?;
        }
        self.add_index_entries(&pk, &values)?;
        self.w
            .raw_table(&meta.table)?
            .insert(pk.as_slice(), body.as_slice())?;
        self.w.changes.touch(&meta.name, &pk);
        Ok(())
    }

    /// Removes the row with that key; `true` when there was one.
    pub fn delete(&mut self, k: &K) -> Result<bool> {
        let meta = self.meta.clone();
        let pk = k.to_key();
        let Some(old) = self.stored_values(&pk)? else {
            return Ok(false);
        };
        self.drop_index_entries(&pk, &old)?;
        self.w.raw_table(&meta.table)?.remove(pk.as_slice())?;
        self.w.changes.touch(&meta.name, &pk);
        Ok(true)
    }

    /// The row with that key, as the batch sees it.
    pub fn get(&mut self, k: &K) -> Result<Option<R>>
    where
        R: DeserializeOwned,
    {
        let meta = self.meta.clone();
        match self.stored_values(&k.to_key())? {
            Some(values) => Ok(Some(meta.row_from(values)?)),
            None => Ok(None),
        }
    }

    /// Whether a row with that key exists.
    pub fn contains(&mut self, k: &K) -> Result<bool> {
        let meta = self.meta.clone();
        let pk = k.to_key();
        Ok(self.w.raw_table(&meta.table)?.get(pk.as_slice())?.is_some())
    }

    /// Reads, maps and writes back: `None` from `f` deletes the row.
    pub fn update(&mut self, k: &K, f: impl FnOnce(Option<R>) -> Option<R>) -> Result<Option<R>>
    where
        R: Serialize + DeserializeOwned,
    {
        let current = self.get(k)?;
        match f(current) {
            Some(row) => {
                self.upsert(&row)?;
                Ok(Some(row))
            }
            None => {
                self.delete(k)?;
                Ok(None)
            }
        }
    }
}

/// A table in a snapshot.
pub struct TableR<'s, K, R> {
    s: &'s Snapshot,
    meta: Arc<Meta>,
    _k: PhantomData<(K, R)>,
}

impl<'s, K: KeyType, R> TableR<'s, K, R> {
    pub(crate) fn open(s: &'s Snapshot, name: &str) -> Result<Self> {
        let meta = open::<K>(&s.decl, name)?;
        Ok(TableR {
            s,
            meta,
            _k: PhantomData,
        })
    }

    /// The row with that key.
    pub fn get(&self, k: &K) -> Result<Option<R>>
    where
        R: DeserializeOwned,
    {
        let pk = k.to_key();
        let Some(t) = self.s.raw_table(&self.meta.table)? else {
            return Ok(None);
        };
        match t.get(pk.as_slice())? {
            Some(g) => Ok(Some(
                self.meta.row_from(self.meta.values_from(&pk, g.value())?)?,
            )),
            None => Ok(None),
        }
    }

    /// The row's column values, by column name.
    pub fn get_values(&self, k: &K) -> Result<Option<BTreeMap<String, Value>>> {
        let pk = k.to_key();
        let Some(t) = self.s.raw_table(&self.meta.table)? else {
            return Ok(None);
        };
        match t.get(pk.as_slice())? {
            Some(g) => {
                let values = self.meta.values_from(&pk, g.value())?;
                Ok(Some(
                    self.meta
                        .columns
                        .iter()
                        .zip(values)
                        .map(|(c, v)| (c.name.clone(), v))
                        .collect(),
                ))
            }
            None => Ok(None),
        }
    }

    /// Whether a row with that key exists.
    pub fn contains(&self, k: &K) -> Result<bool> {
        let pk = k.to_key();
        let Some(t) = self.s.raw_table(&self.meta.table)? else {
            return Ok(false);
        };
        Ok(t.get(pk.as_slice())?.is_some())
    }

    /// The number of rows.
    pub fn len(&self) -> Result<u64> {
        Ok(match self.s.raw_table(&self.meta.table)? {
            Some(t) => t.len()?,
            None => 0,
        })
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// A scan over the rows in primary key order.
    pub fn scan(&self) -> Scan<'s, K, R>
    where
        R: DeserializeOwned,
    {
        let meta = self.meta.clone();
        Scan::new(
            self.s,
            meta.table.clone(),
            Vec::new(),
            Box::new(move |k, v| meta.row_from(meta.values_from(k, v)?)),
        )
    }

    /// The rows in the order of a secondary index, typed by the index's
    /// key `I` (nullable columns as `Option`). Narrow with `prefix`,
    /// `from_prefix` and `to_prefix`.
    pub fn by_index<I: KeyType>(&self, index: &str) -> Result<Scan<'s, I, R>>
    where
        R: DeserializeOwned,
    {
        let ix = self.meta.index(index)?;
        if ix.kinds != I::kinds() || ix.nullable != I::nullable() {
            let shape = |kinds: &[KeyKind], nullable: &[bool]| {
                let parts: Vec<String> = kinds
                    .iter()
                    .zip(nullable)
                    .map(|(k, &n)| if n { format!("{k}?") } else { k.to_string() })
                    .collect();
                format!("index ({})", parts.join(", "))
            };
            return Err(mismatch(
                &format!("{}.{index}", self.meta.name),
                shape(&ix.kinds, &ix.nullable),
                shape(&I::kinds(), &I::nullable()),
            ));
        }
        let meta = self.meta.clone();
        let s = self.s;
        let unique = ix.unique;
        let kinds = ix.kinds.clone();
        let nullable = ix.nullable.clone();
        let index_table = ix.table.clone();
        Ok(Scan::new(
            s,
            index_table,
            Vec::new(),
            Box::new(move |k, v| {
                let pk: &[u8] = if unique {
                    v
                } else {
                    let (_, used) = decode_parts_nullable(&kinds, &nullable, k)?;
                    &k[used..]
                };
                let t = s.raw_table(&meta.table)?.ok_or_else(|| Error::Corrupt {
                    file: meta.name.clone(),
                    reason: "an index entry without a table".into(),
                })?;
                let g = t.get(pk)?.ok_or_else(|| Error::Corrupt {
                    file: meta.name.clone(),
                    reason: format!(
                        "an index entry points at a missing row (pk {})",
                        kinds_str(&meta.pk_kinds)
                    ),
                })?;
                meta.row_from(meta.values_from(pk, g.value())?)
            }),
        ))
    }
}

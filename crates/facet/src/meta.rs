//! The `meta` table every projection file carries, and the checkpoint.

use redb::{TableDefinition, TableHandle};
use uuid::Uuid;

use crate::error::{Error, Result};

pub(crate) const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// The file format this engine writes. A file of another format is
/// rebuilt.
pub const FORMAT: u32 = 1;

pub(crate) const K_FORMAT: &str = "format";
pub(crate) const K_DB_ID: &str = "db_id";
pub(crate) const K_PROJECTION: &str = "projection";
pub(crate) const K_VERSION: &str = "version";
pub(crate) const K_DECL: &str = "decl";
pub(crate) const K_CHECKPOINT: &str = "checkpoint";
pub(crate) const K_CREATED_AT: &str = "created_at";

/// The last event a projection applied: its position and a fingerprint
/// of its content, so a log that changed under the projection is noticed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Mark {
    /// The event's global position.
    pub position: u64,
    /// A crc32 the runtime computes over the event.
    pub fingerprint: u32,
}

/// Where a projection stands: the next position to apply and the mark of
/// the last one applied, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Checkpoint {
    /// The next global position to apply.
    pub next: u64,
    /// The last event applied, `None` when nothing was applied yet.
    pub last: Option<Mark>,
}

impl Checkpoint {
    /// A checkpoint at `next` after applying the event at `position`
    /// with that fingerprint.
    pub fn after(position: u64, fingerprint: u32) -> Checkpoint {
        Checkpoint {
            next: position + 1,
            last: Some(Mark {
                position,
                fingerprint,
            }),
        }
    }

    pub(crate) fn encode(&self) -> [u8; 21] {
        let mut out = [0u8; 21];
        out[1..9].copy_from_slice(&self.next.to_be_bytes());
        if let Some(m) = self.last {
            out[0] = 1;
            out[9..17].copy_from_slice(&m.position.to_be_bytes());
            out[17..21].copy_from_slice(&m.fingerprint.to_be_bytes());
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Checkpoint> {
        if bytes.len() != 21 || bytes[0] > 1 {
            return Err(Error::Value("checkpoint bytes".into()));
        }
        let next = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        let last = (bytes[0] == 1).then(|| Mark {
            position: u64::from_be_bytes(bytes[9..17].try_into().unwrap()),
            fingerprint: u32::from_be_bytes(bytes[17..21].try_into().unwrap()),
        });
        Ok(Checkpoint { next, last })
    }
}

/// What a file's meta table says.
#[derive(Debug, Clone)]
pub(crate) struct FileMeta {
    pub format: u32,
    pub db_id: Uuid,
    pub projection: String,
    pub version: String,
    pub decl: Vec<u8>,
    pub checkpoint: Checkpoint,
}

fn corrupt(file: &str, reason: impl Into<String>) -> Error {
    Error::Corrupt {
        file: file.to_owned(),
        reason: reason.into(),
    }
}

/// Reads the meta table; `None` when the file was never initialised.
pub(crate) fn read_meta(txn: &redb::ReadTransaction, file: &str) -> Result<Option<FileMeta>> {
    let table = match txn.open_table(META) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let get =
        |key: &str| -> Result<Option<Vec<u8>>> { Ok(table.get(key)?.map(|g| g.value().to_vec())) };
    let Some(format) = get(K_FORMAT)? else {
        return Ok(None);
    };
    let need = |key: &str| -> Result<Vec<u8>> {
        get(key)?.ok_or_else(|| corrupt(file, format!("meta has no {key}")))
    };
    let format = u32::from_be_bytes(
        format
            .as_slice()
            .try_into()
            .map_err(|_| corrupt(file, "format bytes"))?,
    );
    let db_id = Uuid::from_slice(&need(K_DB_ID)?).map_err(|_| corrupt(file, "db_id bytes"))?;
    let projection =
        String::from_utf8(need(K_PROJECTION)?).map_err(|_| corrupt(file, "projection name"))?;
    let version = String::from_utf8(need(K_VERSION)?).map_err(|_| corrupt(file, "version"))?;
    let decl = need(K_DECL)?;
    let checkpoint =
        Checkpoint::decode(&need(K_CHECKPOINT)?).map_err(|_| corrupt(file, "checkpoint bytes"))?;
    Ok(Some(FileMeta {
        format,
        db_id,
        projection,
        version,
        decl,
        checkpoint,
    }))
}

/// Writes every meta key: a fresh or rebuilt file.
pub(crate) fn write_meta(
    txn: &redb::WriteTransaction,
    db_id: Uuid,
    projection: &str,
    version: &str,
    decl: &[u8],
) -> Result<()> {
    let mut table = txn.open_table(META)?;
    table.insert(K_FORMAT, FORMAT.to_be_bytes().as_slice())?;
    table.insert(K_DB_ID, db_id.as_bytes().as_slice())?;
    table.insert(K_PROJECTION, projection.as_bytes())?;
    table.insert(K_VERSION, version.as_bytes())?;
    table.insert(K_DECL, decl)?;
    table.insert(K_CHECKPOINT, Checkpoint::default().encode().as_slice())?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    table.insert(K_CREATED_AT, now.to_be_bytes().as_slice())?;
    Ok(())
}

pub(crate) fn write_checkpoint(txn: &redb::WriteTransaction, cp: Checkpoint) -> Result<()> {
    let mut table = txn.open_table(META)?;
    table.insert(K_CHECKPOINT, cp.encode().as_slice())?;
    Ok(())
}

/// Deletes every collection table, leaving `meta`.
pub(crate) fn drop_collections(txn: &redb::WriteTransaction) -> Result<()> {
    let names: Vec<String> = txn
        .list_tables()?
        .map(|h| h.name().to_owned())
        .filter(|n| n.starts_with("c:"))
        .collect();
    for n in names {
        txn.delete_table(TableDefinition::<&[u8], &[u8]>::new(&n))?;
    }
    Ok(())
}

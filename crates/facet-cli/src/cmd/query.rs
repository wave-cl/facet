//! The read commands.

use base64::Engine;
use facet_client::{Page, ReadAt, Scan, Seen};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::client::Conn;
use crate::output::{Format, json_arg, key_text};

#[derive(clap::Args, Debug)]
pub struct ReadAtArgs {
    /// Wait until the projection applied this position (a chron commit's
    /// next) before reading.
    #[arg(long)]
    pub at_least: Option<u64>,
    /// Seconds to wait for it (the daemon's default is 5, at most 30).
    #[arg(long)]
    pub wait: Option<f64>,
}

impl ReadAtArgs {
    fn at(&self) -> ReadAt {
        ReadAt {
            at_least: self.at_least,
            wait: self.wait.map(std::time::Duration::from_secs_f64),
        }
    }
}

#[derive(clap::Args, Debug)]
pub struct PagingArgs {
    /// Leading key parts every entry shares (JSON array, or one bare part).
    #[arg(long)]
    pub prefix: Option<String>,
    /// Entries at or above these leading parts.
    #[arg(long)]
    pub from: Option<String>,
    /// Entries at or below these leading parts.
    #[arg(long)]
    pub to: Option<String>,
    /// Continue after this cursor (printed as `next` by the page before).
    #[arg(long)]
    pub after: Option<String>,
    /// Entries per page (0 = the daemon's default of 100).
    #[arg(long, default_value_t = 0)]
    pub limit: u32,
    /// Descending order.
    #[arg(long)]
    pub reverse: bool,
    /// Keep paging until the scan is exhausted.
    #[arg(long)]
    pub all: bool,
}

impl PagingArgs {
    fn scan(&self, at: ReadAt) -> anyhow::Result<Scan> {
        Ok(Scan {
            prefix: self.prefix.as_deref().map(json_arg),
            from: self.from.as_deref().map(json_arg),
            to: self.to.as_deref().map(json_arg),
            after: self.after.as_deref().map(cursor).transpose()?,
            limit: self.limit,
            reverse: self.reverse,
            at,
        })
    }
}

fn cursor(s: &str) -> anyhow::Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| crate::output::Usage(format!("--after is not a cursor: {e}")).into())
}

fn cursor_text(c: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(c)
}

fn print_seen(format: Format, seen: &Seen) {
    if format == Format::Human {
        eprintln!(
            "seen: checkpoint {} ({:?}), chron head {}",
            seen.checkpoint, seen.state, seen.chron_head
        );
    }
}

fn print_page(format: Format, page: &Page) {
    match format {
        Format::Json => {
            for e in &page.entries {
                println!("{}", json!({ "key": e.key, "value": e.value }));
            }
            if let Some(n) = &page.next {
                println!(
                    "{}",
                    json!({ "next": cursor_text(n), "seen": seen_json(&page.seen) })
                );
            } else {
                println!("{}", json!({ "next": null, "seen": seen_json(&page.seen) }));
            }
        }
        Format::Human => {
            for e in &page.entries {
                println!("{}\t{}", key_text(&e.key), e.value);
            }
            if let Some(n) = &page.next {
                eprintln!("next: {}", cursor_text(n));
            }
            print_seen(format, &page.seen);
        }
    }
}

pub fn seen_json(s: &Seen) -> Value {
    json!({ "checkpoint": s.checkpoint, "state": format!("{:?}", s.state).to_lowercase(), "chron_head": s.chron_head })
}

#[derive(clap::Args, Debug)]
pub struct GetArgs {
    pub projection: String,
    pub collection: String,
    /// The whole key (JSON array, or one bare part); none for a register.
    pub key: Option<String>,
    #[command(flatten)]
    pub at: ReadAtArgs,
}

pub async fn get(conn: &Conn, format: Format, a: GetArgs) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let key = a.key.as_deref().map(json_arg);
    let (v, seen) = c
        .get(&a.projection, &a.collection, key.as_ref(), a.at.at())
        .await?;
    match format {
        Format::Json => println!(
            "{}",
            json!({ "found": v.is_some(), "value": v, "seen": seen_json(&seen) })
        ),
        Format::Human => {
            match &v {
                Some(v) => println!("{v}"),
                None => println!("not found"),
            }
            print_seen(format, &seen);
        }
    }
    Ok(())
}

async fn paged(
    format: Format,
    all: bool,
    mut first: Scan,
    fetch: impl AsyncFn(Scan) -> Result<Page, facet_client::ClientError>,
) -> anyhow::Result<()> {
    loop {
        let page = fetch(first.clone()).await?;
        print_page(format, &page);
        match (&page.next, all) {
            (Some(n), true) => first.after = Some(n.clone()),
            _ => return Ok(()),
        }
    }
}

#[derive(clap::Args, Debug)]
pub struct ScanArgs {
    pub projection: String,
    pub collection: String,
    #[command(flatten)]
    pub paging: PagingArgs,
    #[command(flatten)]
    pub at: ReadAtArgs,
}

pub async fn scan(conn: &Conn, format: Format, a: ScanArgs) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let scan = a.paging.scan(a.at.at())?;
    paged(format, a.paging.all, scan, async |s| {
        c.scan(&a.projection, &a.collection, s).await
    })
    .await
}

#[derive(clap::Args, Debug)]
pub struct LookupArgs {
    pub projection: String,
    pub table: String,
    /// The secondary index.
    #[arg(long)]
    pub index: String,
    #[command(flatten)]
    pub paging: PagingArgs,
    #[command(flatten)]
    pub at: ReadAtArgs,
}

pub async fn lookup(conn: &Conn, format: Format, a: LookupArgs) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let scan = a.paging.scan(a.at.at())?;
    paged(format, a.paging.all, scan, async |s| {
        c.lookup(&a.projection, &a.table, &a.index, s).await
    })
    .await
}

#[derive(clap::Args, Debug)]
pub struct RangeArgs {
    pub projection: String,
    pub collection: String,
    /// The partition (JSON array, or one bare part); none for one set.
    #[arg(long)]
    pub partition: Option<String>,
    /// The lowest score, inclusive.
    #[arg(long)]
    pub min: Option<String>,
    /// The highest score, inclusive.
    #[arg(long)]
    pub max: Option<String>,
    #[arg(long)]
    pub after: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub limit: u32,
    /// Highest first.
    #[arg(long)]
    pub reverse: bool,
    #[command(flatten)]
    pub at: ReadAtArgs,
}

pub async fn range(conn: &Conn, format: Format, a: RangeArgs) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let page = c
        .range(
            &a.projection,
            &a.collection,
            a.partition.as_deref().map(json_arg).as_ref(),
            a.min.as_deref().map(json_arg).as_ref(),
            a.max.as_deref().map(json_arg).as_ref(),
            a.after.as_deref().map(cursor).transpose()?,
            a.limit,
            a.reverse,
            a.at.at(),
        )
        .await?;
    print_page(format, &page);
    Ok(())
}

#[derive(clap::Args, Debug)]
pub struct TopArgs {
    pub projection: String,
    pub collection: String,
    #[arg(long)]
    pub partition: Option<String>,
    /// How many.
    #[arg(long, default_value_t = 10)]
    pub limit: u32,
}

pub async fn top(conn: &Conn, format: Format, a: TopArgs) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let page = c
        .top(
            &a.projection,
            &a.collection,
            a.partition.as_deref().map(json_arg).as_ref(),
            a.limit,
        )
        .await?;
    print_page(format, &page);
    Ok(())
}

#[derive(clap::Args, Debug)]
pub struct BucketsArgs {
    pub projection: String,
    pub collection: String,
    /// The key (JSON array, or one bare part); none for one series.
    #[arg(long)]
    pub key: Option<String>,
    /// minute, hour, day or a width in nanoseconds.
    #[arg(long)]
    pub width: String,
    /// Window starts at or after this, unix nanoseconds.
    #[arg(long)]
    pub from: Option<i64>,
    /// Window starts at or before this, unix nanoseconds.
    #[arg(long)]
    pub to: Option<i64>,
    #[arg(long)]
    pub after: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub limit: u32,
    #[arg(long)]
    pub reverse: bool,
    #[command(flatten)]
    pub at: ReadAtArgs,
}

pub async fn buckets(conn: &Conn, format: Format, a: BucketsArgs) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let page = c
        .buckets(
            &a.projection,
            &a.collection,
            a.key.as_deref().map(json_arg).as_ref(),
            &a.width,
            a.from,
            a.to,
            a.after.as_deref().map(cursor).transpose()?,
            a.limit,
            a.reverse,
            a.at.at(),
        )
        .await?;
    print_page(format, &page);
    Ok(())
}

#[derive(clap::Args, Debug)]
pub struct WatchArgs {
    pub projection: String,
    pub collection: String,
    /// Only this key.
    #[arg(long, conflicts_with = "prefix")]
    pub key: Option<String>,
    /// Only keys under this prefix.
    #[arg(long)]
    pub prefix: Option<String>,
}

pub async fn watch(conn: &Conn, format: Format, a: WatchArgs) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let mut s = c
        .watch(
            &a.projection,
            &a.collection,
            a.key.as_deref().map(json_arg).as_ref(),
            a.prefix.as_deref().map(json_arg).as_ref(),
        )
        .await?;
    while let Some(item) = s.next().await {
        let ch = item?;
        match format {
            Format::Json => println!(
                "{}",
                json!({ "checkpoint": ch.checkpoint, "collection": ch.collection, "keys": ch.keys, "all": ch.all })
            ),
            Format::Human => {
                if ch.all {
                    println!("{}\t{}\tall", ch.checkpoint, ch.collection);
                } else {
                    for k in &ch.keys {
                        println!("{}\t{}\t{}", ch.checkpoint, ch.collection, key_text(k));
                    }
                }
            }
        }
    }
    Ok(())
}

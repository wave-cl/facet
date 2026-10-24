//! Projections, status, health.

use facet_client::{ProjectionStatus, State};
use serde_json::json;

use crate::client::Conn;
use crate::output::{Degraded, Format};

fn state_name(s: State) -> String {
    format!("{s:?}").to_lowercase()
}

fn projection_json(p: &ProjectionStatus) -> serde_json::Value {
    json!({
        "name": p.name, "version": p.version, "state": state_name(p.state),
        "checkpoint": p.checkpoint, "chron_head": p.chron_head, "lag": p.lag,
        "error": p.error, "source": p.source, "event_types": p.event_types,
        "collections": p.collections.iter().map(|c| json!({
            "name": c.name, "kind": c.kind, "key_shape": c.key_shape, "indexes": c.indexes,
        })).collect::<Vec<_>>(),
        "dead_letters": p.dead_letters, "batches": p.batches, "events": p.events,
        "last_batch_unix_nanos": p.last_batch_unix_nanos, "on_error": p.on_error,
        "rebuilt": p.rebuilt, "reconnects": p.reconnects,
    })
}

fn print_projection(format: Format, p: &ProjectionStatus, verbose: bool) {
    match format {
        Format::Json => println!("{}", projection_json(p)),
        Format::Human => {
            let err = p
                .error
                .as_deref()
                .map(|e| format!("  {e}"))
                .unwrap_or_default();
            println!(
                "{:<24} {:<12} checkpoint {:<10} lag {:<8} dead {:<6} source {}{}",
                p.name,
                state_name(p.state),
                p.checkpoint,
                p.lag,
                p.dead_letters,
                p.source,
                err
            );
            if verbose {
                println!(
                    "  version {}  on_error {}  rebuilt {}  reconnects {}  batches {}  events {}",
                    p.version, p.on_error, p.rebuilt, p.reconnects, p.batches, p.events
                );
                println!("  event types: {}", p.event_types.join(", "));
                for c in &p.collections {
                    let kinds = c.key_shape["kinds"]
                        .as_array()
                        .map(|k| {
                            k.iter()
                                .filter_map(|x| x.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_default();
                    let ix = if c.indexes.is_empty() {
                        String::new()
                    } else {
                        format!("  indexes: {}", c.indexes.join(", "))
                    };
                    println!("  {:<20} {:<11} key ({kinds}){ix}", c.name, c.kind);
                }
            }
        }
    }
}

pub async fn projections(conn: &Conn, format: Format, name: Option<String>) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    match name {
        Some(n) => {
            let p = c.projection(&n).await?;
            print_projection(format, &p, true);
        }
        None => {
            for p in c.projections().await? {
                print_projection(format, &p, false);
            }
        }
    }
    Ok(())
}

#[derive(clap::Args, Debug)]
pub struct DeadLettersArgs {
    pub projection: String,
    /// Positions after this one.
    #[arg(long, default_value_t = 0)]
    pub after: u64,
    #[arg(long, default_value_t = 100)]
    pub limit: u32,
}

pub async fn dead_letters(conn: &Conn, format: Format, a: DeadLettersArgs) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let (dead, next) = c.dead_letters(&a.projection, a.after, a.limit).await?;
    for d in &dead {
        let tags: Vec<String> = d
            .tags
            .iter()
            .map(|t| String::from_utf8_lossy(t).into_owned())
            .collect();
        match format {
            Format::Json => println!(
                "{}",
                json!({ "position": d.position, "index": d.index, "tags": tags, "error": d.error, "at_unix_nanos": d.at_unix_nanos })
            ),
            Format::Human => println!("{}\t[{}]\t{}", d.position, tags.join(","), d.error),
        }
    }
    if format == Format::Human && next > 0 && dead.len() as u32 == a.limit {
        eprintln!("more: --after {next}");
    }
    Ok(())
}

pub async fn rebuild(conn: &Conn, format: Format, name: &str) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let p = c.rebuild(name).await?;
    print_projection(format, &p, false);
    Ok(())
}

pub async fn pause(conn: &Conn, format: Format, name: &str) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let p = c.pause(name).await?;
    print_projection(format, &p, false);
    Ok(())
}

pub async fn resume(conn: &Conn, format: Format, name: &str) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let p = c.resume(name).await?;
    print_projection(format, &p, false);
    Ok(())
}

pub async fn drop(conn: &Conn, format: Format, name: &str) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let existed = c.drop_projection(name).await?;
    match format {
        Format::Json => println!("{}", json!({ "existed": existed })),
        Format::Human => println!(
            "{}",
            if existed {
                "dropped"
            } else {
                "nothing to drop"
            }
        ),
    }
    Ok(())
}

#[derive(clap::Args, Debug)]
pub struct StatusArgs {
    /// Degraded when any projection lags more than this many events.
    #[arg(long)]
    pub max_lag: Option<u64>,
}

pub async fn status(conn: &Conn, format: Format, a: StatusArgs) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let s = c.status().await?;
    let ps = c.projections().await?;
    let mut problems = Vec::new();
    if let Some(f) = &s.fatal {
        problems.push(format!("fatal: {f}"));
    }
    for src in &s.sources {
        if !src.connected {
            problems.push(format!(
                "source {} disconnected{}",
                src.source,
                src.error
                    .as_deref()
                    .map(|e| format!(": {e}"))
                    .unwrap_or_default()
            ));
        }
    }
    for p in &ps {
        match p.state {
            State::Failed => problems.push(format!(
                "{} failed: {}",
                p.name,
                p.error.as_deref().unwrap_or("?")
            )),
            State::Paused => problems.push(format!("{} paused", p.name)),
            State::Orphaned => problems.push(format!("{} orphaned", p.name)),
            _ => {}
        }
        if let Some(max) = a.max_lag
            && p.lag > max
        {
            problems.push(format!("{} lags {} events", p.name, p.lag));
        }
    }
    match format {
        Format::Json => println!(
            "{}",
            json!({
                "version": s.version, "uptime_secs": s.uptime_secs, "store_path": s.store_path,
                "chron_db_id": s.chron_db_id, "chron_head": s.chron_head, "fatal": s.fatal,
                "projections": ps.iter().map(projection_json).collect::<Vec<_>>(),
                "sources": s.sources.iter().map(|x| json!({ "source": x.source, "connected": x.connected, "reconnects": x.reconnects, "error": x.error, "buffered": x.buffered })).collect::<Vec<_>>(),
                "degraded": !problems.is_empty(), "problems": problems,
            })
        ),
        Format::Human => {
            println!(
                "facetd {}  up {}s  store {}",
                s.version, s.uptime_secs, s.store_path
            );
            println!("chron {}  head {}", s.chron_db_id, s.chron_head);
            for x in &s.sources {
                println!(
                    "source {:<30} {} reconnects {} buffered {}",
                    x.source,
                    if x.connected {
                        "connected   "
                    } else {
                        "disconnected"
                    },
                    x.reconnects,
                    x.buffered
                );
            }
            for p in &ps {
                print_projection(format, p, false);
            }
            if problems.is_empty() {
                println!("healthy");
            } else {
                for p in &problems {
                    println!("degraded: {p}");
                }
            }
        }
    }
    if !problems.is_empty() {
        return Err(Degraded(problems.join("; ")).into());
    }
    Ok(())
}

pub async fn health(conn: &Conn, format: Format, service: &str) -> anyhow::Result<()> {
    let c = conn.connect().await?;
    let serving = c.health(service).await?;
    match format {
        Format::Json => println!("{}", json!({ "service": service, "serving": serving })),
        Format::Human => println!(
            "{service}: {}",
            if serving { "SERVING" } else { "NOT_SERVING" }
        ),
    }
    if !serving {
        return Err(Degraded(format!("{service} is not serving")).into());
    }
    Ok(())
}

//! End to end with release binaries: a three-node chron group in
//! process, `bank-views serve` and `facet` as child processes, a writer
//! committing bank events while the leader is killed and restarted;
//! at the end every view equals a replay of chron's log.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use chron_client::{Client, ClientConfig, Expected, NewEvent, Transaction};
use chrond::testing::{Cluster, free_port};
use clap::Parser;
use facet_client::{ReadAt, Scan};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

/// The bank example's tag.
const TAG: &str = "account";

/// The bank example's events, as it and bank-views serialise them.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AccountEvent {
    Opened { owner: String },
    Deposited { amount: i64 },
    Withdrawn { amount: i64 },
    Closed,
}

#[derive(Parser, Debug)]
struct Args {
    /// The `bank-views` binary.
    #[arg(long)]
    bank_views: PathBuf,
    /// The `facet` binary.
    #[arg(long)]
    facet: PathBuf,
    /// Where the children's logs go.
    #[arg(long)]
    logs: Option<PathBuf>,
    /// How long the writer runs.
    #[arg(long, default_value_t = 30)]
    secs: u64,
    /// How many times the leader is killed and restarted.
    #[arg(long, default_value_t = 3)]
    kills: u32,
    /// Accounts to spread the events over.
    #[arg(long, default_value_t = 25)]
    accounts: usize,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let logs = match &args.logs {
        Some(p) => {
            std::fs::create_dir_all(p)?;
            p.clone()
        }
        None => tempfile::tempdir()?.keep(),
    };
    eprintln!("logs in {}", logs.display());

    let ttl = Duration::from_millis(1500);
    let mut c = Cluster::start(3, ttl).await;
    let leader = c.wait_for_leader(Duration::from_secs(15)).await;
    let seeds: Vec<String> = (0..3).map(|i| c.addr(i)).collect();
    eprintln!("chron group: {:?}, leader {leader}", seeds);

    // The daemon under test.
    let data = tempfile::tempdir()?;
    let grpc_port = free_port();
    let http_port = free_port();
    let metrics_port = free_port();
    let log = std::fs::File::create(logs.join("bank-views.log"))?;
    let mut child = tokio::process::Command::new(&args.bank_views)
        .args([
            "serve",
            "--data-dir",
            data.path().join("facet").to_str().unwrap(),
            "--chron",
            &seeds.join(","),
            "--listen",
            &format!("127.0.0.1:{grpc_port}"),
            "--http-listen",
            &format!("127.0.0.1:{http_port}"),
            "--metrics-listen",
            &format!("127.0.0.1:{metrics_port}"),
        ])
        .env("FACETD_LOG", "info")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .context("spawning bank-views")?;
    let facet_addr = format!("http://127.0.0.1:{grpc_port}");
    let client = wait_for_daemon(&facet_addr, Duration::from_secs(20)).await?;

    // The writer.
    let ids: Vec<Uuid> = (0..args.accounts).map(|_| Uuid::new_v4()).collect();
    let committed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (ids, committed, failed, stop, seeds) = (
            ids.clone(),
            committed.clone(),
            failed.clone(),
            stop.clone(),
            seeds.clone(),
        );
        tokio::spawn(async move {
            let w = Client::connect_any(
                seeds,
                ClientConfig {
                    failover_timeout: Duration::from_secs(20),
                    ..Default::default()
                },
            )
            .await
            .expect("writer connects");
            for (i, id) in ids.iter().enumerate() {
                let mut t = Transaction::new();
                t.append_one(
                    *id,
                    Expected::Any,
                    NewEvent::new(
                        serde_json::to_vec(&AccountEvent::Opened {
                            owner: format!("o{i}"),
                        })
                        .unwrap(),
                    )
                    .tag(TAG),
                );
                if w.commit(t).await.is_ok() {
                    committed.fetch_add(1, Ordering::Relaxed);
                }
            }
            let mut n: u64 = 7;
            while !stop.load(Ordering::Relaxed) {
                n = n
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let id = ids[(n >> 33) as usize % ids.len()];
                let amount = ((n >> 20) % 400) as i64 + 1;
                let ev = if (n >> 10).is_multiple_of(3) {
                    AccountEvent::Withdrawn { amount }
                } else {
                    AccountEvent::Deposited { amount }
                };
                let mut t = Transaction::new();
                t.append_one(
                    id,
                    Expected::Any,
                    NewEvent::new(serde_json::to_vec(&ev).unwrap()).tag(TAG),
                );
                match w.commit(t).await {
                    Ok(_) => committed.fetch_add(1, Ordering::Relaxed),
                    Err(_) => failed.fetch_add(1, Ordering::Relaxed),
                };
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
        })
    };

    // Kills.
    let started = Instant::now();
    let gap = Duration::from_secs(args.secs) / (args.kills + 1);
    let mut old = leader;
    for round in 0..args.kills {
        tokio::time::sleep(gap).await;
        c.stop(old).await;
        let new = c.wait_for_leader(Duration::from_secs(20)).await;
        eprintln!(
            "[{:>5.1}s] kill {}: {old} stopped, {new} leads",
            started.elapsed().as_secs_f64(),
            round + 1
        );
        // The projection must follow the new leader before the old node
        // is back: wait for the writer to make progress and facet to
        // apply it.
        let before = committed.load(Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_secs(25);
        while committed.load(Ordering::Relaxed) < before + 10 {
            if Instant::now() > deadline {
                bail!("the writer made no progress on the new leader");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mark = c
            .client(new)
            .await
            .head_with(chron_client::ReadOptions {
                consistency: Some(chron_client::Consistency::Local),
                ..Default::default()
            })
            .await?
            .head;
        loop {
            let p = client.projection("bank").await?;
            if p.checkpoint >= mark {
                break;
            }
            if Instant::now() > deadline {
                bail!("facet did not follow the new leader to {mark}: {p:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        c.restart(old).await;
        old = new;
    }
    tokio::time::sleep(gap).await;
    stop.store(true, Ordering::Relaxed);
    writer.await?;
    let committed = committed.load(Ordering::Relaxed);
    let failed = failed.load(Ordering::Relaxed);

    // Ground truth: the log as the leader holds it.
    let leader = c.wait_for_leader(Duration::from_secs(20)).await;
    let chron = c.client(leader).await;
    let head = chron.head().await?;
    let events = chron.read_tag(TAG, 0, 0).await?;
    let mut balance: HashMap<Uuid, i64> = HashMap::new();
    let mut moves: HashMap<Uuid, u64> = HashMap::new();
    let mut deposits: i64 = 0;
    let mut movements = 0u64;
    for ev in &events {
        let id = ev.aggregate.expect("account events have aggregates");
        let e: AccountEvent = serde_json::from_slice(&ev.payload)?;
        match e {
            AccountEvent::Opened { .. } => {
                balance.entry(id).or_insert(0);
            }
            AccountEvent::Closed => {}
            AccountEvent::Deposited { amount } => {
                *balance.entry(id).or_insert(0) += amount;
                deposits += amount;
                *moves.entry(id).or_insert(0) += 1;
                movements += 1;
            }
            AccountEvent::Withdrawn { amount } => {
                *balance.entry(id).or_insert(0) -= amount;
                *moves.entry(id).or_insert(0) += 1;
                movements += 1;
            }
        }
    }

    // facet's view, once it caught up.
    let (_, seen) = client
        .get(
            "bank",
            "count",
            None,
            ReadAt {
                at_least: Some(head),
                wait: Some(Duration::from_secs(30)),
            },
        )
        .await
        .context("facet did not reach the final head")?;
    let p = client.projection("bank").await?;
    let mut problems = Vec::new();
    if p.dead_letters != 0 {
        problems.push(format!("{} dead letters", p.dead_letters));
    }
    let (count, _) = client.get("bank", "count", None, ReadAt::default()).await?;
    if count != Some(json!(movements)) {
        problems.push(format!("count {count:?}, expected {movements}"));
    }
    let mut page = client
        .scan(
            "bank",
            "balance",
            Scan {
                limit: 1000,
                ..Default::default()
            },
        )
        .await?;
    let mut got: HashMap<Uuid, i64> = HashMap::new();
    loop {
        for e in &page.entries {
            let id: Uuid = e.key[0].as_str().unwrap().parse()?;
            got.insert(id, e.value.as_i64().unwrap());
        }
        match page.next.clone() {
            Some(n) => {
                page = client
                    .scan(
                        "bank",
                        "balance",
                        Scan {
                            limit: 1000,
                            after: Some(n),
                            ..Default::default()
                        },
                    )
                    .await?
            }
            None => break,
        }
    }
    if got != balance {
        problems.push(format!(
            "balances differ: {} accounts in facet, {} in the replay",
            got.len(),
            balance.len()
        ));
    }
    let top = client.top("bank", "top", None, 5).await?;
    let mut expected_top: Vec<(Uuid, i64)> = balance.iter().map(|(k, v)| (*k, *v)).collect();
    expected_top.sort_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
    let got_top: Vec<(Uuid, i64)> = top
        .entries
        .iter()
        .map(|e| {
            (
                e.key[0].as_str().unwrap().parse().unwrap(),
                e.value.as_i64().unwrap(),
            )
        })
        .collect();
    if got_top != expected_top.into_iter().take(5).collect::<Vec<_>>() {
        problems.push(format!("top differs: {got_top:?}"));
    }
    let hours = client
        .buckets(
            "bank",
            "deposits",
            None,
            "hour",
            None,
            None,
            None,
            0,
            false,
            ReadAt::default(),
        )
        .await?;
    let bucket_sum: i64 = hours
        .entries
        .iter()
        .map(|e| e.value["sum"].as_i64().unwrap())
        .sum();
    if bucket_sum != deposits {
        problems.push(format!(
            "hour buckets sum to {bucket_sum}, deposits were {deposits}"
        ));
    }
    for (id, n) in &moves {
        let rows = client
            .lookup(
                "bank",
                "transactions",
                "by_account",
                Scan {
                    prefix: Some(json!([id.to_string()])),
                    limit: 10_000,
                    ..Default::default()
                },
            )
            .await?;
        if rows.entries.len() as u64 != *n {
            problems.push(format!(
                "account {id}: {} rows, {n} movements",
                rows.entries.len()
            ));
        }
    }
    let status = client.status().await?;
    let reconnects: u64 = status.sources.iter().map(|s| s.reconnects).sum();
    if reconnects < u64::from(args.kills) {
        problems.push(format!("{reconnects} reconnects for {} kills", args.kills));
    }

    // The CLI against the daemon.
    let out = tokio::process::Command::new(&args.facet)
        .args(["--addr", &facet_addr, "status"])
        .output()
        .await?;
    let cli_status = String::from_utf8_lossy(&out.stdout).into_owned();
    if !out.status.success() {
        problems.push(format!(
            "facet status exited {:?}:\n{cli_status}{}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        ));
    }

    println!(
        "writer: committed={committed} failed={failed} kills={} head={head} events={} movements={movements} accounts={}",
        args.kills,
        events.len(),
        balance.len()
    );
    println!(
        "facet: checkpoint={} dead_letters={} reconnects={reconnects} seen.chron_head={}",
        p.checkpoint, p.dead_letters, seen.chron_head
    );
    print!("{cli_status}");

    let _ = child.kill().await;
    for i in 0..3 {
        if c.nodes[i].is_running() {
            c.stop(i).await;
        }
    }
    if problems.is_empty() {
        println!("E2E OK");
        Ok(())
    } else {
        for p in &problems {
            println!("E2E PROBLEM: {p}");
        }
        bail!("{} problems", problems.len())
    }
}

async fn wait_for_daemon(addr: &str, within: Duration) -> anyhow::Result<facet_client::Client> {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(c) =
            facet_client::Client::connect(addr, facet_client::ClientConfig::default()).await
            && c.status().await.is_ok()
        {
            return Ok(c);
        }
        if Instant::now() > deadline {
            bail!("facetd did not come up at {addr}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

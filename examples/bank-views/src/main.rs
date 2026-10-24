//! `bank-views serve` embeds facetd over the bank projection;
//! `bank-views seed` writes bank events into chron for it to consume.

use std::time::Duration;

use bank_views::AccountEvent;
use chron_client::{Client, ClientConfig, Expected, NewEvent, Transaction, Uuid};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "bank-views",
    version,
    about = "facet's bank example: serve the projections, or seed events"
)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
enum Cmd {
    /// Serve the bank projections (an embedded facetd).
    Serve {
        #[command(flatten)]
        daemon: facetd::Cli,
    },
    /// Commit random bank events into chron.
    Seed {
        /// chron addresses, comma separated.
        #[arg(long, env = "FACET_CHRON", value_delimiter = ',', required = true)]
        chron: Vec<String>,
        /// Bearer token for chron.
        #[arg(long, env = "FACET_CHRON_TOKEN")]
        token: Option<String>,
        /// How many accounts to open.
        #[arg(long, default_value_t = 20)]
        accounts: u32,
        /// How many deposits and withdrawals to make.
        #[arg(long, default_value_t = 200)]
        ops: u32,
        /// Milliseconds between commits.
        #[arg(long, default_value_t = 0)]
        pause_ms: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Args::parse().cmd {
        Cmd::Serve { daemon } => facetd::run(bank_views::app(), daemon).await,
        Cmd::Seed {
            chron,
            token,
            accounts,
            ops,
            pause_ms,
        } => seed(chron, token, accounts, ops, pause_ms).await,
    }
}

async fn seed(
    chron: Vec<String>,
    token: Option<String>,
    accounts: u32,
    ops: u32,
    pause_ms: u64,
) -> anyhow::Result<()> {
    let client = Client::connect_any(
        chron,
        ClientConfig {
            token,
            failover_timeout: Duration::from_secs(20),
            ..Default::default()
        },
    )
    .await?;
    let ids: Vec<Uuid> = (0..accounts).map(|_| Uuid::new_v4()).collect();
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(1)
        | 1;
    let mut next_rand = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for (i, id) in ids.iter().enumerate() {
        commit(
            &client,
            *id,
            &AccountEvent::Opened {
                owner: format!("owner-{i}"),
            },
        )
        .await?;
    }
    let mut last = 0;
    for _ in 0..ops {
        let id = ids[(next_rand() % ids.len() as u64) as usize];
        let amount = (next_rand() % 500) as i64 + 1;
        let ev = if next_rand().is_multiple_of(3) {
            AccountEvent::Withdrawn { amount }
        } else {
            AccountEvent::Deposited { amount }
        };
        last = commit(&client, id, &ev).await?;
        if pause_ms > 0 {
            tokio::time::sleep(Duration::from_millis(pause_ms)).await;
        }
    }
    println!("seeded {accounts} accounts and {ops} movements; chron head {last}");
    Ok(())
}

async fn commit(client: &Client, id: Uuid, ev: &AccountEvent) -> anyhow::Result<u64> {
    let mut t = Transaction::new();
    t.append_one(
        id,
        Expected::Any,
        NewEvent::new(serde_json::to_vec(ev)?).tag(bank_views::TAG),
    );
    Ok(client.commit(t).await?.next)
}

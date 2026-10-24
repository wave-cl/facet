#![allow(dead_code)]

use std::time::Duration;

use facet::{App, HandlerError, OnError, Projection, Source, TypeSchema, Width};
use facet_client::{Client, ClientConfig};
use serde::{Deserialize, Serialize};

pub use facetd::testing::Fixture;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deposit {
    pub account: String,
    pub amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Txn {
    pub id: u64,
    pub account: String,
    pub amount: i64,
    pub note: Option<String>,
}

/// The test application: `bank` over tag `deposit` with every kind of
/// collection, and `fragile`, which fails on an amount of 13.
pub fn app() -> App {
    let (b, (balance, total, seen, feed, txns, top, per_minute)) = Projection::new("bank")
        .source(Source::tag("deposit"))
        .collections(|c| {
            (
                c.map::<String, i64>("balance"),
                c.counter::<(), i64>("total"),
                c.set::<String>("seen"),
                c.list::<String, i64>("feed"),
                c.table::<u64, Txn>("txns")
                    .column("id", TypeSchema::Uint)
                    .column("account", TypeSchema::Str)
                    .column("amount", TypeSchema::Int)
                    .nullable("note", TypeSchema::Str)
                    .primary_key(["id"])
                    .index("by_account", ["account", "amount"])
                    .index("by_note", ["note"])
                    .build(),
                c.sorted_set::<(), String, i64>("top"),
                c.buckets::<String, i64>("per_minute", [Width::Minute, Width::Hour]),
            )
        });
    let bank = b
        .on::<Deposit>("deposit", move |cx, d| {
            let position = cx.position();
            let at = cx.recorded_at();
            let bal = cx
                .map(&balance)?
                .update(&d.account, |b| Some(b.unwrap_or(0) + d.amount))?
                .unwrap();
            cx.counter(&total)?.add(&(), d.amount)?;
            cx.set(&seen)?.insert(&d.account)?;
            cx.list(&feed)?.push_back(&d.account, &d.amount)?;
            cx.table(&txns)?.upsert(&Txn {
                id: position,
                account: d.account.clone(),
                amount: d.amount,
                note: (d.amount < 0).then(|| "withdrawal".to_string()),
            })?;
            cx.sorted_set(&top)?.put(&(), &d.account, bal)?;
            cx.buckets(&per_minute)?.add(&d.account, at, d.amount)?;
            Ok::<(), HandlerError>(())
        })
        .build()
        .unwrap();
    let (b, n) = Projection::new("fragile")
        .source(Source::tag("deposit"))
        .on_error(OnError::Skip)
        .collections(|c| c.counter::<(), i64>("n"));
    let fragile = b
        .on::<Deposit>("deposit", move |cx, d| {
            if d.amount == 13 {
                return Err(HandlerError::Handler("unlucky".into()));
            }
            cx.counter(&n)?.add(&(), 1)?;
            Ok(())
        })
        .build()
        .unwrap();
    App::new().projection(bank).projection(fragile)
}

pub fn deposit(account: &str, amount: i64) -> String {
    serde_json::to_string(&Deposit {
        account: account.into(),
        amount,
    })
    .unwrap()
}

pub async fn client(f: &Fixture) -> Client {
    Client::connect(f.addr(), ClientConfig::default())
        .await
        .unwrap()
}

/// Commits deposits and waits until `bank` applied them.
pub async fn seed(f: &Fixture) -> u64 {
    let payloads = [
        deposit("alice", 100),
        deposit("bob", 50),
        deposit("alice", -30),
        deposit("carol", 13),
        deposit("bob", 25),
    ];
    let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
    let next = f.commit("deposit", &refs).await;
    f.wait_applied("bank", next, Duration::from_secs(20)).await;
    f.wait_applied("fragile", next, Duration::from_secs(20))
        .await;
    next
}

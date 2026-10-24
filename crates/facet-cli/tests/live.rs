//! The `facet` binary against an in-process chrond and facetd.

use std::time::Duration;

use assert_cmd::Command;
use facet::{App, HandlerError, OnError, Projection, Source, TypeSchema};
use predicates::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Deposit {
    account: String,
    amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Txn {
    id: u64,
    account: String,
    amount: i64,
}

fn app() -> App {
    let (b, (balance, total, txns, top)) = Projection::new("bank")
        .source(Source::tag("deposit"))
        .on_error(OnError::Skip)
        .collections(|c| {
            (
                c.map::<String, i64>("balance"),
                c.counter::<(), i64>("total"),
                c.table::<u64, Txn>("txns")
                    .column("id", TypeSchema::Uint)
                    .column("account", TypeSchema::Str)
                    .column("amount", TypeSchema::Int)
                    .primary_key(["id"])
                    .index("by_account", ["account"])
                    .build(),
                c.sorted_set::<(), String, i64>("top"),
            )
        });
    let p = b
        .on::<Deposit>("deposit", move |cx, d| {
            let position = cx.position();
            if d.amount == 13 {
                return Err(HandlerError::Handler("unlucky".into()));
            }
            let bal = cx
                .map(&balance)?
                .update(&d.account, |b| Some(b.unwrap_or(0) + d.amount))?
                .unwrap();
            cx.counter(&total)?.add(&(), d.amount)?;
            cx.table(&txns)?.upsert(&Txn {
                id: position,
                account: d.account.clone(),
                amount: d.amount,
            })?;
            cx.sorted_set(&top)?.put(&(), &d.account, bal)?;
            Ok(())
        })
        .build()
        .unwrap();
    App::new().projection(p)
}

struct Daemon {
    rt: tokio::runtime::Runtime,
    fixture: Option<facetd::testing::Fixture>,
    url: String,
}

impl Daemon {
    fn start() -> Daemon {
        Self::start_with(|_| {})
    }

    fn start_with(f: impl FnOnce(&mut facetd::Options)) -> Daemon {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = rt.block_on(facetd::testing::Fixture::start_with(app(), f));
        let url = fixture.addr();
        Daemon {
            rt,
            fixture: Some(fixture),
            url,
        }
    }

    fn facet(&self) -> Command {
        let mut c = Command::cargo_bin("facet").unwrap();
        c.env("FACET_ADDR", &self.url);
        c
    }

    fn deposit(&self, account: &str, amount: i64) -> u64 {
        let f = self.fixture.as_ref().unwrap();
        let payload = serde_json::to_string(&Deposit {
            account: account.into(),
            amount,
        })
        .unwrap();
        let next = self.rt.block_on(f.commit("deposit", &[&payload]));
        self.rt
            .block_on(f.wait_applied("bank", next, Duration::from_secs(20)));
        next
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(f) = self.fixture.take() {
            self.rt.block_on(f.shutdown());
        }
    }
}

#[test]
fn reads_print_human_lines_and_json() {
    let d = Daemon::start();
    d.deposit("alice", 100);
    d.deposit("bob", 50);
    let next = d.deposit("alice", -30);
    d.facet()
        .args(["get", "bank", "balance", "alice"])
        .assert()
        .success()
        .stdout("70\n")
        .stderr(predicate::str::contains(format!("checkpoint {next}")));
    d.facet()
        .args(["get", "bank", "balance", "nobody"])
        .assert()
        .success()
        .stdout("not found\n");
    d.facet()
        .args(["--json", "get", "bank", "total"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"value\":120"));
    d.facet()
        .args(["scan", "bank", "balance"])
        .assert()
        .success()
        .stdout("\"alice\"\t70\n\"bob\"\t50\n");
    d.facet()
        .args(["scan", "bank", "balance", "--limit", "1", "--reverse"])
        .assert()
        .success()
        .stdout("\"bob\"\t50\n")
        .stderr(predicate::str::contains("next: "));
    d.facet()
        .args(["scan", "bank", "txns", "--limit", "1", "--all", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"account\":\"alice\"").count(2));
    d.facet()
        .args([
            "lookup",
            "bank",
            "txns",
            "--index",
            "by_account",
            "--prefix",
            "bob",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"amount\":50"));
    d.facet()
        .args(["top", "bank", "top", "--limit", "1"])
        .assert()
        .success()
        .stdout("\"alice\"\t70\n");
    d.facet()
        .args(["range", "bank", "top", "--min", "60"])
        .assert()
        .success()
        .stdout("\"alice\"\t70\n");
    d.facet()
        .args([
            "get",
            "bank",
            "balance",
            "alice",
            "--at-least",
            &next.to_string(),
        ])
        .assert()
        .success();
    d.facet()
        .args([
            "get",
            "bank",
            "balance",
            "alice",
            "--at-least",
            &(next + 1000).to_string(),
            "--wait",
            "0.2",
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("not yet at"));
}

#[test]
fn errors_exit_1_and_an_unreachable_daemon_2() {
    let d = Daemon::start();
    d.facet()
        .args(["get", "nope", "x"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("no projection"));
    d.facet()
        .args(["--json", "get", "bank", "balance", "5"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("\"code\":1"));
    Command::cargo_bin("facet")
        .unwrap()
        .env("FACET_ADDR", "http://127.0.0.1:1")
        .args(["status"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("cannot reach"));
}

#[test]
fn projections_status_health_and_admin_commands() {
    let d = Daemon::start();
    let next = d.deposit("carol", 13); // skipped: a dead letter
    d.deposit("carol", 1);
    d.facet().args(["projections"]).assert().success().stdout(
        predicate::str::contains("bank")
            .and(predicate::str::contains("live"))
            .and(predicate::str::contains("dead 1")),
    );
    d.facet()
        .args(["projections", "bank"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("by_account")
                .and(predicate::str::contains("event types: deposit")),
        );
    d.facet()
        .args(["dead-letters", "bank"])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "{}\t[deposit]\tunlucky",
            next - 1
        )));
    d.facet()
        .args(["status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("healthy"));
    d.facet()
        .args(["health"])
        .assert()
        .success()
        .stdout("facet.v1.Query: SERVING\n");
    d.facet().args(["pause", "bank"]).assert().success();
    std::thread::sleep(Duration::from_millis(300));
    d.facet()
        .args(["status"])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("degraded: bank paused"));
    d.facet()
        .args(["--json", "status"])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("\"degraded\":true"));
    d.facet().args(["resume", "bank"]).assert().success();
    d.facet().args(["rebuild", "bank"]).assert().success();
    std::thread::sleep(Duration::from_millis(500));
    d.facet()
        .args(["get", "bank", "total"])
        .assert()
        .success()
        .stdout("1\n");
    d.facet()
        .args(["drop", "bank"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("registered"));
}

#[test]
fn tokens_are_sent() {
    let d = Daemon::start_with(|o| {
        o.token = Some("r".into());
        o.admin_token = Some("a".into());
    });
    d.facet()
        .args(["status"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("bearer"));
    d.facet()
        .args(["--token", "r", "status"])
        .assert()
        .success();
    d.facet()
        .args(["--token", "r", "pause", "bank"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("admin token"));
    d.facet()
        .args(["--token", "r", "--admin-token", "a", "pause", "bank"])
        .assert()
        .success();
    d.facet()
        .args(["--token", "a", "resume", "bank"])
        .assert()
        .success();
}

//! Status, projections, dead letters, rebuild, pause, resume, drop.

use std::time::Duration;

use facet_client::{ClientError, ReadAt, State};
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn status_and_projections_describe_the_runtime() {
    let f = Fixture::start(app()).await;
    let next = seed(&f).await;
    let c = client(&f).await;
    let s = c.status().await.unwrap();
    assert_eq!(s.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(s.fatal, None);
    assert_eq!(s.counts[2], 2, "both live: {s:?}");
    assert_eq!(s.sources.len(), 1, "one tail for the shared tag");
    assert!(s.sources[0].connected);
    assert_eq!(s.chron_db_id.len(), 36);

    let ps = c.projections().await.unwrap();
    assert_eq!(
        ps.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
        ["bank", "fragile"]
    );
    let bank = &ps[0];
    assert_eq!(bank.state, State::Live);
    assert_eq!(bank.checkpoint, next);
    assert_eq!(bank.lag, 0);
    assert_eq!(bank.source, "tag:deposit");
    assert_eq!(bank.event_types, vec!["deposit"]);
    assert_eq!(bank.on_error, "fail");
    assert_eq!(bank.rebuilt, "fresh");
    let names: Vec<&str> = bank.collections.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "balance",
            "total",
            "seen",
            "feed",
            "txns",
            "top",
            "per_minute",
            "__dead"
        ]
    );
    let txns = bank.collections.iter().find(|c| c.name == "txns").unwrap();
    assert_eq!(txns.kind, "table");
    assert_eq!(txns.key_shape["kinds"], json!(["uint"]));
    assert_eq!(txns.key_shape["names"], json!(["id"]));
    assert_eq!(txns.indexes, vec!["by_account", "by_note"]);
    let feed = bank.collections.iter().find(|c| c.name == "feed").unwrap();
    assert_eq!(feed.key_shape["kinds"], json!(["string", "int"]));
    assert_eq!(feed.key_shape["names"], json!(["0", "seq"]));
    let one = c.projection("fragile").await.unwrap();
    assert_eq!(one.on_error, "skip");
    assert_eq!(one.dead_letters, 1);
    assert!(matches!(
        c.projection("nope").await.unwrap_err(),
        ClientError::NotFound(_)
    ));
    f.shutdown().await;
}

#[tokio::test]
async fn dead_letters_page() {
    let f = Fixture::start(app()).await;
    let next = seed(&f).await;
    let c = client(&f).await;
    let (dead, cursor) = c.dead_letters("fragile", 0, 10).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].position, next - 2);
    assert_eq!(dead[0].error, "unlucky");
    assert_eq!(dead[0].tags, vec![b"deposit".to_vec()]);
    assert_eq!(cursor, next - 2);
    let (more, _) = c.dead_letters("fragile", cursor, 10).await.unwrap();
    assert!(more.is_empty());
    let (none, _) = c.dead_letters("bank", 0, 10).await.unwrap();
    assert!(none.is_empty());
    f.shutdown().await;
}

#[tokio::test]
async fn pause_resume_and_rebuild_over_the_wire() {
    let f = Fixture::start(app()).await;
    let next = seed(&f).await;
    let c = client(&f).await;
    let p = c.pause("bank").await.unwrap();
    assert_eq!(p.name, "bank");
    f.wait_state("bank", facet::State::Paused, Duration::from_secs(10))
        .await;
    let later = f.commit("deposit", &[&deposit("alice", 1)]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(c.projection("bank").await.unwrap().checkpoint, next);
    // A paused projection still answers reads, and says so.
    let (_, seen) = c
        .get("bank", "balance", Some(&json!("alice")), ReadAt::default())
        .await
        .unwrap();
    assert_eq!(seen.state, State::Paused);
    c.resume("bank").await.unwrap();
    f.wait_applied("bank", later, Duration::from_secs(10)).await;
    let (v, _) = c
        .get("bank", "balance", Some(&json!("alice")), ReadAt::default())
        .await
        .unwrap();
    assert_eq!(v, Some(json!(71)));

    let p = c.rebuild("bank").await.unwrap();
    assert!(
        matches!(
            p.state,
            State::Rebuilding | State::Starting | State::CatchingUp | State::Live
        ),
        "{:?}",
        p.state
    );
    f.wait_applied("bank", later, Duration::from_secs(10)).await;
    let (v, seen) = c
        .get("bank", "total", None, ReadAt::at_least(later))
        .await
        .unwrap();
    assert_eq!(v, Some(json!(159)));
    assert_eq!(seen.state, State::Live);
    f.shutdown().await;
}

#[tokio::test]
async fn an_orphaned_projection_is_listed_and_can_be_dropped() {
    let f = Fixture::start(app()).await;
    seed(&f).await;
    // The application without `fragile`, over the same store and chron.
    let (b, n) = facet::Projection::new("bank")
        .source(facet::Source::tag("deposit"))
        .collections(|c| c.counter::<(), i64>("total"));
    let bank = b
        .on::<Deposit>("deposit", move |cx, d| {
            cx.counter(&n)?.add(&(), d.amount)?;
            Ok::<(), facet::HandlerError>(())
        })
        .build()
        .unwrap();
    let f = f
        .restart_with(facet::App::new().projection(bank), |_| {})
        .await;
    let c = client(&f).await;
    let ps = c.projections().await.unwrap();
    let orphan = ps.iter().find(|p| p.name == "fragile").expect("listed");
    assert_eq!(orphan.state, State::Orphaned);
    assert!(orphan.checkpoint > 0);
    assert_eq!(
        c.projection("fragile").await.unwrap().state,
        State::Orphaned
    );
    // A registered projection cannot be dropped; an orphan can.
    let err = c.drop_projection("bank").await.unwrap_err();
    assert!(matches!(err, ClientError::FailedPrecondition(_)), "{err}");
    assert!(c.drop_projection("fragile").await.unwrap());
    assert!(!c.drop_projection("fragile").await.unwrap());
    assert!(
        c.projections()
            .await
            .unwrap()
            .iter()
            .all(|p| p.name != "fragile")
    );
    // Reads of an orphan are refused as not found.
    let err = c
        .get("fragile", "n", None, ReadAt::default())
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::NotFound(_)), "{err}");
    f.shutdown().await;
}

#[tokio::test]
async fn a_failed_projection_refuses_reads_with_its_error() {
    let (b, n) = facet::Projection::new("strict")
        .source(facet::Source::tag("deposit"))
        .collections(|c| c.counter::<(), i64>("n"));
    let strict = b
        .on::<Deposit>("deposit", move |cx, d| {
            if d.amount == 13 {
                return Err(facet::HandlerError::Handler("unlucky".into()));
            }
            cx.counter(&n)?.add(&(), 1)?;
            Ok(())
        })
        .build()
        .unwrap();
    let f = Fixture::start(facet::App::new().projection(strict)).await;
    let c = client(&f).await;
    f.commit("deposit", &[&deposit("carol", 13)]).await;
    f.wait_state("strict", facet::State::Failed, Duration::from_secs(10))
        .await;
    let err = c
        .get("strict", "n", None, ReadAt::default())
        .await
        .unwrap_err();
    match err {
        ClientError::FailedPrecondition(m) => assert!(m.contains("unlucky"), "{m}"),
        other => panic!("{other}"),
    }
    let s = c.status().await.unwrap();
    assert_eq!(s.counts[4], 1, "failed: {s:?}");
    assert!(!c.health("facet.v1.Query.live").await.unwrap());
    assert!(c.health("facet.v1.Query").await.unwrap());
    f.shutdown().await;
}

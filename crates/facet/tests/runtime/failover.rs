//! A three-node chron group: the leader is killed and restarted while a
//! writer commits; the projection ends equal to what was committed, with
//! no gap and no double.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use chron_client::{Client, ClientConfig, Expected, NewEvent, Transaction, Uuid};
use chrond::testing::Cluster;
use facet::{OnError, Options, Runtime, RuntimeOptions, State};

use crate::common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_dies_twice_and_nothing_is_lost_or_doubled() {
    let d = tmp();
    let ttl = Duration::from_millis(1500);
    let mut c = Cluster::start(3, ttl).await;
    let leader = c.wait_for_leader(Duration::from_secs(15)).await;
    let seeds: Vec<String> = (0..3).map(|i| c.addr(i)).collect();

    let (app, n, _) = counter_app("1", OnError::Fail);
    let opts = RuntimeOptions::new(d.path().join("facet"), seeds.clone())
        .store(Options::new().fsync(facet::Fsync::Never))
        .status_interval(Duration::from_millis(200))
        .reconnect(Duration::from_millis(50), Duration::from_millis(500))
        .chron_config(ClientConfig {
            failover_timeout: Duration::from_secs(20),
            ..Default::default()
        });
    let rt = Runtime::start(app, opts).await.expect("the runtime starts");

    // A writer through its own failover-aware client.
    let committed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (committed, failed, stop) = (committed.clone(), failed.clone(), stop.clone());
        let seeds = seeds.clone();
        tokio::spawn(async move {
            let client = Client::connect_any(
                seeds,
                ClientConfig {
                    failover_timeout: Duration::from_secs(20),
                    ..Default::default()
                },
            )
            .await
            .expect("writer connects");
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let mut t = Transaction::new();
                for _ in 0..3 {
                    t.append_one(
                        Uuid::from_u128(1),
                        Expected::Any,
                        NewEvent::new(i.to_string()).tag("t"),
                    );
                    i += 1;
                }
                match client.commit(t).await {
                    Ok(_) => {
                        committed.fetch_add(3, Ordering::Relaxed);
                    }
                    Err(_) => {
                        failed.fetch_add(3, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    let mut old = leader;
    for round in 0..2 {
        tokio::time::sleep(Duration::from_millis(800)).await;
        c.stop(old).await;
        let new = c.wait_for_leader(Duration::from_secs(20)).await;
        assert_ne!(new, old, "round {round}");
        // Before the old node comes back, the projection must already be
        // following the new leader, the only place the writes made since
        // the kill can come from. That takes a rediscovery. (A fresh
        // leader's default head lags until it commits in its own epoch,
        // so the mark is taken after the writer made progress, locally.)
        let before = committed.load(Ordering::Relaxed);
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while committed.load(Ordering::Relaxed) < before + 6 {
            assert!(
                std::time::Instant::now() < deadline,
                "round {round}: the writer made no progress on the new leader"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mark = c
            .client(new)
            .await
            .head_with(chron_client::ReadOptions {
                consistency: Some(chron_client::Consistency::Local),
                ..Default::default()
            })
            .await
            .unwrap()
            .head;
        loop {
            let s = rt.status("c").unwrap();
            if s.checkpoint.next >= mark {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "round {round}: the projection did not follow the new leader to {mark}: {s:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        c.restart(old).await;
        old = new;
    }
    tokio::time::sleep(Duration::from_millis(800)).await;
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();

    let committed = committed.load(Ordering::Relaxed);
    let failed = failed.load(Ordering::Relaxed);
    let leader = c.wait_for_leader(Duration::from_secs(20)).await;
    let head = c.client(leader).await.head().await.unwrap();
    wait_applied(&rt, "c", head).await;
    let count = counter_value(&rt, &n) as u64;
    // Every acknowledged event, once; a commit that timed out may or may
    // not have landed, so it widens the band but nothing else does.
    assert!(count >= committed, "count {count} < committed {committed}");
    assert!(
        count <= committed + failed,
        "count {count} > {committed} + {failed}"
    );
    assert_eq!(count, head, "every event was tagged t");
    let s = rt.status("c").unwrap();
    assert_eq!(s.state, State::Live);
    let sources = rt.handle().sources();
    assert_eq!(sources.len(), 1);
    assert!(sources[0].reconnects >= 1, "{sources:?}");
    assert!(committed > 0);
    rt.shutdown().await.unwrap();
    for i in 0..3 {
        if c.nodes[i].is_running() {
            c.stop(i).await;
        }
    }
}

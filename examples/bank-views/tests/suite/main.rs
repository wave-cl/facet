//! The bank projections over an in-process chrond and facetd.

use std::time::Duration;

use bank_views::{AccountEvent, TAG, Txn};
use chron_client::{Expected, NewEvent, Transaction, Uuid};
use facet_client::{Client, ClientConfig, ReadAt};
use facetd::testing::Fixture;
use serde_json::json;

async fn commit(f: &Fixture, id: Uuid, ev: &AccountEvent) -> u64 {
    let c = f.chron_client().await;
    let mut t = Transaction::new();
    t.append_one(
        id,
        Expected::Any,
        NewEvent::new(serde_json::to_vec(ev).unwrap()).tag(TAG),
    );
    c.commit(t).await.unwrap().next
}

#[tokio::test]
async fn the_views_follow_the_bank() {
    let f = Fixture::start(bank_views::app()).await;
    let a = Uuid::from_u128(0xa);
    let b = Uuid::from_u128(0xb);
    commit(
        &f,
        a,
        &AccountEvent::Opened {
            owner: "ann".into(),
        },
    )
    .await;
    commit(
        &f,
        b,
        &AccountEvent::Opened {
            owner: "bob".into(),
        },
    )
    .await;
    commit(&f, a, &AccountEvent::Deposited { amount: 100 }).await;
    commit(&f, b, &AccountEvent::Deposited { amount: 40 }).await;
    commit(&f, a, &AccountEvent::Withdrawn { amount: 30 }).await;
    let next = commit(&f, b, &AccountEvent::Closed).await;
    f.wait_applied("bank", next, Duration::from_secs(20)).await;

    // In process.
    let snap = f.handle().snapshot("bank").unwrap();
    assert_eq!(
        snap.map::<Uuid, i64>("balance").unwrap().get(&a).unwrap(),
        Some(70)
    );
    assert_eq!(
        snap.map::<Uuid, i64>("balance").unwrap().get(&b).unwrap(),
        Some(40)
    );
    assert_eq!(
        snap.map::<Uuid, String>("owners")
            .unwrap()
            .get(&a)
            .unwrap()
            .as_deref(),
        Some("ann")
    );
    assert!(snap.set::<Uuid>("closed").unwrap().contains(&b).unwrap());
    assert_eq!(
        snap.sorted_set::<(), Uuid, i64>("top")
            .unwrap()
            .top(&(), 2)
            .unwrap()
            .items,
        vec![(a, 70), (b, 40)]
    );
    let txns = snap.table::<u64, Txn>("transactions").unwrap();
    assert_eq!(txns.len().unwrap(), 3);
    let by_a = txns
        .by_index::<(Uuid, facet::Timestamp)>("by_account")
        .unwrap()
        .prefix(&(a,))
        .run()
        .unwrap()
        .items;
    assert_eq!(
        by_a.iter().map(|t| t.amount).collect::<Vec<_>>(),
        vec![100, -30]
    );
    assert_eq!(
        snap.counter::<(), i64>("count").unwrap().get(&()).unwrap(),
        Some(3)
    );
    let hours = snap
        .buckets::<(), i64>("deposits")
        .unwrap()
        .range(&(), facet::Width::Hour)
        .unwrap()
        .run()
        .unwrap()
        .items;
    assert_eq!(hours.iter().map(|b| b.sum).sum::<i64>(), 140);
    drop(snap);

    // Over the wire.
    let c = Client::connect(f.addr(), ClientConfig::default())
        .await
        .unwrap();
    let (v, seen) = c
        .get(
            "bank",
            "balance",
            Some(&json!(a.to_string())),
            ReadAt::at_least(next),
        )
        .await
        .unwrap();
    assert_eq!(v, Some(json!(70)));
    assert!(seen.checkpoint >= next);
    let top = c.top("bank", "top", None, 1).await.unwrap();
    assert_eq!(top.entries[0].key, json!([a.to_string()]));
    let p = c.projection("bank").await.unwrap();
    assert_eq!(p.dead_letters, 0);
    f.shutdown().await;
}

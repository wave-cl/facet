//! Live queries: every batch's changes to a collection.

use std::time::Duration;

use futures::StreamExt;
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn a_watch_delivers_each_batch_filtered_by_key_or_prefix() {
    let f = Fixture::start(app()).await;
    seed(&f).await;
    let c = client(&f).await;
    let mut all = c.watch("bank", "balance", None, None).await.unwrap();
    let mut alice = c
        .watch("bank", "balance", Some(&json!("alice")), None)
        .await
        .unwrap();
    let mut feed_bob = c
        .watch("bank", "feed", None, Some(&json!(["bob"])))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let next = f
        .commit("deposit", &[&deposit("bob", 1), &deposit("alice", 2)])
        .await;
    let ch = tokio::time::timeout(Duration::from_secs(5), all.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ch.checkpoint, next);
    assert_eq!(ch.collection, "balance");
    assert_eq!(ch.keys, vec![json!(["alice"]), json!(["bob"])]);
    assert!(!ch.all);
    let ch = tokio::time::timeout(Duration::from_secs(5), alice.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ch.keys, vec![json!(["alice"])]);
    let ch = tokio::time::timeout(Duration::from_secs(5), feed_bob.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ch.collection, "feed");
    assert!(
        ch.keys.iter().all(|k| k[0] == json!("bob")),
        "{:?}",
        ch.keys
    );
    // A batch that does not touch alice is not delivered to her watcher.
    let next2 = f.commit("deposit", &[&deposit("bob", 1)]).await;
    let ch = tokio::time::timeout(Duration::from_secs(5), all.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ch.checkpoint, next2);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), alice.next())
            .await
            .is_err()
    );
    // A rebuild says everything changed.
    c.rebuild("bank").await.unwrap();
    let ch = tokio::time::timeout(Duration::from_secs(5), alice.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(ch.all);
    assert_eq!(ch.checkpoint, 0);
    f.shutdown().await;
}

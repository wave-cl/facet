//! Reads over the wire: every kind of collection, paging, keys as JSON,
//! read your writes.

use std::time::Duration;

use facet_client::{ClientError, ReadAt, Scan, State};
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn get_scan_lookup_range_and_buckets() {
    let f = Fixture::start(app()).await;
    let next = seed(&f).await;
    let c = client(&f).await;

    // Get: a map by a bare scalar key and by an array; a counter by an
    // empty key; a set member; a table row; absence.
    let (v, seen) = c
        .get("bank", "balance", Some(&json!("alice")), ReadAt::default())
        .await
        .unwrap();
    assert_eq!(v, Some(json!(70)));
    assert_eq!(seen.checkpoint, next);
    assert_eq!(seen.state, State::Live);
    let (v, _) = c
        .get("bank", "balance", Some(&json!(["bob"])), ReadAt::default())
        .await
        .unwrap();
    assert_eq!(v, Some(json!(75)));
    let (v, _) = c
        .get("bank", "total", None, ReadAt::default())
        .await
        .unwrap();
    assert_eq!(v, Some(json!(158)));
    let (v, _) = c
        .get("bank", "seen", Some(&json!("carol")), ReadAt::default())
        .await
        .unwrap();
    assert_eq!(v, Some(json!(true)));
    let (v, _) = c
        .get("bank", "seen", Some(&json!("dave")), ReadAt::default())
        .await
        .unwrap();
    assert_eq!(v, None);
    let (row, _) = c
        .get("bank", "txns", Some(&json!(next - 3)), ReadAt::default())
        .await
        .unwrap();
    assert_eq!(
        row,
        Some(json!({"id": next - 3, "account": "alice", "amount": -30, "note": "withdrawal"}))
    );
    let (el, _) = c
        .get(
            "bank",
            "feed",
            Some(&json!(["alice", 1])),
            ReadAt::default(),
        )
        .await
        .unwrap();
    assert_eq!(el, Some(json!(-30)));
    let (score, _) = c
        .get("bank", "top", Some(&json!(["bob"])), ReadAt::default())
        .await
        .unwrap();
    assert_eq!(score, Some(json!(75)));

    // Scan: whole map, a prefix on the list, paging in both directions.
    let p = c.scan("bank", "balance", Scan::default()).await.unwrap();
    assert_eq!(
        p.entries
            .iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect::<Vec<_>>(),
        vec![
            (json!(["alice"]), json!(70)),
            (json!(["bob"]), json!(75)),
            (json!(["carol"]), json!(13))
        ]
    );
    assert!(p.next.is_none());
    let p = c
        .scan(
            "bank",
            "feed",
            Scan {
                prefix: Some(json!(["alice"])),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        p.entries
            .iter()
            .map(|e| e.value.clone())
            .collect::<Vec<_>>(),
        vec![json!(100), json!(-30)]
    );
    assert_eq!(p.entries[0].key, json!(["alice", 0]));
    let mut all = Vec::new();
    let mut after = None;
    let mut pages = 0;
    loop {
        let p = c
            .scan(
                "bank",
                "txns",
                Scan {
                    limit: 2,
                    after: after.clone(),
                    reverse: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        pages += 1;
        all.extend(p.entries.into_iter().map(|e| e.key));
        match p.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(
        all,
        (0..5)
            .rev()
            .map(|i| json!([next - 5 + i]))
            .collect::<Vec<_>>()
    );

    // Lookup through both indexes, one with a nullable column.
    let p = c
        .lookup(
            "bank",
            "txns",
            "by_account",
            Scan {
                prefix: Some(json!(["alice"])),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        p.entries
            .iter()
            .map(|e| e.value["amount"].clone())
            .collect::<Vec<_>>(),
        vec![json!(-30), json!(100)]
    );
    assert_eq!(p.entries[0].key, json!(["alice", -30]));
    let p = c
        .lookup(
            "bank",
            "txns",
            "by_note",
            Scan {
                prefix: Some(json!([null])),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(p.entries.len(), 4, "the deposits have no note");
    let p = c
        .lookup(
            "bank",
            "txns",
            "by_note",
            Scan {
                prefix: Some(json!(["withdrawal"])),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(p.entries.len(), 1);

    // Range and top on the sorted set.
    let p = c.top("bank", "top", None, 2).await.unwrap();
    assert_eq!(
        p.entries
            .iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect::<Vec<_>>(),
        vec![(json!(["bob"]), json!(75)), (json!(["alice"]), json!(70))]
    );
    let p = c
        .range(
            "bank",
            "top",
            None,
            Some(&json!(20)),
            Some(&json!(70)),
            None,
            0,
            false,
            ReadAt::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        p.entries.iter().map(|e| e.key.clone()).collect::<Vec<_>>(),
        vec![json!(["alice"])]
    );

    // Buckets.
    let p = c
        .buckets(
            "bank",
            "per_minute",
            Some(&json!("alice")),
            "hour",
            None,
            None,
            None,
            0,
            false,
            ReadAt::default(),
        )
        .await
        .unwrap();
    assert_eq!(p.entries.len(), 1);
    assert_eq!(p.entries[0].value["count"], json!(2));
    assert_eq!(p.entries[0].value["sum"], json!(70));
    assert_eq!(p.entries[0].value["min"], json!(-30));
    assert!(
        p.entries[0].value["start"].is_string(),
        "{:?}",
        p.entries[0].value
    );
    let err = c
        .buckets(
            "bank",
            "per_minute",
            Some(&json!("alice")),
            "day",
            None,
            None,
            None,
            0,
            false,
            ReadAt::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::InvalidArgument(_)), "{err}");
    f.shutdown().await;
}

#[tokio::test]
async fn bad_requests_are_named() {
    let f = Fixture::start(app()).await;
    seed(&f).await;
    let c = client(&f).await;
    let cases: Vec<(&str, &str, Option<serde_json::Value>)> = vec![
        ("nope", "balance", Some(json!("alice"))),
        ("bank", "nope", Some(json!("alice"))),
    ];
    for (p, coll, key) in cases {
        let err = c
            .get(p, coll, key.as_ref(), ReadAt::default())
            .await
            .unwrap_err();
        assert!(matches!(err, ClientError::NotFound(_)), "{p}/{coll}: {err}");
    }
    let bad: Vec<(&str, serde_json::Value, &str)> = vec![
        ("balance", json!(5), "expected a string"),
        ("balance", json!(["a", "b"]), "the key has 1"),
        ("txns", json!("x"), "expected an unsigned integer"),
        ("balance", json!([]), "a whole key has 1 parts"),
    ];
    for (coll, key, needle) in bad {
        let err = c
            .get("bank", coll, Some(&key), ReadAt::default())
            .await
            .unwrap_err();
        match err {
            ClientError::InvalidArgument(m) => {
                assert!(m.contains(needle), "{coll} {key}: {m:?} lacks {needle:?}")
            }
            other => panic!("{coll} {key}: {other}"),
        }
    }
    let err = c
        .lookup("bank", "txns", "nope", Scan::default())
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::NotFound(_)), "{err}");
    let err = c
        .scan(
            "bank",
            "balance",
            Scan {
                after: Some(b"junk".to_vec()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::InvalidArgument(_)), "{err}");
    f.shutdown().await;
}

#[tokio::test]
async fn read_your_writes_waits_and_times_out() {
    let f = Fixture::start(app()).await;
    let c = client(&f).await;
    let next = f.commit("deposit", &[&deposit("zed", 5)]).await;
    let (v, seen) = c
        .get(
            "bank",
            "balance",
            Some(&json!("zed")),
            ReadAt::at_least(next),
        )
        .await
        .unwrap();
    assert_eq!(v, Some(json!(5)));
    assert!(seen.checkpoint >= next);
    let err = c
        .get(
            "bank",
            "balance",
            Some(&json!("zed")),
            ReadAt {
                at_least: Some(next + 1000),
                wait: Some(Duration::from_millis(300)),
            },
        )
        .await
        .unwrap_err();
    match err {
        ClientError::BoundTimeout {
            checkpoint,
            at_least,
            ..
        } => {
            assert_eq!(at_least, next + 1000);
            assert!(checkpoint >= next);
        }
        other => panic!("{other}"),
    }
    f.shutdown().await;
}

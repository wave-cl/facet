//! Catch-up, live delivery, exactly-once across restarts, read your
//! writes, the watch feed.

use std::time::Duration;

use facet::{OnError, State};

use crate::common::*;

#[tokio::test]
async fn catches_up_then_follows_live() {
    let d = tmp();
    let chron = Chron::start().await;
    let mut head = 0;
    for i in 0..30 {
        head = chron
            .commit("t", &[&i.to_string(), &(i + 1).to_string()])
            .await;
    }
    chron.commit("other", &["ignored"]).await; // not the source
    let (app, n, sum) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    wait_applied(&rt, "c", head).await;
    assert_eq!(counter_value(&rt, &n), 60);
    assert_eq!(
        sum_value(&rt, &sum),
        (0..30).map(|i| 2 * i + 1).sum::<i64>()
    );
    let s = wait_state(&rt, "c", State::Live).await;
    assert_eq!(s.rebuilt, facet::Rebuilt::Fresh);
    assert!(s.batches >= 1 && s.events == 60, "{s:?}");

    // Live: new commits arrive without a restart; read your writes.
    let next = chron.commit("t", &["100"]).await;
    let cp = rt
        .handle()
        .wait_for("c", next, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(cp.next >= next);
    assert_eq!(counter_value(&rt, &n), 61);
    // The checkpoint remembers the last event applied.
    assert_eq!(cp.last.unwrap().position, next - 1);
    // A wait past what exists runs out.
    let err = rt
        .handle()
        .wait_for("c", next + 1000, Duration::from_millis(200))
        .await
        .unwrap_err();
    assert!(
        matches!(err, facet::RuntimeError::BoundTimeout { .. }),
        "{err}"
    );
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn restarts_at_random_points_apply_every_event_once() {
    let d = tmp();
    let chron = Chron::start().await;
    let client = chron.client().await;
    let (app, n, sum) = counter_app("1", OnError::Fail);
    let total = 400;
    let mut committed = 0i64;
    let mut expected_sum = 0i64;
    let mut rt = start_rt(app.clone(), d.path(), &chron).await;
    let mut restarts = 0;
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    while committed < total {
        let k = (seed % 7 + 1) as i64;
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let payloads: Vec<String> = (0..k).map(|i| (committed + i).to_string()).collect();
        let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
        commit(&client, "t", &refs).await;
        expected_sum += (committed..committed + k).sum::<i64>();
        committed += k;
        if seed.is_multiple_of(3) {
            // Stop the runtime wherever it is and start it again.
            rt.shutdown().await.unwrap();
            restarts += 1;
            rt = start_rt(app.clone(), d.path(), &chron).await;
        }
    }
    let head = client.head().await.unwrap();
    wait_applied(&rt, "c", head).await;
    // The last commit may overshoot `total` by up to six events.
    assert!(committed >= total && committed < total + 7);
    assert_eq!(
        counter_value(&rt, &n),
        committed,
        "after {restarts} restarts"
    );
    assert_eq!(sum_value(&rt, &sum), expected_sum);
    assert!(restarts > 0, "the seed never restarted; rerun");
    let s = rt.status("c").unwrap();
    assert_eq!(s.rebuilt, facet::Rebuilt::No);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn batches_see_their_own_writes_and_watchers_see_each_batch() {
    let d = tmp();
    let chron = Chron::start().await;
    let (app, _n, sum) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    let mut rx = rt.handle().watch("c").unwrap();
    // One transaction of 50 numbers lands as one batch: the running sum
    // is right only if every handler saw the previous handler's put.
    let payloads: Vec<String> = (1..=50).map(|i| i.to_string()).collect();
    let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
    let next = chron.commit("t", &refs).await;
    rt.handle()
        .wait_for("c", next, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(sum_value(&rt, &sum), 1275);
    let summary = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.checkpoint.next, next);
    assert!(summary.collection("n").is_some());
    assert!(summary.collection("sum").is_some());
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn pause_holds_the_checkpoint_and_resume_catches_up() {
    let d = tmp();
    let chron = Chron::start().await;
    let (app, n, _) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    let first = chron.commit("t", &["1"]).await;
    wait_applied(&rt, "c", first).await;
    rt.pause("c").await.unwrap();
    wait_state(&rt, "c", State::Paused).await;
    let second = chron.commit("t", &["2", "3"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(rt.status("c").unwrap().checkpoint.next, first);
    assert_eq!(counter_value(&rt, &n), 1);
    rt.resume("c").await.unwrap();
    wait_applied(&rt, "c", second).await;
    assert_eq!(counter_value(&rt, &n), 3);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_chron_restart_is_survived() {
    let d = tmp();
    let mut chron = Chron::start().await;
    let (app, n, _) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    let first = chron.commit("t", &["1"]).await;
    wait_applied(&rt, "c", first).await;
    chron.restart().await;
    let second = chron.commit("t", &["2"]).await;
    wait_applied(&rt, "c", second).await;
    assert_eq!(counter_value(&rt, &n), 2);
    let s = rt.status("c").unwrap();
    assert!(s.reconnects >= 1, "{s:?}");
    assert_eq!(s.rebuilt, facet::Rebuilt::Fresh);
    rt.shutdown().await.unwrap();
}

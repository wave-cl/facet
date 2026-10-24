//! What rebuilds a projection and what does not.

use facet::{OnError, Rebuilt, State};

use crate::common::*;

#[tokio::test]
async fn a_changed_version_rebuilds_a_changed_policy_does_not() {
    let d = tmp();
    let chron = Chron::start().await;
    let head = chron.commit("t", &["1", "2", "3"]).await;
    let (app, n, _) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    wait_applied(&rt, "c", head).await;
    rt.shutdown().await.unwrap();

    let (app, n2, _) = counter_app("1", OnError::Skip);
    let rt = start_rt(app, d.path(), &chron).await;
    assert_eq!(rt.status("c").unwrap().rebuilt, Rebuilt::No);
    assert_eq!(rt.status("c").unwrap().checkpoint.next, head);
    assert_eq!(counter_value(&rt, &n2), 3);
    rt.shutdown().await.unwrap();

    let (app, n3, _) = counter_app("2", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    assert_eq!(rt.status("c").unwrap().rebuilt, Rebuilt::VersionChanged);
    wait_applied(&rt, "c", head).await;
    assert_eq!(counter_value(&rt, &n3), 3, "recomputed from the start");
    let _ = n;
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_operator_rebuild_starts_over_and_catches_up() {
    let d = tmp();
    let chron = Chron::start().await;
    let head = chron.commit("t", &["1", "2", "3"]).await;
    let (app, n, _) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    wait_applied(&rt, "c", head).await;
    let mut rx = rt.handle().watch("c").unwrap();
    rt.rebuild("c").await.unwrap();
    // The rebuild's summary says everything changed, then the catch-up
    // lands the same count again.
    let s = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.checkpoint.next, 0);
    wait_applied(&rt, "c", head).await;
    assert_eq!(counter_value(&rt, &n), 3);
    let more = chron.commit("t", &["4"]).await;
    wait_applied(&rt, "c", more).await;
    assert_eq!(counter_value(&rt, &n), 4);
    assert_eq!(rt.status("c").unwrap().state, State::Live);
    rt.shutdown().await.unwrap();
}

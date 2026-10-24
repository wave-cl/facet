//! Retention that passes a projection's checkpoint.

use facet::{OnError, State};

use crate::common::*;

#[tokio::test]
async fn a_floor_past_the_checkpoint_fails_the_projection_and_a_rebuild_starts_at_the_floor() {
    let d = tmp();
    let dir = tmp();
    let mut opts = chrond::Options::new(dir.path().join("data"), "127.0.0.1:0".parse().unwrap());
    opts.fsync = false;
    opts.segment_max_bytes = Some(2048);
    let running = chrond::start(opts).await.expect("chrond starts");
    let chron = Chron {
        addr: running.local_addr,
        dir,
        running: Some(running),
    };
    let client = chron.client().await;
    let payloads: Vec<String> = (0..100).map(|i| format!("{i:08}")).collect();
    let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
    let mut h1 = 0;
    for _ in 0..3 {
        h1 = commit(&client, "t", &refs).await;
    }
    let (app, n, _) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    wait_applied(&rt, "c", h1).await;
    rt.shutdown().await.unwrap();

    let mut h2 = 0;
    for _ in 0..5 {
        h2 = commit(&client, "t", &refs).await;
    }
    let retained = client.retain(h2).await.unwrap();
    let floor = client.info().await.unwrap().floor;
    assert!(
        floor > h1,
        "floor {floor} did not pass the checkpoint {h1} ({retained:?})"
    );

    let (app, n2, _) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    let s = wait_state(&rt, "c", State::Failed).await;
    let err = s.error.unwrap();
    assert!(
        err.contains(&format!("floor is {floor}")) && err.contains(&format!("apply {h1}")),
        "{err}"
    );
    assert_eq!(s.checkpoint.next, h1, "the checkpoint stands");
    rt.rebuild("c").await.unwrap();
    // Events after the retention, so the rebuilt projection has a batch
    // to commit whatever the floor rounded to.
    let h3 = commit(&client, "t", &["a", "b"]).await;
    wait_applied(&rt, "c", h3).await;
    assert_eq!(
        counter_value(&rt, &n2),
        (h3 - floor) as i64,
        "the events from the floor on"
    );
    assert_eq!(rt.status("c").unwrap().state, State::Live);
    // A restart with nothing applied below the floor is fine: it starts
    // at the floor again.
    rt.shutdown().await.unwrap();
    let (app, n3, _) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    let h4 = commit(&client, "t", &["c"]).await;
    wait_applied(&rt, "c", h4).await;
    assert_eq!(counter_value(&rt, &n3), (h4 - floor) as i64);
    let _ = n;
    rt.shutdown().await.unwrap();
}

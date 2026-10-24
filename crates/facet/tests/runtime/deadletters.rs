//! A failing handler under each error policy.

use facet::{DEAD_LETTERS, DeadLetter, OnError, State};

use crate::common::*;

#[tokio::test]
async fn fail_stops_before_the_event_and_resume_retries_it() {
    let d = tmp();
    let chron = Chron::start().await;
    let (app, n, _) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    let good = chron.commit("t", &["1", "2"]).await;
    wait_applied(&rt, "c", good).await;
    let bad_next = chron.commit("t", &["bad"]).await;
    let s = wait_state(&rt, "c", State::Failed).await;
    assert_eq!(
        s.checkpoint.next, good,
        "nothing of the failed batch landed"
    );
    let err = s.error.unwrap();
    assert!(
        err.contains(&format!("event {}", bad_next - 1)) && err.contains("bad payload"),
        "{err}"
    );
    assert_eq!(counter_value(&rt, &n), 2);
    // More events queue behind it.
    let after = chron.commit("t", &["3"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(rt.status("c").unwrap().checkpoint.next, good);
    // Resume retries the same event and fails the same way.
    rt.resume("c").await.unwrap();
    let s = wait_state(&rt, "c", State::Failed).await;
    assert_eq!(s.checkpoint.next, good);
    assert!(after > bad_next);
    assert_eq!(rt.status("c").unwrap().dead_letters, 0);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn skip_records_the_dead_letter_and_goes_on() {
    let d = tmp();
    let chron = Chron::start().await;
    let (app, n, sum) = counter_app("1", OnError::Skip);
    let rt = start_rt(app, d.path(), &chron).await;
    // bad and panic inside one batch, good events around them.
    let next = chron.commit("t", &["1", "bad", "2", "panic", "3"]).await;
    wait_applied(&rt, "c", next).await;
    assert_eq!(counter_value(&rt, &n), 3);
    assert_eq!(sum_value(&rt, &sum), 6);
    let s = rt.status("c").unwrap();
    assert_eq!(s.dead_letters, 2);
    assert_eq!(s.state, State::Live);
    let snap = rt.handle().snapshot("c").unwrap();
    let dead = snap
        .table::<u64, DeadLetter>(DEAD_LETTERS)
        .unwrap()
        .scan()
        .run()
        .unwrap()
        .items;
    assert_eq!(dead.len(), 2);
    assert_eq!(dead[0].position, next - 4);
    assert!(dead[0].error.contains("bad payload"), "{}", dead[0].error);
    assert_eq!(dead[1].position, next - 2);
    assert!(
        dead[1].error.contains("panic") && dead[1].error.contains("a handler panic"),
        "{}",
        dead[1].error
    );
    assert_eq!(dead[0].tags, vec![bytes::Bytes::from_static(b"t")]);
    assert!(dead[0].aggregate.is_some());
    rt.shutdown().await.unwrap();
}

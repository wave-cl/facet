//! Sources over several event types: the chron stream facet defines.

use facet::{App, CounterHandle, HandlerError, OnError, Projection, Source, State, StreamPolicy};

use crate::common::*;

/// A projection `ab` counting events of types `a` and `b`.
fn ab_app(source: Source, policy: StreamPolicy) -> (App, CounterHandle<(), i64>) {
    let (b, n) = Projection::new("ab")
        .source(source)
        .stream_policy(policy)
        .on_error(OnError::Fail)
        .collections(|c| c.counter::<(), i64>("n"));
    let (n1, n2) = (n.clone(), n.clone());
    let p = b
        .on_raw("a", move |cx, _| {
            cx.counter(&n1)?.add(&(), 1)?;
            Ok::<(), HandlerError>(())
        })
        .on_raw("b", move |cx, _| {
            cx.counter(&n2)?.add(&(), 1)?;
            Ok(())
        })
        .build()
        .unwrap();
    (App::new().projection(p), n)
}

fn count(rt: &facet::Runtime, n: &CounterHandle<(), i64>) -> i64 {
    rt.handle()
        .snapshot("ab")
        .unwrap()
        .counter::<(), i64>(n.name())
        .unwrap()
        .get(&())
        .unwrap()
        .unwrap_or(0)
}

#[tokio::test]
async fn an_auto_source_over_two_types_defines_a_stream() {
    let d = tmp();
    let chron = Chron::start().await;
    chron.commit("a", &["1"]).await;
    chron.commit("b", &["2"]).await;
    let head = chron.commit("x", &["3"]).await;
    let (app, n) = ab_app(Source::Auto, StreamPolicy::Refuse);
    let rt = start_rt(app, d.path(), &chron).await;
    wait_applied(&rt, "ab", head - 1).await;
    let streams = chron.client().await.streams().await.unwrap();
    let s = streams
        .iter()
        .find(|s| s.name == "facet-ab")
        .expect("the stream exists");
    assert_eq!(
        s.tags,
        vec![
            bytes::Bytes::from_static(b"a"),
            bytes::Bytes::from_static(b"b")
        ]
    );
    assert!(s.ready);
    assert_eq!(count(&rt, &n), 2);
    let next = chron.commit("b", &["4"]).await;
    rt.handle()
        .wait_for("ab", next, std::time::Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(count(&rt, &n), 3);
    assert_eq!(
        rt.handle().definition("ab").unwrap().auto_stream_name(),
        "facet-ab"
    );
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_stream_with_other_tags_is_refused_or_recreated() {
    let d = tmp();
    let chron = Chron::start().await;
    let client = chron.client().await;
    client.create_stream("facet-ab", vec!["x"]).await.unwrap();
    chron.commit("a", &["1"]).await;
    let (app, _) = ab_app(Source::Auto, StreamPolicy::Refuse);
    let rt = start_rt(app, d.path(), &chron).await;
    let s = wait_state(&rt, "ab", State::Failed).await;
    let err = s.error.unwrap();
    assert!(
        err.contains("facet-ab") && err.contains("[x]") && err.contains("[a, b]"),
        "{err}"
    );
    rt.shutdown().await.unwrap();
    let streams = client.streams().await.unwrap();
    assert_eq!(
        streams[0].tags,
        vec![bytes::Bytes::from_static(b"x")],
        "left alone"
    );

    let (app, n) = ab_app(Source::Auto, StreamPolicy::Recreate);
    let rt = start_rt(app, d.path(), &chron).await;
    wait_applied(&rt, "ab", 1).await;
    assert_eq!(count(&rt, &n), 1);
    let streams = client.streams().await.unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(
        streams[0].tags,
        vec![
            bytes::Bytes::from_static(b"a"),
            bytes::Bytes::from_static(b"b")
        ]
    );
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_deleted_stream_is_redefined_when_auto_and_fatal_when_explicit() {
    let d = tmp();
    let chron = Chron::start().await;
    let client = chron.client().await;
    let (app, n) = ab_app(Source::Auto, StreamPolicy::Refuse);
    let rt = start_rt(app, d.path(), &chron).await;
    let first = chron.commit("a", &["1"]).await;
    wait_applied(&rt, "ab", first).await;
    assert!(client.delete_stream("facet-ab").await.unwrap());
    let second = chron.commit("b", &["2"]).await;
    wait_applied(&rt, "ab", second).await;
    assert_eq!(count(&rt, &n), 2);
    assert!(
        client
            .streams()
            .await
            .unwrap()
            .iter()
            .any(|s| s.name == "facet-ab"),
        "defined again"
    );
    assert!(rt.handle().sources()[0].reconnects >= 1);
    rt.shutdown().await.unwrap();

    client.create_stream("ops", vec!["a", "b"]).await.unwrap();
    let e = tmp();
    let (app, n) = ab_app(Source::stream("ops"), StreamPolicy::Refuse);
    let rt = start_rt(app, e.path(), &chron).await;
    wait_applied(&rt, "ab", second).await;
    assert_eq!(count(&rt, &n), 2);
    assert!(client.delete_stream("ops").await.unwrap());
    let s = wait_state(&rt, "ab", State::Failed).await;
    assert!(s.error.unwrap().contains("gone"), "names the deletion");
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_explicit_stream_that_does_not_exist_fails_the_projection() {
    let d = tmp();
    let chron = Chron::start().await;
    let (app, _) = ab_app(Source::stream("nope"), StreamPolicy::Refuse);
    let rt = start_rt(app, d.path(), &chron).await;
    let s = wait_state(&rt, "ab", State::Failed).await;
    assert!(s.error.unwrap().contains("does not exist"));
    rt.shutdown().await.unwrap();
}

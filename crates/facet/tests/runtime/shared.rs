//! Two projections on one source share one subscription.

use std::io::{Read, Write};

use facet::{App, HandlerError, OnError, Projection, Source};

use crate::common::*;

fn counting(name: &str) -> Projection {
    let (b, n) = Projection::new(name)
        .source(Source::tag("t"))
        .on_error(OnError::Fail)
        .collections(|c| c.counter::<(), i64>("n"));
    b.on_raw("t", move |cx, _| {
        cx.counter(&n)?.add(&(), 1)?;
        Ok::<(), HandlerError>(())
    })
    .build()
    .unwrap()
}

/// `GET /metrics` over plain HTTP/1.0, off the runtime's thread (the
/// daemon answering runs on it).
async fn scrape(addr: std::net::SocketAddr) -> String {
    tokio::task::spawn_blocking(move || {
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .unwrap();
        write!(
            s,
            "GET /metrics HTTP/1.0\r\nHost: x\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        String::from_utf8_lossy(&out).into_owned()
    })
    .await
    .unwrap()
}

fn open_subscriptions(metrics: &str) -> u64 {
    metrics
        .lines()
        .find(|l| l.starts_with("chron_streams_open{") && l.contains("Subscribe"))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

#[tokio::test]
async fn one_subscription_feeds_every_projection_on_the_source() {
    let d = tmp();
    let dir = tmp();
    let mut opts = chrond::Options::new(dir.path().join("data"), "127.0.0.1:0".parse().unwrap());
    opts.fsync = false;
    opts.metrics_listen = Some("127.0.0.1:0".parse().unwrap());
    let running = chrond::start(opts).await.expect("chrond starts");
    let metrics_addr = running.metrics_addr.unwrap();
    let chron = Chron {
        addr: running.local_addr,
        dir,
        running: Some(running),
    };
    let head = chron.commit("t", &["1", "2", "3"]).await;
    let app = App::new()
        .projection(counting("c1"))
        .projection(counting("c2"))
        .projection(counting("c3"));
    let rt = start_rt(app, d.path(), &chron).await;
    for name in ["c1", "c2", "c3"] {
        wait_applied(&rt, name, head).await;
    }
    let metrics = scrape(metrics_addr).await;
    assert_eq!(open_subscriptions(&metrics), 1, "{metrics}");
    assert_eq!(rt.handle().sources().len(), 1);
    let next = chron.commit("t", &["4"]).await;
    for name in ["c1", "c2", "c3"] {
        rt.handle()
            .wait_for(name, next, std::time::Duration::from_secs(10))
            .await
            .unwrap();
        let count = rt
            .handle()
            .snapshot(name)
            .unwrap()
            .counter::<(), i64>("n")
            .unwrap()
            .get(&())
            .unwrap();
        assert_eq!(count, Some(4), "{name}");
    }
    rt.shutdown().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(open_subscriptions(&scrape(metrics_addr).await), 0);
}

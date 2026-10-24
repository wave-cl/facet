//! The Prometheus endpoint.

use std::io::{Read, Write};

use crate::common::*;

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

/// The value of the first sample of `name` whose label set holds every
/// one of `labels` (given as `name="value"`, in any order).
fn sample(metrics: &str, name: &str, labels: &str) -> Option<f64> {
    let wanted: Vec<&str> = labels.split(',').filter(|l| l.contains('=')).collect();
    metrics
        .lines()
        .filter(|l| l.starts_with(name))
        .find(|l| {
            let open = l.find('{');
            let close = l.find('}');
            match (open, close) {
                (Some(o), Some(c)) => {
                    let set = &l[o + 1..c];
                    let has: Vec<&str> = set.split(',').collect();
                    wanted.iter().all(|w| has.contains(w))
                }
                _ => wanted.is_empty(),
            }
        })
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

#[tokio::test]
async fn the_metrics_describe_projections_sources_and_calls() {
    let f = Fixture::start_with(app(), |o| {
        o.metrics_listen = Some("127.0.0.1:0".parse().unwrap())
    })
    .await;
    let next = seed(&f).await;
    let c = client(&f).await;
    c.get("bank", "total", None, Default::default())
        .await
        .unwrap();
    c.projections().await.unwrap();
    let _ = c.get("nope", "x", None, Default::default()).await;
    let m = scrape(f.metrics_addr()).await;
    assert!(
        m.starts_with("HTTP/1.0 200") || m.starts_with("HTTP/1.1 200"),
        "{m}"
    );
    assert_eq!(
        sample(&m, "facet_projection_checkpoint", "projection=\"bank\""),
        Some(next as f64),
        "{m}"
    );
    assert_eq!(
        sample(&m, "facet_projection_lag_events", "projection=\"bank\""),
        Some(0.0)
    );
    assert_eq!(
        sample(
            &m,
            "facet_projection_state",
            "projection=\"bank\",state=\"live\""
        ),
        Some(1.0)
    );
    assert_eq!(
        sample(
            &m,
            "facet_projection_state",
            "projection=\"bank\",state=\"failed\""
        ),
        Some(0.0)
    );
    assert_eq!(
        sample(
            &m,
            "facet_projection_dead_letters",
            "projection=\"fragile\""
        ),
        Some(1.0)
    );
    assert!(sample(&m, "facet_projection_events_total", "projection=\"bank\"").unwrap() >= 5.0);
    assert_eq!(
        sample(&m, "facet_source_connected", "tag:deposit"),
        Some(1.0)
    );
    assert_eq!(
        sample(&m, "facet_chron_head", "facet_chron_head"),
        Some(next as f64)
    );
    assert_eq!(sample(&m, "facet_fatal", "facet_fatal"), Some(0.0));
    assert_eq!(
        sample(
            &m,
            "facet_rpc_calls_total",
            "method=\"/facet.v1.Query/Get\",code=\"OK\""
        ),
        Some(1.0),
        "{m}"
    );
    assert_eq!(
        sample(
            &m,
            "facet_rpc_calls_total",
            "method=\"/facet.v1.Query/Get\",code=\"NotFound\""
        ),
        Some(1.0)
    );
    assert_eq!(
        sample(
            &m,
            "facet_rpc_calls_total",
            "method=\"/facet.v1.Admin/ListProjections\",code=\"OK\""
        ),
        Some(1.0)
    );
    assert!(m.contains("facet_rpc_duration_seconds_bucket"));
    assert!(sample(&m, "facet_start_time_seconds", "facet_start_time_seconds").unwrap() > 1.0e9);
    f.shutdown().await;
}

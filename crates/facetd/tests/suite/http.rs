//! The HTTP/JSON gateway: every route answers and is counted, errors
//! carry their code, the SSE watch streams, tokens apply.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use serde_json::{Value, json};

use crate::common::*;

/// One HTTP/1.0 request; the status line and the parsed JSON body.
async fn call(addr: SocketAddr, method: &str, path: &str, token: Option<&str>) -> (u16, Value) {
    let method = method.to_owned();
    let path = path.to_owned();
    let token = token.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let auth = token
            .map(|t| format!("Authorization: Bearer {t}\r\n"))
            .unwrap_or_default();
        write!(
            s,
            "{method} {path} HTTP/1.0\r\nHost: x\r\n{auth}Connection: close\r\n\r\n"
        )
        .unwrap();
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        let text = String::from_utf8_lossy(&out).into_owned();
        let status: u16 = text
            .split(' ')
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("");
        let json = serde_json::from_str(body).unwrap_or(Value::String(body.to_owned()));
        (status, json)
    })
    .await
    .unwrap()
}

fn with_http() -> impl FnOnce(&mut facetd::Options) {
    |o| {
        o.http_listen = Some("127.0.0.1:0".parse().unwrap());
        o.metrics_listen = Some("127.0.0.1:0".parse().unwrap());
    }
}

#[tokio::test]
async fn every_route_answers_and_is_counted() {
    let f = Fixture::start_with(app(), with_http()).await;
    let next = seed(&f).await;
    let h = f.http_addr();
    let enc = |j: Value| urlencoding(&j.to_string());

    let (s, b) = call(h, "GET", "/v1/status", None).await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["projections"]["live"], 2);
    assert_eq!(b["sources"][0]["connected"], true);
    let (s, b) = call(h, "GET", "/v1/projections", None).await;
    assert_eq!(s, 200);
    assert_eq!(b["projections"][0]["name"], "bank");
    assert_eq!(b["projections"][0]["state"], "live");
    let (s, b) = call(h, "GET", "/v1/projections/bank", None).await;
    assert_eq!(s, 200);
    assert_eq!(b["checkpoint"], next);
    assert_eq!(b["collections"][4]["key_shape"]["names"], json!(["id"]));
    let (s, b) = call(h, "GET", "/v1/projections/fragile/dead-letters", None).await;
    assert_eq!(s, 200);
    assert_eq!(b["dead_letters"][0]["error"], "unlucky");
    assert_eq!(b["dead_letters"][0]["tags"], json!(["deposit"]));

    let (s, b) = call(
        h,
        "GET",
        &format!(
            "/v1/projections/bank/balance/get?key={}",
            enc(json!("alice"))
        ),
        None,
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(
        b,
        json!({"found": true, "value": 70, "seen": {"checkpoint": next, "state": "live", "chron_head": next}})
    );
    let (s, b) = call(h, "GET", "/v1/projections/bank/total/get", None).await;
    assert_eq!(s, 200);
    assert_eq!(b["value"], 158);
    let (s, b) = call(h, "GET", "/v1/projections/bank/balance/scan?limit=2", None).await;
    assert_eq!(s, 200);
    assert_eq!(b["entries"].as_array().unwrap().len(), 2);
    assert_eq!(b["entries"][0], json!({"key": ["alice"], "value": 70}));
    let cursor = b["next"].as_str().expect("a cursor").to_owned();
    let (s, b) = call(
        h,
        "GET",
        &format!(
            "/v1/projections/bank/balance/scan?after={}",
            urlencoding(&cursor)
        ),
        None,
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["entries"], json!([{"key": ["carol"], "value": 13}]));
    assert!(b["next"].is_null());
    let (s, b) = call(
        h,
        "GET",
        &format!(
            "/v1/projections/bank/txns/lookup?index=by_account&prefix={}",
            enc(json!(["alice"]))
        ),
        None,
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["entries"].as_array().unwrap().len(), 2);
    let (s, b) = call(h, "GET", "/v1/projections/bank/top/top?limit=1", None).await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["entries"], json!([{"key": ["bob"], "value": 75}]));
    let (s, b) = call(
        h,
        "GET",
        &format!(
            "/v1/projections/bank/top/range?min={}&max={}",
            enc(json!(60)),
            enc(json!(80))
        ),
        None,
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["entries"].as_array().unwrap().len(), 2);
    let (s, b) = call(
        h,
        "GET",
        &format!(
            "/v1/projections/bank/per_minute/buckets?key={}&width=hour",
            enc(json!("alice"))
        ),
        None,
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b["entries"][0]["value"]["sum"], 70);
    assert!(b["entries"][0]["key"][0].is_string());

    // Admin routes without an admin token configured are open.
    let (s, b) = call(h, "POST", "/v1/projections/bank/pause", None).await;
    assert_eq!(s, 200, "{b}");
    f.wait_state("bank", facet::State::Paused, Duration::from_secs(10))
        .await;
    let (s, b) = call(h, "POST", "/v1/projections/bank/resume", None).await;
    assert_eq!(s, 200, "{b}");
    let (s, b) = call(h, "POST", "/v1/projections/bank/rebuild", None).await;
    assert_eq!(s, 200, "{b}");
    f.wait_applied("bank", next, Duration::from_secs(10)).await;
    let (s, b) = call(h, "DELETE", "/v1/projections/bank", None).await;
    assert_eq!(s, 409, "{b}");
    assert_eq!(b["code"], "FailedPrecondition");

    // Errors carry their code.
    let (s, b) = call(h, "GET", "/v1/projections/nope", None).await;
    assert_eq!(s, 404);
    assert_eq!(b["code"], "NotFound");
    let (s, b) = call(
        h,
        "GET",
        &format!("/v1/projections/bank/balance/get?key={}", enc(json!(5))),
        None,
    )
    .await;
    assert_eq!(s, 400, "{b}");
    assert_eq!(b["code"], "InvalidArgument");
    let (s, b) = call(
        h,
        "GET",
        &format!(
            "/v1/projections/bank/balance/get?key={}&at_least={}&wait_ms=200",
            enc(json!("alice")),
            next + 1000
        ),
        None,
    )
    .await;
    assert_eq!(s, 504, "{b}");
    assert_eq!(b["code"], "Unavailable");
    assert_eq!(b["checkpoint"], next);
    assert_eq!(b["at_least"], next + 1000);
    let (s, b) = call(h, "GET", "/v1/nope", None).await;
    assert_eq!(s, 404);
    assert_eq!(b["code"], "NotFound");
    let (s, _) = call(h, "PUT", "/v1/status", None).await;
    assert_eq!(s, 405);
    let (s, b) = call(h, "GET", "/openapi.json", None).await;
    assert_eq!(s, 200);
    assert_eq!(b["openapi"], "3.1.0");
    let (s, _) = call(h, "GET", "/", None).await;
    assert_eq!(s, 200);

    // Every route was counted.
    let m = {
        let addr = f.metrics_addr();
        tokio::task::spawn_blocking(move || {
            let mut s = TcpStream::connect(addr).unwrap();
            write!(
                s,
                "GET /metrics HTTP/1.0\r\nHost: x\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            let mut out = String::new();
            s.read_to_string(&mut out).unwrap();
            out
        })
        .await
        .unwrap()
    };
    for route in [
        "/v1/status",
        "/v1/projections/{projection}/{collection}/get",
        "/v1/projections/{projection}/rebuild",
        "unmatched",
    ] {
        assert!(
            m.contains(&format!("route=\"{route}\"")),
            "{route} not counted:\n{m}"
        );
    }
    assert!(m.contains("facet_http_requests_total{route=\"/v1/projections/{projection}/{collection}/get\",status=\"504\"}"), "{m}");
    f.shutdown().await;
}

#[tokio::test]
async fn the_sse_watch_streams_changes() {
    let f = Fixture::start_with(app(), with_http()).await;
    seed(&f).await;
    let h = f.http_addr();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(16);
    let reader = tokio::task::spawn_blocking(move || {
        let mut s = TcpStream::connect(h).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        write!(
            s,
            "GET /v1/projections/bank/balance/watch?key=%22bob%22 HTTP/1.1\r\nHost: x\r\n\r\n"
        )
        .unwrap();
        let mut r = BufReader::new(s);
        let mut line = String::new();
        let mut in_body = false;
        while r.read_line(&mut line).unwrap_or(0) > 0 {
            if !in_body {
                if line == "\r\n" {
                    in_body = true;
                }
            } else if let Some(d) = line.strip_prefix("data:") {
                let d = d.trim().to_owned();
                if d != "keep-alive" && tx.blocking_send(d).is_err() {
                    return;
                }
            }
            line.clear();
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let next = f
        .commit("deposit", &[&deposit("bob", 1), &deposit("alice", 1)])
        .await;
    let data = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let ch: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(ch["checkpoint"], next);
    assert_eq!(ch["collection"], "balance");
    assert_eq!(ch["keys"], json!([["bob"]]));
    assert_eq!(ch["all"], false);
    drop(rx);
    f.shutdown().await;
    let _ = reader.await;
}

#[tokio::test]
async fn the_gateway_wants_the_tokens() {
    let f = Fixture::start_with(app(), |o| {
        with_http()(o);
        o.token = Some("reader".into());
        o.admin_token = Some("boss".into());
    })
    .await;
    seed(&f).await;
    let h = f.http_addr();
    let (s, b) = call(h, "GET", "/v1/status", None).await;
    assert_eq!(s, 401);
    assert_eq!(b["code"], "Unauthenticated");
    let (s, _) = call(h, "GET", "/openapi.json", None).await;
    assert_eq!(s, 200, "the document stays open");
    let (s, _) = call(h, "GET", "/v1/status", Some("reader")).await;
    assert_eq!(s, 200);
    let (s, b) = call(h, "POST", "/v1/projections/bank/pause", Some("reader")).await;
    assert_eq!(s, 403, "{b}");
    assert_eq!(b["code"], "PermissionDenied");
    let (s, _) = call(h, "POST", "/v1/projections/bank/pause", Some("boss")).await;
    assert_eq!(s, 200);
    let (s, _) = call(h, "POST", "/v1/projections/bank/resume", Some("boss")).await;
    assert_eq!(s, 200);
    f.shutdown().await;
}

fn urlencoding(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

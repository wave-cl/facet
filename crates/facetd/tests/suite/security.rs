//! Tokens, the admin token, TLS and mTLS.

use facet_client::{Client, ClientConfig, ClientError, Identity, ReadAt, TlsConfig};
use facetd::testing::pki;
use serde_json::json;

use crate::common::*;

#[tokio::test]
async fn a_token_gates_every_call_and_the_admin_token_the_admin_ones() {
    let f = Fixture::start_with(app(), |o| {
        o.token = Some("reader".into());
        o.admin_token = Some("boss".into());
    })
    .await;
    seed(&f).await;
    let anon = Client::connect(f.addr(), ClientConfig::default())
        .await
        .unwrap();
    let err = anon
        .get("bank", "total", None, ReadAt::default())
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Unauthenticated(_)), "{err}");
    assert!(anon.health("").await.unwrap(), "health stays open");
    let reader = Client::connect(
        f.addr(),
        ClientConfig {
            token: Some("reader".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (v, _) = reader
        .get("bank", "total", None, ReadAt::default())
        .await
        .unwrap();
    assert_eq!(v, Some(json!(158)));
    reader.projections().await.unwrap();
    let err = reader.rebuild("bank").await.unwrap_err();
    assert!(matches!(err, ClientError::PermissionDenied(_)), "{err}");
    let err = reader.pause("bank").await.unwrap_err();
    assert!(matches!(err, ClientError::PermissionDenied(_)), "{err}");
    let boss = Client::connect(
        f.addr(),
        ClientConfig {
            token: Some("reader".into()),
            admin_token: Some("boss".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    boss.pause("bank").await.unwrap();
    boss.resume("bank").await.unwrap();
    // The admin token alone also reads.
    let only_boss = Client::connect(
        f.addr(),
        ClientConfig {
            token: Some("boss".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    only_boss
        .get("bank", "total", None, ReadAt::default())
        .await
        .unwrap();
    f.shutdown().await;
}

#[tokio::test]
async fn tls_and_mtls() {
    let pki = pki();
    let f = Fixture::start_with(app(), |o| {
        o.tls = Some(facetd::Tls {
            cert_pem: pki.server_cert_pem.clone().into_bytes(),
            key_pem: pki.server_key_pem.clone().into_bytes(),
            client_ca_pem: Some(pki.ca_pem.clone().into_bytes()),
        });
    })
    .await;
    seed(&f).await;
    let https = f.addr().replace("http://", "https://");
    let plain = Client::connect(&https, ClientConfig::default()).await;
    assert!(
        plain.is_err() || plain.unwrap().status().await.is_err(),
        "no CA, no client cert"
    );
    let no_cert = Client::connect(
        &https,
        ClientConfig {
            tls: Some(TlsConfig {
                ca_pem: Some(pki.ca_pem.clone().into_bytes()),
                identity: None,
                domain: Some("localhost".into()),
            }),
            ..Default::default()
        },
    )
    .await;
    assert!(
        no_cert.is_err() || no_cert.unwrap().status().await.is_err(),
        "mTLS wants a client certificate"
    );
    let c = Client::connect(
        &https,
        ClientConfig {
            tls: Some(TlsConfig {
                ca_pem: Some(pki.ca_pem.clone().into_bytes()),
                identity: Some(Identity {
                    cert_pem: pki.client_cert_pem.clone().into_bytes(),
                    key_pem: pki.client_key_pem.clone().into_bytes(),
                }),
                domain: Some("localhost".into()),
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (v, _) = c
        .get("bank", "total", None, ReadAt::default())
        .await
        .unwrap();
    assert_eq!(v, Some(json!(158)));
    f.shutdown().await;
}

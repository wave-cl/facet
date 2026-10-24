//! The store is bound to one chron database, and the checkpoint to one
//! log.

use std::path::Path;

use facet::{OnError, Rebuilt, RuntimeError};

use crate::common::*;

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dest = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &dest);
        } else {
            std::fs::copy(e.path(), dest).unwrap();
        }
    }
}

/// A temp dir holding a copy of the chron's data at this moment (the
/// daemon must be stopped).
fn snapshot_of(chron: &Chron) -> tempfile::TempDir {
    let t = tmp();
    copy_dir(&chron.dir.path().join("data"), &t.path().join("data"));
    t
}

#[tokio::test]
async fn another_chron_database_at_the_same_address_is_refused_unless_rebound() {
    let d = tmp();
    let port = chrond::testing::free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut chron = Chron::start_in(tmp(), addr).await;
    let head = chron.commit("t", &["1", "2"]).await;
    let (app, n, _) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    wait_applied(&rt, "c", head).await;
    rt.shutdown().await.unwrap();
    chron.shutdown().await;

    // A fresh chron at the same address: another db_id.
    let chron2 = Chron::start_in(tmp(), addr).await;
    let head2 = chron2.commit("t", &["1"]).await;
    let (app, _, _) = counter_app("1", OnError::Fail);
    let err = facet::Runtime::start(app, rt_options(d.path(), &chron2))
        .await
        .expect_err("refused");
    assert!(
        matches!(err, RuntimeError::Store(facet::Error::WrongDatabase { .. })),
        "{err}"
    );

    let (app, _, _) = counter_app("1", OnError::Fail);
    let rt = facet::Runtime::start(app, rt_options(d.path(), &chron2).rebind(true))
        .await
        .unwrap();
    assert_eq!(rt.status("c").unwrap().rebuilt, Rebuilt::DatabaseChanged);
    wait_applied(&rt, "c", head2).await;
    assert_eq!(counter_value(&rt, &n), 1);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_replaced_or_shorter_log_rebuilds_the_projection() {
    let d = tmp();
    let port = chrond::testing::free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut chron = Chron::start_in(tmp(), addr).await;
    chron.commit("t", &["1", "2", "3"]).await;
    chron.shutdown().await;
    let base_a = snapshot_of(&chron);
    let base_b = snapshot_of(&chron);
    chron.restart().await;
    let head = chron.commit("t", &["40", "50"]).await;
    assert_eq!(head, 5);
    let (app, _, sum) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    wait_applied(&rt, "c", 5).await;
    assert_eq!(sum_value(&rt, &sum), 96);
    rt.shutdown().await.unwrap();
    chron.shutdown().await;

    // The same database id, the same length, other events at positions
    // 3 and 4: the fingerprint of the last applied event differs.
    let mut chron = Chron::start_in(base_a, addr).await;
    assert_eq!(chron.commit("t", &["400", "500"]).await, 5);
    let (app, n, sum) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    assert_eq!(
        rt.status("c").unwrap().rebuilt,
        Rebuilt::No,
        "the file itself was fine"
    );
    wait_applied(&rt, "c", 5).await;
    assert_eq!(counter_value(&rt, &n), 5);
    assert_eq!(
        sum_value(&rt, &sum),
        906,
        "rebuilt because the log was replaced"
    );
    rt.shutdown().await.unwrap();
    chron.shutdown().await;

    // The log at three events: shorter than the checkpoint.
    let chron = Chron::start_in(base_b, addr).await;
    let (app, n, sum) = counter_app("1", OnError::Fail);
    let rt = start_rt(app, d.path(), &chron).await;
    wait_applied(&rt, "c", 3).await;
    assert_eq!(counter_value(&rt, &n), 3, "rebuilt from the shorter log");
    assert_eq!(sum_value(&rt, &sum), 6);
    assert_eq!(rt.status("c").unwrap().checkpoint.next, 3);
    rt.shutdown().await.unwrap();
}

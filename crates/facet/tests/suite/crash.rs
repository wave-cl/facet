//! A real crash: a child process applies batches under `Fsync::Always`
//! and reports each committed checkpoint; the parent kills it with
//! SIGKILL at a random moment and checks that reopening keeps every
//! acknowledged batch and that rows and checkpoint agree exactly.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use facet::{Error, Fsync, Options, ProjectionData, Store};

use crate::common::*;

const CHILD_DIR: &str = "FACET_CRASH_CHILD_DIR";
const CHILD_FSYNC: &str = "FACET_CRASH_FSYNC";

fn child_store(dir: &str, fsync: Fsync) -> Store {
    Store::open(dir, DB, Options::new().fsync(fsync)).unwrap()
}

/// Batch `i` (checkpoint `next = i + 1`) writes `m[i] = 2i`, `m[i + K]`
/// for a second key range, and `r = i`: every table moves every batch.
fn batch(p: &ProjectionData, i: u64) {
    p.apply::<_, Error>(cp(i + 1), |w| {
        let mut m = w.map::<u64, u64>("m")?;
        m.put(&i, &(i * 2))?;
        m.put(&(i + 1_000_000), &i)?;
        w.register("r")?.set(&i)
    })
    .unwrap();
}

/// The child. Only the re-executed test binary has the variable set; in a
/// normal run this returns at once.
#[test]
fn child_writer() {
    let Ok(dir) = std::env::var(CHILD_DIR) else {
        return;
    };
    let fsync = match std::env::var(CHILD_FSYNC).as_deref() {
        Ok("never") => Fsync::Never,
        _ => Fsync::Always,
    };
    let s = child_store(&dir, fsync);
    let (p, reg) = s.register(&simple_decl("1")).unwrap();
    let mut out = std::io::stdout().lock();
    let mut i = reg.checkpoint.next;
    loop {
        batch(&p, i);
        // Acknowledged only once this line is out.
        writeln!(out, "{}", i + 1).unwrap();
        out.flush().unwrap();
        i += 1;
    }
}

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Every row is derivable from the checkpoint: the map holds exactly the
/// keys below `next` (in both ranges) with the right values, and the
/// register holds `next - 1`.
pub fn check_invariant(p: &ProjectionData) -> Result<(), String> {
    let snap = p.snapshot().unwrap();
    let next = snap.checkpoint().next;
    let m = snap.map::<u64, u64>("m").unwrap();
    let all = m.scan().run().unwrap().items;
    if all.len() as u64 != next * 2 {
        return Err(format!("{} rows for checkpoint {next}: {all:?}", all.len()));
    }
    for (k, v) in &all {
        let (i, want) = if *k >= 1_000_000 {
            (*k - 1_000_000, *k - 1_000_000)
        } else {
            (*k, *k * 2)
        };
        if i >= next {
            return Err(format!("row {k} is ahead of checkpoint {next}"));
        }
        if *v != want {
            return Err(format!("row {k} = {v}, want {want}"));
        }
    }
    let r = snap.register::<u64>("r").unwrap().get().unwrap();
    if next == 0 {
        if r.is_some() {
            return Err(format!("register {r:?} with checkpoint 0"));
        }
    } else if r != Some(next - 1) {
        return Err(format!("register {r:?}, checkpoint {next}"));
    }
    Ok(())
}

#[test]
fn kill_dash_nine_keeps_every_acknowledged_batch_and_the_invariant() {
    kill_rounds("always", 4);
}

#[test]
fn kill_dash_nine_without_fsync_keeps_the_invariant() {
    kill_rounds("never", 3);
}

fn kill_rounds(fsync: &str, rounds: u32) {
    let d = tmp();
    let exe = std::env::current_exe().unwrap();
    let mut last_acked = 0;
    for round in 0..rounds {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos() as u64;
        let child = Command::new(&exe)
            .args(["--exact", "crash::child_writer", "--nocapture"])
            .env(CHILD_DIR, d.path())
            .env(CHILD_FSYNC, fsync)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut child = KillOnDrop(child);
        let stdout = child.0.stdout.take().unwrap();
        let mut lines = BufReader::new(stdout).lines();
        let want = 3 + (seed % 25) as usize;
        let mut acked = None;
        let mut seen = 0;
        while seen < want {
            let line = lines.next().expect("the child keeps writing").unwrap();
            if let Ok(p) = line.trim().parse::<u64>() {
                acked = Some(p);
                seen += 1;
            }
        }
        let acked = acked.unwrap();
        std::thread::sleep(Duration::from_micros(seed % 3000));
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        drop(child);

        let s = child_store(d.path().to_str().unwrap(), Fsync::Always);
        let (p, reg) = s
            .register(&simple_decl("1"))
            .unwrap_or_else(|e| panic!("{fsync} round {round} seed {seed}: reopen failed: {e}"));
        let next = reg.checkpoint.next;
        if fsync == "always" {
            assert!(
                next >= acked,
                "{fsync} round {round} seed {seed}: checkpoint {next} lost acknowledged {acked}"
            );
        }
        assert!(
            next >= last_acked,
            "{fsync} round {round} seed {seed}: checkpoint {next} went below the previous round's {last_acked}"
        );
        check_invariant(&p).unwrap_or_else(|e| panic!("{fsync} round {round} seed {seed}: {e}"));
        last_acked = next;
        drop(p);
    }
}

/// The control for the invariant: a row written in a transaction of its
/// own, without the checkpoint, is what a crash between two commits
/// would leave if rows and checkpoint were not one transaction. The
/// checker must see it.
#[test]
fn the_invariant_check_sees_a_row_ahead_of_the_checkpoint() {
    let d = tmp();
    let s = child_store(d.path().to_str().unwrap(), Fsync::Never);
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    for i in 0..3 {
        batch(&p, i);
    }
    check_invariant(&p).unwrap();
    drop(p);
    {
        let db = redb::Database::open(d.path().join("p.redb")).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn
                .open_table(redb::TableDefinition::<&[u8], &[u8]>::new("c:m"))
                .unwrap();
            let key = facet::KeyType::to_key(&3u64);
            let val = facet::vbin::encode(&facet::Value::Uint(6));
            t.insert(key.as_slice(), val.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    let (p, reg) = s.register(&simple_decl("1")).unwrap();
    assert_eq!(reg.checkpoint, cp(3));
    let err = check_invariant(&p).unwrap_err();
    assert!(err.contains("rows for checkpoint 3"), "{err}");
}

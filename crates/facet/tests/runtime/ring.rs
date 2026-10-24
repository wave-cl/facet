//! The ring delivers every event at or past a consumer's position once,
//! in order, and says when it no longer reaches back far enough.

use chron_client::Event;
use facet::runtime::ring::{Ring, Take};
use proptest::prelude::*;

fn ev(position: u64, bytes: usize) -> Event {
    Event {
        position,
        aggregate: None,
        index: 0,
        recorded_at: 0,
        flags: 0,
        commit_id: None,
        tags: vec![],
        metadata: bytes::Bytes::new(),
        payload: bytes::Bytes::from(vec![0u8; bytes]),
    }
}

#[test]
fn a_consumer_behind_the_ring_is_told_so_and_one_ahead_waits() {
    let r = Ring::new(4, 1 << 20);
    assert_eq!(r.take_from(0, 10, 1 << 20), Take::Behind, "not subscribed");
    r.reset(10);
    assert_eq!(r.take_from(10, 10, 1 << 20), Take::Empty);
    assert_eq!(r.take_from(5, 10, 1 << 20), Take::Behind);
    for p in [10, 12, 13, 17, 20, 21] {
        assert!(r.push(ev(p, 1)));
    }
    assert!(!r.push(ev(21, 1)), "out of order is dropped");
    // Capacity 4: 10 and 12 were evicted; complete from 13.
    assert_eq!(r.low(), Some(13));
    assert_eq!(r.take_from(12, 10, 1 << 20), Take::Behind);
    match r.take_from(13, 2, 1 << 20) {
        Take::Events(e) => assert_eq!(
            e.iter().map(|e| e.position).collect::<Vec<_>>(),
            vec![13, 17]
        ),
        other => panic!("{other:?}"),
    }
    match r.take_from(18, 10, 1 << 20) {
        Take::Events(e) => assert_eq!(
            e.iter().map(|e| e.position).collect::<Vec<_>>(),
            vec![20, 21]
        ),
        other => panic!("{other:?}"),
    }
    assert_eq!(r.take_from(22, 10, 1 << 20), Take::Empty);
    r.clear();
    assert_eq!(r.take_from(22, 10, 1 << 20), Take::Behind);
}

proptest! {
    /// A consumer that takes whenever it can, and reads "from chron"
    /// (here: skips to the ring's low mark, as the runner's catch-up read
    /// would) when behind, sees every position exactly once in order.
    #[test]
    fn every_position_once_in_order(
        gaps in proptest::collection::vec(1u64..4, 1..200),
        sizes in proptest::collection::vec(0usize..40, 1..200),
        cap in 1usize..8,
        take_every in 1usize..6,
        take_max in 1usize..5,
    ) {
        let r = Ring::new(cap, 64);
        r.reset(0);
        let mut pos = 0u64;
        let mut pushed = Vec::new();
        let mut seen = Vec::new();
        let mut next = 0u64;
        for (i, (g, s)) in gaps.iter().zip(sizes.iter().cycle()).enumerate() {
            pos += g;
            prop_assert!(r.push(ev(pos, *s)));
            pushed.push(pos);
            if i % take_every == 0 {
                loop {
                    match r.take_from(next, take_max, 64) {
                        Take::Events(e) => {
                            for ev in e {
                                prop_assert!(ev.position >= next);
                                seen.push(ev.position);
                                next = ev.position + 1;
                            }
                        }
                        Take::Empty => break,
                        Take::Behind => {
                            // What chron would hand back: the pushed
                            // positions from `next` up to the ring's low.
                            let low = r.low().unwrap();
                            for p in pushed.iter().filter(|p| **p >= next && **p < low) {
                                seen.push(*p);
                            }
                            next = low;
                        }
                    }
                }
            }
        }
        // Drain.
        loop {
            match r.take_from(next, take_max, 64) {
                Take::Events(e) => { for ev in e { seen.push(ev.position); next = ev.position + 1; } }
                Take::Empty => break,
                Take::Behind => {
                    let low = r.low().unwrap();
                    for p in pushed.iter().filter(|p| **p >= next && **p < low) { seen.push(*p); }
                    next = low;
                }
            }
        }
        prop_assert_eq!(seen, pushed);
    }
}

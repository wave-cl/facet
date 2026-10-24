//! The ring a tail fills: recent events of one source, in position
//! order, complete from `low` on. A runner whose next position is at or
//! past `low` takes its batches here; one behind reads from chron.

use std::collections::VecDeque;
use std::sync::Mutex;

use chron_client::Event;

/// What [`Ring::take_from`] found.
#[derive(Debug, PartialEq)]
pub enum Take {
    /// The next events at or past the position, in order.
    Events(Vec<Event>),
    /// The ring has every event from the position on, and there are
    /// none yet.
    Empty,
    /// The ring does not reach back to the position: read from chron.
    Behind,
}

struct Inner {
    buf: VecDeque<Event>,
    /// The position from which the ring is complete; `None` while the
    /// tail is not subscribed.
    low: Option<u64>,
    bytes: usize,
}

/// A bounded buffer of one source's recent events.
pub struct Ring {
    inner: Mutex<Inner>,
    max_events: usize,
    max_bytes: usize,
}

impl Ring {
    /// A ring holding at most this many events or payload bytes.
    pub fn new(max_events: usize, max_bytes: usize) -> Ring {
        Ring {
            inner: Mutex::new(Inner {
                buf: VecDeque::new(),
                low: None,
                bytes: 0,
            }),
            max_events: max_events.max(1),
            max_bytes: max_bytes.max(1),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The tail subscribed from `from`: the ring is complete from there.
    pub fn reset(&self, from: u64) {
        let mut g = self.lock();
        g.buf.clear();
        g.bytes = 0;
        g.low = Some(from);
    }

    /// The tail lost its subscription: nothing is complete.
    pub fn clear(&self) {
        let mut g = self.lock();
        g.buf.clear();
        g.bytes = 0;
        g.low = None;
    }

    /// The position from which the ring is complete.
    pub fn low(&self) -> Option<u64> {
        self.lock().low
    }

    /// One past the last position held, or `low` when empty.
    pub fn next(&self) -> Option<u64> {
        let g = self.lock();
        g.buf.back().map(|e| e.position + 1).or(g.low)
    }

    /// Appends an event the tail received. Events must arrive in
    /// position order; one out of order is dropped and reported.
    pub fn push(&self, ev: Event) -> bool {
        let mut g = self.lock();
        if g.low.is_none() {
            return false;
        }
        if let Some(last) = g.buf.back()
            && ev.position <= last.position
        {
            return false;
        }
        g.bytes += ev.payload.len();
        g.buf.push_back(ev);
        while g.buf.len() > self.max_events || g.bytes > self.max_bytes {
            if g.buf.len() == 1 {
                break;
            }
            let gone = g.buf.pop_front().unwrap();
            g.bytes -= gone.payload.len();
            g.low = Some(gone.position + 1);
        }
        true
    }

    /// Up to `max_events` events (or `max_bytes` of payload) at or past
    /// `next`.
    pub fn take_from(&self, next: u64, max_events: usize, max_bytes: usize) -> Take {
        let g = self.lock();
        let Some(low) = g.low else {
            return Take::Behind;
        };
        if next < low {
            return Take::Behind;
        }
        let start = g.buf.partition_point(|e| e.position < next);
        let mut out = Vec::new();
        let mut bytes = 0;
        for ev in g.buf.range(start..) {
            if out.len() >= max_events.max(1) || (bytes >= max_bytes && !out.is_empty()) {
                break;
            }
            bytes += ev.payload.len();
            out.push(ev.clone());
        }
        if out.is_empty() {
            Take::Empty
        } else {
            Take::Events(out)
        }
    }

    /// How many events the ring holds.
    pub fn len(&self) -> usize {
        self.lock().buf.len()
    }

    /// Whether the ring holds nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

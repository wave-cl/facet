//! Projections over chron's bank example: every account event carries
//! the tag `account` and a JSON payload tagged by `type`.

use facet::{App, HandlerError, Projection, Source, TypeSchema, Width};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The tag every bank event carries.
pub const TAG: &str = "account";

/// The bank example's events, as it serialises them.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AccountEvent {
    /// The account exists.
    Opened {
        /// Who holds it.
        owner: String,
    },
    /// Money in.
    Deposited {
        /// How much.
        amount: i64,
    },
    /// Money out.
    Withdrawn {
        /// How much.
        amount: i64,
    },
    /// The account is closed.
    Closed,
}

/// One row of the `transactions` table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Txn {
    /// The event's global position.
    pub id: u64,
    /// The account.
    pub account: Uuid,
    /// Signed: deposits positive, withdrawals negative.
    pub amount: i64,
    /// When it was committed, unix nanoseconds.
    pub at: i64,
}

/// The `bank` projection: `balance` (map account -> i64), `owners` (map
/// account -> name), `closed` (set), `top` (sorted set by balance),
/// `deposits` (buckets per hour and day), `transactions` (table with
/// `by_account`), `count` (one counter of money movements).
pub fn app() -> App {
    let (b, (balance, owners, closed, top, deposits, txns, count)) = Projection::new("bank")
        .version("1")
        .source(Source::tag(TAG))
        .collections(|c| {
            (
                c.map::<Uuid, i64>("balance"),
                c.map::<Uuid, String>("owners"),
                c.set::<Uuid>("closed"),
                c.sorted_set::<(), Uuid, i64>("top"),
                c.buckets::<(), i64>("deposits", [Width::Hour, Width::Day]),
                c.table::<u64, Txn>("transactions")
                    .column("id", TypeSchema::Uint)
                    .column("account", TypeSchema::Uuid)
                    .column("amount", TypeSchema::Int)
                    .column("at", TypeSchema::Timestamp)
                    .primary_key(["id"])
                    .index("by_account", ["account", "at"])
                    .build(),
                c.counter::<(), i64>("count"),
            )
        });
    let bank = b
        .on::<AccountEvent>(TAG, move |cx, ev| {
            let id = cx.aggregate().ok_or_else(|| {
                HandlerError::Handler("an account event without an aggregate".into())
            })?;
            let position = cx.position();
            let at = cx.recorded_at();
            let delta = match ev {
                AccountEvent::Opened { owner } => {
                    cx.map(&owners)?.put(&id, &owner)?;
                    cx.map(&balance)?.update(&id, |b| Some(b.unwrap_or(0)))?;
                    cx.sorted_set(&top)?.put(&(), &id, 0)?;
                    return Ok(());
                }
                AccountEvent::Closed => {
                    cx.set(&closed)?.insert(&id)?;
                    return Ok(());
                }
                AccountEvent::Deposited { amount } => amount,
                AccountEvent::Withdrawn { amount } => -amount,
            };
            let now = cx
                .map(&balance)?
                .update(&id, |b| Some(b.unwrap_or(0) + delta))?
                .unwrap_or(0);
            cx.sorted_set(&top)?.put(&(), &id, now)?;
            if delta > 0 {
                cx.buckets(&deposits)?.add(&(), at, delta)?;
            }
            cx.table(&txns)?.upsert(&Txn {
                id: position,
                account: id,
                amount: delta,
                at,
            })?;
            cx.counter(&count)?.add(&(), 1)?;
            Ok(())
        })
        .build()
        .expect("the bank projection is well formed");
    App::new().projection(bank)
}

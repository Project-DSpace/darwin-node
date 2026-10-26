//! Which peers a storer has heard from for a key, so its offers skip them.
//!
//! A peer P is heard from for key K when this node received from P, within
//! [`HEARD_FROM_KEEP`]:
//! - a holding notice (a `PaidNotify` with an empty proof), which P sends once
//!   it stored K and wrote its own paid-list entry, or from its "already
//!   stored" answer when it lists K;
//! - a notice with a proof, which every release sends only after storing K,
//!   and this release only while it holds K: at announce, and when its hold
//!   ends or as the node stops only if its store still holds K then, or,
//!   when its copy was unreadable for the moment as the hold ended, once a
//!   read-back finds it; never from an offer dropped because its copy was
//!   found gone or unreadable;
//! - a fresh offer whose bytes hash to K.
//!
//! **A hint for sending only.** A holding notice is the sender's own claim,
//! never checked against bytes. Nothing here is a possession proof, a holder
//! credit, a vote in the paid-list quorum, an erase proof, or "seen holding"
//! for any waiver. The table and its entries are private to this module, and
//! its one read is [`HeardFrom::offer_skip_reason`], called from the hold in
//! `fresh.rs` alone; a census pin counts that call. Erasure, repair, the
//! paid-list quorum and audits rest on possession proofs, never on this.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use saorsa_core::identity::PeerId;

use crate::ant_protocol::XorName;

/// How long an entry is kept: twice the longest hold, so a notice that
/// arrived just before a hold started still counts when it ends.
pub(crate) const HEARD_FROM_KEEP: Duration = Duration::from_secs(20);

/// The most entries the table keeps, about 4 MiB. When it is full of entries
/// younger than [`HEARD_FROM_KEEP`], a new one is not kept: that only means
/// one more send.
pub(crate) const HEARD_FROM_MAX_ENTRIES: usize = 32_768;

/// The least time between two sweeps of a full table, so a table full of
/// young entries isn't swept on every note.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// How a close-group peer was served when a storer's hold ended. A
/// possession check carries it, and the confirmed-failure warning prints it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Served {
    /// Heard from: no offer went to the peer.
    HeardFrom,
    /// Marked full by its own "full" notice: no offer went to the peer.
    MarkedFull,
    /// Offered the record.
    Offered,
}

impl Served {
    /// The name the logs and counters use.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HeardFrom => "heard_from",
            Self::MarkedFull => "marked_full",
            Self::Offered => "offered",
        }
    }

    /// Whether the storer skipped the peer's offer.
    #[must_use]
    pub const fn skipped(self) -> bool {
        !matches!(self, Self::Offered)
    }
}

/// The heard-from table: (key, peer) to when the peer was last heard from.
#[derive(Default)]
pub(crate) struct HeardFrom {
    inner: Mutex<Table>,
}

#[derive(Default)]
struct Table {
    entries: HashMap<(XorName, PeerId), Instant>,
    last_sweep: Option<Instant>,
}

impl HeardFrom {
    /// Note that `peer` was heard from for `key` at `now`. Returns `false`
    /// when the table is full and the entry was not kept.
    pub(crate) fn note(&self, key: XorName, peer: PeerId, now: Instant) -> bool {
        let mut table = self.inner.lock();
        if table.entries.len() >= HEARD_FROM_MAX_ENTRIES
            && !table.entries.contains_key(&(key, peer))
        {
            let due = table.last_sweep.map_or(true, |at| {
                now.saturating_duration_since(at) >= SWEEP_INTERVAL
            });
            if due {
                table
                    .entries
                    .retain(|_, at| now.saturating_duration_since(*at) < HEARD_FROM_KEEP);
                table.last_sweep = Some(now);
            }
            if table.entries.len() >= HEARD_FROM_MAX_ENTRIES {
                return false;
            }
        }
        table.entries.insert((key, peer), now);
        true
    }

    /// The table's one read: why a storer skips `peer`'s offer for `key`, or
    /// `None` when it offers. Two reasons only: the peer was heard from within
    /// [`HEARD_FROM_KEEP`], or it is marked full.
    ///
    /// A skipped peer is still checked for possession when the hold ends.
    pub(crate) fn offer_skip_reason(
        &self,
        key: &XorName,
        peer: &PeerId,
        marked_full: bool,
        now: Instant,
    ) -> Option<Served> {
        let heard = self
            .inner
            .lock()
            .entries
            .get(&(*key, *peer))
            .is_some_and(|at| now.saturating_duration_since(*at) < HEARD_FROM_KEEP);
        if heard {
            Some(Served::HeardFrom)
        } else if marked_full {
            Some(Served::MarkedFull)
        } else {
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn peer(i: u8) -> PeerId {
        PeerId::from_bytes([i; 32])
    }

    /// A peer heard from is skipped for that key only, and only while the
    /// entry is younger than the keep time.
    #[test]
    fn a_peer_is_skipped_only_for_the_key_and_while_the_entry_is_kept() {
        let table = HeardFrom::default();
        let start = Instant::now();
        assert!(table.note([1; 32], peer(1), start));
        let at = start + Duration::from_secs(5);
        assert_eq!(
            table.offer_skip_reason(&[1; 32], &peer(1), false, at),
            Some(Served::HeardFrom)
        );
        assert_eq!(table.offer_skip_reason(&[2; 32], &peer(1), false, at), None);
        assert_eq!(table.offer_skip_reason(&[1; 32], &peer(2), false, at), None);
        assert_eq!(
            table.offer_skip_reason(&[1; 32], &peer(1), false, start + HEARD_FROM_KEEP),
            None
        );
    }

    /// The two reasons: heard from first, then marked full.
    #[test]
    fn the_skip_reasons_are_heard_from_and_marked_full() {
        let table = HeardFrom::default();
        let now = Instant::now();
        table.note([1; 32], peer(1), now);
        assert_eq!(
            table.offer_skip_reason(&[1; 32], &peer(1), true, now),
            Some(Served::HeardFrom)
        );
        assert_eq!(
            table.offer_skip_reason(&[1; 32], &peer(2), true, now),
            Some(Served::MarkedFull)
        );
        assert_eq!(
            table.offer_skip_reason(&[1; 32], &peer(2), false, now),
            None
        );
        assert!(Served::HeardFrom.skipped() && Served::MarkedFull.skipped());
        assert!(!Served::Offered.skipped());
    }

    /// A full table keeps no new entry, which only means more sends, and
    /// makes room once its entries are old.
    #[test]
    fn a_full_table_ends_in_more_sends_never_fewer() {
        let table = HeardFrom::default();
        let start = Instant::now();
        for i in 0..HEARD_FROM_MAX_ENTRIES {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&(i as u64).to_be_bytes());
            assert!(table.note(key, peer(1), start));
        }
        let late = start + Duration::from_secs(2);
        assert!(!table.note([0xFF; 32], peer(1), late));
        assert_eq!(
            table.offer_skip_reason(&[0xFF; 32], &peer(1), false, late),
            None
        );
        let later = start + HEARD_FROM_KEEP;
        assert!(table.note([0xFF; 32], peer(1), later));
    }
}

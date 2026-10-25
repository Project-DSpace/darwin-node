//! Paged neighbour sync's bookkeeping: which peers read pages, and the hints
//! a responder holds for the paged exchanges in progress.
//!
//! A node pages its hints only to a peer known to read pages (the README's
//! "Messages between nodes"): one whose user agent names
//! [`NEIGHBOR_SYNC_PAGES_FROM_RELEASE`] or a later release, or, while it is
//! not connected, one whose last sync message was a page. Any other peer,
//! one of unknown release among them, gets a single message.
//!
//! A node runs one exchange at a time with each peer ([`ExchangesInProgress`]):
//! its bootstrap sync and its sync rounds both sync, and a responder holds one
//! exchange's hints for each requester.
//!
//! A responder builds its hints for a requester once, at the exchange's first
//! page, and serves every page of the exchange from them ([`ServedHintSets`]).
//! What it holds is bounded by [`NEIGHBOR_SYNC_HINT_SETS_MAX_BYTES`], counted,
//! and dropped after the exchange's last page, when the requester starts
//! another exchange, or when the requester asks for no page for as long as it
//! waits for one.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use saorsa_core::identity::PeerId;

use crate::replication::config::{
    NEIGHBOR_SYNC_HINT_SETS_MAX_BYTES, NEIGHBOR_SYNC_PAGES_FROM_RELEASE,
};
use crate::replication::neighbor_sync::PeerSyncHints;

/// Most peers whose form of sync is remembered. Only routing-table peers sync,
/// and a routing-table removal forgets one, so this bounds only what a
/// lagging event stream could leave behind. A form forgotten this way is
/// learned again from the peer's agent or its next sync message.
const MAX_REMEMBERED_FORMS: usize = 4096;

// ---------------------------------------------------------------------------
// Which peers read pages
// ---------------------------------------------------------------------------

/// The release from which peers read pages: [`NEIGHBOR_SYNC_PAGES_FROM_RELEASE`],
/// or, in a `test-utils` build, the release a test set
/// ([`crate::test_seams::SYNC_PAGES_FROM_RELEASE`]).
pub(crate) fn pages_from_release() -> u64 {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(release) = crate::test_seams::sync_pages_from_release() {
        return release;
    }
    NEIGHBOR_SYNC_PAGES_FROM_RELEASE
}

/// Whether `user_agent` is a node's of a release that reads paged sync.
///
/// That is `node/<version>+ds.<n>` with `n` at least `from_release`, whatever
/// the version before it. Anything else, an agent of no release this fork
/// numbers among them, reads none.
#[must_use]
pub fn agent_reads_pages(user_agent: &str, from_release: u64) -> bool {
    user_agent
        .strip_prefix("node/")
        .and_then(|version| semver::Version::parse(version).ok())
        .and_then(|version| {
            version
                .build
                .as_str()
                .strip_prefix("ds.")
                .and_then(|release| release.split('.').next())
                .and_then(|release| release.parse::<u64>().ok())
        })
        .is_some_and(|release| release >= from_release)
}

/// What each peer last showed of whether it reads pages: the agent of its
/// last connection, or the last sync message it sent, whichever came later.
#[derive(Debug, Default)]
pub(crate) struct SyncForms {
    reads_pages: Mutex<HashMap<PeerId, bool>>,
}

impl SyncForms {
    /// Note what `peer` just showed: that it reads pages, or that it doesn't.
    pub(crate) fn note(&self, peer: PeerId, reads_pages: bool) {
        let mut forms = self.reads_pages.lock();
        if forms.len() >= MAX_REMEMBERED_FORMS && !forms.contains_key(&peer) {
            if let Some(evicted) = forms.keys().next().copied() {
                forms.remove(&evicted);
            }
        }
        forms.insert(peer, reads_pages);
    }

    /// Whether `peer` last showed that it reads pages; `false` when it has
    /// shown nothing.
    pub(crate) fn reads_pages(&self, peer: &PeerId) -> bool {
        self.reads_pages.lock().get(peer).copied().unwrap_or(false)
    }

    /// Forget a peer that left the routing table.
    pub(crate) fn forget(&self, peer: &PeerId) {
        self.reads_pages.lock().remove(peer);
    }
}

// ---------------------------------------------------------------------------
// One exchange at a time with each peer
// ---------------------------------------------------------------------------

/// The peers this node has an exchange in progress with, as requester.
///
/// A node's bootstrap sync and its sync rounds run side by side, and both can
/// pick the same peer. A responder holds the hints of one exchange for each
/// requester, so two exchanges with one peer at once would each drop the
/// other's hints, and both would end busy part way. The second waits its turn
/// in a later cycle instead.
#[derive(Debug, Default)]
pub(crate) struct ExchangesInProgress {
    peers: Mutex<HashSet<PeerId>>,
}

/// An exchange in progress with one peer; dropping it ends the exchange.
#[derive(Debug)]
pub(crate) struct ExchangeTurn {
    exchanges: Arc<ExchangesInProgress>,
    peer: PeerId,
}

impl ExchangesInProgress {
    /// Start an exchange with `peer`, unless one is in progress.
    pub(crate) fn try_start(self: &Arc<Self>, peer: PeerId) -> Option<ExchangeTurn> {
        if self.peers.lock().insert(peer) {
            Some(ExchangeTurn {
                exchanges: Arc::clone(self),
                peer,
            })
        } else {
            None
        }
    }
}

impl Drop for ExchangeTurn {
    fn drop(&mut self) {
        self.exchanges.peers.lock().remove(&self.peer);
    }
}

// ---------------------------------------------------------------------------
// The hints a responder holds for exchanges in progress
// ---------------------------------------------------------------------------

/// The hints a responder built for one requester's paged exchange.
#[derive(Debug, Clone)]
pub(crate) struct ServedHints {
    /// The requester's number for the exchange.
    pub(crate) cycle: u64,
    /// The hints, built once at the exchange's first page.
    pub(crate) hints: Arc<PeerSyncHints>,
    /// Keys on each of the responder's pages.
    pub(crate) page_keys: usize,
    /// How many pages the hints take.
    pub(crate) pages: u32,
}

#[derive(Debug)]
struct Held {
    served: ServedHints,
    bytes: usize,
    last_used: Instant,
    /// Whether a page of the requester's hints, in this exchange, had hints
    /// turned away at the verification queue's capacity.
    inbound_rejected: bool,
}

/// The hint sets a responder holds, one per requester with a paged exchange
/// in progress.
#[derive(Debug)]
pub(crate) struct ServedHintSets {
    inner: Mutex<HeldSets>,
    max_bytes: usize,
}

#[derive(Debug, Default)]
struct HeldSets {
    sets: HashMap<PeerId, Held>,
    bytes: usize,
    /// Sets built since the node started: one per paged exchange served.
    built: u64,
}

impl HeldSets {
    fn drop_set(&mut self, peer: &PeerId) -> bool {
        if let Some(held) = self.sets.remove(peer) {
            self.bytes = self.bytes.saturating_sub(held.bytes);
            true
        } else {
            false
        }
    }

    /// Drop every set whose requester has asked for no page for `idle`: it
    /// gave up on the exchange.
    fn sweep(&mut self, now: Instant, idle: Duration) {
        let stale: Vec<PeerId> = self
            .sets
            .iter()
            .filter(|(_, held)| now.saturating_duration_since(held.last_used) >= idle)
            .map(|(peer, _)| *peer)
            .collect();
        for peer in stale {
            self.drop_set(&peer);
        }
    }
}

impl Default for ServedHintSets {
    fn default() -> Self {
        Self::new(NEIGHBOR_SYNC_HINT_SETS_MAX_BYTES)
    }
}

impl ServedHintSets {
    /// Hint sets holding at most about `max_bytes` of keys.
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(HeldSets::default()),
            max_bytes,
        }
    }

    /// The hints held for `peer`'s exchange `cycle`, if they are; marks them
    /// used. A set held for another exchange of `peer` is dropped: an
    /// exchange's pages come one after another, so the requester has left it.
    pub(crate) fn get(
        &self,
        peer: &PeerId,
        cycle: u64,
        now: Instant,
        idle: Duration,
    ) -> Option<ServedHints> {
        let mut inner = self.inner.lock();
        inner.sweep(now, idle);
        let held_cycle = inner.sets.get(peer).map(|held| held.served.cycle);
        match held_cycle {
            Some(held) if held == cycle => {
                let held = inner.sets.get_mut(peer)?;
                held.last_used = now;
                Some(held.served.clone())
            }
            Some(_) => {
                inner.drop_set(peer);
                None
            }
            None => None,
        }
    }

    /// Whether a new set may be built: less than the budget is held, or
    /// nothing is. At most one set per worker is built at once, so what is
    /// held can pass the budget by that many sets, and no more.
    pub(crate) fn may_build(&self, now: Instant, idle: Duration) -> bool {
        let mut inner = self.inner.lock();
        inner.sweep(now, idle);
        inner.sets.is_empty() || inner.bytes < self.max_bytes
    }

    /// Hold `served` for `peer`, in place of any set it had.
    pub(crate) fn insert(&self, peer: PeerId, served: ServedHints, now: Instant) {
        let bytes = served.hints.bytes();
        let mut inner = self.inner.lock();
        inner.drop_set(&peer);
        inner.bytes = inner.bytes.saturating_add(bytes);
        inner.built = inner.built.saturating_add(1);
        inner.sets.insert(
            peer,
            Held {
                served,
                bytes,
                last_used: now,
                inbound_rejected: false,
            },
        );
    }

    /// Drop `peer`'s set for exchange `cycle`, after its last page.
    pub(crate) fn finish(&self, peer: &PeerId, cycle: u64) {
        let mut inner = self.inner.lock();
        if inner
            .sets
            .get(peer)
            .is_some_and(|held| held.served.cycle == cycle)
        {
            inner.drop_set(peer);
        }
    }

    /// Note whether a page of `peer`'s hints in exchange `cycle` had hints
    /// turned away at capacity, and return whether any page of the exchange
    /// so far had.
    pub(crate) fn note_inbound(&self, peer: &PeerId, cycle: u64, rejected: bool) -> bool {
        let mut inner = self.inner.lock();
        match inner.sets.get_mut(peer) {
            Some(held) if held.served.cycle == cycle => {
                held.inbound_rejected |= rejected;
                held.inbound_rejected
            }
            _ => rejected,
        }
    }

    /// Forget a peer that left the routing table.
    pub(crate) fn forget(&self, peer: &PeerId) {
        self.inner.lock().drop_set(peer);
    }

    /// How many sets are held, and their bytes of keys.
    pub(crate) fn held(&self) -> (usize, usize) {
        let inner = self.inner.lock();
        (inner.sets.len(), inner.bytes)
    }

    /// How many sets have been built since the node started.
    pub(crate) fn built(&self) -> u64 {
        self.inner.lock().built
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::replication::neighbor_sync::RoutingSnapshot;

    fn peer(byte: u8) -> PeerId {
        PeerId::from_bytes([byte; 32])
    }

    fn served(cycle: u64, keys: usize) -> ServedHints {
        let snapshot = Arc::new(RoutingSnapshot::new(peer(0), vec![peer(1)]));
        let mut hints = PeerSyncHints::empty(snapshot, 7);
        hints.replica = vec![[7; 32]; keys];
        ServedHints {
            cycle,
            hints: Arc::new(hints),
            page_keys: 2,
            pages: u32::try_from(keys.div_ceil(2).max(1)).unwrap(),
        }
    }

    const IDLE: Duration = Duration::from_secs(30);

    #[test]
    fn agents_of_the_paging_release_on_read_pages() {
        for agent in [
            "node/0.23.0+ds.17",
            "node/0.23.1+ds.18",
            "node/0.24.0+ds.17.1",
            "node/1.0.0+ds.100",
        ] {
            assert!(agent_reads_pages(agent, 17), "{agent}");
        }
        for agent in [
            "node/0.23.0+ds.15",
            "node/0.23.0+ds.16",
            "node/0.23.0",
            "client/0.23.0+ds.17",
            "node/0.23.0+dsx.17",
            "node/0.23.0+ds.",
            "node/garbage",
            "",
        ] {
            assert!(!agent_reads_pages(agent, 17), "{agent}");
        }
    }

    /// The release nodes page to is past ds.15, the release before pages,
    /// whose nodes can't decode them.
    #[test]
    fn nodes_of_the_release_before_pages_read_none() {
        assert!(!agent_reads_pages(
            "node/0.23.0+ds.15",
            NEIGHBOR_SYNC_PAGES_FROM_RELEASE
        ));
        assert_eq!(NEIGHBOR_SYNC_PAGES_FROM_RELEASE, 17);
    }

    #[test]
    fn a_peer_reads_pages_as_it_last_showed() {
        let forms = SyncForms::default();
        let a = peer(1);
        assert!(!forms.reads_pages(&a), "an unknown peer reads none");
        forms.note(a, true);
        assert!(forms.reads_pages(&a));
        // Rolled back: its next connection's agent, or its next message,
        // shows an older release.
        forms.note(a, false);
        assert!(!forms.reads_pages(&a));
        forms.note(a, true);
        forms.forget(&a);
        assert!(!forms.reads_pages(&a));
    }

    #[test]
    fn the_remembered_forms_are_bounded() {
        let forms = SyncForms::default();
        for index in 0..=MAX_REMEMBERED_FORMS {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
            forms.note(PeerId::from_bytes(bytes), true);
        }
        assert_eq!(forms.reads_pages.lock().len(), MAX_REMEMBERED_FORMS);
    }

    #[test]
    fn one_exchange_at_a_time_with_each_peer() {
        let exchanges = Arc::new(ExchangesInProgress::default());
        let first = exchanges
            .try_start(peer(1))
            .expect("no exchange in progress");
        assert!(exchanges.try_start(peer(1)).is_none(), "one at a time");
        let other = exchanges.try_start(peer(2)).expect("other peers are free");
        drop(first);
        assert!(exchanges.try_start(peer(1)).is_some(), "free once it ends");
        drop(other);
    }

    #[test]
    fn a_set_serves_its_exchange_and_is_dropped_after_it() {
        let sets = ServedHintSets::new(1 << 20);
        let now = Instant::now();
        let a = peer(1);
        assert!(sets.get(&a, 5, now, IDLE).is_none());
        sets.insert(a, served(5, 10), now);
        assert_eq!(sets.held(), (1, 320));
        assert_eq!(sets.built(), 1);
        let got = sets.get(&a, 5, now, IDLE).expect("held for exchange 5");
        assert_eq!((got.cycle, got.pages), (5, 5));

        // Another exchange of the same requester drops it.
        assert!(sets.get(&a, 6, now, IDLE).is_none());
        assert_eq!(sets.held(), (0, 0));

        sets.insert(a, served(7, 4), now);
        sets.finish(&a, 8);
        assert_eq!(sets.held().0, 1, "finishing another exchange keeps it");
        sets.finish(&a, 7);
        assert_eq!(sets.held(), (0, 0));

        sets.insert(a, served(9, 4), now);
        sets.forget(&a);
        assert_eq!(sets.held(), (0, 0));
    }

    #[test]
    fn a_set_whose_requester_stopped_asking_is_dropped() {
        let sets = ServedHintSets::new(1 << 20);
        let start = Instant::now();
        let a = peer(1);
        sets.insert(a, served(5, 10), start);
        assert!(sets.get(&a, 5, start + IDLE / 2, IDLE).is_some());
        // Used at IDLE / 2: still held just before IDLE after that.
        assert!(sets.get(&a, 5, start + IDLE, IDLE).is_some());
        assert!(sets.get(&a, 5, start + IDLE * 3, IDLE).is_none());
        assert_eq!(sets.held(), (0, 0));
    }

    #[test]
    fn the_held_sets_are_bounded_by_bytes_but_one_is_always_allowed() {
        // Room for 10 keys.
        let sets = ServedHintSets::new(320);
        let now = Instant::now();
        assert!(sets.may_build(now, IDLE), "nothing held");
        // One set larger than the whole budget is still served alone.
        sets.insert(peer(1), served(1, 40), now);
        assert!(!sets.may_build(now, IDLE), "over the budget");
        sets.finish(&peer(1), 1);
        sets.insert(peer(2), served(2, 4), now);
        assert!(sets.may_build(now, IDLE), "under the budget");
        sets.insert(peer(3), served(3, 6), now);
        assert_eq!(sets.held(), (2, 320));
        assert!(!sets.may_build(now, IDLE), "at the budget");
    }

    #[test]
    fn an_exchange_with_a_rejected_page_stays_unclean() {
        let sets = ServedHintSets::new(1 << 20);
        let now = Instant::now();
        let a = peer(1);
        sets.insert(a, served(5, 10), now);
        assert!(!sets.note_inbound(&a, 5, false));
        assert!(sets.note_inbound(&a, 5, true));
        assert!(
            sets.note_inbound(&a, 5, false),
            "a later clean page doesn't clear it"
        );
        assert!(
            !sets.note_inbound(&a, 6, false),
            "another exchange's page is its own"
        );
    }
}

//! Paged neighbour sync's bookkeeping: what a node keeps of its exchanges with
//! each peer, and the hints a responder holds for the paged exchanges in
//! progress.
//!
//! A node pages its hints only to a peer known to read pages (the README's
//! "Messages between nodes"): one whose connection's user agent names
//! [`NEIGHBOR_SYNC_PAGES_FROM_RELEASE`] or a later release. Any other peer, one
//! that isn't connected or of unknown release among them, gets a single
//! message, which every release reads; sending it connects the peer, so its
//! next exchange reads a fresh agent. A peer whose first page went unanswered
//! gets a single message in its next exchange too ([`SyncPeers`]), so an agent
//! left from a connection that has since ended pages it at most once.
//!
//! A node runs one exchange at a time with each peer ([`SyncPeers::try_start`]):
//! its bootstrap sync and its sync rounds both sync, and a responder holds one
//! exchange's hints for each requester. What a node holds of its peers'
//! answers, across every exchange in progress, is bounded and counted
//! ([`ReceivedHints`]).
//!
//! Hints for a peer that come to more than one exchange carries are sent a
//! window at a time ([`WindowStart`]): each exchange with the peer takes the
//! next keys of each list, round and round, from a window kept for each role
//! this node has in the exchange ([`SyncRole`]).
//! The window moves on by the keys that were delivered ([`WindowCut::commit`]),
//! so an exchange that ends part way leaves the rest of its window to the next.
//!
//! A responder builds its hints for a requester once, at the exchange's first
//! page, keeps only the window it serves, and serves every page of the
//! exchange from them ([`ServedHintSets`]). What it holds is bounded by
//! [`NEIGHBOR_SYNC_HINT_SETS_MAX_BYTES`], counted, and dropped after the
//! exchange's last page, when the requester starts another exchange, or when
//! the requester asks for no page for as long as it waits for one.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::logging::warn;
use parking_lot::Mutex;
use rand::Rng;
use saorsa_core::identity::PeerId;

use crate::replication::config::{
    MAX_NEIGHBOR_SYNC_PAGES, NEIGHBOR_SYNC_HINT_SETS_MAX_BYTES, NEIGHBOR_SYNC_PAGES_FROM_RELEASE,
    NEIGHBOR_SYNC_RECEIVED_HINTS_MAX_BYTES,
};
use crate::replication::neighbor_sync::{advance, PeerSyncHints, WindowStart};

/// Most peers whose next window, or whose "single message next", a node
/// keeps. Only routing-table peers sync, and a routing-table removal forgets
/// one, so this bounds only what a lagging event stream could leave behind.
/// A window forgotten this way starts again at a random place; a mark
/// forgotten this way costs one more unanswered page at most.
const MAX_PEERS_KEPT: usize = 4096;

/// Bytes of one key.
const KEY_BYTES: usize = 32;

/// Exchanges with a peer in a row that deliver none of a window's keys before
/// the window's start is moved on a little ([`WindowCut::commit`]), so a range
/// whose first page is persistently refused can't hold the window in place.
const STALLS_BEFORE_SHIFT: u32 = 3;

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

/// The release a node's version names: `n` of `<version>+ds.<n>`, whatever
/// the version before it, if it names one.
#[must_use]
pub fn release_of(version: &str) -> Option<u64> {
    semver::Version::parse(version).ok().and_then(|version| {
        version
            .build
            .as_str()
            .strip_prefix("ds.")
            .and_then(|release| release.split('.').next())
            .and_then(|release| release.parse::<u64>().ok())
    })
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
        .and_then(release_of)
        .is_some_and(|release| release >= from_release)
}

// ---------------------------------------------------------------------------
// What a node keeps of its exchanges with each peer
// ---------------------------------------------------------------------------

/// What a node keeps of its sync exchanges with each peer: the exchanges in
/// progress, the peers whose next exchange is a single message, where the
/// next window of the hints it sends each peer starts, and what it holds of
/// its peers' answers.
#[derive(Debug)]
pub(crate) struct SyncPeers {
    exchanges: Mutex<HashSet<PeerId>>,
    single_next: Mutex<HashSet<PeerId>>,
    windows: Windows,
    received: Arc<ReceivedHints>,
}

/// Where the next window of each peer's hints starts, for each role this node
/// sends them in.
type Windows = Arc<Mutex<HashMap<(PeerId, SyncRole), PeerWindow>>>;

/// The role in which this node sends a peer its hints. Each role cuts its own
/// window of them: a node and its peer often sync each other at the same
/// time, and a shared window would let the later exchange's cut pass over
/// the keys the earlier one didn't deliver. Exchanges in one role don't
/// overlap: one exchange with a peer at a time as requester
/// ([`SyncPeers::try_start`]), and one request of a peer at a time as
/// responder (its admission).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SyncRole {
    /// This node's own exchange with the peer.
    Requester,
    /// This node's answer to the peer's exchange.
    Responder,
}

/// Where the next window of a peer's hints starts, and how many exchanges in a
/// row delivered none of their window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PeerWindow {
    start: WindowStart,
    stalls: u32,
}

/// A window cut from a peer's hints ([`SyncPeers::window`]). The peer's next
/// window was moved on past the whole of it when it was cut, so an exchange
/// running at the same time takes the keys after it; once the exchange ends,
/// [`Self::commit`] moves it back to just past the keys that were delivered.
#[derive(Debug)]
pub(crate) struct WindowCut {
    windows: Windows,
    peer: PeerId,
    role: SyncRole,
    start: WindowStart,
    next: WindowStart,
    /// Keys of each list the window took, and the list's length.
    replica: (usize, usize),
    paid: (usize, usize),
}

impl WindowCut {
    /// The exchange delivered the first `replica` replica keys and the first
    /// `paid` paid keys of the window: the peer's next window starts just
    /// past them, so keys the exchange didn't deliver go first in the next.
    /// Nothing changes if another exchange cut the peer's window since. When
    /// [`STALLS_BEFORE_SHIFT`] exchanges in a row delivered none, the start
    /// moves on by a few keys, at most about one page of the window.
    pub(crate) fn commit(self, replica: usize, paid: usize) {
        let replica = replica.min(self.replica.0);
        let paid = paid.min(self.paid.0);
        let mut windows = self.windows.lock();
        let Some(window) = windows.get_mut(&(self.peer, self.role)) else {
            return;
        };
        if window.start != self.next {
            return;
        }
        if replica + paid > 0 {
            window.stalls = 0;
            window.start = WindowStart {
                replica: advance(self.start.replica, replica, self.replica.1),
                paid: advance(self.start.paid, paid, self.paid.1),
            };
            return;
        }
        window.stalls = window.stalls.saturating_add(1);
        if window.stalls < STALLS_BEFORE_SHIFT {
            window.start = self.start;
            return;
        }
        window.stalls = 0;
        let mut rng = rand::thread_rng();
        let mut shift = |take: usize| {
            if take == 0 {
                0
            } else {
                rng.gen_range(1..=(take / MAX_NEIGHBOR_SYNC_PAGES as usize).max(1))
            }
        };
        let replica_shift = shift(self.replica.0);
        let paid_shift = shift(self.paid.0);
        window.start = WindowStart {
            replica: advance(self.start.replica, replica_shift, self.replica.1),
            paid: advance(self.start.paid, paid_shift, self.paid.1),
        };
    }

    /// Commit the keys on the first `pages` pages of `page_keys` keys of
    /// `hints`, the window: its replica keys first, then its paid keys.
    pub(crate) fn commit_pages(self, hints: &PeerSyncHints, pages: u32, page_keys: usize) {
        let keys = usize::try_from(pages)
            .unwrap_or(usize::MAX)
            .saturating_mul(page_keys);
        let replica = keys.min(hints.replica.len());
        let paid = (keys - replica).min(hints.paid.len());
        self.commit(replica, paid);
    }
}

/// An exchange in progress with one peer; dropping it ends the exchange.
#[derive(Debug)]
pub(crate) struct ExchangeTurn {
    peers: Arc<SyncPeers>,
    peer: PeerId,
}

impl Default for SyncPeers {
    fn default() -> Self {
        Self::new(NEIGHBOR_SYNC_RECEIVED_HINTS_MAX_BYTES)
    }
}

impl SyncPeers {
    /// No exchanges, holding at most `received_max_bytes` of peers' answers.
    pub(crate) fn new(received_max_bytes: usize) -> Self {
        Self {
            exchanges: Mutex::new(HashSet::new()),
            single_next: Mutex::new(HashSet::new()),
            windows: Arc::new(Mutex::new(HashMap::new())),
            received: Arc::new(ReceivedHints::new(received_max_bytes)),
        }
    }

    /// Start an exchange with `peer`, unless one is in progress.
    ///
    /// A node's bootstrap sync and its sync rounds run side by side, and both
    /// can pick the same peer. A responder holds the hints of one exchange
    /// for each requester, so two exchanges with one peer at once would each
    /// drop the other's hints, and both would end busy part way. The second
    /// waits its turn in a later cycle instead.
    pub(crate) fn try_start(self: &Arc<Self>, peer: PeerId) -> Option<ExchangeTurn> {
        if self.exchanges.lock().insert(peer) {
            Some(ExchangeTurn {
                peers: Arc::clone(self),
                peer,
            })
        } else {
            None
        }
    }

    /// Send `peer` a single message in its next exchange, whatever its agent
    /// names: the first page of its last exchange went unanswered.
    pub(crate) fn mark_single_next(&self, peer: PeerId) {
        let mut marked = self.single_next.lock();
        if marked.len() >= MAX_PEERS_KEPT && !marked.contains(&peer) {
            if let Some(evicted) = marked.iter().next().copied() {
                marked.remove(&evicted);
            }
        }
        marked.insert(peer);
    }

    /// Whether `peer`'s next exchange is a single message, as
    /// [`Self::mark_single_next`] set; the mark is spent on this exchange.
    pub(crate) fn take_single_next(&self, peer: &PeerId) -> bool {
        self.single_next.lock().remove(peer)
    }

    /// The hints this node sends `peer` in one exchange, of at most
    /// `max_keys` keys: all of them, or, when they come to more, the next
    /// window of them ([`PeerSyncHints::window`]), from where the last
    /// exchange with the peer in `role` left off, with the cut to
    /// [`WindowCut::commit`] once the exchange ends. Logs a warning with both
    /// counts when the hints don't all fit.
    pub(crate) fn window(
        &self,
        peer: &PeerId,
        role: SyncRole,
        hints: PeerSyncHints,
        max_keys: usize,
    ) -> (PeerSyncHints, Option<WindowCut>) {
        let (replica_total, paid_total) = (hints.replica.len(), hints.paid.len());
        let total = replica_total + paid_total;
        if total <= max_keys {
            return (hints, None);
        }
        let mut windows = self.windows.lock();
        let key = (*peer, role);
        let kept = windows.get(&key).copied().unwrap_or_else(|| {
            // A peer's first window starts at a random place, so a node that
            // restarts doesn't send the same keys first every time.
            let mut rng = rand::thread_rng();
            PeerWindow {
                start: WindowStart {
                    replica: rng.gen(),
                    paid: rng.gen(),
                },
                stalls: 0,
            }
        });
        let start = kept.start;
        let (window, next) = hints.window(max_keys, start);
        let cut = next.map(|next| {
            if windows.len() >= MAX_PEERS_KEPT && !windows.contains_key(&key) {
                if let Some(evicted) = windows.keys().next().copied() {
                    windows.remove(&evicted);
                }
            }
            windows.insert(
                key,
                PeerWindow {
                    start: next,
                    stalls: kept.stalls,
                },
            );
            WindowCut {
                windows: Arc::clone(&self.windows),
                peer: *peer,
                role,
                start,
                next,
                replica: (window.replica.len(), replica_total),
                paid: (window.paid.len(), paid_total),
            }
        });
        drop(windows);
        warn!(
            "Neighbor sync with {peer}: {total} hints ({replica_total} replica, {paid_total} \
             paid), more than one exchange carries; this one carries {} ({} replica, {} paid), \
             and the next goes on from where its delivery ends",
            window.replica.len() + window.paid.len(),
            window.replica.len(),
            window.paid.len(),
        );
        (window, cut)
    }

    /// What the node holds of its peers' answers.
    pub(crate) fn received(&self) -> &Arc<ReceivedHints> {
        &self.received
    }

    /// Forget a peer that left the routing table.
    pub(crate) fn forget(&self, peer: &PeerId) {
        self.single_next.lock().remove(peer);
        let mut windows = self.windows.lock();
        windows.remove(&(*peer, SyncRole::Requester));
        windows.remove(&(*peer, SyncRole::Responder));
    }
}

impl Drop for ExchangeTurn {
    fn drop(&mut self) {
        self.peers.exchanges.lock().remove(&self.peer);
    }
}

// ---------------------------------------------------------------------------
// What a requester holds of its peers' answers
// ---------------------------------------------------------------------------

/// The bytes of keys a requester holds of its peers' answers to paged
/// exchanges, across every exchange in progress: its bootstrap sync's, its
/// sync round's and their replacements'. An exchange takes a page's keys only
/// while the total is under the budget, and gives them back once the hints
/// are admitted.
#[derive(Debug)]
pub(crate) struct ReceivedHints {
    state: Mutex<ReceivedState>,
    max_bytes: usize,
}

#[derive(Debug, Default)]
struct ReceivedState {
    held: usize,
    most: usize,
}

/// The keys of a peer's answer one exchange holds against [`ReceivedHints`];
/// given back when it is dropped.
#[derive(Debug, Default)]
pub(crate) struct HeldAnswer {
    budget: Option<Arc<ReceivedHints>>,
    bytes: usize,
}

impl ReceivedHints {
    /// A budget of `max_bytes` of keys.
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            state: Mutex::new(ReceivedState::default()),
            max_bytes,
        }
    }

    /// Hold up to `keys` more keys for `held`, as many as the budget has room
    /// for, and return how many it holds.
    pub(crate) fn hold(self: &Arc<Self>, held: &mut HeldAnswer, keys: usize) -> usize {
        let mut state = self.state.lock();
        let room = self.max_bytes.saturating_sub(state.held) / KEY_BYTES;
        let granted = keys.min(room);
        let bytes = granted * KEY_BYTES;
        state.held += bytes;
        state.most = state.most.max(state.held);
        drop(state);
        if held.budget.is_none() {
            held.budget = Some(Arc::clone(self));
        }
        held.bytes += bytes;
        granted
    }

    /// Bytes of keys held now.
    pub(crate) fn held(&self) -> usize {
        self.state.lock().held
    }

    /// The most bytes of keys held at once since the node started.
    pub(crate) fn most_held(&self) -> usize {
        self.state.lock().most
    }
}

impl HeldAnswer {
    /// Bytes of keys held.
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for HeldAnswer {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget {
            let mut state = budget.state.lock();
            state.held = state.held.saturating_sub(self.bytes);
        }
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
    /// The hints, built once at the exchange's first page: the window of
    /// them this exchange serves.
    pub(crate) hints: Arc<PeerSyncHints>,
    /// Keys on each of the responder's pages.
    pub(crate) page_keys: usize,
    /// How many pages the hints take: the pages this exchange serves.
    pub(crate) pages: u32,
    /// How many pages the responder announces: every page its hints for the
    /// requester take, more than [`Self::pages`] when they come to more than
    /// an exchange carries ([`crate::replication::neighbor_sync::announced_pages`]).
    pub(crate) announced: u32,
}

/// The repair proofs of a page the responder served, held back until the
/// requester's next request of the exchange shows that the page arrived.
#[derive(Debug, Clone)]
pub(crate) struct HeldProofs {
    /// The hints the page was cut from.
    pub(crate) hints: Arc<PeerSyncHints>,
    /// The page.
    pub(crate) page: u32,
    /// Keys on each of the responder's pages.
    pub(crate) page_keys: usize,
}

#[derive(Debug)]
struct Held {
    served: ServedHints,
    bytes: usize,
    last_used: Instant,
    /// Whether a page of the requester's hints, in this exchange, had hints
    /// turned away at the verification queue's capacity.
    inbound_rejected: bool,
    /// The page whose repair proofs wait for the requester's next request.
    proofs: Option<u32>,
    /// Pages the requester's later requests showed arrived: pages
    /// `0..arrived`.
    arrived: u32,
    /// The window the hints were cut to, if they were: committed with the
    /// pages that arrived when the set is dropped.
    cut: Option<WindowCut>,
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
    /// Drop `peer`'s set, moving its window on by the pages that arrived.
    fn drop_set(&mut self, peer: &PeerId) -> bool {
        if let Some(held) = self.sets.remove(peer) {
            self.bytes = self.bytes.saturating_sub(held.bytes);
            if let Some(cut) = held.cut {
                cut.commit_pages(&held.served.hints, held.arrived, held.served.page_keys);
            }
            true
        } else {
            false
        }
    }

    /// Drop every set whose requester has asked for no page for `idle`: it
    /// gave up on the exchange, and proofs held back for it go with it.
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
    /// nothing is. At most one set per worker is built at once, and a set
    /// holds no more than one exchange serves, so what is held can pass the
    /// budget by that many sets, and no more.
    pub(crate) fn may_build(&self, now: Instant, idle: Duration) -> bool {
        let mut inner = self.inner.lock();
        inner.sweep(now, idle);
        inner.sets.is_empty() || inner.bytes < self.max_bytes
    }

    /// Hold `served` for `peer`, in place of any set it had, with the window
    /// `cut` its hints were cut to, if they were.
    pub(crate) fn insert(
        &self,
        peer: PeerId,
        served: ServedHints,
        cut: Option<WindowCut>,
        now: Instant,
    ) {
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
                proofs: None,
                arrived: 0,
                cut,
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

    /// Note that `peer`'s request for `page` of exchange `cycle` arrived,
    /// which its requester sends only once every page before it arrived.
    pub(crate) fn note_arrived(&self, peer: &PeerId, cycle: u64, page: u32) {
        let mut inner = self.inner.lock();
        if let Some(held) = inner.sets.get_mut(peer) {
            if held.served.cycle == cycle {
                held.arrived = held.arrived.max(page.min(held.served.pages));
            }
        }
    }

    /// Hold back the repair proofs of `page` of `peer`'s exchange `cycle`
    /// until a later request of the exchange arrives, in place of any held
    /// for it. Nothing is held when the set isn't.
    pub(crate) fn hold_proofs(&self, peer: &PeerId, cycle: u64, page: u32) {
        let mut inner = self.inner.lock();
        if let Some(held) = inner.sets.get_mut(peer) {
            if held.served.cycle == cycle {
                held.proofs = Some(page);
            }
        }
    }

    /// Drop the proofs held back for `page` of `peer`'s exchange `cycle`,
    /// unless they were taken: the page was never sent.
    pub(crate) fn drop_proofs(&self, peer: &PeerId, cycle: u64, page: u32) {
        let mut inner = self.inner.lock();
        if let Some(held) = inner.sets.get_mut(peer) {
            if held.served.cycle == cycle && held.proofs == Some(page) {
                held.proofs = None;
            }
        }
    }

    /// The proofs held back for `peer`'s exchange `cycle`, taken: a request
    /// of the exchange after the page they were held for arrived, which its
    /// requester sends only once it has that page. Proofs of a set left idle
    /// are gone, never taken.
    pub(crate) fn take_proofs(
        &self,
        peer: &PeerId,
        cycle: u64,
        now: Instant,
        idle: Duration,
    ) -> Option<HeldProofs> {
        let mut inner = self.inner.lock();
        inner.sweep(now, idle);
        let held = inner.sets.get_mut(peer)?;
        if held.served.cycle != cycle {
            return None;
        }
        let page = held.proofs.take()?;
        Some(HeldProofs {
            hints: Arc::clone(&held.served.hints),
            page,
            page_keys: held.served.page_keys,
        })
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

/// The pages an exchange runs, in which the responder announces
/// `responder_pages` and the requester `requester_pages`: as many as the side
/// that announces more, and no more than an exchange takes
/// ([`MAX_NEIGHBOR_SYNC_PAGES`]), the requester following no further.
pub(crate) fn exchange_rounds(responder_pages: u32, requester_pages: u32) -> u32 {
    responder_pages
        .max(requester_pages)
        .min(MAX_NEIGHBOR_SYNC_PAGES)
}

/// Whether the repair proofs of the keys on `page` wait for the requester's
/// next request, in an exchange of `rounds` pages: every page's but the last,
/// after which the requester sends nothing more, so the last page's keys get
/// no proofs from this exchange.
pub(crate) fn proofs_wait_for_next_page(page: u32, rounds: u32) -> bool {
    page.saturating_add(1) < rounds
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::ant_protocol::XorName;
    use crate::replication::neighbor_sync::RoutingSnapshot;

    fn peer(byte: u8) -> PeerId {
        PeerId::from_bytes([byte; 32])
    }

    fn hints(replica: usize, paid: usize) -> PeerSyncHints {
        let snapshot = Arc::new(RoutingSnapshot::new(peer(0), vec![peer(1)]));
        let mut hints = PeerSyncHints::empty(snapshot, 7);
        hints.replica = (0..replica)
            .map(|n| {
                let mut key = [0u8; 32];
                key[..8].copy_from_slice(&(n as u64).to_be_bytes());
                key
            })
            .collect();
        hints.paid = (0..paid)
            .map(|n| {
                let mut key = [1u8; 32];
                key[..8].copy_from_slice(&(n as u64).to_be_bytes());
                key
            })
            .collect();
        hints
    }

    fn served(cycle: u64, keys: usize) -> ServedHints {
        let pages = u32::try_from(keys.div_ceil(2).max(1)).unwrap();
        ServedHints {
            cycle,
            hints: Arc::new(hints(keys, 0)),
            page_keys: 2,
            pages,
            announced: pages,
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

    #[test]
    fn a_version_names_its_release_after_ds() {
        assert_eq!(release_of("0.23.0+ds.15"), Some(15));
        assert_eq!(release_of("0.24.0+ds.17.1"), Some(17));
        for none in ["0.23.0", "0.23.0+dsx.17", "0.23.0+ds.", "garbage", ""] {
            assert_eq!(release_of(none), None, "{none:?}");
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
    }

    /// The last release whose nodes read no pages. This tree announced it, by
    /// its version, until the release that carries pages was cut. When a
    /// release without pages is cut before it, its number goes here.
    const LAST_RELEASE_WITHOUT_PAGES: u64 = 15;

    /// The release nodes page from is the one this tree is cut as: nodes of
    /// this tree page to each other only once their agents name it, and a
    /// release numbered past it that lacked pages would be paged and never
    /// answer. So the tree's version names the release before pages (not cut
    /// yet) or the release nodes page from (the cut). Once that release has
    /// shipped, this relaxes to "at least the release nodes page from".
    #[test]
    fn the_release_nodes_page_from_is_the_one_this_tree_is_cut_as() {
        let own = release_of(env!("CARGO_PKG_VERSION")).expect("a +ds version");
        assert!(
            own == LAST_RELEASE_WITHOUT_PAGES || own == NEIGHBOR_SYNC_PAGES_FROM_RELEASE,
            "this tree is ds.{own}: nodes page from ds.{NEIGHBOR_SYNC_PAGES_FROM_RELEASE} \
             (NEIGHBOR_SYNC_PAGES_FROM_RELEASE), and the last release without pages is \
             ds.{LAST_RELEASE_WITHOUT_PAGES}; set the one that moved"
        );
        const { assert!(LAST_RELEASE_WITHOUT_PAGES < NEIGHBOR_SYNC_PAGES_FROM_RELEASE) };
    }

    #[test]
    fn a_peer_marked_single_gets_one_single_message() {
        let peers = SyncPeers::default();
        let a = peer(1);
        assert!(!peers.take_single_next(&a), "unmarked");
        peers.mark_single_next(a);
        assert!(peers.take_single_next(&a), "marked");
        assert!(!peers.take_single_next(&a), "spent on one exchange");
        peers.mark_single_next(a);
        peers.forget(&a);
        assert!(!peers.take_single_next(&a), "forgotten with the peer");
    }

    #[test]
    fn the_marks_and_windows_kept_are_bounded() {
        let peers = SyncPeers::default();
        for index in 0..=MAX_PEERS_KEPT {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
            let id = PeerId::from_bytes(bytes);
            peers.mark_single_next(id);
            let _cut = peers.window(&id, SyncRole::Requester, hints(3, 0), 2);
        }
        assert_eq!(peers.single_next.lock().len(), MAX_PEERS_KEPT);
        assert_eq!(peers.windows.lock().len(), MAX_PEERS_KEPT);
    }

    #[test]
    fn one_exchange_at_a_time_with_each_peer() {
        let peers = Arc::new(SyncPeers::default());
        let first = peers.try_start(peer(1)).expect("no exchange in progress");
        assert!(peers.try_start(peer(1)).is_none(), "one at a time");
        let other = peers.try_start(peer(2)).expect("other peers are free");
        drop(first);
        assert!(peers.try_start(peer(1)).is_some(), "free once it ends");
        drop(other);
    }

    /// Hints over what one exchange carries are sent a window at a time, the
    /// next exchange with the peer going on from where the last ended, so
    /// every key of both lists goes out within the exchanges it takes to go
    /// round each list once.
    #[test]
    fn windows_go_round_every_key_of_a_peer() {
        let peers = SyncPeers::default();
        let a = peer(1);
        let all = hints(500, 100);
        let max_keys = 256;
        let (mut replica_seen, mut paid_seen) = (HashSet::new(), HashSet::new());
        // 213 replica and 43 paid hints a window: 3 windows go round both.
        for _ in 0..3 {
            let (window, cut) = peers.window(&a, SyncRole::Requester, all.clone(), max_keys);
            cut.expect("cut")
                .commit(window.replica.len(), window.paid.len());
            assert_eq!(window.replica.len() + window.paid.len(), max_keys);
            assert!(!window.paid.is_empty(), "the paid hints have a share");
            replica_seen.extend(window.replica.iter().copied());
            paid_seen.extend(window.paid.iter().copied());
        }
        assert_eq!(replica_seen.len(), 500, "every replica hint went out");
        assert_eq!(paid_seen.len(), 100, "every paid hint went out");

        // Hints that fit go whole, and leave the peer's window where it was.
        let few = hints(10, 5);
        let (sent, cut) = peers.window(&a, SyncRole::Requester, few.clone(), max_keys);
        assert!(cut.is_none());
        assert_eq!((sent.replica, sent.paid), (few.replica, few.paid));
    }

    fn index_of(list: &[XorName], key: &XorName) -> usize {
        list.iter()
            .position(|k| k == key)
            .expect("a key of the list")
    }

    /// Where `window` starts in each list of `all`.
    fn starts_of(all: &PeerSyncHints, window: &PeerSyncHints) -> (usize, usize) {
        (
            index_of(&all.replica, &window.replica[0]),
            index_of(&all.paid, &window.paid[0]),
        )
    }

    /// A window moves on by the keys that were delivered: an exchange stopped
    /// after `k` pages makes the next window start `k` pages on, one that
    /// delivered all of it by the whole window, and one that delivered
    /// nothing leaves it where it was, until several in a row move it a
    /// little.
    #[test]
    fn a_window_moves_on_by_what_was_delivered() {
        let page_keys = 4;
        let all = hints(500, 100);
        let a = peer(1);

        // Stopped after 3 pages: 12 replica keys delivered, no paid ones.
        let peers = SyncPeers::default();
        let (window, cut) = peers.window(&a, SyncRole::Requester, all.clone(), 256);
        let first = starts_of(&all, &window);
        cut.expect("cut").commit_pages(&window, 3, page_keys);
        let (window, cut) = peers.window(&a, SyncRole::Requester, all.clone(), 256);
        assert_eq!(
            starts_of(&all, &window),
            ((first.0 + 12) % 500, first.1),
            "on by the 3 pages delivered, no paid key among them"
        );

        // Delivered whole: on by the whole window, 213 replica and 43 paid.
        let start = starts_of(&all, &window);
        cut.expect("cut")
            .commit(window.replica.len(), window.paid.len());
        let (window, cut) = peers.window(&a, SyncRole::Requester, all.clone(), 256);
        assert_eq!(
            starts_of(&all, &window),
            ((start.0 + 213) % 500, (start.1 + 43) % 100)
        );

        // Delivered nothing: the same window again, until the stalls move it.
        let start = starts_of(&all, &window);
        cut.expect("cut").commit(0, 0);
        for _ in 1..STALLS_BEFORE_SHIFT {
            let (window, cut) = peers.window(&a, SyncRole::Requester, all.clone(), 256);
            assert_eq!(starts_of(&all, &window), start);
            cut.expect("cut").commit(0, 0);
        }
        let (window, _) = peers.window(&a, SyncRole::Requester, all.clone(), 256);
        let shifted = starts_of(&all, &window);
        // 213 replica and 43 paid keys a window: at most one key of each.
        assert_eq!(
            shifted,
            ((start.0 + 1) % 500, (start.1 + 1) % 100),
            "moved on a little after the stalls"
        );

        // Cuts in one role don't overlap (one exchange with a peer at a
        // time); should two, a cut made since isn't moved back by the
        // earlier one.
        let peers = SyncPeers::default();
        let (window, first_cut) = peers.window(&a, SyncRole::Requester, all.clone(), 256);
        let start = starts_of(&all, &window);
        let (_, second_cut) = peers.window(&a, SyncRole::Requester, all.clone(), 256);
        first_cut.expect("cut").commit(0, 0);
        second_cut.expect("cut").commit(213, 43);
        let (window, _) = peers.window(&a, SyncRole::Requester, all.clone(), 256);
        assert_eq!(starts_of(&all, &window).0, (start.0 + 426) % 500);
    }

    /// A node and its peer syncing each other at once cut windows in both
    /// roles: each role goes on from its own window, so the keys the first
    /// exchange didn't deliver go first in the next exchange in its role,
    /// whatever the other role's exchange delivered meanwhile.
    #[test]
    fn overlapping_cuts_in_both_roles_keep_each_roles_undelivered_keys() {
        let page_keys = 4;
        let all = hints(500, 100);
        let a = peer(1);
        let peers = SyncPeers::default();

        let (requested, requester_cut) = peers.window(&a, SyncRole::Requester, all.clone(), 256);
        let requester_start = starts_of(&all, &requested);
        let (served, responder_cut) = peers.window(&a, SyncRole::Responder, all.clone(), 256);
        let responder_start = starts_of(&all, &served);

        // The requester's exchange delivers 3 pages of its window; the
        // responder's, cut meanwhile, all of its own.
        requester_cut
            .expect("cut")
            .commit_pages(&requested, 3, page_keys);
        responder_cut
            .expect("cut")
            .commit(served.replica.len(), served.paid.len());

        let (requested, _) = peers.window(&a, SyncRole::Requester, all.clone(), 256);
        assert_eq!(
            starts_of(&all, &requested),
            ((requester_start.0 + 12) % 500, requester_start.1),
            "the requester's undelivered keys go first in its next window"
        );
        let (served, _) = peers.window(&a, SyncRole::Responder, all.clone(), 256);
        assert_eq!(
            starts_of(&all, &served),
            (
                (responder_start.0 + 213) % 500,
                (responder_start.1 + 43) % 100
            ),
            "the responder's window went on by all it delivered"
        );

        peers.forget(&a);
        assert!(peers.windows.lock().is_empty(), "both roles forgotten");
    }

    /// A responder's set moves the requester's window on by the pages the
    /// requester's later requests showed arrived, once the set is dropped.
    #[test]
    fn a_served_window_moves_on_by_the_pages_that_arrived() {
        let all = hints(600, 1);
        let a = peer(1);
        let peers = SyncPeers::default();
        let sets = ServedHintSets::new(1 << 20);
        let now = Instant::now();

        let (window, cut) = peers.window(&a, SyncRole::Requester, all.clone(), 512);
        let start = starts_of(&all, &window).0;
        sets.insert(
            a,
            ServedHints {
                cycle: 5,
                hints: Arc::new(window),
                page_keys: 2,
                pages: 256,
                announced: 301,
            },
            cut,
            now,
        );
        sets.note_arrived(&a, 5, 1);
        sets.note_arrived(&a, 6, 9);
        sets.note_arrived(&a, 5, 4);
        sets.note_arrived(&a, 5, 3);
        // The requester gave up after its request for page 4: pages 0 to 3
        // arrived.
        assert!(sets.get(&a, 5, now + IDLE * 2, IDLE).is_none());
        let (window, _) = peers.window(&a, SyncRole::Requester, all.clone(), 512);
        assert_eq!(starts_of(&all, &window).0, (start + 8) % 600);
    }

    #[test]
    fn what_a_requester_holds_of_answers_is_bounded_and_given_back() {
        // Room for 10 keys.
        let budget = Arc::new(ReceivedHints::new(320));
        let mut first = HeldAnswer::default();
        assert_eq!(budget.hold(&mut first, 6), 6);
        let mut second = HeldAnswer::default();
        assert_eq!(budget.hold(&mut second, 6), 4, "only the room left");
        assert_eq!(budget.hold(&mut second, 1), 0, "none left");
        assert_eq!((first.bytes(), second.bytes()), (192, 128));
        assert_eq!(budget.held(), 320);
        drop(first);
        assert_eq!(budget.held(), 128, "given back once dropped");
        drop(second);
        assert_eq!(budget.held(), 0);
        assert_eq!(budget.most_held(), 320, "the most held at once");
    }

    #[test]
    fn a_set_serves_its_exchange_and_is_dropped_after_it() {
        let sets = ServedHintSets::new(1 << 20);
        let now = Instant::now();
        let a = peer(1);
        assert!(sets.get(&a, 5, now, IDLE).is_none());
        sets.insert(a, served(5, 10), None, now);
        assert_eq!(sets.held(), (1, 320));
        assert_eq!(sets.built(), 1);
        let got = sets.get(&a, 5, now, IDLE).expect("held for exchange 5");
        assert_eq!((got.cycle, got.pages), (5, 5));

        // Another exchange of the same requester drops it.
        assert!(sets.get(&a, 6, now, IDLE).is_none());
        assert_eq!(sets.held(), (0, 0));

        sets.insert(a, served(7, 4), None, now);
        sets.finish(&a, 8);
        assert_eq!(sets.held().0, 1, "finishing another exchange keeps it");
        sets.finish(&a, 7);
        assert_eq!(sets.held(), (0, 0));

        sets.insert(a, served(9, 4), None, now);
        sets.forget(&a);
        assert_eq!(sets.held(), (0, 0));
    }

    #[test]
    fn a_set_whose_requester_stopped_asking_is_dropped() {
        let sets = ServedHintSets::new(1 << 20);
        let start = Instant::now();
        let a = peer(1);
        sets.insert(a, served(5, 10), None, start);
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
        sets.insert(peer(1), served(1, 40), None, now);
        assert!(!sets.may_build(now, IDLE), "over the budget");
        sets.finish(&peer(1), 1);
        sets.insert(peer(2), served(2, 4), None, now);
        assert!(sets.may_build(now, IDLE), "under the budget");
        sets.insert(peer(3), served(3, 6), None, now);
        assert_eq!(sets.held(), (2, 320));
        assert!(!sets.may_build(now, IDLE), "at the budget");
    }

    #[test]
    fn an_exchange_with_a_rejected_page_stays_unclean() {
        let sets = ServedHintSets::new(1 << 20);
        let now = Instant::now();
        let a = peer(1);
        sets.insert(a, served(5, 10), None, now);
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

    /// A page's repair proofs wait for the requester's next request of the
    /// exchange: taken once, then gone.
    #[test]
    fn a_pages_proofs_are_taken_by_the_next_request_of_its_exchange() {
        let sets = ServedHintSets::new(1 << 20);
        let now = Instant::now();
        let a = peer(1);
        sets.insert(a, served(5, 10), None, now);
        sets.hold_proofs(&a, 5, 0);
        assert!(
            sets.take_proofs(&a, 6, now, IDLE).is_none(),
            "another exchange's request shows nothing of this one"
        );
        sets.insert(a, served(5, 10), None, now);
        sets.hold_proofs(&a, 5, 2);
        let taken = sets.take_proofs(&a, 5, now, IDLE).expect("held for page 2");
        assert_eq!((taken.page, taken.page_keys), (2, 2));
        assert_eq!(
            taken.hints.page(taken.page, taken.page_keys).replica,
            &taken.hints.replica[4..6]
        );
        assert!(sets.take_proofs(&a, 5, now, IDLE).is_none(), "taken once");
    }

    /// Proofs of a page never sent, or of a set its requester left idle, are
    /// dropped, never recorded.
    #[test]
    fn proofs_of_a_page_not_followed_up_are_dropped() {
        let sets = ServedHintSets::new(1 << 20);
        let start = Instant::now();
        let a = peer(1);
        sets.insert(a, served(5, 10), None, start);
        sets.hold_proofs(&a, 5, 1);
        sets.drop_proofs(&a, 5, 0);
        assert!(
            sets.take_proofs(&a, 5, start, IDLE).is_some(),
            "dropping another page's leaves these"
        );
        sets.hold_proofs(&a, 5, 1);
        sets.drop_proofs(&a, 5, 1);
        assert!(
            sets.take_proofs(&a, 5, start, IDLE).is_none(),
            "the page wasn't sent"
        );

        sets.hold_proofs(&a, 5, 3);
        assert!(
            sets.take_proofs(&a, 5, start + IDLE * 2, IDLE).is_none(),
            "the requester stopped asking"
        );
        assert_eq!(sets.held(), (0, 0));
    }

    #[test]
    fn only_the_last_pages_proofs_are_not_held() {
        assert!(proofs_wait_for_next_page(0, 2));
        assert!(proofs_wait_for_next_page(254, 256));
        assert!(!proofs_wait_for_next_page(1, 2), "the last page");
        assert!(!proofs_wait_for_next_page(0, 1), "a one-page exchange");
    }

    /// An exchange runs as many pages as the side that announces more, up to
    /// what an exchange takes: a side announcing more than that (its hints
    /// come to more than one exchange carries) ends the exchange at its 256th
    /// page, whose proofs aren't held.
    #[test]
    fn an_exchange_runs_the_pages_of_the_side_with_more_up_to_its_bound() {
        assert_eq!(exchange_rounds(3, 1), 3);
        assert_eq!(exchange_rounds(1, 5), 5);
        assert_eq!(exchange_rounds(300, 2), MAX_NEIGHBOR_SYNC_PAGES);
        assert_eq!(exchange_rounds(2, u32::MAX), MAX_NEIGHBOR_SYNC_PAGES);
        let rounds = exchange_rounds(300, 2);
        assert!(proofs_wait_for_next_page(
            MAX_NEIGHBOR_SYNC_PAGES - 2,
            rounds
        ));
        assert!(!proofs_wait_for_next_page(
            MAX_NEIGHBOR_SYNC_PAGES - 1,
            rounds
        ));
    }
}

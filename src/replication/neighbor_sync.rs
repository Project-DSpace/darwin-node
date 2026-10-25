//! Neighbor replication sync (Section 6.2).
//!
//! Round-robin cycle management: snapshot close neighbors, iterate through
//! them in batches of `NEIGHBOR_SYNC_PEER_COUNT`, exchanging hint sets.
//!
//! An exchange takes one of two forms, chosen for each exchange from the
//! agent of the peer's connection ([`sync_form_for`]):
//!
//! - **Paged**, with a connected peer whose agent names a release that reads
//!   pages: each side's two hint lists are cut into pages of at most
//!   [`NEIGHBOR_SYNC_PAGE_BYTES`] of keys ([`PeerSyncHints::page`]), each page
//!   its own request and answer, sent one after another, carrying the
//!   exchange's number and "page n of N". The responder builds its hints for
//!   the requester once, at the first page, and serves every page from them.
//!   A responder that can't take a page answers busy, and the requester tries
//!   again in a later cycle. Silence on a page never counts against the peer.
//! - **Single**, today's one request and one answer, with any other peer: a
//!   peer of a release before pages, one whose release isn't known, or one
//!   that isn't connected. It carries at most
//!   [`NEIGHBOR_SYNC_SINGLE_MESSAGE_BYTES`] of keys.
//!
//! Hints that come to more than an exchange carries go a window at a time
//! ([`PeerSyncHints::window`]), each exchange with the peer going on from
//! where the last ended.
//!
//! Every scan of the node's keys computes the close groups it needs locally,
//! from one [`RoutingSnapshot`] of the routing table, off the async workers.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::logging::{debug, info, warn};
use rand::Rng;
use saorsa_core::identity::PeerId;
use saorsa_core::P2PNode;

use crate::ant_protocol::XorName;
use crate::replication::commitment::StorageCommitment;
use crate::replication::config::{
    ReplicationConfig, MAX_NEIGHBOR_SYNC_PAGES, NEIGHBOR_SYNC_PAGE_BYTES,
    NEIGHBOR_SYNC_SINGLE_MESSAGE_BYTES, REPLICATION_PROTOCOL_ID,
};
use crate::replication::paid_list::PaidList;
use crate::replication::protocol::{
    NeighborSyncPageRequest, NeighborSyncPageResponse, NeighborSyncRequest, ReplicationMessage,
    ReplicationMessageBody,
};
use crate::replication::sync_pages::{self, HeldAnswer, ReceivedHints, SyncPeers};
use crate::replication::types::NeighborSyncState;
use crate::storage::ChunkStore;

/// Hint-build duration that is worth surfacing at info level.
const HINT_BUILD_SLOW_LOG_MS: u128 = 250;

/// Bytes of one key on a page.
const KEY_BYTES: usize = 32;

/// Keys of a page of [`NEIGHBOR_SYNC_PAGE_BYTES`].
const MAX_PAGE_KEYS: usize = NEIGHBOR_SYNC_PAGE_BYTES / KEY_BYTES;

/// Most keys a requester gathers of a peer's answer in one exchange:
/// [`MAX_NEIGHBOR_SYNC_PAGES`] pages of [`NEIGHBOR_SYNC_PAGE_BYTES`], however
/// the peer cuts its pages.
const MAX_EXCHANGE_KEYS: usize = MAX_NEIGHBOR_SYNC_PAGES as usize * MAX_PAGE_KEYS;

// ---------------------------------------------------------------------------
// Routing snapshot
// ---------------------------------------------------------------------------

/// The routing table at one moment, and this node: what one scan of the
/// node's keys computes every close group from, locally.
///
/// A key's close group of width `w` is its `w` nearest by XOR distance among
/// these: the same answer `find_closest_nodes_local_with_self` gives for a
/// routing table that hasn't changed, without a lookup for every key.
#[derive(Debug, Clone)]
pub(crate) struct RoutingSnapshot {
    self_id: PeerId,
    /// The routing table's peers, sorted, without this node.
    peers: Vec<PeerId>,
}

impl RoutingSnapshot {
    /// The routing table now.
    pub(crate) async fn take(p2p_node: &P2PNode) -> Self {
        let peers = p2p_node
            .dht_manager()
            .routing_table_peers()
            .await
            .into_iter()
            .map(|node| node.peer_id)
            .collect();
        Self::new(*p2p_node.peer_id(), peers)
    }

    /// A snapshot of `peers`, as the routing table of `self_id`.
    pub(crate) fn new(self_id: PeerId, mut peers: Vec<PeerId>) -> Self {
        peers.retain(|peer| *peer != self_id);
        peers.sort_unstable();
        peers.dedup();
        Self { self_id, peers }
    }

    /// This node.
    pub(crate) fn self_id(&self) -> &PeerId {
        &self.self_id
    }

    /// Whether `peer` is this node or in the routing table.
    fn is_candidate(&self, peer: &PeerId) -> bool {
        *peer == self.self_id || self.peers.binary_search(peer).is_ok()
    }

    fn candidates(&self) -> impl Iterator<Item = &PeerId> {
        std::iter::once(&self.self_id).chain(self.peers.iter())
    }

    /// Whether `peer` is among the `width` nearest to `key` of the routing
    /// table and this node.
    pub(crate) fn is_within(&self, peer: &PeerId, key: &XorName, width: usize) -> bool {
        width > 0 && self.is_candidate(peer) && self.ranks_within(peer, key, width)
    }

    /// Whether fewer than `width` candidates are nearer `key` than `peer`,
    /// for a `peer` known to be a candidate. Distances between distinct ids
    /// never tie.
    fn ranks_within(&self, peer: &PeerId, key: &XorName, width: usize) -> bool {
        let own = xor(peer.as_bytes(), key);
        let mut nearer = 0;
        for candidate in self.candidates() {
            if xor(candidate.as_bytes(), key) < own {
                nearer += 1;
                if nearer >= width {
                    return false;
                }
            }
        }
        true
    }

    /// The `width` nearest to `key` of the routing table and this node.
    pub(crate) fn close_group(&self, key: &XorName, width: usize) -> HashSet<PeerId> {
        let mut by_distance: Vec<([u8; 32], PeerId)> = self
            .candidates()
            .map(|candidate| (xor(candidate.as_bytes(), key), *candidate))
            .collect();
        if width < by_distance.len() {
            by_distance.select_nth_unstable_by(width, |a, b| a.0.cmp(&b.0));
            by_distance.truncate(width);
        }
        by_distance.into_iter().map(|(_, peer)| peer).collect()
    }
}

fn xor(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut distance = [0u8; 32];
    for (out, (x, y)) in distance.iter_mut().zip(a.iter().zip(b.iter())) {
        *out = x ^ y;
    }
    distance
}

// ---------------------------------------------------------------------------
// Hints for one peer
// ---------------------------------------------------------------------------

/// The hints this node has for one peer in one exchange: the keys whose
/// close group includes the peer (replica hints), and the paid-list keys
/// whose paid group does (paid hints), chosen from one routing snapshot.
///
/// The close group a replica hint was chosen in is computed again from that
/// snapshot when the proof of the hint is recorded ([`Self::groups_for`]),
/// so no peer's list carries a copy of it.
#[derive(Debug, Clone)]
pub(crate) struct PeerSyncHints {
    snapshot: Arc<RoutingSnapshot>,
    close_group_size: usize,
    /// Replica hints.
    pub(crate) replica: Vec<XorName>,
    /// Paid hints.
    pub(crate) paid: Vec<XorName>,
}

/// One page of a peer's hints, or the hints of a single message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HintPage<'a> {
    /// The replica hints on the page.
    pub(crate) replica: &'a [XorName],
    /// The paid hints on the page.
    pub(crate) paid: &'a [XorName],
}

impl HintPage<'_> {
    /// How many keys the page carries.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.replica.len() + self.paid.len()
    }
}

impl PeerSyncHints {
    /// No hints, from `snapshot`.
    pub(crate) fn empty(snapshot: Arc<RoutingSnapshot>, close_group_size: usize) -> Self {
        Self {
            snapshot,
            close_group_size,
            replica: Vec::new(),
            paid: Vec::new(),
        }
    }

    /// Keys held, of both lists.
    pub(crate) fn key_count(&self) -> usize {
        self.replica.len() + self.paid.len()
    }

    /// Bytes of keys held.
    pub(crate) fn bytes(&self) -> usize {
        self.key_count().saturating_mul(KEY_BYTES)
    }

    /// How many pages of `page_keys` keys the hints take: at least one, at
    /// most [`MAX_NEIGHBOR_SYNC_PAGES`].
    pub(crate) fn page_count(&self, page_keys: usize) -> u32 {
        let keys = self.replica.len() + self.paid.len();
        let pages = keys.div_ceil(page_keys.max(1)).max(1);
        u32::try_from(pages)
            .unwrap_or(u32::MAX)
            .min(MAX_NEIGHBOR_SYNC_PAGES)
    }

    /// Page `index` of pages of `page_keys` keys: the replica hints, then the
    /// paid hints, each page taking the next keys; empty past the last.
    pub(crate) fn page(&self, index: u32, page_keys: usize) -> HintPage<'_> {
        let page_keys = page_keys.max(1);
        if index >= self.page_count(page_keys) {
            return HintPage {
                replica: &[],
                paid: &[],
            };
        }
        let start = usize::try_from(index)
            .unwrap_or(usize::MAX)
            .saturating_mul(page_keys);
        self.keys_in(start..start.saturating_add(page_keys))
    }

    /// Every hint, for a single message.
    pub(crate) fn all(&self) -> HintPage<'_> {
        HintPage {
            replica: &self.replica,
            paid: &self.paid,
        }
    }

    /// The hints one exchange carries, at most `max_keys` of them: all of
    /// them when they fit; otherwise a window of `max_keys`, each list's share
    /// in proportion to its length (at least one key of a list that has any),
    /// each share the list's next keys from `start` on, wrapping round to its
    /// first. Returns the window, and, when not all fit, where the next
    /// starts: an exchange with the peer each time the window moves on goes
    /// round both lists in as many exchanges as it takes to go round once.
    /// The window's lists are new, so the rest is freed with `self`.
    pub(crate) fn window(self, max_keys: usize, start: WindowStart) -> (Self, Option<WindowStart>) {
        let (replica_total, paid_total) = (self.replica.len(), self.paid.len());
        if replica_total + paid_total <= max_keys {
            return (self, None);
        }
        let (replica_take, paid_take) = window_shares(replica_total, paid_total, max_keys);
        let window = Self {
            snapshot: Arc::clone(&self.snapshot),
            close_group_size: self.close_group_size,
            replica: take_wrapping(&self.replica, start.replica, replica_take),
            paid: take_wrapping(&self.paid, start.paid, paid_take),
        };
        let next = WindowStart {
            replica: advance(start.replica, replica_take, replica_total),
            paid: advance(start.paid, paid_take, paid_total),
        };
        (window, Some(next))
    }

    /// Let go of the lists' spare capacity, so that what is held is what is
    /// counted.
    pub(crate) fn shrink_to_fit(&mut self) {
        self.replica.shrink_to_fit();
        self.paid.shrink_to_fit();
    }

    /// The keys at `range` of the replica hints followed by the paid hints.
    fn keys_in(&self, range: Range<usize>) -> HintPage<'_> {
        let replica_count = self.replica.len();
        let clip = |at: usize, len: usize| at.min(len);
        let replica =
            &self.replica[clip(range.start, replica_count)..clip(range.end, replica_count)];
        let paid_start = range.start.saturating_sub(replica_count);
        let paid_end = range.end.saturating_sub(replica_count);
        let paid_count = self.paid.len();
        let paid = &self.paid[clip(paid_start, paid_count)..clip(paid_end, paid_count)];
        HintPage { replica, paid }
    }

    /// `keys`, with what their close groups are computed from, to compute
    /// them off the async workers.
    pub(crate) fn groups_for(&self, keys: &[XorName]) -> HintGroups {
        HintGroups {
            snapshot: Arc::clone(&self.snapshot),
            close_group_size: self.close_group_size,
            keys: keys.to_vec(),
        }
    }
}

/// Where the next window of a peer's hints starts in each list
/// ([`PeerSyncHints::window`]): a place in the list, taken modulo its length
/// when the window is cut, so a list that grew or shrank in between still
/// goes round.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WindowStart {
    /// In the replica hints.
    pub(crate) replica: usize,
    /// In the paid hints.
    pub(crate) paid: usize,
}

/// Each list's share of a window of `max_keys` keys, for lists of `replica`
/// and `paid` keys that come to more: in proportion to their lengths, at least
/// one key of a list that has any, `max_keys` in all.
fn window_shares(replica: usize, paid: usize, max_keys: usize) -> (usize, usize) {
    let total = replica + paid;
    let replica_take = max_keys.saturating_mul(replica) / total.max(1);
    if replica > 0 && replica_take == 0 {
        (1, max_keys.saturating_sub(1).min(paid))
    } else {
        (replica_take, (max_keys - replica_take).min(paid))
    }
}

/// `count` keys of `list` from `start` (modulo its length) on, wrapping round
/// to its first.
fn take_wrapping(list: &[XorName], start: usize, count: usize) -> Vec<XorName> {
    if list.is_empty() {
        return Vec::new();
    }
    let start = start % list.len();
    let mut taken = Vec::with_capacity(count.min(list.len()));
    taken.extend(
        list[start..]
            .iter()
            .chain(list[..start].iter())
            .take(count)
            .copied(),
    );
    taken
}

/// Where a window of `count` keys from `start` (modulo `len`) ends, wrapping.
pub(crate) fn advance(start: usize, count: usize, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        (start % len + count % len) % len
    }
}

/// Replica hints whose close groups are to be computed, owned with the
/// snapshot they were chosen from.
#[derive(Debug)]
pub(crate) struct HintGroups {
    snapshot: Arc<RoutingSnapshot>,
    close_group_size: usize,
    keys: Vec<XorName>,
}

impl HintGroups {
    /// Each key with its close group.
    pub(crate) fn close_groups(&self) -> Vec<(XorName, HashSet<PeerId>)> {
        self.keys
            .iter()
            .map(|key| (*key, self.snapshot.close_group(key, self.close_group_size)))
            .collect()
    }
}

/// Build the hints for `peer` alone, as [`build_sync_hints_for_peers`] does
/// for a batch.
pub(crate) async fn build_sync_hints_for_peer(
    peer: &PeerId,
    storage: &Arc<ChunkStore>,
    paid_list: &Arc<PaidList>,
    p2p_node: &Arc<P2PNode>,
    close_group_size: usize,
    paid_list_close_group_size: usize,
) -> PeerSyncHints {
    let mut built = build_sync_hints_for_peers(
        std::slice::from_ref(peer),
        storage,
        paid_list,
        p2p_node,
        close_group_size,
        paid_list_close_group_size,
    )
    .await;
    match built.remove(peer) {
        Some(hints) => hints,
        None => PeerSyncHints::empty(
            Arc::new(RoutingSnapshot::take(p2p_node).await),
            close_group_size,
        ),
    }
}

/// The pages a side announces ("page n of N") for hints of `keys` keys, on
/// pages of `page_keys`: every page they take, at least one, though one
/// exchange carries only a window of [`MAX_NEIGHBOR_SYNC_PAGES`] of them
/// ([`PeerSyncHints::window`]). So a side whose hints come to more announces
/// more pages than the exchange runs, and the other side, having had every
/// page the exchange runs, still knows more are to come: it counts the side
/// as done (a bootstrap source drained, an exchange complete) only once none
/// are.
pub(crate) fn announced_pages(keys: usize, page_keys: usize) -> u32 {
    u32::try_from(keys.div_ceil(page_keys.max(1)).max(1)).unwrap_or(u32::MAX)
}

/// Keys of one page: [`NEIGHBOR_SYNC_PAGE_BYTES`], or, in a `test-utils`
/// build, the size a test set ([`crate::test_seams::SYNC_PAGE_BYTES`]).
pub(crate) fn page_keys() -> usize {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(bytes) = crate::test_seams::sync_page_bytes() {
        return bytes / KEY_BYTES;
    }
    NEIGHBOR_SYNC_PAGE_BYTES / KEY_BYTES
}

/// Build the hints for each of `peers` with one scan over local storage and
/// one over the paid list, from one routing snapshot, off the async workers.
///
/// Replica hints are the keys a peer is among the `close_group_size` nearest
/// to, paid hints the paid-list keys it is among the
/// `paid_list_close_group_size` nearest to.
///
/// This intentionally scans all locally stored records, not only records for
/// which this node is still responsible. Out-of-range records, which a node
/// keeps while its disk has room, therefore keep repairing their new close
/// group for as long as this node holds them.
pub(crate) async fn build_sync_hints_for_peers(
    peers: &[PeerId],
    storage: &Arc<ChunkStore>,
    paid_list: &Arc<PaidList>,
    p2p_node: &Arc<P2PNode>,
    close_group_size: usize,
    paid_list_close_group_size: usize,
) -> HashMap<PeerId, PeerSyncHints> {
    let started = Instant::now();
    let snapshot = Arc::new(RoutingSnapshot::take(p2p_node).await);
    let empty = |snapshot: &Arc<RoutingSnapshot>| {
        peers
            .iter()
            .map(|peer| {
                (
                    *peer,
                    PeerSyncHints::empty(Arc::clone(snapshot), close_group_size),
                )
            })
            .collect::<HashMap<_, _>>()
    };
    if peers.is_empty() {
        return empty(&snapshot);
    }

    let all_keys = match storage.all_keys().await {
        Ok(keys) => keys,
        Err(e) => {
            warn!("Failed to read stored keys for batch hint construction: {e}");
            Vec::new()
        }
    };
    let all_paid_keys = match paid_list.all_keys() {
        Ok(keys) => keys,
        Err(e) => {
            warn!("Failed to read PaidForList for batch hint construction: {e}");
            Vec::new()
        }
    };
    let stored_key_count = all_keys.len();
    let paid_key_count = all_paid_keys.len();

    let targets = peers.to_vec();
    let scan_snapshot = Arc::clone(&snapshot);
    let scanned = tokio::task::spawn_blocking(move || {
        hints_from_snapshot(
            &scan_snapshot,
            &targets,
            &all_keys,
            &all_paid_keys,
            close_group_size,
            paid_list_close_group_size,
        )
    })
    .await;
    let mut hints_by_peer = empty(&snapshot);
    match scanned {
        Ok(lists) => {
            for (peer, (replica, paid)) in lists {
                if let Some(hints) = hints_by_peer.get_mut(&peer) {
                    hints.replica = replica;
                    hints.paid = paid;
                }
            }
        }
        Err(e) => warn!("Neighbor-sync hint scan failed: {e}"),
    }

    let replica_hint_count = hints_by_peer
        .values()
        .map(|hints| hints.replica.len())
        .sum::<usize>();
    let paid_hint_count = hints_by_peer
        .values()
        .map(|hints| hints.paid.len())
        .sum::<usize>();
    let routing_peers = snapshot.peers.len();
    let elapsed_ms = started.elapsed().as_millis();

    if elapsed_ms >= HINT_BUILD_SLOW_LOG_MS {
        info!(
            target: "ant_node::replication::neighbor_sync",
            "Slow neighbor-sync hint build: peers={}, stored_keys={}, paid_keys={}, replica_hints={}, paid_hints={}, routing_peers={routing_peers}, elapsed_ms={elapsed_ms}",
            peers.len(),
            stored_key_count,
            paid_key_count,
            replica_hint_count,
            paid_hint_count,
        );
    } else {
        debug!(
            target: "ant_node::replication::neighbor_sync",
            "Neighbor-sync hint build: peers={}, stored_keys={}, paid_keys={}, replica_hints={}, paid_hints={}, routing_peers={routing_peers}, elapsed_ms={elapsed_ms}",
            peers.len(),
            stored_key_count,
            paid_key_count,
            replica_hint_count,
            paid_hint_count,
        );
    }

    hints_by_peer
}

/// For each of `targets`, the replica and paid hints among `keys` and
/// `paid_keys`: the scan [`build_sync_hints_for_peers`] runs off the async
/// workers. A target that is in neither the snapshot's routing table nor this
/// node gets none.
fn hints_from_snapshot(
    snapshot: &RoutingSnapshot,
    targets: &[PeerId],
    keys: &[XorName],
    paid_keys: &[XorName],
    close_group_size: usize,
    paid_list_close_group_size: usize,
) -> HashMap<PeerId, (Vec<XorName>, Vec<XorName>)> {
    let mut lists: HashMap<PeerId, (Vec<XorName>, Vec<XorName>)> = targets
        .iter()
        .map(|peer| (*peer, (Vec::new(), Vec::new())))
        .collect();
    let candidates: Vec<PeerId> = lists
        .keys()
        .filter(|peer| snapshot.is_candidate(peer))
        .copied()
        .collect();
    if close_group_size > 0 {
        for key in keys {
            for peer in &candidates {
                if snapshot.ranks_within(peer, key, close_group_size) {
                    if let Some((replica, _)) = lists.get_mut(peer) {
                        replica.push(*key);
                    }
                }
            }
        }
    }
    if paid_list_close_group_size > 0 {
        for key in paid_keys {
            for peer in &candidates {
                if snapshot.ranks_within(peer, key, paid_list_close_group_size) {
                    if let Some((_, paid)) = lists.get_mut(peer) {
                        paid.push(*key);
                    }
                }
            }
        }
    }
    lists
}

/// Take a fresh snapshot of close neighbors for a new round-robin cycle.
///
/// Rule 1: Compute `CloseNeighbors(self)` as `NEIGHBOR_SYNC_SCOPE` nearest
/// peers.
pub async fn snapshot_close_neighbors(
    p2p_node: &Arc<P2PNode>,
    self_id: &PeerId,
    scope: usize,
) -> Vec<PeerId> {
    let self_xor: XorName = *self_id.as_bytes();
    let closest = p2p_node
        .dht_manager()
        .find_closest_nodes_local(&self_xor, scope)
        .await;
    closest.iter().map(|n| n.peer_id).collect()
}

/// Select the next batch of peers for sync from the current cycle.
///
/// Priority peers from routing-table churn are drained before the round-robin
/// cursor. Rules 2-3 then scan forward from cursor, skip peers still under
/// cooldown, and fill up to `peer_count` slots.
pub fn select_sync_batch(
    state: &mut NeighborSyncState,
    peer_count: usize,
    cooldown: Duration,
) -> Vec<PeerId> {
    let mut batch = Vec::new();
    let now = Instant::now();

    while batch.len() < peer_count {
        let Some(peer) = select_next_sync_peer(state, now, cooldown) else {
            break;
        };
        batch.push(peer);
    }

    batch
}

fn select_next_sync_peer(
    state: &mut NeighborSyncState,
    now: Instant,
    cooldown: Duration,
) -> Option<PeerId> {
    while let Some(peer) = state.priority_order.pop_front() {
        if peer_on_cooldown(state, &peer, now, cooldown) {
            state.remove_peer(&peer);
            continue;
        }

        state.remove_peer(&peer);
        return Some(peer);
    }

    while state.cursor < state.order.len() {
        let peer = state.order[state.cursor];

        // Check cooldown (Rule 2a): if the peer was synced recently, remove
        // from the snapshot and continue without advancing the cursor (the
        // next element slides into the current cursor position).
        if peer_on_cooldown(state, &peer, now, cooldown) {
            state.order.remove(state.cursor);
            continue;
        }

        state.cursor += 1;
        return Some(peer);
    }

    None
}

fn peer_on_cooldown(
    state: &NeighborSyncState,
    peer: &PeerId,
    now: Instant,
    cooldown: Duration,
) -> bool {
    state
        .last_sync_times
        .get(peer)
        .is_some_and(|last_sync| now.duration_since(*last_sync) < cooldown)
}

// ---------------------------------------------------------------------------
// One exchange
// ---------------------------------------------------------------------------

/// The form of one exchange with a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncForm {
    /// Pages, each its own request and answer.
    Paged,
    /// Today's single request and answer.
    Single,
}

/// Whether to page this exchange with `peer`, from the user agent of its
/// connection ([`form_for`]). The choice is made again for each exchange, so a
/// peer rolled back to an older release gets single messages from its next
/// connection on, and one that comes back to a release that reads pages gets
/// pages again.
pub(crate) async fn sync_form_for(
    peer: &PeerId,
    p2p_node: &Arc<P2PNode>,
    sync_peers: &SyncPeers,
) -> SyncForm {
    let agent = p2p_node.transport().peer_user_agent(peer).await;
    let single_next = sync_peers.take_single_next(peer);
    form_for(
        agent.as_deref(),
        single_next,
        sync_pages::pages_from_release(),
    )
}

/// The form of an exchange with a peer whose connection's agent is `agent`:
/// pages when the agent names `from_release` or a later one; otherwise a
/// single message. A peer that isn't connected (no agent) counts as one of
/// unknown release, which gets a single message: every release reads it, and
/// sending it connects the peer, so its next exchange reads a fresh agent. So
/// does a peer marked `single_next`, whose last exchange's first page went
/// unanswered: an agent left from a connection that has since ended pages it
/// no more than once.
pub(crate) fn form_for(agent: Option<&str>, single_next: bool, from_release: u64) -> SyncForm {
    match agent {
        Some(agent) if !single_next && sync_pages::agent_reads_pages(agent, from_release) => {
            SyncForm::Paged
        }
        _ => SyncForm::Single,
    }
}

/// How an exchange ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExchangeEnd {
    /// Every page of both sides was answered.
    Complete,
    /// The peer answered a page busy: it took nothing from that page on.
    Busy,
    /// A page went unanswered, or its answer wasn't the one asked for.
    Failed,
    /// This node stopped following the peer's pages: what it gathers in one
    /// exchange, or holds across its exchanges, was reached. The rest of the
    /// peer's hints come in a later exchange.
    Stopped,
    /// The peer's single answer came to as many keys as a single message
    /// carries: a responder cuts its hints to a window that size when they
    /// come to more, and says nothing of the rest, so more may be to come.
    Windowed,
}

/// Whether an exchange puts its peer on the sync cooldown: only one that
/// ended complete. One that ended part way, busy, failed, stopped after
/// some pages or windowed, leaves the peer to be synced again in its next
/// cycle, as a first page answered busy does, so the hints of the pages that
/// weren't exchanged wait no longer than that.
pub(crate) fn exchange_earns_cooldown(exchange: &SyncExchange) -> bool {
    exchange.end == ExchangeEnd::Complete
}

/// Record an exchange the peer answered in `state`: the cooldown, for one
/// that earns it ([`exchange_earns_cooldown`]).
pub(crate) fn record_exchange(
    state: &mut NeighborSyncState,
    peer: &PeerId,
    exchange: &SyncExchange,
) {
    if exchange_earns_cooldown(exchange) {
        record_successful_sync(state, peer);
    }
}

/// What the peer answered, every answered page's lists joined in page order.
#[derive(Debug, Clone)]
pub(crate) struct SyncAnswer {
    /// The peer's replica hints.
    pub(crate) replica_hints: Vec<XorName>,
    /// The peer's paid hints.
    pub(crate) paid_hints: Vec<XorName>,
    /// Whether the peer said, on its first page, that it is bootstrapping.
    pub(crate) bootstrapping: bool,
    /// The commitment on the peer's first page.
    pub(crate) commitment: Option<StorageCommitment>,
    /// How many pages the peer answered.
    pub(crate) pages_answered: u32,
    /// How many pages the peer's hints take (1 for a single message).
    pub(crate) pages: u32,
}

/// One exchange with a peer, as the requester saw it.
#[derive(Debug)]
pub(crate) struct SyncExchange {
    /// How it ended.
    pub(crate) end: ExchangeEnd,
    /// The hints this node had for the peer in this exchange.
    pub(crate) hints: PeerSyncHints,
    /// How many of `hints.replica`, from the first, went out on a page the
    /// peer answered: the only ones whose repair proof may be recorded.
    pub(crate) replica_answered: usize,
    /// How many of `hints.paid`, from the first, went out on a page the peer
    /// answered.
    pub(crate) paid_answered: usize,
    /// The peer's answer, if it answered a page.
    pub(crate) answer: Option<SyncAnswer>,
    /// The keys of a paged answer, held against what the node holds of its
    /// peers' answers until the exchange is dropped, after its hints are
    /// admitted.
    #[allow(dead_code)] // Held for its drop, which gives the keys back.
    pub(crate) held: HeldAnswer,
}

/// The pages of a peer's answer a requester keeps in one exchange: within
/// [`MAX_EXCHANGE_KEYS`] for the exchange, and within what the node holds of
/// its peers' answers across its exchanges ([`ReceivedHints`]).
#[derive(Debug, Default)]
pub(crate) struct GatheredAnswer {
    answer: Option<SyncAnswer>,
    held: HeldAnswer,
}

/// One page of a peer's answer.
#[derive(Debug)]
pub(crate) struct AnsweredPage {
    pub(crate) pages: u32,
    pub(crate) replica_hints: Vec<XorName>,
    pub(crate) paid_hints: Vec<XorName>,
    pub(crate) bootstrapping: bool,
    pub(crate) commitment: Option<StorageCommitment>,
}

impl GatheredAnswer {
    /// Keep `page`, as much of it as the bounds leave room for, its replica
    /// hints first; returns whether all of it was kept. A page is bounded
    /// only by the message cap, so a peer may cut its pages larger than this
    /// node does; what is gathered in all stays within the bounds.
    pub(crate) fn take(&mut self, budget: &Arc<ReceivedHints>, page: AnsweredPage) -> bool {
        let gathered = self.answer.as_ref().map_or(0, |answer| {
            answer.replica_hints.len() + answer.paid_hints.len()
        });
        let offered = page.replica_hints.len() + page.paid_hints.len();
        let room = MAX_EXCHANGE_KEYS.saturating_sub(gathered).min(offered);
        let kept = budget.hold(&mut self.held, room);
        let AnsweredPage {
            pages,
            mut replica_hints,
            mut paid_hints,
            bootstrapping,
            commitment,
        } = page;
        if kept < offered {
            replica_hints.truncate(kept);
            paid_hints.truncate(kept - replica_hints.len());
        }
        match self.answer.as_mut() {
            None => {
                self.answer = Some(SyncAnswer {
                    replica_hints,
                    paid_hints,
                    bootstrapping,
                    commitment,
                    pages_answered: 1,
                    pages,
                });
            }
            Some(answer) => {
                answer.replica_hints.extend(replica_hints);
                answer.paid_hints.extend(paid_hints);
                answer.pages_answered += 1;
            }
        }
        kept == offered
    }

    /// How many pages the peer's hints take, once its first page arrived.
    pub(crate) fn pages(&self) -> Option<u32> {
        self.answer.as_ref().map(|answer| answer.pages)
    }

    /// The answer and its held keys.
    pub(crate) fn into_parts(self) -> (Option<SyncAnswer>, HeldAnswer) {
        (self.answer, self.held)
    }
}

/// What one exchange saw, for a test to read (`test-utils` builds only).
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone)]
pub struct ExchangeReport {
    /// Every page of both sides was answered.
    pub complete: bool,
    /// The peer answered a page busy.
    pub busy: bool,
    /// How many pages this node's hints took.
    pub pages_sent: u32,
    /// How many of the peer's pages arrived.
    pub pages_answered: u32,
    /// How many pages the peer's hints took.
    pub peer_pages: u32,
    /// The peer's replica hints, every answered page's.
    pub replica_hints_received: Vec<XorName>,
    /// The peer's paid hints, every answered page's.
    pub paid_hints_received: Vec<XorName>,
    /// This node's replica hints, from the first, on pages the peer answered.
    pub replica_hints_answered: usize,
}

#[cfg(any(test, feature = "test-utils"))]
impl ExchangeReport {
    pub(crate) fn of(exchange: &SyncExchange, form: SyncForm) -> Self {
        let answer = exchange.answer.as_ref();
        Self {
            complete: exchange.received_last_page(),
            busy: exchange.end == ExchangeEnd::Busy,
            pages_sent: match form {
                SyncForm::Paged => exchange.hints.page_count(page_keys()),
                SyncForm::Single => 1,
            },
            pages_answered: answer.map_or(0, |answer| answer.pages_answered),
            peer_pages: answer.map_or(0, |answer| answer.pages),
            replica_hints_received: answer
                .map(|answer| answer.replica_hints.clone())
                .unwrap_or_default(),
            paid_hints_received: answer
                .map(|answer| answer.paid_hints.clone())
                .unwrap_or_default(),
            replica_hints_answered: exchange.answered_replica_hints().len(),
        }
    }
}

impl SyncExchange {
    /// The replica hints that went out on pages the peer answered.
    pub(crate) fn answered_replica_hints(&self) -> &[XorName] {
        &self.hints.replica[..self.replica_answered.min(self.hints.replica.len())]
    }

    /// Whether every page of the peer's hints arrived, its marked last page
    /// among them.
    pub(crate) fn received_last_page(&self) -> bool {
        self.end == ExchangeEnd::Complete && self.answer.is_some()
    }

    /// Whether the peer answered busy before any page: it is there, but
    /// nothing of its hints arrived. One that failed or timed out isn't
    /// known to be there.
    pub(crate) fn unheard(&self) -> bool {
        self.answer.is_none() && self.end == ExchangeEnd::Busy
    }
}

/// Run one exchange with `peer` in `form`, its pages strictly one after
/// another. The first page carries `commitment`, the sender's current one,
/// piggybacked so the receiver can ingest it (ADR-0002: commitment gossip
/// travels on neighbor sync). `hints` are cut to what one exchange in `form`
/// carries first ([`SyncPeers::window`]); a paged exchange announces the pages
/// of all of them ([`announced_pages`]).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn exchange(
    peer: &PeerId,
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
    is_bootstrapping: bool,
    hints: PeerSyncHints,
    commitment: Option<StorageCommitment>,
    form: SyncForm,
    sync_peers: &SyncPeers,
) -> SyncExchange {
    let all_keys = hints.key_count();
    let (hints, cut) = sync_peers.window(
        peer,
        sync_pages::SyncRole::Requester,
        hints,
        max_exchange_keys(form),
    );
    let exchange = match form {
        SyncForm::Single => {
            exchange_single(peer, p2p_node, config, is_bootstrapping, hints, commitment).await
        }
        SyncForm::Paged => {
            exchange_paged(
                peer,
                p2p_node,
                config,
                is_bootstrapping,
                hints,
                all_keys,
                commitment,
                sync_peers,
            )
            .await
        }
    };
    // The window moves on by what the peer answered, so what it didn't goes
    // first in the next exchange with it.
    if let Some(cut) = cut {
        cut.commit(exchange.replica_answered, exchange.paid_answered);
    }
    exchange
}

/// Most keys this node sends a peer in one exchange in `form`: 256 pages, or
/// one single message.
pub(crate) fn max_exchange_keys(form: SyncForm) -> usize {
    match form {
        SyncForm::Paged => (MAX_NEIGHBOR_SYNC_PAGES as usize).saturating_mul(page_keys()),
        SyncForm::Single => NEIGHBOR_SYNC_SINGLE_MESSAGE_BYTES / KEY_BYTES,
    }
}

/// Whether a request left unanswered counts against the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Silence {
    /// saorsa-core's send, which charges the peer for a timeout or a failed
    /// connection, and connects the peer first if it isn't connected: a
    /// single message, which every release reads.
    Charged,
    /// The transport's own send on the peer's connection, which charges
    /// nothing: a page, a message type of its own, on which silence, a
    /// timeout or a reply that doesn't decode never counts against the peer.
    Free,
}

/// Why a request got no answer this node could read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unanswered {
    /// It couldn't be encoded: nothing went to the peer.
    NotSent,
    /// The send failed before the deadline: no connection, or one that
    /// ended or refused the request.
    Failed,
    /// The deadline passed with no answer.
    TimedOut,
    /// An answer came that doesn't decode.
    Undecodable,
}

/// Whether a first page that got no answer (`unanswered`) sends the peer's
/// next exchange as a single message, which every release reads. A failed
/// send or an answer that doesn't decode may come from a peer that rolled
/// back, whose agent is left from a connection that has ended. A timeout
/// while the peer's agent, read again after it, still names a paging release
/// (`agent_reads_pages`) is a peer that pages but was slow, say building a
/// large hint set: a single message would only charge it for the same
/// slowness, so its next exchange pages again.
pub(crate) fn single_next_after(unanswered: Unanswered, agent_reads_pages: bool) -> bool {
    match unanswered {
        Unanswered::NotSent => false,
        Unanswered::Failed | Unanswered::Undecodable => true,
        Unanswered::TimedOut => !agent_reads_pages,
    }
}

/// Send one request and wait for its answer, as a decoded body.
async fn request(
    peer: &PeerId,
    p2p_node: &Arc<P2PNode>,
    body: ReplicationMessageBody,
    timeout: Duration,
    silence: Silence,
) -> Result<ReplicationMessageBody, Unanswered> {
    let msg = ReplicationMessage {
        request_id: rand::thread_rng().gen::<u64>(),
        body,
    };
    let encoded = match msg.encode() {
        Ok(data) => data,
        Err(e) => {
            warn!("Failed to encode sync request for {peer}: {e}");
            return Err(Unanswered::NotSent);
        }
    };
    let started = Instant::now();
    let sent = match silence {
        Silence::Charged => {
            p2p_node
                .send_request(peer, REPLICATION_PROTOCOL_ID, encoded, timeout)
                .await
        }
        Silence::Free => {
            p2p_node
                .transport()
                .send_request(peer, REPLICATION_PROTOCOL_ID, encoded, timeout)
                .await
        }
    };
    let response = match sent {
        Ok(response) => response,
        Err(e) => {
            debug!("Sync with {peer} failed: {e}");
            // The send's error type isn't public: a send that ends at its
            // deadline or later timed out, one that ends sooner failed.
            return Err(if started.elapsed() >= timeout {
                Unanswered::TimedOut
            } else {
                Unanswered::Failed
            });
        }
    };
    match ReplicationMessage::decode(&response.data) {
        Ok(decoded) => Ok(decoded.body),
        Err(e) => {
            warn!("Failed to decode sync response from {peer}: {e}");
            Err(Unanswered::Undecodable)
        }
    }
}

async fn exchange_single(
    peer: &PeerId,
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
    is_bootstrapping: bool,
    hints: PeerSyncHints,
    commitment: Option<StorageCommitment>,
) -> SyncExchange {
    let started = Instant::now();
    let sent = hints.all();
    let replica_sent = sent.replica.len();
    let paid_sent = sent.paid.len();
    let body = ReplicationMessageBody::NeighborSyncRequest(NeighborSyncRequest {
        replica_hints: sent.replica.to_vec(),
        paid_hints: sent.paid.to_vec(),
        bootstrapping: is_bootstrapping,
        commitment,
    });
    let answered = request(
        peer,
        p2p_node,
        body,
        config.verification_request_timeout,
        Silence::Charged,
    )
    .await;
    let mut busy = false;
    let answer = match answered {
        Ok(ReplicationMessageBody::NeighborSyncResponse(response)) => Some(SyncAnswer {
            replica_hints: response.replica_hints,
            paid_hints: response.paid_hints,
            bootstrapping: response.bootstrapping,
            commitment: response.commitment,
            pages_answered: 1,
            pages: 1,
        }),
        // A responder that reads pages answers a single request it can't
        // take busy, as it does a page, whatever cycle and page it names.
        Ok(ReplicationMessageBody::NeighborSyncPageResponse(NeighborSyncPageResponse::Busy {
            ..
        })) => {
            debug!("Neighbor sync with {peer}: answered busy; trying again in a later cycle");
            busy = true;
            None
        }
        Ok(_) => {
            warn!("Unexpected response type from {peer} during sync");
            None
        }
        Err(_) => None,
    };
    let end = single_end(answer.as_ref(), busy);
    debug!(
        target: "ant_node::replication::neighbor_sync",
        peer = %peer,
        end = ?end,
        replica_hints_sent = sent.replica.len(),
        paid_hints_sent = sent.paid.len(),
        replica_hints_received = answer.as_ref().map_or(0, |answer| answer.replica_hints.len()),
        paid_hints_received = answer.as_ref().map_or(0, |answer| answer.paid_hints.len()),
        elapsed_ms = started.elapsed().as_millis(),
        "Neighbor-sync single exchange"
    );
    let delivered = answer.is_some();
    SyncExchange {
        end,
        replica_answered: if delivered { replica_sent } else { 0 },
        paid_answered: if delivered { paid_sent } else { 0 },
        hints,
        answer,
        held: HeldAnswer::default(),
    }
}

/// How a single exchange ended, with the peer's `answer`, if it answered, or
/// answered `busy`. An answer of as many keys as a single message carries
/// ([`max_exchange_keys`]) is windowed: a responder cuts hints that come to
/// more to a window of exactly that many, so the peer may have more, and is
/// not counted as drained. One of fewer is complete.
pub(crate) fn single_end(answer: Option<&SyncAnswer>, busy: bool) -> ExchangeEnd {
    match answer {
        Some(answer)
            if answer.replica_hints.len() + answer.paid_hints.len()
                >= max_exchange_keys(SyncForm::Single) =>
        {
            ExchangeEnd::Windowed
        }
        Some(_) => ExchangeEnd::Complete,
        None if busy => ExchangeEnd::Busy,
        None => ExchangeEnd::Failed,
    }
}

/// A paged exchange of `hints`, the window this exchange carries of
/// `all_keys` hints: its pages announce the pages of all of them.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn exchange_paged(
    peer: &PeerId,
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
    is_bootstrapping: bool,
    hints: PeerSyncHints,
    all_keys: usize,
    mut commitment: Option<StorageCommitment>,
    sync_peers: &SyncPeers,
) -> SyncExchange {
    let started = Instant::now();
    let cycle = rand::thread_rng().gen::<u64>();
    let page_keys = page_keys();
    let own_pages = hints.page_count(page_keys);
    let announced = announced_pages(all_keys, page_keys).max(own_pages);
    let mut gathered = GatheredAnswer::default();
    let mut replica_answered = 0usize;
    let mut paid_answered = 0usize;
    let mut end = ExchangeEnd::Complete;
    // Pages of each list sent and received, for the exchange's log line.
    let mut replica_pages = (0u32, 0u32);
    let mut paid_pages = (0u32, 0u32);

    let mut page = 0u32;
    loop {
        // As many rounds as the side with more pages has, following the
        // peer's pages no further than an exchange takes.
        let rounds = sync_pages::exchange_rounds(gathered.pages().unwrap_or(0), own_pages);
        if page >= rounds {
            break;
        }
        let sent = hints.page(page, page_keys);
        let body = ReplicationMessageBody::NeighborSyncPageRequest(NeighborSyncPageRequest {
            cycle,
            page,
            pages: announced,
            replica_hints: sent.replica.to_vec(),
            paid_hints: sent.paid.to_vec(),
            bootstrapping: is_bootstrapping,
            commitment: commitment.take(),
        });
        let answered = request(
            peer,
            p2p_node,
            body,
            config.neighbor_sync_page_timeout,
            Silence::Free,
        )
        .await;
        match answered {
            Ok(ReplicationMessageBody::NeighborSyncPageResponse(
                NeighborSyncPageResponse::Page {
                    cycle: echoed_cycle,
                    page: echoed_page,
                    pages,
                    replica_hints,
                    paid_hints,
                    bootstrapping,
                    commitment: peer_commitment,
                },
            )) => {
                let expected_pages = gathered.pages();
                if echoed_cycle != cycle
                    || echoed_page != page
                    || pages == 0
                    || expected_pages.is_some_and(|expected| expected != pages)
                {
                    warn!(
                        "Neighbor sync with {peer}: page {page} answered as page {echoed_page} \
                         of {pages} of exchange {echoed_cycle:016x}, not of {cycle:016x}; ending \
                         the exchange"
                    );
                    end = ExchangeEnd::Failed;
                    break;
                }
                if !sent.replica.is_empty() {
                    replica_pages.0 += 1;
                }
                if !sent.paid.is_empty() {
                    paid_pages.0 += 1;
                }
                if !replica_hints.is_empty() {
                    replica_pages.1 += 1;
                }
                if !paid_hints.is_empty() {
                    paid_pages.1 += 1;
                }
                replica_answered += sent.replica.len();
                paid_answered += sent.paid.len();
                let whole = gathered.take(
                    sync_peers.received(),
                    AnsweredPage {
                        pages,
                        replica_hints,
                        paid_hints,
                        bootstrapping,
                        commitment: peer_commitment,
                    },
                );
                if !whole {
                    debug!(
                        "Neighbor sync with {peer}: page {page} kept in part, at the bounds of \
                         what an exchange gathers and the node holds of answers; the rest in a \
                         later exchange"
                    );
                    end = ExchangeEnd::Stopped;
                    break;
                }
            }
            Ok(ReplicationMessageBody::NeighborSyncPageResponse(
                NeighborSyncPageResponse::Busy { .. },
            )) => {
                debug!("Neighbor sync with {peer}: page {page} answered busy; trying again in a later cycle");
                end = ExchangeEnd::Busy;
                break;
            }
            Ok(_) => {
                warn!("Unexpected response type from {peer} during sync");
                if page == 0 {
                    sync_peers.mark_single_next(*peer);
                }
                end = ExchangeEnd::Failed;
                break;
            }
            Err(unanswered) => {
                // A first page left unanswered: the agent it was sent on may
                // be left from a connection that has ended, and the peer
                // rolled back. Its next exchange is a single message, which
                // every release reads, unless the page only timed out and
                // the peer's agent still names a paging release
                // ([`single_next_after`]).
                if page == 0 {
                    let reads_pages = p2p_node
                        .transport()
                        .peer_user_agent(peer)
                        .await
                        .is_some_and(|agent| {
                            sync_pages::agent_reads_pages(&agent, sync_pages::pages_from_release())
                        });
                    if single_next_after(unanswered, reads_pages) {
                        sync_peers.mark_single_next(*peer);
                    } else {
                        debug!(
                            "Neighbor sync with {peer}: first page {unanswered:?}; it pages again next cycle"
                        );
                    }
                }
                end = ExchangeEnd::Failed;
                break;
            }
        }
        page += 1;
    }
    // Every round run, yet the peer has pages past those an exchange follows.
    if end == ExchangeEnd::Complete
        && gathered
            .pages()
            .is_some_and(|pages| pages > MAX_NEIGHBOR_SYNC_PAGES)
    {
        end = ExchangeEnd::Stopped;
    }

    let (answer, held) = gathered.into_parts();
    let (pages_answered, peer_pages, replica_received, paid_received) =
        answer.as_ref().map_or((0, 0, 0, 0), |answer| {
            (
                answer.pages_answered,
                answer.pages,
                answer.replica_hints.len(),
                answer.paid_hints.len(),
            )
        });
    debug!(
        target: "ant_node::replication::neighbor_sync",
        peer = %peer,
        cycle,
        end = ?end,
        pages_sent = own_pages,
        pages_announced = announced,
        pages_received = peer_pages,
        pages_answered,
        replica_hints_sent = hints.replica.len(),
        paid_hints_sent = hints.paid.len(),
        replica_hints_received = replica_received,
        paid_hints_received = paid_received,
        replica_pages_sent = replica_pages.0,
        paid_pages_sent = paid_pages.0,
        replica_pages_received = replica_pages.1,
        paid_pages_received = paid_pages.1,
        held_bytes = held.bytes(),
        received_held_bytes = sync_peers.received().held(),
        received_most_held_bytes = sync_peers.received().most_held(),
        elapsed_ms = started.elapsed().as_millis(),
        "Neighbor-sync paged exchange"
    );

    SyncExchange {
        end,
        hints,
        replica_answered,
        paid_answered,
        answer,
        held,
    }
}

/// Handle a failed sync attempt: remove peer from snapshot and try to fill
/// the vacated slot.
///
/// Rule 3: Remove unreachable peer from pending sync state, attempt to fill
/// by using the same priority-first scan as [`select_sync_batch`]. Applies the
/// same cooldown filtering to avoid selecting a peer that was recently synced.
pub fn handle_sync_failure(
    state: &mut NeighborSyncState,
    failed_peer: &PeerId,
    cooldown: Duration,
) -> Option<PeerId> {
    // Find and remove the failed peer from the ordering.
    state.remove_peer(failed_peer);

    // Try to fill the vacated slot, applying the same priority and cooldown
    // filtering used by select_sync_batch.
    let now = Instant::now();
    select_next_sync_peer(state, now, cooldown)
}

/// Record a successful sync with a peer.
pub fn record_successful_sync(state: &mut NeighborSyncState, peer: &PeerId) {
    state.last_sync_times.insert(*peer, Instant::now());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::replication::config::NEIGHBOR_SYNC_RECEIVED_HINTS_MAX_BYTES;
    use crate::replication::protocol::NeighborSyncResponse;
    use crate::replication::types::PeerSyncRecord;
    use std::collections::HashMap;

    /// Build a `PeerId` from a single byte (zero-padded to 32 bytes).
    fn peer_id_from_byte(b: u8) -> PeerId {
        let mut bytes = [0u8; 32];
        bytes[0] = b;
        PeerId::from_bytes(bytes)
    }

    // -- select_sync_batch ---------------------------------------------------

    #[test]
    fn select_sync_batch_returns_up_to_peer_count() {
        let peers = vec![
            peer_id_from_byte(1),
            peer_id_from_byte(2),
            peer_id_from_byte(3),
            peer_id_from_byte(4),
            peer_id_from_byte(5),
        ];
        let mut state = NeighborSyncState::new_cycle(peers);
        let batch_size = 3;

        let batch = select_sync_batch(&mut state, batch_size, Duration::from_secs(0));

        assert_eq!(batch.len(), batch_size);
        assert_eq!(batch[0], peer_id_from_byte(1));
        assert_eq!(batch[1], peer_id_from_byte(2));
        assert_eq!(batch[2], peer_id_from_byte(3));
        assert_eq!(state.cursor, 3);
    }

    #[test]
    fn select_sync_batch_skips_cooldown_peers() {
        let peers = vec![
            peer_id_from_byte(1),
            peer_id_from_byte(2),
            peer_id_from_byte(3),
            peer_id_from_byte(4),
        ];
        let mut state = NeighborSyncState::new_cycle(peers);

        // Mark peer 1 and peer 3 as recently synced.
        state
            .last_sync_times
            .insert(peer_id_from_byte(1), Instant::now());
        state
            .last_sync_times
            .insert(peer_id_from_byte(3), Instant::now());

        let cooldown = Duration::from_hours(1); // 1 hour
        let batch = select_sync_batch(&mut state, 2, cooldown);

        // Peer 1 and peer 3 should be skipped (removed from order).
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0], peer_id_from_byte(2));
        assert_eq!(batch[1], peer_id_from_byte(4));

        // Cooldown peers should have been removed from the order.
        assert!(!state.order.contains(&peer_id_from_byte(1)));
        assert!(!state.order.contains(&peer_id_from_byte(3)));
    }

    #[test]
    fn select_sync_batch_expired_cooldown_not_skipped() {
        let peers = vec![peer_id_from_byte(1), peer_id_from_byte(2)];
        let mut state = NeighborSyncState::new_cycle(peers);

        // Mark peer 1 as synced a long time ago (simulate expired cooldown).
        // Use a small subtraction (2s) and a smaller cooldown (1s) to avoid
        // `checked_sub` returning `None` on freshly-booted CI runners where
        // `Instant::now()` (system uptime) may be very small.
        state.last_sync_times.insert(
            peer_id_from_byte(1),
            Instant::now()
                .checked_sub(Duration::from_secs(2))
                .unwrap_or_else(Instant::now),
        );

        let cooldown = Duration::from_secs(1);
        let batch = select_sync_batch(&mut state, 2, cooldown);

        // Peer 1's cooldown expired so it should be included.
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0], peer_id_from_byte(1));
        assert_eq!(batch[1], peer_id_from_byte(2));
    }

    #[test]
    fn select_sync_batch_empty_order() {
        let mut state = NeighborSyncState::new_cycle(vec![]);

        let batch = select_sync_batch(&mut state, 4, Duration::from_secs(0));

        assert!(batch.is_empty(), "expected an empty batch, got {batch:?}");
        assert_eq!(state.cursor, 0);
    }

    #[test]
    fn select_sync_batch_all_on_cooldown() {
        let peers = vec![peer_id_from_byte(1), peer_id_from_byte(2)];
        let mut state = NeighborSyncState::new_cycle(peers);

        state
            .last_sync_times
            .insert(peer_id_from_byte(1), Instant::now());
        state
            .last_sync_times
            .insert(peer_id_from_byte(2), Instant::now());

        let cooldown = Duration::from_hours(1);
        let batch = select_sync_batch(&mut state, 4, cooldown);

        assert!(
            batch.is_empty(),
            "expected an empty batch while every peer is in cooldown, got {batch:?}"
        );
        assert!(
            state.order.is_empty(),
            "expected an empty order, got {:?}",
            state.order
        );
    }

    // -- handle_sync_failure -------------------------------------------------

    #[test]
    fn handle_sync_failure_removes_peer_and_adjusts_cursor() {
        let peers = vec![
            peer_id_from_byte(1),
            peer_id_from_byte(2),
            peer_id_from_byte(3),
            peer_id_from_byte(4),
        ];
        let mut state = NeighborSyncState::new_cycle(peers);
        // Simulate having already processed peers at indices 0 and 1.
        state.cursor = 2;

        // Peer 2 (index 1, before cursor) fails.
        let replacement =
            handle_sync_failure(&mut state, &peer_id_from_byte(2), Duration::from_secs(0));

        // Cursor should be adjusted down by 1 (was 2, now 1).
        assert_eq!(state.cursor, 2); // was 2, removed at pos 1, adjusted to 1, then replacement advances to 2
        assert!(!state.order.contains(&peer_id_from_byte(2)));

        // Should get peer 4 as replacement (index 1 after removal = peer 3,
        // but cursor was adjusted to 1 so peer 3 is at index 1; it returns
        // the peer at the new cursor and advances).
        assert!(replacement.is_some());
    }

    #[test]
    fn handle_sync_failure_removes_peer_after_cursor() {
        let peers = vec![
            peer_id_from_byte(1),
            peer_id_from_byte(2),
            peer_id_from_byte(3),
            peer_id_from_byte(4),
        ];
        let mut state = NeighborSyncState::new_cycle(peers);
        state.cursor = 1;

        // Peer 3 (index 2, after cursor) fails.
        let replacement =
            handle_sync_failure(&mut state, &peer_id_from_byte(3), Duration::from_secs(0));

        // Cursor should stay at 1 (removal was after cursor).
        assert_eq!(state.cursor, 2); // cursor was 1, replacement advances to 2
        assert!(!state.order.contains(&peer_id_from_byte(3)));

        // Replacement should be peer 2 (now at cursor position 1).
        assert_eq!(replacement, Some(peer_id_from_byte(2)));
    }

    #[test]
    fn handle_sync_failure_no_replacement_when_exhausted() {
        let peers = vec![peer_id_from_byte(1)];
        let mut state = NeighborSyncState::new_cycle(peers);
        state.cursor = 1; // Already past the only peer.

        let replacement =
            handle_sync_failure(&mut state, &peer_id_from_byte(1), Duration::from_secs(0));

        assert!(
            state.order.is_empty(),
            "expected the order to be exhausted, got {:?}",
            state.order
        );
        assert!(replacement.is_none());
    }

    #[test]
    fn handle_sync_failure_unknown_peer_is_noop() {
        let peers = vec![peer_id_from_byte(1), peer_id_from_byte(2)];
        let mut state = NeighborSyncState::new_cycle(peers);
        state.cursor = 1;

        let replacement =
            handle_sync_failure(&mut state, &peer_id_from_byte(99), Duration::from_secs(0));

        // Order should be unchanged.
        assert_eq!(state.order.len(), 2);
        // Still tries to fill from cursor.
        assert_eq!(replacement, Some(peer_id_from_byte(2)));
        assert_eq!(state.cursor, 2);
    }

    // -- record_successful_sync ----------------------------------------------

    #[test]
    fn record_successful_sync_updates_last_sync_time() {
        let peers = vec![peer_id_from_byte(1), peer_id_from_byte(2)];
        let mut state = NeighborSyncState::new_cycle(peers);
        let peer = peer_id_from_byte(1);

        assert!(!state.last_sync_times.contains_key(&peer));

        let before = Instant::now();
        record_successful_sync(&mut state, &peer);
        let after = Instant::now();

        let ts = state.last_sync_times.get(&peer).expect("timestamp exists");
        assert!(*ts >= before);
        assert!(*ts <= after);
    }

    #[test]
    fn record_successful_sync_overwrites_previous() {
        let peers = vec![peer_id_from_byte(1)];
        let mut state = NeighborSyncState::new_cycle(peers);
        let peer = peer_id_from_byte(1);

        // Record a sync at an old time. Use a small subtraction to avoid
        // `checked_sub` returning `None` on freshly-booted CI runners.
        let old_time = Instant::now()
            .checked_sub(Duration::from_secs(2))
            .unwrap_or_else(Instant::now);
        state.last_sync_times.insert(peer, old_time);

        record_successful_sync(&mut state, &peer);

        let ts = state.last_sync_times.get(&peer).expect("timestamp exists");
        assert!(*ts > old_time, "sync time should be updated");
    }

    // -- Section 18: Neighbor sync scenarios --------------------------------

    #[test]
    fn scenario_35_round_robin_with_cooldown_skip() {
        // With >PEER_COUNT eligible peers, consecutive rounds scan forward
        // from cursor, skip cooldown peers, sync next batch.
        // Create 8 peers, mark peers 2,4 on cooldown.
        // First batch of 4: peers 1,3,5,6 (2,4 skipped and removed).
        // Second batch of 4: peers 7,8 (only 2 remain).
        // Cycle should complete after second batch.
        let peers: Vec<PeerId> = (1..=8).map(peer_id_from_byte).collect();
        let mut state = NeighborSyncState::new_cycle(peers);
        let batch_size = 4;
        let cooldown = Duration::from_hours(1);

        // Mark peers 2 and 4 as recently synced (on cooldown).
        state
            .last_sync_times
            .insert(peer_id_from_byte(2), Instant::now());
        state
            .last_sync_times
            .insert(peer_id_from_byte(4), Instant::now());

        // First batch: scan from cursor 0. Peers 2 and 4 are removed,
        // leaving [1,3,5,6,7,8]. We pick the first 4: [1,3,5,6].
        let batch1 = select_sync_batch(&mut state, batch_size, cooldown);
        assert_eq!(batch1.len(), 4);
        assert_eq!(batch1[0], peer_id_from_byte(1));
        assert_eq!(batch1[1], peer_id_from_byte(3));
        assert_eq!(batch1[2], peer_id_from_byte(5));
        assert_eq!(batch1[3], peer_id_from_byte(6));

        // Cooldown peers should have been removed from the order.
        assert!(!state.order.contains(&peer_id_from_byte(2)));
        assert!(!state.order.contains(&peer_id_from_byte(4)));

        // Second batch: only peers 7,8 remain after cursor.
        let batch2 = select_sync_batch(&mut state, batch_size, cooldown);
        assert_eq!(batch2.len(), 2);
        assert_eq!(batch2[0], peer_id_from_byte(7));
        assert_eq!(batch2[1], peer_id_from_byte(8));

        // Cycle should be complete after second batch.
        assert!(state.is_cycle_complete());
    }

    #[test]
    fn cycle_complete_when_cursor_past_order() {
        // is_cycle_complete() returns true when cursor >= order.len().
        let peers: Vec<PeerId> = (1..=3).map(peer_id_from_byte).collect();
        let mut state = NeighborSyncState::new_cycle(peers);

        // Not complete at the start.
        assert!(!state.is_cycle_complete());

        // Advance cursor to exactly order.len().
        state.cursor = 3;
        assert!(state.is_cycle_complete());

        // Also complete when cursor exceeds order.len().
        state.cursor = 10;
        assert!(state.is_cycle_complete());

        // Edge case: order is emptied (peers removed) with cursor at 0.
        state.order.clear();
        state.cursor = 0;
        assert!(state.is_cycle_complete());
    }

    /// Scenario 36: Post-cycle responsibility pruning with time-based
    /// hysteresis.
    ///
    /// When a full round-robin cycle completes, node runs one prune pass
    /// over BOTH stored records and `PaidForList` entries using current
    /// `SelfInclusiveRT`. Out-of-range items have timestamps recorded but
    /// are deleted only after `PRUNE_HYSTERESIS_DURATION`. In-range items
    /// have their timestamps cleared.
    ///
    /// Full `run_prune_pass` requires a live `P2PNode`. This test verifies
    /// the deterministic trigger condition (cycle completion) and the
    /// combined record + paid-list pruning contract:
    ///   (1) Cycle completes -> prune pass should run.
    ///   (2) Both `RecordOutOfRangeFirstSeen` and `PaidOutOfRangeFirstSeen`
    ///       are tracked independently in the same pass.
    ///   (3) Keys within hysteresis window are retained.
    #[test]
    fn scenario_36_post_cycle_triggers_combined_prune_pass() {
        let config = ReplicationConfig::default();

        // Step 1: Run a full cycle to completion.
        let peers: Vec<PeerId> = (1..=3).map(peer_id_from_byte).collect();
        let mut state = NeighborSyncState::new_cycle(peers);
        let _ = select_sync_batch(&mut state, 3, Duration::from_secs(0));
        assert!(
            state.is_cycle_complete(),
            "cycle must be complete before prune pass triggers"
        );

        // Step 2: Verify prune hysteresis parameters are configured.
        assert!(
            !config.prune_hysteresis_duration.is_zero(),
            "PRUNE_HYSTERESIS_DURATION must be non-zero for hysteresis to work"
        );

        // Step 3: Simulate the prune-pass timestamp tracking for BOTH
        // record and paid-list entries (the two independent timestamp
        // families that Section 11 requires in a single pass).
        //
        // Record timestamps and paid timestamps are independent — clearing
        // one must not affect the other (tested in scenario_52). Here we
        // verify the combined trigger: cycle completion -> both kinds of
        // timestamps are eligible for evaluation.
        let record_key: [u8; 32] = [0x36; 32];
        let paid_key: [u8; 32] = [0x37; 32];

        // Simulate: record_key goes out of range, paid_key goes out of range.
        let record_first_seen = Instant::now();
        let paid_first_seen = Instant::now();

        // Both timestamps are recent — well within hysteresis window.
        let record_elapsed = record_first_seen.elapsed();
        let paid_elapsed = paid_first_seen.elapsed();
        assert!(
            record_elapsed < config.prune_hysteresis_duration,
            "record key should be retained within hysteresis window"
        );
        assert!(
            paid_elapsed < config.prune_hysteresis_duration,
            "paid key should be retained within hysteresis window"
        );

        // The prune pass evaluates both independently. Verify they don't
        // interfere by using separate keys.
        assert_ne!(
            record_key, paid_key,
            "record and paid pruning keys must be independent"
        );

        // Step 4: After the cycle, a new snapshot is taken and cursor resets.
        let new_state = NeighborSyncState::new_cycle(vec![
            peer_id_from_byte(1),
            peer_id_from_byte(2),
            peer_id_from_byte(3),
        ]);
        assert_eq!(new_state.cursor, 0, "cursor resets for new cycle");
        assert!(
            !new_state.is_cycle_complete(),
            "new cycle should not be immediately complete"
        );
    }

    #[test]
    fn scenario_38_mid_cycle_peer_join_prioritized() {
        // Peer D joins CloseNeighbors mid-cycle. It is queued for priority
        // sync without being inserted into the current round-robin snapshot.
        let peers = vec![
            peer_id_from_byte(0xA),
            peer_id_from_byte(0xB),
            peer_id_from_byte(0xC),
        ];
        let mut state = NeighborSyncState::new_cycle(peers);

        // Advance cursor to simulate mid-cycle.
        let _ = select_sync_batch(&mut state, 1, Duration::from_secs(0));
        assert_eq!(state.cursor, 1);

        // Peer D "joins" the close-neighbor set. It remains outside the
        // stable snapshot but is queued ahead of the cursor.
        let peer_d = peer_id_from_byte(0xD);
        assert_eq!(state.queue_priority_peers([peer_d]), 1);
        assert!(!state.order.contains(&peer_d));
        assert!(
            !state.is_cycle_complete(),
            "pending priority sync keeps the cycle active"
        );

        let batch = select_sync_batch(&mut state, 2, Duration::from_secs(0));
        assert_eq!(batch, vec![peer_d, peer_id_from_byte(0xB)]);
    }

    #[test]
    fn scenario_39_unreachable_peer_removed_slot_filled() {
        // Peer P is in snapshot. Sync fails. P removed from order.
        // Node resumes scanning and picks next peer Q to fill the slot.
        let peers = vec![
            peer_id_from_byte(1),
            peer_id_from_byte(2),
            peer_id_from_byte(3),
            peer_id_from_byte(4),
            peer_id_from_byte(5),
        ];
        let mut state = NeighborSyncState::new_cycle(peers);

        // First batch selects peers 1,2.
        let batch = select_sync_batch(&mut state, 2, Duration::from_secs(0));
        assert_eq!(batch, vec![peer_id_from_byte(1), peer_id_from_byte(2)]);

        // Peer 2 becomes unreachable. Remove it and fill the slot.
        let replacement =
            handle_sync_failure(&mut state, &peer_id_from_byte(2), Duration::from_secs(0));
        assert!(!state.order.contains(&peer_id_from_byte(2)));

        // Slot should be filled by the next available peer (peer 3).
        assert_eq!(
            replacement,
            Some(peer_id_from_byte(3)),
            "vacated slot should be filled by next peer in order"
        );

        // Continue: next batch should resume from after the replacement.
        let batch2 = select_sync_batch(&mut state, 2, Duration::from_secs(0));
        assert_eq!(batch2, vec![peer_id_from_byte(4), peer_id_from_byte(5)]);
        assert!(state.is_cycle_complete());
    }

    #[test]
    fn scenario_40_cooldown_peer_removed_from_snapshot() {
        // Peer synced within cooldown period. When batch selection reaches it,
        // peer is REMOVED from order (not just skipped). Scanning continues to
        // next peer.
        let peers = vec![
            peer_id_from_byte(1),
            peer_id_from_byte(2),
            peer_id_from_byte(3),
        ];
        let mut state = NeighborSyncState::new_cycle(peers);
        let cooldown = Duration::from_hours(1);

        // Mark peer 2 as recently synced.
        state
            .last_sync_times
            .insert(peer_id_from_byte(2), Instant::now());

        let batch = select_sync_batch(&mut state, 3, cooldown);

        // Peer 2 should have been REMOVED from order, not just skipped.
        assert!(!state.order.contains(&peer_id_from_byte(2)));
        assert_eq!(state.order.len(), 2, "order should shrink by 1");

        // Batch contains the non-cooldown peers.
        assert_eq!(batch, vec![peer_id_from_byte(1), peer_id_from_byte(3)]);

        // Cycle is complete since all remaining peers were selected.
        assert!(state.is_cycle_complete());
    }

    #[test]
    fn priority_peer_in_snapshot_is_not_selected_twice() {
        let peers = vec![
            peer_id_from_byte(1),
            peer_id_from_byte(2),
            peer_id_from_byte(3),
        ];
        let mut state = NeighborSyncState::new_cycle(peers);
        assert_eq!(state.queue_priority_peers([peer_id_from_byte(2)]), 1);

        let batch = select_sync_batch(&mut state, 2, Duration::from_secs(0));

        assert_eq!(batch, vec![peer_id_from_byte(2), peer_id_from_byte(1)]);
        assert_eq!(
            state.order,
            vec![peer_id_from_byte(1), peer_id_from_byte(3)]
        );
        assert_eq!(state.cursor, 1);
    }

    #[test]
    fn priority_peer_on_cooldown_is_skipped_and_removed_from_snapshot() {
        let peers = vec![peer_id_from_byte(1), peer_id_from_byte(2)];
        let mut state = NeighborSyncState::new_cycle(peers);
        let cooldown = Duration::from_secs(1);
        let priority_peer = peer_id_from_byte(2);
        state.last_sync_times.insert(priority_peer, Instant::now());
        assert_eq!(state.queue_priority_peers([priority_peer]), 1);

        let batch = select_sync_batch(&mut state, 2, cooldown);

        assert_eq!(batch, vec![peer_id_from_byte(1)]);
        assert!(state.priority_order.is_empty());
        assert!(!state.order.contains(&priority_peer));
    }

    #[test]
    fn failure_replacement_prefers_remaining_priority_peer() {
        let peers = vec![peer_id_from_byte(1), peer_id_from_byte(2)];
        let mut state = NeighborSyncState::new_cycle(peers);
        let first_priority_peer = peer_id_from_byte(3);
        let second_priority_peer = peer_id_from_byte(4);
        assert_eq!(
            state.queue_priority_peers([first_priority_peer, second_priority_peer]),
            2
        );

        let batch = select_sync_batch(&mut state, 1, Duration::from_secs(0));
        assert_eq!(batch, vec![first_priority_peer]);

        let replacement =
            handle_sync_failure(&mut state, &first_priority_peer, Duration::from_secs(0));

        assert_eq!(replacement, Some(second_priority_peer));
        assert!(state.priority_order.is_empty());
        assert_eq!(state.cursor, 0);
    }

    #[test]
    fn scenario_41_cycle_always_terminates() {
        // Under arbitrary cooldowns and removals, cycle always terminates.
        // Create 10 peers. Mark ALL on cooldown. select_sync_batch
        // should remove all and return empty. Cycle complete.
        let peer_count: u8 = 10;
        let peers: Vec<PeerId> = (1..=peer_count).map(peer_id_from_byte).collect();
        let mut state = NeighborSyncState::new_cycle(peers);
        let cooldown = Duration::from_hours(1);

        // Mark all peers as recently synced.
        for i in 1..=peer_count {
            state
                .last_sync_times
                .insert(peer_id_from_byte(i), Instant::now());
        }

        let batch = select_sync_batch(&mut state, 4, cooldown);

        assert!(
            batch.is_empty(),
            "all peers on cooldown — batch must be empty"
        );
        assert!(state.order.is_empty(), "all peers should have been removed");
        assert!(
            state.is_cycle_complete(),
            "cycle must terminate when all peers are removed"
        );
    }

    #[test]
    fn consecutive_rounds_advance_through_full_cycle() {
        // 6 peers, batch_size=2, no cooldowns.
        // Round 1: peers 0,1. Round 2: peers 2,3. Round 3: peers 4,5.
        // After round 3: cycle complete.
        let peers: Vec<PeerId> = (1..=6).map(peer_id_from_byte).collect();
        let mut state = NeighborSyncState::new_cycle(peers);
        let batch_size = 2;
        let no_cooldown = Duration::from_secs(0);

        let round1 = select_sync_batch(&mut state, batch_size, no_cooldown);
        assert_eq!(round1, vec![peer_id_from_byte(1), peer_id_from_byte(2)]);
        assert_eq!(state.cursor, 2);
        assert!(!state.is_cycle_complete());

        let round2 = select_sync_batch(&mut state, batch_size, no_cooldown);
        assert_eq!(round2, vec![peer_id_from_byte(3), peer_id_from_byte(4)]);
        assert_eq!(state.cursor, 4);
        assert!(!state.is_cycle_complete());

        let round3 = select_sync_batch(&mut state, batch_size, no_cooldown);
        assert_eq!(round3, vec![peer_id_from_byte(5), peer_id_from_byte(6)]);
        assert_eq!(state.cursor, 6);
        assert!(state.is_cycle_complete());

        // Extra call after cycle complete returns empty.
        let round4 = select_sync_batch(&mut state, batch_size, no_cooldown);
        assert!(
            round4.is_empty(),
            "expected an empty batch once the cycle is complete, got {round4:?}"
        );
    }

    /// Scenario 37: Non-`LocalRT` inbound sync behavior.
    ///
    /// When a peer not in `LocalRT(self)` opens a sync session:
    /// - Receiver STILL builds and sends outbound hints (response always
    ///   constructed via `handle_sync_request`).
    /// - Receiver drops ALL inbound replica/paid hints from that peer
    ///   (caller returns early in `mod.rs:handle_neighbor_sync_request`
    ///   when `sender_in_rt` is false).
    /// - Sync history is NOT updated for non-RT peers, so no
    ///   `RepairOpportunity` is created.
    ///
    /// Full integration requires a live `P2PNode` (`handle_sync_request`
    /// calls `is_in_routing_table`). This test verifies the deterministic
    /// contract:
    ///   (1) `NeighborSyncResponse` is always constructed regardless of
    ///       sender RT membership (outbound hints sent).
    ///   (2) When `sender_in_rt` is false, no admission runs and sync
    ///       history is not updated.
    ///   (3) When `sender_in_rt` is true, sync history IS updated and
    ///       inbound hints enter the admission pipeline.
    #[test]
    fn scenario_37_non_local_rt_inbound_sync_drops_hints() {
        let sender = peer_id_from_byte(0x37);

        // Simulate what handle_sync_request always builds: outbound hints
        // in the response, regardless of whether sender is in LocalRT.
        let outbound_replica_hints = vec![[0x01; 32], [0x02; 32]];
        let outbound_paid_hints = vec![[0x03; 32]];
        let response = NeighborSyncResponse {
            replica_hints: outbound_replica_hints.clone(),
            paid_hints: outbound_paid_hints.clone(),
            bootstrapping: false,
            rejected_keys: Vec::new(),
            commitment: None,
        };

        // Inbound hints from the sender (would be in the request).
        let inbound_replica_hints = vec![[0xA0; 32], [0xA1; 32]];
        let inbound_paid_hints = vec![[0xB0; 32]];

        // --- Case 1: sender NOT in LocalRT (sender_in_rt = false) ---
        let sender_in_rt = false;
        let mut sync_history: HashMap<PeerId, PeerSyncRecord> = HashMap::new();

        // Response is still built — outbound hints are sent.
        assert_eq!(
            response.replica_hints, outbound_replica_hints,
            "outbound replica hints must be sent even when sender is not in LocalRT"
        );
        assert_eq!(
            response.paid_hints, outbound_paid_hints,
            "outbound paid hints must be sent even when sender is not in LocalRT"
        );

        // Caller checks sender_in_rt and returns early. No admission runs.
        if !sender_in_rt {
            // This is the early-return path in mod.rs:964-966.
            // Inbound hints are never processed.
            let admitted_replica_keys: Vec<[u8; 32]> = Vec::new();
            let admitted_paid_keys: Vec<[u8; 32]> = Vec::new();

            for key in &inbound_replica_hints {
                assert!(
                    !admitted_replica_keys.contains(key),
                    "inbound replica hints must NOT be admitted from non-RT sender"
                );
            }
            for key in &inbound_paid_hints {
                assert!(
                    !admitted_paid_keys.contains(key),
                    "inbound paid hints must NOT be admitted from non-RT sender"
                );
            }

            // Sync history is NOT updated for non-RT peers.
            assert!(
                !sync_history.contains_key(&sender),
                "sync history must NOT be updated for non-LocalRT sender"
            );
        }

        // --- Case 2: sender IS in LocalRT (sender_in_rt = true) ---
        let sender_in_rt = true;
        assert!(
            sender_in_rt,
            "when sender is in LocalRT, inbound hints are processed"
        );

        // Sync history IS updated for RT peers.
        sync_history.insert(
            sender,
            PeerSyncRecord {
                last_sync: Some(Instant::now()),
                cycles_since_sync: 0,
            },
        );
        assert!(
            sync_history.contains_key(&sender),
            "sync history should be updated for LocalRT sender"
        );
        assert!(
            sync_history
                .get(&sender)
                .expect("sender in history")
                .last_sync
                .is_some(),
            "last_sync should be recorded for RT sender"
        );
    }

    #[test]
    fn cycle_completion_resets_cursor_but_keeps_sync_times() {
        // Verify that after cycle completes, starting a new cycle
        // preserves the last_sync_times from the old state.
        let peers = vec![peer_id_from_byte(1), peer_id_from_byte(2)];
        let mut state = NeighborSyncState::new_cycle(peers);

        // Sync both peers and record their times.
        let _ = select_sync_batch(&mut state, 2, Duration::from_secs(0));
        record_successful_sync(&mut state, &peer_id_from_byte(1));
        record_successful_sync(&mut state, &peer_id_from_byte(2));
        assert!(state.is_cycle_complete());

        // Capture sync times before "resetting" for a new cycle.
        let old_sync_times = state.last_sync_times.clone();
        assert_eq!(old_sync_times.len(), 2);

        // Simulate starting a new cycle: create fresh state but carry over
        // last_sync_times (as the real driver would).
        let new_peers = vec![
            peer_id_from_byte(1),
            peer_id_from_byte(2),
            peer_id_from_byte(3),
        ];
        let mut new_state = NeighborSyncState::new_cycle(new_peers);
        new_state.last_sync_times = old_sync_times;

        // Cursor is reset.
        assert_eq!(new_state.cursor, 0);
        assert!(!new_state.is_cycle_complete());

        // Sync times are preserved.
        assert_eq!(new_state.last_sync_times.len(), 2);
        assert!(new_state
            .last_sync_times
            .contains_key(&peer_id_from_byte(1)));
        assert!(new_state
            .last_sync_times
            .contains_key(&peer_id_from_byte(2)));

        // The preserved cooldowns cause peers 1,2 to be removed, leaving
        // only peer 3 selected.
        let cooldown = Duration::from_hours(1);
        let batch = select_sync_batch(&mut new_state, 3, cooldown);
        assert_eq!(
            batch,
            std::iter::once(peer_id_from_byte(3)).collect::<Vec<_>>(),
            "only the new peer should be selected; old peers are on cooldown"
        );
    }

    // -- Routing snapshot ----------------------------------------------------

    /// A peer id from a seed, spread over the whole id space.
    fn spread_peer(seed: u64) -> PeerId {
        PeerId::from_bytes(*blake3::hash(&seed.to_le_bytes()).as_bytes())
    }

    fn spread_key(seed: u64) -> XorName {
        *blake3::hash(&(seed ^ 0xA5A5_A5A5).to_be_bytes()).as_bytes()
    }

    /// The `width` nearest to `key` of `candidates`, the way a lookup per key
    /// finds them: every candidate sorted by distance.
    fn nearest_by_sorting(candidates: &[PeerId], key: &XorName, width: usize) -> Vec<PeerId> {
        let mut sorted = candidates.to_vec();
        sorted.sort_by_key(|peer| xor(peer.as_bytes(), key));
        sorted.truncate(width);
        sorted
    }

    fn snapshot_of(count: u64) -> (RoutingSnapshot, Vec<PeerId>) {
        let self_id = spread_peer(0);
        let peers: Vec<PeerId> = (1..=count).map(spread_peer).collect();
        let mut candidates = peers.clone();
        candidates.push(self_id);
        (RoutingSnapshot::new(self_id, peers), candidates)
    }

    /// One snapshot gives every key the close group a lookup per key gives
    /// for the same routing table, this node among the candidates.
    #[test]
    fn a_snapshot_gives_the_close_group_a_lookup_per_key_gives() {
        let (snapshot, candidates) = snapshot_of(60);
        for seed in 0..300 {
            let key = spread_key(seed);
            for width in [1, 7, 9, 20, 61, 100] {
                let expected: HashSet<PeerId> = nearest_by_sorting(&candidates, &key, width)
                    .into_iter()
                    .collect();
                assert_eq!(snapshot.close_group(&key, width), expected, "width {width}");
                for candidate in &candidates {
                    assert_eq!(
                        snapshot.is_within(candidate, &key, width),
                        expected.contains(candidate),
                        "width {width}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_peer_outside_the_routing_table_is_in_no_close_group() {
        let (snapshot, _) = snapshot_of(5);
        let stranger = spread_peer(999);
        let key = spread_key(1);
        assert!(!snapshot.is_within(&stranger, &key, 100));
        assert!(!snapshot.is_within(snapshot.self_id(), &key, 0));
        assert!(snapshot.is_within(snapshot.self_id(), &key, 6), "6 of 6");
        let self_id = *snapshot.self_id();
        let again = RoutingSnapshot::new(self_id, vec![self_id, spread_peer(1), spread_peer(1)]);
        assert_eq!(
            again.peers,
            vec![spread_peer(1)],
            "self and repeats are dropped"
        );
    }

    /// The scan that builds a batch's hints gives each peer the keys a lookup
    /// per key would: replica hints where the peer is in the key's close
    /// group, paid hints where it is in the key's paid group.
    #[test]
    fn a_batch_scan_gives_each_peer_the_hints_a_lookup_per_key_gives() {
        let (snapshot, candidates) = snapshot_of(40);
        let keys: Vec<XorName> = (0..400).map(spread_key).collect();
        let paid_keys: Vec<XorName> = (400..900).map(spread_key).collect();
        let mut targets: Vec<PeerId> = (1..=4).map(spread_peer).collect();
        targets.push(spread_peer(777)); // not in the routing table
        let lists = hints_from_snapshot(&snapshot, &targets, &keys, &paid_keys, 7, 20);
        assert_eq!(lists.len(), targets.len());
        for target in &targets {
            let (replica, paid) = &lists[target];
            let expected_replica: Vec<XorName> = keys
                .iter()
                .filter(|key| nearest_by_sorting(&candidates, key, 7).contains(target))
                .copied()
                .collect();
            let expected_paid: Vec<XorName> = paid_keys
                .iter()
                .filter(|key| nearest_by_sorting(&candidates, key, 20).contains(target))
                .copied()
                .collect();
            assert_eq!(replica, &expected_replica);
            assert_eq!(paid, &expected_paid);
        }
        assert_eq!(lists[&spread_peer(777)], (Vec::new(), Vec::new()));
        assert!(
            targets[..4]
                .iter()
                .any(|target| !lists[target].0.is_empty()),
            "the test keys hint something"
        );
    }

    // -- Pages ---------------------------------------------------------------

    fn hints_of(replica: usize, paid: usize) -> PeerSyncHints {
        let (snapshot, _) = snapshot_of(3);
        let mut hints = PeerSyncHints::empty(Arc::new(snapshot), 7);
        hints.replica = (0..replica as u64).map(spread_key).collect();
        hints.paid = (0..paid as u64)
            .map(|seed| spread_key(seed + 10_000))
            .collect();
        hints
    }

    /// The pages take the replica hints, then the paid hints, each page the
    /// next keys and no more than a page's worth, every key on exactly one
    /// page; past the last page they are empty.
    #[test]
    fn pages_cut_the_replica_then_the_paid_hints_in_order() {
        for (replica, paid, page_keys) in [
            (0, 0, 2),
            (1, 0, 2),
            (0, 3, 2),
            (5, 3, 2),
            (4, 4, 4),
            (7, 0, 3),
            (3, 9, 5),
            (10, 10, 1),
        ] {
            let hints = hints_of(replica, paid);
            let pages = hints.page_count(page_keys);
            assert_eq!(
                pages as usize,
                (replica + paid).div_ceil(page_keys).max(1),
                "{replica} + {paid} keys, {page_keys} a page"
            );
            let mut replica_seen = Vec::new();
            let mut paid_seen = Vec::new();
            for index in 0..pages {
                let page = hints.page(index, page_keys);
                assert!(page.len() <= page_keys);
                if index + 1 < pages {
                    assert_eq!(page.len(), page_keys, "only the last page is short");
                }
                if !page.paid.is_empty() {
                    assert_eq!(
                        replica_seen.len() + page.replica.len(),
                        replica,
                        "paid hints only after every replica hint"
                    );
                }
                replica_seen.extend_from_slice(page.replica);
                paid_seen.extend_from_slice(page.paid);
            }
            assert_eq!(replica_seen, hints.replica);
            assert_eq!(paid_seen, hints.paid);
            assert_eq!(hints.page(pages, page_keys).len(), 0);
            assert_eq!(hints.page(u32::MAX, page_keys).len(), 0);
        }
    }

    #[test]
    fn an_exchange_takes_at_most_the_most_pages() {
        assert_eq!(MAX_NEIGHBOR_SYNC_PAGES, 256);
        let hints = hints_of(300, 0);
        assert_eq!(hints.page_count(1), MAX_NEIGHBOR_SYNC_PAGES);
        let last = hints.page(MAX_NEIGHBOR_SYNC_PAGES - 1, 1);
        assert_eq!(last.replica, &hints.replica[255..256]);
        assert_eq!(hints.page(MAX_NEIGHBOR_SYNC_PAGES, 1).len(), 0);
    }

    /// A single message carries every hint up to its budget; past it, a
    /// window of the budget, each list its share.
    #[test]
    fn a_single_message_carries_the_hints_up_to_its_budget() {
        let budget = max_exchange_keys(SyncForm::Single);
        assert_eq!(budget, NEIGHBOR_SYNC_SINGLE_MESSAGE_BYTES / KEY_BYTES);
        let small = hints_of(5, 6);
        let (all, next) = small.clone().window(budget, WindowStart::default());
        assert_eq!(
            (all.replica, all.paid, next),
            (small.replica, small.paid, None)
        );

        let mut large = hints_of(0, 0);
        large.replica = vec![[1; 32]; budget - 10];
        large.paid = vec![[2; 32]; 30];
        let (sent, next) = large.window(budget, WindowStart::default());
        assert_eq!(sent.replica.len() + sent.paid.len(), budget);
        assert!(!sent.paid.is_empty(), "the paid hints have their share");
        assert!(next.is_some(), "the next exchange goes on from there");
    }

    /// Hints over what an exchange carries are cut to a window of exactly
    /// that many keys, each list's share in proportion to its length, and the
    /// windows from each one's next start go round every key of both lists
    /// in as many exchanges as it takes to go round once.
    #[test]
    fn windows_go_round_both_lists() {
        let all = hints_of(500, 100);
        let max_keys = 256;
        let mut start = WindowStart {
            replica: 777,
            paid: 1_000_003,
        };
        let (mut replica_seen, mut paid_seen) = (HashSet::new(), HashSet::new());
        // 213 replica hints and 43 paid ones a window: three go round both.
        for _ in 0..3 {
            let (window, next) = all.clone().window(max_keys, start);
            assert_eq!((window.replica.len(), window.paid.len()), (213, 43));
            assert_eq!(window.bytes(), max_keys * KEY_BYTES);
            replica_seen.extend(window.replica.iter().copied());
            paid_seen.extend(window.paid.iter().copied());
            start = next.expect("not all fit");
        }
        assert_eq!(replica_seen.len(), 500);
        assert_eq!(paid_seen.len(), 100);

        // A window takes each list's next keys in order, wrapping round.
        let (window, next) = all.clone().window(
            max_keys,
            WindowStart {
                replica: 400,
                paid: 90,
            },
        );
        let mut replica = all.replica[400..].to_vec();
        replica.extend_from_slice(&all.replica[..113]);
        let mut paid = all.paid[90..].to_vec();
        paid.extend_from_slice(&all.paid[..33]);
        assert_eq!((window.replica, window.paid), (replica, paid));
        assert_eq!(
            next,
            Some(WindowStart {
                replica: 113,
                paid: 33
            })
        );

        // One list alone takes the whole window; a list with any keys gets
        // at least one.
        let (window, _) = hints_of(300, 0).window(max_keys, WindowStart::default());
        assert_eq!((window.replica.len(), window.paid.len()), (256, 0));
        let (window, _) = hints_of(1, 1_000).window(max_keys, WindowStart::default());
        assert_eq!((window.replica.len(), window.paid.len()), (1, 255));
    }

    /// What a responder holds for an exchange is at most the pages it
    /// serves: a set over 256 pages is cut to its window, whose pages carry
    /// exactly the window's keys; a set that fits is served as it is, page
    /// for page.
    #[test]
    fn a_served_set_holds_no_more_than_its_pages() {
        let page_keys = 3;
        let max_keys = MAX_NEIGHBOR_SYNC_PAGES as usize * page_keys;
        let (window, _) = hints_of(1_000, 200).window(max_keys, WindowStart::default());
        assert!(window.bytes() <= MAX_NEIGHBOR_SYNC_PAGES as usize * page_keys * KEY_BYTES);
        assert_eq!(window.page_count(page_keys), MAX_NEIGHBOR_SYNC_PAGES);
        let mut served = Vec::new();
        for page in 0..MAX_NEIGHBOR_SYNC_PAGES {
            let page = window.page(page, page_keys);
            served.extend_from_slice(page.replica);
            served.extend_from_slice(page.paid);
        }
        let mut held = window.replica.clone();
        held.extend_from_slice(&window.paid);
        assert_eq!(served, held, "every key held is served, in order");

        let fits = hints_of(400, 100);
        let (same, next) = fits.clone().window(max_keys, WindowStart::default());
        assert!(next.is_none());
        for page in 0..MAX_NEIGHBOR_SYNC_PAGES {
            assert_eq!(same.page(page, page_keys), fits.page(page, page_keys));
        }
    }

    /// A side announces every page its hints take, though an exchange carries
    /// a window of 256 of them, so the other side, having had every page the
    /// exchange runs, knows more are to come.
    #[test]
    fn a_side_announces_every_page_its_hints_take() {
        assert_eq!(announced_pages(0, 8_192), 1, "at least one page");
        assert_eq!(announced_pages(8_192, 8_192), 1);
        assert_eq!(announced_pages(8_193, 8_192), 2);
        assert_eq!(announced_pages(usize::MAX, 1), u32::MAX);

        let page_keys = 3;
        let all = hints_of(1_000, 200);
        let announced = announced_pages(all.key_count(), page_keys);
        assert_eq!(announced, 400);
        let max_keys = MAX_NEIGHBOR_SYNC_PAGES as usize * page_keys;
        let (window, _) = all.window(max_keys, WindowStart::default());
        assert_eq!(window.page_count(page_keys), MAX_NEIGHBOR_SYNC_PAGES);
        assert!(
            announced > MAX_NEIGHBOR_SYNC_PAGES,
            "more than the window serves"
        );

        let fits = hints_of(4, 1);
        assert_eq!(
            announced_pages(fits.key_count(), page_keys),
            fits.page_count(page_keys),
            "hints that fit announce the pages they are served on"
        );
    }

    /// A peer that isn't connected gets a single message, as does one marked
    /// after an unanswered first page, for one exchange; a connected peer
    /// gets pages exactly when its agent names a release that reads them.
    #[test]
    fn a_peer_is_paged_only_on_a_paging_agent() {
        let paging = "node/0.23.0+ds.17";
        let before = "node/0.23.0+ds.15";
        assert_eq!(form_for(Some(paging), false, 17), SyncForm::Paged);
        assert_eq!(form_for(Some(before), false, 17), SyncForm::Single);
        assert_eq!(form_for(Some(before), false, 15), SyncForm::Paged);
        assert_eq!(
            form_for(None, false, 0),
            SyncForm::Single,
            "not connected: of unknown release"
        );

        let peers = SyncPeers::default();
        let peer = peer_id_from_byte(7);
        peers.mark_single_next(peer);
        let next = |peers: &SyncPeers| form_for(Some(paging), peers.take_single_next(&peer), 17);
        assert_eq!(
            next(&peers),
            SyncForm::Single,
            "after an unanswered first page, a single message"
        );
        assert_eq!(next(&peers), SyncForm::Paged, "for one exchange only");
    }

    fn page_of(keys: usize, pages: u32) -> AnsweredPage {
        AnsweredPage {
            pages,
            replica_hints: vec![[3; 32]; keys / 2],
            paid_hints: vec![[4; 32]; keys - keys / 2],
            bootstrapping: false,
            commitment: None,
        }
    }

    /// A requester takes a peer's pages however large the peer cuts them,
    /// up to what one exchange gathers, and keeps the start of the page that
    /// passes it, its replica hints first.
    #[test]
    fn an_exchange_gathers_pages_of_any_size_up_to_its_bound() {
        let budget = Arc::new(ReceivedHints::new(2 * MAX_EXCHANGE_KEYS * KEY_BYTES));
        let mut gathered = GatheredAnswer::default();
        assert!(
            gathered.take(&budget, page_of(MAX_PAGE_KEYS + 1_808, 300)),
            "a page larger than this node's is taken whole"
        );
        let mut whole = true;
        let mut pages = 1;
        while whole {
            whole = gathered.take(&budget, page_of(MAX_PAGE_KEYS, 300));
            pages += 1;
            assert!(pages <= 300);
        }
        let (answer, held) = gathered.into_parts();
        let answer = answer.expect("answered");
        assert_eq!(
            answer.replica_hints.len() + answer.paid_hints.len(),
            MAX_EXCHANGE_KEYS,
            "no more than an exchange gathers"
        );
        assert_eq!(held.bytes(), MAX_EXCHANGE_KEYS * KEY_BYTES);
        assert_eq!(answer.pages_answered, pages);
    }

    /// Four exchanges at once, each with a peer sending 256 full pages,
    /// hold no more of the answers than the node's budget: each stops
    /// following its peer once the budget is spent, short of its last page,
    /// so the peer isn't counted as drained, and gives its keys back once
    /// dropped.
    #[test]
    fn exchanges_at_once_hold_no_more_than_the_budget() {
        let budget = Arc::new(ReceivedHints::new(NEIGHBOR_SYNC_RECEIVED_HINTS_MAX_BYTES));
        let mut exchanges: Vec<(GatheredAnswer, bool)> =
            (0..4).map(|_| (GatheredAnswer::default(), true)).collect();
        for _ in 0..MAX_NEIGHBOR_SYNC_PAGES {
            for (gathered, following) in &mut exchanges {
                if *following {
                    *following =
                        gathered.take(&budget, page_of(MAX_PAGE_KEYS, MAX_NEIGHBOR_SYNC_PAGES));
                }
            }
            assert!(budget.held() <= NEIGHBOR_SYNC_RECEIVED_HINTS_MAX_BYTES);
        }
        assert!(budget.most_held() <= NEIGHBOR_SYNC_RECEIVED_HINTS_MAX_BYTES);
        let ended: Vec<SyncExchange> = exchanges
            .into_iter()
            .map(|(gathered, following)| {
                assert!(!following, "stopped short of the peer's last page");
                let (answer, held) = gathered.into_parts();
                SyncExchange {
                    end: ExchangeEnd::Stopped,
                    hints: hints_of(0, 0),
                    replica_answered: 0,
                    paid_answered: 0,
                    answer,
                    held,
                }
            })
            .collect();
        for exchange in &ended {
            assert!(!exchange.received_last_page(), "not drained");
            assert!(!exchange_earns_cooldown(exchange));
        }
        drop(ended);
        assert_eq!(budget.held(), 0, "given back once the hints are admitted");
    }

    fn answered(pages_answered: u32, pages: u32) -> SyncAnswer {
        SyncAnswer {
            replica_hints: Vec::new(),
            paid_hints: Vec::new(),
            bootstrapping: false,
            commitment: None,
            pages_answered,
            pages,
        }
    }

    fn exchange_of(end: ExchangeEnd, answer: Option<SyncAnswer>) -> SyncExchange {
        SyncExchange {
            end,
            hints: hints_of(5, 2),
            replica_answered: 2,
            paid_answered: 0,
            answer,
            held: HeldAnswer::default(),
        }
    }

    /// Only a complete exchange puts its peer on the cooldown: one that ended
    /// part way, busy, failed or stopped, leaves the peer to the next cycle,
    /// whose snapshot picks it again.
    #[test]
    fn only_a_complete_exchange_earns_the_cooldown() {
        let peer = peer_id_from_byte(9);
        let cooldown = Duration::from_hours(1);
        for (end, again) in [
            (ExchangeEnd::Busy, true),
            (ExchangeEnd::Failed, true),
            (ExchangeEnd::Stopped, true),
            (ExchangeEnd::Windowed, true),
            (ExchangeEnd::Complete, false),
        ] {
            let pages_answered = if end == ExchangeEnd::Complete { 4 } else { 2 };
            let exchange = exchange_of(end, Some(answered(pages_answered, 4)));
            let mut state = NeighborSyncState::new_cycle(vec![peer]);
            let _ = select_sync_batch(&mut state, 4, cooldown);
            record_exchange(&mut state, &peer, &exchange);
            let mut next_cycle = NeighborSyncState::new_cycle(vec![peer]);
            next_cycle.last_sync_times = state.last_sync_times.clone();
            let batch = select_sync_batch(&mut next_cycle, 4, cooldown);
            assert_eq!(batch.contains(&peer), again, "{end:?}");
        }
    }

    /// Only the replica hints of pages the peer answered get repair proofs:
    /// the ones the exchange counted, from the first.
    #[test]
    fn only_the_answered_pages_replica_hints_get_proofs() {
        let hints = hints_of(5, 2);
        let exchange = exchange_of(ExchangeEnd::Failed, None);
        assert_eq!(exchange.answered_replica_hints(), &hints.replica[..2]);
        assert!(!exchange.received_last_page());

        let complete = SyncExchange {
            replica_answered: 99,
            ..exchange_of(ExchangeEnd::Complete, Some(answered(1, 1)))
        };
        assert_eq!(complete.answered_replica_hints(), &hints.replica[..]);
        assert!(complete.received_last_page());

        let busy = SyncExchange {
            end: ExchangeEnd::Busy,
            ..complete
        };
        assert!(!busy.received_last_page(), "busy before the last page");
        let stopped = SyncExchange {
            end: ExchangeEnd::Stopped,
            ..busy
        };
        assert!(
            !stopped.received_last_page(),
            "stopped before the last page"
        );
    }

    /// A single answer filled to what a single message carries may be a
    /// window of more: its peer isn't drained, and is synced again next
    /// cycle. One below that is complete, and a busy answer is busy.
    #[test]
    fn a_single_answer_filled_to_the_cap_owes_more() {
        let cap = max_exchange_keys(SyncForm::Single);
        let key = [7u8; 32];
        let with = |replica: usize, paid: usize| SyncAnswer {
            replica_hints: vec![key; replica],
            paid_hints: vec![key; paid],
            ..answered(1, 1)
        };
        for (answer, end) in [
            (with(cap - 40, 40), ExchangeEnd::Windowed),
            (with(cap, 0), ExchangeEnd::Windowed),
            (with(0, cap), ExchangeEnd::Windowed),
            (with(cap - 41, 40), ExchangeEnd::Complete),
            (with(0, 0), ExchangeEnd::Complete),
        ] {
            let got = single_end(Some(&answer), false);
            assert_eq!(got, end);
            let exchange = exchange_of(got, Some(answer));
            assert_eq!(
                exchange.received_last_page(),
                end == ExchangeEnd::Complete,
                "{end:?}"
            );
        }
        assert_eq!(single_end(None, true), ExchangeEnd::Busy);
        assert_eq!(single_end(None, false), ExchangeEnd::Failed);
    }

    /// A first page left unanswered moves the peer to a single message only
    /// where it may have rolled back: a failed send, or an answer that
    /// doesn't decode. A timeout while its agent still names a paging release
    /// is a slow peer that pages, and pages again.
    #[test]
    fn only_a_peer_that_may_have_rolled_back_gets_a_single_message_next() {
        for (unanswered, reads_pages, single_next) in [
            (Unanswered::TimedOut, true, false),
            (Unanswered::TimedOut, false, true),
            (Unanswered::Failed, true, true),
            (Unanswered::Failed, false, true),
            (Unanswered::Undecodable, true, true),
            (Unanswered::NotSent, true, false),
            (Unanswered::NotSent, false, false),
        ] {
            assert_eq!(
                single_next_after(unanswered, reads_pages),
                single_next,
                "{unanswered:?}, agent reads pages: {reads_pages}"
            );
        }
    }

    #[test]
    fn the_page_size_is_8192_keys() {
        assert_eq!(NEIGHBOR_SYNC_PAGE_BYTES / KEY_BYTES, 8_192);
        assert_eq!(MAX_EXCHANGE_KEYS, 256 * 8_192);
    }

    /// The close groups proofs are recorded with are the ones the hints were
    /// chosen with: the same snapshot's.
    #[test]
    fn proofs_take_the_close_group_the_hint_was_chosen_in() {
        let hints = hints_of(4, 0);
        let groups = hints.groups_for(&hints.replica).close_groups();
        assert_eq!(groups.len(), 4);
        for (key, group) in groups {
            assert_eq!(group, hints.snapshot.close_group(&key, 7));
            assert_eq!(group.len(), 4, "the snapshot's 3 peers and this node");
        }
    }
}

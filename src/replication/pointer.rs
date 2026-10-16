//! Pointer replication (ADR-0016).
//!
//! Pointers ride the same replication machinery chunks do — the same close
//! groups, the same neighbour-sync rounds and churn triggers, the same quorum,
//! pruning and possession rules — but not the chunk pipeline itself. That
//! pipeline assumes a record never changes and that its key is the hash of its
//! bytes. A pointer's address is stable while its state changes, and two honest
//! replicas may hold different valid signatures over one state. So a pointer is
//! replicated by *state*:
//!
//! - **Fresh.** A node that accepts a paid state from a client forwards the
//!   record, with the proof that paid for it, to the rest of the close group.
//!   Each receiver checks the signature, its own responsibility and the payment
//!   itself before storing it.
//! - **Repair.** Every neighbour-sync round pushes hints: the states the sender
//!   holds that the receiver should hold. A receiver that lacks a hinted state,
//!   or holds an older one, asks the close group which state each holds, adopts
//!   the best state a quorum of them hold exactly, and fetches it from one of
//!   them. The record verifies itself; the quorum stands in for the payment
//!   proof, as presence quorum does for a chunk.
//! - **Pruning.** A record this node has been out of range of is kept, served
//!   and hinted while the disk has room. Under disk pressure (see
//!   [`super::retention`]), a record out of range for the hysteresis period
//!   and farther from the node than the erase floor may be erased, farthest
//!   first, each only once all but one of the current close group prove they
//!   hold that state or a newer one by returning a valid record, however far
//!   the node is from the group.
//! - **Possession.** Some minutes after offering a fresh state, the node asks
//!   each close-group member for the record. A member that is still responsible
//!   and cannot produce that state or a newer one is penalised.
//!
//! Requests go only to peers that have sent a pointer message themselves (see
//! [`PointerReplication::is_capable`]). A peer built before pointers cannot
//! decode them, and saorsa-core counts an unanswered request against the peer
//! it was sent to; asking only capable peers keeps older peers out of it. Every
//! neighbour-sync round pushes hints, empty or not, so capability is learned
//! within a cycle.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ant_protocol::pointer::{Pointer, PointerState, POINTER_WIRE_LEN};
use futures::stream::{self, StreamExt};
use parking_lot::Mutex;
use rand::Rng;
use saorsa_core::identity::PeerId;
use saorsa_core::{P2PNode, TrustEvent};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::ant_protocol::XorName;
use crate::logging::{debug, info, warn};
use crate::payment::{
    PaymentVerifier, VerificationContext, MAX_PAYMENT_PROOF_SIZE_BYTES,
    MIN_PAYMENT_PROOF_SIZE_BYTES,
};
use crate::pointer::store::{Inspected, PointerStore};
use crate::replication::admission;
use crate::replication::commitment_state::ResponderCommitmentState;
use crate::replication::config::{
    storage_admission_width, ReplicationConfig, FRESH_REPLICATION_DELIVERY_MAX_RETRIES,
    REPLICATION_PROTOCOL_ID,
};
use crate::replication::protocol::{
    PointerFetchRequest, PointerFetchResponse, PointerFreshOffer, PointerHints,
    PointerStateRequest, PointerStateResponse, PointerStateSummary, ReplicationMessage,
    ReplicationMessageBody, MAX_POINTER_HINTS_PER_MESSAGE, MAX_POINTER_STATE_REQUEST_ADDRESSES,
};
use crate::replication::pruning::prune_proofs_needed;
use crate::replication::retention::{self, DiskRetention};
use crate::storage::{CapacityVerdict, ChunkStore};

use super::REPLICATION_TRUST_WEIGHT;

/// Inbound fresh offers verified at once. An offer costs a signature check and
/// an on-chain payment lookup, so a flood is dropped rather than queued; one
/// that is dropped is repaired by the next neighbour-sync round.
const MAX_CONCURRENT_OFFERS: usize = 16;

/// Fetch and state requests served at once.
const MAX_CONCURRENT_SERVES: usize = 32;

/// How often the pending hints are looked at.
const VERIFICATION_TICK: Duration = Duration::from_millis(500);

/// Most addresses awaiting verification at once, so a hint flood cannot grow
/// memory without bound. A hint dropped here comes back next round.
const MAX_PENDING: usize = 65_536;

/// Most addresses verified per tick.
const MAX_VERIFICATIONS_PER_TICK: usize = 256;

/// Verifications and fetches in flight at once within a tick.
const VERIFICATION_CONCURRENCY: usize = 8;

/// Undecided rounds an address gets before it is dropped. It comes back with
/// the next hint.
const MAX_UNDECIDED_ROUNDS: u32 = 5;

/// Wait before retrying an undecided address, doubled each round.
const UNDECIDED_BACKOFF: Duration = Duration::from_secs(30);

/// Most records a pass asks the close group about before erasing, farthest
/// from the node first.
const MAX_PRUNE_CANDIDATES_PER_PASS: usize = 256;

/// One peer's answer about one address: the state it holds there, if any.
type StateAnswer = ((PeerId, XorName), Option<PointerState>);

/// What one pointer prune pass found and did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PointerPruneResult {
    /// Records held.
    pub total: usize,
    /// Records this node is within the retention width of.
    pub in_range: usize,
    /// Records first seen out of range this pass.
    pub newly_marked: usize,
    /// Out-of-range records still inside the hysteresis period.
    pub hysteresis_pending: usize,
    /// Records out of range for the whole hysteresis period.
    pub candidates: usize,
    /// Candidates a retained storage commitment still holds.
    pub held_by_commitment: usize,
    /// Whether the node was under disk pressure for this pass, the only state
    /// in which it erases records.
    pub disk_pressure: bool,
    /// Candidates strictly beyond the erase floor.
    pub beyond_floor: usize,
    /// Records erased this pass.
    pub pruned: usize,
}

/// Everything that must hold, at the moment of erasure, for a pointer record
/// to be erased.
// Independent conditions of one decision, each read from a different place;
// a state machine would hide which of them failed.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PointerEraseCheck {
    /// The node is under disk pressure.
    pub(crate) under_disk_pressure: bool,
    /// The address is strictly beyond the erase floor.
    pub(crate) beyond_floor: bool,
    /// All but one of the current close group proved they hold this state or
    /// a newer one.
    pub(crate) others_hold: bool,
    /// A retained storage commitment still holds the address.
    pub(crate) committed: bool,
    /// The node is within the retention width of the address again.
    pub(crate) responsible: bool,
    /// How long the address has been continuously out of range, if it is.
    pub(crate) out_of_range_for: Option<Duration>,
    /// The out-of-range wait.
    pub(crate) hysteresis: Duration,
}

/// Whether a pointer record may be erased: every condition of
/// [`PointerEraseCheck`] must hold. Nothing stands in for the close group's
/// proof, however far the node is from the address.
pub(crate) fn pointer_may_be_erased(check: &PointerEraseCheck) -> bool {
    check.under_disk_pressure
        && check.beyond_floor
        && check.others_hold
        && !check.committed
        && !check.responsible
        && check
            .out_of_range_for
            .is_some_and(|waited| waited >= check.hysteresis)
}

/// A pointer state this node accepted from a paying client, to be offered to
/// the rest of its close group.
pub struct PointerFreshWrite {
    /// The record, in its canonical encoding.
    pub record: Vec<u8>,
    /// The proof that paid for its state.
    pub payment_proof: Vec<u8>,
}

/// A state this node was told about and has not yet verified.
#[derive(Debug, Clone)]
struct Pending {
    /// The best state hinted so far.
    wanted: PointerState,
    /// Undecided rounds so far.
    attempts: u32,
    /// Not to be looked at again before this.
    not_before: Instant,
}

/// What a verification round decided for one address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// A quorum of the close group hold this state and it beats what is held
    /// here: fetch it from one of `holders`.
    Adopt {
        /// The state to fetch.
        state: PointerState,
        /// The peers that said they hold exactly it.
        holders: Vec<PeerId>,
    },
    /// Nothing reached quorum, but the peers that did not answer could still
    /// make one. Ask again later.
    Undecided,
    /// No state that beats what is held can reach quorum.
    Refused,
}

/// Decide what a round of answers supports.
///
/// `group_size` is the whole close group this node would ask, capable or not:
/// a peer that cannot be asked counts as unanswered, not as a vote. The quorum
/// is therefore the one a chunk would need, and a network where few peers
/// understand pointers yet cannot repair on the word of those few.
pub(crate) fn evaluate(
    held: Option<&PointerState>,
    group_size: usize,
    answers: &[(PeerId, Option<PointerState>)],
    quorum_needed: usize,
) -> Verdict {
    if group_size == 0 || quorum_needed == 0 {
        return Verdict::Undecided;
    }
    let mut by_state: Vec<(PointerState, Vec<PeerId>)> = Vec::new();
    for (peer, answer) in answers {
        let Some(state) = answer else { continue };
        match by_state
            .iter_mut()
            .find(|(known, _)| known.state_id == state.state_id)
        {
            Some((_, holders)) => holders.push(*peer),
            None => by_state.push((*state, vec![*peer])),
        }
    }
    let beats_held = |state: &PointerState| held.is_none_or(|held| state.replaces(held));

    let best = by_state
        .iter()
        .filter(|(state, holders)| holders.len() >= quorum_needed && beats_held(state))
        .fold(
            None::<&(PointerState, Vec<PeerId>)>,
            |best, candidate| match best {
                Some(current) if !candidate.0.replaces(&current.0) => Some(current),
                _ => Some(candidate),
            },
        );
    if let Some((state, holders)) = best {
        return Verdict::Adopt {
            state: *state,
            holders: holders.clone(),
        };
    }

    let unanswered = group_size.saturating_sub(answers.len());
    let largest = by_state
        .iter()
        .filter(|(state, _)| beats_held(state))
        .map(|(_, holders)| holders.len())
        .max()
        .unwrap_or(0);
    if largest.saturating_add(unanswered) >= quorum_needed {
        Verdict::Undecided
    } else {
        Verdict::Refused
    }
}

/// Pointer replication for one node. See the module documentation.
pub struct PointerReplication {
    store: PointerStore,
    chunks: Arc<ChunkStore>,
    p2p: Arc<P2PNode>,
    payments: Arc<PaymentVerifier>,
    config: Arc<ReplicationConfig>,
    is_bootstrapping: Arc<RwLock<bool>>,
    /// Outbound record transfers, shared with chunk replication.
    send_semaphore: Arc<Semaphore>,
    offer_permits: Arc<Semaphore>,
    serve_permits: Arc<Semaphore>,
    /// Peers that have sent a pointer message: the only ones asked anything.
    capable: Mutex<HashSet<PeerId>>,
    /// Hinted states awaiting verification, by address.
    pending: Mutex<HashMap<XorName, Pending>>,
    /// When each held address was first seen continuously out of range.
    out_of_range: Mutex<HashMap<XorName, Instant>>,
    shutdown: CancellationToken,
    tracker: TaskTracker,
}

impl PointerReplication {
    /// Pointer replication over `store`, sharing the engine's resources.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: PointerStore,
        chunks: Arc<ChunkStore>,
        p2p: Arc<P2PNode>,
        payments: Arc<PaymentVerifier>,
        config: Arc<ReplicationConfig>,
        is_bootstrapping: Arc<RwLock<bool>>,
        send_semaphore: Arc<Semaphore>,
        shutdown: CancellationToken,
        tracker: TaskTracker,
    ) -> Self {
        Self {
            store,
            chunks,
            p2p,
            payments,
            config,
            is_bootstrapping,
            send_semaphore,
            offer_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_OFFERS)),
            serve_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_SERVES)),
            capable: Mutex::new(HashSet::new()),
            pending: Mutex::new(HashMap::new()),
            out_of_range: Mutex::new(HashMap::new()),
            shutdown,
            tracker,
        }
    }

    /// The pointer store this replication serves from.
    pub(crate) fn store(&self) -> &PointerStore {
        &self.store
    }

    // -----------------------------------------------------------------------
    // Capability
    // -----------------------------------------------------------------------

    fn mark_capable(&self, peer: &PeerId) {
        self.capable.lock().insert(*peer);
    }

    /// Whether `peer` has sent a pointer message, and may therefore be asked.
    pub fn is_capable(&self, peer: &PeerId) -> bool {
        self.capable.lock().contains(peer)
    }

    /// Forget a peer that left the routing table.
    pub(crate) fn forget_peer(&self, peer: &PeerId) {
        self.capable.lock().remove(peer);
    }

    /// Addresses currently awaiting verification. Tests only.
    #[cfg(any(test, feature = "test-utils"))]
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.lock().len()
    }

    // -----------------------------------------------------------------------
    // Messaging
    // -----------------------------------------------------------------------

    async fn send_one_way(&self, peer: &PeerId, body: ReplicationMessageBody) -> bool {
        let msg = ReplicationMessage {
            request_id: rand::thread_rng().gen::<u64>(),
            body,
        };
        let Ok(bytes) = msg.encode() else {
            warn!("Failed to encode a pointer replication message");
            return false;
        };
        self.p2p
            .send_message(peer, REPLICATION_PROTOCOL_ID, bytes, &[])
            .await
            .is_ok()
    }

    async fn request(
        &self,
        peer: &PeerId,
        body: ReplicationMessageBody,
        timeout: Duration,
    ) -> Option<ReplicationMessageBody> {
        let msg = ReplicationMessage {
            request_id: rand::thread_rng().gen::<u64>(),
            body,
        };
        let bytes = msg.encode().ok()?;
        let response = self
            .p2p
            .send_request(peer, REPLICATION_PROTOCOL_ID, bytes, timeout)
            .await
            .ok()?;
        ReplicationMessage::decode(&response.data)
            .ok()
            .map(|msg| msg.body)
    }

    async fn penalise(&self, peer: &PeerId) {
        self.p2p
            .report_trust_event(
                peer,
                TrustEvent::ApplicationFailure(REPLICATION_TRUST_WEIGHT),
            )
            .await;
    }

    /// Ask `peer` for the record it holds at `address`, and check what comes
    /// back: a valid signature and the right address. Anything else counts
    /// against the peer.
    async fn fetch_record(&self, peer: &PeerId, address: &XorName) -> Option<Pointer> {
        let body = self
            .request(
                peer,
                ReplicationMessageBody::PointerFetchRequest(PointerFetchRequest {
                    address: *address,
                }),
                self.config.fetch_request_timeout,
            )
            .await?;
        let ReplicationMessageBody::PointerFetchResponse(response) = body else {
            return None;
        };
        if response.address != *address {
            return None;
        }
        let bytes = response.record?;
        match Pointer::from_bytes(&bytes) {
            Ok(record) if record.address() == *address => Some(record),
            Ok(_) | Err(_) => {
                debug!(
                    "Peer {peer} served an invalid pointer record for {}",
                    hex::encode(address)
                );
                self.penalise(peer).await;
                None
            }
        }
    }

    // -----------------------------------------------------------------------
    // Hints
    // -----------------------------------------------------------------------

    /// Push hints to `peers` in the background: for each, the states this
    /// node holds whose close group, in this node's view, includes that peer.
    ///
    /// Every peer gets at least one message, empty or not. That is how a peer
    /// learns this node understands pointers.
    pub(crate) fn push_hints_detached(self: &Arc<Self>, peers: Vec<PeerId>) {
        if peers.is_empty() {
            return;
        }
        let this = Arc::clone(self);
        self.tracker
            .spawn(async move { this.push_hints(&peers).await });
    }

    /// Push hints to `peers` and wait until they are sent. This is what each
    /// neighbour-sync round runs in the background; tests call it to drive a
    /// round.
    pub async fn push_hints(&self, peers: &[PeerId]) {
        let self_id = *self.p2p.peer_id();
        let mut by_peer: HashMap<PeerId, Vec<PointerStateSummary>> =
            peers.iter().map(|peer| (*peer, Vec::new())).collect();
        for state in self.store.held_states() {
            if self.shutdown.is_cancelled() {
                return;
            }
            let group = self
                .p2p
                .dht_manager()
                .find_closest_nodes_local_with_self(&state.address, self.config.close_group_size)
                .await;
            for node in group {
                if node.peer_id == self_id {
                    continue;
                }
                if let Some(hints) = by_peer.get_mut(&node.peer_id) {
                    hints.push(state.into());
                }
            }
        }
        for (peer, hints) in by_peer {
            let batches: Vec<Vec<PointerStateSummary>> = if hints.is_empty() {
                vec![Vec::new()]
            } else {
                hints
                    .chunks(MAX_POINTER_HINTS_PER_MESSAGE)
                    .map(<[PointerStateSummary]>::to_vec)
                    .collect()
            };
            for hints in batches {
                self.send_one_way(
                    &peer,
                    ReplicationMessageBody::PointerHints(PointerHints { hints }),
                )
                .await;
            }
        }
    }

    /// Take in hints from `source`: queue each hinted state this node should
    /// hold and lacks, or holds an older state than.
    pub(crate) async fn handle_hints(&self, source: PeerId, hints: Vec<PointerStateSummary>) {
        self.mark_capable(&source);
        if hints.is_empty() {
            return;
        }
        // As for chunks: only a peer in our own routing table may tell us what
        // to hold.
        if !self.p2p.dht_manager().is_in_routing_table(&source).await {
            debug!("Dropping pointer hints from {source}: not in the routing table");
            return;
        }
        let self_id = *self.p2p.peer_id();
        let width = storage_admission_width(self.config.close_group_size);
        for summary in hints.into_iter().take(MAX_POINTER_HINTS_PER_MESSAGE) {
            let hinted = PointerState::from(summary);
            match self.store.state(&hinted.address) {
                Some(held) => {
                    if held.state_id == hinted.state_id || !hinted.replaces(&held) {
                        continue;
                    }
                }
                None => {
                    if !admission::is_responsible(&self_id, &hinted.address, &self.p2p, width).await
                    {
                        continue;
                    }
                }
            }
            self.enqueue(hinted);
        }
    }

    fn enqueue(&self, hinted: PointerState) {
        let mut pending = self.pending.lock();
        let len = pending.len();
        match pending.get_mut(&hinted.address) {
            Some(entry) => {
                if hinted.replaces(&entry.wanted) {
                    entry.wanted = hinted;
                    entry.attempts = 0;
                    entry.not_before = Instant::now();
                }
            }
            None if len < MAX_PENDING => {
                pending.insert(
                    hinted.address,
                    Pending {
                        wanted: hinted,
                        attempts: 0,
                        not_before: Instant::now(),
                    },
                );
            }
            None => {}
        }
    }

    // -----------------------------------------------------------------------
    // Verification and fetch
    // -----------------------------------------------------------------------

    /// Start the loop that verifies and fetches hinted states.
    pub(crate) fn start_verification_loop(self: &Arc<Self>) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = this.shutdown.cancelled() => break,
                    () = tokio::time::sleep(VERIFICATION_TICK) => this.verify_due().await,
                }
            }
        })
    }

    /// Verify and fetch whatever is due now. Also the tests' way to drive it.
    pub async fn verify_due(&self) {
        // As for chunks (ADR-0011): a node that cannot store what it would
        // fetch does not spend the network's time finding it. The hints wait,
        // and come back each round if they are dropped meanwhile.
        if self.chunks.capacity_verdict() == CapacityVerdict::Full {
            return;
        }
        let now = Instant::now();
        let due: Vec<(XorName, PointerState)> = self
            .pending
            .lock()
            .iter()
            .filter(|(_, entry)| entry.not_before <= now)
            .take(MAX_VERIFICATIONS_PER_TICK)
            .map(|(address, entry)| (*address, entry.wanted))
            .collect();
        if due.is_empty() {
            return;
        }

        let self_id = *self.p2p.peer_id();
        let mut groups: HashMap<XorName, Vec<PeerId>> = HashMap::new();
        let mut asked: HashMap<PeerId, Vec<XorName>> = HashMap::new();
        for (address, _) in &due {
            let group: Vec<PeerId> = self
                .p2p
                .dht_manager()
                .find_closest_nodes_local(address, self.config.close_group_size)
                .await
                .into_iter()
                .map(|node| node.peer_id)
                .filter(|peer| *peer != self_id)
                .collect();
            for peer in group.iter().filter(|peer| self.is_capable(peer)) {
                asked.entry(*peer).or_default().push(*address);
            }
            groups.insert(*address, group);
        }

        let answers = self.ask_states(asked).await;

        let decisions: Vec<(XorName, Verdict)> = due
            .iter()
            .map(|(address, _)| {
                let group = groups.get(address).map_or(&[][..], Vec::as_slice);
                let answered: Vec<(PeerId, Option<PointerState>)> = group
                    .iter()
                    .filter_map(|peer| {
                        answers
                            .get(&(*peer, *address))
                            .map(|answer| (*peer, *answer))
                    })
                    .collect();
                let held = self.store.state(address);
                let verdict = evaluate(
                    held.as_ref(),
                    group.len(),
                    &answered,
                    self.config.quorum_needed(group.len()),
                );
                (*address, verdict)
            })
            .collect();

        let outcomes: Vec<(XorName, bool)> = stream::iter(decisions)
            .map(|(address, verdict)| async move {
                match verdict {
                    Verdict::Adopt { state, holders } => {
                        let stored = self.fetch_and_store(address, state, &holders).await;
                        (address, stored)
                    }
                    Verdict::Undecided => (address, false),
                    Verdict::Refused => {
                        info!(
                            "No quorum backs a newer state for pointer {}",
                            hex::encode(address)
                        );
                        self.pending.lock().remove(&address);
                        (address, true)
                    }
                }
            })
            .buffer_unordered(VERIFICATION_CONCURRENCY)
            .collect()
            .await;

        let now = Instant::now();
        let mut pending = self.pending.lock();
        for (address, settled) in outcomes {
            if settled {
                pending.remove(&address);
                continue;
            }
            if let Some(entry) = pending.get_mut(&address) {
                entry.attempts = entry.attempts.saturating_add(1);
                if entry.attempts >= MAX_UNDECIDED_ROUNDS {
                    pending.remove(&address);
                } else {
                    let backoff = UNDECIDED_BACKOFF
                        .saturating_mul(2u32.saturating_pow(entry.attempts.saturating_sub(1)));
                    entry.not_before = now + backoff;
                }
            }
        }
    }

    /// Ask each peer which state it holds at the addresses listed for it.
    async fn ask_states(
        &self,
        asked: HashMap<PeerId, Vec<XorName>>,
    ) -> HashMap<(PeerId, XorName), Option<PointerState>> {
        let requests: Vec<(PeerId, Vec<XorName>)> = asked
            .into_iter()
            .flat_map(|(peer, addresses)| {
                addresses
                    .chunks(MAX_POINTER_STATE_REQUEST_ADDRESSES)
                    .map(|chunk| (peer, chunk.to_vec()))
                    .collect::<Vec<_>>()
            })
            .collect();
        let replies: Vec<Vec<StateAnswer>> = stream::iter(requests)
            .map(|(peer, addresses)| async move {
                let body = self
                    .request(
                        &peer,
                        ReplicationMessageBody::PointerStateRequest(PointerStateRequest {
                            addresses: addresses.clone(),
                        }),
                        self.config.verification_request_timeout,
                    )
                    .await;
                let Some(ReplicationMessageBody::PointerStateResponse(response)) = body else {
                    return Vec::new();
                };
                addresses
                    .iter()
                    .zip(response.states)
                    .map(|(address, summary)| {
                        // An answer about a different address is no answer.
                        let state = summary
                            .filter(|summary| summary.address == *address)
                            .map(PointerState::from);
                        ((peer, *address), state)
                    })
                    .collect()
            })
            .buffer_unordered(VERIFICATION_CONCURRENCY)
            .collect()
            .await;
        replies.into_iter().flatten().collect()
    }

    /// Fetch `wanted` from one of `holders` and store it. Whether it is now
    /// held, or nothing further can come of this round.
    async fn fetch_and_store(
        &self,
        address: XorName,
        wanted: PointerState,
        holders: &[PeerId],
    ) -> bool {
        for holder in holders {
            let Some(record) = self.fetch_record(holder, &address).await else {
                continue;
            };
            // The quorum backed this state. A holder that has moved on since
            // serves a different one, which that quorum says nothing about.
            if record.state_id() != wanted.state_id {
                continue;
            }
            match self.store_verified(record, None).await {
                Ok(_) => return true,
                Err(e) => {
                    warn!(
                        "Could not store replicated pointer {}: {e}",
                        hex::encode(address)
                    );
                    return false;
                }
            }
        }
        false
    }

    /// Store a record whose signature has been checked, if it still belongs
    /// here and beats what is held.
    async fn store_verified(
        &self,
        record: Pointer,
        width: Option<usize>,
    ) -> crate::error::Result<bool> {
        let state = record.state();
        let self_id = *self.p2p.peer_id();
        let width = width.unwrap_or_else(|| storage_admission_width(self.config.close_group_size));
        let held = self.store.state(&state.address).is_some();
        if !held && !admission::is_responsible(&self_id, &state.address, &self.p2p, width).await {
            return Ok(false);
        }
        if !self.store.admits(&state) {
            return Ok(false);
        }
        let reservation = self.chunks.reserve(POINTER_WIRE_LEN as u64)?;
        self.store.commit(record, Some(reservation)).await?;
        Ok(true)
    }

    // -----------------------------------------------------------------------
    // Serving
    // -----------------------------------------------------------------------

    /// Answer a fetch request in the background.
    pub(crate) fn serve_fetch_detached(
        self: &Arc<Self>,
        source: PeerId,
        request: PointerFetchRequest,
        request_id: u64,
        rr_message_id: Option<String>,
    ) {
        self.mark_capable(&source);
        let this = Arc::clone(self);
        self.tracker.spawn(async move {
            let Ok(_permit) = this.serve_permits.acquire().await else {
                return;
            };
            // `get` verifies the signature before serving, so a record damaged
            // on this disk is never handed on.
            let record = match this.store.get(&request.address).await {
                Ok(Some(record)) => Some(record.to_bytes()),
                Ok(None) | Err(_) => None,
            };
            super::send_replication_response(
                &source,
                &this.p2p,
                request_id,
                ReplicationMessageBody::PointerFetchResponse(PointerFetchResponse {
                    address: request.address,
                    record,
                }),
                rr_message_id.as_deref(),
            )
            .await;
        });
    }

    /// Answer a state request in the background, from the index.
    pub(crate) fn serve_state_detached(
        self: &Arc<Self>,
        source: PeerId,
        request: PointerStateRequest,
        request_id: u64,
        rr_message_id: Option<String>,
    ) {
        self.mark_capable(&source);
        let this = Arc::clone(self);
        self.tracker.spawn(async move {
            let Ok(_permit) = this.serve_permits.acquire().await else {
                return;
            };
            let states = request
                .addresses
                .iter()
                .take(MAX_POINTER_STATE_REQUEST_ADDRESSES)
                .map(|address| this.store.state(address).map(PointerStateSummary::from))
                .collect();
            super::send_replication_response(
                &source,
                &this.p2p,
                request_id,
                ReplicationMessageBody::PointerStateResponse(PointerStateResponse { states }),
                rr_message_id.as_deref(),
            )
            .await;
        });
    }

    // -----------------------------------------------------------------------
    // Fresh replication
    // -----------------------------------------------------------------------

    /// Start the loop that offers freshly paid states to their close groups.
    pub(crate) fn start_fresh_drainer(
        self: &Arc<Self>,
        mut writes: mpsc::UnboundedReceiver<PointerFreshWrite>,
    ) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = this.shutdown.cancelled() => break,
                    write = writes.recv() => match write {
                        Some(write) => this.replicate_fresh(write).await,
                        None => break,
                    },
                }
            }
        })
    }

    /// Offer a freshly paid state to the rest of its close group, and schedule
    /// the check that they took it.
    pub async fn replicate_fresh(self: &Arc<Self>, write: PointerFreshWrite) {
        let Ok(state) = PointerState::parse(&write.record) else {
            warn!("A fresh pointer write did not parse; not replicating it");
            return;
        };
        let self_id = *self.p2p.peer_id();
        let targets: Vec<PeerId> = self
            .p2p
            .dht_manager()
            .find_closest_nodes_local_with_self(&state.address, self.config.close_group_size)
            .await
            .into_iter()
            .map(|node| node.peer_id)
            .filter(|peer| *peer != self_id)
            .collect();

        let msg = ReplicationMessage {
            request_id: rand::thread_rng().gen::<u64>(),
            body: ReplicationMessageBody::PointerFreshOffer(PointerFreshOffer {
                record: write.record,
                proof_of_payment: write.payment_proof,
            }),
        };
        let Ok(encoded) = msg.encode() else {
            warn!(
                "Failed to encode a fresh pointer offer for {}",
                hex::encode(state.address)
            );
            return;
        };
        let encoded = Arc::new(encoded);
        for peer in &targets {
            let p2p = Arc::clone(&self.p2p);
            let bytes = Arc::clone(&encoded);
            let semaphore = Arc::clone(&self.send_semaphore);
            let peer = *peer;
            self.tracker.spawn(async move {
                let Ok(_permit) = semaphore.acquire().await else {
                    return;
                };
                for attempt in 0..=FRESH_REPLICATION_DELIVERY_MAX_RETRIES {
                    match p2p
                        .send_message(&peer, REPLICATION_PROTOCOL_ID, bytes.as_ref().clone(), &[])
                        .await
                    {
                        Ok(()) => break,
                        Err(e) => debug!(
                            "Fresh pointer offer to {peer} failed (attempt {}): {e}",
                            attempt + 1
                        ),
                    }
                }
            });
        }
        debug!(
            "Fresh pointer {} offered to {} peers",
            hex::encode(state.address),
            targets.len()
        );
        self.schedule_possession_check(state, targets);
    }

    /// Take a fresh offer from `source` in the background, unless too many are
    /// already being verified.
    pub(crate) fn accept_offer_detached(
        self: &Arc<Self>,
        source: PeerId,
        offer: PointerFreshOffer,
    ) {
        self.mark_capable(&source);
        let Ok(permit) = Arc::clone(&self.offer_permits).try_acquire_owned() else {
            info!("Dropping a fresh pointer offer from {source}: too many in flight");
            return;
        };
        let this = Arc::clone(self);
        self.tracker.spawn(async move {
            let _permit = permit;
            this.accept_offer(source, offer).await;
        });
    }

    /// Verify and store a fresh offer. Cheapest checks first; the signature and
    /// the on-chain payment lookup only for a state that would change something
    /// here and that this node is responsible for.
    async fn accept_offer(&self, source: PeerId, offer: PointerFreshOffer) {
        if offer.record.len() != POINTER_WIRE_LEN
            || !(MIN_PAYMENT_PROOF_SIZE_BYTES..=MAX_PAYMENT_PROOF_SIZE_BYTES)
                .contains(&offer.proof_of_payment.len())
        {
            debug!("Dropping a malformed fresh pointer offer from {source}");
            self.penalise(&source).await;
            return;
        }
        let parsed = match self.store.inspect(&offer.record).await {
            Ok(Inspected::Candidate(parsed)) => parsed,
            Ok(Inspected::Unchanged(_) | Inspected::Stale(_)) => return,
            Err(e) => {
                debug!("Dropping an unparseable fresh pointer offer from {source}: {e}");
                self.penalise(&source).await;
                return;
            }
        };
        let state = *parsed.state();
        let self_id = *self.p2p.peer_id();
        // As for a chunk: a fresh offer is taken across the wider paid width.
        if !admission::is_responsible(
            &self_id,
            &state.address,
            &self.p2p,
            self.config.paid_list_close_group_size,
        )
        .await
        {
            return;
        }
        if !self.store.admits(&state)
            || self
                .chunks
                .check_capacity_for(POINTER_WIRE_LEN as u64)
                .is_err()
        {
            return;
        }
        let record = match self.store.verify(parsed).await {
            Ok(record) => record,
            Err(e) => {
                debug!("Fresh pointer offer from {source} does not verify: {e}");
                self.penalise(&source).await;
                return;
            }
        };
        if let Err(e) = self
            .payments
            .verify_pointer_payment_in(
                &state.address,
                &state.state_id,
                &offer.proof_of_payment,
                VerificationContext::FreshReplication,
            )
            .await
        {
            info!(
                "Fresh pointer offer for {} from {source} is not paid for: {e}",
                hex::encode(state.address)
            );
            return;
        }
        match self
            .store_verified(record, Some(self.config.paid_list_close_group_size))
            .await
        {
            Ok(true) => debug!(
                "Stored fresh pointer {} from {source}",
                hex::encode(state.address)
            ),
            Ok(false) => {}
            Err(e) => warn!(
                "Could not store fresh pointer {}: {e}",
                hex::encode(state.address)
            ),
        }
    }

    // -----------------------------------------------------------------------
    // Possession
    // -----------------------------------------------------------------------

    fn schedule_possession_check(self: &Arc<Self>, fresh: PointerState, peers: Vec<PeerId>) {
        if peers.is_empty() {
            return;
        }
        let min = self.config.possession_check_delay_min;
        let max = self.config.possession_check_delay_max.max(min);
        let delay = if max > min {
            rand::thread_rng().gen_range(min..=max)
        } else {
            min
        };
        let this = Arc::clone(self);
        self.tracker.spawn(async move {
            tokio::select! {
                () = this.shutdown.cancelled() => return,
                () = tokio::time::sleep(delay) => {}
            }
            this.check_possession(fresh, &peers).await;
        });
    }

    /// Ask each peer for the record, and penalise any still responsible for it
    /// that cannot produce `fresh` or a state that replaces it.
    pub async fn check_possession(&self, fresh: PointerState, peers: &[PeerId]) {
        let self_id = *self.p2p.peer_id();
        let group: HashSet<PeerId> = self
            .p2p
            .dht_manager()
            .find_closest_nodes_local_with_self(&fresh.address, self.config.close_group_size)
            .await
            .into_iter()
            .map(|node| node.peer_id)
            .collect();
        for peer in peers {
            // A peer that has left the group owes nothing, and one that cannot
            // be asked is never judged on its silence.
            if *peer == self_id || !group.contains(peer) || !self.is_capable(peer) {
                continue;
            }
            let holds = self
                .fetch_record(peer, &fresh.address)
                .await
                .is_some_and(|record| {
                    let state = record.state();
                    state.state_id == fresh.state_id || state.replaces(&fresh)
                });
            if !holds {
                warn!(
                    "Peer {peer} does not hold pointer {} it was offered",
                    hex::encode(fresh.address)
                );
                self.penalise(peer).await;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Pruning
    // -----------------------------------------------------------------------

    /// Track the records this node is out of range of, and erase some of them
    /// when the disk needs the room. Run when a neighbour-sync cycle completes.
    ///
    /// A record out of range is kept, served and hinted while the disk has
    /// room. Under disk pressure, records out of range for the hysteresis
    /// period and strictly beyond the erase floor are erased farthest from the
    /// node first, each only once all but one of the current close group prove
    /// they hold it, until the pressure is over (see [`super::retention`]).
    ///
    /// `allow_remote` is false while bootstrapping: candidacy is still tracked,
    /// but nothing that needs other peers' proof is decided, so nothing is
    /// erased.
    ///
    /// A record a retained storage commitment still commits to is never
    /// deleted, exactly as for a chunk: an auditor pinning that commitment may
    /// still open it, and a node that deleted it would fail that audit.
    pub async fn prune_pass(
        &self,
        allow_remote: bool,
        commitment_state: Option<&ResponderCommitmentState>,
        retention: &DiskRetention,
    ) -> PointerPruneResult {
        let committed = |address: &XorName| commitment_state.is_some_and(|cs| cs.is_held(address));
        let self_id = *self.p2p.peer_id();
        let self_xor = *self_id.as_bytes();
        let retention_width = storage_admission_width(self.config.close_group_size);
        let now = Instant::now();
        let held = self.store.held_states();
        let mut result = PointerPruneResult {
            total: held.len(),
            disk_pressure: retention.refresh(),
            ..PointerPruneResult::default()
        };
        let floor = retention
            .floor_from_routing_table(&self.p2p, &self_id, self.config.close_group_size)
            .await;

        let mut candidates = Vec::new();
        for state in &held {
            if admission::is_responsible(&self_id, &state.address, &self.p2p, retention_width).await
            {
                self.out_of_range.lock().remove(&state.address);
                result.in_range += 1;
                continue;
            }
            let first_seen = {
                let mut out_of_range = self.out_of_range.lock();
                match out_of_range.entry(state.address) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => {
                        result.newly_marked += 1;
                        *entry.insert(now)
                    }
                }
            };
            if now.duration_since(first_seen) < self.config.prune_hysteresis_duration {
                result.hysteresis_pending += 1;
                continue;
            }
            result.candidates += 1;
            if retention::is_beyond_floor(&self_xor, &state.address, floor.as_ref()) {
                result.beyond_floor += 1;
            }
            if committed(&state.address) {
                result.held_by_commitment += 1;
                continue;
            }
            candidates.push(*state);
        }
        {
            let held: HashSet<XorName> = held.iter().map(|state| state.address).collect();
            self.out_of_range
                .lock()
                .retain(|address, _| held.contains(address));
        }

        // The close group is asked for proof only where an erasure could
        // follow: under disk pressure, beyond the floor, once bootstrap allows
        // asking anyone.
        let may_ask = allow_remote && !*self.is_bootstrapping.read().await;
        let ordered: Vec<PointerState> = if may_ask {
            retention::erase_order(
                result.disk_pressure,
                &self_xor,
                floor.as_ref(),
                candidates,
                |state| state.address,
            )
            .into_iter()
            .take(MAX_PRUNE_CANDIDATES_PER_PASS)
            .collect()
        } else {
            Vec::new()
        };
        let erased = retention::erase_while_under_pressure(retention, ordered, move |state| {
            self.erase_if_proven(state, commitment_state, retention)
        })
        .await;
        result.pruned = erased.into_iter().filter(|erased| *erased).count();

        info!(
            "Pointer prune pass complete: total={} in_range={} newly_marked={} \
             hysteresis_pending={} candidates={} held_by_commitment={} disk_pressure={} \
             beyond_floor={} pruned={}",
            result.total,
            result.in_range,
            result.newly_marked,
            result.hysteresis_pending,
            result.candidates,
            result.held_by_commitment,
            u8::from(result.disk_pressure),
            result.beyond_floor,
            result.pruned,
        );
        result
    }

    /// Erase one candidate if all but one of its close group prove they hold
    /// it and every check still passes just before deleting. Returns whether
    /// it was erased.
    async fn erase_if_proven(
        &self,
        state: PointerState,
        commitment_state: Option<&ResponderCommitmentState>,
        retention: &DiskRetention,
    ) -> bool {
        if self.shutdown.is_cancelled() {
            return false;
        }
        let others_hold = self.others_hold(&state).await;
        if !others_hold {
            return false;
        }
        // Re-check just before deleting: the group may have moved back, a
        // commitment may hold the record again, or the floor may have moved.
        let self_id = *self.p2p.peer_id();
        let floor = retention
            .floor_from_routing_table(&self.p2p, &self_id, self.config.close_group_size)
            .await;
        let responsible = admission::is_responsible(
            &self_id,
            &state.address,
            &self.p2p,
            storage_admission_width(self.config.close_group_size),
        )
        .await;
        let check = PointerEraseCheck {
            under_disk_pressure: retention.under_pressure(),
            beyond_floor: retention::is_beyond_floor(
                self_id.as_bytes(),
                &state.address,
                floor.as_ref(),
            ),
            others_hold,
            committed: commitment_state.is_some_and(|cs| cs.is_held(&state.address)),
            responsible,
            out_of_range_for: self
                .out_of_range
                .lock()
                .get(&state.address)
                .map(Instant::elapsed),
            hysteresis: self.config.prune_hysteresis_duration,
        };
        pointer_may_be_erased(&check) && self.delete(&state.address).await
    }

    /// Whether all but one of the current close group prove they hold `ours`
    /// or a newer state, by returning a valid record.
    async fn others_hold(&self, ours: &PointerState) -> bool {
        let group: Vec<PeerId> = self
            .p2p
            .dht_manager()
            .find_closest_nodes_local(&ours.address, self.config.close_group_size)
            .await
            .into_iter()
            .map(|node| node.peer_id)
            .collect();
        let needed = prune_proofs_needed(group.len());
        if needed == 0 {
            return false;
        }
        let mut proofs = 0usize;
        for peer in group.iter().filter(|peer| self.is_capable(peer)) {
            let proves = self
                .fetch_record(peer, &ours.address)
                .await
                .is_some_and(|record| {
                    let state = record.state();
                    state.state_id == ours.state_id || state.replaces(ours)
                });
            if proves {
                proofs += 1;
                if proofs >= needed {
                    return true;
                }
            }
        }
        false
    }

    async fn delete(&self, address: &XorName) -> bool {
        self.out_of_range.lock().remove(address);
        match self.store.delete(address).await {
            Ok(true) => {
                self.chunks.release(POINTER_WIRE_LEN as u64);
                true
            }
            Ok(false) => false,
            Err(e) => {
                warn!("Could not prune pointer {}: {e}", hex::encode(address));
                false
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use ant_protocol::pointer::{PointerTarget, PointerTargetKind};

    fn state(counter: u64, target: u8, id: u8) -> PointerState {
        PointerState {
            state_id: [id; 32],
            address: [7; 32],
            counter,
            target: PointerTarget::new(PointerTargetKind::Chunk, [target; 32]),
        }
    }

    fn peer(byte: u8) -> PeerId {
        PeerId::from_bytes([byte; 32])
    }

    fn answers(list: &[(u8, Option<PointerState>)]) -> Vec<(PeerId, Option<PointerState>)> {
        list.iter().map(|(p, s)| (peer(*p), *s)).collect()
    }

    #[test]
    fn a_state_a_quorum_holds_is_adopted_from_its_holders() {
        let newer = state(3, 1, 30);
        let got = evaluate(
            None,
            7,
            &answers(&[
                (1, Some(newer)),
                (2, Some(newer)),
                (3, Some(newer)),
                (4, Some(newer)),
                (5, None),
            ]),
            4,
        );
        match got {
            Verdict::Adopt { state, holders } => {
                assert_eq!(state.state_id, newer.state_id);
                assert_eq!(holders.len(), 4);
            }
            other => panic!("expected Adopt, got {other:?}"),
        }
    }

    #[test]
    fn a_state_below_quorum_is_never_adopted() {
        // One peer, or three, holding a state is not the network's word.
        let lone = state(9, 1, 90);
        let got = evaluate(
            None,
            7,
            &answers(&[
                (1, Some(lone)),
                (2, Some(lone)),
                (3, Some(lone)),
                (4, None),
                (5, None),
                (6, None),
                (7, None),
            ]),
            4,
        );
        assert_eq!(got, Verdict::Refused);

        // With one peer silent, three holders plus that peer could still make
        // four: undecided, not refused.
        let got = evaluate(
            None,
            7,
            &answers(&[
                (1, Some(lone)),
                (2, Some(lone)),
                (3, Some(lone)),
                (4, None),
                (5, None),
                (6, None),
            ]),
            4,
        );
        assert_eq!(got, Verdict::Undecided);
    }

    #[test]
    fn silence_is_not_a_vote_so_an_unanswered_group_stays_undecided() {
        let newer = state(3, 1, 30);
        let got = evaluate(None, 7, &answers(&[(1, Some(newer)), (2, Some(newer))]), 4);
        assert_eq!(got, Verdict::Undecided);
    }

    #[test]
    fn nothing_older_than_what_is_held_is_adopted() {
        let held = state(5, 1, 50);
        let older = state(4, 1, 40);
        let got = evaluate(
            Some(&held),
            7,
            &answers(&[
                (1, Some(older)),
                (2, Some(older)),
                (3, Some(older)),
                (4, Some(older)),
                (5, Some(older)),
            ]),
            4,
        );
        assert_eq!(got, Verdict::Refused);
    }

    #[test]
    fn of_two_states_with_quorum_the_merge_winner_is_adopted() {
        let lower = state(2, 9, 20);
        let winner = state(2, 1, 21);
        assert!(winner.replaces(&lower));
        let got = evaluate(
            None,
            8,
            &answers(&[
                (1, Some(lower)),
                (2, Some(lower)),
                (3, Some(lower)),
                (4, Some(lower)),
                (5, Some(winner)),
                (6, Some(winner)),
                (7, Some(winner)),
                (8, Some(winner)),
            ]),
            4,
        );
        match got {
            Verdict::Adopt { state, .. } => assert_eq!(state.state_id, winner.state_id),
            other => panic!("expected the winner, got {other:?}"),
        }
    }

    #[test]
    fn a_group_nobody_can_be_asked_in_is_undecided() {
        assert_eq!(evaluate(None, 0, &[], 0), Verdict::Undecided);
    }

    // -- Erasure --------------------------------------------------------------

    const WAIT: Duration = Duration::from_hours(72);

    /// A pointer every condition allows erasing.
    fn erasable() -> PointerEraseCheck {
        PointerEraseCheck {
            under_disk_pressure: true,
            beyond_floor: true,
            others_hold: true,
            committed: false,
            responsible: false,
            out_of_range_for: Some(WAIT),
            hysteresis: WAIT,
        }
    }

    fn at(address: XorName) -> PointerState {
        PointerState {
            address,
            ..state(1, 1, 1)
        }
    }

    #[test]
    fn a_pointer_is_erased_only_when_every_condition_holds() {
        assert!(pointer_may_be_erased(&erasable()));
    }

    #[test]
    fn no_pointer_is_erased_without_disk_pressure_even_with_full_proof() {
        let check = PointerEraseCheck {
            under_disk_pressure: false,
            ..erasable()
        };
        assert!(!pointer_may_be_erased(&check));
    }

    #[test]
    fn no_pointer_is_erased_under_pressure_without_the_close_groups_proof() {
        // However far the node is from the group: far beyond the floor, past
        // the wait, under pressure, the proof is still required.
        let check = PointerEraseCheck {
            others_hold: false,
            ..erasable()
        };
        assert!(!pointer_may_be_erased(&check));
    }

    #[test]
    fn no_pointer_within_the_floor_is_erased() {
        let check = PointerEraseCheck {
            beyond_floor: false,
            ..erasable()
        };
        assert!(!pointer_may_be_erased(&check));
    }

    #[test]
    fn a_committed_back_in_range_or_waiting_pointer_is_kept() {
        for check in [
            PointerEraseCheck {
                committed: true,
                ..erasable()
            },
            PointerEraseCheck {
                responsible: true,
                ..erasable()
            },
            PointerEraseCheck {
                out_of_range_for: None,
                ..erasable()
            },
            PointerEraseCheck {
                out_of_range_for: Some(WAIT.saturating_sub(Duration::from_secs(1))),
                ..erasable()
            },
        ] {
            assert!(!pointer_may_be_erased(&check), "{check:?}");
        }
    }

    #[test]
    fn pointers_are_considered_farthest_first_and_only_beyond_the_floor() {
        let node = [0u8; 32];
        let floor = [0x20u8; 32];
        let candidates = vec![
            at([0x10; 32]),
            at([0xF0; 32]),
            at([0x40; 32]),
            at([0x80; 32]),
        ];

        let ordered: Vec<XorName> =
            retention::erase_order(true, &node, Some(&floor), candidates.clone(), |s| s.address)
                .iter()
                .map(|s| s.address)
                .collect();
        assert_eq!(ordered, vec![[0xF0; 32], [0x80; 32], [0x40; 32]]);

        assert!(
            retention::erase_order(false, &node, Some(&floor), candidates.clone(), |s| s
                .address)
            .is_empty(),
            "nothing is considered without disk pressure"
        );
        assert!(
            retention::erase_order(true, &node, None, candidates, |s| s.address).is_empty(),
            "nothing is considered without a floor"
        );
    }
}

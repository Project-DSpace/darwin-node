//! Fresh replication (Section 6.1).
//!
//! When a node accepts a newly written record with valid `PoP`:
//! 1. Store locally (already done by chunk handler).
//! 2. Record the key in `PaidForList(self)` and send `PaidNotify` to every
//!    peer in `PaidCloseGroup(K)` — immediately, never behind back-pressure.
//!    Close-group peers get a holding notice (an empty proof: "I hold K"),
//!    which every release ignores; the rest get the slim proof. A storer whose
//!    own paid-list write failed sends the slim proof to all of them.
//! 3. Hold the offers up to `FRESH_OFFER_HOLD`, so the other storers'
//!    holding notices arrive first; the hold ends sooner once every
//!    close-group peer has been heard from or is marked full.
//! 4. When the hold ends, each close-group peer not heard from gets a notice
//!    with the slim proof and, unless it is marked full, an offer (the notice
//!    then follows the offer), bounded by
//!    the pending-offer permits so a write burst cannot pile up chunk-sized
//!    buffers. Every close-group peer is still checked for possession: a peer
//!    skipped from when the hold ends, a peer offered from when its offer is
//!    dispatched.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::logging::{debug, warn};
use bytes::Bytes;
use parking_lot::Mutex;
use rand::Rng;
use saorsa_core::identity::PeerId;
use saorsa_core::P2PNode;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use crate::ant_protocol::XorName;
use crate::replication::admission;
use crate::replication::config::{
    ReplicationConfig, FRESH_REPLICATION_DELIVERY_MAX_RETRIES, HELD_PROOFS_MAX_BYTES,
    REPLICATION_PROTOCOL_ID,
};
use crate::replication::fresh_metrics::{self, FreshCounter};
use crate::replication::heard_from::{HeardFrom, Served};
use crate::replication::paid_list::PaidList;
use crate::replication::possession::PossessionCheckEvent;
use crate::replication::protocol::{
    self, FreshReplicationOffer, PaidNotify, ReplicationMessage, ReplicationMessageBody,
};

/// A newly-stored chunk that needs fresh replication.
///
/// Sent from the chunk PUT handler to the replication engine via an
/// unbounded channel so that the PUT response is not blocked by
/// replication fan-out. The event deliberately carries no chunk bytes: the
/// chunk is already on disk, and the offer dispatcher reads it back only
/// once it holds a pending-offer permit, so a replication backlog queues as
/// small events rather than chunk-sized buffers.
pub struct FreshWriteEvent {
    /// Content-address of the stored chunk.
    pub key: XorName,
    /// The slim proof to forward (`crate::payment::slim`), or empty when there
    /// is none: the storer then sends holding notices only, and no offers.
    pub payment_proof: Vec<u8>,
    /// `true` when this PUT stored the chunk; `false` when the node answered
    /// "already stored", and may only send holding notices.
    pub stored: bool,
}

/// How long after a key's holding notices sent from an "already stored"
/// answer the node sends none again for that key.
pub(crate) const HELD_NOTICE_INTERVAL: Duration = Duration::from_secs(20);

/// The most keys the node remembers sending holding notices for from an
/// "already stored" answer. When full, it sends none: that only means more
/// offers to this node.
pub(crate) const HELD_NOTICE_MAX_KEYS: usize = 32_768;

/// Keys this node sent holding notices for from an "already stored" answer,
/// so repeated PUTs of a held chunk send nothing more within
/// [`HELD_NOTICE_INTERVAL`].
#[derive(Default)]
pub(crate) struct HeldNotices {
    sent: Mutex<HashMap<XorName, Instant>>,
}

impl HeldNotices {
    /// Take the key's turn to send: `true` when no notices went out for it in
    /// the interval and there is room to remember it.
    pub(crate) fn take_turn(&self, key: &XorName, now: Instant) -> bool {
        let mut sent = self.sent.lock();
        if let Some(at) = sent.get(key) {
            if now.saturating_duration_since(*at) < HELD_NOTICE_INTERVAL {
                return false;
            }
        }
        if sent.len() >= HELD_NOTICE_MAX_KEYS && !sent.contains_key(key) {
            sent.retain(|_, at| now.saturating_duration_since(*at) < HELD_NOTICE_INTERVAL);
            if sent.len() >= HELD_NOTICE_MAX_KEYS {
                return false;
            }
        }
        sent.insert(*key, now);
        true
    }
}

/// A write whose hold has ended and whose chunk offer is waiting for a
/// pending-offer permit. Carries no chunk bytes.
pub(crate) struct FreshOfferEvent {
    pub(crate) key: XorName,
    pub(crate) payment_proof: Vec<u8>,
    /// Storage read-backs attempted so far; see `MAX_FRESH_READ_ATTEMPTS`.
    pub(crate) read_attempts: u32,
    /// The close-group peers to offer the record to: those not heard from and
    /// not marked full when the hold ended.
    pub(crate) targets: Vec<PeerId>,
    /// Whether each target also gets the notice with the slim proof once its
    /// offer is sent: true unless this node's own paid-list write failed, in
    /// which case the whole paid group already got it.
    pub(crate) notify_after: bool,
}

/// Handles shared by everything that sends fresh offers, so the offer
/// dispatcher task and the direct entry point run one pipeline.
pub(crate) struct FreshOfferContext {
    pub(crate) p2p_node: Arc<P2PNode>,
    /// Limits concurrent outbound chunk transfers across the engine.
    pub(crate) send_semaphore: Arc<Semaphore>,
    /// Delayed possession checks (ADR-0003) of offered peers are scheduled
    /// here once an offer's sends are dispatched.
    pub(crate) possession_check_tx: mpsc::UnboundedSender<PossessionCheckEvent>,
    /// Offers encoded and handed to at least one per-peer send, so tests can
    /// tell a write that went out from one lost to back-pressure.
    pub(crate) dispatched: Arc<AtomicU64>,
}

/// An encoded fresh offer and the pending-offer permit it holds.
///
/// Wrapped with [`Bytes::from_owner`], so the per-peer send tasks and the
/// transport share the one buffer through reference-counted handles, and the
/// permit is released only when the last handle anywhere is dropped. That is
/// what caps the encoded offers alive at once at `MAX_PENDING_FRESH_OFFERS`.
struct EncodedOffer {
    bytes: Vec<u8>,
    _pending: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for EncodedOffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Rules 6-8: record the paid key locally and announce it to
/// `PaidCloseGroup(K)`.
///
/// This is the evidence peers need to repair the key later, so it runs the
/// moment a write is accepted and is never gated by the pending-offer permit
/// or the send semaphore. A notice carries no record: a holding notice holds
/// only the key, and a slim proof about 5.5 KB per kept quote.
///
/// `slim_proof` is the proof to forward, already slim, or empty when there is
/// none. With this node's own paid-list entry written, close-group peers get
/// holding notices and the rest of the paid group the slim proof. Without it,
/// the whole paid group gets the slim proof, as before holding notices.
pub(crate) async fn announce_paid_write(
    key: &XorName,
    slim_proof: &[u8],
    paid_list: &PaidList,
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
) -> Announced {
    // Rule 6: Node that validates PoP adds K to PaidForList(self).
    let recorded = match paid_list.insert(key).await {
        Ok(_) => true,
        Err(e) => {
            warn!("Failed to add key {} to PaidForList: {e}", hex::encode(key));
            false
        }
    };
    // Rules 7-8: PaidNotify to every member of PaidCloseGroup(K).
    let close_group = send_paid_notices(key, slim_proof, recorded, p2p_node, config).await;
    Announced {
        recorded,
        close_group,
    }
}

/// What [`announce_paid_write`] did: whether this node's own paid-list entry
/// was written, and the close group it saw.
pub(crate) struct Announced {
    pub(crate) recorded: bool,
    pub(crate) close_group: Vec<PeerId>,
}

/// After an "already stored" answer: send the close group holding notices,
/// only when this node lists the key in its paid list, passes the
/// self-closeness check a fresh PUT makes, and sent none for the key within
/// [`HELD_NOTICE_INTERVAL`]. Nothing else is sent: no proof, no offer.
pub(crate) async fn announce_held(
    key: &XorName,
    paid_list: &PaidList,
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
    held: &HeldNotices,
) {
    if !matches!(paid_list.contains(key), Ok(true)) {
        return;
    }
    let self_id = *p2p_node.peer_id();
    if !admission::is_responsible(
        &self_id,
        key,
        p2p_node,
        crate::storage::SELF_CLOSENESS_GATE_WIDTH,
    )
    .await
    {
        return;
    }
    if !held.take_turn(key, Instant::now()) {
        return;
    }
    let close_group = close_group_peers(key, p2p_node, config).await;
    send_notice(
        key,
        Vec::new(),
        close_group.iter(),
        p2p_node,
        FreshCounter::HeldAnswerNoticesSent,
    );
}

/// Rules 2-3: send fresh offers to `target_peers`, the close-group peers the
/// hold left to offer, and schedule their delayed possession check
/// (ADR-0003).
///
/// With `notify_after`, each peer also gets the notice with the slim proof,
/// once its offer has been sent. The receiver has then usually recorded the
/// key from the offer and drops the notice before admission, so a peer the
/// client's PUT missed isn't sent a notice and an offer for one key at once.
///
/// `pending_offer` is the caller's permit from the pending-offer semaphore;
/// it is held with the encoded offer until the last handle to it is dropped.
/// `data` and `proof_of_payment` are taken by value so they move into the
/// offer instead of being copied.
pub(crate) async fn send_fresh_offers(
    ctx: &FreshOfferContext,
    key: &XorName,
    data: Vec<u8>,
    proof_of_payment: Vec<u8>,
    target_peers: Vec<PeerId>,
    notify_after: bool,
    pending_offer: OwnedSemaphorePermit,
) {
    let notice_proof = notify_after.then(|| proof_of_payment.clone());
    let offer = FreshReplicationOffer {
        key: *key,
        data,
        proof_of_payment,
    };
    let request_id = rand::thread_rng().gen::<u64>();
    let offer_msg = ReplicationMessage {
        request_id,
        body: ReplicationMessageBody::FreshReplicationOffer(offer),
    };

    let Ok(encoded) = offer_msg.encode() else {
        warn!(
            "Failed to encode FreshReplicationOffer for {}",
            hex::encode(key),
        );
        return;
    };
    // One encoded copy serves every per-peer send task and every retry; the
    // transport borrows it through `Bytes` instead of taking a copy. The
    // pending-offer permit travels with the buffer.
    let encoded = Bytes::from_owner(EncodedOffer {
        bytes: encoded,
        _pending: pending_offer,
    });
    for peer in &target_peers {
        let p2p = Arc::clone(&ctx.p2p_node);
        let offer = encoded.clone();
        let peer_id = *peer;
        let sem = Arc::clone(&ctx.send_semaphore);
        let notice_proof = notice_proof.clone();
        let key = *key;
        tokio::spawn(async move {
            // Acquire a permit before sending — this caps the number of
            // concurrent outbound replication transfers across the engine.
            let _permit = sem.acquire().await;
            debug!(
                "Replication send permit acquired for peer {peer_id} ({} available)",
                sem.available_permits()
            );
            // ADR-0003: best-effort delivery. Retry the push up to
            // FRESH_REPLICATION_DELIVERY_MAX_RETRIES times on a transport
            // failure so a transient hiccup doesn't silently drop the offer.
            // Possession is judged separately by the delayed possession check.
            let mut attempt = 0u32;
            loop {
                protocol::record_sent(&offer);
                fresh_metrics::bump(FreshCounter::OffersSent);
                match p2p
                    .send_message(&peer_id, REPLICATION_PROTOCOL_ID, offer.clone(), &[])
                    .await
                {
                    Ok(()) => break,
                    Err(e) => {
                        if attempt >= FRESH_REPLICATION_DELIVERY_MAX_RETRIES {
                            debug!(
                                "Failed to send fresh offer to {peer_id} after {} attempts: {e}",
                                attempt + 1
                            );
                            break;
                        }
                        attempt += 1;
                        debug!(
                            "Retrying fresh offer to {peer_id} (attempt {}): {e}",
                            attempt + 1
                        );
                    }
                }
            }
            // The notice with the slim proof, after the offer, sent or not.
            if let Some(proof) = notice_proof {
                send_notice(
                    &key,
                    proof,
                    std::iter::once(&peer_id),
                    &p2p,
                    FreshCounter::ProofNoticesSent,
                );
            }
        });
    }

    debug!(
        "Fresh replication initiated for {} to {} peers",
        hex::encode(key),
        target_peers.len()
    );

    // Schedule the delayed possession check (ADR-0003) for the offered
    // peers. A closed receiver (engine shutting down) is ignored.
    if !target_peers.is_empty() {
        ctx.dispatched.fetch_add(1, Ordering::Relaxed);
        fresh_metrics::checks_scheduled(Served::Offered, target_peers.len() as u64);
        let _ = ctx.possession_check_tx.send(PossessionCheckEvent {
            key: *key,
            peers: target_peers
                .into_iter()
                .map(|peer| (peer, Served::Offered))
                .collect(),
        });
    }
}

/// An offer dropped before it was sent, because its record is gone or can't
/// be read: its peers still get the notice with the slim proof they would
/// have had after the offer. No possession check is scheduled for them.
pub(crate) fn notify_unoffered(p2p_node: &Arc<P2PNode>, event: &FreshOfferEvent) {
    if event.notify_after {
        send_notice(
            &event.key,
            event.payment_proof.clone(),
            event.targets.iter(),
            p2p_node,
            FreshCounter::ProofNoticesSent,
        );
    }
}

/// The close group offers go to: the `close_group_size` nearest in this
/// node's view, self included in the query and then left out.
async fn close_group_peers(
    key: &XorName,
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
) -> Vec<PeerId> {
    let self_id = *p2p_node.peer_id();
    p2p_node
        .dht_manager()
        .find_closest_nodes_local_with_self(key, config.close_group_size)
        .await
        .into_iter()
        .map(|n| n.peer_id)
        .filter(|peer| *peer != self_id)
        .collect()
}

/// Who gets which notice for a stored key, as (holding, with the slim proof):
/// holding notices to the close group when this node's own paid-list entry is
/// written, the slim proof to every other paid-group member, and nothing
/// carrying a proof when there is none.
fn notice_recipients(
    paid_group: &[PeerId],
    close_group: &[PeerId],
    recorded: bool,
    has_proof: bool,
) -> (Vec<PeerId>, Vec<PeerId>) {
    let holding: Vec<PeerId> = if recorded {
        close_group.to_vec()
    } else {
        Vec::new()
    };
    let holding_set: HashSet<&PeerId> = holding.iter().collect();
    let with_proof = if has_proof {
        paid_group
            .iter()
            .filter(|peer| !holding_set.contains(peer))
            .copied()
            .collect()
    } else {
        Vec::new()
    };
    (holding, with_proof)
}

/// Send `PaidNotify(K)` to every peer in `PaidCloseGroup(K)` (fire-and-forget):
/// holding notices and notices with the slim proof, as [`notice_recipients`]
/// splits them.
///
/// Per Invariant 16, as amended for holding notices: the sender MUST attempt
/// a notice to every member, with the slim proof, except that a close-group
/// peer gets a holding notice once this node has recorded the key itself. A
/// notice carries no record, so it is neither gated by the send semaphore nor
/// by the pending-offer permit.
async fn send_paid_notices(
    key: &XorName,
    slim_proof: &[u8],
    recorded: bool,
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
) -> Vec<PeerId> {
    let self_id = *p2p_node.peer_id();
    let paid_group: Vec<PeerId> = p2p_node
        .dht_manager()
        .find_closest_nodes_local_with_self(key, config.paid_list_close_group_size)
        .await
        .into_iter()
        .map(|n| n.peer_id)
        .filter(|peer| *peer != self_id)
        .collect();
    let close_group = close_group_peers(key, p2p_node, config).await;
    let (holding, with_proof) =
        notice_recipients(&paid_group, &close_group, recorded, !slim_proof.is_empty());
    send_notice(
        key,
        Vec::new(),
        holding.iter(),
        p2p_node,
        FreshCounter::HoldingNoticesSent,
    );
    send_notice(
        key,
        slim_proof.to_vec(),
        with_proof.iter(),
        p2p_node,
        FreshCounter::ProofNoticesSent,
    );
    close_group
}

/// Encode one `PaidNotify(K)` and send it to each peer, fire-and-forget. An
/// empty proof makes it a holding notice. `counter` counts the recipients.
fn send_notice<'a>(
    key: &XorName,
    proof_of_payment: Vec<u8>,
    peers: impl Iterator<Item = &'a PeerId>,
    p2p_node: &Arc<P2PNode>,
    counter: FreshCounter,
) {
    let mut peers = peers.peekable();
    if peers.peek().is_none() {
        return;
    }
    let notify = PaidNotify {
        key: *key,
        proof_of_payment,
    };
    let request_id = rand::thread_rng().gen::<u64>();
    let msg = ReplicationMessage {
        request_id,
        body: ReplicationMessageBody::PaidNotify(notify),
    };

    let Ok(encoded) = msg.encode() else {
        warn!("Failed to encode PaidNotify for {}", hex::encode(key));
        return;
    };
    // One buffer for every recipient; the sends only take handles.
    let encoded = Bytes::from(encoded);
    for peer in peers {
        let p2p = Arc::clone(p2p_node);
        let data = encoded.clone();
        let peer_id = *peer;
        protocol::record_sent(&data);
        fresh_metrics::bump(counter);
        tokio::spawn(async move {
            if let Err(e) = p2p
                .send_message(&peer_id, REPLICATION_PROTOCOL_ID, data, &[])
                .await
            {
                debug!("Failed to send PaidNotify to {peer_id}: {e}");
            }
        });
    }
}

// ---------------------------------------------------------------------------
// The hold
// ---------------------------------------------------------------------------

/// How often the holds are looked over for one that has ended.
pub(crate) const HOLD_POLL: Duration = Duration::from_millis(250);

/// A stored write whose offers are held. It keeps the key, the slim proof and
/// the close group it was announced to; never the record.
pub(crate) struct Hold {
    pub(crate) key: XorName,
    pub(crate) proof: Vec<u8>,
    /// Whether this node's own paid-list entry was written when the write was
    /// announced. If not, the whole paid group already got the slim proof, and
    /// the write is tried again when the hold ends.
    pub(crate) recorded: bool,
    pub(crate) close_group: Vec<PeerId>,
    pub(crate) started: Instant,
}

impl Hold {
    /// What the hold counts against [`HELD_PROOFS_MAX_BYTES`].
    fn bytes(&self) -> usize {
        self.proof.len() + self.key.len() + self.close_group.len() * 32
    }
}

/// Why a hold ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HoldEnd {
    /// Every close-group peer was heard from or marked full.
    Early,
    /// The hold time passed.
    AtHold,
    /// The held proofs reached their cap, and this was the oldest hold.
    AtCap,
}

/// The holds of one node, oldest first, capped in bytes.
#[derive(Default)]
pub(crate) struct Holds {
    inner: Mutex<HoldQueue>,
}

#[derive(Default)]
struct HoldQueue {
    holds: VecDeque<Hold>,
    bytes: usize,
}

impl Holds {
    /// Start a hold. Returns the holds the cap ends early, oldest first: they
    /// get their sends now, which only means more sends.
    pub(crate) fn start(&self, hold: Hold) -> Vec<Hold> {
        let mut queue = self.inner.lock();
        queue.bytes += hold.bytes();
        queue.holds.push_back(hold);
        let mut ended = Vec::new();
        while queue.bytes > HELD_PROOFS_MAX_BYTES {
            let Some(oldest) = queue.holds.pop_front() else {
                break;
            };
            queue.bytes = queue.bytes.saturating_sub(oldest.bytes());
            ended.push(oldest);
        }
        ended
    }

    /// Take the holds that have ended at `now`: those `hold_len` old, and
    /// those whose every close-group peer `all_skipped` says was heard from or
    /// is marked full.
    pub(crate) fn take_ended(
        &self,
        now: Instant,
        hold_len: Duration,
        all_skipped: impl Fn(&Hold) -> bool,
    ) -> Vec<(Hold, HoldEnd)> {
        let mut queue = self.inner.lock();
        let mut ended = Vec::new();
        let mut kept = VecDeque::with_capacity(queue.holds.len());
        let mut bytes = 0;
        for hold in queue.holds.drain(..) {
            if now.saturating_duration_since(hold.started) >= hold_len {
                ended.push((hold, HoldEnd::AtHold));
            } else if all_skipped(&hold) {
                ended.push((hold, HoldEnd::Early));
            } else {
                bytes += hold.bytes();
                kept.push_back(hold);
            }
        }
        queue.holds = kept;
        queue.bytes = bytes;
        ended
    }

    /// When the oldest hold reaches its time, if any hold is open.
    pub(crate) fn next_end(&self, hold_len: Duration) -> Option<Instant> {
        self.inner
            .lock()
            .holds
            .front()
            .map(|hold| hold.started + hold_len)
    }

    /// Take the holds `hold_len` old at `now`, oldest first. Holds start in
    /// arrival order, so each ends at its own time and the sends keep the
    /// spacing the writes arrived with, rather than leaving in one burst.
    pub(crate) fn take_due(&self, now: Instant, hold_len: Duration) -> Vec<Hold> {
        let mut queue = self.inner.lock();
        let mut due = Vec::new();
        while queue
            .holds
            .front()
            .is_some_and(|hold| now.saturating_duration_since(hold.started) >= hold_len)
        {
            let Some(hold) = queue.holds.pop_front() else {
                break;
            };
            queue.bytes = queue.bytes.saturating_sub(hold.bytes());
            due.push(hold);
        }
        due
    }

    /// How many holds are open.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().holds.len()
    }
}

/// Whether `peer` is marked full by its own "full" notice. Node release 1b
/// keeps those marks; this release keeps none, so no peer is marked full and
/// the second skip reason never applies.
fn is_marked_full(_peer: &PeerId) -> bool {
    false
}

/// Why each of `peers` is skipped for `key`'s offer, or `None` when it is
/// offered. The one place the heard-from table is read.
fn skip_reasons(
    heard_from: &HeardFrom,
    key: &XorName,
    peers: &[PeerId],
    now: Instant,
) -> Vec<(PeerId, Option<Served>)> {
    peers
        .iter()
        .map(|peer| {
            let reason = heard_from.offer_skip_reason(key, peer, is_marked_full(peer), now);
            (*peer, reason)
        })
        .collect()
}

/// How a hold's close group is served when it ends.
#[derive(Debug, Default, PartialEq, Eq)]
struct Serving {
    /// Skipped, with why: checked for possession when the hold ends.
    skipped: Vec<(PeerId, Served)>,
    /// Marked full and not heard from: each gets the notice with the slim
    /// proof when the hold ends.
    notified: Vec<PeerId>,
    /// Not heard from and not marked full: each is offered the record, gets
    /// the notice with the slim proof once its offer is sent, and is checked
    /// from the dispatch path.
    offered: Vec<PeerId>,
}

impl Serving {
    /// Close-group peers neither skipped nor offered. Zero by construction;
    /// counted, and pinned, so a change that leaves a peer out shows.
    fn unserved(&self, close_group: usize) -> usize {
        close_group.saturating_sub(self.skipped.len() + self.offered.len())
    }
}

/// Split a close group by its skip reasons.
fn serve(reasons: Vec<(PeerId, Option<Served>)>) -> Serving {
    let mut serving = Serving::default();
    for (peer, reason) in reasons {
        match reason {
            Some(Served::HeardFrom) => serving.skipped.push((peer, Served::HeardFrom)),
            Some(Served::MarkedFull) => {
                serving.skipped.push((peer, Served::MarkedFull));
                serving.notified.push(peer);
            }
            Some(Served::Offered) | None => serving.offered.push(peer),
        }
    }
    serving
}

/// Everything holding and ending a hold needs.
#[derive(Clone)]
pub(crate) struct HoldContext {
    pub(crate) p2p_node: Arc<P2PNode>,
    pub(crate) config: Arc<ReplicationConfig>,
    pub(crate) paid_list: Arc<PaidList>,
    pub(crate) heard_from: Arc<HeardFrom>,
    pub(crate) holds: Arc<Holds>,
    pub(crate) offer_tx: mpsc::UnboundedSender<FreshOfferEvent>,
    pub(crate) possession_check_tx: mpsc::UnboundedSender<PossessionCheckEvent>,
}

impl HoldContext {
    /// Hold a stored write's offers. Holds the cap ends early end here. A
    /// zero hold (a test's, through the test-only seam) ends at once.
    pub(crate) async fn hold(&self, key: XorName, proof: Vec<u8>, announced: Announced) {
        let hold = Hold {
            key,
            proof,
            recorded: announced.recorded,
            close_group: announced.close_group,
            started: Instant::now(),
        };
        if self.config.fresh_offer_hold().is_zero() {
            self.end_hold(hold, HoldEnd::AtHold).await;
            return;
        }
        let ended = self.holds.start(hold);
        for hold in ended {
            self.end_hold(hold, HoldEnd::AtCap).await;
        }
    }

    /// When the oldest open hold reaches its time.
    pub(crate) fn next_hold_end(&self) -> Option<Instant> {
        self.holds.next_end(self.config.fresh_offer_hold())
    }

    /// End the holds that have reached their time, each at its own time.
    pub(crate) async fn end_due_holds(&self) {
        let due = self
            .holds
            .take_due(Instant::now(), self.config.fresh_offer_hold());
        for hold in due {
            self.end_hold(hold, HoldEnd::AtHold).await;
        }
    }

    /// End every hold that has ended by now: at its time, or early once every
    /// close-group peer is heard from or marked full.
    pub(crate) async fn end_ended_holds(&self) {
        let now = Instant::now();
        let ended = self
            .holds
            .take_ended(now, self.config.fresh_offer_hold(), |hold| {
                skip_reasons(&self.heard_from, &hold.key, &hold.close_group, now)
                    .iter()
                    .all(|(_, reason)| reason.is_some())
            });
        for (hold, end) in ended {
            self.end_hold(hold, end).await;
        }
    }

    /// End one hold: each close-group peer in this node's view now that was
    /// not heard from gets a notice with the slim proof and, unless marked
    /// full, an offer, with the notice sent after it; every skipped peer's
    /// possession check is scheduled now.
    async fn end_hold(&self, hold: Hold, end: HoldEnd) {
        fresh_metrics::bump(match end {
            HoldEnd::Early => FreshCounter::HoldsEndedEarly,
            HoldEnd::AtHold => FreshCounter::HoldsEndedAtHold,
            HoldEnd::AtCap => FreshCounter::HoldsEndedAtCap,
        });
        let key = hold.key;
        if !hold.recorded {
            // The write failed when the record was stored: try it once more.
            // If it fails again, neighbour sync's paid hints bring it back.
            if let Err(e) = self.paid_list.insert(&key).await {
                warn!(
                    "Failed again to add key {} to PaidForList: {e}",
                    hex::encode(key)
                );
            }
        }
        let close_group = close_group_peers(&key, &self.p2p_node, &self.config).await;
        let serving = serve(skip_reasons(
            &self.heard_from,
            &key,
            &close_group,
            Instant::now(),
        ));
        let unserved = serving.unserved(close_group.len());
        if unserved > 0 {
            fresh_metrics::add(FreshCounter::PeersLeftUnserved, unserved as u64);
        }
        for (_, served) in &serving.skipped {
            fresh_metrics::bump(match served {
                Served::MarkedFull => FreshCounter::OffersSkippedMarkedFull,
                Served::HeardFrom | Served::Offered => FreshCounter::OffersSkippedHeardFrom,
            });
            fresh_metrics::checks_scheduled(*served, 1);
        }
        // A storer whose own write failed sent the whole paid group the slim
        // proof when it announced the write. The others sent their close group
        // holding notices, so the peers not heard from get the proof: a peer
        // marked full now, a peer offered once its offer is sent.
        if hold.recorded {
            send_notice(
                &key,
                hold.proof.clone(),
                serving.notified.iter(),
                &self.p2p_node,
                FreshCounter::ProofNoticesSent,
            );
        }
        // A closed receiver (engine shutting down) is ignored.
        if !serving.skipped.is_empty() {
            let _ = self.possession_check_tx.send(PossessionCheckEvent {
                key,
                peers: serving.skipped,
            });
        }
        if serving.offered.is_empty() {
            return;
        }
        let offered = serving.offered.len();
        if self
            .offer_tx
            .send(FreshOfferEvent {
                key,
                payment_proof: hold.proof,
                read_attempts: 0,
                targets: serving.offered,
                notify_after: hold.recorded,
            })
            .is_err()
        {
            // The engine is shutting down.
            fresh_metrics::add(FreshCounter::PeersLeftUnserved, offered as u64);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// The permit is released with the last handle to the encoded offer, not
    /// the first: a slice the transport still holds keeps the offer counted.
    #[test]
    fn the_pending_offer_permit_outlives_every_handle() {
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits)
            .try_acquire_owned()
            .expect("free permit");
        let offer = Bytes::from_owner(EncodedOffer {
            bytes: vec![7; 16],
            _pending: permit,
        });
        let held_elsewhere = offer.slice(4..8);

        drop(offer);
        assert_eq!(permits.available_permits(), 0);
        drop(held_elsewhere);
        assert_eq!(permits.available_permits(), 1);
    }

    fn peers(range: std::ops::Range<u8>) -> Vec<PeerId> {
        range.map(|i| PeerId::from_bytes([i; 32])).collect()
    }

    /// A storer whose paid-list entry is written sends its close group
    /// holding notices and only the rest of the paid group the slim proof.
    #[test]
    fn the_close_group_gets_holding_notices_and_the_rest_the_slim_proof() {
        let paid_group = peers(1..20);
        let close_group = peers(1..7);
        let (holding, with_proof) = notice_recipients(&paid_group, &close_group, true, true);
        assert_eq!(holding, close_group);
        assert_eq!(with_proof, peers(7..20));
    }

    /// A storer whose own paid-list write failed sends no holding notices:
    /// every paid-group member gets the slim proof, as before.
    #[test]
    fn a_failed_paid_list_write_sends_no_holding_notices() {
        let paid_group = peers(1..20);
        let close_group = peers(1..7);
        let (holding, with_proof) = notice_recipients(&paid_group, &close_group, false, true);
        assert!(holding.is_empty());
        assert_eq!(with_proof, paid_group);
    }

    /// With no proof to forward, nothing carrying a proof goes out: holding
    /// notices only, or nothing at all when the paid-list write failed too.
    #[test]
    fn with_no_proof_only_holding_notices_go_out() {
        let paid_group = peers(1..20);
        let close_group = peers(1..7);
        let (holding, with_proof) = notice_recipients(&paid_group, &close_group, true, false);
        assert_eq!(holding, close_group);
        assert!(with_proof.is_empty());
        let (holding, with_proof) = notice_recipients(&paid_group, &close_group, false, false);
        assert!(holding.is_empty() && with_proof.is_empty());
    }

    /// Holding notices from an "already stored" answer go out at most once per
    /// key in the interval, and a full set sends nothing more.
    #[test]
    fn held_notices_go_out_once_per_key_in_the_interval_and_never_past_the_cap() {
        let held = HeldNotices::default();
        let start = Instant::now();
        let key = [9u8; 32];
        assert!(held.take_turn(&key, start));
        assert!(!held.take_turn(&key, start + Duration::from_secs(19)));
        assert!(held.take_turn(&key, start + HELD_NOTICE_INTERVAL));

        let full = HeldNotices::default();
        for i in 0..HELD_NOTICE_MAX_KEYS {
            let mut k = [0u8; 32];
            k[..8].copy_from_slice(&(i as u64).to_be_bytes());
            assert!(full.take_turn(&k, start));
        }
        assert!(!full.take_turn(&[0xFF; 32], start + Duration::from_secs(1)));
        // Once the old turns have passed, the set makes room again.
        assert!(full.take_turn(&[0xFF; 32], start + HELD_NOTICE_INTERVAL));
    }

    fn hold_of(key: u8, proof_len: usize, started: Instant) -> Hold {
        Hold {
            key: [key; 32],
            proof: vec![1; proof_len],
            recorded: true,
            close_group: peers(1..7),
            started,
        }
    }

    /// A hold ends at the hold time, or sooner once every close-group peer is
    /// skipped; the rest stay open.
    #[test]
    fn a_hold_ends_at_the_hold_time_or_early_when_every_peer_is_skipped() {
        let holds = Holds::default();
        let start = Instant::now();
        assert!(holds.start(hold_of(1, 100, start)).is_empty());
        assert!(holds.start(hold_of(2, 100, start)).is_empty());
        let hold_len = Duration::from_secs(10);

        let ended = holds.take_ended(start + Duration::from_secs(1), hold_len, |h| {
            h.key == [2; 32]
        });
        assert_eq!(ended.len(), 1);
        assert_eq!((ended[0].0.key, ended[0].1), ([2; 32], HoldEnd::Early));
        assert_eq!(holds.len(), 1);

        assert!(holds
            .take_ended(start + Duration::from_secs(9), hold_len, |_| false)
            .is_empty());
        let ended = holds.take_ended(start + hold_len, hold_len, |_| false);
        assert_eq!(ended.len(), 1);
        assert_eq!((ended[0].0.key, ended[0].1), ([1; 32], HoldEnd::AtHold));
        assert_eq!(holds.len(), 0);
    }

    /// Each hold ends at its own time, oldest first, so the sends keep the
    /// spacing the writes arrived with; the keeper wakes at the oldest one's.
    #[test]
    fn each_hold_ends_at_its_own_time() {
        let holds = Holds::default();
        let start = Instant::now();
        let hold_len = Duration::from_secs(10);
        assert_eq!(holds.next_end(hold_len), None);
        for i in 0..3u8 {
            holds.start(hold_of(
                i,
                100,
                start + Duration::from_millis(u64::from(i) * 100),
            ));
        }
        assert_eq!(holds.next_end(hold_len), Some(start + hold_len));
        assert!(holds
            .take_due(start + Duration::from_secs(9), hold_len)
            .is_empty());

        let due = holds.take_due(start + hold_len + Duration::from_millis(150), hold_len);
        assert_eq!(due.iter().map(|h| h.key[0]).collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(
            holds.next_end(hold_len),
            Some(start + Duration::from_millis(200) + hold_len)
        );
        assert_eq!(holds.len(), 1);
    }

    /// At the cap in bytes the oldest holds end early, which only means more
    /// sends; the newest stays held.
    #[test]
    fn holds_at_their_cap_end_the_oldest_early() {
        let holds = Holds::default();
        let start = Instant::now();
        let proof = 1024 * 1024;
        let per_hold = hold_of(0, proof, start).bytes();
        let fit = HELD_PROOFS_MAX_BYTES / per_hold;
        for i in 0..fit {
            assert!(holds
                .start(hold_of(u8::try_from(i).unwrap(), proof, start))
                .is_empty());
        }
        let ended = holds.start(hold_of(0xEE, proof, start));
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].key, [0; 32]);
        assert_eq!(holds.len(), fit);
    }

    /// Every close-group peer is served when a hold ends: skipped (heard from
    /// or marked full) or offered. A peer marked full is notified when the
    /// hold ends, a peer offered after its offer.
    #[test]
    fn no_close_group_peer_is_left_unserved_when_a_hold_ends() {
        let p = peers(1..8);
        let reasons = vec![
            (p[0], Some(Served::HeardFrom)),
            (p[1], Some(Served::MarkedFull)),
            (p[2], None),
            (p[3], Some(Served::HeardFrom)),
            (p[4], None),
            (p[5], None),
        ];
        let serving = serve(reasons);
        assert_eq!(serving.unserved(6), 0);
        assert_eq!(
            serving.skipped,
            vec![
                (p[0], Served::HeardFrom),
                (p[1], Served::MarkedFull),
                (p[3], Served::HeardFrom)
            ]
        );
        assert_eq!(serving.offered, vec![p[2], p[4], p[5]]);
        assert_eq!(serving.notified, vec![p[1]]);
    }

    /// An offer is skipped only for a peer the heard-from table names for the
    /// key, or one marked full; every other close-group peer is offered, and
    /// an empty table (or a lost entry) only means more offers.
    #[test]
    fn an_offer_is_skipped_only_for_a_peer_heard_from_or_marked_full() {
        let table = HeardFrom::default();
        let now = Instant::now();
        let key = [5u8; 32];
        let p = peers(1..4);
        table.note(key, p[0], now);
        table.note([6u8; 32], p[1], now);
        let reasons = skip_reasons(&table, &key, &p, now);
        assert_eq!(
            reasons,
            vec![(p[0], Some(Served::HeardFrom)), (p[1], None), (p[2], None)]
        );
        assert_eq!(serve(reasons).offered, vec![p[1], p[2]]);
        assert_eq!(
            serve(skip_reasons(&HeardFrom::default(), &key, &p, now)).offered,
            p
        );
    }
}

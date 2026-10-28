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
//! 4. When the hold ends, every peer of the close group, as announced or as
//!    it is now, that was not heard from gets the notice with the slim proof
//!    at once, never behind back-pressure; a peer still in the close group
//!    and not marked full is then offered the record, bounded by the
//!    pending-offer permits so a write burst cannot pile up chunk-sized
//!    buffers. The close group here is the `CLOSE_GROUP_SIZE` nearest
//!    (excluding self), one more for each peer marked full among them (at
//!    most 20 in all): the nearest beyond stand in for a full peer. Every
//!    peer of the unwidened close group is still checked for possession: a
//!    peer skipped from when the hold ends, a peer offered from when its
//!    offer is dispatched; the stand-ins are offered but not checked. When
//!    the node stops, the open holds' notices are sent before it does.

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
use crate::payment::{deserialize_root_proof, detect_proof_type, ProofType};
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
use crate::storage::ChunkStore;

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
/// pending-offer permit. Carries no chunk bytes. Its targets got the notice
/// with the slim proof when the hold ended, so neither the queue nor the
/// permits delay a peer's paid-list evidence, unless the record could not be
/// read then: those notices wait in `proof_notices` for a read that works.
pub(crate) struct FreshOfferEvent {
    pub(crate) key: XorName,
    pub(crate) payment_proof: Vec<u8>,
    /// Storage read-backs attempted so far; see `MAX_FRESH_READ_ATTEMPTS`.
    pub(crate) read_attempts: u32,
    /// The close-group peers to offer the record to: those not heard from and
    /// not marked full when the hold ended.
    pub(crate) targets: Vec<PeerId>,
    /// The targets inside the unwidened close group, whose possession is
    /// checked once the offer is dispatched; the stand-ins for peers marked
    /// full are offered the record but not checked ([`possession_checked`]).
    pub(crate) checked: Vec<PeerId>,
    /// The notices with the slim proof deferred because the record was
    /// indexed but unreadable for the moment as its hold ended: sent, before
    /// any offer, once a read-back finds it ([`deferred_notices_after`]), and
    /// never if it is gone or the reads give up. Empty for a record held then.
    pub(crate) proof_notices: Vec<PeerId>,
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
    /// Root-paid offers whose possession check is pending, and the peers that
    /// refused them.
    pub(crate) root_offers: Arc<RootOfferRefusals>,
}

/// Root-paid fresh offers (proof tag `0x03`) waiting for their delayed
/// possession check, by the offer's request id, each with the peers excused
/// for refusing it.
///
/// An up-to-date client pays through the root vault only when every node it
/// was quoted by accepts root proofs: a node quotes a settlement-version-3
/// request only when it knows a root vault, and an older node or one with
/// none answers it `StorerUpdateRequired`. This node, though, offers the
/// record to its own view of the close group, which can hold a peer the
/// client never asked, and that peer may be one that can't check the proof.
/// It refuses the offer with a `FreshReplicationResponse::Rejected` carrying
/// the offer's request id, and is dropped from that key's possession check,
/// so an honest refusal is never charged as a missing chunk; neighbour sync
/// places the record with it later.
///
/// A quote doesn't record the settlement version it was asked at: a
/// version-3 quote is byte for byte a version-2 one, and signs no version. So
/// a quote in a root proof doesn't show that its peer can check a root proof;
/// an older node or one with no root vault may have given it at version 2,
/// to a client that mixes quotes of both versions or during a mixed-version
/// rollout. A node with no root vault refuses a root proof at payment
/// verification, and nowhere earlier: the checks before it (the proof's
/// size, the record's size and address, storage-admission range, free
/// space) read nothing of the proof's tag. So a refusal is excused exactly
/// when its reason is a payment-verification refusal
/// ([`is_payment_refusal`]), whether or not the proof holds a quote from the
/// peer, as payment outcomes are never charged on the receiving side either.
/// A peer that refuses for any other reason (out of range, out of space, a
/// failed write) stays in the possession check, charged as a missing chunk
/// as every refusal of a single-node offer is.
///
/// The peer of the paid quote (the quote whose hash the root proof names,
/// paid straight to its payee) is never excused, whatever its reason: a
/// node paid for a record it then can't take is charged. The reason is the
/// peer's own word, so a paid peer refusing at payment verification would
/// otherwise keep the payment and skip the check; the same holds when a
/// client paid a version-2 quote through the root vault. The quotes are not
/// signature-checked here: a client that names another peer's quote as the
/// paid one only moves that always-charged place to that peer.
///
/// Offers carry the slim proof ([`crate::payment::slim`]), which keeps only
/// the median-priced quotes, the paid one always among them, so the paid
/// quote's peer is read from the slim proof. Which unpaid quotes the slim
/// proof keeps changes nothing here: no excuse depends on them.
///
/// What stays open: any peer other than the paid one can word its refusal as
/// a payment-verification refusal and so be left out of the key's possession
/// check for that offer, whatever its real reason. That is inherent: a node
/// that can't check a root proof is told from any other only by its own
/// word, and the reason is neither signed nor checked. The peer is still
/// checked for the record at any later possession check, and neighbour
/// sync places the record with it.
///
/// Single-node offers are not tracked, and a peer that accepts a root-paid
/// offer, or doesn't answer, is checked as before. A node answers a one-way
/// offer only to refuse a root-paid one (from ds.20); ds.19 answers none, so
/// a ds.19 peer that refuses stays in the check.
///
/// An entry lives from the offer's send to its possession check, as the
/// check's own event does, so the map holds no more entries than there are
/// checks pending.
#[derive(Default)]
pub(crate) struct RootOfferRefusals {
    offers: parking_lot::Mutex<HashMap<u64, RootOffer>>,
}

/// One root-paid offer: the key, the peers it was sent to, the peer of the
/// paid quote, and those excused for refusing it.
struct RootOffer {
    key: XorName,
    targets: HashSet<PeerId>,
    paid: Option<PeerId>,
    refused: HashSet<PeerId>,
}

/// What the possession check reads of a root proof's quotes: the peer of the
/// quote it paid, the quote whose hash is the proof's paid quote hash.
/// `paid` is `None` only when no quote has that hash, a proof this node's own
/// verification refuses before it sends any offer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RootQuotes {
    pub(crate) paid: Option<PeerId>,
}

/// What a refusal of a fresh offer does to the key's possession check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// Not a refusal of a tracked root-paid offer, by a peer it was sent to,
    /// naming its key: nothing changes.
    Untracked,
    /// The peer's quote is not the paid one and it refused at payment
    /// verification, so it may be one that can't check a root proof: it is
    /// left out of the possession check.
    Excused,
    /// The peer's quote is the paid one, whatever its reason, or the peer
    /// refused for a reason other than payment verification: it stays in the
    /// possession check.
    Charged,
}

impl RootOfferRefusals {
    /// Track the root-paid offer `request_id` of `key`, sent to `targets`,
    /// whose root proof holds the quotes `proof`.
    pub(crate) fn open(
        &self,
        request_id: u64,
        key: XorName,
        targets: &[PeerId],
        proof: &RootQuotes,
    ) {
        let targets: HashSet<PeerId> = targets.iter().copied().collect();
        self.offers.lock().insert(
            request_id,
            RootOffer {
                key,
                targets,
                paid: proof.paid,
                refused: HashSet::new(),
            },
        );
    }

    /// Note that `source` refused the offer `request_id` of `key`, giving
    /// `reason`. Only a peer the offer was sent to, naming the offer's key,
    /// counts. The peer of the paid quote is charged whatever `reason` is;
    /// another is excused only when `reason` is a payment-verification
    /// refusal, whether or not the proof holds a quote from it.
    pub(crate) fn note_refusal(
        &self,
        request_id: u64,
        source: &PeerId,
        key: &XorName,
        reason: &str,
    ) -> Refusal {
        let mut offers = self.offers.lock();
        let Some(offer) = offers.get_mut(&request_id) else {
            return Refusal::Untracked;
        };
        if offer.key != *key || !offer.targets.contains(source) {
            return Refusal::Untracked;
        }
        if offer.paid == Some(*source) || !is_payment_refusal(reason) {
            return Refusal::Charged;
        }
        offer.refused.insert(*source);
        Refusal::Excused
    }

    /// Close the offer `request_id`: `peers` without those that refused it.
    /// Unknown ids leave `peers` as they are.
    #[cfg(test)]
    pub(crate) fn close(&self, request_id: u64, peers: Vec<PeerId>) -> Vec<PeerId> {
        self.close_served(request_id, peers, |peer| peer)
    }

    /// Close the offer `request_id`: the possession check's `peers`, each
    /// with how it was served, without those that refused the offer. Unknown
    /// ids leave `peers` as they are.
    pub(crate) fn close_served<T>(
        &self,
        request_id: u64,
        peers: Vec<T>,
        peer_of: impl Fn(&T) -> &PeerId,
    ) -> Vec<T> {
        let Some(offer) = self.offers.lock().remove(&request_id) else {
            return peers;
        };
        peers
            .into_iter()
            .filter(|entry| !offer.refused.contains(peer_of(entry)))
            .collect()
    }

    /// How many root-paid offers wait for their possession check.
    #[cfg(test)]
    pub(crate) fn pending(&self) -> usize {
        self.offers.lock().len()
    }
}

/// How a fresh-offer receiver's refusal reason starts when the payment
/// verifier found no payment for the proof.
pub(crate) const PAYMENT_VERIFICATION_FAILED: &str = "Payment verification failed";

/// How a fresh-offer receiver's refusal reason starts when the payment
/// verifier returned an error, as it does for a root proof at a node with no
/// root vault.
pub(crate) const PAYMENT_VERIFICATION_ERROR: &str = "Payment verification error";

/// Whether a fresh-offer refusal `reason` is a payment-verification refusal.
/// Every released node words these two refusals so.
fn is_payment_refusal(reason: &str) -> bool {
    reason.starts_with(PAYMENT_VERIFICATION_FAILED)
        || reason.starts_with(PAYMENT_VERIFICATION_ERROR)
}

/// Whether a fresh offer's proof pays through the root vault (tag `0x03`):
/// the offers whose refusal a receiver answers even when they came one-way,
/// and a sender of the root release reads ([`RootOfferRefusals`]).
pub(crate) fn is_root_paid(proof_of_payment: &[u8]) -> bool {
    match detect_proof_type(proof_of_payment) {
        Some(ProofType::Root) => true,
        Some(ProofType::SingleNode | ProofType::Merkle) | None => false,
    }
}

/// For a proof that pays through the root vault, the peer of the quote it
/// paid, which is charged whatever its refusal of the offer says. `None` for
/// any other proof, and for a root proof that doesn't decode, whose offer is
/// then checked as a single-node one is.
fn root_quotes(proof_of_payment: &[u8]) -> Option<RootQuotes> {
    match detect_proof_type(proof_of_payment) {
        Some(ProofType::Root) => deserialize_root_proof(proof_of_payment).ok().map(|proof| {
            let peer_quotes = &proof.payment.proof_of_payment.peer_quotes;
            RootQuotes {
                paid: peer_quotes
                    .iter()
                    .find(|(_, quote)| quote.hash() == proof.quote_hash)
                    .map(|(peer, _)| PeerId::from_bytes(*peer.as_bytes())),
            }
        }),
        Some(ProofType::SingleNode | ProofType::Merkle) | None => None,
    }
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
    #[cfg(any(test, feature = "test-utils"))]
    let insert = if crate::test_seams::fault_fires(crate::test_seams::Fault::FailPaidList, key) {
        Err(crate::error::Error::Storage(
            "test seam: paid-list write failed".to_string(),
        ))
    } else {
        paid_list.insert(key).await
    };
    #[cfg(not(any(test, feature = "test-utils")))]
    let insert = paid_list.insert(key).await;
    let recorded = match insert {
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
        close_group: close_group.peers,
        checked: close_group.checked,
    }
}

/// What [`announce_paid_write`] did: whether this node's own paid-list entry
/// was written, and the close group it saw.
pub(crate) struct Announced {
    pub(crate) recorded: bool,
    /// The close group, widened past the peers marked full ([`CloseGroup`]).
    pub(crate) close_group: Vec<PeerId>,
    /// Its members the possession check probes ([`CloseGroup::checked`]).
    pub(crate) checked: Vec<PeerId>,
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
    let close_group = close_group_peers(key, p2p_node, config, &marked_full(p2p_node)).await;
    send_notice(
        key,
        Vec::new(),
        close_group.peers.iter(),
        p2p_node,
        FreshCounter::HeldAnswerNoticesSent,
    );
}

/// Rules 2-3: send fresh offers to `target_peers`, the close-group peers the
/// hold left to offer, and schedule the delayed possession check (ADR-0003)
/// of `checked_peers`, those of them inside the unwidened close group.
///
/// Sends no notice: the caller sent the targets the notice with the slim
/// proof before handing their offer here, so the permits and the send
/// semaphore never delay a peer's paid-list evidence.
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
    checked_peers: Vec<PeerId>,
    pending_offer: OwnedSemaphorePermit,
) {
    // The paid quotes' issuers, for the possession check: a full peer paid for
    // the record is charged for not holding it. A slim proof keeps exactly
    // the median-priced quotes, so it names the same issuers as the full one.
    let paid_quote_issuers = crate::replication::full::paid_quote_issuers(&proof_of_payment);

    let root_quoted = root_quotes(&proof_of_payment);
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
    // Tracked before any send, so a refusal that comes back at once is noted.
    // Only an offer whose possession check is scheduled is tracked: the
    // check's event closes it.
    let root_offer = root_quoted
        .filter(|_| !target_peers.is_empty() && !checked_peers.is_empty())
        .map(|quotes| {
            ctx.root_offers
                .open(request_id, *key, &target_peers, &quotes);
            request_id
        });
    for peer in &target_peers {
        let p2p = Arc::clone(&ctx.p2p_node);
        let offer = encoded.clone();
        let peer_id = *peer;
        let sem = Arc::clone(&ctx.send_semaphore);
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
        });
    }

    debug!(
        "Fresh replication initiated for {} to {} peers",
        hex::encode(key),
        target_peers.len()
    );

    // Schedule the delayed possession check (ADR-0003) for the offered
    // peers inside the unwidened close group, the stand-ins left out. A
    // closed receiver (engine shutting down) is ignored.
    if !checked_peers.is_empty() {
        ctx.dispatched.fetch_add(1, Ordering::Relaxed);
        fresh_metrics::checks_scheduled(Served::Offered, checked_peers.len() as u64);
        let sent = ctx.possession_check_tx.send(PossessionCheckEvent {
            key: *key,
            peers: checked_peers
                .into_iter()
                .map(|peer| (peer, Served::Offered))
                .collect(),
            paid_quote_issuers,
            root_offer,
        });
        if let (Err(_), Some(id)) = (sent, root_offer) {
            ctx.root_offers
                .close_served(id, Vec::<PeerId>::new(), |peer| peer);
        }
    }
}

/// What the offer dispatcher does with a write after reading its record
/// back from storage.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FreshRead<T, E> {
    /// The record was read: offer it.
    Offer(T),
    /// The record is gone: drop the offer.
    Gone,
    /// The read failed and the write has had its attempts: drop the offer.
    GiveUp { attempts: u32, error: E },
    /// The read failed: try again later, with this many attempts made.
    Retry { attempts: u32, error: E },
}

/// Sort a dispatcher read-back, made after `prior_attempts` failed ones,
/// into what happens next. A dropped offer sends nothing, no notice either:
/// its targets got the notice with the slim proof when the hold ended, while
/// this node held the record, and a notice sent now would come from a node
/// that has just found it holds no readable copy, which receivers would
/// count as heard from and skip offering. The notices deferred because the
/// record was unreadable as the hold ended go only with a read that works
/// ([`deferred_notices_after`]). No possession check is scheduled for a
/// dropped offer's targets.
pub(crate) fn fresh_read<T, E>(
    read: Result<Option<T>, E>,
    prior_attempts: u32,
    max_attempts: u32,
) -> FreshRead<T, E> {
    match read {
        Ok(Some(data)) => FreshRead::Offer(data),
        Ok(None) => FreshRead::Gone,
        Err(error) => {
            let attempts = prior_attempts + 1;
            if attempts >= max_attempts {
                FreshRead::GiveUp { attempts, error }
            } else {
                FreshRead::Retry { attempts, error }
            }
        }
    }
}

/// The deferred notices with the slim proof a dispatcher read-back sends:
/// all of them once the read finds the record, so they come from a node that
/// has just read its copy, and none while it cannot be read, once it is gone
/// or once the reads give up.
pub(crate) fn deferred_notices_after<'a, T, E>(
    read: &FreshRead<T, E>,
    deferred: &'a [PeerId],
) -> &'a [PeerId] {
    match read {
        FreshRead::Offer(_) => deferred,
        FreshRead::Gone | FreshRead::GiveUp { .. } | FreshRead::Retry { .. } => &[],
    }
}

/// What a hold's end finds of its record in this node's store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordAtEnd {
    /// Stored and claimed: the notices with the slim proof go now.
    Held,
    /// Indexed, but a read failed and none has worked since: the notices
    /// wait for the dispatcher's read-back of the record.
    Unreadable,
    /// Removed, quarantined or proven wrong: no notice goes.
    Gone,
}

/// The notices with the slim proof a hold's end sends now and those it
/// defers to the record's read-back, as (now, deferred), from the
/// [`proof_notice_recipients`] of a record not gone.
fn proof_notices_at_end(
    record: RecordAtEnd,
    recipients: Vec<PeerId>,
) -> (Vec<PeerId>, Vec<PeerId>) {
    match record {
        RecordAtEnd::Held => (recipients, Vec::new()),
        RecordAtEnd::Unreadable => (Vec::new(), recipients),
        RecordAtEnd::Gone => (Vec::new(), Vec::new()),
    }
}

/// Where a hold's record stands as the hold ends. In-memory lookups only, no
/// read-back.
pub(crate) fn record_at_hold_end(storage: &ChunkStore, key: &XorName) -> RecordAtEnd {
    if matches!(storage.exists(key), Ok(true)) {
        RecordAtEnd::Held
    } else if storage.is_suspect(key) {
        RecordAtEnd::Unreadable
    } else {
        RecordAtEnd::Gone
    }
}

/// A key's close group as notices and offers see it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CloseGroup {
    /// The `close_group_size` nearest in this node's view, one more for each
    /// peer marked full among them (at most 20 in all), this node left out.
    pub(crate) peers: Vec<PeerId>,
    /// The members the possession check probes: the unwidened close group,
    /// this node left out ([`possession_checked`]). The stand-ins beyond it
    /// are offered the record but not checked.
    pub(crate) checked: Vec<PeerId>,
}

/// The peers this node marks full now, from their own "full" notices.
fn marked_full(p2p_node: &P2PNode) -> HashSet<PeerId> {
    crate::replication::full::view_for(p2p_node.peer_id())
        .marks
        .marked_set(Instant::now())
}

/// The close group offers go to: the `close_group_size` nearest in this
/// node's view, widened past the peers in `marked` (marked full), self
/// included in the query and then left out.
async fn close_group_peers(
    key: &XorName,
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
    marked: &HashSet<PeerId>,
) -> CloseGroup {
    let self_id = *p2p_node.peer_id();
    let widened = crate::replication::full::widened_close_group_with(
        p2p_node,
        key,
        config.close_group_size,
        marked,
    )
    .await;
    // The stand-ins: past the unwidened close group (the 7 nearest, the node
    // itself counted). They take the offer, older nodes included, since a
    // chunk receiver admits a fresh offer anywhere among the key's 20 nearest
    // (`paid_list_close_group_size`). They are left unchecked because no peer
    // past the close group was possession-checked before ds.17 (ds.16
    // checks the 7 nearest): nothing is waived for them, and one that
    // takes the record is, by intent, not recorded as seen holding it.
    let checked = possession_checked(&widened, &self_id, config.close_group_size);
    let peers = widened
        .into_iter()
        .filter(|peer| *peer != self_id)
        .collect();
    CloseGroup { peers, checked }
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

/// The peers of `widened`, a key's close group widened past the peers marked
/// full (nearest first, this node included), that the possession check
/// probes: the unwidened close group, its first `close_group_size`, this
/// node left out, exactly the peers ds.16 checks. A peer marked
/// full among them is still checked, through the waiver; the stand-ins
/// beyond them are offered the record but not checked.
fn possession_checked(
    widened: &[PeerId],
    self_id: &PeerId,
    close_group_size: usize,
) -> Vec<PeerId> {
    widened
        .iter()
        .take(close_group_size)
        .filter(|peer| *peer != self_id)
        .copied()
        .collect()
}

/// Send `PaidNotify(K)` to every peer in `PaidCloseGroup(K)` (fire-and-forget):
/// holding notices and notices with the slim proof, as [`notice_recipients`]
/// splits them.
///
/// Per Invariant 16, as amended for holding notices: the sender MUST attempt
/// a notice to every member, with the slim proof, except that a close-group
/// peer gets a holding notice once this node has recorded the key itself,
/// and the slim proof when the hold ends unless it was heard from. A
/// notice carries no record, so it is neither gated by the send semaphore nor
/// by the pending-offer permit.
async fn send_paid_notices(
    key: &XorName,
    slim_proof: &[u8],
    recorded: bool,
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
) -> CloseGroup {
    let self_id = *p2p_node.peer_id();
    let paid_group: Vec<PeerId> = p2p_node
        .dht_manager()
        .find_closest_nodes_local_with_self(key, config.paid_list_close_group_size)
        .await
        .into_iter()
        .map(|n| n.peer_id)
        .filter(|peer| *peer != self_id)
        .collect();
    let close_group = close_group_peers(key, p2p_node, config, &marked_full(p2p_node)).await;
    let (holding, with_proof) = notice_recipients(
        &paid_group,
        &close_group.peers,
        recorded,
        !slim_proof.is_empty(),
    );
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
/// Returns the send tasks, which only the shutdown flush waits on.
pub(crate) fn send_notice<'a>(
    key: &XorName,
    proof_of_payment: Vec<u8>,
    peers: impl Iterator<Item = &'a PeerId>,
    p2p_node: &Arc<P2PNode>,
    counter: FreshCounter,
) -> Vec<tokio::task::JoinHandle<()>> {
    let holding = proof_of_payment.is_empty();
    let mut peers = peers
        .filter(|peer| !holding_notice_lost(holding, peer))
        .peekable();
    if peers.peek().is_none() {
        return Vec::new();
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
        return Vec::new();
    };
    // One buffer for every recipient; the sends only take handles.
    let encoded = Bytes::from(encoded);
    peers
        .map(|peer| {
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
            })
        })
        .collect()
}

/// Whether a holding notice to `peer` is dropped, as if lost on the way: only
/// in a `test-utils` build, when the harness names the peer
/// ([`crate::test_seams::FAULTS_FILE`]).
#[allow(clippy::missing_const_for_fn, unused_variables)]
fn holding_notice_lost(holding: bool, peer: &PeerId) -> bool {
    #[cfg(any(test, feature = "test-utils"))]
    if holding {
        return crate::test_seams::fault_set(
            crate::test_seams::Fault::DropHoldingTo,
            peer.as_bytes(),
        );
    }
    false
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

/// Why each of `peers` is skipped for `key`'s offer, or `None` when it is
/// offered, with `marked` the peers marked full by their own "full" notices.
/// The one place the heard-from table is read.
fn skip_reasons(
    heard_from: &HeardFrom,
    marked: &HashSet<PeerId>,
    key: &XorName,
    peers: &[PeerId],
    now: Instant,
) -> Vec<(PeerId, Option<Served>)> {
    peers
        .iter()
        .map(|peer| {
            let reason = heard_from.offer_skip_reason(key, peer, marked.contains(peer), now);
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
    /// Not heard from and not marked full: each gets the notice with the slim
    /// proof when the hold ends, is then offered the record, and is checked
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

/// The peers of the close group a hold was announced to that are no longer
/// in the close group it ends with, in the announced order.
fn left_close_group(announced: &[PeerId], now: &[PeerId]) -> Vec<PeerId> {
    let now: HashSet<&PeerId> = now.iter().collect();
    announced
        .iter()
        .filter(|peer| !now.contains(peer))
        .copied()
        .collect()
}

/// Who gets the notice with the slim proof when a hold ends, at once: every
/// peer of the close group now that was not heard from (marked full, or about
/// to be offered), and every peer that has left the announced close group
/// since and was not heard from either, `left` giving their skip reasons. A
/// peer that left is still in the paid group, and the holding notice it was
/// sent at announce recorded nothing, so this is its paid-list evidence from
/// this storer.
///
/// Nothing when this node's own paid-list write failed at announce: the
/// whole paid group got the slim proof then. Nothing either when this node
/// no longer holds the record (`held`, checked as the hold ends): a receiver
/// notes the sender of a proof as heard from and skips offering it, so the
/// notice goes only from a node that still holds the record.
fn proof_notice_recipients(
    serving: &Serving,
    left: &[(PeerId, Option<Served>)],
    recorded: bool,
    held: bool,
) -> Vec<PeerId> {
    if !recorded || !held {
        return Vec::new();
    }
    serving
        .notified
        .iter()
        .chain(serving.offered.iter())
        .copied()
        .chain(
            left.iter()
                .filter(|(_, reason)| *reason != Some(Served::HeardFrom))
                .map(|(peer, _)| *peer),
        )
        .collect()
}

/// Everything holding and ending a hold needs.
#[derive(Clone)]
pub(crate) struct HoldContext {
    pub(crate) p2p_node: Arc<P2PNode>,
    pub(crate) config: Arc<ReplicationConfig>,
    pub(crate) paid_list: Arc<PaidList>,
    pub(crate) storage: Arc<ChunkStore>,
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

    /// When the node stops: end every open hold without offers or checks,
    /// sending the notices with the slim proof each would have sent at its
    /// end, and wait for those sends. The engine's task drain bounds the
    /// wait. The offers already queued sent their notices when their holds
    /// ended.
    pub(crate) async fn flush_on_shutdown(&self) {
        let open = self.holds.take_due(Instant::now(), Duration::ZERO);
        let mut sends = Vec::new();
        for hold in open {
            // A record unreadable now sends nothing: no read-back follows.
            let (_, _, proof_notices, _) = self.split(&hold).await;
            sends.extend(send_notice(
                &hold.key,
                hold.proof,
                proof_notices.iter(),
                &self.p2p_node,
                FreshCounter::ProofNoticesSent,
            ));
        }
        for send in sends {
            let _ = send.await;
        }
    }

    /// How a hold's peers are served when it ends: the close group now, its
    /// split by skip reasons, and who gets the notice with the slim proof:
    /// now while the record is held; once a read-back finds it (the last
    /// list) while it is indexed but unreadable for the moment; and no one
    /// when it was quarantined, removed or proven wrong during the hold
    /// ([`record_at_hold_end`], no read-back).
    async fn split(&self, hold: &Hold) -> (CloseGroup, Serving, Vec<PeerId>, Vec<PeerId>) {
        let marked = marked_full(&self.p2p_node);
        let close_group = close_group_peers(&hold.key, &self.p2p_node, &self.config, &marked).await;
        let now = Instant::now();
        let serving = serve(skip_reasons(
            &self.heard_from,
            &marked,
            &hold.key,
            &close_group.peers,
            now,
        ));
        let left = skip_reasons(
            &self.heard_from,
            &marked,
            &hold.key,
            &left_close_group(&hold.close_group, &close_group.peers),
            now,
        );
        let record = record_at_hold_end(&self.storage, &hold.key);
        let recipients =
            proof_notice_recipients(&serving, &left, hold.recorded, record != RecordAtEnd::Gone);
        if record == RecordAtEnd::Unreadable && !recipients.is_empty() {
            debug!(
                "Chunk {} unreadable for now as its hold ends, deferring its proof notices \
                 to its read-back",
                hex::encode(hold.key)
            );
        }
        if record == RecordAtEnd::Gone && hold.recorded {
            debug!(
                "Chunk {} no longer stored as its hold ends, sending no proof notices",
                hex::encode(hold.key)
            );
        }
        let (proof_notices, deferred) = proof_notices_at_end(record, recipients);
        (close_group, serving, proof_notices, deferred)
    }

    /// End every hold that has ended by now: at its time, or early once every
    /// close-group peer is heard from or marked full.
    pub(crate) async fn end_ended_holds(&self) {
        let now = Instant::now();
        let marked = marked_full(&self.p2p_node);
        let ended = self
            .holds
            .take_ended(now, self.config.fresh_offer_hold(), |hold| {
                skip_reasons(&self.heard_from, &marked, &hold.key, &hold.close_group, now)
                    .iter()
                    .all(|(_, reason)| reason.is_some())
            });
        for (hold, end) in ended {
            self.end_hold(hold, end).await;
        }
    }

    /// End one hold: each peer [`proof_notice_recipients`] names gets the
    /// notice with the slim proof now while the record is still held, or
    /// once the dispatcher's read-back finds it while it is indexed but
    /// unreadable for the moment, and each close-group peer in this node's
    /// view now that was not heard from and is not marked full is then
    /// offered the record; every skipped peer's possession check is scheduled
    /// now.
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
        let (close_group, serving, proof_notices, deferred) = self.split(&hold).await;
        let unserved = serving.unserved(close_group.peers.len());
        if unserved > 0 {
            fresh_metrics::add(FreshCounter::PeersLeftUnserved, unserved as u64);
        }
        for (_, served) in &serving.skipped {
            fresh_metrics::bump(match served {
                Served::MarkedFull => FreshCounter::OffersSkippedMarkedFull,
                Served::HeardFrom | Served::Offered => FreshCounter::OffersSkippedHeardFrom,
            });
        }
        // The possession check probes the skipped peers inside the unwidened
        // close group; a stand-in for a full peer is not checked.
        let skipped_checked: Vec<(PeerId, Served)> = serving
            .skipped
            .iter()
            .filter(|(peer, _)| close_group.checked.contains(peer))
            .copied()
            .collect();
        for (_, served) in &skipped_checked {
            fresh_metrics::checks_scheduled(*served, 1);
        }
        // A storer whose own write failed sent the whole paid group the slim
        // proof when it announced the write. The others sent their close group
        // holding notices, so the peers not heard from get the proof now,
        // before any offer waits for a permit.
        send_notice(
            &key,
            hold.proof.clone(),
            proof_notices.iter(),
            &self.p2p_node,
            FreshCounter::ProofNoticesSent,
        );
        // A closed receiver (engine shutting down) is ignored.
        if !skipped_checked.is_empty() {
            let _ = self.possession_check_tx.send(PossessionCheckEvent {
                key,
                peers: skipped_checked,
                paid_quote_issuers: crate::replication::full::paid_quote_issuers(&hold.proof),
                // Skipped peers were sent no offer, so none refused one.
                root_offer: None,
            });
        }
        // A record unreadable for the moment still goes to the dispatcher with
        // no peer to offer it to: its read-back is what sends the deferred
        // notices.
        if serving.offered.is_empty() && deferred.is_empty() {
            return;
        }
        let offered = serving.offered.len();
        let checked = serving
            .offered
            .iter()
            .filter(|peer| close_group.checked.contains(peer))
            .copied()
            .collect();
        if self
            .offer_tx
            .send(FreshOfferEvent {
                key,
                payment_proof: hold.proof,
                read_attempts: 0,
                targets: serving.offered,
                checked,
                proof_notices: deferred,
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

    /// Ranks 1 to 7 of the widened group, the unwidened close group, are
    /// possession-checked, as ds.16 checks them; ranks 8 and beyond
    /// are stand-ins, offered the record but not checked. The node itself is
    /// never checked.
    #[test]
    fn only_the_close_group_is_possession_checked_and_stand_ins_are_not() {
        let peers: Vec<PeerId> = (1..=12u8).map(|n| PeerId::from_bytes([n; 32])).collect();
        let outside = PeerId::from_bytes([0xEE; 32]);
        assert_eq!(possession_checked(&peers, &outside, 7), peers[..7].to_vec());
        assert_eq!(
            possession_checked(&peers[..5], &outside, 7),
            peers[..5].to_vec()
        );
        // This node at rank 3: the other six of the close group.
        let mut expected = peers[..7].to_vec();
        expected.remove(2);
        assert_eq!(possession_checked(&peers, &peers[2], 7), expected);
    }

    /// With one peer marked full inside the close group, the group widens to
    /// 8, and the checked set is still the unwidened close group minus this
    /// node: the full peer is checked (through the waiver), the stand-in at
    /// rank 8 isn't.
    #[test]
    fn a_marked_peer_widens_the_offer_not_the_check() {
        let peers: Vec<PeerId> = (1..=12u8).map(|n| PeerId::from_bytes([n; 32])).collect();
        let self_id = peers[0];
        let full = peers[3];
        let len = crate::replication::full::widened_len(&peers, 7, |peer| *peer == full);
        assert_eq!(len, 8);
        let widened = &peers[..len];
        let unwidened_minus_self: Vec<PeerId> = peers[1..7].to_vec();
        assert_eq!(
            possession_checked(widened, &self_id, 7),
            unwidened_minus_self
        );
        assert!(possession_checked(widened, &self_id, 7).contains(&full));
    }

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

    fn peer(byte: u8) -> PeerId {
        PeerId::from_bytes([byte; 32])
    }

    /// A root proof whose quotes come from `quoted`, naming as paid a hash
    /// none of them has.
    fn root_proof(quoted: &[PeerId]) -> Vec<u8> {
        root_proof_paying(quoted, None)
    }

    /// A root proof whose quotes come from `quoted`, each at its own price,
    /// naming as paid the quote of `quoted[paid]`, or with `None` a hash none
    /// of them has.
    fn root_proof_paying(quoted: &[PeerId], paid: Option<usize>) -> Vec<u8> {
        use crate::payment::{serialize_root_proof, PaymentProof, RootPaymentProof};

        let peer_quotes: Vec<_> = quoted
            .iter()
            .zip(1u64..)
            .map(|(peer, price)| {
                let quote = evmlib::PaymentQuote {
                    content: xor_name::XorName([0x42; 32]),
                    timestamp: std::time::UNIX_EPOCH,
                    price: evmlib::common::Amount::from(price),
                    rewards_address: evmlib::RewardsAddress::new([1; 20]),
                    committed_key_count: 0,
                    commitment_pin: None,
                    pub_key: Vec::new(),
                    signature: Vec::new(),
                };
                (evmlib::EncodedPeerId::new(*peer.as_bytes()), quote)
            })
            .collect();
        let quote_hash = paid.map_or_else(
            || evmlib::common::QuoteHash::from([1; 32]),
            |paid| peer_quotes[paid].1.hash(),
        );
        serialize_root_proof(&RootPaymentProof {
            payment: PaymentProof {
                proof_of_payment: evmlib::ProofOfPayment { peer_quotes },
                tx_hashes: Vec::new(),
                commitment_sidecars: Vec::new(),
            },
            quote_hash,
            amount: evmlib::common::Amount::from(1u64),
            index: 0,
            leaf_count: 1,
            path: Vec::new(),
        })
        .expect("encode")
    }

    /// Only a root proof makes an offer one a peer may refuse unpenalised,
    /// and it names the peer of the quote it paid.
    #[test]
    fn only_a_root_proof_is_root_paid() {
        use crate::ant_protocol::{PROOF_TAG_MERKLE, PROOF_TAG_ROOT, PROOF_TAG_SINGLE_NODE};

        assert_eq!(root_quotes(&root_proof(&[])), Some(RootQuotes::default()));
        assert_eq!(
            root_quotes(&root_proof(&[peer(4), peer(5), peer(6)])),
            Some(RootQuotes { paid: None })
        );
        assert_eq!(
            root_quotes(&root_proof_paying(&[peer(4), peer(5), peer(6)], Some(1))),
            Some(RootQuotes {
                paid: Some(peer(5)),
            })
        );
        assert_eq!(root_quotes(&[PROOF_TAG_SINGLE_NODE, 0x90]), None);
        assert_eq!(root_quotes(&[PROOF_TAG_MERKLE, 0x90]), None);
        assert_eq!(root_quotes(&[0x7f, 0x90]), None);
        assert_eq!(root_quotes(&[]), None);
        // A root tag that doesn't decode is checked as a single-node offer.
        assert_eq!(root_quotes(&[PROOF_TAG_ROOT, 0x90]), None);
    }

    /// A refusal reason that is not about payment, as a peer out of range
    /// for the key gives.
    const OUT_OF_RANGE: &str = "Not in storage-admission range for this key";

    /// The refusal a node with no root vault gives a root-paid offer, worded
    /// as `handle_fresh_offer` words it.
    fn no_root_vault() -> String {
        format!("{PAYMENT_VERIFICATION_ERROR}: no root vault configured")
    }

    /// A peer that refuses a root-paid offer at payment verification is left
    /// out of that key's possession check; one that accepted or never
    /// answered stays in it. A refusal from a peer the offer was not sent to,
    /// naming another key, or for another offer, is ignored, and closing the
    /// offer forgets it.
    #[test]
    fn a_peer_that_refuses_a_root_paid_offer_is_left_out_of_its_possession_check() {
        let offers = RootOfferRefusals::default();
        let key = [0x42; 32];
        let targets = vec![peer(1), peer(2), peer(3)];
        let refused = no_root_vault();
        offers.open(7, key, &targets, &RootQuotes::default());
        assert_eq!(offers.pending(), 1);

        assert_eq!(
            offers.note_refusal(7, &peer(2), &key, &refused),
            Refusal::Excused
        );
        assert_eq!(
            offers.note_refusal(7, &peer(9), &key, &refused),
            Refusal::Untracked,
            "not a target"
        );
        assert_eq!(
            offers.note_refusal(7, &peer(3), &[0x43; 32], &refused),
            Refusal::Untracked,
            "another key"
        );
        assert_eq!(
            offers.note_refusal(8, &peer(1), &key, &refused),
            Refusal::Untracked,
            "another offer"
        );

        assert_eq!(offers.close(7, targets.clone()), vec![peer(1), peer(3)]);
        assert_eq!(offers.pending(), 0);
        // Closed: a late refusal is ignored, and the id no longer filters.
        assert_eq!(
            offers.note_refusal(7, &peer(1), &key, &refused),
            Refusal::Untracked
        );
        assert_eq!(offers.close(7, targets.clone()), targets);
    }

    /// A peer that refuses the offer for a reason other than payment
    /// verification stays in the key's possession check, as a refuser of a
    /// single-node offer does, whether or not the proof holds a quote from
    /// it. Offers carry the slim proof, which usually holds only the paid
    /// quote, so being unquoted excuses nothing: the close group's other
    /// members keep their check. A peer the slim proof holds no quote from
    /// that refuses at payment verification is still excused.
    #[test]
    fn a_peer_that_refuses_a_root_paid_offer_for_another_reason_stays_in_its_possession_check() {
        let key = [0x42; 32];
        let targets = vec![peer(1), peer(2), peer(3), peer(4)];
        // Peer 1's quote is the paid one; peer 2's unpaid quote is in the
        // proof too, and so is peer 8's, a peer the offer isn't sent to.
        // Peers 3 and 4 hold no quote in it, as most of the close group hold
        // none in a slim proof.
        let proof = root_proof_paying(&[peer(1), peer(2), peer(8)], Some(0));
        let quotes = root_quotes(&proof).expect("a root proof");
        for reason in [OUT_OF_RANGE, "Out of space", "Failed to store chunk", ""] {
            let offers = RootOfferRefusals::default();
            offers.open(7, key, &targets, &quotes);
            assert_eq!(
                offers.note_refusal(7, &peer(2), &key, reason),
                Refusal::Charged,
                "a quoted peer refusing with {reason:?}"
            );
            assert_eq!(
                offers.note_refusal(7, &peer(3), &key, reason),
                Refusal::Charged,
                "an unquoted peer refusing with {reason:?}"
            );
            assert_eq!(
                offers.note_refusal(7, &peer(4), &key, &no_root_vault()),
                Refusal::Excused,
                "an unquoted peer refusing at payment verification"
            );
            // A quoted peer the offer was not sent to counts for nothing.
            assert_eq!(
                offers.note_refusal(7, &peer(8), &key, reason),
                Refusal::Untracked
            );
            assert_eq!(
                offers.close(7, targets.clone()),
                vec![peer(1), peer(2), peer(3)]
            );
        }
    }

    /// A quote doesn't record its settlement version, so a quote an older
    /// node or one with no root vault gave at version 2 (or unversioned) can
    /// sit in a root proof: its bytes are the same as a version-3 quote's.
    /// That peer can't check the proof and refuses the offer at payment
    /// verification, so it is excused like an unquoted peer, whichever of
    /// the two payment-verification refusals it gives, and it is left out of
    /// the key's possession check.
    #[test]
    fn a_peer_quoted_at_version_2_in_a_root_proof_that_refuses_at_payment_verification_is_excused()
    {
        let offers = RootOfferRefusals::default();
        let key = [0x42; 32];
        let targets = vec![peer(1), peer(2), peer(3)];
        // Peers 1 and 2 quoted at version 2; the proof can't tell.
        let proof = root_proof(&[peer(1), peer(2), peer(3)]);
        let quoted = root_quotes(&proof).expect("a root proof");
        offers.open(7, key, &targets, &quoted);

        // The two reasons a fresh-offer receiver gives at payment
        // verification, worded as `handle_fresh_offer` words them.
        let error = format!("{PAYMENT_VERIFICATION_ERROR}: no root vault configured");
        let failed = format!("{PAYMENT_VERIFICATION_FAILED}: payment required");
        assert_eq!(
            offers.note_refusal(7, &peer(1), &key, &error),
            Refusal::Excused
        );
        assert_eq!(
            offers.note_refusal(7, &peer(2), &key, &failed),
            Refusal::Excused
        );

        assert_eq!(offers.close(7, targets), vec![peer(3)]);
    }

    /// The peer whose quote the root proof paid is charged whatever it
    /// says: refusing at payment verification, with either reason, or for
    /// any other reason, it stays in the key's possession check, so a node
    /// paid for a record it then doesn't take can't keep the payment and skip
    /// the check. An unpaid quoted peer refusing at payment verification
    /// beside it is still excused.
    #[test]
    fn the_paid_peer_that_refuses_a_root_paid_offer_stays_in_its_possession_check() {
        let key = [0x42; 32];
        let targets = vec![peer(1), peer(2), peer(3)];
        // Peer 2's quote is the paid one.
        let proof = root_proof_paying(&[peer(1), peer(2), peer(3)], Some(1));
        let quotes = root_quotes(&proof).expect("a root proof");
        assert_eq!(quotes.paid, Some(peer(2)));

        let error = format!("{PAYMENT_VERIFICATION_ERROR}: no root vault configured");
        let failed = format!("{PAYMENT_VERIFICATION_FAILED}: payment required");
        for reason in [error.as_str(), failed.as_str(), OUT_OF_RANGE] {
            let offers = RootOfferRefusals::default();
            offers.open(7, key, &targets, &quotes);
            assert_eq!(
                offers.note_refusal(7, &peer(2), &key, reason),
                Refusal::Charged,
                "the paid peer refusing with {reason:?}"
            );
            assert_eq!(
                offers.note_refusal(7, &peer(1), &key, &error),
                Refusal::Excused
            );
            assert_eq!(offers.close(7, targets.clone()), vec![peer(2), peer(3)]);
        }
    }

    /// Only the two payment-verification refusals count as one.
    #[test]
    fn only_payment_verification_reasons_are_payment_refusals() {
        assert!(is_payment_refusal(
            "Payment verification failed: payment required"
        ));
        assert!(is_payment_refusal("Payment verification error: rpc down"));
        assert!(!is_payment_refusal(OUT_OF_RANGE));
        assert!(!is_payment_refusal("Missing proof of payment"));
        assert!(!is_payment_refusal("payment verification error"));
        assert!(!is_payment_refusal(""));
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
    /// or marked full) or offered. Every peer not heard from is notified when
    /// the hold ends (see the next test).
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

    /// When a hold ends, every close-group peer not heard from gets the
    /// notice with the slim proof at once, the peers to be offered included:
    /// their notice is decided here, before their offer is queued, so an
    /// offer waiting behind a full pending-offer semaphore (or never sent)
    /// never delays or loses it. A storer whose paid-list write failed sent
    /// the whole paid group the proof at announce, and sends none here.
    #[test]
    fn every_peer_not_heard_from_gets_the_proof_when_the_hold_ends() {
        let p = peers(1..8);
        let serving = serve(vec![
            (p[0], Some(Served::HeardFrom)),
            (p[1], Some(Served::MarkedFull)),
            (p[2], None),
            (p[3], None),
        ]);
        let notices = proof_notice_recipients(&serving, &[], true, true);
        assert_eq!(notices, vec![p[1], p[2], p[3]]);
        for target in &serving.offered {
            assert!(notices.contains(target), "offered {target:?} has no notice");
        }
        assert!(proof_notice_recipients(&serving, &[], false, true).is_empty());
    }

    /// A record quarantined or removed during its hold gets no notice with
    /// the slim proof when the hold ends: a receiver would note this node as
    /// heard from and skip offering it. The peers to be offered still get
    /// their offer queued, which the dispatcher drops when it finds the
    /// record gone, and the skipped peers' possession checks still run.
    #[test]
    fn a_record_no_longer_held_sends_no_proof_when_the_hold_ends() {
        let p = peers(1..8);
        let serving = serve(vec![
            (p[0], Some(Served::HeardFrom)),
            (p[1], Some(Served::MarkedFull)),
            (p[2], None),
        ]);
        let left_reasons = vec![(p[5], None)];
        assert!(proof_notice_recipients(&serving, &left_reasons, true, false).is_empty());
        assert!(proof_notice_recipients(&serving, &left_reasons, false, false).is_empty());
        assert_eq!(
            proof_notice_recipients(&serving, &left_reasons, true, true),
            vec![p[1], p[2], p[5]]
        );
    }

    /// A record indexed but unreadable for the moment as its hold ends sends
    /// no notice with the slim proof then and none with a read-back that
    /// still fails: the notices wait in the offer event and go with the
    /// first read-back that finds the record. A record proven wrong defers
    /// nothing, and reads that give up send nothing.
    #[tokio::test]
    async fn a_record_unreadable_as_its_hold_ends_sends_its_proof_once_read() {
        use crate::replication::config::MAX_FRESH_READ_ATTEMPTS;
        let dir = tempfile::TempDir::new().expect("temp dir");
        let store = ChunkStore::new(crate::storage::ChunkStoreConfig {
            root_dir: dir.path().to_path_buf(),
            ..crate::storage::ChunkStoreConfig::test_default()
        })
        .await
        .expect("open store");
        let content = b"unreadable-for-a-moment".to_vec();
        let key = crate::client::compute_address(&content);
        store.put(&key, &content).await.expect("put");
        assert_eq!(record_at_hold_end(&store, &key), RecordAtEnd::Held);

        // The file is briefly something the store refuses to read: the read
        // fails, and the chunk stays indexed but suspect.
        let path = store.test_chunk_path(&key);
        let aside = path.with_extension("aside");
        std::fs::rename(&path, &aside).expect("move the file aside");
        std::fs::create_dir(&path).expect("a directory in its place");
        assert!(store.get(&key).await.is_err(), "the read fails");
        assert_eq!(record_at_hold_end(&store, &key), RecordAtEnd::Unreadable);

        let p = peers(1..4);
        let serving = serve(p.iter().map(|peer| (*peer, None)).collect());
        let record = record_at_hold_end(&store, &key);
        let recipients = proof_notice_recipients(&serving, &[], true, record != RecordAtEnd::Gone);
        let (now, deferred) = proof_notices_at_end(record, recipients);
        assert!(now.is_empty(), "no notice while the record is unreadable");
        assert_eq!(deferred, p, "every peer not heard from waits for the read");

        // Read-backs while it is still unreadable send nothing, and keep it.
        for prior in 0..2 {
            let read = fresh_read(
                store.get_or_quarantine(&key).await,
                prior,
                MAX_FRESH_READ_ATTEMPTS,
            );
            assert!(matches!(read, FreshRead::Retry { .. }), "read {prior}");
            assert!(deferred_notices_after(&read, &deferred).is_empty());
        }
        assert!(store.is_indexed(&key), "an unreadable file is kept");

        // The fault clears: the next read-back finds the record and sends.
        std::fs::remove_dir(&path).expect("remove the directory");
        std::fs::rename(&aside, &path).expect("put the file back");
        let read = fresh_read(
            store.get_or_quarantine(&key).await,
            2,
            MAX_FRESH_READ_ATTEMPTS,
        );
        assert!(matches!(read, FreshRead::Offer(_)), "the read works");
        assert_eq!(deferred_notices_after(&read, &deferred), p.as_slice());
        assert_eq!(record_at_hold_end(&store, &key), RecordAtEnd::Held);

        // Reads that give up send nothing.
        let gave_up: FreshRead<Vec<u8>, ()> = FreshRead::GiveUp {
            attempts: MAX_FRESH_READ_ATTEMPTS,
            error: (),
        };
        assert!(deferred_notices_after(&gave_up, &deferred).is_empty());

        // A record proven wrong is gone for the hold, not unreadable: its
        // notices are neither sent nor deferred.
        let wrong_content = b"proven-wrong".to_vec();
        let wrong = crate::client::compute_address(&wrong_content);
        store.put(&wrong, &wrong_content).await.expect("put");
        std::fs::write(store.test_chunk_path(&wrong), b"tampered").expect("tamper");
        assert!(store.get(&wrong).await.is_err(), "verification fails");
        assert!(store.is_indexed(&wrong), "a plain read removes nothing");
        assert_eq!(record_at_hold_end(&store, &wrong), RecordAtEnd::Gone);
        let recipients = proof_notice_recipients(&serving, &[], true, false);
        assert_eq!(
            proof_notices_at_end(RecordAtEnd::Gone, recipients),
            (Vec::new(), Vec::new())
        );
    }

    /// A peer that left the close group during the hold, but was in it at
    /// announce, got only a holding notice, which records nothing. Unless it
    /// was heard from, it gets the slim proof when the hold ends, though it
    /// is no longer offered; a peer that joined is served as any other.
    #[test]
    fn a_peer_that_left_the_close_group_during_the_hold_still_gets_the_proof() {
        let announced = peers(1..8);
        let now = peers(3..10);
        let left = left_close_group(&announced, &now);
        assert_eq!(left, peers(1..3));

        let serving = serve(now.iter().map(|peer| (*peer, None)).collect());
        assert_eq!(serving.unserved(now.len()), 0);
        let left_reasons = vec![(left[0], Some(Served::HeardFrom)), (left[1], None)];
        let notices = proof_notice_recipients(&serving, &left_reasons, true, true);
        assert!(!notices.contains(&left[0]), "a peer heard from is skipped");
        assert!(notices.contains(&left[1]), "a peer that left is notified");
        assert!(!serving.offered.contains(&left[1]), "and not offered");
        for peer in &now {
            assert!(notices.contains(peer));
        }
        assert_eq!(notices.len(), now.len() + 1);

        assert!(left_close_group(&announced, &announced).is_empty());
    }

    /// The offer dispatcher's arms: a record read back is offered, one gone
    /// is dropped at once, and a failed read is retried until its attempts
    /// run out, then dropped. A dropped offer sends nothing, no notice
    /// either (the dispatcher has no call that sends one, which the census
    /// of proof senders pins).
    #[test]
    fn the_dispatcher_offers_a_record_read_back_and_drops_one_gone_or_unreadable() {
        let max = 3;
        assert_eq!(
            fresh_read::<_, ()>(Ok(Some(vec![1u8])), 0, max),
            FreshRead::Offer(vec![1u8])
        );
        assert_eq!(fresh_read::<Vec<u8>, ()>(Ok(None), 0, max), FreshRead::Gone);
        assert_eq!(
            fresh_read::<Vec<u8>, _>(Err("io"), 0, max),
            FreshRead::Retry {
                attempts: 1,
                error: "io"
            }
        );
        assert_eq!(
            fresh_read::<Vec<u8>, _>(Err("io"), max - 1, max),
            FreshRead::GiveUp {
                attempts: max,
                error: "io"
            }
        );
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
        let none = HashSet::new();
        let reasons = skip_reasons(&table, &none, &key, &p, now);
        assert_eq!(
            reasons,
            vec![(p[0], Some(Served::HeardFrom)), (p[1], None), (p[2], None)]
        );
        assert_eq!(serve(reasons).offered, vec![p[1], p[2]]);
        assert_eq!(
            serve(skip_reasons(&HeardFrom::default(), &none, &key, &p, now)).offered,
            p
        );
        // A peer marked full and not heard from is skipped as marked full,
        // and gets the notice with the slim proof; one heard from is skipped
        // as heard from.
        let marked: HashSet<PeerId> = [p[0], p[2]].into_iter().collect();
        let reasons = skip_reasons(&table, &marked, &key, &p, now);
        assert_eq!(
            reasons,
            vec![
                (p[0], Some(Served::HeardFrom)),
                (p[1], None),
                (p[2], Some(Served::MarkedFull)),
            ]
        );
        let serving = serve(reasons);
        assert_eq!(serving.offered, vec![p[1]]);
        assert_eq!(serving.notified, vec![p[2]]);
    }
}

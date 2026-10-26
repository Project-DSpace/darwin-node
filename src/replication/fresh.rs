//! Fresh replication (Section 6.1).
//!
//! When a node accepts a newly written record with valid `PoP`:
//! 1. Store locally (already done by chunk handler).
//! 2. Record the key in `PaidForList(self)` and send `PaidNotify` to every
//!    peer in `PaidCloseGroup(K)` — immediately, never behind back-pressure.
//! 3. Send fresh offers to `CLOSE_GROUP_SIZE` nearest peers (excluding self),
//!    bounded by the pending-offer permits so a write burst cannot pile up
//!    chunk-sized buffers.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::logging::{debug, warn};
use bytes::Bytes;
use rand::Rng;
use saorsa_core::identity::PeerId;
use saorsa_core::P2PNode;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use crate::ant_protocol::XorName;
use crate::payment::{deserialize_root_proof, detect_proof_type, ProofType};
use crate::replication::config::{
    ReplicationConfig, FRESH_REPLICATION_DELIVERY_MAX_RETRIES, REPLICATION_PROTOCOL_ID,
};
use crate::replication::paid_list::PaidList;
use crate::replication::possession::PossessionCheckEvent;
use crate::replication::protocol::{
    FreshReplicationOffer, PaidNotify, ReplicationMessage, ReplicationMessageBody,
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
    /// Serialized proof-of-payment.
    pub payment_proof: Vec<u8>,
}

/// A write whose paid-list evidence has been announced and whose chunk offer
/// is waiting for a pending-offer permit. Carries no chunk bytes.
pub(crate) struct FreshOfferEvent {
    pub(crate) key: XorName,
    pub(crate) payment_proof: Vec<u8>,
    /// Storage read-backs attempted so far; see `MAX_FRESH_READ_ATTEMPTS`.
    pub(crate) read_attempts: u32,
}

/// Handles shared by everything that sends fresh offers, so the offer
/// dispatcher task and the direct entry point run one pipeline.
pub(crate) struct FreshOfferContext {
    pub(crate) p2p_node: Arc<P2PNode>,
    pub(crate) config: Arc<ReplicationConfig>,
    /// Limits concurrent outbound chunk transfers across the engine.
    pub(crate) send_semaphore: Arc<Semaphore>,
    /// Delayed possession checks (ADR-0003) are scheduled here once an
    /// offer's sends are dispatched.
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
/// rollout. Such a peer refuses the offer at payment verification. A refusal
/// whose reason is a payment-verification refusal
/// ([`is_payment_refusal`]) is therefore excused from every peer, quoted or
/// not, as payment outcomes are never charged on the receiving side either.
/// A peer the proof holds no quote from is excused whatever its reason. A
/// quoted peer that refuses for any other reason (out of range, out of
/// space, a failed write) stays in the possession check, charged as a
/// missing chunk as every refusal of a single-node offer is. The quotes are
/// not signature-checked here: a client that lists a peer it never asked only
/// narrows that peer's excuse to payment refusals.
///
/// Single-node offers are not tracked, and a peer that accepts a root-paid
/// offer, or doesn't answer, is checked as before.
///
/// An entry lives from the offer's send to its possession check, as the
/// check's own event does, so the map holds no more entries than there are
/// checks pending.
#[derive(Default)]
pub(crate) struct RootOfferRefusals {
    offers: parking_lot::Mutex<HashMap<u64, RootOffer>>,
}

/// One root-paid offer: the key, the peers it was sent to, those of them
/// the proof holds a quote from, and those excused for refusing it.
struct RootOffer {
    key: XorName,
    targets: HashSet<PeerId>,
    quoted: HashSet<PeerId>,
    refused: HashSet<PeerId>,
}

/// What a refusal of a fresh offer does to the key's possession check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// Not a refusal of a tracked root-paid offer, by a peer it was sent to,
    /// naming its key: nothing changes.
    Untracked,
    /// The proof holds no quote from the peer, or the peer refused at
    /// payment verification, so it may be one that can't check a root proof:
    /// it is left out of the possession check.
    Excused,
    /// The peer quoted the key for its root payment and refused for a reason
    /// other than payment verification: it stays in the possession check.
    Charged,
}

impl RootOfferRefusals {
    /// Track the root-paid offer `request_id` of `key`, sent to `targets`,
    /// whose proof holds a quote from each of `quoted`.
    pub(crate) fn open(
        &self,
        request_id: u64,
        key: XorName,
        targets: &[PeerId],
        quoted: &HashSet<PeerId>,
    ) {
        let targets: HashSet<PeerId> = targets.iter().copied().collect();
        let quoted = targets.intersection(quoted).copied().collect();
        self.offers.lock().insert(
            request_id,
            RootOffer {
                key,
                targets,
                quoted,
                refused: HashSet::new(),
            },
        );
    }

    /// Note that `source` refused the offer `request_id` of `key`, giving
    /// `reason`. Only a peer the offer was sent to, naming the offer's key,
    /// counts; it is excused when the proof holds no quote from it or when
    /// `reason` is a payment-verification refusal.
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
        if offer.quoted.contains(source) && !is_payment_refusal(reason) {
            return Refusal::Charged;
        }
        offer.refused.insert(*source);
        Refusal::Excused
    }

    /// Close the offer `request_id`: `peers` without those that refused it.
    /// Unknown ids leave `peers` as they are.
    pub(crate) fn close(&self, request_id: u64, peers: Vec<PeerId>) -> Vec<PeerId> {
        let Some(offer) = self.offers.lock().remove(&request_id) else {
            return peers;
        };
        peers
            .into_iter()
            .filter(|peer| !offer.refused.contains(peer))
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

/// For a proof that pays through the root vault, the peers it holds a quote
/// from; a peer outside them that can't check a root proof may refuse the
/// offer. `None` for any other proof, and for a root proof that doesn't
/// decode, whose offer is then checked as a single-node one is.
fn root_quoted_peers(proof_of_payment: &[u8]) -> Option<HashSet<PeerId>> {
    match detect_proof_type(proof_of_payment) {
        Some(ProofType::Root) => deserialize_root_proof(proof_of_payment).ok().map(|proof| {
            proof
                .payment
                .proof_of_payment
                .peer_quotes
                .iter()
                .map(|(peer, _)| PeerId::from_bytes(*peer.as_bytes()))
                .collect()
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
/// or the send semaphore; both messages are small metadata.
pub(crate) async fn announce_paid_write(
    key: &XorName,
    proof_of_payment: &[u8],
    paid_list: &PaidList,
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
) {
    // Rule 6: Node that validates PoP adds K to PaidForList(self).
    if let Err(e) = paid_list.insert(key).await {
        warn!("Failed to add key {} to PaidForList: {e}", hex::encode(key));
    }
    // Rules 7-8: PaidNotify to every member of PaidCloseGroup(K).
    send_paid_notify(key, proof_of_payment, p2p_node, config).await;
}

/// Rules 2-3: send fresh offers to the close group and schedule the delayed
/// possession check (ADR-0003) for the responsible peers.
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
    pending_offer: OwnedSemaphorePermit,
) {
    let self_id = *ctx.p2p_node.peer_id();

    // Use the self-inclusive query to get the true close group, then filter
    // self out.
    let closest = ctx
        .p2p_node
        .dht_manager()
        .find_closest_nodes_local_with_self(key, ctx.config.close_group_size)
        .await;
    let target_peers: Vec<PeerId> = closest
        .iter()
        .filter(|n| n.peer_id != self_id)
        .map(|n| n.peer_id)
        .collect();

    let root_quoted = root_quoted_peers(&proof_of_payment);
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
    let root_offer = root_quoted
        .filter(|_| !target_peers.is_empty())
        .map(|quoted| {
            ctx.root_offers
                .open(request_id, *key, &target_peers, &quoted);
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

    // Schedule the delayed possession check (ADR-0003) for the responsible
    // close-group peers. A closed receiver (engine shutting down) is ignored.
    if !target_peers.is_empty() {
        ctx.dispatched.fetch_add(1, Ordering::Relaxed);
        let sent = ctx.possession_check_tx.send(PossessionCheckEvent {
            key: *key,
            peers: target_peers,
            root_offer,
        });
        if let (Err(_), Some(id)) = (sent, root_offer) {
            ctx.root_offers.close(id, Vec::new());
        }
    }
}

/// Send `PaidNotify(K)` to every peer in `PaidCloseGroup(K)` (fire-and-forget).
///
/// Per Invariant 16: sender MUST attempt delivery to every member. The
/// message is small metadata (no chunk data), so it is neither gated by the
/// send semaphore nor by the pending-offer permit.
async fn send_paid_notify(
    key: &XorName,
    proof_of_payment: &[u8],
    p2p_node: &Arc<P2PNode>,
    config: &ReplicationConfig,
) {
    let self_id = *p2p_node.peer_id();
    let paid_group = p2p_node
        .dht_manager()
        .find_closest_nodes_local_with_self(key, config.paid_list_close_group_size)
        .await;

    let notify = PaidNotify {
        key: *key,
        proof_of_payment: proof_of_payment.to_vec(),
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
    for node in &paid_group {
        if node.peer_id == self_id {
            continue;
        }
        let p2p = Arc::clone(p2p_node);
        let data = encoded.clone();
        let peer_id = node.peer_id;
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

    fn peer(byte: u8) -> PeerId {
        PeerId::from_bytes([byte; 32])
    }

    /// A root proof whose quotes come from `quoted`.
    fn root_proof(quoted: &[PeerId]) -> Vec<u8> {
        use crate::payment::{serialize_root_proof, PaymentProof, RootPaymentProof};

        let peer_quotes = quoted
            .iter()
            .map(|peer| {
                let quote = evmlib::PaymentQuote {
                    content: xor_name::XorName([0x42; 32]),
                    timestamp: std::time::SystemTime::now(),
                    price: evmlib::common::Amount::from(1u64),
                    rewards_address: evmlib::RewardsAddress::new([1; 20]),
                    committed_key_count: 0,
                    commitment_pin: None,
                    pub_key: Vec::new(),
                    signature: Vec::new(),
                };
                (evmlib::EncodedPeerId::new(*peer.as_bytes()), quote)
            })
            .collect();
        serialize_root_proof(&RootPaymentProof {
            payment: PaymentProof {
                proof_of_payment: evmlib::ProofOfPayment { peer_quotes },
                tx_hashes: Vec::new(),
                commitment_sidecars: Vec::new(),
            },
            quote_hash: evmlib::common::QuoteHash::from([1; 32]),
            amount: evmlib::common::Amount::from(1u64),
            index: 0,
            leaf_count: 1,
            path: Vec::new(),
        })
        .expect("encode")
    }

    /// Only a root proof makes an offer one a peer may refuse unpenalised,
    /// and it names the peers it holds a quote from.
    #[test]
    fn only_a_root_proof_is_root_paid() {
        use crate::ant_protocol::{PROOF_TAG_MERKLE, PROOF_TAG_ROOT, PROOF_TAG_SINGLE_NODE};

        assert_eq!(root_quoted_peers(&root_proof(&[])), Some(HashSet::new()));
        assert_eq!(
            root_quoted_peers(&root_proof(&[peer(4), peer(5)])),
            Some([peer(4), peer(5)].into_iter().collect())
        );
        assert_eq!(root_quoted_peers(&[PROOF_TAG_SINGLE_NODE, 0x90]), None);
        assert_eq!(root_quoted_peers(&[PROOF_TAG_MERKLE, 0x90]), None);
        assert_eq!(root_quoted_peers(&[0x7f, 0x90]), None);
        assert_eq!(root_quoted_peers(&[]), None);
        // A root tag that doesn't decode is checked as a single-node offer.
        assert_eq!(root_quoted_peers(&[PROOF_TAG_ROOT, 0x90]), None);
    }

    /// A refusal reason that is not about payment, as a peer out of range
    /// for the key gives.
    const OUT_OF_RANGE: &str = "Not in storage-admission range for this key";

    /// A peer that refuses a root-paid offer is left out of that key's
    /// possession check; one that accepted or never answered stays in it. A
    /// refusal from a peer the offer was not sent to, naming another key, or
    /// for another offer, is ignored, and closing the offer forgets it.
    #[test]
    fn a_peer_that_refuses_a_root_paid_offer_is_left_out_of_its_possession_check() {
        let offers = RootOfferRefusals::default();
        let key = [0x42; 32];
        let targets = vec![peer(1), peer(2), peer(3)];
        offers.open(7, key, &targets, &HashSet::new());
        assert_eq!(offers.pending(), 1);

        assert_eq!(
            offers.note_refusal(7, &peer(2), &key, OUT_OF_RANGE),
            Refusal::Excused
        );
        assert_eq!(
            offers.note_refusal(7, &peer(9), &key, OUT_OF_RANGE),
            Refusal::Untracked,
            "not a target"
        );
        assert_eq!(
            offers.note_refusal(7, &peer(3), &[0x43; 32], OUT_OF_RANGE),
            Refusal::Untracked,
            "another key"
        );
        assert_eq!(
            offers.note_refusal(8, &peer(1), &key, OUT_OF_RANGE),
            Refusal::Untracked,
            "another offer"
        );

        assert_eq!(offers.close(7, targets.clone()), vec![peer(1), peer(3)]);
        assert_eq!(offers.pending(), 0);
        // Closed: a late refusal is ignored, and the id no longer filters.
        assert_eq!(
            offers.note_refusal(7, &peer(1), &key, OUT_OF_RANGE),
            Refusal::Untracked
        );
        assert_eq!(offers.close(7, targets.clone()), targets);
    }

    /// A peer the root proof holds a quote from that refuses the offer for a
    /// reason other than payment verification stays in the key's possession
    /// check, as a refuser of a single-node offer does; an unquoted peer that
    /// refuses beside it is still left out.
    #[test]
    fn a_quoted_peer_that_refuses_a_root_paid_offer_for_another_reason_stays_in_its_possession_check(
    ) {
        let offers = RootOfferRefusals::default();
        let key = [0x42; 32];
        let targets = vec![peer(1), peer(2), peer(3)];
        let proof = root_proof(&[peer(1), peer(2), peer(8)]);
        let quoted = root_quoted_peers(&proof).expect("a root proof");
        offers.open(7, key, &targets, &quoted);

        assert_eq!(
            offers.note_refusal(7, &peer(1), &key, OUT_OF_RANGE),
            Refusal::Charged
        );
        assert_eq!(
            offers.note_refusal(7, &peer(3), &key, OUT_OF_RANGE),
            Refusal::Excused
        );
        // A quoted peer the offer was not sent to counts for nothing.
        assert_eq!(
            offers.note_refusal(7, &peer(8), &key, OUT_OF_RANGE),
            Refusal::Untracked
        );

        assert_eq!(offers.close(7, targets), vec![peer(1), peer(2)]);
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
        let quoted = root_quoted_peers(&proof).expect("a root proof");
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
}

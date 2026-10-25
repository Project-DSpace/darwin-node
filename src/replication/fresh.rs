//! Fresh replication (Section 6.1).
//!
//! When a node accepts a newly written record with valid `PoP`:
//! 1. Store locally (already done by chunk handler).
//! 2. Record the key in `PaidForList(self)` and send `PaidNotify` to every
//!    peer in `PaidCloseGroup(K)` — immediately, never behind back-pressure.
//! 3. Send fresh offers to `CLOSE_GROUP_SIZE` nearest peers (excluding self),
//!    one more for each peer marked full among them (at most 20 in all),
//!    bounded by the pending-offer permits so a write burst cannot pile up
//!    chunk-sized buffers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::logging::{debug, warn};
use bytes::Bytes;
use rand::Rng;
use saorsa_core::identity::PeerId;
use saorsa_core::P2PNode;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use crate::ant_protocol::XorName;
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
    // self out. One more for each peer marked full inside it: the
    // nearest beyond stand in for it, at most 20 in all.
    let widened = crate::replication::full::widened_close_group(
        &ctx.p2p_node,
        key,
        ctx.config.close_group_size,
    )
    .await;
    // The stand-ins: past the unwidened storage admission width (9 nearest,
    // the node itself counted), where a receiver that doesn't widen (an
    // older node, or one without the same marks) rightly refuses the offer.
    // They are offered the record but not possession-checked, as no peer
    // past the 9 nearest was before: nothing is waived for them, and one
    // that does take the record is, by intent, not recorded as seen holding
    // it.
    let checked_peers = possession_checked(&widened, &self_id, ctx.config.close_group_size);
    let target_peers: Vec<PeerId> = widened
        .into_iter()
        .filter(|peer| *peer != self_id)
        .collect();
    // The paid quotes' issuers, for the possession check: a full peer paid for
    // the record is charged for not holding it.
    let paid_quote_issuers = crate::replication::full::paid_quote_issuers(&proof_of_payment);

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
    // close-group peers, the stand-ins left out. A closed receiver (engine
    // shutting down) is ignored.
    if !checked_peers.is_empty() {
        ctx.dispatched.fetch_add(1, Ordering::Relaxed);
        let _ = ctx.possession_check_tx.send(PossessionCheckEvent {
            key: *key,
            peers: checked_peers,
            paid_quote_issuers,
        });
    }
}

/// The peers of `widened`, a key's close group widened past the peers marked
/// full (nearest first, this node included), that the possession check
/// probes: those inside the unwidened storage admission width, this node
/// left out. The stand-ins beyond it, which a receiver that doesn't widen
/// refuses, are offered the record but not checked.
fn possession_checked(
    widened: &[PeerId],
    self_id: &PeerId,
    close_group_size: usize,
) -> Vec<PeerId> {
    widened
        .iter()
        .take(crate::replication::config::storage_admission_width(
            close_group_size,
        ))
        .filter(|peer| *peer != self_id)
        .copied()
        .collect()
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

    /// Ranks 1 to 9 of the widened group are inside every receiver's
    /// admission width and are possession-checked; ranks 10 and beyond are
    /// stand-ins, offered the record but not checked. The node itself is
    /// never checked.
    #[test]
    fn only_the_nine_nearest_are_possession_checked_and_stand_ins_are_not() {
        let peers: Vec<PeerId> = (1..=12u8).map(|n| PeerId::from_bytes([n; 32])).collect();
        let outside = PeerId::from_bytes([0xEE; 32]);
        assert_eq!(possession_checked(&peers, &outside, 7), peers[..9].to_vec());
        assert_eq!(
            possession_checked(&peers[..7], &outside, 7),
            peers[..7].to_vec()
        );
        // This node at rank 3: the other eight of the nine nearest.
        let mut expected = peers[..9].to_vec();
        expected.remove(2);
        assert_eq!(possession_checked(&peers, &peers[2], 7), expected);
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
}

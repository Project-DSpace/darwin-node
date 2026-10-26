//! Fresh replication (Section 6.1).
//!
//! When a node accepts a newly written record with valid `PoP`:
//! 1. Store locally (already done by chunk handler).
//! 2. Record the key in `PaidForList(self)` and send `PaidNotify` to every
//!    peer in `PaidCloseGroup(K)` — immediately, never behind back-pressure.
//!    Close-group peers get a holding notice (an empty proof: "I hold K"),
//!    which every release ignores; the rest get the slim proof. A storer whose
//!    own paid-list write failed sends the slim proof to all of them.
//! 3. Send fresh offers to `CLOSE_GROUP_SIZE` nearest peers (excluding self),
//!    bounded by the pending-offer permits so a write burst cannot pile up
//!    chunk-sized buffers.

use std::collections::{HashMap, HashSet};
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
) {
    // Rule 6: Node that validates PoP adds K to PaidForList(self).
    let recorded = match paid_list.insert(key).await {
        Ok(_) => true,
        Err(e) => {
            warn!("Failed to add key {} to PaidForList: {e}", hex::encode(key));
            false
        }
    };
    // Rules 7-8: PaidNotify to every member of PaidCloseGroup(K).
    send_paid_notices(key, slim_proof, recorded, p2p_node, config).await;
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
    send_notice(key, Vec::new(), close_group.iter(), p2p_node);
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
    // close-group peers. A closed receiver (engine shutting down) is ignored.
    if !target_peers.is_empty() {
        ctx.dispatched.fetch_add(1, Ordering::Relaxed);
        let _ = ctx.possession_check_tx.send(PossessionCheckEvent {
            key: *key,
            peers: target_peers,
        });
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
) {
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
    send_notice(key, Vec::new(), holding.iter(), p2p_node);
    send_notice(key, slim_proof.to_vec(), with_proof.iter(), p2p_node);
}

/// Encode one `PaidNotify(K)` and send it to each peer, fire-and-forget. An
/// empty proof makes it a holding notice.
fn send_notice<'a>(
    key: &XorName,
    proof_of_payment: Vec<u8>,
    peers: impl Iterator<Item = &'a PeerId>,
    p2p_node: &Arc<P2PNode>,
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
}

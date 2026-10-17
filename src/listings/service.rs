//! Listings for the envelope chunks this node stores.
//!
//! A client PUT of an envelope carries its payment proof, so the node finds the
//! full payment on chain and records the listing as it stores the chunk,
//! keeping only the payment fields the envelope shows. A fresh replication
//! offer carries the same proof and is recorded the same way. Any valid
//! payment for the address passes that check, so a little later the node asks
//! the chunk's close group for its listing once, and takes the copy a quorum
//! of it serves instead if that differs, or dates the same listing earlier,
//! and checks out. If the chain can't tell yet whether it checks out, the
//! node asks again later.
//!
//! A chunk that arrives without a proof (repair) has none: the node asks the
//! chunk's close group for its listing. A listing that shows its quote is
//! checked on chain and taken from any node, preferring the copy a quorum of
//! the close group serves; one taken without a quorum is provisional, kept but
//! not served, until a later round finds a quorum or the rounds run out. One
//! that hides its quote can't be tied to the file on chain, so it is taken
//! only when a quorum of the close group return the same one, and its shown
//! fields still check out. A copy a quorum serves is dated so that a quorum
//! of the peers date it then or earlier, leaving out dates that fail their
//! own check ([`check_copies`]). Copies are checked as far as possible
//! without the chain before any is looked up on it, and the payment of a
//! copy fewer than a quorum serve is searched for only briefly. While the
//! chain can't be read for a copy that could be taken, the node keeps
//! asking, past the end of the rounds if need be. The envelopes still being
//! checked are kept in the store, so the checks go on after a restart.
//!
//! A listing is deleted with its chunk. Listings are served on
//! [`ant_listings::TOPIC`].

use super::chain::{listings_rpc_urls, Chain, Fetched, Search};
use super::store::ListingStore;
use crate::ant_protocol::{XorName, CLOSE_GROUP_SIZE};
use crate::error::{Error, Result};
use crate::logging::{debug, info, warn};
use crate::storage::ChunkStore;
use ::ant_protocol::evm::PaymentQuote;
use ::ant_protocol::payment::proof::deserialize_single_node_proof;
use ::ant_protocol::{detect_proof_type, ProofType};
use ant_listings::{
    check_copies, check_get_response, check_offline, choose_verified, find_payment, is_envelope,
    merge_copies, payment_lookup, quorum_listing, quorum_needed, verify_listing_receipts, Envelope,
    Listing, ListingKey, ListingsBody, ListingsMessage, MergedListing, Payment, PaymentError,
    PaymentField, MAX_LIST_LIMIT, TOPIC,
};
use parking_lot::{Mutex, RwLock};
use saorsa_core::identity::PeerId;
use saorsa_core::P2PNode;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Semaphore};

/// How long after an envelope is stored the node checks its listing with the
/// close group. A client PUT records one straight away; this leaves time for
/// that, on this node and the others, before asking them.
const CHECK_DELAY: Duration = Duration::from_secs(30);

/// Waits between further attempts to fetch a missing listing.
const RETRY_DELAYS: [Duration; 3] = [
    Duration::from_mins(1),
    Duration::from_mins(10),
    Duration::from_hours(1),
];

/// The longest wait between the attempts that go on after [`RETRY_DELAYS`]
/// while the chain can't be read: each is twice the one before, up to this.
const MAX_UNREAD_DELAY: Duration = Duration::from_hours(6);

/// How long to wait for a peer to answer a listing request.
const PEER_TIMEOUT: Duration = Duration::from_secs(10);

/// The most envelopes repaired at once. Each repair asks the close group and
/// may look payments up on chain, and a node taking over a range can receive
/// thousands of envelopes together.
const MAX_REPAIRS: usize = 16;

/// The most distinct transactions of a payment proof read to find an
/// envelope's payment. A proof pays an envelope in one transaction, or a few,
/// but can name thousands.
const MAX_PROOF_TXS: usize = 4;

/// How long after starting the node checks its listings against its chunks,
/// so the check doesn't compete with joining the network.
const SWEEP_DELAY: Duration = Duration::from_mins(2);

/// How many listings that check reads at a time, and the pause between.
const SWEEP_BATCH: usize = 256;
const SWEEP_PAUSE: Duration = Duration::from_millis(100);

/// Listing requests sent to other nodes, waiting for their answers, by the
/// peer asked and the request id.
type Waiting = Mutex<HashMap<(PeerId, u64), oneshot::Sender<Option<Listing>>>>;

/// Records and serves the listings for the envelopes this node stores.
pub struct ListingService {
    store: ListingStore,
    chunks: Arc<ChunkStore>,
    chain: Chain,
    vault: [u8; 20],
    p2p: RwLock<Option<Arc<P2PNode>>>,
    /// The requests waiting for answers, which [`Self::handle_response`]
    /// hands them.
    waiting: Waiting,
    /// Bounds the repairs running at once.
    repairs: Semaphore,
    /// Envelopes whose fresh offer is being looked up, so offering one again
    /// doesn't start another lookup.
    fresh_offers: Mutex<HashSet<XorName>>,
    /// Tests only: the close group's answers, given in place of asking it.
    #[cfg(test)]
    close_group: Mutex<Option<CloseGroup>>,
}

/// Tests only: what [`ListingService::ask_close_group`] returns, and how
/// many times it was asked.
#[cfg(test)]
struct CloseGroup {
    asked: Vec<PeerId>,
    answers: Vec<(PeerId, Listing)>,
    times: usize,
}

/// What the close group's answers decide for an envelope held without a
/// listing.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Repair {
    /// Take this listing.
    Settled(Listing),
    /// Take this listing for now, without serving it: it shows its quote and
    /// checks out, but no quorum of the peers asked served it.
    Provisional(Listing),
    /// Nothing to take yet.
    Nothing,
    /// Nothing to take yet, and the chain couldn't be read for a copy fewer
    /// than a quorum of the peers asked served, which might have been taken.
    Unread,
}

/// How a check of an envelope's listing with the close group ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Checked {
    /// Nothing more to check.
    Done,
    /// Try again later: nothing could be taken yet, the listing taken has no
    /// quorum behind it, or the chain has nothing yet for the copy a quorum
    /// serves.
    Again,
    /// Try again later: the chain couldn't be read for the copy a quorum
    /// serves, or, with no listing held, for a copy fewer serve. The checks
    /// go on until the chain answers, past the end of the retry schedule if
    /// need be; until then the listing held stays as it is, a provisional
    /// one unserved.
    ChainUnread,
}

/// Whether this node holds a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held {
    Yes,
    /// There, but it can't be read now.
    Unreadable,
    /// Gone for good.
    Gone,
}

impl ListingService {
    /// Open the listing store under `root_dir`, leaving `disk_reserve` bytes
    /// of the disk free, and start watching `chunks` for newly stored
    /// envelopes and deleted chunks. `rpc_url` and `vault` are the payment
    /// chain's RPC endpoint and payment vault.
    ///
    /// Must be called within a Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if the store can't be opened or the RPC client built.
    pub async fn new(
        root_dir: &Path,
        disk_reserve: u64,
        chunks: Arc<ChunkStore>,
        rpc_url: String,
        vault: [u8; 20],
    ) -> Result<Arc<Self>> {
        let store = ListingStore::open(root_dir, disk_reserve).await?;
        let service = Arc::new(Self {
            store,
            chunks: Arc::clone(&chunks),
            chain: Chain::with_rpcs(listings_rpc_urls(&rpc_url))?,
            vault,
            p2p: RwLock::new(None),
            waiting: Waiting::default(),
            repairs: Semaphore::new(MAX_REPAIRS),
            fresh_offers: Mutex::new(HashSet::new()),
            #[cfg(test)]
            close_group: Mutex::new(None),
        });

        let (envelopes_tx, envelopes_rx) = mpsc::unbounded_channel();
        chunks.set_stored_observer(Box::new(move |address, content| {
            if is_envelope(content) {
                let _ = envelopes_tx.send(*address);
            }
        }));
        let (removed_tx, removed_rx) = mpsc::unbounded_channel();
        chunks.set_removed_observer(Box::new(move |address| {
            let _ = removed_tx.send(*address);
        }));
        tokio::spawn(Self::watch_new_envelopes(
            Arc::downgrade(&service),
            envelopes_rx,
        ));
        tokio::spawn(Self::watch_removed_chunks(
            Arc::downgrade(&service),
            removed_rx,
        ));
        tokio::spawn(Self::resume(Arc::downgrade(&service)));
        Ok(service)
    }

    /// Attach the node's P2P handle, used to ask other nodes for listings.
    pub fn attach_p2p_node(&self, node: Arc<P2PNode>) {
        *self.p2p.write() = Some(node);
    }

    /// For a client PUT: the listing to record if `content` is an envelope,
    /// found from the payment proof that came with it, with only the payment
    /// fields the envelope shows. `Ok(None)` for any other chunk.
    ///
    /// # Errors
    ///
    /// Returns a message for the client if `content` is a malformed envelope,
    /// of an unknown version, or its payment can't be found. The PUT is then
    /// refused, so the client can retry with the right proof.
    pub async fn listing_for_put(
        &self,
        address: &XorName,
        content: &[u8],
        proof: Option<&[u8]>,
    ) -> std::result::Result<Option<Listing>, String> {
        if !is_envelope(content) {
            return Ok(None);
        }
        let envelope =
            Envelope::from_bytes(content).map_err(|e| format!("invalid envelope: {e}"))?;
        // A new upload: version 2's metadata rules, whatever the version.
        envelope
            .check_new_upload()
            .map_err(|e| format!("invalid envelope: {e}"))?;
        let proof = proof.ok_or("an envelope needs its payment proof")?;
        let payment = self.find_payment(address, proof).await?;
        Ok(Some(Listing::new(&envelope, &payment, unix_now())))
    }

    /// The full payment for `address` in a single-node payment proof, looked
    /// for in the proof's first [`MAX_PROOF_TXS`] distinct transactions.
    async fn find_payment(
        &self,
        address: &XorName,
        proof: &[u8],
    ) -> std::result::Result<Payment, String> {
        if detect_proof_type(proof) != Some(ProofType::SingleNode) {
            return Err("an envelope must be paid with a single-node payment".into());
        }
        let proof = deserialize_single_node_proof(proof)?;
        let quotes: Vec<PaymentQuote> = proof
            .proof_of_payment
            .peer_quotes
            .into_iter()
            .map(|(_, quote)| quote)
            .collect();

        let mut seen = HashSet::new();
        let tx_hashes = proof
            .tx_hashes
            .iter()
            .map(|tx_hash| tx_hash.0)
            .filter(|tx_hash| seen.insert(*tx_hash))
            .take(MAX_PROOF_TXS);
        for tx_hash in tx_hashes {
            let payment = self
                .chain
                .payment(&tx_hash)
                .await
                .map_err(|e| format!("could not read the payment from the chain: {e}"))?;
            let Some((receipt, timestamp)) = payment else {
                debug!(
                    "Envelope payment {} not found on chain",
                    hex::encode(tx_hash)
                );
                continue;
            };
            match find_payment(address, &quotes, &receipt, timestamp, &self.vault) {
                Ok(payment) => return Ok(payment),
                Err(e) => debug!(
                    "Transaction {} does not pay for this envelope: {e}",
                    hex::encode(tx_hash)
                ),
            }
        }
        Err("none of the proof's transactions pays for this envelope".into())
    }

    /// For a fresh replication offer just stored, which carries the upload's
    /// payment proof as a client PUT does: record the listing of an envelope,
    /// in the background. Unlike a PUT, nothing is refused if the payment
    /// can't be found; repair asks the close group instead. While one offer of
    /// an envelope is being looked up, others are ignored.
    pub fn record_from_fresh_offer(
        self: &Arc<Self>,
        address: XorName,
        content: &[u8],
        proof: &[u8],
    ) {
        if !is_envelope(content) || !self.fresh_offers.lock().insert(address) {
            return;
        }
        let service = Arc::clone(self);
        let content = content.to_vec();
        let proof = proof.to_vec();
        tokio::spawn(async move {
            service.record_offered(address, &content, &proof).await;
            service.fresh_offers.lock().remove(&address);
        });
    }

    async fn record_offered(&self, address: XorName, content: &[u8], proof: &[u8]) {
        if matches!(self.store.contains(address).await, Ok(true)) {
            return;
        }
        match self.listing_for_put(&address, content, Some(proof)).await {
            Ok(Some(listing)) => {
                self.record(listing).await;
            }
            Ok(None) => {}
            Err(e) => debug!(
                "No listing from the fresh offer of {}: {e}",
                hex::encode(address)
            ),
        }
    }

    /// Keep a listing found for a client PUT, once its chunk is stored.
    /// Returns whether it was recorded: the first listing of an envelope
    /// stands. A listing the store can't take now is left to the check that
    /// follows every stored envelope, which asks the close group.
    pub async fn record(&self, listing: Listing) -> bool {
        let address = listing.address;
        match self.keep(listing, false).await {
            Ok(kept) => kept,
            Err(e) => {
                warn!(
                    "Could not record the listing of {}: {e}",
                    hex::encode(address)
                );
                false
            }
        }
    }

    /// Keep a listing, provisional or settled, unless the envelope already
    /// has one. Returns whether it was kept: `false` if another listing came
    /// first or the chunk has gone.
    ///
    /// # Errors
    ///
    /// Returns an error if the store can't take it now.
    async fn keep(&self, listing: Listing, provisional: bool) -> Result<bool> {
        let address = listing.address;
        let inserted = if provisional {
            self.store.insert_provisional(listing).await?
        } else {
            self.store.insert(listing).await?
        };
        if !inserted {
            debug!("{} already has a listing", hex::encode(address));
            return Ok(false);
        }
        if provisional {
            info!("Recorded a provisional listing of {}", hex::encode(address));
        } else {
            info!("Recorded the listing of {}", hex::encode(address));
        }
        // The chunk may have been deleted while the listing was found.
        Ok(!self.drop_if_gone(&address).await)
    }

    /// Answer a message received on [`TOPIC`]. Returns the encoded response,
    /// or `None` for messages that need none, such as responses to this node's
    /// own requests.
    pub async fn handle_message(&self, data: &[u8]) -> Option<Vec<u8>> {
        let message = match ListingsMessage::decode(data) {
            Ok(message) => message,
            Err(e) => {
                debug!("Ignoring an undecodable listings message: {e}");
                return None;
            }
        };
        let body = match message.body {
            ListingsBody::ListRequest { from, limit } => self.list(from, limit).await,
            ListingsBody::GetRequest { address } => ListingsBody::GetResponse {
                listing: self.held_listing(address).await,
            },
            _ => return None,
        };
        ListingsMessage {
            request_id: message.request_id,
            body,
        }
        .encode()
        .map_err(|e| warn!("Could not encode a listings response: {e}"))
        .ok()
    }

    /// Take a message `source` sent on [`TOPIC`] if it needs no answer,
    /// handing an answer to one of this node's own requests to the request
    /// waiting for it. Returns `false` for a request, which
    /// [`Self::handle_message`] answers.
    pub fn handle_response(&self, source: &PeerId, data: &[u8]) -> bool {
        let message = match ListingsMessage::decode(data) {
            Ok(message) => message,
            Err(e) => {
                debug!("Ignoring an undecodable listings message from {source}: {e}");
                return true;
            }
        };
        match message.body {
            ListingsBody::ListRequest { .. } | ListingsBody::GetRequest { .. } => false,
            ListingsBody::GetResponse { listing } => {
                route_response(&self.waiting, source, message.request_id, listing);
                true
            }
            // Nodes send no list requests: nothing waits for these.
            ListingsBody::ListResponse { .. } | ListingsBody::Error { .. } => true,
        }
    }

    /// Listings in the index from `from` on. Listings outside the index are
    /// never listed, nor are provisional ones. Reads on past listings whose
    /// chunk has gone, so a page comes back empty only at the end.
    async fn list(&self, from: ListingKey, limit: u32) -> ListingsBody {
        let limit = usize::try_from(limit.min(MAX_LIST_LIMIT)).unwrap_or(0);
        let mut held = Vec::with_capacity(limit);
        let mut more = false;
        let mut next = Some(from);
        while let Some(from) = next {
            let (listings, later) = match self.store.range(from, limit - held.len()).await {
                Ok(page) => page,
                Err(e) => {
                    warn!("Could not read listings: {e}");
                    return ListingsBody::Error {
                        message: "listings are unavailable".into(),
                    };
                }
            };
            more = later;
            next = if later {
                listings.last().and_then(|last| last.key().checked_next())
            } else {
                None
            };
            for listing in listings {
                if self.still_held(&listing.address).await {
                    held.push(listing);
                }
            }
            if held.len() >= limit {
                break;
            }
        }
        ListingsBody::ListResponse {
            listings: held,
            more,
        }
    }

    /// The listing for `address`, in the index or not, if this node holds both
    /// it and the chunk, and the listing isn't provisional.
    async fn held_listing(&self, address: XorName) -> Option<Listing> {
        let listing = match self.store.served(address).await {
            Ok(listing) => listing?,
            Err(e) => {
                warn!(
                    "Could not read the listing of {}: {e}",
                    hex::encode(address)
                );
                return None;
            }
        };
        self.still_held(&address).await.then_some(listing)
    }

    /// Whether this node holds the chunk at `address`. The chunk store's own
    /// answer leaves out chunks it can't read at the moment, so a chunk it
    /// doesn't claim is read to tell those from chunks that have gone.
    async fn held(&self, address: &XorName) -> Held {
        match self.chunks.exists(address) {
            // When in doubt, keep the listing: the next request asks again.
            Ok(true) | Err(_) => Held::Yes,
            Ok(false) => match self.chunks.get_or_quarantine(address).await {
                Ok(Some(_)) => Held::Yes,
                Ok(None) => Held::Gone,
                Err(_) => Held::Unreadable,
            },
        }
    }

    /// Whether the listing of `address` can be served now, its chunk held. A
    /// listing whose chunk has gone without its removal being reported is
    /// dropped here; one whose chunk can't be read now is kept, but not
    /// served.
    async fn still_held(&self, address: &XorName) -> bool {
        match self.held(address).await {
            Held::Yes => true,
            Held::Unreadable => false,
            Held::Gone => {
                self.forget(address).await;
                false
            }
        }
    }

    /// Drop the listing of `address` if its chunk has gone. Returns whether
    /// it did.
    async fn drop_if_gone(&self, address: &XorName) -> bool {
        if self.held(address).await != Held::Gone {
            return false;
        }
        self.forget(address).await;
        true
    }

    /// Remove the listing of a chunk this node no longer holds.
    async fn forget(&self, address: &XorName) {
        match self.store.remove(*address).await {
            Ok(true) => debug!(
                "Dropped the listing of {} with its chunk",
                hex::encode(address)
            ),
            Ok(false) => {}
            Err(e) => warn!(
                "Could not drop the listing of {}: {e}",
                hex::encode(address)
            ),
        }
    }

    /// Delete the listing of each chunk the node deletes.
    async fn watch_removed_chunks(
        service: Weak<Self>,
        mut removed: mpsc::UnboundedReceiver<XorName>,
    ) {
        while let Some(address) = removed.recv().await {
            let Some(listings) = service.upgrade() else {
                return;
            };
            // Most chunks are not envelopes. One stored again since keeps its
            // listing: it still describes it.
            if matches!(listings.store.contains(address).await, Ok(true)) {
                listings.drop_if_gone(&address).await;
            }
        }
    }

    /// After a start: go on checking the envelopes a previous run was still
    /// checking with the close group, including the listings it left
    /// provisional, then drop the listings whose chunk went without the
    /// removal reaching them (the node stopped first).
    async fn resume(service: Weak<Self>) {
        let Some(listings) = service.upgrade() else {
            return;
        };
        let mut addresses = BTreeSet::new();
        match listings.store.provisional_addresses().await {
            Ok(provisional) => addresses.extend(provisional),
            Err(e) => warn!("Could not read the provisional listings: {e}"),
        }
        match listings.store.pending_addresses().await {
            Ok(pending) => addresses.extend(pending),
            Err(e) => warn!("Could not read the envelopes still being checked: {e}"),
        }
        drop(listings);
        for address in addresses {
            tokio::spawn(Self::ensure_listing_with_retries(service.clone(), address));
        }
        tokio::time::sleep(SWEEP_DELAY).await;
        Self::sweep(service).await;
    }

    /// Drop every listing whose chunk has gone, a batch at a time.
    async fn sweep(service: Weak<Self>) {
        let mut after = None;
        let mut dropped = 0usize;
        loop {
            let Some(listings) = service.upgrade() else {
                return;
            };
            let batch = match listings.store.addresses_after(after, SWEEP_BATCH).await {
                Ok(batch) => batch,
                Err(e) => {
                    warn!("Could not check the listings against the chunks: {e}");
                    return;
                }
            };
            let Some(&last) = batch.last() else {
                break;
            };
            for address in &batch {
                if listings.drop_if_gone(address).await {
                    dropped += 1;
                }
            }
            after = Some(last);
            drop(listings);
            tokio::time::sleep(SWEEP_PAUSE).await;
        }
        if dropped > 0 {
            info!("Dropped {dropped} listing(s) whose chunk has gone");
        }
    }

    /// Check the listing of every newly stored envelope with the close group,
    /// a little after it lands.
    async fn watch_new_envelopes(
        service: Weak<Self>,
        mut envelopes: mpsc::UnboundedReceiver<XorName>,
    ) {
        while let Some(address) = envelopes.recv().await {
            let Some(listings) = service.upgrade() else {
                return;
            };
            // Kept until the check is done, so a restart doesn't cut it short.
            if let Err(e) = listings.store.mark_pending(address).await {
                warn!(
                    "Could not keep envelope {} for checking after a restart: {e}",
                    hex::encode(address)
                );
            }
            drop(listings);
            tokio::spawn(Self::ensure_listing_with_retries(service.clone(), address));
        }
    }

    /// Run [`Self::ensure_listing`] on the retry schedule until it is done or
    /// the schedule runs out. While the chain can't be read
    /// ([`Checked::ChainUnread`]), the attempts go on past the end of the
    /// schedule, each wait twice the one before, up to [`MAX_UNREAD_DELAY`],
    /// so an outage of the chain's endpoint doesn't end them.
    async fn ensure_listing_with_retries(service: Weak<Self>, address: XorName) {
        tokio::time::sleep(CHECK_DELAY).await;
        let mut retries = RETRY_DELAYS.iter().copied();
        let mut delay = CHECK_DELAY;
        let mut overdue = false;
        loop {
            let Some(listings) = service.upgrade() else {
                return;
            };
            let outcome = match listings.repairs.acquire().await {
                Ok(_repairing) => listings.ensure_listing(&address).await,
                Err(_) => return,
            };
            let chain_unread = match outcome {
                Ok(Checked::Done) => {
                    listings.done_checking(&address).await;
                    return;
                }
                Ok(Checked::Again) => false,
                Ok(Checked::ChainUnread) => true,
                Err(e) => {
                    debug!("Listing check for {} failed: {e}", hex::encode(address));
                    false
                }
            };
            delay = match retries.next() {
                Some(next) => next,
                None if chain_unread => {
                    if !overdue {
                        warn!(
                            "Could not read the payment chain to check the listing of {}; \
                             checking it again until it can",
                            hex::encode(address)
                        );
                        overdue = true;
                    }
                    delay.saturating_mul(2).min(MAX_UNREAD_DELAY)
                }
                None => {
                    listings.give_up(&address).await;
                    return;
                }
            };
            drop(listings);
            tokio::time::sleep(delay).await;
        }
    }

    /// After the last attempt for `address`, one that could read the chain:
    /// a provisional listing is settled, and the envelope is no longer
    /// checked.
    async fn give_up(&self, address: &XorName) {
        match self.store.settle(*address).await {
            Ok(true) => info!(
                "Keeping the listing of {}: it checks out on chain, though no \
                 quorum of the close group served it",
                hex::encode(address)
            ),
            Ok(false) => {
                if matches!(self.store.contains(*address).await, Ok(false)) {
                    warn!(
                        "The close group had no listing to take for envelope {}",
                        hex::encode(address)
                    );
                }
            }
            Err(e) => warn!(
                "Could not settle the listing of {}: {e}",
                hex::encode(address)
            ),
        }
        self.done_checking(address).await;
    }

    /// Clear the mark that would have the envelope at `address` checked
    /// again after a restart.
    async fn done_checking(&self, address: &XorName) {
        if let Err(e) = self.store.clear_pending(*address).await {
            warn!(
                "Could not mark envelope {} as checked: {e}",
                hex::encode(address)
            );
        }
    }

    /// Make sure an envelope this node stores has a listing the close group
    /// agrees with. One without a listing, or with a provisional one, takes
    /// what the close group decides; one recorded from an upload's proof is
    /// checked against it, once unless the chain can't tell (see
    /// [`Checked`]).
    async fn ensure_listing(&self, address: &XorName) -> Result<Checked> {
        let provisional = self.store.is_provisional(*address).await?;
        let held = if !provisional && self.store.contains(*address).await? {
            match self.store.served(*address).await? {
                Some(listing) => Some(listing),
                // A record that doesn't decode: left as it is.
                None => return Ok(Checked::Done),
            }
        } else {
            None
        };
        let Some(content) = self.chunks.get_or_quarantine(address).await? else {
            // Gone: so is any listing it had.
            self.forget(address).await;
            return Ok(Checked::Done);
        };
        // Not `check_new_upload`: the envelope may predate version 2.
        let envelope = match Envelope::from_bytes(&content) {
            Ok(envelope) => envelope,
            Err(e) => {
                debug!(
                    "Stored chunk {} is not a valid envelope: {e}",
                    hex::encode(address)
                );
                return Ok(Checked::Done);
            }
        };
        let (asked, answers) = self.ask_close_group(address).await?;
        if let Some(held) = held {
            return self.confirm(&envelope, &held, &asked, answers).await;
        }
        let repair = match self.repair_from(&envelope, &asked, answers).await {
            Ok(repair) => repair,
            Err(e) => {
                debug!(
                    "Could not look up the listing a quorum of the close group serves for {}: {e}",
                    hex::encode(address)
                );
                return Ok(Checked::ChainUnread);
            }
        };
        // A provisional listing held checked out on chain before: a copy the
        // chain can't be read for now doesn't hold back settling it.
        let unread = repair == Repair::Unread && !provisional;
        Ok(if self.apply_repair(address, provisional, repair).await? {
            Checked::Done
        } else if unread {
            Checked::ChainUnread
        } else {
            Checked::Again
        })
    }

    /// Ask the peers nearest `address`, not counting this node, for their
    /// listing of it: the peers asked, and the usable answers.
    async fn ask_close_group(
        &self,
        address: &XorName,
    ) -> Result<(Vec<PeerId>, Vec<(PeerId, Listing)>)> {
        #[cfg(test)]
        if let Some(close_group) = self.close_group.lock().as_mut() {
            close_group.times += 1;
            return Ok((close_group.asked.clone(), close_group.answers.clone()));
        }
        let p2p = self
            .p2p
            .read()
            .clone()
            .ok_or_else(|| Error::Network("no P2P node attached".into()))?;
        let self_id = *p2p.peer_id();
        // Upstream's quorum targets: the nearest peers, not counting this node.
        let asked: Vec<PeerId> = p2p
            .dht_manager()
            .find_closest_nodes_local(address, CLOSE_GROUP_SIZE)
            .await
            .into_iter()
            .map(|node| node.peer_id)
            .filter(|peer| *peer != self_id)
            .collect();
        let responses = futures::future::join_all(
            asked
                .iter()
                .map(|peer| request_listing(&p2p, &self.waiting, peer, address)),
        )
        .await;
        let answers = asked
            .iter()
            .zip(responses)
            .filter_map(
                |(peer, listing)| match check_get_response(address, listing) {
                    Ok(listing) => listing.map(|listing| (*peer, listing)),
                    Err(e) => {
                        debug!("Peer {peer} sent an unusable listing: {e}");
                        None
                    }
                },
            )
            .collect();
        Ok((asked, answers))
    }

    /// Check `held`, a listing recorded from an upload's proof, against the
    /// close group's answers. Any valid payment for the address passed that
    /// check, so a node given another one, outside the close group or before
    /// joining it, must not keep a listing the close group doesn't serve: the
    /// copy a quorum of the peers `asked` serve ([`quorum_listing`]) replaces
    /// it if it differs, or dates the same listing earlier, and checks out,
    /// dated as [`check_copies`] dates it. Otherwise it stays, and nothing is
    /// looked up on chain. A copy the chain has nothing for yet, or can't be
    /// read for now, is asked about again later.
    ///
    /// # Errors
    ///
    /// Returns an error if the store can't be read or take the copy now.
    async fn confirm(
        &self,
        envelope: &Envelope,
        held: &Listing,
        asked: &[PeerId],
        answers: Vec<(PeerId, Listing)>,
    ) -> Result<Checked> {
        let groups = merge_copies(answers);
        let needed = quorum_needed(asked.len());
        // At most one copy has a quorum: it needs more than half the peers.
        let Some((group, listing)) = groups
            .iter()
            .find_map(|group| Some((group, quorum_listing(group, asked, needed)?)))
        else {
            return Ok(Checked::Done);
        };
        if !gives_way(held, &listing) {
            return Ok(Checked::Done);
        }
        let fetched = match self.fetch_for(envelope, &listing, Search::Full).await {
            Ok(Some(fetched)) => fetched,
            Ok(None) => return Ok(Checked::Done),
            Err(e) => {
                debug!(
                    "Could not look up the listing a quorum of the close group serves for {}: {e}",
                    hex::encode(listing.address)
                );
                return Ok(Checked::ChainUnread);
            }
        };
        let now = unix_now();
        let checked = check_copies(group, asked, needed, |listing| {
            verify_listing_receipts(listing, envelope, &fetched, &self.vault, now)
        });
        match checked {
            // Its date can differ from the one looked up for: taking it
            // checks again that the held listing gives way.
            Ok((listing, _)) => {
                self.take_quorum_copy(listing).await?;
                Ok(Checked::Done)
            }
            // Not on chain yet, or a search for it that ran out.
            Err(PaymentError::NotFound) => Ok(Checked::Again),
            Err(e) => {
                debug!(
                    "The listing a quorum of the close group serves for {} doesn't check out: {e}",
                    hex::encode(listing.address)
                );
                Ok(Checked::Done)
            }
        }
    }

    /// Put `listing`, the copy a quorum of the close group serves, checked,
    /// in place of the listing held for its address, if that gives way to it
    /// ([`gives_way`]) or is provisional. Returns whether it did.
    ///
    /// # Errors
    ///
    /// Returns an error if the store can't be read or take it now.
    async fn take_quorum_copy(&self, listing: Listing) -> Result<bool> {
        let address = listing.address;
        let replaceable = match self.store.served(address).await? {
            Some(held) => gives_way(&held, &listing),
            // Provisional; or no listing, the chunk gone; or a record that
            // doesn't decode, left as it is.
            None => self.store.is_provisional(address).await?,
        };
        if !replaceable || !self.store.replace(listing, false).await? {
            return Ok(false);
        }
        info!(
            "Took the listing a quorum of the close group serves for {} in place of \
             the one held",
            hex::encode(address)
        );
        // The chunk may have been deleted while the close group was asked.
        self.drop_if_gone(&address).await;
        Ok(true)
    }

    /// Keep what the close group decided for `address`, whose listing is
    /// held provisionally now if `provisional`. Returns `false` to try again
    /// later.
    async fn apply_repair(
        &self,
        address: &XorName,
        provisional: bool,
        repair: Repair,
    ) -> Result<bool> {
        match repair {
            Repair::Settled(listing) => {
                if provisional {
                    if self.store.replace(listing, false).await? {
                        info!(
                            "A quorum of the close group confirmed the listing of {}",
                            hex::encode(address)
                        );
                    }
                    // The chunk may have been deleted while the close group
                    // was asked.
                    self.drop_if_gone(address).await;
                } else if !self.keep(listing.clone(), false).await? {
                    // Another listing came first, recorded from a proof while
                    // the close group was asked, or the chunk has gone. This
                    // was the close-group check that listing is due, so the
                    // quorum's copy still decides. A store error tries again
                    // later.
                    self.take_quorum_copy(listing).await?;
                }
                Ok(true)
            }
            Repair::Provisional(listing) => {
                if provisional {
                    self.store.replace(listing, true).await?;
                    Ok(self.drop_if_gone(address).await)
                } else if self.keep(listing, true).await? {
                    Ok(false)
                } else {
                    // Another listing came first, from an upload's proof, or
                    // the chunk has gone.
                    Ok(true)
                }
            }
            Repair::Nothing | Repair::Unread => Ok(false),
        }
    }

    /// Decide on the close group's answers, checking on chain the copies that
    /// could be taken. The copy a quorum serves is looked up first: if it
    /// checks out, it is taken whatever the others are, so they are never
    /// looked up. Any other copy can come from one peer, with a quote of its
    /// own dated whenever it likes, so the search for its payment is brief
    /// ([`Search::Brief`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the chain can't be read for the copy a quorum
    /// serves: the others are not decided on without it. If it can't be read
    /// for another copy and nothing is taken, the answer is
    /// [`Repair::Unread`].
    async fn repair_from(
        &self,
        envelope: &Envelope,
        asked: &[PeerId],
        answers: Vec<(PeerId, Listing)>,
    ) -> Result<Repair> {
        let groups = merge_copies(answers);
        let quote_shown = envelope.flags().shown.contains(PaymentField::Quote);
        let needed = quorum_needed(asked.len());
        let now = unix_now();
        // At most one copy has a quorum: it needs more than half the peers.
        let (quorum, others): (Vec<usize>, Vec<usize>) =
            (0..groups.len()).partition(|&i| groups[i].count_among(asked) >= needed);
        let mut fetched: Vec<Option<Fetched>> = vec![None; groups.len()];
        for &i in &quorum {
            if let Some(listing) = quorum_listing(&groups[i], asked, needed) {
                fetched[i] = self.fetch_for(envelope, &listing, Search::Full).await?;
            }
        }
        let mut unread = false;
        // Without the quote, a copy too few peers serve is never taken.
        if quote_shown {
            let decided = decide_repair(envelope, asked, &groups, &fetched, &self.vault, now);
            if matches!(decided, Repair::Settled(_)) {
                return Ok(decided);
            }
            for &i in &others {
                let Some(listing) = groups[i].listing_among(asked) else {
                    continue;
                };
                fetched[i] = match self.fetch_for(envelope, &listing, Search::Brief).await {
                    Ok(fetched) => fetched,
                    Err(e) => {
                        debug!(
                            "Could not look up a listing of {}: {e}",
                            hex::encode(listing.address)
                        );
                        unread = true;
                        None
                    }
                };
            }
        }
        let decided = decide_repair(envelope, asked, &groups, &fetched, &self.vault, now);
        Ok(if unread && decided == Repair::Nothing {
            Repair::Unread
        } else {
            decided
        })
    }

    /// What the chain has for `listing`, a copy the close group serves,
    /// fetched once, searching for the payment of a quote as far as `search`
    /// goes. `None` if the copy can't check out whatever the chain says.
    ///
    /// # Errors
    ///
    /// Returns an error if the chain can't be read now.
    async fn fetch_for(
        &self,
        envelope: &Envelope,
        listing: &Listing,
        search: Search,
    ) -> Result<Option<Fetched>> {
        if let Err(e) = check_offline(listing, envelope, &self.vault) {
            debug!(
                "A listing of {} is unusable: {e}",
                hex::encode(listing.address)
            );
            return Ok(None);
        }
        let Ok(lookup) = payment_lookup(listing) else {
            return Ok(None);
        };
        self.chain
            .fetch(&lookup, &listing.payment, &self.vault, search)
            .await
            .map(Some)
    }
}

/// Whether `held` gives way to `quorum`, a copy a quorum of the close group
/// serves: it is another listing, or the same one dated later. A node that
/// stored an envelope late, or was handed it with a later payment of the
/// address, would otherwise keep a later `first_seen` than the close group's.
fn gives_way(held: &Listing, quorum: &Listing) -> bool {
    !held.eq_apart_from_first_seen(quorum) || held.first_seen > quorum.first_seen
}

/// The repair rule, given the merged answers of the peers `asked` and what the
/// chain has for each group (`None`: not fetched).
///
/// - The envelope shows its quote: of the copies that check out, the one
///   [`choose_verified`] keeps. Settled if a quorum of `asked` served it,
///   otherwise provisional: a quoting node can pay a quote of its own before
///   the uploader, so the close group's copy wins when there is one.
/// - It hides its quote: a copy at least [`quorum_needed`] of `asked` served,
///   whose shown fields check out.
///
/// Each copy is checked and dated by [`check_copies`]: one a quorum serves as
/// [`quorum_listing`] dates it, over the dates that pass their own check; any
/// other by the median of its peers' dates that check out.
fn decide_repair<P: PartialEq>(
    envelope: &Envelope,
    asked: &[P],
    groups: &[MergedListing<P>],
    fetched: &[Option<Fetched>],
    vault: &[u8; 20],
    now: u64,
) -> Repair {
    let needed = quorum_needed(asked.len());
    let check = |group: &MergedListing<P>, fetched: &Fetched| {
        check_copies(group, asked, needed, |listing| {
            verify_listing_receipts(listing, envelope, fetched, vault, now)
        })
    };
    if envelope.flags().shown.contains(PaymentField::Quote) {
        let checked: Vec<_> = groups
            .iter()
            .zip(fetched)
            .filter_map(|(group, fetched)| match check(group, fetched.as_ref()?) {
                Ok((listing, checked)) => Some((group, listing, checked)),
                Err(e) => {
                    debug!(
                        "A listing of {} doesn't check out: {e}",
                        hex::encode(group.address())
                    );
                    None
                }
            })
            .collect();
        return match choose_verified(checked, asked) {
            Some(chosen) if chosen.quorum => Repair::Settled(chosen.item),
            Some(chosen) => Repair::Provisional(chosen.item),
            None => Repair::Nothing,
        };
    }
    for (group, fetched) in groups.iter().zip(fetched) {
        if group.count_among(asked) < needed {
            continue;
        }
        let Some(fetched) = fetched else {
            continue;
        };
        match check(group, fetched) {
            Ok((listing, _)) => return Repair::Settled(listing),
            Err(e) => debug!(
                "A listing of {} that a quorum served doesn't check out: {e}",
                hex::encode(group.address())
            ),
        }
    }
    Repair::Nothing
}

/// Now, in Unix seconds.
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// Ask `peer` for its listing of `address`. `None` if it has none or doesn't
/// answer in time.
async fn request_listing(
    p2p: &P2PNode,
    waiting: &Waiting,
    peer: &PeerId,
    address: &XorName,
) -> Option<Listing> {
    let request_id = rand::random::<u64>();
    let request = ListingsMessage {
        request_id,
        body: ListingsBody::GetRequest { address: *address },
    }
    .encode()
    .ok()?;

    // Waiting before the request goes, so the answer can't be missed.
    let (answer, answered) = oneshot::channel();
    let _pending = Pending::register(waiting, (*peer, request_id), answer);
    p2p.send_message(peer, TOPIC, request, &[]).await.ok()?;
    tokio::time::timeout(PEER_TIMEOUT, answered)
        .await
        .ok()?
        .ok()?
}

/// A request waiting for its answer, until dropped.
struct Pending<'a> {
    waiting: &'a Waiting,
    key: (PeerId, u64),
}

impl<'a> Pending<'a> {
    fn register(
        waiting: &'a Waiting,
        key: (PeerId, u64),
        answer: oneshot::Sender<Option<Listing>>,
    ) -> Self {
        waiting.lock().insert(key, answer);
        Self { waiting, key }
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.waiting.lock().remove(&self.key);
    }
}

/// Hand `listing`, the answer `source` sent to request `request_id`, to the
/// request waiting for it, if one is.
fn route_response(waiting: &Waiting, source: &PeerId, request_id: u64, listing: Option<Listing>) {
    let answer = waiting.lock().remove(&(*source, request_id));
    if let Some(answer) = answer {
        let _ = answer.send(listing);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::super::chain::tests::{chain_with, vault_log, MockChain, VAULT};
    use super::super::store::tests::{signed_quote, signed_quote_at};
    use super::*;
    use crate::storage::ChunkStoreConfig;
    use ::ant_protocol::payment::proof::{serialize_single_node_proof, PaymentProof};
    use ant_listings::self_encryption::{ChunkInfo, DataMap, XorName as SeXorName};
    use ant_listings::{
        encode_data_map, first_seen_at, Flags, ListingPayment, Metadata, PaymentReceipt,
        ShownFields, MAGIC,
    };
    use tempfile::TempDir;
    use tokio::time::Instant;

    const BLOCK_TIME: u64 = 1_790_000_000;

    /// A chain endpoint nothing answers at.
    const DEAD_CHAIN: &str = "http://127.0.0.1:0";

    async fn service_on(dir: &TempDir, rpc_url: &str) -> (Arc<ListingService>, Arc<ChunkStore>) {
        let chunks = Arc::new(
            ChunkStore::new(ChunkStoreConfig {
                root_dir: dir.path().to_path_buf(),
                ..ChunkStoreConfig::test_default()
            })
            .await
            .unwrap(),
        );
        let listings =
            ListingService::new(dir.path(), 0, Arc::clone(&chunks), rpc_url.into(), VAULT)
                .await
                .unwrap();
        (listings, chunks)
    }

    /// A service whose chain is unreachable: for tests that never reach it.
    /// Every system refuses port 0 at once, so this works on a paused clock.
    async fn service(dir: &TempDir) -> (Arc<ListingService>, Arc<ChunkStore>) {
        service_on(dir, DEAD_CHAIN).await
    }

    fn data_map(seed: u8) -> DataMap {
        let infos = (0..3u8)
            .map(|index| ChunkInfo {
                index: usize::from(index),
                dst_hash: SeXorName([seed.wrapping_add(index); 32]),
                src_hash: SeXorName([seed.wrapping_add(index).wrapping_add(50); 32]),
                src_size: 10,
            })
            .collect();
        DataMap::new(infos)
    }

    fn metadata() -> Metadata {
        Metadata {
            content_type: Some("text/plain".into()),
            name: Some("a.txt".into()),
        }
    }

    fn envelope_with(seed: u8, shown: ShownFields, in_index: bool) -> Envelope {
        Envelope::readable(&data_map(seed), metadata(), shown, in_index).unwrap()
    }

    fn envelope(seed: u8) -> Envelope {
        envelope_with(seed, ShownFields::ALL, true)
    }

    /// The full payment of `envelope`, by a quote signed with test key `key`.
    fn payment(envelope: &Envelope, key: usize, block_number: u64) -> Payment {
        let quote = signed_quote(envelope.address(), key, 87 + block_number);
        Payment {
            tx_hash: [u8::try_from(block_number % 256).unwrap(); 32],
            block_number,
            block_timestamp: BLOCK_TIME + 2 * block_number,
            log_index: 4,
            quote: rmp_serde::to_vec(&quote).unwrap(),
        }
    }

    fn quote_hash(payment: &Payment) -> [u8; 32] {
        let quote: PaymentQuote = rmp_serde::from_slice(&payment.quote).unwrap();
        quote.hash().0
    }

    /// The receipt that pays `payment`, and its block's timestamp.
    fn paid(payment: &Payment) -> (Option<PaymentReceipt>, Option<u64>) {
        (
            Some(PaymentReceipt {
                tx_hash: payment.tx_hash,
                success: true,
                block_number: payment.block_number,
                logs: vec![vault_log(quote_hash(payment), payment.log_index)],
            }),
            Some(payment.block_timestamp),
        )
    }

    fn listing_for(envelope: &Envelope, block_number: u64) -> Listing {
        Listing::new(envelope, &payment(envelope, 0, block_number), BLOCK_TIME)
    }

    async fn ask(listings: &ListingService, body: ListingsBody) -> ListingsBody {
        let request = ListingsMessage {
            request_id: 42,
            body,
        }
        .encode()
        .unwrap();
        let response =
            ListingsMessage::decode(&listings.handle_message(&request).await.unwrap()).unwrap();
        assert_eq!(response.request_id, 42);
        response.body
    }

    async fn get(listings: &ListingService, address: XorName) -> Option<Listing> {
        let ListingsBody::GetResponse { listing } =
            ask(listings, ListingsBody::GetRequest { address }).await
        else {
            panic!("expected a get response");
        };
        listing
    }

    async fn list(listings: &ListingService, from: ListingKey, limit: u32) -> (Vec<Listing>, bool) {
        let ListingsBody::ListResponse { listings, more } =
            ask(listings, ListingsBody::ListRequest { from, limit }).await
        else {
            panic!("expected a list response");
        };
        (listings, more)
    }

    /// Wait for `condition`, checking every 20 ms. The background work the
    /// tests wait for writes to disk, and with the whole suite running at
    /// once a disk sync can take many seconds, so the deadline is far off:
    /// a test that passes waits only as long as it needs.
    async fn wait_until(mut condition: impl AsyncFnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !condition().await {
            assert!(Instant::now() < deadline, "timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn serves_listings_for_held_chunks_in_order() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let (a, b) = (envelope(1), envelope(100));
        chunks.put(&a.address(), &a.to_bytes()).await.unwrap();
        chunks.put(&b.address(), &b.to_bytes()).await.unwrap();
        listings.record(listing_for(&b, 20)).await;
        listings.record(listing_for(&a, 10)).await;

        let (page, more) = list(&listings, ListingKey::MIN, 10).await;
        assert!(!more);
        assert_eq!(
            page.iter().map(|l| l.address).collect::<Vec<_>>(),
            vec![a.address(), b.address()]
        );
        let (page, more) = list(&listings, ListingKey::MIN, 1).await;
        assert_eq!(page.len(), 1);
        assert!(more);

        let listing = get(&listings, b.address()).await.unwrap();
        assert_eq!(listing.payment.block_number, Some(20));
    }

    #[tokio::test]
    async fn listings_outside_the_index_are_got_but_never_listed() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let unlisted = envelope_with(1, ShownFields::ALL, false);
        let private = Envelope::withheld(
            &data_map(2),
            &[9; 32],
            Metadata::default(),
            ShownFields::NONE,
            false,
        )
        .unwrap();
        let mut recorded = Vec::new();
        for envelope in [&unlisted, &private] {
            chunks
                .put(&envelope.address(), &envelope.to_bytes())
                .await
                .unwrap();
            let listing = listing_for(envelope, 10);
            listings.record(listing.clone()).await;
            recorded.push(listing);
        }
        let (page, more) = list(&listings, ListingKey::MIN, 256).await;
        assert_eq!(page, Vec::<Listing>::new());
        assert!(!more);
        assert_eq!(
            get(&listings, unlisted.address()).await.as_ref(),
            recorded.first()
        );
        let private_listing = get(&listings, private.address()).await.unwrap();
        assert_eq!(private_listing.payment, ListingPayment::default());
        assert_eq!(private_listing.size, None);
    }

    #[tokio::test]
    async fn deletes_a_listing_with_its_chunk() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        // Outside the index: nothing would ever read it again.
        let a = envelope_with(1, ShownFields::ALL, false);
        chunks.put(&a.address(), &a.to_bytes()).await.unwrap();
        listings.record(listing_for(&a, 10)).await;
        assert!(listings.store.contains(a.address()).await.unwrap());

        chunks.delete(&a.address()).await.unwrap();
        let store = listings.store.clone();
        wait_until(async || !store.contains(a.address()).await.unwrap()).await;
        assert!(get(&listings, a.address()).await.is_none());
    }

    #[tokio::test]
    async fn drops_listings_whose_chunk_is_gone() {
        let dir = TempDir::new().unwrap();
        let (listings, _chunks) = service(&dir).await;
        // A listing whose chunk this node never stored.
        let a = envelope(1);
        listings.store.insert(listing_for(&a, 10)).await.unwrap();
        assert!(get(&listings, a.address()).await.is_none());
        assert!(!listings.store.contains(a.address()).await.unwrap());
    }

    #[tokio::test]
    async fn ignores_responses_and_garbage() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        let response = ListingsMessage {
            request_id: 1,
            body: ListingsBody::GetResponse { listing: None },
        }
        .encode()
        .unwrap();
        assert!(listings.handle_message(&response).await.is_none());
        assert!(listings.handle_message(&[0xff; 4]).await.is_none());
    }

    #[tokio::test]
    async fn puts_of_other_chunks_need_no_listing() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        assert_eq!(
            listings
                .listing_for_put(&[1; 32], b"ordinary chunk bytes", None)
                .await,
            Ok(None)
        );
    }

    #[tokio::test]
    async fn refuses_envelopes_without_a_usable_proof() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        let a = envelope(1);
        assert!(listings
            .listing_for_put(&a.address(), &a.to_bytes(), None)
            .await
            .is_err());
        assert!(listings
            .listing_for_put(&a.address(), &a.to_bytes(), Some(&[0xff, 1, 2]))
            .await
            .is_err());
        let mut broken = a.to_bytes();
        broken.truncate(12);
        assert!(listings
            .listing_for_put(&a.address(), &broken, None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn refuses_unknown_versions_and_names_version_2_forbids() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        let map = encode_data_map(&data_map(1)).unwrap();
        let mut unknown = MAGIC.to_vec();
        unknown.extend_from_slice(&[3, 0x3f, 0, 0]);
        unknown.extend_from_slice(&map);
        let error = listings
            .listing_for_put(&[1; 32], &unknown, None)
            .await
            .unwrap_err();
        assert!(error.contains("invalid envelope"), "{error}");

        // A version 1 envelope, built by hand, named with a slash look-alike.
        let name = "a\u{ff0f}b".as_bytes();
        let mut v1 = MAGIC.to_vec();
        v1.extend_from_slice(&[1, 0, u8::try_from(name.len()).unwrap()]);
        v1.extend_from_slice(name);
        v1.extend_from_slice(&map);
        assert!(Envelope::from_bytes(&v1).is_ok());
        let error = listings
            .listing_for_put(&[1; 32], &v1, None)
            .await
            .unwrap_err();
        assert!(error.contains("invalid envelope"), "{error}");
    }

    /// A single-node proof paying `quote` in `tx_hash`, among other quotes.
    fn single_node_proof(quote: &PaymentQuote, tx_hash: [u8; 32]) -> Vec<u8> {
        proof_naming(quote, &[tx_hash])
    }

    /// A single-node proof for `quote`, among other quotes, naming
    /// `tx_hashes`.
    fn proof_naming(quote: &PaymentQuote, tx_hashes: &[[u8; 32]]) -> Vec<u8> {
        let mut peer_quotes = vec![(evmlib::EncodedPeerId::new([1; 32]), quote.clone())];
        for (i, other) in (2u8..4).enumerate() {
            peer_quotes.push((
                evmlib::EncodedPeerId::new([other; 32]),
                signed_quote(quote.content.0, 1, 500 + u64::try_from(i).unwrap()),
            ));
        }
        serialize_single_node_proof(&PaymentProof {
            proof_of_payment: evmlib::ProofOfPayment { peer_quotes },
            tx_hashes: tx_hashes
                .iter()
                .map(|tx_hash| evmlib::common::TxHash::from(*tx_hash))
                .collect(),
            commitment_sidecars: Vec::new(),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn a_put_records_only_the_shown_fields() {
        // Block 300 pays the envelope's quote at log index 4.
        let shown = ShownFields::ALL
            .without(PaymentField::TxHash)
            .without(PaymentField::BlockTimestamp);
        let listed = envelope_with(1, shown, true);
        let quote = signed_quote(listed.address(), 0, 87);
        // The default upload: data map withheld, outside the index, all five
        // hidden. Block 310 pays its quote at log index 2.
        let private = Envelope::withheld(
            &data_map(2),
            &[3; 32],
            Metadata::default(),
            ShownFields::NONE,
            false,
        )
        .unwrap();
        let private_quote = signed_quote(private.address(), 0, 88);
        let mut chain = chain_with(400, &[]);
        chain.add_receipt(PaymentReceipt {
            tx_hash: [0x77; 32],
            success: true,
            block_number: 300,
            logs: vec![vault_log(quote.hash().0, 4)],
        });
        chain.add_receipt(PaymentReceipt {
            tx_hash: [0x78; 32],
            success: true,
            block_number: 310,
            logs: vec![vault_log(private_quote.hash().0, 2)],
        });
        let url = MockChain::serve(Arc::new(Mutex::new(chain))).await;
        let dir = TempDir::new().unwrap();
        let (listings, _) = service_on(&dir, &url).await;

        let proof = single_node_proof(&quote, [0x77; 32]);
        let listing = listings
            .listing_for_put(&listed.address(), &listed.to_bytes(), Some(&proof))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(listing.flags.shown, shown);
        assert_eq!(listing.payment.tx_hash, None);
        assert_eq!(listing.payment.block_timestamp, None);
        assert_eq!(listing.payment.block_number, Some(300));
        assert_eq!(listing.payment.log_index, Some(4));
        assert_eq!(
            listing.payment.quote,
            Some(rmp_serde::to_vec(&quote).unwrap())
        );
        // In the index with its time hidden: dated by the hour it was seen,
        // never before its block.
        let first_seen = listing.first_seen.unwrap();
        assert_eq!(first_seen % 3600, 0);
        assert!(first_seen >= first_seen_at(BLOCK_TIME + 600));
        assert!(listing.validate().is_ok());

        // The private envelope: its payment is found and checked in full, and
        // none of it recorded; no date, as it is outside the index; no size,
        // as its data map is withheld.
        let proof = single_node_proof(&private_quote, [0x78; 32]);
        let listing = listings
            .listing_for_put(&private.address(), &private.to_bytes(), Some(&proof))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(listing.address, private.address());
        assert_eq!(listing.payment, ListingPayment::default());
        assert_eq!(listing.first_seen, None);
        assert_eq!(listing.size, None);
        assert!(listing.flags.withheld && !listing.flags.in_index);
        assert_eq!(listing.flags.shown, ShownFields::NONE);
        assert!(listing.matches(&private));
        assert!(listing.validate().is_ok());

        // A proof whose tx doesn't pay its quote: refused, so the client
        // retries, though nothing of the payment would be recorded.
        let proof = single_node_proof(&private_quote, [0x77; 32]);
        assert!(listings
            .listing_for_put(&private.address(), &private.to_bytes(), Some(&proof))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_fresh_offer_records_the_listing_too() {
        let unlisted = envelope_with(1, ShownFields::ALL, false);
        let quote = signed_quote(unlisted.address(), 0, 87);
        let mut chain = chain_with(400, &[]);
        chain.add_receipt(PaymentReceipt {
            tx_hash: [0x55; 32],
            success: true,
            block_number: 12,
            logs: vec![vault_log(quote.hash().0, 1)],
        });
        let url = MockChain::serve(Arc::new(Mutex::new(chain))).await;
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_on(&dir, &url).await;
        chunks
            .put(&unlisted.address(), &unlisted.to_bytes())
            .await
            .unwrap();
        listings.record_from_fresh_offer(
            unlisted.address(),
            &unlisted.to_bytes(),
            &single_node_proof(&quote, [0x55; 32]),
        );
        let store = listings.store.clone();
        wait_until(async || store.contains(unlisted.address()).await.unwrap()).await;
        let listing = get(&listings, unlisted.address()).await.unwrap();
        assert_eq!(listing.payment.tx_hash, Some([0x55; 32]));
        assert_eq!(listing.payment.block_timestamp, Some(BLOCK_TIME + 24));
        assert_eq!(listing.first_seen, None);
    }

    /// Copies of `listing` from `peers`, each with its own `first_seen`.
    fn copies(listing: &Listing, peers: &[(u8, Option<u64>)]) -> Vec<(u8, Listing)> {
        peers
            .iter()
            .map(|&(peer, first_seen)| {
                let mut copy = listing.clone();
                copy.first_seen = first_seen;
                (peer, copy)
            })
            .collect()
    }

    const ASKED: [u8; 7] = [1, 2, 3, 4, 5, 6, 7];

    fn decide(
        envelope: &Envelope,
        answers: Vec<(u8, Listing)>,
        fetched_for: impl Fn(&MergedListing<u8>) -> Option<Fetched>,
    ) -> Repair {
        let groups = merge_copies(answers);
        let fetched: Vec<_> = groups.iter().map(fetched_for).collect();
        decide_repair(
            envelope,
            &ASKED,
            &groups,
            &fetched,
            &VAULT,
            BLOCK_TIME + 7200,
        )
    }

    #[test]
    fn a_listing_without_its_quote_needs_a_quorum() {
        let hidden = envelope_with(1, ShownFields::NONE, false);
        let listing = listing_for(&hidden, 10);
        let nothing_to_fetch = |_: &MergedListing<u8>| Some(Vec::new());

        // One node serving a listing nobody else has.
        let alone = copies(&listing, &[(1, None)]);
        assert_eq!(decide(&hidden, alone, nothing_to_fetch), Repair::Nothing);

        // Three agree: not enough of seven.
        let three = copies(&listing, &[(1, None), (2, None), (3, None)]);
        assert_eq!(decide(&hidden, three, nothing_to_fetch), Repair::Nothing);

        // Four agree: taken.
        let four = copies(&listing, &[(1, None), (2, None), (3, None), (4, None)]);
        assert_eq!(
            decide(&hidden, four, nothing_to_fetch),
            Repair::Settled(listing)
        );
    }

    #[test]
    fn three_nodes_serving_a_changed_listing_lose() {
        // Only the block shown: checked against the block's timestamp.
        let envelope = envelope_with(1, ShownFields::NONE.with(PaymentField::BlockNumber), false);
        let right = listing_for(&envelope, 10);
        let mut changed = right.clone();
        changed.payment.block_number = Some(11);
        let block_exists = |_: &MergedListing<u8>| Some(vec![(None, Some(BLOCK_TIME))]);

        let mut answers = copies(&changed, &[(1, None), (2, None), (3, None)]);
        answers.extend(copies(
            &right,
            &[(4, None), (5, None), (6, None), (7, None)],
        ));
        assert_eq!(
            decide(&envelope, answers, block_exists),
            Repair::Settled(right.clone())
        );

        // Three against three: neither is taken.
        let mut answers = copies(&changed, &[(1, None), (2, None), (3, None)]);
        answers.extend(copies(&right, &[(4, None), (5, None), (6, None)]));
        assert_eq!(decide(&envelope, answers, block_exists), Repair::Nothing);

        // A quorum for a listing whose shown block isn't on chain: not taken.
        let answers = copies(&changed, &[(1, None), (2, None), (3, None), (4, None)]);
        assert_eq!(
            decide(&envelope, answers, |_| Some(vec![(None, None)])),
            Repair::Nothing
        );
    }

    #[test]
    fn a_quorum_dates_a_listing_as_a_quorum_of_its_peers_do() {
        // In the index, nothing shown: dated by first_seen alone.
        let envelope = envelope_with(1, ShownFields::NONE, true);
        let listing = listing_for(&envelope, 10);
        let hour = first_seen_at(BLOCK_TIME);
        let dated_by = |dates: &[u64]| {
            let peers: Vec<(u8, Option<u64>)> = ASKED
                .iter()
                .zip(dates)
                .map(|(peer, date)| (*peer, Some(*date)))
                .collect();
            decide(&envelope, copies(&listing, &peers), |_| Some(Vec::new()))
        };
        let taken_on = |date| {
            let mut taken = listing.clone();
            taken.first_seen = Some(date);
            Repair::Settled(taken)
        };

        // All seven answer: their median.
        let seven = [0, 0, 1, 1, 2, 2, 3].map(|hours| hour + hours * 3600);
        assert_eq!(dated_by(&seven), taken_on(hour + 3600));

        // Fewer: the latest of the earliest four, so that four date it then
        // or earlier. One dating it in the future is not needed for that.
        let five = [0, 0, 1, 2, 100].map(|hours| hour + hours * 3600);
        assert_eq!(dated_by(&five), taken_on(hour + 7200));

        // Three dating it in 1970 can't move it before the earliest date
        // another gives, however few others answer.
        assert_eq!(dated_by(&[0, 0, 0, hour]), taken_on(hour));
        assert_eq!(dated_by(&[0, 0, 0, hour, hour]), taken_on(hour));
        assert_eq!(
            dated_by(&[0, 0, 0, hour, hour - 3600]),
            taken_on(hour - 3600)
        );

        // Four answer and one dates it in the future: that date is dropped,
        // so one peer can't fail a listing three others date honestly.
        assert_eq!(
            dated_by(&[hour, hour, hour, hour + 100 * 3600]),
            taken_on(hour)
        );
        // Three: not a quorum, so a listing hiding its quote isn't taken.
        assert_eq!(dated_by(&[hour, hour, hour]), Repair::Nothing);
    }

    #[test]
    fn dates_out_of_bounds_count_only_as_far_as_they_check_out() {
        // In the index, all shown but the block's time: dated by first_seen,
        // which the payment's block bounds from below.
        let envelope = envelope_with(
            1,
            ShownFields::ALL.without(PaymentField::BlockTimestamp),
            true,
        );
        let upload = payment(&envelope, 0, 10);
        let listing = Listing::new(&envelope, &upload, BLOCK_TIME);
        let hour = listing.first_seen.unwrap();
        let h = 3600;
        // Too early for any node to have recorded it, and in the future.
        let (early, future) = (hour - 10 * h, hour + 100 * h);
        let dated_by = |dates: &[u64]| {
            let peers: Vec<(u8, Option<u64>)> = ASKED
                .iter()
                .zip(dates)
                .map(|(peer, date)| (*peer, Some(*date)))
                .collect();
            decide(&envelope, copies(&listing, &peers), |_| {
                Some(vec![paid(&upload)])
            })
        };
        let dated = |first_seen| Listing {
            first_seen: Some(first_seen),
            ..listing.clone()
        };

        // Without a quorum each date is checked on its own and the median of
        // those that pass taken: one too early or in the future neither
        // fails the listing nor moves its date.
        assert_eq!(
            dated_by(&[early, hour, hour + h]),
            Repair::Provisional(dated(hour + h))
        );
        assert_eq!(
            dated_by(&[early, hour, future]),
            Repair::Provisional(dated(hour))
        );
        // With a quorum, dates that fail their own check are dropped first:
        // three dating it before its payment neither fail it nor date it.
        // Fewer than four dates are left, so it takes their median.
        assert_eq!(
            dated_by(&[early, early, early, hour, hour + h]),
            Repair::Settled(dated(hour + h))
        );
        // Where every date passes, the latest of the earliest four.
        assert_eq!(
            dated_by(&[hour, hour, hour, hour + h, hour + h]),
            Repair::Settled(dated(hour + h))
        );
        assert_eq!(
            dated_by(&[hour, hour, hour, hour, hour + h]),
            Repair::Settled(dated(hour))
        );
        // Four dating it before its payment, which no node can: those dates
        // are dropped, and the one that checks out dates it.
        assert_eq!(
            dated_by(&[early, early, early, early, hour]),
            Repair::Settled(dated(hour))
        );
        // None checks out: nothing is taken.
        assert_eq!(dated_by(&[early, early, early, future]), Repair::Nothing);
    }

    #[test]
    fn a_listing_with_its_quote_prefers_the_close_group() {
        let envelope = envelope(1);
        let upload = payment(&envelope, 0, 20);
        let listing = Listing::new(&envelope, &upload, BLOCK_TIME);
        // A quoting node's own quote, paid earlier.
        let front_run = payment(&envelope, 1, 10);
        let front_listing = Listing::new(&envelope, &front_run, BLOCK_TIME);
        let fetched = |group: &MergedListing<u8>| {
            let payment = if group.payment().tx_hash == Some(upload.tx_hash) {
                &upload
            } else {
                &front_run
            };
            Some(vec![paid(payment)])
        };

        // One node: taken, but only for now.
        let alone = copies(&listing, &[(1, None)]);
        assert_eq!(
            decide(&envelope, alone, fetched),
            Repair::Provisional(listing.clone())
        );

        // The close group's copy beats an earlier payment.
        let mut answers = copies(&front_listing, &[(1, None)]);
        answers.extend(copies(
            &listing,
            &[(2, None), (3, None), (4, None), (5, None)],
        ));
        assert_eq!(
            decide(&envelope, answers, fetched),
            Repair::Settled(listing.clone())
        );

        // Without a quorum, the earliest payment stands in.
        let mut answers = copies(&front_listing, &[(1, None)]);
        answers.extend(copies(&listing, &[(2, None)]));
        assert_eq!(
            decide(&envelope, answers, fetched),
            Repair::Provisional(front_listing)
        );

        // A copy whose payment isn't on chain is never taken.
        let alone = copies(&listing, &[(1, None), (2, None), (3, None), (4, None)]);
        assert_eq!(
            decide(&envelope, alone, |_| Some(Vec::new())),
            Repair::Nothing
        );
    }

    #[test]
    fn copies_that_dont_match_the_envelope_are_refused() {
        let envelope = envelope_with(1, ShownFields::NONE, false);
        let mut forged = listing_for(&envelope, 10);
        forged.metadata.name = Some("other.txt".into());
        let answers = copies(&forged, &[(1, None), (2, None), (3, None), (4, None)]);
        assert_eq!(
            decide(&envelope, answers, |_| Some(Vec::new())),
            Repair::Nothing
        );
        // Flags flipped into the index.
        let mut indexed = listing_for(&envelope, 10);
        indexed.flags = Flags {
            in_index: true,
            ..indexed.flags
        };
        let hour = Some(first_seen_at(BLOCK_TIME));
        let answers = copies(&indexed, &[(1, hour), (2, hour), (3, hour), (4, hour)]);
        assert_eq!(
            decide(&envelope, answers, |_| Some(Vec::new())),
            Repair::Nothing
        );
    }

    /// Calls the mock chain has answered for `method`.
    fn calls_to(chain: &Mutex<MockChain>, method: &str) -> usize {
        chain.lock().calls.iter().filter(|c| *c == method).count()
    }

    async fn serve(chain: MockChain) -> (Arc<Mutex<MockChain>>, String) {
        let chain = Arc::new(Mutex::new(chain));
        let url = MockChain::serve(Arc::clone(&chain)).await;
        (chain, url)
    }

    fn peers(count: u8) -> Vec<PeerId> {
        (1..=count).map(|i| PeerId::from_bytes([i; 32])).collect()
    }

    /// The bytes of a quote naming `address`, dated 1970, with a signature
    /// that isn't one.
    fn forged_quote(address: XorName) -> Vec<u8> {
        let mut quote = signed_quote(address, 0, 87);
        quote.timestamp = UNIX_EPOCH;
        quote.signature = vec![7; quote.signature.len()];
        rmp_serde::to_vec(&quote).unwrap()
    }

    #[tokio::test]
    async fn copies_that_cant_check_out_are_never_looked_up() {
        let (chain, url) = serve(chain_with(1000, &[])).await;
        let dir = TempDir::new().unwrap();
        let (listings, _) = service_on(&dir, &url).await;
        let asked = peers(7);
        let quote_only = ShownFields::NONE.with(PaymentField::Quote);

        // One peer's copy hides what its envelope shows, so the quote would
        // be searched for over the whole chain.
        let envelope = envelope(1);
        let mismatched = Listing {
            address: envelope.address(),
            flags: Flags {
                shown: quote_only,
                in_index: false,
                withheld: false,
            },
            metadata: metadata(),
            size: envelope.file_size(),
            payment: ListingPayment {
                quote: Some(forged_quote(envelope.address())),
                ..ListingPayment::default()
            },
            first_seen: None,
        };
        assert!(mismatched.validate().is_ok());
        assert_eq!(
            listings
                .repair_from(&envelope, &asked, vec![(asked[0], mismatched)])
                .await
                .unwrap(),
            Repair::Nothing
        );

        // Flags as the envelope's, the quote's signature forged.
        let envelope = envelope_with(2, quote_only, false);
        let mut forged = Listing::new(&envelope, &payment(&envelope, 0, 10), BLOCK_TIME);
        forged.payment.quote = Some(forged_quote(envelope.address()));
        assert_eq!(
            listings
                .repair_from(&envelope, &asked, vec![(asked[0], forged)])
                .await
                .unwrap(),
            Repair::Nothing
        );
        // As before, dated in the future by the one peer that sent it.
        let indexed = envelope_with(3, quote_only, true);
        let mut future = Listing::new(&indexed, &payment(&indexed, 0, 10), BLOCK_TIME);
        future.payment.quote = Some(forged_quote(indexed.address()));
        future.first_seen = Some(first_seen_at(u64::MAX / 2));
        assert_eq!(
            listings
                .repair_from(&indexed, &asked, vec![(asked[0], future)])
                .await
                .unwrap(),
            Repair::Nothing
        );
        assert!(chain.lock().calls.is_empty(), "{:?}", chain.lock().calls);

        // A signed quote nobody paid is looked up: it could have checked out.
        let unpaid = Listing::new(&envelope, &payment(&envelope, 1, 10), BLOCK_TIME);
        assert_eq!(
            listings
                .repair_from(&envelope, &asked, vec![(asked[0], unpaid)])
                .await
                .unwrap(),
            Repair::Nothing
        );
        assert!(calls_to(&chain, "eth_getLogs") > 0);
    }

    #[tokio::test]
    async fn a_copy_a_quorum_serves_is_looked_up_before_the_others() {
        // The envelope shows only its quote, paid in block 300.
        let envelope = envelope_with(1, ShownFields::NONE.with(PaymentField::Quote), false);
        let upload = payment(&envelope, 0, 300);
        let mut mock = chain_with(1000, &[]);
        let (receipt, _) = paid(&upload);
        mock.add_receipt(receipt.unwrap());
        let (chain, url) = serve(mock).await;
        let dir = TempDir::new().unwrap();
        let (listings, _) = service_on(&dir, &url).await;
        let asked = peers(7);
        let honest = Listing::new(&envelope, &upload, BLOCK_TIME);
        let mut answers: Vec<(PeerId, Listing)> = asked[..4]
            .iter()
            .map(|peer| (*peer, honest.clone()))
            .collect();
        assert_eq!(
            listings
                .repair_from(&envelope, &asked, answers.clone())
                .await
                .unwrap(),
            Repair::Settled(honest.clone())
        );
        let alone = std::mem::take(&mut chain.lock().calls);

        // A fifth peer's own quote, signed and never paid, would cost a
        // search of its own.
        let own = Listing::new(&envelope, &payment(&envelope, 1, 10), BLOCK_TIME);
        answers.push((asked[4], own));
        assert_eq!(
            listings
                .repair_from(&envelope, &asked, answers)
                .await
                .unwrap(),
            Repair::Settled(honest)
        );
        // The same lookup again, now with the blocks it read known.
        let again = chain.lock().calls.clone();
        let count = |calls: &[String], method| calls.iter().filter(|c| *c == method).count();
        for method in ["eth_getLogs", "eth_getTransactionReceipt"] {
            assert_eq!(count(&again, method), count(&alone, method), "{again:?}");
        }
        assert!(again.len() <= alone.len(), "{again:?}");
    }

    #[tokio::test]
    async fn a_quote_too_few_peers_serve_is_searched_for_briefly() {
        // The envelope shows only its quote, so a copy is looked up by its
        // quote's time: 50,000 blocks, with a provider that searches at most
        // 1,000 at a time.
        let envelope = envelope_with(1, ShownFields::NONE.with(PaymentField::Quote), false);
        let upload = payment(&envelope, 0, 300);
        let mut mock = chain_with(50_000, &[]);
        mock.add_receipt(paid(&upload).0.unwrap());
        mock.max_log_range = Some(1_000);
        let (chain, url) = serve(mock).await;
        let dir = TempDir::new().unwrap();
        let (listings, _) = service_on(&dir, &url).await;
        let asked = peers(7);
        let log_calls = || {
            let calls = std::mem::take(&mut chain.lock().calls);
            calls.iter().filter(|call| *call == "eth_getLogs").count()
        };
        // A peer's own quote for the address, signed, dated 1970 and never
        // paid: the search for its payment starts at block 0.
        let quote = signed_quote_at(envelope.address(), 1, 87, UNIX_EPOCH);
        let own = Payment {
            quote: rmp_serde::to_vec(&quote).unwrap(),
            ..upload.clone()
        };
        let own = Listing::new(&envelope, &own, BLOCK_TIME);

        // Sent by one peer: a few queries, and nothing found.
        let alone = served_by(&asked[..1], &own);
        let repair = listings.repair_from(&envelope, &asked, alone).await;
        assert_eq!(repair.unwrap(), Repair::Nothing);
        assert_eq!(log_calls(), Search::Brief.log_calls());
        // Served by a quorum, as the close group's copy is: searched for in
        // full.
        let quorum = served_by(&asked[..4], &own);
        let repair = listings.repair_from(&envelope, &asked, quorum).await;
        assert_eq!(repair.unwrap(), Repair::Nothing);
        assert_eq!(log_calls(), Search::Full.log_calls());

        // The upload's copy from one peer, paid soon after its quote: still
        // found.
        let honest = Listing::new(&envelope, &upload, BLOCK_TIME);
        let alone = served_by(&asked[..1], &honest);
        let repair = listings.repair_from(&envelope, &asked, alone).await;
        assert_eq!(repair.unwrap(), Repair::Provisional(honest));
        assert!(log_calls() <= Search::Brief.log_calls());
    }

    #[tokio::test]
    async fn a_put_reads_only_the_first_few_transactions_of_its_proof() {
        let listed = envelope(1);
        let quote = signed_quote(listed.address(), 0, 87);
        let mut mock = chain_with(400, &[]);
        mock.add_receipt(PaymentReceipt {
            tx_hash: [0x77; 32],
            success: true,
            block_number: 300,
            logs: vec![vault_log(quote.hash().0, 4)],
        });
        let (chain, url) = serve(mock).await;
        let dir = TempDir::new().unwrap();
        let (listings, _) = service_on(&dir, &url).await;

        // Thousands of hashes, the paying one last: refused after a few.
        let mut tx_hashes: Vec<[u8; 32]> = (0..2000u32)
            .map(|i| {
                let mut tx_hash = [0u8; 32];
                tx_hash[..4].copy_from_slice(&i.to_be_bytes());
                tx_hash
            })
            .collect();
        tx_hashes.push([0x77; 32]);
        let proof = proof_naming(&quote, &tx_hashes);
        assert!(listings
            .listing_for_put(&listed.address(), &listed.to_bytes(), Some(&proof))
            .await
            .is_err());
        assert_eq!(calls_to(&chain, "eth_getTransactionReceipt"), MAX_PROOF_TXS);

        // A hash named again is read once.
        chain.lock().calls.clear();
        let proof = proof_naming(&quote, &[[1; 32], [1; 32], [1; 32], [1; 32], [0x77; 32]]);
        assert!(listings
            .listing_for_put(&listed.address(), &listed.to_bytes(), Some(&proof))
            .await
            .unwrap()
            .is_some());
        assert_eq!(calls_to(&chain, "eth_getTransactionReceipt"), 2);
    }

    #[tokio::test]
    async fn an_envelope_offered_again_while_looked_up_is_looked_up_once() {
        let unlisted = envelope_with(1, ShownFields::ALL, false);
        let quote = signed_quote(unlisted.address(), 0, 87);
        // The payment is not on chain.
        let (chain, url) = serve(chain_with(400, &[])).await;
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_on(&dir, &url).await;
        chunks
            .put(&unlisted.address(), &unlisted.to_bytes())
            .await
            .unwrap();
        let proof = single_node_proof(&quote, [0x55; 32]);
        for _ in 0..10 {
            listings.record_from_fresh_offer(unlisted.address(), &unlisted.to_bytes(), &proof);
        }
        let offers = Arc::clone(&listings);
        wait_until(async || offers.fresh_offers.lock().is_empty()).await;
        assert_eq!(calls_to(&chain, "eth_getTransactionReceipt"), 1);
        assert!(!listings.store.contains(unlisted.address()).await.unwrap());
    }

    /// The file holding the chunk at `address` under `dir`.
    fn chunk_file(dir: &TempDir, address: &XorName) -> std::path::PathBuf {
        let name = hex::encode(address);
        let chunks = dir.path().join(crate::storage::file_store::CHUNKS_DIR_NAME);
        std::fs::read_dir(chunks)
            .unwrap()
            .filter_map(|shard| Some(shard.ok()?.path().join(&name)))
            .find(|path| path.exists())
            .unwrap()
    }

    #[tokio::test]
    async fn a_listing_whose_chunk_cant_be_read_now_is_kept_but_not_served() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let a = envelope_with(1, ShownFields::ALL, false);
        chunks.put(&a.address(), &a.to_bytes()).await.unwrap();
        let listing = listing_for(&a, 10);
        listings.record(listing.clone()).await;

        // Something that can't be read stands where the chunk's file was.
        let file = chunk_file(&dir, &a.address());
        std::fs::remove_file(&file).unwrap();
        std::fs::create_dir(&file).unwrap();
        assert!(chunks.get(&a.address()).await.is_err());
        assert!(!chunks.exists(&a.address()).unwrap());
        assert!(get(&listings, a.address()).await.is_none());
        assert!(listings.store.contains(a.address()).await.unwrap());

        // Readable again: served again.
        std::fs::remove_dir(&file).unwrap();
        std::fs::write(&file, a.to_bytes()).unwrap();
        assert_eq!(get(&listings, a.address()).await, Some(listing));
    }

    #[tokio::test]
    async fn a_page_reads_on_past_listings_whose_chunk_has_gone() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        // First in the index: listings whose chunks this node doesn't hold.
        for seed in 1..=5u8 {
            let stale = envelope(seed);
            listings
                .store
                .insert(listing_for(&stale, 10 + u64::from(seed)))
                .await
                .unwrap();
        }
        let (a, b) = (envelope(100), envelope(101));
        for (envelope, block) in [(&a, 50), (&b, 60)] {
            chunks
                .put(&envelope.address(), &envelope.to_bytes())
                .await
                .unwrap();
            listings.record(listing_for(envelope, block)).await;
        }

        let (page, more) = list(&listings, ListingKey::MIN, 1).await;
        assert_eq!(
            page.iter().map(|l| l.address).collect::<Vec<_>>(),
            vec![a.address()]
        );
        assert!(more);
        let (page, more) = list(&listings, ListingKey::MIN, 2).await;
        assert_eq!(
            page.iter().map(|l| l.address).collect::<Vec<_>>(),
            vec![a.address(), b.address()]
        );
        assert!(!more);
        // The stale ones are gone for good.
        for seed in 1..=5u8 {
            assert!(!listings
                .store
                .contains(envelope(seed).address())
                .await
                .unwrap());
        }
    }

    #[tokio::test]
    async fn a_sweep_drops_the_listings_whose_chunk_has_gone() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let kept = envelope_with(1, ShownFields::ALL, false);
        chunks.put(&kept.address(), &kept.to_bytes()).await.unwrap();
        listings.record(listing_for(&kept, 10)).await;
        // Removed while the node was stopped, in the index and outside it.
        let (indexed, outside) = (envelope(2), envelope_with(3, ShownFields::ALL, false));
        for envelope in [&indexed, &outside] {
            listings
                .store
                .insert(listing_for(envelope, 11))
                .await
                .unwrap();
        }

        ListingService::sweep(Arc::downgrade(&listings)).await;
        assert!(listings.store.contains(kept.address()).await.unwrap());
        assert!(!listings.store.contains(indexed.address()).await.unwrap());
        assert!(!listings.store.contains(outside.address()).await.unwrap());
    }

    #[tokio::test]
    async fn a_provisional_listing_is_kept_unserved_until_a_quorum_settles_it() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let envelope = envelope(1);
        let address = envelope.address();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let front_run = Listing::new(&envelope, &payment(&envelope, 1, 10), BLOCK_TIME);
        let upload = Listing::new(&envelope, &payment(&envelope, 0, 20), BLOCK_TIME);

        // Taken without a quorum: kept, but neither served nor listed.
        let repair = Repair::Provisional(front_run.clone());
        assert!(!listings
            .apply_repair(&address, false, repair)
            .await
            .unwrap());
        assert!(listings.store.is_provisional(address).await.unwrap());
        assert_eq!(listings.store.get(address).await.unwrap(), Some(front_run));
        assert!(get(&listings, address).await.is_none());
        assert_eq!(
            list(&listings, ListingKey::MIN, 10).await.0,
            Vec::<Listing>::new()
        );

        // A later round, still without a quorum: replaced, still provisional.
        let repair = Repair::Provisional(upload.clone());
        assert!(!listings.apply_repair(&address, true, repair).await.unwrap());
        assert!(get(&listings, address).await.is_none());

        // A quorum settles it.
        let repair = Repair::Settled(upload.clone());
        assert!(listings.apply_repair(&address, true, repair).await.unwrap());
        assert!(!listings.store.is_provisional(address).await.unwrap());
        assert_eq!(get(&listings, address).await, Some(upload.clone()));
        assert_eq!(list(&listings, ListingKey::MIN, 10).await.0, vec![upload]);
    }

    #[tokio::test]
    async fn a_listing_from_an_upload_wins_over_a_provisional_one() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let envelope = envelope(1);
        let address = envelope.address();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let upload = Listing::new(&envelope, &payment(&envelope, 0, 20), BLOCK_TIME);
        let front_run = Listing::new(&envelope, &payment(&envelope, 1, 10), BLOCK_TIME);
        assert!(listings.record(upload.clone()).await);

        let repair = Repair::Provisional(front_run);
        assert!(listings
            .apply_repair(&address, false, repair)
            .await
            .unwrap());
        assert!(!listings.store.is_provisional(address).await.unwrap());
        assert_eq!(get(&listings, address).await, Some(upload));
    }

    #[tokio::test]
    async fn a_repair_doesnt_bring_back_a_listing_dropped_with_its_chunk() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let envelope = envelope_with(1, ShownFields::ALL, false);
        let address = envelope.address();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let front_run = Listing::new(&envelope, &payment(&envelope, 1, 10), BLOCK_TIME);
        let upload = Listing::new(&envelope, &payment(&envelope, 0, 20), BLOCK_TIME);
        let repair = Repair::Provisional(front_run.clone());
        assert!(!listings
            .apply_repair(&address, false, repair)
            .await
            .unwrap());

        // Deleted while the close group was asked again.
        chunks.delete(&address).await.unwrap();
        let store = listings.store.clone();
        wait_until(async || !store.contains(address).await.unwrap()).await;
        let repair = Repair::Settled(upload.clone());
        assert!(listings.apply_repair(&address, true, repair).await.unwrap());
        assert!(!listings.store.contains(address).await.unwrap());

        // Gone before the removal reached the listing.
        listings.store.insert_provisional(front_run).await.unwrap();
        let repair = Repair::Settled(upload);
        assert!(listings.apply_repair(&address, true, repair).await.unwrap());
        assert!(!listings.store.contains(address).await.unwrap());
    }

    /// From an envelope landing to the last check of its listing.
    fn whole_schedule() -> Duration {
        RETRY_DELAYS
            .iter()
            .fold(CHECK_DELAY, |total, delay| total + *delay)
    }

    #[tokio::test(start_paused = true)]
    async fn a_provisional_listing_is_served_once_the_checks_run_out() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let envelope = envelope(1);
        let address = envelope.address();
        let start = Instant::now();
        // Stored, so its checks start. No P2P node is attached: each fails.
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let listing = Listing::new(&envelope, &payment(&envelope, 1, 10), BLOCK_TIME);
        let repair = Repair::Provisional(listing.clone());
        assert!(!listings
            .apply_repair(&address, false, repair)
            .await
            .unwrap());

        // Just before the last check: still provisional, so not served.
        tokio::time::sleep_until(start + whole_schedule() - Duration::from_secs(1)).await;
        assert!(listings.store.is_provisional(address).await.unwrap());
        assert!(get(&listings, address).await.is_none());
        assert_eq!(
            listings.store.pending_addresses().await.unwrap(),
            vec![address]
        );

        // The last check fails too: settled, served and listed.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let checked = Arc::clone(&listings);
        wait_until(async || checked.store.pending_addresses().await.unwrap().is_empty()).await;
        assert!(!listings.store.is_provisional(address).await.unwrap());
        assert_eq!(get(&listings, address).await, Some(listing.clone()));
        assert_eq!(list(&listings, ListingKey::MIN, 10).await.0, vec![listing]);
    }

    /// The waits between the attempts that go on after the retry schedule
    /// while the chain can't be read.
    fn unread_delays() -> impl Iterator<Item = Duration> {
        let last = RETRY_DELAYS[RETRY_DELAYS.len() - 1];
        std::iter::successors(Some(last), |wait| Some((*wait * 2).min(MAX_UNREAD_DELAY))).skip(1)
    }

    /// How many times `listings` has asked its close group.
    fn times_asked(listings: &ListingService) -> usize {
        listings.close_group.lock().as_ref().unwrap().times
    }

    #[tokio::test(start_paused = true)]
    async fn a_check_the_chain_cant_answer_goes_on_until_it_can() {
        // The chain can't be reached for now.
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        // Let the start read what an earlier run left to check first, or it
        // can find this envelope's mark too, and check it twice.
        tokio::time::sleep(Duration::from_millis(1)).await;
        let envelope = envelope_with(1, ShownFields::NONE.with(PaymentField::BlockNumber), true);
        let address = envelope.address();
        let upload = listing_for(&envelope, 10);
        let planted = listing_for(&envelope, 11);
        // Every time, a quorum of the close group serves the upload's copy.
        let asked = peers(7);
        answer_for(&listings, &asked, served_by(&asked[..4], &upload));
        let start = Instant::now();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        assert!(listings.record(planted.clone()).await);
        let checked = Arc::clone(&listings);

        // Each check can't read the chain for it, to the last of the
        // schedule and on after it, each wait twice the one before up to
        // the longest.
        let hour = Duration::from_hours(1);
        let waits: Vec<Duration> = unread_delays().take(4).collect();
        assert_eq!(waits, [2 * hour, 4 * hour, 6 * hour, 6 * hour]);
        let mut due = start + whole_schedule();
        for (times, wait) in (1 + RETRY_DELAYS.len()..).zip(waits) {
            tokio::time::sleep_until(due - Duration::from_secs(1)).await;
            assert_eq!(times_asked(&listings), times - 1);
            tokio::time::sleep_until(due + Duration::from_secs(1)).await;
            wait_until(async || times_asked(&checked) == times).await;
            due += wait;
        }
        // Kept and served meanwhile, and still to be checked.
        assert_eq!(get(&listings, address).await, Some(planted));
        assert_eq!(
            listings.store.pending_addresses().await.unwrap(),
            vec![address]
        );

        // The chain answers again: the next check takes the close group's
        // copy, and is the last.
        let chain = Arc::new(Mutex::new(chain_with(1000, &[])));
        listings.chain.answer_from(chain);
        tokio::time::sleep_until(due + Duration::from_secs(1)).await;
        wait_until(async || checked.store.pending_addresses().await.unwrap().is_empty()).await;
        assert_eq!(get(&listings, address).await, Some(upload));
        let times = times_asked(&listings);
        tokio::time::sleep(2 * MAX_UNREAD_DELAY).await;
        assert_eq!(times_asked(&listings), times);
    }

    #[tokio::test(start_paused = true)]
    async fn a_repair_the_chain_cant_answer_goes_on_until_it_can() {
        // All shown: a copy is looked up by its tx hash, on a chain that
        // refuses every call for now.
        let envelope = envelope(1);
        let address = envelope.address();
        let upload = payment(&envelope, 0, 20);
        let mut mock = chain_with(1000, &[]);
        mock.add_receipt(paid(&upload).0.unwrap());
        mock.fails = Some(Box::new(|_, _| true));
        let chain = Arc::new(Mutex::new(mock));
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        listings.chain.answer_from(Arc::clone(&chain));
        // Let the start read what an earlier run left to check first, or it
        // can find this envelope's mark too, and check it twice.
        tokio::time::sleep(Duration::from_millis(1)).await;
        // Every time, one peer serves the upload's copy: no quorum, so it is
        // taken only once its payment is found.
        let honest = Listing::new(&envelope, &upload, BLOCK_TIME);
        let asked = peers(7);
        answer_for(&listings, &asked, served_by(&asked[..1], &honest));
        let start = Instant::now();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let checked = Arc::clone(&listings);

        // No attempt of the schedule can read the chain, nor the first after
        // it: nothing taken, and still to be checked.
        let mut waits = unread_delays();
        let due = start + whole_schedule() + waits.next().unwrap();
        tokio::time::sleep_until(due + Duration::from_secs(1)).await;
        wait_until(async || times_asked(&checked) == 2 + RETRY_DELAYS.len()).await;
        assert!(!listings.store.contains(address).await.unwrap());
        assert_eq!(
            listings.store.pending_addresses().await.unwrap(),
            vec![address]
        );

        // The chain answers again: the next attempt finds the payment and,
        // the schedule over, serves the copy though no quorum served it.
        chain.lock().fails = None;
        tokio::time::sleep_until(due + waits.next().unwrap() + Duration::from_secs(1)).await;
        wait_until(async || checked.store.pending_addresses().await.unwrap().is_empty()).await;
        assert!(!listings.store.is_provisional(address).await.unwrap());
        assert_eq!(get(&listings, address).await, Some(honest));
    }

    #[tokio::test(start_paused = true)]
    async fn checks_cut_short_by_a_restart_go_on_after_it() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        // One envelope with a provisional listing, one with no listing yet.
        let provisional = envelope(1);
        let bare = envelope_with(2, ShownFields::NONE, false);
        for envelope in [&provisional, &bare] {
            chunks
                .put(&envelope.address(), &envelope.to_bytes())
                .await
                .unwrap();
        }
        let listing = Listing::new(&provisional, &payment(&provisional, 1, 10), BLOCK_TIME);
        let repair = Repair::Provisional(listing.clone());
        assert!(!listings
            .apply_repair(&provisional.address(), false, repair)
            .await
            .unwrap());
        let marked = Arc::clone(&listings);
        wait_until(async || marked.store.pending_addresses().await.unwrap().len() == 2).await;
        drop(marked);
        // Only the provisional mark says the first is still being checked.
        listings
            .store
            .clear_pending(provisional.address())
            .await
            .unwrap();

        // The node stops before the first check, and starts again.
        drop(listings);
        let start = Instant::now();
        let listings =
            ListingService::new(dir.path(), 0, Arc::clone(&chunks), DEAD_CHAIN.into(), VAULT)
                .await
                .unwrap();

        tokio::time::sleep_until(start + whole_schedule() - Duration::from_secs(1)).await;
        assert!(listings
            .store
            .is_provisional(provisional.address())
            .await
            .unwrap());
        assert_eq!(
            listings.store.pending_addresses().await.unwrap(),
            vec![bare.address()]
        );

        // Both checked to the end of the schedule.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let checked = Arc::clone(&listings);
        wait_until(async || checked.store.pending_addresses().await.unwrap().is_empty()).await;
        wait_until(async || {
            !checked
                .store
                .is_provisional(provisional.address())
                .await
                .unwrap()
        })
        .await;
        assert_eq!(get(&listings, provisional.address()).await, Some(listing));
        assert!(!listings.store.contains(bare.address()).await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn an_envelope_deleted_before_its_check_is_checked_no_more() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let envelope = envelope(1);
        let address = envelope.address();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let marked = Arc::clone(&listings);
        wait_until(async || marked.store.pending_addresses().await.unwrap() == vec![address]).await;

        chunks.delete(&address).await.unwrap();
        tokio::time::sleep(CHECK_DELAY + Duration::from_secs(1)).await;
        wait_until(async || marked.store.pending_addresses().await.unwrap().is_empty()).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_check_waits_while_the_most_repairs_are_running() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let envelope = envelope(1);
        let address = envelope.address();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let marked = Arc::clone(&listings);
        wait_until(async || marked.store.pending_addresses().await.unwrap() == vec![address]).await;
        chunks.delete(&address).await.unwrap();

        // Every slot taken: the check is due, but doesn't run.
        let slots = u32::try_from(MAX_REPAIRS).unwrap();
        let running = listings.repairs.acquire_many(slots).await.unwrap();
        tokio::time::sleep(CHECK_DELAY + Duration::from_mins(5)).await;
        assert_eq!(
            listings.store.pending_addresses().await.unwrap(),
            vec![address]
        );

        // A slot frees: it runs, and finds the envelope gone.
        drop(running);
        wait_until(async || marked.store.pending_addresses().await.unwrap().is_empty()).await;
    }

    #[tokio::test]
    async fn a_repair_the_store_cant_take_is_tried_again() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let envelope = envelope(1);
        let address = envelope.address();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let listing = listing_for(&envelope, 10);

        // The disk is full for a while: an error, so the schedule goes on.
        listings.store.fail_writes(true);
        for repair in [
            Repair::Settled(listing.clone()),
            Repair::Provisional(listing.clone()),
        ] {
            assert!(listings
                .apply_repair(&address, false, repair)
                .await
                .is_err());
        }
        // A PUT's listing is left to the check that follows the store.
        assert!(!listings.record(listing.clone()).await);
        assert!(!listings.store.contains(address).await.unwrap());

        // Room again: the next attempt takes it.
        listings.store.fail_writes(false);
        let repair = Repair::Settled(listing.clone());
        assert!(listings
            .apply_repair(&address, false, repair)
            .await
            .unwrap());
        assert_eq!(get(&listings, address).await, Some(listing));
    }

    /// Have `listings` take `answers` as its close group's, from `asked`.
    fn answer_for(listings: &ListingService, asked: &[PeerId], answers: Vec<(PeerId, Listing)>) {
        *listings.close_group.lock() = Some(CloseGroup {
            asked: asked.to_vec(),
            answers,
            times: 0,
        });
    }

    /// `listing` as each of `peers` serves it.
    fn served_by(peers: &[PeerId], listing: &Listing) -> Vec<(PeerId, Listing)> {
        peers.iter().map(|peer| (*peer, listing.clone())).collect()
    }

    #[tokio::test]
    async fn a_listing_from_a_proof_gives_way_to_the_one_the_close_group_serves() {
        // In the index, only the block shown: dated by first_seen.
        let envelope = envelope_with(1, ShownFields::NONE.with(PaymentField::BlockNumber), true);
        let address = envelope.address();
        let (chain, url) = serve(chain_with(1000, &[])).await;
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_on(&dir, &url).await;
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        // Someone paid for the address again and PUT the envelope here with
        // that proof, after the upload.
        let upload = listing_for(&envelope, 10);
        let planted = listing_for(&envelope, 11);
        assert!(listings.record(planted.clone()).await);
        let asked = peers(7);
        // Its check asks the close group though a listing is held. No P2P
        // node is attached, so it fails, to be tried again.
        assert!(listings.ensure_listing(&address).await.is_err());

        // The close group serves the same, recorded in another hour: kept,
        // and nothing looked up.
        let mut later = planted.clone();
        later.first_seen = later.first_seen.map(|hour| hour + 3600);
        let same = served_by(&asked[..5], &later);
        let checked = listings.confirm(&envelope, &planted, &asked, same).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        // Three serve the upload's: no quorum, kept, nothing looked up.
        let mut three = served_by(&asked[..3], &upload);
        three.extend(served_by(&asked[3..5], &planted));
        let checked = listings.confirm(&envelope, &planted, &asked, three).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert!(chain.lock().calls.is_empty(), "{:?}", chain.lock().calls);
        // A quorum serves a listing whose block isn't on chain (yet): kept,
        // and asked about again later.
        let unpaid = listing_for(&envelope, 5000);
        let answers = served_by(&asked[..4], &unpaid);
        let checked = listings.confirm(&envelope, &planted, &asked, answers).await;
        assert_eq!(checked.unwrap(), Checked::Again);
        assert_eq!(get(&listings, address).await, Some(planted.clone()));
        // One whose block's time is later than its date allows: kept for good.
        let mut misdated = upload.clone();
        misdated.first_seen = Some(first_seen_at(BLOCK_TIME) - 10 * 3600);
        let answers = served_by(&asked[..4], &misdated);
        let checked = listings.confirm(&envelope, &planted, &asked, answers).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(planted.clone()));

        // A quorum serves the upload's, which checks out: taken instead.
        let answers = served_by(&asked[..4], &upload);
        let checked = listings.confirm(&envelope, &planted, &asked, answers).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(upload.clone()));
        assert_eq!(list(&listings, ListingKey::MIN, 10).await.0, vec![upload]);
    }

    #[tokio::test]
    async fn one_peer_dating_the_close_groups_copy_in_the_future_doesnt_keep_another() {
        // In the index, only the block shown: dated by first_seen.
        let envelope = envelope_with(1, ShownFields::NONE.with(PaymentField::BlockNumber), true);
        let address = envelope.address();
        let (_chain, url) = serve(chain_with(1000, &[])).await;
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_on(&dir, &url).await;
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let upload = listing_for(&envelope, 10);
        let planted = listing_for(&envelope, 11);
        assert!(listings.record(planted.clone()).await);
        let asked = peers(7);

        // Four serve the upload's copy, one of them dated decades ahead:
        // that date is dropped, and the copy taken as the others date it.
        let mut ahead = upload.clone();
        ahead.first_seen = Some(first_seen_at(4_102_444_800));
        let mut answers = served_by(&asked[..3], &upload);
        answers.extend(served_by(&asked[3..4], &ahead));
        let checked = listings.confirm(&envelope, &planted, &asked, answers).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(upload));
    }

    #[tokio::test]
    async fn a_listing_with_its_quote_from_a_proof_gives_way_to_the_close_group() {
        let envelope = envelope(1);
        let address = envelope.address();
        let upload = payment(&envelope, 0, 20);
        let mut mock = chain_with(1000, &[]);
        mock.add_receipt(paid(&upload).0.unwrap());
        let (chain, url) = serve(mock).await;
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_on(&dir, &url).await;
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        // Another payment of the address, earlier, with a quote of its own.
        let planted = Listing::new(&envelope, &payment(&envelope, 1, 10), BLOCK_TIME);
        let upload = Listing::new(&envelope, &upload, BLOCK_TIME);
        assert!(listings.record(planted.clone()).await);
        let asked = peers(7);

        // One peer serving the upload's is no quorum: kept, nothing looked up.
        let mut answers = served_by(&asked[..1], &upload);
        answers.extend(served_by(&asked[1..3], &planted));
        let checked = listings.confirm(&envelope, &planted, &asked, answers).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(planted.clone()));
        assert!(chain.lock().calls.is_empty(), "{:?}", chain.lock().calls);

        // A quorum: the close group's copy wins over the earlier payment.
        let mut answers = served_by(&asked[..4], &upload);
        answers.extend(served_by(&asked[4..5], &planted));
        let checked = listings.confirm(&envelope, &planted, &asked, answers).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(upload));
    }

    #[tokio::test]
    async fn a_listing_from_a_proof_dated_later_than_the_close_group_takes_its_date() {
        // In the index, nothing shown: the date is all there is to tell
        // another payment of the address from the upload's.
        let envelope = envelope_with(1, ShownFields::NONE, true);
        let address = envelope.address();
        let (chain, url) = serve(chain_with(10, &[])).await;
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_on(&dir, &url).await;
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let upload = listing_for(&envelope, 10);
        // Paid again and PUT here ten hours after the upload.
        let mut redated = upload.clone();
        redated.first_seen = Some(first_seen_at(BLOCK_TIME + 10 * 3600));
        assert!(redated.eq_apart_from_first_seen(&upload));
        assert!(listings.record(redated.clone()).await);
        let asked = peers(7);

        // Three serve the upload's date: no quorum, kept.
        let three = served_by(&asked[..3], &upload);
        let checked = listings.confirm(&envelope, &redated, &asked, three).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(redated.clone()));

        // Four do: the held copy takes their date, with nothing to look up.
        let four = served_by(&asked[..4], &upload);
        let checked = listings.confirm(&envelope, &redated, &asked, four).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(upload.clone()));
        assert_eq!(
            list(&listings, ListingKey::MIN, 10).await.0,
            vec![upload.clone()]
        );
        assert!(chain.lock().calls.is_empty(), "{:?}", chain.lock().calls);

        // A quorum dating it later doesn't move it.
        let later = served_by(&asked[..5], &redated);
        let checked = listings.confirm(&envelope, &upload, &asked, later).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(upload));
    }

    #[tokio::test]
    async fn a_few_peers_dating_a_listing_early_cant_backdate_it() {
        // In the index, nothing shown: no chain to check a date against.
        let envelope = envelope_with(1, ShownFields::NONE, true);
        let address = envelope.address();
        let (chain, url) = serve(chain_with(10, &[])).await;
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_on(&dir, &url).await;
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let held = listing_for(&envelope, 10);
        let hour = held.first_seen.unwrap();
        assert!(listings.record(held.clone()).await);
        let asked = peers(7);
        let dated = |first_seen| Listing {
            first_seen: Some(first_seen),
            ..held.clone()
        };

        // Three date it in 1970, and only one other answers: kept. Their
        // median would be 1970.
        let mut answers = served_by(&asked[..3], &dated(0));
        answers.extend(served_by(&asked[3..4], &held));
        let checked = listings
            .confirm(&envelope, &held, &asked, answers.clone())
            .await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(held.clone()));
        // Two others: kept.
        answers.extend(served_by(&asked[4..5], &held));
        let checked = listings.confirm(&envelope, &held, &asked, answers).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(held.clone()));

        // Four date it an hour earlier, the three 1970 again: the hour
        // before is taken.
        let earlier = dated(hour - 3600);
        let mut answers = served_by(&asked[..4], &earlier);
        answers.extend(served_by(&asked[4..], &dated(0)));
        let checked = listings.confirm(&envelope, &held, &asked, answers).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(earlier));
        assert!(chain.lock().calls.is_empty(), "{:?}", chain.lock().calls);
    }

    #[tokio::test]
    async fn a_listing_from_a_proof_is_checked_again_while_the_chain_cant_be_read() {
        // In the index, only the block shown: dated by first_seen.
        let envelope = envelope_with(1, ShownFields::NONE.with(PaymentField::BlockNumber), true);
        let address = envelope.address();
        let upload = listing_for(&envelope, 10);
        let planted = listing_for(&envelope, 11);
        let asked = peers(7);
        let quorum = || served_by(&asked[..4], &upload);

        // No chain at all: kept, and asked about again.
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        assert!(listings.record(planted.clone()).await);
        let checked = listings
            .confirm(&envelope, &planted, &asked, quorum())
            .await;
        assert_eq!(checked.unwrap(), Checked::ChainUnread);
        assert_eq!(get(&listings, address).await, Some(planted.clone()));

        // A provider that refuses for a while.
        let mut mock = chain_with(1000, &[]);
        mock.fails = Some(Box::new(|method, _| method == "eth_getBlockByNumber"));
        let (chain, url) = serve(mock).await;
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_on(&dir, &url).await;
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let marked = Arc::clone(&listings);
        wait_until(async || marked.store.pending_addresses().await.unwrap() == vec![address]).await;
        assert!(listings.record(planted.clone()).await);
        answer_for(&listings, &asked, quorum());
        let checked = listings.ensure_listing(&address).await;
        assert_eq!(checked.unwrap(), Checked::ChainUnread);
        assert!(calls_to(&chain, "eth_getBlockByNumber") > 0);
        // Kept and served meanwhile, and still to be checked.
        assert_eq!(get(&listings, address).await, Some(planted.clone()));
        assert_eq!(
            listings.store.pending_addresses().await.unwrap(),
            vec![address]
        );

        // The provider answers again: the next check takes the close
        // group's copy.
        chain.lock().fails = None;
        let checked = listings.ensure_listing(&address).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert_eq!(get(&listings, address).await, Some(upload.clone()));
        assert_eq!(list(&listings, ListingKey::MIN, 10).await.0, vec![upload]);
    }

    #[tokio::test]
    async fn a_repair_waits_for_the_chain_to_check_the_copy_a_quorum_serves() {
        // Everything shown: each copy is looked up by its tx hash.
        let envelope = envelope(1);
        let address = envelope.address();
        let upload = payment(&envelope, 0, 20);
        let front_run = payment(&envelope, 1, 10);
        let mut mock = chain_with(1000, &[]);
        mock.add_receipt(paid(&upload).0.unwrap());
        mock.add_receipt(paid(&front_run).0.unwrap());
        // The upload's receipt can't be read for now.
        let upload_tx = format!("0x{}", hex::encode(upload.tx_hash));
        mock.fails = Some(Box::new(move |method, params| {
            method == "eth_getTransactionReceipt" && params[0].as_str() == Some(&upload_tx)
        }));
        let (chain, url) = serve(mock).await;
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_on(&dir, &url).await;
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let marked = Arc::clone(&listings);
        wait_until(async || marked.store.pending_addresses().await.unwrap() == vec![address]).await;
        let upload = Listing::new(&envelope, &upload, BLOCK_TIME);
        let front_run = Listing::new(&envelope, &front_run, BLOCK_TIME);
        let asked = peers(7);
        let mut answers = served_by(&asked[..4], &upload);
        answers.extend(served_by(&asked[4..5], &front_run));

        // The other copy would check out, but nothing is decided without
        // the quorum's: it is not even looked up.
        assert!(listings
            .repair_from(&envelope, &asked, answers.clone())
            .await
            .is_err());
        assert_eq!(calls_to(&chain, "eth_getTransactionReceipt"), 1);
        answer_for(&listings, &asked, answers);
        let checked = listings.ensure_listing(&address).await;
        assert_eq!(checked.unwrap(), Checked::ChainUnread);
        assert!(!listings.store.contains(address).await.unwrap());

        // A provisional listing an earlier attempt took stays unserved while
        // the chain can't be read for the quorum's copy.
        assert!(listings
            .store
            .insert_provisional(front_run.clone())
            .await
            .unwrap());
        let checked = listings.ensure_listing(&address).await;
        assert_eq!(checked.unwrap(), Checked::ChainUnread);
        assert!(listings.store.is_provisional(address).await.unwrap());
        assert!(get(&listings, address).await.is_none());
        assert_eq!(
            listings.store.pending_addresses().await.unwrap(),
            vec![address]
        );

        // The chain answers again: the quorum's copy settles it.
        chain.lock().fails = None;
        let checked = listings.ensure_listing(&address).await;
        assert_eq!(checked.unwrap(), Checked::Done);
        assert!(!listings.store.is_provisional(address).await.unwrap());
        assert_eq!(get(&listings, address).await, Some(upload));
    }

    #[tokio::test]
    async fn payments_of_the_quote_made_first_by_others_dont_hide_the_upload() {
        // The quote shown with its log index, or with its block's time.
        for (seed, field) in [
            (1, PaymentField::LogIndex),
            (2, PaymentField::BlockTimestamp),
        ] {
            let shown = ShownFields::NONE.with(PaymentField::Quote).with(field);
            let envelope = envelope_with(seed, shown, false);
            let upload = payment(&envelope, 0, 300);
            // The quoting node pays the uploader's quote four times first,
            // in earlier blocks, at another log index.
            let mut mock = chain_with(1000, &[]);
            for i in 0..4u8 {
                mock.add_receipt(PaymentReceipt {
                    tx_hash: [200 + i; 32],
                    success: true,
                    block_number: 100 + u64::from(i),
                    logs: vec![vault_log(quote_hash(&upload), 9)],
                });
            }
            mock.add_receipt(paid(&upload).0.unwrap());
            let (_chain, url) = serve(mock).await;
            let dir = TempDir::new().unwrap();
            let (listings, _) = service_on(&dir, &url).await;
            let asked = peers(7);
            let honest = Listing::new(&envelope, &upload, BLOCK_TIME);
            let first = Payment {
                tx_hash: [200; 32],
                block_number: 100,
                block_timestamp: BLOCK_TIME + 200,
                log_index: 9,
                quote: upload.quote.clone(),
            };
            let own = Listing::new(&envelope, &first, BLOCK_TIME);
            assert_ne!(own, honest);

            // Its own copy checks out on its own.
            let alone = served_by(&asked[..1], &own);
            let repair = listings.repair_from(&envelope, &asked, alone).await;
            assert_eq!(repair.unwrap(), Repair::Provisional(own.clone()));
            // The copy the close group serves is still found, and wins.
            let mut answers = served_by(&asked[..4], &honest);
            answers.extend(served_by(&asked[4..5], &own));
            let repair = listings.repair_from(&envelope, &asked, answers).await;
            assert_eq!(repair.unwrap(), Repair::Settled(honest), "{field:?}");
        }
    }

    #[tokio::test]
    async fn a_listing_recorded_while_the_close_group_is_asked_gives_way_to_its_quorum() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        let envelope = envelope(1);
        let address = envelope.address();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let settled = Listing::new(&envelope, &payment(&envelope, 0, 20), BLOCK_TIME);
        // The check found no listing and asked the close group; meanwhile a
        // fresh offer's lookup recorded another valid payment.
        let recorded = Listing::new(&envelope, &payment(&envelope, 1, 10), BLOCK_TIME);
        assert!(listings.record(recorded).await);

        let repair = Repair::Settled(settled.clone());
        assert!(listings
            .apply_repair(&address, false, repair)
            .await
            .unwrap());
        assert_eq!(get(&listings, address).await, Some(settled.clone()));
        assert_eq!(list(&listings, ListingKey::MIN, 10).await.0, vec![settled]);

        // The same with only a date to tell them apart: the quorum's earlier
        // one is taken, a later one is not.
        let hidden = envelope_with(2, ShownFields::NONE, true);
        let address = hidden.address();
        chunks.put(&address, &hidden.to_bytes()).await.unwrap();
        let settled = listing_for(&hidden, 10);
        let mut recorded = settled.clone();
        recorded.first_seen = Some(first_seen_at(BLOCK_TIME + 5 * 3600));
        assert!(listings.record(recorded.clone()).await);
        let repair = Repair::Settled(recorded.clone());
        assert!(listings
            .apply_repair(&address, false, repair)
            .await
            .unwrap());
        assert_eq!(get(&listings, address).await, Some(recorded));
        let repair = Repair::Settled(settled.clone());
        assert!(listings
            .apply_repair(&address, false, repair)
            .await
            .unwrap());
        assert_eq!(get(&listings, address).await, Some(settled.clone()));
        let mut later = settled.clone();
        later.first_seen = Some(first_seen_at(BLOCK_TIME + 3600));
        let repair = Repair::Settled(later);
        assert!(listings
            .apply_repair(&address, false, repair)
            .await
            .unwrap());
        assert_eq!(get(&listings, address).await, Some(settled));

        // One a concurrent check left provisional gives way too.
        let third = envelope_with(3, ShownFields::ALL, false);
        let address = third.address();
        chunks.put(&address, &third.to_bytes()).await.unwrap();
        let front_run = Listing::new(&third, &payment(&third, 1, 10), BLOCK_TIME);
        let upload = Listing::new(&third, &payment(&third, 0, 20), BLOCK_TIME);
        assert!(listings.store.insert_provisional(front_run).await.unwrap());
        let repair = Repair::Settled(upload.clone());
        assert!(listings
            .apply_repair(&address, false, repair)
            .await
            .unwrap());
        assert!(!listings.store.is_provisional(address).await.unwrap());
        assert_eq!(get(&listings, address).await, Some(upload));
    }

    #[tokio::test]
    async fn answers_reach_the_request_waiting_for_them() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        let waiting = &listings.waiting;
        let (peer, other) = (PeerId::from_bytes([1; 32]), PeerId::from_bytes([2; 32]));
        let listing = listing_for(&envelope(1), 10);
        let message = |request_id, body| ListingsMessage { request_id, body }.encode().unwrap();
        let found = || ListingsBody::GetResponse {
            listing: Some(listing.clone()),
        };
        let (answer, mut answered) = oneshot::channel();
        let pending = Pending::register(waiting, (peer, 7), answer);

        // Another peer, another request: answers, but not to this one.
        assert!(listings.handle_response(&other, &message(7, found())));
        assert!(listings.handle_response(&peer, &message(8, found())));
        // A request is left to be answered.
        let request = ListingsBody::GetRequest {
            address: listing.address,
        };
        assert!(!listings.handle_response(&peer, &message(7, request)));
        let list = ListingsBody::ListRequest {
            from: ListingKey::MIN,
            limit: 1,
        };
        assert!(!listings.handle_response(&peer, &message(7, list)));
        // Garbage, and answers nothing waits for, need nothing more.
        assert!(listings.handle_response(&peer, &[0xff; 4]));
        let refused = ListingsBody::Error {
            message: "no".into(),
        };
        assert!(listings.handle_response(&peer, &message(7, refused)));
        assert!(answered.try_recv().is_err());

        assert!(listings.handle_response(&peer, &message(7, found())));
        assert_eq!(answered.try_recv().unwrap(), Some(listing.clone()));
        assert!(waiting.lock().is_empty());

        // A request given up on stops waiting.
        let (answer, _answered) = oneshot::channel();
        let pending_again = Pending::register(waiting, (peer, 9), answer);
        drop(pending_again);
        drop(pending);
        assert!(waiting.lock().is_empty());
    }
}

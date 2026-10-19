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
//! copy fewer than a quorum serve is searched for only briefly. A copy
//! showing a block before the payment vault was deployed paid something
//! else, and is never taken.
//!
//! When the rounds run out while the chain can't be read for a copy that
//! could be taken, or while an envelope in the index has no listing and its
//! close group nothing to take yet, the node checks again: each wait twice
//! the last up to [`MAX_UNREAD_DELAY`], for at most [`MAX_RECHECK_TIME`],
//! and at most [`RECHECKS_PER_HOUR`] such checks an hour, spread out. An
//! envelope in the index whose close group serves only copies that can
//! never check out is given up at once. The envelopes still being checked
//! are kept in the store, so the checks go on after a restart, and a
//! restart keeps the end of their re-checks.
//!
//! A listing is deleted with its chunk. Listings are served on
//! [`ant_listings::TOPIC`]: pages of the index, any listing by address, and
//! the store's change feed for an address range, as numbered records or as
//! digest entries in address order. A reader can watch a range: each new
//! record in it is pushed to the reader as it is made. Requests of a kind
//! this node doesn't know are answered as unsupported.

use super::chain::{listings_rpc_urls, Chain, Fetched, Search};
use super::store::{FeedWatcher, ListingStore};
use crate::ant_protocol::{XorName, CLOSE_GROUP_SIZE};
use crate::error::{Error, Result};
use crate::logging::{debug, info, warn};
use crate::storage::ChunkStore;
use ::ant_protocol::evm::PaymentQuote;
use ::ant_protocol::payment::proof::deserialize_single_node_proof;
use ::ant_protocol::{detect_proof_type, ProofType};
use ant_listings::{
    check_copies, check_get_response, check_offline, choose_verified, find_payment, is_envelope,
    listing_to_check, merge_copies, payment_lookup, push_request_id, quorum_listing, quorum_needed,
    verify_listing_receipts, view_hash, AddressRange, ChangesRequest, ChangesResponse, Envelope,
    FeedChange, Listing, ListingKey, ListingsBody, ListingsMessage, Lookup, MergedListing, Payment,
    PaymentError, PaymentField, RangeListRequest, RangeListResponse, MAX_LIST_LIMIT, MAX_WATCHERS,
    TOPIC, VIEW_PEERS, WATCH_LEASE,
};
use parking_lot::{Mutex, RwLock};
use saorsa_core::identity::PeerId;
use saorsa_core::P2PNode;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::Instant;

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

/// The longest wait between the checks that go on after [`RETRY_DELAYS`]
/// while they are inconclusive (see [`Checked`]): each is twice the one
/// before, up to this.
const MAX_UNREAD_DELAY: Duration = Duration::from_hours(6);

/// How long checks go on after [`RETRY_DELAYS`] while they are
/// inconclusive. Then the envelope is given up.
const MAX_RECHECK_TIME: Duration = Duration::from_hours(7 * 24);

/// The most checks a node makes in an hour after their schedules have run
/// out, [`RECHECK_SPACING`] apart, so that envelopes checked again can't
/// pile up.
const RECHECKS_PER_HOUR: u64 = 60;

/// The least time between two checks made after their schedules have run
/// out.
const RECHECK_SPACING: Duration = Duration::from_secs(3_600 / RECHECKS_PER_HOUR);

/// The most records of the change feed read to answer one
/// `ChangesRequest`, in the range asked about or not. The answer stops there
/// and says where, and the reader asks on from it.
const MAX_FEED_READ: usize = 4_096;

/// The most listing requests from other nodes answered at once, from
/// building an answer until it has been sent: permits of their own, apart
/// from those chunk requests take, so that neither kind holds up the other
/// ([`ListingService::request_permits`]).
const MAX_REQUESTS: usize = 16;

/// The answer to a message on [`TOPIC`] that doesn't decode but whose
/// request id does: a request of a kind this node doesn't know.
const UNSUPPORTED: &str = "unsupported";

/// The most records one push carries, as many as one answer.
const MAX_PUSHED: usize = MAX_LIST_LIMIT as usize;

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
    /// The block the vault was deployed in, where known: a copy showing an
    /// earlier block paid something else.
    vault_deployment_block: Option<u64>,
    p2p: RwLock<Option<Arc<P2PNode>>>,
    /// The requests waiting for answers, which [`Self::handle_response`]
    /// hands them.
    waiting: Waiting,
    /// Bounds the repairs running at once.
    repairs: Semaphore,
    /// The permits listing requests from other nodes take while they are
    /// answered ([`Self::request_permits`]).
    requests: Semaphore,
    /// When the next check made after its schedule has run out may start.
    next_recheck: Mutex<Instant>,
    /// Envelopes whose fresh offer is being looked up, so offering one again
    /// doesn't start another lookup.
    fresh_offers: Mutex<HashSet<XorName>>,
    /// The readers watching ranges of the change feed, shared with the
    /// store's feed watcher, which queues their pushes.
    watches: Arc<Mutex<Watches>>,
    /// Tests only: the close group's answers, given in place of asking it.
    #[cfg(test)]
    close_group: Mutex<Option<CloseGroup>>,
    /// Tests only: this node's nearest peers, given in place of its routing
    /// table's.
    #[cfg(test)]
    view: Mutex<Option<Vec<[u8; 32]>>>,
    /// Tests only: the pushes this node sends, kept here in place of being
    /// sent.
    #[cfg(test)]
    pushed: Mutex<Option<Vec<SentPush>>>,
}

/// Tests only: a push this node sent, encoded, and the reader it went to.
#[cfg(test)]
type SentPush = (PeerId, Vec<u8>);

/// A reader's watch on a range of the change feed
/// ([`ChangesRequest::watch`]).
#[derive(Debug, Clone, Copy)]
struct Watch {
    range: AddressRange,
    /// The request id its pushes carry: [`push_request_id`] of the id of the
    /// request that set it.
    push_id: u64,
    /// When it lapses, if the reader asks nothing more.
    until: Instant,
}

/// The watches on the change feed: one per reader, at most
/// [`MAX_WATCHERS`] at once.
#[derive(Debug, Default)]
struct Watches(HashMap<PeerId, Watch>);

impl Watches {
    /// A `ChangesRequest` from `peer` at `now`: its watch, if any, ends, and
    /// `new` is set in its place if given, unless [`MAX_WATCHERS`] other
    /// readers still watch, so a reader that watches is never pushed out.
    /// Returns whether `new` was set.
    fn renew(&mut self, peer: PeerId, new: Option<Watch>, now: Instant) -> bool {
        self.0.retain(|_, watch| watch.until > now);
        self.0.remove(&peer);
        match new {
            Some(watch) if self.0.len() < MAX_WATCHERS => {
                self.0.insert(peer, watch);
                true
            }
            _ => false,
        }
    }

    /// Every watch set, lapsed or not.
    fn all(&self) -> Vec<(PeerId, Watch)> {
        self.0.iter().map(|(peer, watch)| (*peer, *watch)).collect()
    }

    /// Whether `peer` still watches with the watch whose pushes carry
    /// `push_id`, unlapsed at `now`.
    fn holds(&self, peer: &PeerId, push_id: u64, now: Instant) -> bool {
        self.0
            .get(peer)
            .is_some_and(|watch| watch.push_id == push_id && watch.until > now)
    }
}

/// A push to a reader watching a range: the new records in it, made as they
/// commit, and the request id the reader's watch gives its pushes. The view
/// is filled in as it is sent.
struct Push {
    peer: PeerId,
    request_id: u64,
    response: ChangesResponse,
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
    /// Nothing to take, ever: no copy the peers served can check out,
    /// whatever the chain says later
    /// ([`ListingService::none_can_check_out`]).
    Refused,
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
    /// go on past the end of the retry schedule while the chain can't be
    /// read, for at most [`MAX_RECHECK_TIME`]; until it answers the listing
    /// held stays as it is, a provisional one unserved.
    ChainUnread,
    /// An envelope in the index held without a listing, none taken: the
    /// close group didn't answer, or had nothing to take yet. If `definite`,
    /// no copy it served can ever check out ([`Repair::Refused`]). Once the
    /// retry schedule has run out, the checks of such an envelope end there
    /// if `definite`, and otherwise go on as for [`Checked::ChainUnread`].
    Unlisted {
        /// Whether no copy the close group served can ever check out.
        definite: bool,
    },
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
    /// chain's RPC endpoint and payment vault, and `vault_deployment_block`
    /// the block the vault was deployed in, where known: a copy of a listing
    /// showing an earlier block paid something else, and is never taken.
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
        vault_deployment_block: Option<u64>,
    ) -> Result<Arc<Self>> {
        let store = ListingStore::open(root_dir, disk_reserve).await?;
        let watches = Arc::new(Mutex::new(Watches::default()));
        let (pushes_tx, pushes_rx) = mpsc::unbounded_channel();
        store.watch_feed(Self::feed_watcher(Arc::clone(&watches), pushes_tx));
        let service = Arc::new(Self {
            store,
            chunks: Arc::clone(&chunks),
            chain: Chain::with_rpcs(listings_rpc_urls(&rpc_url))?,
            vault,
            vault_deployment_block,
            p2p: RwLock::new(None),
            waiting: Waiting::default(),
            repairs: Semaphore::new(MAX_REPAIRS),
            requests: Semaphore::new(MAX_REQUESTS),
            next_recheck: Mutex::new(Instant::now()),
            fresh_offers: Mutex::new(HashSet::new()),
            watches,
            #[cfg(test)]
            close_group: Mutex::new(None),
            #[cfg(test)]
            view: Mutex::new(None),
            #[cfg(test)]
            pushed: Mutex::new(None),
        });
        if let Some(block) = vault_deployment_block {
            info!(
                "Listings showing a block before {block}, when the payment vault was deployed, \
                 are never taken"
            );
        }

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
        tokio::spawn(Self::send_pushes(Arc::downgrade(&service), pushes_rx));
        tokio::spawn(Self::resume(Arc::downgrade(&service)));
        Ok(service)
    }

    /// Attach the node's P2P handle, used to ask other nodes for listings.
    pub fn attach_p2p_node(&self, node: Arc<P2PNode>) {
        *self.p2p.write() = Some(node);
    }

    /// The permits listing requests from other nodes take while they are
    /// answered, until each answer has been sent: [`MAX_REQUESTS`], apart
    /// from those chunk requests take. Answers to this node's own requests
    /// take none.
    pub fn request_permits(&self) -> &Semaphore {
        &self.requests
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

    /// Answer a message `source` sent on [`TOPIC`]. Returns the encoded
    /// response, or `None` for messages that need none, such as responses to
    /// this node's own requests. One that doesn't decode, but whose request
    /// id does, is a request of a kind this node doesn't know: it is answered
    /// `Error { "unsupported" }` at once, so that it costs its sender no wait
    /// (the routing hook first lets [`Self::handle_response`] hand it to a
    /// request of this node's waiting on it, if one is).
    pub async fn handle_message(&self, source: &PeerId, data: &[u8]) -> Option<Vec<u8>> {
        let message = match ListingsMessage::decode(data) {
            Ok(message) => message,
            Err(e) => {
                let Some(request_id) = request_id_of(data) else {
                    debug!("Ignoring an undecodable listings message from {source}: {e}");
                    return None;
                };
                debug!("Answering a listings message from {source} as unsupported: {e}");
                let refusal = ListingsBody::Error {
                    message: UNSUPPORTED.into(),
                };
                return encode_response(request_id, refusal);
            }
        };
        let request_id = message.request_id;
        let body = match message.body {
            ListingsBody::ListRequest { from, limit } => self.list(from, limit).await,
            ListingsBody::GetRequest { address } => ListingsBody::GetResponse {
                listing: self.held_listing(address).await,
            },
            ListingsBody::ChangesRequest(request) => {
                self.changes(source, request_id, &request).await
            }
            ListingsBody::RangeListRequest(request) => self.range_list(&request).await,
            _ => return None,
        };
        encode_response(request_id, body)
    }

    /// Take a message `source` sent on [`TOPIC`] if it needs no answer,
    /// handing an answer to one of this node's own requests to the request
    /// waiting for it. Returns `false` for a request, which
    /// [`Self::handle_message`] answers. A message that doesn't decode is
    /// such an answer, which can't be read, if a request of this node waits
    /// on its request id from `source`; otherwise, if its request id reads,
    /// it is a request of a kind this node doesn't know, and is left to be
    /// answered.
    pub fn handle_response(&self, source: &PeerId, data: &[u8]) -> bool {
        let message = match ListingsMessage::decode(data) {
            Ok(message) => message,
            Err(e) => {
                let Some(request_id) = request_id_of(data) else {
                    debug!("Ignoring an undecodable listings message from {source}: {e}");
                    return true;
                };
                let waiting = self.waiting.lock().remove(&(*source, request_id));
                if let Some(answer) = waiting {
                    debug!("An answer from {source} to a listings request doesn't decode: {e}");
                    let _ = answer.send(None);
                    return true;
                }
                return false;
            }
        };
        match message.body {
            ListingsBody::ListRequest { .. }
            | ListingsBody::GetRequest { .. }
            | ListingsBody::ChangesRequest(_)
            | ListingsBody::RangeListRequest(_) => false,
            ListingsBody::GetResponse { listing } => {
                route_response(&self.waiting, source, message.request_id, listing);
                true
            }
            // Nodes send no list or feed requests: nothing waits for these.
            ListingsBody::ListResponse { .. }
            | ListingsBody::Error { .. }
            | ListingsBody::ChangesResponse(_)
            | ListingsBody::RangeListResponse(_) => true,
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

    /// The answer to `request`, request `request_id` from `source`: the
    /// change feed's records in its range after its `after`, at most
    /// [`MAX_LIST_LIMIT`] and as many as reading [`MAX_FEED_READ`] records
    /// finds, with where the feed stands, the range's totals and this node's
    /// view. The request ends `source`'s watch, and sets a new one if it
    /// asks for one and fewer than [`MAX_WATCHERS`] other readers watch
    /// (answered either way). That is done before the feed is read, so a
    /// record made meanwhile is pushed, or in the answer, or both.
    async fn changes(
        &self,
        source: &PeerId,
        request_id: u64,
        request: &ChangesRequest,
    ) -> ListingsBody {
        let range = request.range();
        let now = Instant::now();
        let watch = request.watch.then(|| Watch {
            range,
            push_id: push_request_id(request_id),
            until: now + Duration::from_secs(WATCH_LEASE),
        });
        let watching = self.watches.lock().renew(*source, watch, now);
        if request.watch && !watching {
            debug!("Refused a watch from {source}: {MAX_WATCHERS} readers already watch");
        }
        let limit = usize::try_from(request.limit.min(MAX_LIST_LIMIT)).unwrap_or(0);
        let page = match self
            .store
            .feed_page(request.epoch, request.after, range, limit, MAX_FEED_READ)
            .await
        {
            Ok(Some(page)) => page,
            failed => {
                if let Err(e) = failed {
                    warn!("Could not read the listing change feed: {e}");
                }
                // Refused, so no watch is kept for it.
                self.watches.lock().renew(*source, None, now);
                return feed_unavailable();
            }
        };
        let peers = self.view_peers().await;
        let more = page.more();
        ListingsBody::ChangesResponse(ChangesResponse {
            epoch: page.epoch,
            head: page.head,
            next: page.next,
            count: page.count,
            set_hash: page.set_hash,
            changes: page.changes,
            more,
            view: view_hash(&peers),
            peers: if request.want_peers {
                peers
            } else {
                Vec::new()
            },
        })
    }

    /// The answer to `request`: the digest entries the change feed holds in
    /// its range after its `after`, in address order, at most
    /// [`MAX_LIST_LIMIT`]. An entry whose chunk has gone is dropped with its
    /// listing, as [`Self::list`] drops one, and the page reads on past it.
    /// One whose chunk can't be read now, or whose listing can't be dropped
    /// now, stays, as the feed and its totals keep it: a reader's copy of
    /// the range then matches them.
    async fn range_list(&self, request: &RangeListRequest) -> ListingsBody {
        let range = request.range();
        let limit = usize::try_from(request.limit.min(MAX_LIST_LIMIT)).unwrap_or(0);
        let mut entries = Vec::with_capacity(limit);
        let mut after = request.after;
        loop {
            let (page, more) = match self
                .store
                .feed_entries(range, after, limit - entries.len())
                .await
            {
                Ok(Some(page)) => page,
                Ok(None) => return feed_unavailable(),
                Err(e) => {
                    warn!("Could not read the listing change feed: {e}");
                    return feed_unavailable();
                }
            };
            after = page.last().map(|entry| entry.address).or(after);
            for entry in page {
                if self.held(&entry.address).await == Held::Gone
                    && self.forget(&entry.address).await
                {
                    continue;
                }
                entries.push(entry);
            }
            if !more || entries.len() >= limit {
                return ListingsBody::RangeListResponse(RangeListResponse { entries, more });
            }
        }
    }

    /// This node's nearest peers in its routing table, at most
    /// [`VIEW_PEERS`], in increasing order: the view a `ChangesResponse`
    /// gives the hash of.
    async fn view_peers(&self) -> Vec<[u8; 32]> {
        #[cfg(test)]
        {
            let given = self.view.lock().clone();
            if let Some(peers) = given {
                return peers;
            }
        }
        let Some(p2p) = self.p2p.read().clone() else {
            return Vec::new();
        };
        let mut peers: Vec<[u8; 32]> = p2p
            .dht_manager()
            .find_closest_nodes_local(p2p.peer_id().as_bytes(), VIEW_PEERS)
            .await
            .into_iter()
            .map(|node| *node.peer_id.as_bytes())
            .collect();
        peers.sort_unstable();
        peers.dedup();
        peers.truncate(VIEW_PEERS);
        peers
    }

    /// The store's feed watcher: for each watch whose range a committed
    /// write added records to, a push of those records with the range's
    /// totals at the write's head, queued for [`Self::send_pushes`]. It runs
    /// as the write commits, so it only queues.
    fn feed_watcher(
        watches: Arc<Mutex<Watches>>,
        pushes: mpsc::UnboundedSender<Push>,
    ) -> FeedWatcher {
        Box::new(move |commit| {
            let watching = watches.lock().all();
            for (peer, watch) in watching {
                let changes: Vec<FeedChange> = commit
                    .records
                    .iter()
                    .filter(|record| watch.range.contains(&record.address()))
                    .copied()
                    .collect();
                // One write records one address, but for the start's moves:
                // a push no reader could take is left to its next request.
                if changes.is_empty() || changes.len() > MAX_PUSHED {
                    continue;
                }
                let Some((count, set_hash)) = commit.totals(&watch.range) else {
                    continue;
                };
                let _ = pushes.send(Push {
                    peer,
                    request_id: watch.push_id,
                    response: ChangesResponse {
                        epoch: commit.epoch,
                        head: commit.head,
                        next: commit.head,
                        count,
                        set_hash,
                        changes,
                        more: false,
                        view: [0; 32],
                        peers: Vec::new(),
                    },
                });
            }
        })
    }

    /// Send each push the feed watcher queues to its reader, while the
    /// reader's watch holds, with this node's view. Each is sent on its own
    /// and never waited for or acknowledged, so a failed send drops nothing
    /// else: the reader's next request catches up on what it missed.
    async fn send_pushes(service: Weak<Self>, mut pushes: mpsc::UnboundedReceiver<Push>) {
        while let Some(push) = pushes.recv().await {
            let Some(listings) = service.upgrade() else {
                return;
            };
            let holds = listings
                .watches
                .lock()
                .holds(&push.peer, push.request_id, Instant::now());
            if !holds {
                continue;
            }
            let peers = listings.view_peers().await;
            let message = ListingsMessage {
                request_id: push.request_id,
                body: ListingsBody::ChangesResponse(ChangesResponse {
                    view: view_hash(&peers),
                    ..push.response
                }),
            };
            match message.encode() {
                Ok(bytes) => listings.push_to(push.peer, bytes),
                Err(e) => warn!("Could not encode a listings push: {e}"),
            }
        }
    }

    /// Send `bytes`, a push, to `peer`, without waiting for it.
    fn push_to(&self, peer: PeerId, bytes: Vec<u8>) {
        #[cfg(test)]
        if let Some(pushed) = self.pushed.lock().as_mut() {
            pushed.push((peer, bytes));
            return;
        }
        let Some(p2p) = self.p2p.read().clone() else {
            return;
        };
        tokio::spawn(async move {
            if let Err(e) = p2p.send_message(&peer, TOPIC, bytes, &[]).await {
                debug!("Could not push a listings feed record to {peer}: {e}");
            }
        });
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

    /// Remove the listing of a chunk this node no longer holds. Returns
    /// whether the store holds none for it now: false if it couldn't be
    /// removed.
    async fn forget(&self, address: &XorName) -> bool {
        match self.store.remove(*address).await {
            Ok(removed) => {
                if removed {
                    debug!(
                        "Dropped the listing of {} with its chunk",
                        hex::encode(address)
                    );
                }
                true
            }
            Err(e) => {
                warn!(
                    "Could not drop the listing of {}: {e}",
                    hex::encode(address)
                );
                false
            }
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
    /// removal reaching them (the node stopped first). The checks of an
    /// envelope whose schedule had run out go on as re-checks, ending when
    /// they would have.
    async fn resume(service: Weak<Self>) {
        let Some(listings) = service.upgrade() else {
            return;
        };
        // Each envelope, with when its re-checks began, if they have.
        let mut checks: BTreeMap<XorName, Option<u64>> = BTreeMap::new();
        match listings.store.provisional_addresses().await {
            Ok(provisional) => {
                checks.extend(provisional.into_iter().map(|address| (address, None)));
            }
            Err(e) => warn!("Could not read the provisional listings: {e}"),
        }
        match listings.store.pending_checks().await {
            Ok(pending) => checks.extend(pending),
            Err(e) => warn!("Could not read the envelopes still being checked: {e}"),
        }
        drop(listings);
        for (address, rechecks_since) in checks {
            tokio::spawn(Self::ensure_listing_with_retries(
                service.clone(),
                address,
                rechecks_since,
            ));
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
            tokio::spawn(Self::ensure_listing_with_retries(
                service.clone(),
                address,
                None,
            ));
        }
    }

    /// Run [`Self::ensure_listing`] on the retry schedule until it is done or
    /// the schedule runs out. Then, if the last check was inconclusive (the
    /// chain couldn't be read, [`Checked::ChainUnread`], or an envelope in
    /// the index still has no listing to take, [`Checked::Unlisted`]), the
    /// checks go on as re-checks: each wait twice the one before, up to
    /// [`MAX_UNREAD_DELAY`], for at most [`MAX_RECHECK_TIME`], and each in
    /// the next of the slots all re-checks share, [`RECHECK_SPACING`] apart.
    /// An envelope in the index whose close group serves only copies that
    /// can never check out is given up at once. `rechecks_since` is when a
    /// previous run began the envelope's re-checks, if it did: they go on
    /// from the next slot, and end when they would have.
    async fn ensure_listing_with_retries(
        service: Weak<Self>,
        address: XorName,
        rechecks_since: Option<u64>,
    ) {
        tokio::time::sleep(CHECK_DELAY).await;
        let mut retries = RETRY_DELAYS.iter().copied();
        let mut delay = CHECK_DELAY;
        // When the re-checks end, once they have begun.
        let mut rechecks_until = None;
        if let Some(since) = rechecks_since {
            let Some(until) = rechecks_end(since) else {
                if let Some(listings) = service.upgrade() {
                    listings.end_checks(&address, RECHECKS_RAN_OUT).await;
                }
                return;
            };
            rechecks_until = Some(until);
            retries.by_ref().for_each(|wait| delay = wait);
        }
        loop {
            if let Some(until) = rechecks_until {
                let Some(listings) = service.upgrade() else {
                    return;
                };
                let Some(slot) = listings.recheck_slot(until) else {
                    listings.end_checks(&address, RECHECKS_RAN_OUT).await;
                    return;
                };
                drop(listings);
                tokio::time::sleep_until(slot).await;
            }
            let Some(listings) = service.upgrade() else {
                return;
            };
            let outcome = match listings.repairs.acquire().await {
                Ok(_repairing) => listings.ensure_listing(&address).await,
                Err(_) => return,
            };
            let outcome = match outcome {
                Ok(Checked::Done) => {
                    listings.done_checking(&address).await;
                    return;
                }
                Ok(outcome) => Some(outcome),
                Err(e) => {
                    debug!("Listing check for {} failed: {e}", hex::encode(address));
                    None
                }
            };
            if let Some(next) = retries.next() {
                delay = next;
            } else {
                match outcome {
                    Some(Checked::Unlisted { definite: true }) => {
                        listings
                            .end_checks(
                                &address,
                                "no copy its close group serves can check out, whatever \
                                 the chain says later",
                            )
                            .await;
                        return;
                    }
                    Some(Checked::ChainUnread | Checked::Unlisted { definite: false }) => {
                        let until = if let Some(until) = rechecks_until {
                            until
                        } else {
                            let chain_unread = outcome == Some(Checked::ChainUnread);
                            let until = listings.begin_rechecks(&address, chain_unread).await;
                            rechecks_until = Some(until);
                            until
                        };
                        delay = delay.saturating_mul(2).min(MAX_UNREAD_DELAY);
                        if Instant::now() + delay > until {
                            listings.end_checks(&address, RECHECKS_RAN_OUT).await;
                            return;
                        }
                    }
                    _ => {
                        listings.give_up(&address).await;
                        return;
                    }
                }
            }
            drop(listings);
            tokio::time::sleep(delay).await;
        }
    }

    /// The slot for the next check made after its schedule has run out: the
    /// first at least [`RECHECK_SPACING`] after the last slot given, and not
    /// before now. Re-checks are spread that far apart, so a node makes at
    /// most [`RECHECKS_PER_HOUR`] an hour, however many envelopes it checks
    /// again. `None` if that slot is after `until`, when the envelope's
    /// re-checks end: the slot is then left for the next envelope.
    fn recheck_slot(&self, until: Instant) -> Option<Instant> {
        let mut next = self.next_recheck.lock();
        let slot = (*next).max(Instant::now());
        if slot > until {
            return None;
        }
        *next = slot + RECHECK_SPACING;
        drop(next);
        Some(slot)
    }

    /// The checks of `address` go on past their schedule from now, as
    /// re-checks: record when they began, so that a restart keeps their end,
    /// and say why. Returns when they end.
    async fn begin_rechecks(&self, address: &XorName, chain_unread: bool) -> Instant {
        if let Err(e) = self.store.mark_rechecking(*address, unix_now()).await {
            warn!(
                "Could not record when the checks of envelope {} began again: {e}",
                hex::encode(address)
            );
        }
        let days = MAX_RECHECK_TIME.as_secs() / 86_400;
        if chain_unread {
            warn!(
                "Could not read the payment chain to check the listing of {}; checking it \
                 again for up to {days} days",
                hex::encode(address)
            );
        } else {
            info!(
                "The close group of envelope {} has no listing to take yet; checking it again \
                 for up to {days} days",
                hex::encode(address)
            );
        }
        Instant::now() + MAX_RECHECK_TIME
    }

    /// Stop checking the listing of the envelope at `address`, saying why.
    async fn end_checks(&self, address: &XorName, why: &str) {
        warn!(
            "Stopped checking the listing of envelope {}: {why}",
            hex::encode(address)
        );
        self.done_checking(address).await;
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
        // An envelope in the index held without any listing, provisional or
        // not: its checks go on past the schedule while inconclusive.
        let unlisted = held.is_none() && !provisional && envelope.flags().in_index;
        let (asked, answers) = match self.ask_close_group(address).await {
            Ok(asked) => asked,
            Err(e) if unlisted => {
                debug!(
                    "Could not ask the close group of {} for its listing: {e}",
                    hex::encode(address)
                );
                return Ok(Checked::Unlisted { definite: false });
            }
            Err(e) => return Err(e),
        };
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
        let refused = repair == Repair::Refused;
        let taken = matches!(repair, Repair::Settled(_) | Repair::Provisional(_));
        Ok(if self.apply_repair(address, provisional, repair).await? {
            Checked::Done
        } else if unread {
            Checked::ChainUnread
        } else if unlisted && !taken {
            Checked::Unlisted { definite: refused }
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
            Repair::Nothing | Repair::Unread | Repair::Refused => Ok(false),
        }
    }

    /// Decide on the close group's answers, checking on chain the copies that
    /// could be taken. The copy a quorum serves is looked up first: if it
    /// checks out, it is taken whatever the others are, so they are never
    /// looked up. Any other copy can come from one peer, with a quote of its
    /// own dated whenever it likes, so the search for its payment is brief
    /// ([`Search::Brief`]). When nothing is taken and no copy could ever be,
    /// the answer is [`Repair::Refused`] ([`Self::none_can_check_out`]).
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
        // The copies looked up that can't check out whatever the chain says.
        let mut refused = vec![false; groups.len()];
        for &i in &quorum {
            if let Some(listing) = quorum_listing(&groups[i], asked, needed) {
                fetched[i] = self.fetch_for(envelope, &listing, Search::Full).await?;
                refused[i] = fetched[i].is_none();
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
                        continue;
                    }
                };
                refused[i] = fetched[i].is_none();
            }
        }
        let decided = decide_repair(envelope, asked, &groups, &fetched, &self.vault, now);
        if decided != Repair::Nothing {
            return Ok(decided);
        }
        if unread {
            return Ok(Repair::Unread);
        }
        Ok(
            if self
                .none_can_check_out(envelope, asked, &groups, &fetched, &refused)
                .await
            {
                Repair::Refused
            } else {
                Repair::Nothing
            },
        )
    }

    /// Whether no copy in `groups`, the close group's answers, none of them
    /// taken, can ever be taken, whatever the chain says later, so that
    /// checking again would change nothing. Each must fail a check that needs
    /// nothing from the chain (`refused` marks those found so when looked
    /// up; see [`Self::refused_offline`]), or fail the check of what it
    /// shows against what was `fetched` for it in a way the chain has
    /// finalized ([`fails_for_good`]). A copy not looked up because too few
    /// peers serve it, one whose payment isn't found, and one found only in
    /// blocks not yet final could still be taken. `false` when there is no
    /// copy at all: the close group has nothing to say yet.
    async fn none_can_check_out(
        &self,
        envelope: &Envelope,
        asked: &[PeerId],
        groups: &[MergedListing<PeerId>],
        fetched: &[Option<Fetched>],
        refused: &[bool],
    ) -> bool {
        if groups.is_empty() {
            return false;
        }
        let needed = quorum_needed(asked.len());
        let now = unix_now();
        // The chain's finalized block, read once, when first needed.
        let mut final_block: Option<u64> = None;
        for ((group, fetched), refused) in groups.iter().zip(fetched).zip(refused) {
            if *refused {
                continue;
            }
            let Some(listing) = listing_to_check(group, asked, needed) else {
                return false;
            };
            if self.refused_offline(envelope, &listing).is_some() {
                continue;
            }
            let Some(found) = fetched else {
                return false;
            };
            let checked = check_copies(group, asked, needed, |listing| {
                verify_listing_receipts(listing, envelope, found, &self.vault, now)
            });
            let (Err(error), Ok(lookup)) = (checked, payment_lookup(&listing)) else {
                return false;
            };
            let finalized = match final_block {
                Some(block) => block,
                None => match self.chain.finalized_block().await {
                    Ok(block) => *final_block.insert(block),
                    Err(e) => {
                        debug!("Could not read the payment chain's finalized block: {e}");
                        return false;
                    }
                },
            };
            if !fails_for_good(&error, &lookup, found, finalized) {
                return false;
            }
        }
        true
    }

    /// Why `listing`, a copy of `envelope`'s listing, can't check out
    /// whatever the chain says, if it can't: it shows a block before the
    /// payment vault was deployed, so it paid something else, or it fails a
    /// check that needs nothing from the chain ([`check_offline`]).
    fn refused_offline(&self, envelope: &Envelope, listing: &Listing) -> Option<String> {
        if let (Some(block), Some(deployed)) =
            (listing.payment.block_number, self.vault_deployment_block)
        {
            if block < deployed {
                return Some(format!(
                    "paid in block {block}, before the payment vault was deployed in block \
                     {deployed}"
                ));
            }
        }
        check_offline(listing, envelope, &self.vault)
            .err()
            .map(|e| e.to_string())
    }

    /// What the chain has for `listing`, a copy the close group serves,
    /// fetched once, searching for the payment of a quote as far as `search`
    /// goes. `None` if the copy can't check out whatever the chain says
    /// ([`Self::refused_offline`]): it is never looked up.
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
        if let Some(why) = self.refused_offline(envelope, listing) {
            debug!(
                "A listing of {} is unusable: {why}",
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

/// Why the checks of an envelope end once their re-checks run out.
const RECHECKS_RAN_OUT: &str =
    "the checks stayed inconclusive for as long as they go on past their schedule";

/// When re-checks that began at `since` (Unix seconds) end, on this run's
/// clock: [`MAX_RECHECK_TIME`] after they began, and never further off than
/// that from now. `None` if that has passed.
fn rechecks_end(since: u64) -> Option<Instant> {
    let end = since.saturating_add(MAX_RECHECK_TIME.as_secs());
    let left = end.checked_sub(unix_now()).filter(|left| *left > 0)?;
    Some(Instant::now() + Duration::from_secs(left).min(MAX_RECHECK_TIME))
}

/// Whether `error`, from checking a copy of a listing against `fetched`,
/// what the chain had for its `lookup`, can't change: the chain shows, in
/// blocks at or before `finalized`, the last it has finalized, that the
/// payment the copy describes is not a payment of its quote, or for its
/// address, to the vault, or is not where or when the copy says. A payment
/// not found, or found only in blocks not yet final, could still check out.
fn fails_for_good(
    error: &PaymentError,
    lookup: &Lookup,
    fetched: &Fetched,
    finalized: u64,
) -> bool {
    let shown_by_the_chain = matches!(
        error,
        PaymentError::NotPaid
            | PaymentError::Failed
            | PaymentError::WrongTransaction
            | PaymentError::WrongBlock
            | PaymentError::WrongTimestamp
            | PaymentError::FirstSeenTooEarly
    );
    if !shown_by_the_chain {
        return false;
    }
    match lookup {
        // No receipt: the block's timestamp, checked.
        Lookup::Block { block_number } => {
            *block_number <= finalized && fetched.iter().any(|(_, timestamp)| timestamp.is_some())
        }
        _ => {
            !fetched.is_empty()
                && fetched.iter().all(|(receipt, _)| {
                    receipt
                        .as_ref()
                        .is_some_and(|receipt| receipt.block_number <= finalized)
                })
        }
    }
}

/// The answer to a feed request while the change feed can't be read.
fn feed_unavailable() -> ListingsBody {
    ListingsBody::Error {
        message: "the change feed is unavailable".into(),
    }
}

/// The response to request `request_id`, encoded.
fn encode_response(request_id: u64, body: ListingsBody) -> Option<Vec<u8>> {
    ListingsMessage { request_id, body }
        .encode()
        .map_err(|e| warn!("Could not encode a listings response: {e}"))
        .ok()
}

/// The request id `data`, a message on [`TOPIC`], starts with, if it reads
/// as one, whether or not the rest does.
fn request_id_of(data: &[u8]) -> Option<u64> {
    postcard::take_from_bytes::<u64>(data)
        .ok()
        .map(|(request_id, _)| request_id)
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

/// Tests, and the helpers `hooks_tests` shares.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
pub(super) mod tests {
    use super::super::chain::tests::{chain_with, vault_log, MockChain, VAULT};
    use super::super::store::tests::{signed_quote, signed_quote_at};
    use super::*;
    use crate::storage::ChunkStoreConfig;
    use ::ant_protocol::payment::proof::{serialize_single_node_proof, PaymentProof};
    use ant_listings::self_encryption::{ChunkInfo, DataMap, XorName as SeXorName};
    use ant_listings::{
        check_changes_page, check_push, check_range_page, encode_data_map, first_seen_at,
        ChangesPage, DigestEntry, FeedSync, Flags, ListingPayment, Metadata, PaymentReceipt,
        Pushed, SetHash, ShownFields, MAGIC,
    };
    use tempfile::TempDir;

    const BLOCK_TIME: u64 = 1_790_000_000;

    /// A chain endpoint nothing answers at.
    pub const DEAD_CHAIN: &str = "http://127.0.0.1:0";

    /// A listing service over a new chunk store in `dir`, reading payments
    /// through `rpc_url`.
    pub async fn service_on(
        dir: &TempDir,
        rpc_url: &str,
    ) -> (Arc<ListingService>, Arc<ChunkStore>) {
        service_with(dir, rpc_url, None).await
    }

    /// [`service_on`], knowing that the vault was deployed in
    /// `vault_deployment_block`.
    async fn service_with(
        dir: &TempDir,
        rpc_url: &str,
        vault_deployment_block: Option<u64>,
    ) -> (Arc<ListingService>, Arc<ChunkStore>) {
        let chunks = Arc::new(
            ChunkStore::new(ChunkStoreConfig {
                root_dir: dir.path().to_path_buf(),
                ..ChunkStoreConfig::test_default()
            })
            .await
            .unwrap(),
        );
        let listings = ListingService::new(
            dir.path(),
            0,
            Arc::clone(&chunks),
            rpc_url.into(),
            VAULT,
            vault_deployment_block,
        )
        .await
        .unwrap();
        (listings, chunks)
    }

    /// A service whose chain is unreachable: for tests that never reach it.
    /// Every system refuses port 0 at once, so this works on a paused clock.
    async fn service(dir: &TempDir) -> (Arc<ListingService>, Arc<ChunkStore>) {
        service_on(dir, DEAD_CHAIN).await
    }

    /// The listing service of a node starting again on the store in `dir`
    /// and on `chunks`, its chain unreachable. The chunk store tells the
    /// first service only of chunks stored and removed.
    async fn restart(dir: &TempDir, chunks: &Arc<ChunkStore>) -> Arc<ListingService> {
        ListingService::new(
            dir.path(),
            0,
            Arc::clone(chunks),
            DEAD_CHAIN.into(),
            VAULT,
            None,
        )
        .await
        .unwrap()
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

    /// A readable envelope of `data_map(seed)`, showing `shown`.
    pub fn envelope_with(seed: u8, shown: ShownFields, in_index: bool) -> Envelope {
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

    /// The peer the tests' requests come from, unless they say otherwise.
    fn reader() -> PeerId {
        PeerId::from_bytes([0xee; 32])
    }

    /// The answer `listings` gives `source` for `body`, as request
    /// `request_id`.
    async fn ask_as(
        listings: &ListingService,
        source: &PeerId,
        request_id: u64,
        body: ListingsBody,
    ) -> ListingsBody {
        let request = ListingsMessage { request_id, body }.encode().unwrap();
        let response = listings.handle_message(source, &request).await.unwrap();
        let response = ListingsMessage::decode(&response).unwrap();
        assert_eq!(response.request_id, request_id);
        response.body
    }

    async fn ask(listings: &ListingService, body: ListingsBody) -> ListingsBody {
        ask_as(listings, &reader(), 42, body).await
    }

    /// The listing `listings` serves for `address`, asked on the listings
    /// topic.
    pub async fn get(listings: &ListingService, address: XorName) -> Option<Listing> {
        let ListingsBody::GetResponse { listing } =
            ask(listings, ListingsBody::GetRequest { address }).await
        else {
            panic!("expected a get response");
        };
        listing
    }

    /// A page of the index `listings` serves, asked on the listings topic.
    pub async fn list(
        listings: &ListingService,
        from: ListingKey,
        limit: u32,
    ) -> (Vec<Listing>, bool) {
        let ListingsBody::ListResponse { listings, more } =
            ask(listings, ListingsBody::ListRequest { from, limit }).await
        else {
            panic!("expected a list response");
        };
        (listings, more)
    }

    /// The digest entries in the change feed of `listings`' store, in
    /// address order.
    pub async fn feed_entries(listings: &ListingService) -> Vec<DigestEntry> {
        let (entries, more) = listings
            .store
            .feed_entries(AddressRange::ALL, None, usize::MAX)
            .await
            .unwrap()
            .expect("the change feed can be read");
        assert!(!more);
        entries
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
        assert!(listings
            .handle_message(&reader(), &response)
            .await
            .is_none());
        // Not even a request id reads: nothing to answer.
        assert!(listings
            .handle_message(&reader(), &[0xff; 4])
            .await
            .is_none());
        assert!(listings.handle_message(&reader(), &[]).await.is_none());
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
    pub fn single_node_proof(quote: &PaymentQuote, tx_hash: [u8; 32]) -> Vec<u8> {
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
        // Nothing to take, ever: none of the copies can check out.
        assert_eq!(
            listings
                .repair_from(&envelope, &asked, vec![(asked[0], mismatched)])
                .await
                .unwrap(),
            Repair::Refused
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
            Repair::Refused
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
            Repair::Refused
        );
        assert!(chain.lock().calls.is_empty(), "{:?}", chain.lock().calls);

        // A signed quote nobody paid is looked up: it could have checked
        // out, and could still.
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
        let chunks = dir
            .path()
            .join(crate::storage::chunk_store::CHUNKS_DIR_NAME);
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
        let listings = restart(&dir, &chunks).await;

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

    /// Envelopes and a record exactly as the client writes them (`golden`,
    /// whose literals darwin-client pins too): they read back as themselves,
    /// the same inputs build the same bytes under this lock, and a node lists
    /// them as the client chose, finding each payment from the proof its PUT
    /// carries.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn golden_envelopes_list_as_the_client_writes_them() {
        use super::super::golden;
        use ant_listings::{salted_hash, EnvelopeProof};

        let hex = |text: &str| hex::decode(text).unwrap();
        let address = |text: &str| -> XorName { hex(text).try_into().unwrap() };
        let map_bytes = hex(golden::DATA_MAP);

        // The data maps and the salted hash, under this lock's encoders.
        assert_eq!(encode_data_map(&golden::data_map()).unwrap(), map_bytes);
        assert_eq!(
            encode_data_map(&golden::child_map()).unwrap(),
            hex(golden::CHILD_MAP)
        );
        let child: DataMap = rmp_serde::from_slice(&hex(golden::CHILD_MAP)).unwrap();
        assert_eq!(child, golden::child_map());
        assert_eq!(
            salted_hash(&golden::SALT, &map_bytes).to_vec(),
            hex(golden::SALTED_HASH)
        );

        // Each envelope reads back as its own bytes, at the address the client
        // stores it at, with the flags, metadata and size the client chose.
        let flags = |in_index, withheld| Flags {
            shown: ShownFields::ALL,
            in_index,
            withheld,
        };
        let mut envelopes = Vec::new();
        for (bytes, at, flags, size) in [
            (
                golden::PRIVATE_ENVELOPE,
                golden::PRIVATE_ADDRESS,
                flags(false, true),
                None,
            ),
            (
                golden::UNLISTED_ENVELOPE,
                golden::UNLISTED_ADDRESS,
                flags(false, false),
                Some(11_999),
            ),
            (
                golden::LISTED_ENVELOPE,
                golden::LISTED_ADDRESS,
                flags(true, false),
                Some(11_999),
            ),
        ] {
            let bytes = hex(bytes);
            let envelope = Envelope::from_bytes(&bytes).unwrap();
            envelope.check_new_upload().unwrap();
            assert_eq!(envelope.to_bytes(), bytes);
            assert_eq!(envelope.address(), address(at));
            assert_eq!(crate::client::compute_address(&bytes), address(at));
            assert_eq!(envelope.flags(), flags);
            assert_eq!(envelope.metadata(), &golden::metadata());
            assert_eq!(envelope.file_size(), size);
            envelopes.push(envelope);
        }
        let [private, unlisted, listed] = <[Envelope; 3]>::try_from(envelopes).unwrap();
        // The same envelopes, built here from the inputs.
        assert_eq!(
            Envelope::withheld(
                &golden::data_map(),
                &golden::SALT,
                golden::metadata(),
                ShownFields::ALL,
                false
            )
            .unwrap()
            .to_bytes(),
            private.to_bytes()
        );
        for (in_index, envelope) in [(false, &unlisted), (true, &listed)] {
            let built = Envelope::readable(
                &golden::data_map(),
                golden::metadata(),
                ShownFields::ALL,
                in_index,
            )
            .unwrap();
            assert_eq!(built.to_bytes(), envelope.to_bytes());
        }

        // The readable ones carry the data map as stored; the private one only
        // its salted hash, which the data map and the salt reveal.
        for envelope in [&unlisted, &listed] {
            assert_eq!(envelope.data_map_bytes(), Some(map_bytes.as_slice()));
            assert_eq!(envelope.data_map(), Some(&golden::data_map()));
        }
        assert_eq!(private.data_map(), None);
        assert_eq!(
            private.salted_hash().map(|hash| hash.to_vec()),
            Some(hex(golden::SALTED_HASH))
        );
        assert!(private.check_reveal(&map_bytes, &golden::SALT).is_ok());
        assert!(private.check_reveal(&map_bytes, &[4; 32]).is_err());

        // The private upload's record: its envelope and payment, and the data
        // map and salt that reveal it, in the one encoding a proof has.
        let record = EnvelopeProof::from_hex(golden::PRIVATE_RECORD).unwrap();
        assert_eq!(record.to_hex().unwrap(), golden::PRIVATE_RECORD);
        assert_eq!(record.address, private.address());
        assert_eq!(record.envelope, private.to_bytes());
        assert_eq!(record.tx_hash, golden::PRIVATE_RECORD_TX);
        assert_eq!(record.data_map.as_deref(), Some(map_bytes.as_slice()));
        assert_eq!(record.salt, Some(golden::SALT));
        let private_quote: PaymentQuote = rmp_serde::from_slice(&record.quote).unwrap();
        assert_eq!(private_quote.content.0, private.address());

        // A node finds each payment from the proof its PUT carries, the private
        // one in the record's transaction, and only the listed one is in the
        // index.
        let paid = [
            (&private, private_quote, golden::PRIVATE_RECORD_TX),
            (
                &unlisted,
                signed_quote(unlisted.address(), 0, 88),
                [0x61; 32],
            ),
            (&listed, signed_quote(listed.address(), 0, 89), [0x62; 32]),
        ];
        let mut chain = chain_with(400, &[]);
        for (block, (_, quote, tx_hash)) in (300..).zip(&paid) {
            chain.add_receipt(PaymentReceipt {
                tx_hash: *tx_hash,
                success: true,
                block_number: block,
                logs: vec![vault_log(quote.hash().0, 2)],
            });
        }
        let url = MockChain::serve(Arc::new(Mutex::new(chain))).await;
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_on(&dir, &url).await;
        for (envelope, quote, tx_hash) in &paid {
            let listing = listings
                .listing_for_put(
                    &envelope.address(),
                    &envelope.to_bytes(),
                    Some(&single_node_proof(quote, *tx_hash)),
                )
                .await
                .unwrap()
                .unwrap();
            assert!(listing.matches(envelope));
            assert!(listing.validate().is_ok());
            chunks
                .put(&envelope.address(), &envelope.to_bytes())
                .await
                .unwrap();
            assert!(listings.record(listing).await);
        }
        let (page, more) = list(&listings, ListingKey::MIN, 256).await;
        assert!(!more);
        assert_eq!(
            page.iter()
                .map(|listing| listing.address)
                .collect::<Vec<_>>(),
            vec![listed.address()]
        );
        // The change feed holds the listed one alone, as it is served.
        assert_eq!(
            feed_entries(&listings).await,
            vec![DigestEntry::new(&page[0])]
        );
        assert!(feed_entries(&listings).await[0]
            .describes(&get(&listings, listed.address()).await.unwrap()));
        for (envelope, quote, tx_hash) in &paid {
            let listing = get(&listings, envelope.address()).await.unwrap();
            assert_eq!(listing.flags, envelope.flags());
            assert_eq!(listing.metadata, golden::metadata());
            assert_eq!(listing.size, envelope.file_size());
            // Every payment field shown, as the client chose.
            assert_eq!(listing.payment.tx_hash, Some(*tx_hash));
            assert!(listing.payment.block_number.is_some());
            assert!(listing.payment.block_timestamp.is_some());
            assert_eq!(listing.payment.log_index, Some(2));
            assert_eq!(
                listing.payment.quote,
                Some(rmp_serde::to_vec(quote).unwrap())
            );
        }
    }

    // The change feed, served.

    /// A listing in the index at `address`, paid in block `block`, showing
    /// every payment field but the quote: all the change feed reads.
    fn listed_at(address: XorName, block: u64) -> Listing {
        Listing {
            address,
            flags: Flags {
                shown: ShownFields::ALL.without(PaymentField::Quote),
                in_index: true,
                withheld: false,
            },
            metadata: Metadata::default(),
            size: Some(10),
            payment: ListingPayment {
                tx_hash: Some([0x5a; 32]),
                block_number: Some(block),
                block_timestamp: Some(BLOCK_TIME + block),
                log_index: Some(0),
                quote: None,
            },
            first_seen: None,
        }
    }

    /// A `ChangesRequest` for the records in `range` after record `after`
    /// of `epoch`, a page at most.
    fn changes_request(range: AddressRange, epoch: u64, after: u64, watch: bool) -> ChangesRequest {
        ChangesRequest {
            epoch,
            after,
            prefix: range.prefix(),
            bits: range.bits(),
            limit: MAX_LIST_LIMIT,
            want_peers: false,
            watch,
        }
    }

    /// The answer `listings` gives `source` for `request`, as request
    /// `request_id`, and what a reader's check of it says.
    async fn changes_for(
        listings: &ListingService,
        source: &PeerId,
        request_id: u64,
        request: &ChangesRequest,
    ) -> (ChangesResponse, ChangesPage) {
        let body = ListingsBody::ChangesRequest(request.clone());
        let ListingsBody::ChangesResponse(response) =
            ask_as(listings, source, request_id, body).await
        else {
            panic!("expected a changes response");
        };
        let page = check_changes_page(request, &response).unwrap();
        (response, page)
    }

    /// Every entry `listings` lists in `range`, read `limit` at a time with
    /// `RangeListRequest`s, each page checked as a reader checks it.
    async fn range_listed(
        listings: &ListingService,
        range: AddressRange,
        limit: u32,
    ) -> Vec<DigestEntry> {
        let mut entries = Vec::new();
        let mut after = None;
        loop {
            let request = RangeListRequest {
                prefix: range.prefix(),
                bits: range.bits(),
                after,
                limit,
            };
            let body = ListingsBody::RangeListRequest(request.clone());
            let ListingsBody::RangeListResponse(response) = ask(listings, body).await else {
                panic!("expected a range list response");
            };
            let next = check_range_page(&request, &response).unwrap();
            entries.extend(response.entries);
            match next {
                Some(next) => after = Some(next),
                None => return entries,
            }
        }
    }

    /// The pushes `listings` has sent since they were last taken: the
    /// reader each went to, its request id, and the push.
    fn take_pushes(listings: &ListingService) -> Vec<(PeerId, u64, ChangesResponse)> {
        let pushed = std::mem::take(listings.pushed.lock().as_mut().unwrap());
        pushed
            .into_iter()
            .map(|(peer, bytes)| {
                let message = ListingsMessage::decode(&bytes).unwrap();
                let ListingsBody::ChangesResponse(push) = message.body else {
                    panic!("a push is a changes response");
                };
                (peer, message.request_id, push)
            })
            .collect()
    }

    /// Wait until `listings` has sent `count` pushes since they were last
    /// taken.
    async fn wait_for_pushes(listings: &Arc<ListingService>, count: usize) {
        let pushing = Arc::clone(listings);
        wait_until(async || pushing.pushed.lock().as_ref().unwrap().len() >= count).await;
    }

    #[tokio::test]
    async fn changes_are_answered_from_the_feed_with_the_ranges_totals() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        let view = vec![[3; 32], [1; 32], [2; 32]];
        *listings.view.lock() = Some(view.clone());
        // Records 1 to 3: two listings in the lower half of the address
        // space and one in the upper. Record 4: the first one's removal.
        for (address, block) in [(0x10u8, 1), (0x90, 2), (0x20, 3)] {
            let listing = listed_at([address; 32], block);
            listings.store.insert(listing).await.unwrap();
        }
        listings.store.remove([0x10; 32]).await.unwrap();
        let lower = AddressRange::new([0; 32], 1);
        let kept = DigestEntry::new(&listed_at([0x20; 32], 3));
        let reader = reader();

        // A reader new to the feed learns where it stands, with the range's
        // totals and the peers it asked for.
        let mut first = changes_request(lower, 0, 0, false);
        first.want_peers = true;
        let (answer, page) = changes_for(&listings, &reader, 1, &first).await;
        let epoch = answer.epoch;
        assert_eq!(page, ChangesPage::Restart { epoch, head: 4 });
        assert_eq!(
            (answer.next, answer.changes.len(), answer.more),
            (4, 0, false)
        );
        assert_eq!((answer.count, answer.set_hash), (1, SetHash::of([&kept])));
        assert_eq!(answer.view, view_hash(&view));
        let mut peers = answer.peers.clone();
        peers.sort_unstable();
        assert_eq!(peers, [[1; 32], [2; 32], [3; 32]]);

        // From the start: the range's records, in order, and no peers
        // unless asked for.
        let (answer, page) = changes_for(
            &listings,
            &reader,
            2,
            &changes_request(lower, epoch, 0, false),
        )
        .await;
        assert_eq!(page, ChangesPage::Follows(None));
        let records = vec![
            FeedChange::Entry {
                seq: 3,
                entry: kept,
            },
            FeedChange::Removed {
                seq: 4,
                address: [0x10; 32],
            },
        ];
        assert_eq!(answer.changes, records);
        assert_eq!(answer.peers, Vec::<[u8; 32]>::new());
        assert_eq!(answer.view, view_hash(&view));
        // A record at a time.
        let mut one = changes_request(lower, epoch, 0, false);
        one.limit = 1;
        let (answer, page) = changes_for(&listings, &reader, 3, &one).await;
        assert_eq!(page, ChangesPage::Follows(Some(3)));
        assert_eq!(answer.changes, &records[..1]);
        one.after = 3;
        let (answer, page) = changes_for(&listings, &reader, 4, &one).await;
        assert_eq!(page, ChangesPage::Follows(None));
        assert_eq!(answer.changes, &records[1..]);
        // The upper half has its own record and totals.
        let upper = AddressRange::new([0x80; 32], 1);
        let (answer, _) = changes_for(
            &listings,
            &reader,
            5,
            &changes_request(upper, epoch, 0, false),
        )
        .await;
        let seqs: Vec<u64> = answer.changes.iter().map(FeedChange::seq).collect();
        assert_eq!((seqs, answer.count), (vec![2], 1));
        // Another epoch, or a record beyond the head: read afresh.
        let other = if epoch == 1 { 2 } else { 1 };
        for (epoch, after) in [(other, 0), (epoch, 9)] {
            let request = changes_request(lower, epoch, after, false);
            let (_, page) = changes_for(&listings, &reader, 6, &request).await;
            assert_eq!(
                page,
                ChangesPage::Restart {
                    epoch: answer.epoch,
                    head: 4
                }
            );
        }

        // While the feed can't be read, the node says so, and keeps no watch
        // for the request.
        listings.store.hide_feed();
        let body = ListingsBody::ChangesRequest(changes_request(lower, epoch, 0, true));
        assert!(matches!(
            ask(&listings, body).await,
            ListingsBody::Error { .. }
        ));
        assert!(listings.watches.lock().all().is_empty());
        let body = ListingsBody::RangeListRequest(RangeListRequest {
            prefix: [0; 32],
            bits: 0,
            after: None,
            limit: 1,
        });
        assert!(matches!(
            ask(&listings, body).await,
            ListingsBody::Error { .. }
        ));
    }

    #[tokio::test]
    async fn an_answer_holds_a_page_at_most_and_reads_so_many_records_at_most() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        let at = |first: u8, n: u16| {
            let mut address = [first; 32];
            address[1..3].copy_from_slice(&n.to_be_bytes());
            address
        };
        // More records in the lower half of the address space than a page
        // holds.
        let lower_ones: Vec<Listing> = (0..300).map(|n| listed_at(at(0x00, n), 1)).collect();
        listings.store.insert_all(&lower_ones);
        let lower = AddressRange::new([0; 32], 1);
        let reader = reader();
        let (start, _) =
            changes_for(&listings, &reader, 1, &changes_request(lower, 0, 0, false)).await;
        let epoch = start.epoch;
        let mut more_than_a_page = changes_request(lower, epoch, 0, false);
        more_than_a_page.limit = u32::MAX;
        let (answer, page) = changes_for(&listings, &reader, 2, &more_than_a_page).await;
        assert_eq!(answer.changes.len(), 256);
        assert_eq!(page, ChangesPage::Follows(Some(256)));

        // Then more in the upper half than one answer reads: an answer for
        // the lower half reads that many, finds none of its own, and says
        // where it stopped.
        let upper_ones: Vec<Listing> = (0..5_000).map(|n| listed_at(at(0x80, n), 1)).collect();
        listings.store.insert_all(&upper_ones);
        let read = u64::try_from(MAX_FEED_READ).unwrap();
        assert_eq!(read, 4_096);
        let (answer, page) = changes_for(
            &listings,
            &reader,
            3,
            &changes_request(lower, epoch, 300, false),
        )
        .await;
        assert_eq!(answer.head, 5_300);
        assert_eq!(page, ChangesPage::Follows(Some(300 + read)));
        assert_eq!(answer.changes, Vec::new());
        assert_eq!(answer.count, 300);
        // The next answer reads on from there.
        let request = changes_request(lower, epoch, 300 + read, false);
        let (_, page) = changes_for(&listings, &reader, 4, &request).await;
        assert_eq!(page, ChangesPage::Follows(None));
    }

    #[tokio::test]
    async fn range_lists_give_what_the_feed_holds_in_a_range() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        for (block, seed) in (10..).zip(1..=7u8) {
            let envelope = envelope(seed);
            chunks
                .put(&envelope.address(), &envelope.to_bytes())
                .await
                .unwrap();
            assert!(listings.record(listing_for(&envelope, block)).await);
        }
        // Two listed whose chunks went without the removal reaching them.
        let gone = [envelope(50).address(), envelope(51).address()];
        for seed in [50u8, 51] {
            let listing = listing_for(&envelope(seed), 99);
            listings.store.insert(listing).await.unwrap();
        }
        let held: Vec<DigestEntry> = feed_entries(&listings)
            .await
            .into_iter()
            .filter(|entry| !gone.contains(&entry.address))
            .collect();
        assert_eq!(held.len(), 7);

        // The whole space, two at a time: in address order, read past the
        // gone ones, whose listings are dropped for good.
        assert_eq!(range_listed(&listings, AddressRange::ALL, 2).await, held);
        for address in gone {
            assert!(!listings.store.contains(address).await.unwrap());
        }
        assert_eq!(feed_entries(&listings).await, held);
        // A range: its own entries alone.
        let range = AddressRange::new(held[3].address, 1);
        let inside: Vec<DigestEntry> = held
            .iter()
            .filter(|entry| range.contains(&entry.address))
            .copied()
            .collect();
        assert_eq!(range_listed(&listings, range, 256).await, inside);
        // Asked for none: none.
        let body = ListingsBody::RangeListRequest(RangeListRequest {
            prefix: [0; 32],
            bits: 0,
            after: None,
            limit: 0,
        });
        let ListingsBody::RangeListResponse(response) = ask(&listings, body).await else {
            panic!("expected a range list response");
        };
        assert_eq!(response.entries, Vec::new());

        // An entry whose chunk can't be read now stays, as the feed and its
        // totals keep it, though its listing isn't served meanwhile.
        let unreadable = held[0].address;
        let file = chunk_file(&dir, &unreadable);
        std::fs::remove_file(&file).unwrap();
        std::fs::create_dir(&file).unwrap();
        assert!(chunks.get(&unreadable).await.is_err());
        assert!(!chunks.exists(&unreadable).unwrap());
        assert!(get(&listings, unreadable).await.is_none());
        assert_eq!(range_listed(&listings, AddressRange::ALL, 256).await, held);
    }

    #[tokio::test]
    async fn a_watch_is_pushed_each_new_record_in_its_range() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        *listings.pushed.lock() = Some(Vec::new());
        let view = vec![[7; 32], [8; 32]];
        *listings.view.lock() = Some(view.clone());
        let lower = AddressRange::new([0; 32], 1);
        let (reader, probe) = (PeerId::from_bytes([1; 32]), PeerId::from_bytes([2; 32]));
        let first = listed_at([0x10; 32], 1);
        listings.store.insert(first).await.unwrap();
        let (start, _) =
            changes_for(&listings, &reader, 10, &changes_request(lower, 0, 0, false)).await;
        let epoch = start.epoch;
        // The reader watches the lower half; a probe watches the whole
        // space, so every write is pushed to someone and can be waited for.
        let watch = changes_request(lower, epoch, start.head, true);
        let (_, page) = changes_for(&listings, &reader, 11, &watch).await;
        assert_eq!(page, ChangesPage::Follows(None));
        let everything = changes_request(AddressRange::ALL, epoch, start.head, true);
        changes_for(&listings, &probe, 20, &everything).await;

        // A record in the upper half reaches the probe alone. One in the
        // lower half reaches both: the reader as the complement of the id of
        // its watching request.
        listings
            .store
            .insert(listed_at([0x90; 32], 2))
            .await
            .unwrap();
        let entry = listed_at([0x20; 32], 3);
        listings.store.insert(entry.clone()).await.unwrap();
        wait_for_pushes(&listings, 3).await;
        let pushes = take_pushes(&listings);
        let heads: Vec<u64> = pushes
            .iter()
            .filter(|(peer, ..)| *peer == probe)
            .map(|(_, id, push)| {
                assert_eq!(*id, push_request_id(20));
                push.head
            })
            .collect();
        let head = start.head + 2;
        assert_eq!(heads, [head - 1, head]);
        let to_reader: Vec<&(PeerId, u64, ChangesResponse)> =
            pushes.iter().filter(|(peer, ..)| *peer == reader).collect();
        let [(_, id, push)] = to_reader[..] else {
            panic!("one push to the reader: {to_reader:?}");
        };
        assert_eq!((*id, *id), (push_request_id(11), !11));
        check_push(lower, push).unwrap();
        assert_eq!(
            (push.epoch, push.head, push.next, push.more),
            (epoch, head, head, false)
        );
        let pushed_entry = FeedChange::Entry {
            seq: head,
            entry: DigestEntry::new(&entry),
        };
        assert_eq!(push.changes, [pushed_entry]);
        assert_eq!(push.peers, Vec::<[u8; 32]>::new());
        assert_eq!(push.view, view_hash(&view));
        // Its totals are the range's at its head, as an answer gives them.
        let renew = changes_request(lower, epoch, head, true);
        let (answer, _) = changes_for(&listings, &reader, 12, &renew).await;
        assert_eq!((push.count, push.set_hash), (answer.count, answer.set_hash));
        assert_eq!(push.count, 2);

        // A removal is pushed too, now as the complement of the id of the
        // request that renewed the watch.
        listings.store.remove([0x10; 32]).await.unwrap();
        wait_for_pushes(&listings, 2).await;
        let pushes = take_pushes(&listings);
        let (_, id, push) = pushes.iter().find(|(peer, ..)| *peer == reader).unwrap();
        assert_eq!(*id, push_request_id(12));
        check_push(lower, push).unwrap();
        let removal = FeedChange::Removed {
            seq: head + 1,
            address: [0x10; 32],
        };
        assert_eq!(push.changes, [removal]);

        // A request without a watch ends the reader's: the next write
        // reaches the probe alone.
        changes_for(
            &listings,
            &reader,
            13,
            &changes_request(lower, epoch, 0, false),
        )
        .await;
        listings
            .store
            .insert(listed_at([0x30; 32], 4))
            .await
            .unwrap();
        wait_for_pushes(&listings, 1).await;
        let to: Vec<PeerId> = take_pushes(&listings)
            .into_iter()
            .map(|(peer, ..)| peer)
            .collect();
        assert_eq!(to, [probe]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_watch_lasts_until_the_next_request_or_its_lease_ends() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        *listings.pushed.lock() = Some(Vec::new());
        let readers: Vec<PeerId> = (1..=5u8).map(|i| PeerId::from_bytes([i; 32])).collect();
        let reader = readers[0];
        let (start, _) = changes_for(
            &listings,
            &reader,
            1,
            &changes_request(AddressRange::ALL, 0, 0, false),
        )
        .await;
        let watch = changes_request(AddressRange::ALL, start.epoch, start.head, true);
        let lease = Duration::from_secs(WATCH_LEASE);
        assert_eq!(lease, Duration::from_mins(3));
        let mut block = 0;
        let mut write = async || {
            block += 1;
            let mut address = [0x40; 32];
            address[31] = u8::try_from(block).unwrap();
            let listing = listed_at(address, block);
            listings.store.insert(listing).await.unwrap();
        };

        // Watched; a second before its lease ends, renewed; a second before
        // the new one ends, still watched, with the new request's id.
        let almost = lease.saturating_sub(Duration::from_secs(1));
        changes_for(&listings, &reader, 2, &watch).await;
        tokio::time::sleep(almost).await;
        changes_for(&listings, &reader, 3, &watch).await;
        tokio::time::sleep(almost).await;
        write().await;
        wait_for_pushes(&listings, 1).await;
        let pushes = take_pushes(&listings);
        assert_eq!((pushes[0].0, pushes[0].1), (reader, push_request_id(3)));

        // Once its lease ends, with no request since, it lapses. Four other
        // readers watch then, which a lapsed watch doesn't stop, and the
        // next write reaches them alone.
        tokio::time::sleep(Duration::from_secs(2)).await;
        for (id, other) in (10..).zip(&readers[1..]) {
            changes_for(&listings, other, id, &watch).await;
        }
        write().await;
        wait_for_pushes(&listings, 4).await;
        let mut to: Vec<PeerId> = take_pushes(&listings)
            .into_iter()
            .map(|(peer, ..)| peer)
            .collect();
        to.sort_unstable_by_key(|peer| *peer.as_bytes());
        assert_eq!(to, &readers[1..]);
    }

    #[tokio::test]
    async fn a_node_with_four_watches_refuses_a_fifth_but_answers_it() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        *listings.pushed.lock() = Some(Vec::new());
        let readers: Vec<PeerId> = (1..=5u8).map(|i| PeerId::from_bytes([i; 32])).collect();
        let (start, _) = changes_for(
            &listings,
            &readers[0],
            1,
            &changes_request(AddressRange::ALL, 0, 0, false),
        )
        .await;
        let watch = changes_request(AddressRange::ALL, start.epoch, start.head, true);
        assert_eq!(MAX_WATCHERS, 4);
        // Five ask to watch, and every one is answered.
        for (id, reader) in (100..).zip(&readers) {
            let (_, page) = changes_for(&listings, reader, id, &watch).await;
            assert_eq!(page, ChangesPage::Follows(None));
        }
        let pushed_to = |pushes: Vec<(PeerId, u64, ChangesResponse)>| {
            let mut to: Vec<(PeerId, u64)> =
                pushes.into_iter().map(|(peer, id, _)| (peer, id)).collect();
            to.sort_unstable_by_key(|(peer, _)| *peer.as_bytes());
            to
        };
        // The first four are pushed the next record; the fifth isn't.
        let first = listed_at([0x10; 32], 1);
        listings.store.insert(first).await.unwrap();
        wait_for_pushes(&listings, 4).await;
        let expected: Vec<(PeerId, u64)> = (100..)
            .zip(&readers[..4])
            .map(|(id, reader)| (*reader, push_request_id(id)))
            .collect();
        assert_eq!(pushed_to(take_pushes(&listings)), expected);

        // A reader that watches renews its watch with four set: it is never
        // pushed out. One that stops watching makes room for the fifth.
        changes_for(&listings, &readers[0], 200, &watch).await;
        let mut unwatch = watch.clone();
        unwatch.watch = false;
        changes_for(&listings, &readers[1], 201, &unwatch).await;
        changes_for(&listings, &readers[4], 202, &watch).await;
        let second = listed_at([0x20; 32], 2);
        listings.store.insert(second).await.unwrap();
        wait_for_pushes(&listings, 4).await;
        let expected = vec![
            (readers[0], push_request_id(200)),
            (readers[2], push_request_id(102)),
            (readers[3], push_request_id(103)),
            (readers[4], push_request_id(202)),
        ];
        assert_eq!(pushed_to(take_pushes(&listings)), expected);
    }

    #[tokio::test]
    async fn a_reader_follows_the_feed_through_answers_and_pushes() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        *listings.pushed.lock() = Some(Vec::new());
        let list_one = async |seed: u8| {
            let envelope = envelope(seed);
            chunks
                .put(&envelope.address(), &envelope.to_bytes())
                .await
                .unwrap();
            assert!(
                listings
                    .record(listing_for(&envelope, u64::from(seed)))
                    .await
            );
        };
        for seed in 1..=3 {
            list_one(seed).await;
        }
        // A reader's mirror of the whole space, as darwin-listings keeps it:
        // a round reads the node's feed afresh, then follows it, watching.
        let reader = reader();
        let mut sync = FeedSync::new(AddressRange::ALL);
        let mut ids = 1_000u64..;
        let mut round = async |sync: &mut FeedSync| {
            sync.begin_round();
            while let Some(body) = sync.next_request(false, true) {
                let id = ids.next().unwrap();
                sync.answer(ask_as(&listings, &reader, id, body).await);
            }
            assert!(sync.in_sync(), "{:?}", sync.outcome());
        };
        round(&mut sync).await;
        assert!(sync.entries().copied().eq(feed_entries(&listings).await));

        // Each new listing is pushed, and taken between rounds.
        for seed in 4..=5 {
            list_one(seed).await;
            wait_for_pushes(&listings, 1).await;
            let (_, _, push) = take_pushes(&listings).remove(0);
            let address = envelope(seed).address();
            assert_eq!(sync.push(&push), Pushed::Applied(vec![address]));
        }
        assert!(sync.entries().copied().eq(feed_entries(&listings).await));
        // A push missed: the next one finds the mirror behind, and the next
        // round's request catches up.
        list_one(6).await;
        wait_for_pushes(&listings, 1).await;
        drop(take_pushes(&listings));
        list_one(7).await;
        wait_for_pushes(&listings, 1).await;
        let (_, _, push) = take_pushes(&listings).remove(0);
        assert_eq!(sync.push(&push), Pushed::Behind);
        round(&mut sync).await;
        assert!(sync.entries().copied().eq(feed_entries(&listings).await));
        assert_eq!(sync.len(), 7);
    }

    #[tokio::test]
    async fn requests_of_a_kind_this_node_doesnt_know_are_refused_at_once() {
        let dir = TempDir::new().unwrap();
        let (listings, _) = service(&dir).await;
        let (peer, other) = (PeerId::from_bytes([1; 32]), PeerId::from_bytes([2; 32]));
        // Request 42 of a kind added later: variant 9, and whatever it holds.
        let unknown: [u8; 5] = [42, 9, 1, 2, 3];
        assert!(ListingsMessage::decode(&unknown).is_err());
        let unsupported = |request_id| ListingsMessage {
            request_id,
            body: ListingsBody::Error {
                message: "unsupported".into(),
            },
        };
        // Nothing of this node's waits on it: left to be answered, and
        // answered at once as unsupported, under its own id.
        assert!(!listings.handle_response(&peer, &unknown));
        let answer = listings.handle_message(&peer, &unknown).await.unwrap();
        assert_eq!(ListingsMessage::decode(&answer).unwrap(), unsupported(42));
        // An id of any size.
        let mut large = postcard::to_stdvec(&(u64::MAX - 1)).unwrap();
        large.extend_from_slice(&[200, 1]);
        assert!(!listings.handle_response(&peer, &large));
        let answer = listings.handle_message(&peer, &large).await.unwrap();
        assert_eq!(
            ListingsMessage::decode(&answer).unwrap(),
            unsupported(u64::MAX - 1)
        );

        // A request of this node's waits on id 42 from `peer`: the message is
        // that request's answer, which can't be read, so the request has its
        // answer, none, and nothing is sent back. From another peer it is
        // still a request.
        let (answer, mut answered) = oneshot::channel();
        let _pending = Pending::register(&listings.waiting, (peer, 42), answer);
        assert!(!listings.handle_response(&other, &unknown));
        assert!(listings.handle_response(&peer, &unknown));
        assert_eq!(answered.try_recv().unwrap(), None);
        assert!(listings.waiting.lock().is_empty());

        // Bytes in which not even a request id reads: neither.
        for garbage in [&[][..], &[0xff; 4][..], &[0x80; 11][..]] {
            assert!(listings.handle_response(&peer, garbage));
            assert!(listings.handle_message(&peer, garbage).await.is_none());
        }
    }

    // Repair, bounded.

    #[test]
    fn a_check_fails_for_good_only_against_final_blocks() {
        use ant_listings::ListingError;
        let receipt = |block_number| PaymentReceipt {
            tx_hash: [1; 32],
            success: true,
            block_number,
            logs: Vec::new(),
        };
        let found = |block| vec![(Some(receipt(block)), Some(BLOCK_TIME))];
        let by_tx = Lookup::Transaction { tx_hash: [1; 32] };
        let by_quote = Lookup::Quote {
            quote_hash: [2; 32],
            block_number: None,
            search_from: 0,
            not_after: None,
        };
        // The chain shows, in a final block, that the transaction doesn't pay
        // the vault for the quote, or isn't where or when the copy says.
        for error in [
            PaymentError::NotPaid,
            PaymentError::Failed,
            PaymentError::WrongTransaction,
            PaymentError::WrongBlock,
            PaymentError::WrongTimestamp,
            PaymentError::FirstSeenTooEarly,
        ] {
            assert!(fails_for_good(&error, &by_tx, &found(100), 100), "{error}");
            assert!(
                fails_for_good(&error, &by_quote, &found(90), 100),
                "{error}"
            );
            // In a block not yet final: it could change.
            assert!(!fails_for_good(&error, &by_tx, &found(101), 100), "{error}");
        }
        // Every payment found must be final.
        let both = vec![
            (Some(receipt(90)), Some(BLOCK_TIME)),
            (Some(receipt(101)), Some(BLOCK_TIME)),
        ];
        assert!(!fails_for_good(
            &PaymentError::NotPaid,
            &by_quote,
            &both,
            100
        ));
        // Nothing found: it could be paid yet, or found by a longer search.
        assert!(!fails_for_good(
            &PaymentError::NotFound,
            &by_tx,
            &Vec::new(),
            100
        ));
        assert!(!fails_for_good(
            &PaymentError::NotPaid,
            &by_tx,
            &Vec::new(),
            100
        ));
        // A copy dated in the future, or by too few nodes: not the chain's
        // word.
        for error in [ListingError::TimeInFuture, ListingError::MissingFirstSeen] {
            let error = PaymentError::Malformed(error);
            assert!(!fails_for_good(&error, &by_tx, &found(90), 100), "{error}");
        }
        // Only the block shown: its time is final once the block is.
        let by_block = Lookup::Block { block_number: 90 };
        let timed = vec![(None, Some(BLOCK_TIME))];
        assert!(fails_for_good(
            &PaymentError::WrongTimestamp,
            &by_block,
            &timed,
            100
        ));
        assert!(!fails_for_good(
            &PaymentError::WrongTimestamp,
            &by_block,
            &timed,
            89
        ));
        assert!(!fails_for_good(
            &PaymentError::NotFound,
            &by_block,
            &vec![(None, None)],
            100
        ));
    }

    #[tokio::test]
    async fn a_copy_paid_before_the_vault_was_deployed_is_refused_unread() {
        let (chain, url) = serve(chain_with(1000, &[])).await;
        let dir = TempDir::new().unwrap();
        let (listings, _) = service_with(&dir, &url, Some(600)).await;
        let envelope = envelope(1);
        let asked = peers(7);
        // A quorum serves a copy paid in block 500, before the vault was
        // deployed in block 600: it paid something else, and nothing is
        // read from the chain for it.
        let old = listing_for(&envelope, 500);
        let repair = listings
            .repair_from(&envelope, &asked, served_by(&asked[..4], &old))
            .await;
        assert_eq!(repair.unwrap(), Repair::Refused);
        assert!(chain.lock().calls.is_empty(), "{:?}", chain.lock().calls);
        // One paid in the deployment's block is looked up, and isn't found.
        let later = listing_for(&envelope, 600);
        let repair = listings
            .repair_from(&envelope, &asked, served_by(&asked[..4], &later))
            .await;
        assert_eq!(repair.unwrap(), Repair::Nothing);
        assert!(calls_to(&chain, "eth_getTransactionReceipt") > 0);

        // A node that isn't told when the vault was deployed looks the old
        // one up too.
        let dir = TempDir::new().unwrap();
        let (unaware, _) = service_on(&dir, &url).await;
        chain.lock().calls.clear();
        let repair = unaware
            .repair_from(&envelope, &asked, served_by(&asked[..4], &old))
            .await;
        assert_eq!(repair.unwrap(), Repair::Nothing);
        assert!(calls_to(&chain, "eth_getTransactionReceipt") > 0);
    }

    #[tokio::test]
    async fn a_copy_the_chain_shows_paid_elsewhere_is_refused_once_final() {
        // Everything shown: the copy is looked up by its tx hash, whose
        // receipt pays its quote to another vault.
        let envelope = envelope(1);
        let elsewhere = payment(&envelope, 0, 300);
        let mut log = vault_log(quote_hash(&elsewhere), elsewhere.log_index);
        log.address = [0x77; 20];
        let mut mock = chain_with(1000, &[]);
        mock.add_receipt(PaymentReceipt {
            tx_hash: elsewhere.tx_hash,
            success: true,
            block_number: 300,
            logs: vec![log],
        });
        mock.finalized = Some(299);
        let (chain, url) = serve(mock).await;
        let dir = TempDir::new().unwrap();
        let (listings, _) = service_on(&dir, &url).await;
        let asked = peers(7);
        let copy = Listing::new(&envelope, &elsewhere, BLOCK_TIME);
        let quorum = served_by(&asked[..4], &copy);

        // Its block is not final yet: the chain could still change.
        let repair = listings
            .repair_from(&envelope, &asked, quorum.clone())
            .await;
        assert_eq!(repair.unwrap(), Repair::Nothing);
        // Once it is, the copy can never check out.
        chain.lock().finalized = Some(300);
        let repair = listings.repair_from(&envelope, &asked, quorum).await;
        assert_eq!(repair.unwrap(), Repair::Refused);
        // Beside a copy whose payment isn't found, it could still be taken.
        let unfound = Listing::new(&envelope, &payment(&envelope, 1, 400), BLOCK_TIME);
        let mut answers = served_by(&asked[..1], &copy);
        answers.extend(served_by(&asked[1..2], &unfound));
        let repair = listings.repair_from(&envelope, &asked, answers).await;
        assert_eq!(repair.unwrap(), Repair::Nothing);
        // A provider that won't say which block is final: nothing is
        // refused for good.
        chain.lock().fails = Some(Box::new(|method, params| {
            method == "eth_getBlockByNumber" && params[0] == "finalized"
        }));
        let repair = listings
            .repair_from(&envelope, &asked, served_by(&asked[..4], &copy))
            .await;
        assert_eq!(repair.unwrap(), Repair::Nothing);
    }

    #[tokio::test(start_paused = true)]
    async fn checks_end_at_once_when_no_copy_can_ever_check_out() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service_with(&dir, DEAD_CHAIN, Some(1_000)).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        // In the index, and every copy the close group serves was paid
        // before the vault was deployed.
        let envelope = envelope(1);
        let address = envelope.address();
        let asked = peers(7);
        answer_for(
            &listings,
            &asked,
            served_by(&asked, &listing_for(&envelope, 500)),
        );
        let start = Instant::now();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();

        // Checked to the end of the schedule, then given up: no re-checks.
        let checked = Arc::clone(&listings);
        tokio::time::sleep_until(start + whole_schedule() + Duration::from_secs(1)).await;
        wait_until(async || checked.store.pending_addresses().await.unwrap().is_empty()).await;
        let schedule = 1 + RETRY_DELAYS.len();
        assert_eq!(times_asked(&listings), schedule);
        assert!(!listings.store.contains(address).await.unwrap());
        tokio::time::sleep(MAX_RECHECK_TIME).await;
        assert_eq!(times_asked(&listings), schedule);
    }

    #[tokio::test(start_paused = true)]
    async fn an_envelope_in_the_index_without_a_listing_is_checked_again_for_a_week() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        // The close group has nothing to take.
        let envelope = envelope(1);
        let address = envelope.address();
        answer_for(&listings, &peers(7), Vec::new());
        let start = Instant::now();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        let checked = Arc::clone(&listings);

        // After the schedule, checked again, each wait twice the last up to
        // the longest, until the next would come over a week after the
        // re-checks began.
        let mut due = start + whole_schedule();
        let end = due + MAX_RECHECK_TIME;
        assert_eq!(MAX_RECHECK_TIME, Duration::from_hours(7 * 24));
        let mut times = 1 + RETRY_DELAYS.len();
        for wait in unread_delays() {
            tokio::time::sleep_until(due + Duration::from_secs(1)).await;
            wait_until(async || times_asked(&checked) == times).await;
            if due + wait > end {
                break;
            }
            // Still to be checked, after a restart too.
            assert_eq!(
                listings.store.pending_addresses().await.unwrap(),
                vec![address]
            );
            tokio::time::sleep_until(due + wait - Duration::from_secs(1)).await;
            assert_eq!(times_asked(&listings), times);
            due += wait;
            times += 1;
        }
        // 2 hours, 4, then every 6 to the week's end: 29 re-checks.
        assert_eq!(times, 1 + RETRY_DELAYS.len() + 29);
        // Then given up, with no mark left.
        wait_until(async || checked.store.pending_addresses().await.unwrap().is_empty()).await;
        tokio::time::sleep(2 * MAX_UNREAD_DELAY).await;
        assert_eq!(times_asked(&listings), times);
    }

    #[tokio::test(start_paused = true)]
    async fn a_check_the_chain_cant_answer_for_a_week_is_given_up() {
        // As in `a_check_the_chain_cant_answer_goes_on_until_it_can`, but
        // the chain never answers.
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        let envelope = envelope_with(1, ShownFields::NONE.with(PaymentField::BlockNumber), true);
        let address = envelope.address();
        let planted = listing_for(&envelope, 11);
        let asked = peers(7);
        answer_for(
            &listings,
            &asked,
            served_by(&asked[..4], &listing_for(&envelope, 10)),
        );
        let start = Instant::now();
        chunks.put(&address, &envelope.to_bytes()).await.unwrap();
        assert!(listings.record(planted.clone()).await);

        let checked = Arc::clone(&listings);
        let past_the_end = start + whole_schedule() + MAX_RECHECK_TIME + Duration::from_secs(1);
        tokio::time::sleep_until(past_the_end).await;
        wait_until(async || checked.store.pending_addresses().await.unwrap().is_empty()).await;
        assert_eq!(times_asked(&listings), 1 + RETRY_DELAYS.len() + 29);
        // The listing held stays as it was.
        assert_eq!(get(&listings, address).await, Some(planted));
    }

    #[tokio::test(start_paused = true)]
    async fn checks_past_their_schedule_are_spread_a_minute_apart() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        answer_for(&listings, &peers(7), Vec::new());
        // Five envelopes in the index, stored together, none with a listing
        // to take: their schedules run out together.
        let start = Instant::now();
        for seed in 1..=5 {
            let envelope = envelope(seed);
            chunks
                .put(&envelope.address(), &envelope.to_bytes())
                .await
                .unwrap();
        }
        let checked = Arc::clone(&listings);
        let schedules = 5 * (1 + RETRY_DELAYS.len());
        tokio::time::sleep_until(start + whole_schedule() + Duration::from_secs(1)).await;
        wait_until(async || times_asked(&checked) == schedules).await;

        // Their first re-checks are all due two hours later, and are made
        // one a minute: at most 60 an hour.
        assert_eq!(
            (RECHECKS_PER_HOUR, RECHECK_SPACING),
            (60, Duration::from_mins(1))
        );
        let first = start + whole_schedule() + Duration::from_hours(2);
        for n in 1..=5u32 {
            let slot = first + RECHECK_SPACING * (n - 1);
            tokio::time::sleep_until(slot - Duration::from_secs(1)).await;
            let made = schedules + usize::try_from(n).unwrap();
            assert_eq!(times_asked(&listings), made - 1);
            tokio::time::sleep_until(slot + Duration::from_secs(1)).await;
            wait_until(async || times_asked(&checked) == made).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_slot_past_the_end_of_the_rechecks_is_left_for_the_next() {
        let dir = TempDir::new().unwrap();
        let (listings, _chunks) = service(&dir).await;
        let now = Instant::now();
        let first = listings
            .recheck_slot(now + Duration::from_hours(1))
            .unwrap();
        assert_eq!(first, now);
        // The next slot is a minute on: an envelope whose re-checks end
        // before it takes none.
        let second = now + RECHECK_SPACING;
        assert_eq!(listings.recheck_slot(second - Duration::from_secs(1)), None);
        // The next envelope has it, and the one after the minute after.
        assert_eq!(listings.recheck_slot(second), Some(second));
        assert_eq!(
            listings.recheck_slot(now + Duration::from_hours(1)),
            Some(second + RECHECK_SPACING)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_restart_keeps_when_the_rechecks_end() {
        let dir = TempDir::new().unwrap();
        let (listings, chunks) = service(&dir).await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        // Two envelopes in the index whose re-checks began before the node
        // stopped: one with an hour of them left, one with none.
        let (some_left, none_left) = (envelope(1), envelope(2));
        for envelope in [&some_left, &none_left] {
            chunks
                .put(&envelope.address(), &envelope.to_bytes())
                .await
                .unwrap();
        }
        let marked = Arc::clone(&listings);
        wait_until(async || marked.store.pending_addresses().await.unwrap().len() == 2).await;
        drop(marked);
        let week = MAX_RECHECK_TIME.as_secs();
        let hour = Duration::from_hours(1).as_secs();
        for (envelope, since) in [
            (&some_left, unix_now() - week + hour),
            (&none_left, unix_now() - week - hour),
        ] {
            let store = &listings.store;
            store
                .mark_rechecking(envelope.address(), since)
                .await
                .unwrap();
        }

        // Started again: the schedule isn't run again. The one with time
        // left is checked once, from the start's delay, and then not again,
        // as the next check would come after its end; the other not at all.
        drop(listings);
        let start = Instant::now();
        let listings = restart(&dir, &chunks).await;
        answer_for(&listings, &peers(7), Vec::new());
        let checked = Arc::clone(&listings);
        tokio::time::sleep_until(start + CHECK_DELAY + Duration::from_secs(1)).await;
        wait_until(async || checked.store.pending_addresses().await.unwrap().is_empty()).await;
        assert_eq!(times_asked(&listings), 1);
        tokio::time::sleep(MAX_RECHECK_TIME).await;
        assert_eq!(times_asked(&listings), 1);
    }
}

//! Listings across real nodes: a paid envelope stored on one node is served
//! on the listings topic by its close group within 30 s, the nodes that got it
//! only by fresh offer included. The harness wires a node's three listings
//! hooks as `src/node.rs` does: the chunk handler records a listing from a
//! PUT, the replication engine from a fresh offer, and the protocol loop
//! serves listings. A change that drops any of them fails here.
//!
//! The envelope's quote is paid on a local Anvil chain, which every node's
//! listing service reads to find the payment from the proof. The nodes'
//! storage payment checks take the address from their caches, as the
//! replication suites do, so what is under test is the listings.
//!
//! The change feed's tests run a node's chunk store and listing service on a
//! P2P node of their own, answering on the listings topic as `src/node.rs`
//! does, and read it from another P2P node over the network as a reader
//! does, with `FeedSync`: the feed across a restart, a new epoch after a
//! wipe and after another version wrote to the store, a reader built on
//! darwin-listings 0.2.1 answered as before, and requests of kinds the node
//! doesn't know answered at once. They read no chain: each listing is
//! recorded as a PUT records it once its payment is found.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::anvil::TestAnvil;
use super::testnet::{TestNetwork, TestNetworkConfig};
use ant_listings::self_encryption::{ChunkInfo, DataMap, XorName as SeXorName};
use ant_listings::{
    check_changes_page, AddressRange, ChangesPage, ChangesRequest, ChangesResponse, DigestEntry,
    Envelope, FeedSync, Listing, ListingKey, ListingsBody, ListingsMessage, Metadata, Payment,
    Pushed, ShownFields, MAX_LIST_LIMIT, TOPIC,
};
use ant_node::ant_protocol::{
    ChunkMessage, ChunkMessageBody, ChunkPutRequest, ChunkPutResponse, ChunkQuoteRequest,
    ChunkQuoteResponse, CLOSE_GROUP_SIZE,
};
use ant_node::client::send_and_await_chunk_response;
use ant_node::listings::ListingService;
use ant_node::payment::{serialize_single_node_proof, PaymentProof};
use ant_node::storage::{ChunkStore, ChunkStoreConfig};
use evmlib::common::Amount;
use evmlib::{EncodedPeerId, PaymentQuote, ProofOfPayment};
use heed::types::Bytes;
use heed::{Database, EnvOpenOptions};
use rand::Rng;
use saorsa_core::identity::{NodeIdentity, PeerId};
use saorsa_core::{IPDiversityConfig, MultiAddr, NodeConfig as CoreNodeConfig, P2PEvent, P2PNode};
use serde::{Deserialize, Serialize};
use serial_test::serial;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// How soon after the PUT the close group must serve the listing.
const SERVED_WITHIN: Duration = Duration::from_secs(30);

/// How many of the close group must serve it: a quorum of 7.
const SERVED_BY: usize = 4;

/// The timeout of one quote or PUT exchange.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// The timeout of one listing request.
const LISTING_TIMEOUT: Duration = Duration::from_secs(5);

/// A listed envelope, every payment field shown, of a three-chunk file.
fn envelope() -> Envelope {
    let infos = (0..3u8)
        .map(|index| ChunkInfo {
            index: usize::from(index),
            dst_hash: SeXorName([0x40 + index; 32]),
            src_hash: SeXorName([0x50 + index; 32]),
            src_size: 1_000,
        })
        .collect();
    Envelope::readable(
        &DataMap::new(infos),
        Metadata::default(),
        ShownFields::ALL,
        true,
    )
    .unwrap()
}

/// A signed quote from `peer` for storing `size` bytes at `address`.
async fn quote(
    client: &Arc<P2PNode>,
    peer: &PeerId,
    address: [u8; 32],
    size: u64,
) -> Result<PaymentQuote, String> {
    let request_id: u64 = rand::thread_rng().gen();
    let message = ChunkMessage {
        request_id,
        body: ChunkMessageBody::QuoteRequest(ChunkQuoteRequest {
            address,
            data_size: size,
            data_type: 0,
        }),
    }
    .encode()
    .map_err(|e| format!("encode quote request: {e}"))?;
    let quote = send_and_await_chunk_response(
        client,
        peer,
        message,
        request_id,
        REQUEST_TIMEOUT,
        &[],
        |body| match body {
            ChunkMessageBody::QuoteResponse(ChunkQuoteResponse::Success { quote, .. }) => {
                Some(Ok(quote))
            }
            ChunkMessageBody::QuoteResponse(ChunkQuoteResponse::Error(e)) => {
                Some(Err(format!("quote error: {e:?}")))
            }
            _ => None,
        },
        |e| format!("send quote request: {e}"),
        || "quote request timed out".to_string(),
    )
    .await?;
    rmp_serde::from_slice(&quote).map_err(|e| format!("decode quote: {e}"))
}

/// PUT `content` at `address` to `peer` with `proof`.
async fn put(
    client: &Arc<P2PNode>,
    peer: &PeerId,
    address: [u8; 32],
    content: &[u8],
    proof: Vec<u8>,
) -> Result<(), String> {
    let request_id: u64 = rand::thread_rng().gen();
    let message = ChunkMessage {
        request_id,
        body: ChunkMessageBody::PutRequest(ChunkPutRequest::with_payment(
            address,
            bytes::Bytes::copy_from_slice(content),
            proof,
        )),
    }
    .encode()
    .map_err(|e| format!("encode put request: {e}"))?;
    send_and_await_chunk_response(
        client,
        peer,
        message,
        request_id,
        REQUEST_TIMEOUT,
        &[],
        |body| match body {
            ChunkMessageBody::PutResponse(ChunkPutResponse::Success { .. }) => Some(Ok(())),
            ChunkMessageBody::PutResponse(other) => Some(Err(format!("put refused: {other:?}"))),
            _ => None,
        },
        |e| format!("send put: {e}"),
        || "put timed out".to_string(),
    )
    .await
}

/// The listing `peer` serves for `address`, asked on the listings topic as a
/// client asks. `None` if it has none, or doesn't answer in time.
async fn ask_listing(client: &Arc<P2PNode>, peer: &PeerId, address: [u8; 32]) -> Option<Listing> {
    // Subscribed before asking, so the answer can't be missed.
    let mut events = client.subscribe_events();
    let request_id: u64 = rand::thread_rng().gen();
    let message = ListingsMessage {
        request_id,
        body: ListingsBody::GetRequest { address },
    }
    .encode()
    .ok()?;
    client.send_message(peer, TOPIC, message, &[]).await.ok()?;
    let deadline = Instant::now() + LISTING_TIMEOUT;
    loop {
        let event = match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Ok(event)) => event,
            Ok(Err(RecvError::Lagged(_))) => continue,
            Ok(Err(RecvError::Closed)) | Err(_) => return None,
        };
        let P2PEvent::Message {
            topic,
            source: Some(source),
            data,
            ..
        } = event
        else {
            continue;
        };
        if topic != TOPIC || source != *peer {
            continue;
        }
        let Ok(answer) = ListingsMessage::decode(&data) else {
            continue;
        };
        if answer.request_id != request_id {
            continue;
        }
        return match answer.body {
            ListingsBody::GetResponse { listing } => listing,
            _ => None,
        };
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[allow(clippy::too_many_lines)]
async fn a_paid_envelope_is_served_by_its_close_group_within_30_seconds() {
    let anvil = TestAnvil::new().await.expect("start anvil");
    let wallet = anvil.create_funded_wallet().expect("funded wallet");
    let config = TestNetworkConfig::small()
        .with_payment_enforcement()
        .with_evm_network(anvil.to_network());
    let mut network = TestNetwork::new(config).await.expect("build network");
    network.start().await.expect("start network");
    network.warmup_dht().await.expect("warm up the DHT");

    let envelope = envelope();
    let address = envelope.address();
    let content = envelope.to_bytes();
    for node in network.nodes() {
        if let Some(protocol) = &node.ant_protocol {
            protocol.payment_verifier().cache_insert(address);
        }
    }

    // The last node is the client. The close group is the 7 nodes nearest the
    // address, the client left out.
    let client = network
        .nodes()
        .last()
        .and_then(|node| node.p2p_node.clone())
        .expect("the client node");
    let client_peer = *client.peer_id();
    let group: Vec<PeerId> = client
        .dht()
        .find_closest_nodes(&address, CLOSE_GROUP_SIZE + 1)
        .await
        .expect("the nodes nearest the address")
        .iter()
        .map(|node| node.peer_id)
        .filter(|peer| *peer != client_peer)
        .take(CLOSE_GROUP_SIZE)
        .collect();
    assert!(
        group.len() > SERVED_BY,
        "only {} nodes found near the address",
        group.len()
    );

    // Paid as a client pays: quotes from the close group, the median issuer
    // paid 3 times its price on chain.
    let mut quotes = Vec::new();
    for peer in &group {
        match quote(&client, peer, address, content.len() as u64).await {
            Ok(quote) => quotes.push((*peer, quote)),
            Err(e) => eprintln!("no quote from {peer}: {e}"),
        }
    }
    assert!(!quotes.is_empty(), "no node quoted");
    quotes.sort_by_key(|(_, quote)| quote.price);
    let median = &quotes[quotes.len() / 2].1;
    let amount = median
        .price
        .checked_mul(Amount::from(3u64))
        .expect("no overflow");
    let (paid, _gas) = wallet
        .pay_for_quotes(vec![(median.hash(), median.rewards_address, amount)])
        .await
        .expect("pay on chain");
    let tx_hashes: Vec<evmlib::common::TxHash> = paid.values().copied().collect();
    let proof = serialize_single_node_proof(&PaymentProof {
        proof_of_payment: ProofOfPayment {
            peer_quotes: quotes
                .iter()
                .map(|(peer, quote)| (EncodedPeerId::new(*peer.as_bytes()), quote.clone()))
                .collect(),
        },
        tx_hashes: tx_hashes.clone(),
        commitment_sidecars: Vec::new(),
    })
    .expect("serialize the proof");

    // One PUT, to the nearest node: the rest of the close group get the
    // envelope only from its fresh offers.
    let target = group[0];
    let started = Instant::now();
    put(&client, &target, address, &content, proof)
        .await
        .expect("the nearest node stores the envelope");

    let mut served = Vec::new();
    loop {
        served.clear();
        for peer in &group {
            if let Some(listing) = ask_listing(&client, peer, address).await {
                assert!(
                    listing.matches(&envelope),
                    "{peer} serves another listing: {listing:?}"
                );
                assert!(
                    listing
                        .payment
                        .tx_hash
                        .is_some_and(|tx| tx_hashes.iter().any(|paid| paid.0 == tx)),
                    "{peer} serves a listing without the payment's transaction: {listing:?}"
                );
                served.push(*peer);
            }
        }
        let by_offer = served.iter().filter(|peer| **peer != target).count();
        if served.contains(&target) && by_offer >= SERVED_BY - 1 {
            break;
        }
        assert!(
            started.elapsed() < SERVED_WITHIN,
            "{:?} after the PUT, {} of the close group serve the listing ({} by fresh offer, \
             the node PUT to {}): {served:?}",
            started.elapsed(),
            served.len(),
            by_offer,
            if served.contains(&target) {
                "among them"
            } else {
                "not"
            },
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    network.shutdown().await.expect("shut the network down");
}

// ---------------------------------------------------------------------------
// The change feed over the network
// ---------------------------------------------------------------------------

/// The payment vault the feed tests' nodes name. They read no chain.
const VAULT: [u8; 20] = [0x69; 20];

/// An RPC endpoint nothing answers at.
const NO_CHAIN: &str = "http://127.0.0.1:0";

/// How long a reader waits for a node's answer, as readers do.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a node starting again waits for the run before it, in this
/// process, to let its stores go.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(30);

/// A listed envelope of a three-chunk file made from `seed`, every payment
/// field shown, in the index or not.
fn envelope_of(seed: u8, in_index: bool) -> Envelope {
    let infos = (0..3u8)
        .map(|index| ChunkInfo {
            index: usize::from(index),
            dst_hash: SeXorName([seed.wrapping_add(index); 32]),
            src_hash: SeXorName([seed.wrapping_add(index) ^ 0x80; 32]),
            src_size: 1_000,
        })
        .collect();
    Envelope::readable(
        &DataMap::new(infos),
        Metadata::default(),
        ShownFields::ALL,
        in_index,
    )
    .unwrap()
}

/// A payment in block `block`, as the chain would give it.
fn payment_in(block: u64) -> Payment {
    Payment {
        tx_hash: [u8::try_from(block % 256).unwrap(); 32],
        block_number: block,
        block_timestamp: 1_790_000_000 + 2 * block,
        log_index: 0,
        quote: vec![0x93, 1, 2, 3],
    }
}

/// The digest entries of `listings`, in address order, as a node's change
/// feed holds them.
fn entries_of(listings: &[Listing]) -> Vec<DigestEntry> {
    let mut entries: Vec<DigestEntry> = listings.iter().map(DigestEntry::new).collect();
    entries.sort_unstable_by_key(|entry| entry.address);
    entries
}

/// The request id a message on the listings topic starts with.
fn request_id_of(data: &[u8]) -> Option<u64> {
    postcard::take_from_bytes::<u64>(data)
        .ok()
        .map(|(request_id, _)| request_id)
}

/// Run `open` until it succeeds: a store a stopped run of a node had open in
/// this process is let go a moment after the run ends, as a process's are
/// when it exits.
async fn open_when_free<T, E, F>(mut open: impl FnMut() -> F) -> T
where
    E: std::fmt::Display,
    F: Future<Output = Result<T, E>>,
{
    let deadline = Instant::now() + RELEASE_TIMEOUT;
    loop {
        match open().await {
            Ok(opened) => return opened,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "can't open the node's store: {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// A P2P node as `identity` on loopback, on a port the system picks.
async fn loopback_node(identity: Arc<NodeIdentity>) -> Arc<P2PNode> {
    let mut config = CoreNodeConfig::builder()
        .port(0)
        .ipv6(false)
        .local(true)
        .connection_timeout(Duration::from_secs(2))
        .build()
        .expect("a node config");
    config.diversity_config = Some(IPDiversityConfig::permissive());
    config.node_identity = Some(identity);
    let node = P2PNode::new(config).await.expect("a P2P node");
    node.start().await.expect("the P2P node starts");
    Arc::new(node)
}

/// Where `node` listens, on loopback.
async fn loopback_addr(node: &P2PNode) -> MultiAddr {
    let port = node
        .listen_addrs()
        .await
        .iter()
        .find_map(MultiAddr::port)
        .expect("a listening port");
    MultiAddr::quic(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

/// A node, as far as listings go: its chunk store and listing service in a
/// directory of its own, on a P2P node of its own, answering on the
/// listings topic as `src/node.rs` routes the topic.
struct FeedNode {
    p2p: Arc<P2PNode>,
    addr: MultiAddr,
    chunks: Arc<ChunkStore>,
    listings: Arc<ListingService>,
    routing: JoinHandle<()>,
}

impl FeedNode {
    /// Start a node as `identity` on its data in `dir`.
    async fn start(dir: &Path, identity: &Arc<NodeIdentity>) -> Self {
        let chunks = Arc::new(
            open_when_free(|| {
                ChunkStore::new(ChunkStoreConfig {
                    root_dir: dir.to_path_buf(),
                    ..ChunkStoreConfig::test_default()
                })
            })
            .await,
        );
        let listings = open_when_free(|| {
            ListingService::new(dir, 0, Arc::clone(&chunks), NO_CHAIN.into(), VAULT, None)
        })
        .await;
        let p2p = loopback_node(Arc::clone(identity)).await;
        listings.attach_p2p_node(Arc::clone(&p2p));
        let addr = loopback_addr(&p2p).await;
        let routing = tokio::spawn(route_listings(Arc::clone(&p2p), Arc::clone(&listings)));
        Self {
            p2p,
            addr,
            chunks,
            listings,
            routing,
        }
    }

    fn peer(&self) -> PeerId {
        *self.p2p.peer_id()
    }

    /// List `envelope`, paid in block `block`: its chunk stored, and its
    /// listing recorded as a PUT records it once its payment is found.
    async fn list(&self, envelope: &Envelope, block: u64) -> Listing {
        self.chunks
            .put(&envelope.address(), &envelope.to_bytes())
            .await
            .unwrap();
        let listing = Listing::new(envelope, &payment_in(block), 1_790_000_000);
        assert!(self.listings.record(listing.clone()).await);
        listing
    }

    /// Stop, as a node's process ends.
    async fn stop(self) {
        self.routing.abort();
        let _ = self.routing.await;
        if let Err(e) = self.p2p.shutdown().await {
            eprintln!("the node's P2P node didn't shut down cleanly: {e}");
        }
    }
}

/// Answer what `p2p` receives on the listings topic with `listings`, as
/// `src/node.rs` routes the topic: an answer to one of the node's own
/// requests goes to the request, and a request waits for one of the listing
/// service's own permits.
async fn route_listings(p2p: Arc<P2PNode>, listings: Arc<ListingService>) {
    let mut events = p2p.subscribe_events();
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => return,
        };
        let P2PEvent::Message {
            topic,
            source: Some(source),
            data,
            ..
        } = event
        else {
            continue;
        };
        if topic != TOPIC {
            continue;
        }
        let (p2p, listings) = (Arc::clone(&p2p), Arc::clone(&listings));
        tokio::spawn(async move {
            if listings.handle_response(&source, &data) {
                return;
            }
            let Ok(_permit) = listings.request_permits().acquire().await else {
                return;
            };
            if let Some(answer) = listings.handle_message(&source, &data).await {
                if let Err(e) = p2p.send_message(&source, TOPIC, answer, &[]).await {
                    eprintln!("a listings answer to {source} wasn't sent: {e}");
                }
            }
        });
    }
}

/// A reader of nodes' listings, on a P2P node of its own.
struct Reader {
    p2p: Arc<P2PNode>,
    events: broadcast::Receiver<P2PEvent>,
    /// The pushes that came while it waited for answers.
    pushes: Vec<ChangesResponse>,
    /// The request ids it uses, one after another from a random start: the
    /// complements, which pushes carry, are never among them.
    ids: std::ops::RangeFrom<u64>,
}

impl Reader {
    async fn start() -> Self {
        let identity = Arc::new(NodeIdentity::generate().expect("an identity"));
        let p2p = loopback_node(identity).await;
        let events = p2p.subscribe_events();
        let first = rand::thread_rng().gen_range(1..u64::MAX / 4);
        Self {
            p2p,
            events,
            pushes: Vec::new(),
            ids: first..,
        }
    }

    /// The next message `node` sends it on the listings topic before
    /// `deadline`.
    async fn next_from(&mut self, node: &FeedNode, deadline: Instant) -> Option<Vec<u8>> {
        loop {
            let event = match tokio::time::timeout_at(deadline, self.events.recv()).await {
                Ok(Ok(event)) => event,
                Ok(Err(RecvError::Lagged(_))) => continue,
                Ok(Err(RecvError::Closed)) | Err(_) => return None,
            };
            if let P2PEvent::Message {
                topic,
                source: Some(source),
                data,
                ..
            } = event
            {
                if topic == TOPIC && source == node.peer() {
                    return Some(data);
                }
            }
        }
    }

    /// Keep `data` if it is a push.
    fn keep_push(&mut self, data: &[u8]) {
        if let Ok(ListingsMessage {
            body: ListingsBody::ChangesResponse(push),
            ..
        }) = ListingsMessage::decode(data)
        {
            self.pushes.push(push);
        }
    }

    /// Send `request`, a message with request id `request_id`, to `node`,
    /// and wait for the message `node` sends back with that id, as bytes.
    /// `None` if none comes within [`ANSWER_TIMEOUT`]. A node that has just
    /// started again at another address can't be dialled for a moment: the
    /// request is sent again until it goes, as a reader asks again.
    async fn exchange(
        &mut self,
        node: &FeedNode,
        request_id: u64,
        request: Vec<u8>,
    ) -> Option<Vec<u8>> {
        let sending = Instant::now();
        while let Err(e) = self
            .p2p
            .send_message(
                &node.peer(),
                TOPIC,
                request.clone(),
                std::slice::from_ref(&node.addr),
            )
            .await
        {
            assert!(
                sending.elapsed() < RELEASE_TIMEOUT,
                "the node can't be reached: {e}"
            );
            eprintln!("Sending to the node failed, trying again: {e}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let deadline = Instant::now() + ANSWER_TIMEOUT;
        loop {
            let data = self.next_from(node, deadline).await?;
            if request_id_of(&data) == Some(request_id) {
                return Some(data);
            }
            self.keep_push(&data);
        }
    }

    /// `node`'s answer to `body`.
    async fn ask(&mut self, node: &FeedNode, body: ListingsBody) -> Option<ListingsBody> {
        let request_id = self.ids.next().unwrap();
        let request = ListingsMessage { request_id, body }.encode().unwrap();
        let answer = self.exchange(node, request_id, request).await?;
        Some(ListingsMessage::decode(&answer).expect("an answer").body)
    }

    /// A round of `sync`'s requests to `node`, as a reader polls a node.
    /// Returns how many it made.
    async fn round(&mut self, node: &FeedNode, sync: &mut FeedSync, watch: bool) -> usize {
        sync.begin_round();
        let mut asked = 0;
        while let Some(body) = sync.next_request(false, watch) {
            asked += 1;
            match self.ask(node, body).await {
                Some(answer) => drop(sync.answer(answer)),
                None => sync.no_answer(),
            }
        }
        asked
    }

    /// The next push from `node`, if one comes within `within`.
    async fn push(&mut self, node: &FeedNode, within: Duration) -> Option<ChangesResponse> {
        let deadline = Instant::now() + within;
        while self.pushes.is_empty() {
            let data = self.next_from(node, deadline).await?;
            self.keep_push(&data);
        }
        Some(self.pushes.remove(0))
    }

    /// `node`'s answer to `body`, sent and read as a reader built on
    /// darwin-listings 0.2.1 sends and reads them: every byte of it must
    /// decode.
    async fn ask_as_v021(&mut self, node: &FeedNode, body: BodyV021) -> BodyV021 {
        let request_id = self.ids.next().unwrap();
        let request = postcard::to_stdvec(&MessageV021 { request_id, body }).unwrap();
        let answer = self
            .exchange(node, request_id, request)
            .await
            .expect("an answer");
        let (answer, rest) =
            postcard::take_from_bytes::<MessageV021>(&answer).expect("an answer 0.2.1 reads");
        assert!(rest.is_empty(), "{} bytes after the answer", rest.len());
        answer.body
    }
}

/// The messages a reader built on darwin-listings 0.2.1 knows: the first
/// five kinds of `ListingsBody`, laid out as it lays them out.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum BodyV021 {
    ListRequest { from: ListingKey, limit: u32 },
    ListResponse { listings: Vec<Listing>, more: bool },
    GetRequest { address: [u8; 32] },
    GetResponse { listing: Option<Listing> },
    Error { message: String },
}

/// A message as a reader built on darwin-listings 0.2.1 sends and reads it.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct MessageV021 {
    request_id: u64,
    body: BodyV021,
}

/// A `ChangesRequest` for the whole address space after record `after` of
/// `epoch`.
fn changes_after(epoch: u64, after: u64) -> ChangesRequest {
    ChangesRequest {
        epoch,
        after,
        prefix: AddressRange::ALL.prefix(),
        bits: AddressRange::ALL.bits(),
        limit: MAX_LIST_LIMIT,
        want_peers: false,
        watch: false,
    }
}

/// `node`'s answer to `request`, with what it says as a reader checks it.
async fn changes_from(
    reader: &mut Reader,
    node: &FeedNode,
    request: ChangesRequest,
) -> (ChangesPage, ChangesResponse) {
    let answer = reader
        .ask(node, ListingsBody::ChangesRequest(request.clone()))
        .await;
    let Some(ListingsBody::ChangesResponse(answer)) = answer else {
        panic!("expected the node's changes, got {answer:?}");
    };
    let page = check_changes_page(&request, &answer).expect("a valid answer");
    (page, answer)
}

/// Remove `dir` and what is in it, once the node that had it open lets it
/// go, and make it again, empty: an operator wiping a node's data.
async fn wipe(dir: &Path) {
    let deadline = Instant::now() + RELEASE_TIMEOUT;
    loop {
        match std::fs::remove_dir_all(dir) {
            Ok(()) => break,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Err(e) => {
                assert!(Instant::now() < deadline, "can't wipe the node's data: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    std::fs::create_dir_all(dir).unwrap();
}

/// The tables a version of the node without the change feed keeps the
/// listings in the index in.
struct OlderTables {
    index: Database<Bytes, Bytes>,
    index_addresses: Database<Bytes, Bytes>,
}

/// Open the listing store in `dir` as a version of the node without the
/// change feed does, with only its own tables, and run `op` on them in one
/// write transaction.
async fn as_another_version(dir: &Path, op: impl FnOnce(&mut heed::RwTxn, &OlderTables)) {
    let path = dir.join("listings.mdb");
    let deadline = Instant::now() + RELEASE_TIMEOUT;
    let env = loop {
        #[allow(unsafe_code)]
        // SAFETY: the node that had the store open has stopped, and nothing
        // else opens it while this runs.
        let opened = unsafe {
            EnvOpenOptions::new()
                .map_size(1 << 30)
                .max_dbs(10)
                .open(&path)
        };
        match opened {
            Ok(env) => break env,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "can't open the listing store: {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };
    let mut wtxn = env.write_txn().unwrap();
    let mut table = |name| env.create_database(&mut wtxn, Some(name)).unwrap();
    let tables = OlderTables {
        index: table("index"),
        index_addresses: table("index_addresses"),
    };
    for name in ["unindexed", "quote_keys", "provisional", "pending"] {
        let _: Database<Bytes, Bytes> = table(name);
    }
    op(&mut wtxn, &tables);
    wtxn.commit().unwrap();
    env.prepare_for_closing().wait();
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_nodes_change_feed_survives_a_restart() {
    let dir = TempDir::new().unwrap();
    let identity = Arc::new(NodeIdentity::generate().unwrap());
    let node = FeedNode::start(dir.path(), &identity).await;
    let mut listed = Vec::new();
    for block in 1..=3u8 {
        listed.push(node.list(&envelope_of(block * 4, true), block.into()).await);
    }
    let mut reader = Reader::start().await;

    // A reader's first round reads the node's range afresh, then follows
    // its change feed, watching it.
    let mut sync = FeedSync::new(AddressRange::ALL);
    reader.round(&node, &mut sync, true).await;
    assert!(sync.in_sync(), "{:?}: {:?}", sync.outcome(), sync.problem());
    assert_eq!(
        sync.entries().copied().collect::<Vec<_>>(),
        entries_of(&listed)
    );
    let epoch = sync.epoch();

    // A new listing is pushed to it as the node records it.
    let fourth = node.list(&envelope_of(16, true), 4).await;
    listed.push(fourth.clone());
    let push = reader.push(&node, ANSWER_TIMEOUT).await.expect("a push");
    assert_eq!(sync.push(&push), Pushed::Applied(vec![fourth.address]));
    let head = sync.head();

    // The node stops, and starts again on the same store.
    node.stop().await;
    let node = FeedNode::start(dir.path(), &identity).await;
    // The watch ended with the run that kept it: a listing recorded before
    // the reader asks again isn't pushed.
    let fifth = node.list(&envelope_of(20, true), 5).await;
    listed.push(fifth);
    assert!(reader.push(&node, Duration::from_secs(2)).await.is_none());
    // The reader's next round goes on from where its mirror stood, in the
    // same epoch, with one request, which brings that listing.
    assert_eq!(reader.round(&node, &mut sync, true).await, 1);
    assert!(sync.in_sync(), "{:?}: {:?}", sync.outcome(), sync.problem());
    assert_eq!((sync.epoch(), sync.head()), (epoch, head + 1));
    assert_eq!(
        sync.entries().copied().collect::<Vec<_>>(),
        entries_of(&listed)
    );
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_wiped_node_starts_its_feed_in_a_new_epoch() {
    let dir = TempDir::new().unwrap();
    let identity = Arc::new(NodeIdentity::generate().unwrap());
    let node = FeedNode::start(dir.path(), &identity).await;
    for block in 1..=2u8 {
        node.list(&envelope_of(block * 4, true), block.into()).await;
    }
    let mut reader = Reader::start().await;
    let mut sync = FeedSync::new(AddressRange::ALL);
    reader.round(&node, &mut sync, false).await;
    assert!(sync.in_sync(), "{:?}: {:?}", sync.outcome(), sync.problem());
    assert_eq!(sync.len(), 2);
    let (epoch, head) = (sync.epoch(), sync.head());

    // The node's data is wiped, and it starts again as the same node.
    node.stop().await;
    wipe(dir.path()).await;
    let node = FeedNode::start(dir.path(), &identity).await;

    // Asked for the changes after where the reader stood, it sends none,
    // only where its feed stands: another epoch, with nothing in it.
    let (page, answer) = changes_from(&mut reader, &node, changes_after(epoch, head)).await;
    let ChangesPage::Restart {
        epoch: new_epoch,
        head: 0,
    } = page
    else {
        panic!("expected a new feed, got {page:?}");
    };
    assert_ne!(new_epoch, epoch);
    assert_eq!(answer.count, 0);
    // The reader's mirror reads the range afresh, and is empty...
    reader.round(&node, &mut sync, false).await;
    assert!(sync.in_sync(), "{:?}: {:?}", sync.outcome(), sync.problem());
    assert_eq!((sync.epoch(), sync.len()), (new_epoch, 0));
    // ...and follows the new feed from there.
    let listed = node.list(&envelope_of(40, true), 9).await;
    assert_eq!(reader.round(&node, &mut sync, false).await, 1);
    assert!(sync.in_sync(), "{:?}: {:?}", sync.outcome(), sync.problem());
    assert_eq!(
        sync.entries().copied().collect::<Vec<_>>(),
        entries_of(&[listed])
    );
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_store_another_version_wrote_to_starts_a_new_epoch() {
    let dir = TempDir::new().unwrap();
    let identity = Arc::new(NodeIdentity::generate().unwrap());
    let node = FeedNode::start(dir.path(), &identity).await;
    let mut listed = Vec::new();
    for block in 1..=3u8 {
        listed.push(node.list(&envelope_of(block * 4, true), block.into()).await);
    }
    let mut reader = Reader::start().await;
    let mut sync = FeedSync::new(AddressRange::ALL);
    reader.round(&node, &mut sync, false).await;
    assert!(sync.in_sync(), "{:?}: {:?}", sync.outcome(), sync.problem());
    let (epoch, head) = (sync.epoch(), sync.head());
    node.stop().await;

    // A version without the change feed, such as one the node was
    // downgraded to, only opens the store: the feed goes on as it was.
    as_another_version(dir.path(), |_, _| {}).await;
    let node = FeedNode::start(dir.path(), &identity).await;
    assert_eq!(reader.round(&node, &mut sync, false).await, 1);
    assert!(sync.in_sync(), "{:?}: {:?}", sync.outcome(), sync.problem());
    assert_eq!((sync.epoch(), sync.head()), (epoch, head));
    node.stop().await;

    // It drops a listing, as it does when the listing's chunk goes, and
    // records nothing of that in the feed. Upgraded again, the node numbers
    // what it serves afresh under a new epoch, and a reader following the
    // old one is told where the new one stands.
    let dropped = listed.remove(1).address;
    as_another_version(dir.path(), |wtxn, tables| {
        let key = tables
            .index_addresses
            .get(wtxn, &dropped)
            .unwrap()
            .expect("the listing's key")
            .to_vec();
        assert!(tables.index.delete(wtxn, &key).unwrap());
        assert!(tables.index_addresses.delete(wtxn, &dropped).unwrap());
    })
    .await;
    let node = FeedNode::start(dir.path(), &identity).await;
    let (page, answer) = changes_from(&mut reader, &node, changes_after(epoch, head)).await;
    let ChangesPage::Restart {
        epoch: new_epoch,
        head: 2,
    } = page
    else {
        panic!("expected the feed numbered afresh, got {page:?}");
    };
    assert_ne!(new_epoch, epoch);
    assert_eq!(answer.count, 2);
    // The reader's mirror reads the range afresh, without that listing.
    reader.round(&node, &mut sync, false).await;
    assert!(sync.in_sync(), "{:?}: {:?}", sync.outcome(), sync.problem());
    assert_eq!(sync.epoch(), new_epoch);
    assert_eq!(
        sync.entries().copied().collect::<Vec<_>>(),
        entries_of(&listed)
    );
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_reader_built_on_darwin_listings_0_2_1_is_answered_as_before() {
    // Its messages are laid out as listings.v2 pins them.
    let pinned = MessageV021 {
        request_id: 7,
        body: BodyV021::ListRequest {
            from: ListingKey {
                time: 1_790_000_000,
                block_number: 300,
                log_index: 7,
                address: [0xab; 32],
            },
            limit: 256,
        },
    };
    let mut expected = vec![0x07, 0x00, 0x80, 0xf7, 0xc4, 0xd5, 0x06, 0xac, 0x02, 0x07];
    expected.extend([0xab; 32]);
    expected.extend([0x80, 0x02]);
    assert_eq!(postcard::to_stdvec(&pinned).unwrap(), expected);

    let dir = TempDir::new().unwrap();
    let identity = Arc::new(NodeIdentity::generate().unwrap());
    let node = FeedNode::start(dir.path(), &identity).await;
    let mut indexed = Vec::new();
    for block in [3u8, 1, 2] {
        indexed.push(node.list(&envelope_of(block * 4, true), block.into()).await);
    }
    indexed.sort_by_key(Listing::key);
    let outside = node.list(&envelope_of(40, false), 9).await;
    let mut reader = Reader::start().await;

    // The index in one page, and in pages of two.
    let list = |from, limit| BodyV021::ListRequest { from, limit };
    let listed = |listings: &[Listing], more| BodyV021::ListResponse {
        listings: listings.to_vec(),
        more,
    };
    let page = reader.ask_as_v021(&node, list(ListingKey::MIN, 256)).await;
    assert_eq!(page, listed(&indexed, false));
    let page = reader.ask_as_v021(&node, list(ListingKey::MIN, 2)).await;
    assert_eq!(page, listed(&indexed[..2], true));
    let next = indexed[1].key().checked_next().unwrap();
    let page = reader.ask_as_v021(&node, list(next, 2)).await;
    assert_eq!(page, listed(&indexed[2..], false));
    // Any listing by address, in the index or not, and none for an address
    // without one.
    for listing in indexed.iter().chain([&outside]) {
        let address = listing.address;
        let got = reader
            .ask_as_v021(&node, BodyV021::GetRequest { address })
            .await;
        assert_eq!(
            got,
            BodyV021::GetResponse {
                listing: Some(listing.clone())
            }
        );
    }
    let address = [0x5a; 32];
    let got = reader
        .ask_as_v021(&node, BodyV021::GetRequest { address })
        .await;
    assert_eq!(got, BodyV021::GetResponse { listing: None });
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn requests_a_node_doesnt_know_cost_no_wait() {
    let dir = TempDir::new().unwrap();
    let identity = Arc::new(NodeIdentity::generate().unwrap());
    let node = FeedNode::start(dir.path(), &identity).await;
    node.list(&envelope_of(4, true), 1).await;
    let mut reader = Reader::start().await;

    // A request of a kind added after this release is answered at once, as
    // unsupported, so its sender doesn't wait out its timeout.
    let request_id = reader.ids.next().unwrap();
    let mut request = postcard::to_stdvec(&request_id).unwrap();
    request.extend([9, 1, 2, 3]);
    let sent = Instant::now();
    let answer = reader
        .exchange(&node, request_id, request)
        .await
        .expect("an answer");
    assert!(
        sent.elapsed() < ANSWER_TIMEOUT / 2,
        "answered after {:?}",
        sent.elapsed()
    );
    let unsupported = ListingsMessage {
        request_id,
        body: ListingsBody::Error {
            message: "unsupported".into(),
        },
    };
    assert_eq!(ListingsMessage::decode(&answer).unwrap(), unsupported);

    // A ChangesRequest following an epoch the node never had gets no
    // records, only where the node's feed stands, so that its sender reads
    // the range afresh.
    let (page, _) = changes_from(&mut reader, &node, changes_after(0, 0)).await;
    let ChangesPage::Restart { epoch, head: 1 } = page else {
        panic!("expected where the feed stands, got {page:?}");
    };
    let (page, answer) = changes_from(&mut reader, &node, changes_after(epoch ^ 1, 7)).await;
    assert_eq!(page, ChangesPage::Restart { epoch, head: 1 });
    assert_eq!(answer.count, 1);
    node.stop().await;
}

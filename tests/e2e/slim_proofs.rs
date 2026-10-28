//! Slim proofs read by receivers' own payment checks, on a local Anvil chain.
//!
//! The replication suites fill every node's payment cache, so receivers there
//! accept offers and notices without reading the proof. Here only a real
//! payment stands behind a write: a client pays the median quote on chain and
//! PUTs to one storer, and every other node's cache starts empty. The storer
//! forwards the slim proof (the median-priced quotes, the transaction hashes,
//! byte strings, no sidecars), and each receiver checks it as any proof:
//! signature, payee and amount, in the `FreshReplicationOffer`, `PaidNotify`
//! and `PointerFreshOffer` contexts. Then two answers that send no proof: an
//! "already stored" one, and a PUT accepted from the payment cache whose
//! proof the slim builder declines. Neither costs the storer any trust in
//! the moments its holding notices are handled, and the declined proof
//! sends no offer.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::anvil::TestAnvil;
use super::testnet::{TestNetwork, TestNetworkConfig, TestNode};
use ant_node::ant_protocol::chunk::{PointerPutRequest, PointerPutResponse};
use ant_node::ant_protocol::{
    ChunkMessage, ChunkMessageBody, ChunkPutRequest, ChunkPutResponse, ChunkQuoteRequest,
    ChunkQuoteResponse, CLOSE_GROUP_SIZE, DATA_TYPE_CHUNK, PROOF_TAG_SINGLE_NODE,
};
use ant_node::client::compute_address;
use ant_node::payment::{serialize_single_node_proof, PaymentProof};
use ant_node::pointer::{
    Pointer, PointerTarget, PointerTargetKind, DATA_TYPE_POINTER, POINTER_WIRE_LEN,
};
use ant_node::replication::fresh_metrics::{read, FreshCounter};
use evmlib::common::Amount;
use evmlib::wallet::Wallet;
use evmlib::{EncodedPeerId, PaymentQuote, ProofOfPayment};
use rand::Rng;
use saorsa_core::identity::PeerId;
use saorsa_core::P2PNode;
use saorsa_pqc::api::sig::ml_dsa_65;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// The hold the network runs with: shorter than a node's, so the hold-end
/// notices and offers come sooner.
const HOLD: Duration = Duration::from_secs(3);

/// How long replication has to carry a write to every node it should reach.
const SETTLE: Duration = Duration::from_secs(60);

/// How long after the network forms the trust checks start. Neighbour-sync
/// requests a node sends while the network forms, to peers it can't reach
/// yet, time out 15 s later and are charged to those peers as connection
/// timeouts, which a trust check would take for a penalty.
const FORMING: Duration = Duration::from_secs(20);

/// How long after an answer that sends only holding notices the receivers'
/// trust in the sender is read: the notices land within it, and a penalty
/// for one is charged as it is handled. Kept short, so a transport timeout
/// charged for unrelated traffic in a busy test network is unlikely to land
/// inside it.
const NOTICES_LAND: Duration = Duration::from_secs(2);

/// The timeout of one quote or PUT exchange.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How often to look while waiting.
const POLL: Duration = Duration::from_millis(250);

/// The counters a refused proof or a refused message moves, anywhere in the
/// process.
const REFUSALS: [FreshCounter; 6] = [
    FreshCounter::PopVerificationErrors,
    FreshCounter::PaidNotifyRejected,
    FreshCounter::PointerOffersRefused,
    FreshCounter::ProofsRefusedFormNotice,
    FreshCounter::ProofsRefusedFormOffer,
    FreshCounter::ProofsRefusedFormPointerOffer,
];

fn refusals() -> Vec<u64> {
    REFUSALS.iter().map(|c| read(*c)).collect()
}

fn p2p(node: &TestNode) -> &Arc<P2PNode> {
    node.p2p_node.as_ref().expect("p2p node")
}

fn peer(node: &TestNode) -> PeerId {
    *p2p(node).peer_id()
}

fn node_of<'a>(network: &'a TestNetwork, peer_id: &PeerId) -> &'a TestNode {
    network
        .nodes()
        .iter()
        .find(|node| {
            node.p2p_node
                .as_ref()
                .is_some_and(|p| p.peer_id() == peer_id)
        })
        .expect("a node of the network")
}

/// The `CLOSE_GROUP_SIZE` nodes nearest `address` in the client's view, the
/// client left out.
async fn close_group(client: &Arc<P2PNode>, address: &[u8; 32]) -> Vec<PeerId> {
    let own = *client.peer_id();
    client
        .dht()
        .find_closest_nodes(address, CLOSE_GROUP_SIZE + 1)
        .await
        .expect("the nodes nearest the address")
        .iter()
        .map(|node| node.peer_id)
        .filter(|peer| *peer != own)
        .take(CLOSE_GROUP_SIZE)
        .collect()
}

/// The close group of `address` as `storer` sees it, itself left out: the
/// peers its offers go to. The client is a node of this network too, so it
/// can be one of them, and the client's own close group can then hold a node
/// the storer's doesn't.
async fn offered_by(network: &TestNetwork, storer: &PeerId, address: &[u8; 32]) -> Vec<PeerId> {
    p2p(node_of(network, storer))
        .dht_manager()
        .find_closest_nodes_local_with_self(address, CLOSE_GROUP_SIZE)
        .await
        .into_iter()
        .map(|node| node.peer_id)
        .filter(|peer| peer != storer)
        .collect()
}

/// Hand one chunk-protocol request to `peer`'s request handler, as a
/// client's request arrives, and read its answer. In process: what is under
/// test is what the nodes send each other, not the client's connections.
async fn ask<T>(
    network: &TestNetwork,
    peer: &PeerId,
    body: ChunkMessageBody,
    answer: impl Fn(ChunkMessageBody) -> Option<Result<T, String>>,
) -> Result<T, String> {
    let request_id: u64 = rand::thread_rng().gen();
    let message = ChunkMessage { request_id, body }
        .encode()
        .map_err(|e| format!("encode: {e}"))?;
    let protocol = node_of(network, peer)
        .ant_protocol
        .as_ref()
        .ok_or("the node has no request handler")?;
    let reply = tokio::time::timeout(REQUEST_TIMEOUT, protocol.try_handle_request(&message))
        .await
        .map_err(|_| "timed out".to_string())?
        .map_err(|e| format!("handle: {e}"))?
        .ok_or("no answer")?;
    let body = ChunkMessage::decode(&reply)
        .map_err(|e| format!("decode: {e}"))?
        .body;
    answer(body).unwrap_or_else(|| Err("another kind of answer".to_string()))
}

/// A signed quote from `peer` for `size` bytes of `data_type` named `content`.
async fn quote(
    network: &TestNetwork,
    peer: &PeerId,
    content: [u8; 32],
    size: u64,
    data_type: u32,
) -> Result<PaymentQuote, String> {
    let bytes = ask(
        network,
        peer,
        ChunkMessageBody::QuoteRequest(ChunkQuoteRequest {
            address: content,
            data_size: size,
            data_type,
        }),
        |body| match body {
            ChunkMessageBody::QuoteResponse(ChunkQuoteResponse::Success { quote, .. }) => {
                Some(Ok(quote))
            }
            ChunkMessageBody::QuoteResponse(ChunkQuoteResponse::Error(e)) => {
                Some(Err(format!("quote error: {e:?}")))
            }
            _ => None,
        },
    )
    .await?;
    rmp_serde::from_slice(&bytes).map_err(|e| format!("decode quote: {e}"))
}

/// Pay as a client pays: quotes from `group` for `content`, the median issuer
/// paid three times its price on chain. The whole proof, every quote kept.
async fn pay(
    network: &TestNetwork,
    wallet: &Wallet,
    group: &[PeerId],
    content: [u8; 32],
    size: u64,
    data_type: u32,
) -> Vec<u8> {
    let mut quotes = Vec::new();
    for peer in group {
        match quote(network, peer, content, size, data_type).await {
            Ok(quote) => quotes.push((*peer, quote)),
            Err(e) => eprintln!("no quote from {peer}: {e}"),
        }
    }
    assert!(quotes.len() > 1, "only {} nodes quoted", quotes.len());
    let mut by_price = quotes.clone();
    by_price.sort_by_key(|(_, quote)| quote.price);
    let median = &by_price[by_price.len() / 2].1;
    let amount = median
        .price
        .checked_mul(Amount::from(3u64))
        .expect("no overflow");
    let (paid, _gas) = wallet
        .pay_for_quotes(vec![(median.hash(), median.rewards_address, amount)])
        .await
        .expect("pay on chain");
    serialize_single_node_proof(&PaymentProof {
        proof_of_payment: ProofOfPayment {
            peer_quotes: quotes
                .iter()
                .map(|(peer, quote)| (EncodedPeerId::new(*peer.as_bytes()), quote.clone()))
                .collect(),
        },
        tx_hashes: paid.values().copied().collect(),
        commitment_sidecars: Vec::new(),
    })
    .expect("serialize the proof")
}

/// PUT a chunk to `peer` and return its answer.
async fn put_chunk(
    network: &TestNetwork,
    peer: &PeerId,
    address: [u8; 32],
    content: &[u8],
    proof: Vec<u8>,
) -> ChunkPutResponse {
    ask(
        network,
        peer,
        ChunkMessageBody::PutRequest(ChunkPutRequest::with_payment(
            address,
            bytes::Bytes::copy_from_slice(content),
            proof,
        )),
        |body| match body {
            ChunkMessageBody::PutResponse(response) => Some(Ok(response)),
            _ => None,
        },
    )
    .await
    .expect("a PUT answer")
}

fn holds_chunk(node: &TestNode, address: &[u8; 32]) -> bool {
    node.ant_protocol
        .as_ref()
        .is_some_and(|protocol| matches!(protocol.storage().exists(address), Ok(true)))
}

fn lists_key(node: &TestNode, address: &[u8; 32]) -> bool {
    node.replication_engine
        .as_ref()
        .is_some_and(|engine| matches!(engine.paid_list().contains(address), Ok(true)))
}

fn holds_pointer(node: &TestNode, record: &Pointer) -> bool {
    node.ant_protocol
        .as_ref()
        .and_then(|protocol| protocol.pointer_service())
        .and_then(|service| service.store().state(&record.address()))
        .is_some_and(|state| state.state_id == record.state_id())
}

/// Wait until `done` holds, or `within` passes.
async fn wait_until(mut done: impl FnMut() -> bool, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        tokio::time::sleep(POLL).await;
    }
    done()
}

/// Every node's trust in `storer`, the storer itself left out.
fn trust_in(network: &TestNetwork, storer: &PeerId) -> Vec<(PeerId, f64)> {
    network
        .nodes()
        .iter()
        .filter(|node| peer(node) != *storer)
        .map(|node| (peer(node), p2p(node).peer_trust(storer)))
        .collect()
}

fn assert_no_trust_lost(
    network: &TestNetwork,
    storer: &PeerId,
    before: &[(PeerId, f64)],
    what: &str,
) {
    for (node, was) in before {
        let now = p2p(node_of(network, node)).peer_trust(storer);
        assert!(
            now >= was - f64::EPSILON,
            "{what}: {node} lowered its trust in the storer {storer}: {was} -> {now}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[allow(clippy::too_many_lines)]
async fn receivers_check_slim_proofs_against_the_chain() {
    // The nodes' logs, when RUST_LOG asks for them: the trust changes say
    // which reason a lowered score came from.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("off")),
        )
        .with_writer(std::io::stderr)
        .try_init();
    let anvil = TestAnvil::new().await.expect("start anvil");
    let wallet = anvil.create_funded_wallet().expect("funded wallet");
    let mut config = TestNetworkConfig::small()
        .with_payment_enforcement()
        .with_evm_network(anvil.to_network());
    config.fresh_offer_hold = HOLD;
    let mut network = TestNetwork::new(config).await.expect("build network");
    network.start().await.expect("start network");
    network.warmup_dht().await.expect("warm up the DHT");
    let formed = Instant::now();
    let client = Arc::clone(p2p(network.nodes().last().expect("the client node")));

    // ---- A chunk, paid on chain, PUT to one storer ----
    let content: Vec<u8> = (0..4096).map(|_| rand::random::<u8>()).collect();
    let address = compute_address(&content);
    let group = close_group(&client, &address).await;
    assert!(
        group.len() > 2,
        "only {} nodes near the address",
        group.len()
    );
    let proof = pay(
        &network,
        &wallet,
        &group,
        address,
        content.len() as u64,
        DATA_TYPE_CHUNK,
    )
    .await;
    let refused_before = refusals();
    let storer = group[0];
    let response = put_chunk(&network, &storer, address, &content, proof.clone()).await;
    assert!(
        matches!(response, ChunkPutResponse::Success { .. }),
        "the storer refused the paid PUT: {response:?}"
    );

    // Every peer of the storer's close group stores it from the storer's
    // slim-proof offer, its own payment check reading the chain.
    let offered = offered_by(&network, &storer, &address).await;
    for member in &offered {
        assert!(
            wait_until(|| holds_chunk(node_of(&network, member), &address), SETTLE).await,
            "close-group member {member} never stored the record from the slim-proof offer"
        );
    }
    // Every paid-group member (in a network this small, every node) lists the
    // key: the rest of the close group from the offer or the hold-end notice,
    // the others from the slim-proof notice at announce.
    for node in network.nodes() {
        assert!(
            wait_until(|| lists_key(node, &address), SETTLE).await,
            "paid-group member {} never listed the key from a slim-proof notice",
            peer(node)
        );
    }
    assert_eq!(
        refusals(),
        refused_before,
        "a receiver refused a slim proof (counters {REFUSALS:?})"
    );

    tokio::time::sleep_until(formed + FORMING).await;

    // ---- The same chunk, PUT again to a node that holds it ----
    // It answers "already stored" and may send holding notices: no proof, no
    // offer, and no penalty from any receiver.
    let holder = offered[0];
    let trust_before = trust_in(&network, &holder);
    let held_before = read(FreshCounter::HeldAnswerNoticesSent);
    let response = put_chunk(&network, &holder, address, &content, proof).await;
    assert!(
        matches!(response, ChunkPutResponse::AlreadyExists { .. }),
        "a node holding the chunk answered {response:?}"
    );
    assert!(
        wait_until(
            || read(FreshCounter::HeldAnswerNoticesSent) > held_before,
            NOTICES_LAND
        )
        .await,
        "the \"already stored\" answer sent no holding notices"
    );
    tokio::time::sleep(NOTICES_LAND).await;
    assert_eq!(refusals(), refused_before, "a holding notice was refused");
    assert_no_trust_lost(
        &network,
        &holder,
        &trust_before,
        "an \"already stored\" answer",
    );

    // ---- A pointer, paid on chain at its state, PUT to one storer ----
    let (pk, sk) = ml_dsa_65().generate_keypair().expect("keypair");
    let record = Pointer::sign(
        &sk,
        &pk,
        0,
        PointerTarget::new(PointerTargetKind::Chunk, address),
    )
    .expect("sign");
    let pointer_group = close_group(&client, &record.address()).await;
    let proof = pay(
        &network,
        &wallet,
        &pointer_group,
        record.state_id(),
        POINTER_WIRE_LEN as u64,
        DATA_TYPE_POINTER,
    )
    .await;
    let pointer_storer = pointer_group[0];
    let response = ask(
        &network,
        &pointer_storer,
        ChunkMessageBody::PointerPutRequest(PointerPutRequest::with_payment(
            bytes::Bytes::from(record.to_bytes()),
            proof,
        )),
        |body| match body {
            ChunkMessageBody::PointerPutResponse(response) => Some(Ok(response)),
            _ => None,
        },
    )
    .await
    .expect("a pointer PUT answer");
    assert!(
        matches!(response, PointerPutResponse::Success { .. }),
        "the storer refused the paid pointer PUT: {response:?}"
    );
    for member in &offered_by(&network, &pointer_storer, &record.address()).await {
        assert!(
            wait_until(|| holds_pointer(node_of(&network, member), &record), SETTLE).await,
            "close-group member {member} never stored the pointer from the slim-proof offer"
        );
    }
    assert_eq!(
        refusals(),
        refused_before,
        "a receiver refused a pointer offer's slim proof"
    );

    // ---- A PUT accepted from the payment cache, with a garbled proof ----
    // As a client's retry after a store that failed once its payment was
    // checked: the storer's payment cache holds the address, and the retry
    // carries a garbled proof. The slim builder declines it, so the storer
    // sends holding notices only, holds nothing and offers nothing. The
    // client's PUTs with the paid proof reach the rest of the close group
    // right after, as they would.
    let content: Vec<u8> = (0..4096).map(|_| rand::random::<u8>()).collect();
    let address = compute_address(&content);
    let group = close_group(&client, &address).await;
    let proof = pay(
        &network,
        &wallet,
        &group,
        address,
        content.len() as u64,
        DATA_TYPE_CHUNK,
    )
    .await;
    let storer = group[0];
    let storer_node = node_of(&network, &storer);
    storer_node
        .ant_protocol
        .as_ref()
        .expect("protocol")
        .payment_verifier()
        .cache_insert(address);
    let engine = storer_node.replication_engine.as_ref().expect("engine");
    let dispatched_before = engine.fresh_offers_dispatched();
    let trust_before = trust_in(&network, &storer);
    let declined_before = read(FreshCounter::SlimProofsDeclined);
    let response = put_chunk(
        &network,
        &storer,
        address,
        &content,
        vec![PROOF_TAG_SINGLE_NODE; 64],
    )
    .await;
    assert!(
        matches!(response, ChunkPutResponse::Success { .. }),
        "the storer refused a PUT its cache answers for: {response:?}"
    );
    assert!(read(FreshCounter::SlimProofsDeclined) > declined_before);
    for member in &offered_by(&network, &storer, &address).await {
        let response = put_chunk(&network, member, address, &content, proof.clone()).await;
        assert!(
            matches!(
                response,
                ChunkPutResponse::Success { .. } | ChunkPutResponse::AlreadyExists { .. }
            ),
            "{member} refused the paid PUT: {response:?}"
        );
    }
    tokio::time::sleep(NOTICES_LAND).await;
    assert_no_trust_lost(&network, &storer, &trust_before, "a declined proof");
    tokio::time::sleep(HOLD + NOTICES_LAND).await;
    assert_eq!(
        engine.fresh_offers_dispatched(),
        dispatched_before,
        "the storer offered a record whose proof it declined"
    );
    assert_eq!(
        refusals(),
        refused_before,
        "a declined proof still reached a receiver"
    );

    network.shutdown().await.expect("shut the network down");
}

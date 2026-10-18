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

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::anvil::TestAnvil;
use super::testnet::{TestNetwork, TestNetworkConfig};
use ant_listings::self_encryption::{ChunkInfo, DataMap, XorName as SeXorName};
use ant_listings::{
    Envelope, Listing, ListingsBody, ListingsMessage, Metadata, ShownFields, TOPIC,
};
use ant_node::ant_protocol::{
    ChunkMessage, ChunkMessageBody, ChunkPutRequest, ChunkPutResponse, ChunkQuoteRequest,
    ChunkQuoteResponse, CLOSE_GROUP_SIZE,
};
use ant_node::client::send_and_await_chunk_response;
use ant_node::payment::{serialize_single_node_proof, PaymentProof};
use evmlib::common::Amount;
use evmlib::{EncodedPeerId, PaymentQuote, ProofOfPayment};
use rand::Rng;
use saorsa_core::identity::PeerId;
use saorsa_core::{P2PEvent, P2PNode};
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
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

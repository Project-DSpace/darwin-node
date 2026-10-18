//! The listings hook of the chunk handler, through the handler itself: a node
//! records an envelope's listing when its PUT stores it, keeping only the
//! payment fields the envelope shows, and refuses an envelope whose payment it
//! can't find. A merge that builds the handler without its listing service,
//! or bypasses the hook in the PUT path, fails here.
//!
//! The payment verifier has each address in its cache, as if paid, so what
//! decides each PUT here is the listing hook alone.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::chain::tests::{chain_with, vault_log, MockChain};
use super::service::tests::{envelope_with, get, service_on, single_node_proof, DEAD_CHAIN};
use super::store::tests::signed_quote;
use super::ListingService;
use crate::ant_protocol::{
    ChunkMessage, ChunkMessageBody, ChunkPutRequest, ChunkPutResponse, ProtocolError, XorName,
    CLOSE_GROUP_SIZE,
};
use crate::payment::metrics::QuotingMetricsTracker;
use crate::payment::{
    EvmVerifierConfig, PaymentVerifier, PaymentVerifierConfig, PriceFloorConfig, QuoteGenerator,
};
use crate::storage::{AntProtocol, ChunkStore};
use ant_listings::{PaymentField, PaymentReceipt, ShownFields};
use bytes::Bytes;
use evmlib::RewardsAddress;
use parking_lot::Mutex;
use std::sync::Arc;
use tempfile::TempDir;

/// A chunk handler over `chunks` that records and serves listings with
/// `listings`, as a node builds it.
fn handler(chunks: Arc<ChunkStore>, listings: Arc<ListingService>) -> AntProtocol {
    let rewards = RewardsAddress::new([1; 20]);
    let verifier = PaymentVerifier::new(PaymentVerifierConfig {
        evm: EvmVerifierConfig::default(),
        cache_capacity: 1_000,
        close_group_size: CLOSE_GROUP_SIZE,
        local_rewards_address: rewards,
        price_floor: PriceFloorConfig::default(),
    });
    let quotes = QuoteGenerator::new(rewards, QuotingMetricsTracker::new(0));
    AntProtocol::new(chunks, Arc::new(verifier), Arc::new(quotes)).with_listing_service(listings)
}

/// PUT `content` at `address` through `handler`, its payment cached as
/// verified, with `proof` if given.
async fn put(
    handler: &AntProtocol,
    address: XorName,
    content: &[u8],
    proof: Option<Vec<u8>>,
) -> ChunkPutResponse {
    handler.payment_verifier().cache_insert(address);
    let content = Bytes::copy_from_slice(content);
    let request = match proof {
        Some(proof) => ChunkPutRequest::with_payment(address, content, proof),
        None => ChunkPutRequest::new(address, content),
    };
    let message = ChunkMessage {
        request_id: 7,
        body: ChunkMessageBody::PutRequest(request),
    }
    .encode()
    .unwrap();
    let response = handler.try_handle_request(&message).await.unwrap().unwrap();
    match ChunkMessage::decode(&response).unwrap().body {
        ChunkMessageBody::PutResponse(response) => response,
        other => panic!("expected a PUT response, got {other:?}"),
    }
}

/// The message of a refused PUT, which must be the payment's.
fn refusal(response: ChunkPutResponse) -> String {
    match response {
        ChunkPutResponse::Error(ProtocolError::PaymentFailed(message)) => message,
        other => panic!("expected the PUT refused for its payment, got {other:?}"),
    }
}

#[tokio::test]
async fn an_envelope_put_without_its_proof_is_refused() {
    let dir = TempDir::new().unwrap();
    let (listings, chunks) = service_on(&dir, DEAD_CHAIN).await;
    let handler = handler(Arc::clone(&chunks), Arc::clone(&listings));
    let envelope = envelope_with(1, ShownFields::ALL, true);

    let message = refusal(put(&handler, envelope.address(), &envelope.to_bytes(), None).await);
    assert!(message.contains("needs its payment proof"), "{message}");
    assert!(!chunks.exists(&envelope.address()).unwrap());
    assert_eq!(get(&listings, envelope.address()).await, None);
}

#[tokio::test]
async fn a_paid_envelope_is_listed_with_exactly_its_shown_fields() {
    // Shown: the block, the log index and the quote; hidden: the transaction
    // and the time.
    let shown = ShownFields::ALL
        .without(PaymentField::TxHash)
        .without(PaymentField::BlockTimestamp);
    let envelope = envelope_with(1, shown, true);
    let quote = signed_quote(envelope.address(), 0, 87);
    let mut chain = chain_with(400, &[]);
    chain.add_receipt(PaymentReceipt {
        tx_hash: [0x77; 32],
        success: true,
        block_number: 300,
        logs: vec![vault_log(quote.hash().0, 4)],
    });
    let url = MockChain::serve(Arc::new(Mutex::new(chain))).await;
    let dir = TempDir::new().unwrap();
    let (listings, chunks) = service_on(&dir, &url).await;
    let handler = handler(Arc::clone(&chunks), Arc::clone(&listings));

    let response = put(
        &handler,
        envelope.address(),
        &envelope.to_bytes(),
        Some(single_node_proof(&quote, [0x77; 32])),
    )
    .await;
    assert!(
        matches!(response, ChunkPutResponse::Success { address } if address == envelope.address()),
        "{response:?}"
    );
    assert!(chunks.exists(&envelope.address()).unwrap());
    let listing = get(&listings, envelope.address())
        .await
        .expect("the PUT recorded the envelope's listing");
    assert!(listing.matches(&envelope));
    assert_eq!(listing.flags.shown, shown);
    assert_eq!(listing.payment.tx_hash, None);
    assert_eq!(listing.payment.block_timestamp, None);
    assert_eq!(listing.payment.block_number, Some(300));
    assert_eq!(listing.payment.log_index, Some(4));
    assert_eq!(
        listing.payment.quote,
        Some(rmp_serde::to_vec(&quote).unwrap())
    );
}

#[tokio::test]
async fn an_envelope_whose_proof_pays_something_else_is_refused() {
    let envelope = envelope_with(1, ShownFields::ALL, false);
    let other = envelope_with(2, ShownFields::ALL, false);
    // The transaction pays another envelope's quote.
    let quote = signed_quote(envelope.address(), 0, 87);
    let paid = signed_quote(other.address(), 0, 88);
    let mut chain = chain_with(400, &[]);
    chain.add_receipt(PaymentReceipt {
        tx_hash: [0x78; 32],
        success: true,
        block_number: 310,
        logs: vec![vault_log(paid.hash().0, 2)],
    });
    let url = MockChain::serve(Arc::new(Mutex::new(chain))).await;
    let dir = TempDir::new().unwrap();
    let (listings, chunks) = service_on(&dir, &url).await;
    let handler = handler(Arc::clone(&chunks), Arc::clone(&listings));

    // A proof naming the envelope's quote in a transaction that doesn't pay it,
    // and one naming the other envelope's quote, paid.
    for proof in [
        single_node_proof(&quote, [0x78; 32]),
        single_node_proof(&paid, [0x78; 32]),
    ] {
        let response = put(
            &handler,
            envelope.address(),
            &envelope.to_bytes(),
            Some(proof),
        )
        .await;
        refusal(response);
        assert!(!chunks.exists(&envelope.address()).unwrap());
        assert_eq!(get(&listings, envelope.address()).await, None);
    }
}

#[tokio::test]
async fn an_ordinary_chunk_is_still_taken() {
    let dir = TempDir::new().unwrap();
    let (listings, chunks) = service_on(&dir, DEAD_CHAIN).await;
    let handler = handler(Arc::clone(&chunks), Arc::clone(&listings));
    let content = b"an ordinary chunk, no envelope";
    let address = crate::client::compute_address(content);

    let response = put(&handler, address, content, None).await;
    assert!(
        matches!(response, ChunkPutResponse::Success { address: stored } if stored == address),
        "{response:?}"
    );
    assert_eq!(
        chunks.get(&address).await.unwrap().as_deref(),
        Some(&content[..])
    );
    assert_eq!(get(&listings, address).await, None);
}

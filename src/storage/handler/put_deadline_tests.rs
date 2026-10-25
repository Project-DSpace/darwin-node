//! A PUT's payment check answers inside the time a released client waits
//! for it, whatever the RPCs do: through the chunk handler, its payment read
//! from a chain served over JSON-RPC on 127.0.0.1
//! (`payment::settlement::mock`). With a first RPC that takes calls and
//! never answers, a single PUT and an envelope PUT verify within the
//! client's wait, and a whole 64-chunk wave within the PUT's own deadline;
//! with every RPC stalled, the node still answers before the client gives
//! up, and a later try passes once the chain answers.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::payment::metrics::QuotingMetricsTracker;
use crate::payment::settlement::mock::{self, Act, Bundle, Chain, Spec};
use crate::payment::CLIENT_STORE_WAIT;
use crate::storage::ChunkStoreConfig;
use ant_listings::self_encryption::{ChunkInfo, DataMap, XorName as SeXorName};
use ant_listings::{Envelope, Metadata, ShownFields};
use evmlib::RewardsAddress;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;

/// A chunk handler whose payment check reads the chain through `providers`,
/// in that order, as a node's does, with its listings when `listings`.
async fn handler(providers: &[&mock::Provider], dir: &TempDir, listings: bool) -> AntProtocol {
    let storage = Arc::new(
        ChunkStore::new(ChunkStoreConfig {
            root_dir: dir.path().to_path_buf(),
            ..ChunkStoreConfig::test_default()
        })
        .await
        .unwrap(),
    );
    let rewards = RewardsAddress::new([1; 20]);
    let quotes = QuoteGenerator::new(rewards, QuotingMetricsTracker::new(0));
    let protocol = AntProtocol::new(
        Arc::clone(&storage),
        Arc::new(mock::verifier(providers)),
        Arc::new(quotes),
    );
    if !listings {
        return protocol;
    }
    let service = ListingService::new(
        dir.path(),
        0,
        storage,
        providers[0].url.to_string(),
        mock::VAULT,
        None,
    )
    .await
    .unwrap();
    // As on a node, the listings read the same RPCs as the payment check.
    service.set_payment_rpcs_for_tests(providers.iter().map(|p| p.url.clone()).collect());
    protocol.with_listing_service(service)
}

/// PUT `content`, paid as `bundle` says, its proof naming `tx_hash`.
async fn put(
    handler: &AntProtocol,
    content: &[u8],
    bundle: &Bundle,
    tx_hash: [u8; 32],
) -> ChunkPutResponse {
    let mut request = ChunkPutRequest::new(bundle.address, Bytes::copy_from_slice(content));
    request.payment_proof = Some(bundle.proof(&[tx_hash]));
    handler.handle_put(request).await
}

/// Random chunk bytes, and the quotes for their address.
fn chunk(seed: u8) -> (Vec<u8>, Bundle) {
    let content: Vec<u8> = (0..1_024u32)
        .map(|i| u8::try_from((i * 31 + u32::from(seed) * 7) % 251).unwrap())
        .chain(std::iter::once(seed))
        .collect();
    let bundle = Bundle::new(compute_address(&content));
    (content, bundle)
}

/// A readable envelope, shown in full.
fn envelope() -> Envelope {
    let infos = (0..3u8)
        .map(|index| ChunkInfo {
            index: usize::from(index),
            dst_hash: SeXorName([0x30 + index; 32]),
            src_hash: SeXorName([0x60 + index; 32]),
            src_size: 10,
        })
        .collect();
    Envelope::readable(
        &DataMap::new(infos),
        Metadata {
            content_type: Some("text/plain".into()),
            name: Some("a.txt".into()),
        },
        ShownFields::ALL,
        true,
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_put_verifies_within_the_clients_wait_when_the_first_rpc_never_answers() {
    let chain = Chain::shared();
    let (content, bundle) = chunk(1);
    chain.lock().pay(mock::tx(1), vec![bundle.entry()]);
    let stalled = mock::provider(&chain, Spec::stalled()).await;
    let good = mock::provider(&chain, Spec::default()).await;
    let dir = TempDir::new().unwrap();
    let handler = handler(&[&stalled, &good], &dir, false).await;

    let started = Instant::now();
    let response = put(&handler, &content, &bundle, mock::tx(1)).await;
    let took = started.elapsed();
    assert!(
        matches!(response, ChunkPutResponse::Success { .. }),
        "{response:?}"
    );
    assert!(took < CLIENT_STORE_WAIT, "{took:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_envelope_put_verifies_within_the_clients_wait_when_the_first_rpc_never_answers() {
    let chain = Chain::shared();
    let envelope = envelope();
    let bundle = Bundle::new(envelope.address());
    chain.lock().pay(mock::tx(2), vec![bundle.entry()]);
    let stalled = mock::provider(&chain, Spec::stalled()).await;
    let good = mock::provider(&chain, Spec::default()).await;
    let dir = TempDir::new().unwrap();
    let handler = handler(&[&stalled, &good], &dir, true).await;

    let started = Instant::now();
    let response = put(&handler, &envelope.to_bytes(), &bundle, mock::tx(2)).await;
    let took = started.elapsed();
    assert!(
        matches!(response, ChunkPutResponse::Success { .. }),
        "{response:?}"
    );
    assert!(took < CLIENT_STORE_WAIT, "{took:?}");
    // The envelope's lookup took the receipt the payment check had read.
    assert_eq!(
        good.receipt_reads().len() + stalled.receipt_reads().len(),
        2,
        "the stalled call and the one that answered, and no third"
    );
    let listing = handler
        .listing_service()
        .unwrap()
        .held_listing_for_tests(envelope.address())
        .await
        .expect("the envelope's listing is recorded");
    assert_eq!(listing.payment.tx_hash, Some(mock::tx(2)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wave_of_puts_verifies_within_the_clients_wait_when_the_first_rpc_never_answers() {
    let chain = Chain::shared();
    let wave: Vec<(Vec<u8>, Bundle)> = (0..64u8).map(chunk).collect();
    chain.lock().pay(
        mock::tx(3),
        wave.iter().map(|(_, bundle)| bundle.entry()).collect(),
    );
    let stalled = mock::provider(&chain, Spec::stalled()).await;
    let good = mock::provider(&chain, Spec::default()).await;
    let dir = TempDir::new().unwrap();
    let handler = handler(&[&stalled, &good], &dir, false).await;

    // Each PUT's payment check, as the PUT makes it, all at once. Timed apart
    // from the stores that follow, whose disk writes this test doesn't
    // measure.
    let started = Instant::now();
    let checks = futures::future::join_all(wave.iter().map(|(content, bundle)| async {
        let mut request = ChunkPutRequest::new(bundle.address, Bytes::copy_from_slice(content));
        request.payment_proof = Some(bundle.proof(&[mock::tx(3)]));
        let checked = handler.check_put_payment(&bundle.address, &request).await;
        (checked.is_ok(), started.elapsed())
    }))
    .await;
    for (verified, took) in checks {
        assert!(verified, "a chunk of the wave was refused");
        // Within the deadline the PUT puts on its check, not just the
        // client's wait: a check past it would be refused.
        assert!(took < PUT_PAYMENT_DEADLINE, "{took:?}");
    }
    // The receipt is read once for the whole wave. With the first RPC
    // stalled it answers only once the second is asked, past the receipts'
    // head start, so the wave's records are read beside it, and whichever
    // answers first settles each chunk.
    assert_eq!(
        stalled.receipt_reads().len(),
        1,
        "one receipt call to the stalled RPC"
    );

    for (content, bundle) in &wave {
        let response = put(&handler, content, bundle, mock::tx(3)).await;
        assert!(
            matches!(response, ChunkPutResponse::Success { .. }),
            "{response:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_every_rpc_stalled_a_put_is_answered_before_the_client_gives_up() {
    let chain = Chain::shared();
    let (content, bundle) = chunk(4);
    chain.lock().pay(mock::tx(4), vec![bundle.entry()]);
    // Both stall every request until `answering` is set, then answer.
    let answering = Arc::new(AtomicBool::new(false));
    let spec = || {
        let answering = Arc::clone(&answering);
        Spec::acting(move |_| {
            if answering.load(Ordering::SeqCst) {
                Act::Answer
            } else {
                Act::Stall
            }
        })
    };
    let one = mock::provider(&chain, spec()).await;
    let two = mock::provider(&chain, spec()).await;
    let dir = TempDir::new().unwrap();
    let handler = handler(&[&one, &two], &dir, true).await;

    let started = Instant::now();
    let response = put(&handler, &content, &bundle, mock::tx(4)).await;
    let took = started.elapsed();
    let ChunkPutResponse::Error(ProtocolError::PaymentFailed(message)) = response else {
        panic!("refused with the payment error: {response:?}");
    };
    assert!(message.contains("did not finish"), "{message}");
    assert!(took >= PUT_PAYMENT_DEADLINE, "{took:?}");
    assert!(took < CLIENT_STORE_WAIT, "{took:?}");

    // The chain answers again: the client's next try, with the same proof,
    // passes within the client's wait.
    answering.store(true, Ordering::SeqCst);
    let started = Instant::now();
    let response = put(&handler, &content, &bundle, mock::tx(4)).await;
    let took = started.elapsed();
    assert!(
        matches!(response, ChunkPutResponse::Success { .. }),
        "{response:?}"
    );
    assert!(took < CLIENT_STORE_WAIT, "{took:?}");
}

/// With every RPC slow but answering, a PUT whose proof names a transaction
/// that doesn't pay its quote still verifies by the record's read, read
/// empty and then paid, within the PUT's deadline: the record's read starts
/// beside the proof's receipt after the receipt's short head start, not
/// once the receipt answers. Read after the receipt, the record's read
/// empty, the pause and the read again end past the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_every_rpc_slow_a_put_paid_elsewhere_by_its_receipt_verifies_in_time() {
    let chain = Chain::shared();
    let (content, bundle) = chunk(5);
    let (_, other) = chunk(6);
    chain.lock().pay(mock::tx(5), vec![bundle.entry()]);
    chain.lock().pay(mock::tx(6), vec![other.entry()]);
    let slow = |behind| Spec {
        behind,
        delay: Duration::from_millis(2_300),
        ..Spec::default()
    };
    // The first RPC never sees the payment, so the record reads empty there
    // and is read again at the second.
    let one = mock::provider(&chain, slow(Duration::from_secs(3_600))).await;
    let two = mock::provider(&chain, slow(Duration::ZERO)).await;
    let dir = TempDir::new().unwrap();
    let handler = handler(&[&one, &two], &dir, false).await;

    let started = Instant::now();
    let response = put(&handler, &content, &bundle, mock::tx(6)).await;
    let took = started.elapsed();
    assert!(
        matches!(response, ChunkPutResponse::Success { .. }),
        "{response:?} after {took:?}"
    );
    assert!(took < PUT_PAYMENT_DEADLINE, "{took:?}");
    assert!(
        !one.receipt_reads().is_empty() || !two.receipt_reads().is_empty(),
        "the proof's receipt was read"
    );
    assert!(
        !two.record_reads().is_empty(),
        "the record was read again at the second RPC"
    );
}

/// The deadline sits below the client's wait, with room for the request and
/// the answer to cross the network.
#[test]
fn the_payment_deadline_is_inside_the_clients_wait() {
    assert!(PUT_PAYMENT_DEADLINE < CLIENT_STORE_WAIT);
    assert_eq!(CLIENT_STORE_WAIT, std::time::Duration::from_secs(10));
}

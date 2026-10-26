//! Live driver for the settlement-version quote gate (ADR-0013).
//!
//! Every other test of this gate calls the handler in-process. This one puts
//! the four cases on a real QUIC connection between two nodes built from this
//! branch, closing the gap ADR-0013 recorded before this test existed: the
//! gate had never run over a real connection, because the devnet speaks the
//! pre-versioned dialect. ADR-0013 now records this run under "The gate
//! itself, over a real connection".
//!
//! Four cases per quote path, all against the same node so routing state and
//! quote pricing are shared:
//!
//! - **Unversioned** request — the old client against an upgraded node. Must
//!   still be served, because a node cannot tell a client that settles
//!   correctly but predates the version field from one that does not.
//! - **Current version** — the new client against an upgraded node. Must be
//!   served, so the gate is invisible to clients that can pay.
//! - **Below the minimum** — must come back `ClientUpdateRequired`, refused
//!   *before* any quote is issued, so nothing has been committed on-chain.
//! - **Above this node's current** — must come back `StorerUpdateRequired`.
//!   A lagging node never reports a client fault.
//!
//! `CURRENT` is 3 (root proofs, beside the single-node proofs of version 2,
//! whose payment covers its record's size class) and `MIN` stays 1, so no
//! released client is below the minimum and none is above this node's
//! version: both refusals are driven here by crafting the versions directly.
//! A node of the release before settlement version 2 answers a version-2
//! client with `StorerUpdateRequired`, the refusal this file drives with
//! `CURRENT + 1`.
//!
//! This testnet's nodes know no root vault, so the highest version they honour
//! is the one before root proofs: the "current" case declares that version,
//! a version-3 request is refused as `StorerUpdateRequired` naming it, and so
//! is a version above this build's.
//!
//! A second test pays as an unversioned client does, the full amount at
//! today's price, and checks that this release stores it, at any size.
//!
//! This fork refuses merkle (batch) payments, so on the merkle path the two
//! "served" cases come back refused with `BATCH_PAYMENTS_REFUSED` instead.
//! The version gate runs first, so the two version refusals are unchanged.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::testnet::TestNetworkConfig;
use super::TestHarness;
use ant_node::ant_protocol::{
    ChunkMessage, ChunkMessageBody, ChunkQuoteRequest, ChunkQuoteRequestV2, ChunkQuoteResponse,
    MerkleCandidateQuoteRequest, MerkleCandidateQuoteRequestV2, MerkleCandidateQuoteResponse,
    ProtocolError, CURRENT_SETTLEMENT_VERSION, MIN_SUPPORTED_SETTLEMENT_VERSION,
};
use ant_node::client::send_and_await_chunk_response;
use ant_node::payment::{BATCH_PAYMENTS_REFUSED, ROOT_SETTLEMENT_VERSION};
use saorsa_core::identity::PeerId;
use saorsa_core::P2PNode;
use serial_test::serial;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Node that answers every quote in this driver.
const STORER_INDEX: usize = 1;
/// Node the requests are sent from.
const CLIENT_INDEX: usize = 3;
/// Wire timeout for one quote round trip.
const QUOTE_TIMEOUT: Duration = Duration::from_secs(20);
/// Quote size used throughout; well under `MAX_CHUNK_SIZE`.
const QUOTE_DATA_SIZE: u64 = 4096;

/// Request ids, unique per request so a late response cannot be mistaken for
/// the answer to a later one. That matters here specifically: the fallback
/// path this gate coexists with re-asks under a *new* id.
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(0x5E77_1E00);

fn next_request_id() -> u64 {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Send one chunk message and return the single-node quote response.
async fn single_node_quote(
    client: &Arc<P2PNode>,
    storer: &PeerId,
    request_id: u64,
    body: ChunkMessageBody,
) -> ChunkQuoteResponse {
    let message = ChunkMessage { request_id, body };
    let bytes = message.encode().expect("encode quote request");
    send_and_await_chunk_response(
        client,
        storer,
        bytes,
        request_id,
        QUOTE_TIMEOUT,
        &[],
        |body| match body {
            ChunkMessageBody::QuoteResponse(response) => Some(Ok(response)),
            _ => None,
        },
        |e| format!("send single-node quote request: {e}"),
        || "single-node quote request timed out".to_string(),
    )
    .await
    .expect("single-node quote round trip")
}

/// Send one chunk message and return the merkle candidate quote response.
async fn merkle_quote(
    client: &Arc<P2PNode>,
    storer: &PeerId,
    request_id: u64,
    body: ChunkMessageBody,
) -> MerkleCandidateQuoteResponse {
    let message = ChunkMessage { request_id, body };
    let bytes = message.encode().expect("encode merkle quote request");
    send_and_await_chunk_response(
        client,
        storer,
        bytes,
        request_id,
        QUOTE_TIMEOUT,
        &[],
        |body| match body {
            ChunkMessageBody::MerkleCandidateQuoteResponse(response) => Some(Ok(response)),
            _ => None,
        },
        |e| format!("send merkle quote request: {e}"),
        || "merkle quote request timed out".to_string(),
    )
    .await
    .expect("merkle quote round trip")
}

/// A version below the minimum this node accepts. `saturating_sub` rather than
/// `- 1` because `MIN` is a constant that a future bump moves.
fn below_minimum_version() -> u32 {
    MIN_SUPPORTED_SETTLEMENT_VERSION.saturating_sub(1)
}

/// A version newer than this node understands.
fn above_current_version() -> u32 {
    CURRENT_SETTLEMENT_VERSION.saturating_add(1)
}

/// The highest version a node with no root vault honours, as every node of
/// this testnet is: the one before root proofs.
fn honoured_version() -> u32 {
    ROOT_SETTLEMENT_VERSION.saturating_sub(1)
}

#[tokio::test]
#[serial]
async fn settlement_version_gate_observed_on_live_network() {
    // Route node logs to stderr so a run with
    // `RUST_LOG=ant_node::quote::settlement=warn` shows the refusal lines an
    // operator would have to find in production.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let harness = TestHarness::setup_with_config(TestNetworkConfig::minimal())
        .await
        .expect("setup settlement-gate network");
    harness.warmup_dht().await.expect("warmup");

    let client = harness.node(CLIENT_INDEX).expect("client node");
    let storer_peer = *harness.node(STORER_INDEX).expect("storer node").peer_id();

    let address = [0x51_u8; 32];
    let merkle_address = [0x52_u8; 32];
    let timestamp = unix_now();

    // -- Old client, upgraded node. The pre-versioned request shape, which is
    //    every client released to date, must still get a quote.
    let legacy = single_node_quote(
        &client,
        &storer_peer,
        next_request_id(),
        ChunkMessageBody::QuoteRequest(ChunkQuoteRequest {
            address,
            data_size: QUOTE_DATA_SIZE,
            data_type: 0,
        }),
    )
    .await;
    let legacy_merkle = merkle_quote(
        &client,
        &storer_peer,
        next_request_id(),
        ChunkMessageBody::MerkleCandidateQuoteRequest(MerkleCandidateQuoteRequest {
            address: merkle_address,
            data_type: 0,
            data_size: QUOTE_DATA_SIZE,
            merkle_payment_timestamp: timestamp,
        }),
    )
    .await;

    // -- New client, upgraded node, settling under the highest version the
    //    node honours. The case the whole fleet lands on.
    let mut honoured = ChunkQuoteRequestV2::new(address, QUOTE_DATA_SIZE);
    honoured.settlement_version = honoured_version();
    let current = single_node_quote(
        &client,
        &storer_peer,
        next_request_id(),
        ChunkMessageBody::QuoteRequestV2(honoured),
    )
    .await;
    let mut honoured_merkle =
        MerkleCandidateQuoteRequestV2::new(merkle_address, QUOTE_DATA_SIZE, timestamp);
    honoured_merkle.settlement_version = honoured_version();
    let current_merkle = merkle_quote(
        &client,
        &storer_peer,
        next_request_id(),
        ChunkMessageBody::MerkleCandidateQuoteRequestV2(honoured_merkle),
    )
    .await;

    // -- A client that may pay through the root vault, at a node that knows
    //    none. Refused with a verdict about the node, before any payment.
    let rootless = single_node_quote(
        &client,
        &storer_peer,
        next_request_id(),
        ChunkMessageBody::QuoteRequestV2(ChunkQuoteRequestV2::new(address, QUOTE_DATA_SIZE)),
    )
    .await;

    // -- A client this node will not settle with. Refused, and refused with a
    //    verdict about the client.
    let mut stale = ChunkQuoteRequestV2::new(address, QUOTE_DATA_SIZE);
    stale.settlement_version = below_minimum_version();
    let refused = single_node_quote(
        &client,
        &storer_peer,
        next_request_id(),
        ChunkMessageBody::QuoteRequestV2(stale),
    )
    .await;

    let mut stale_merkle =
        MerkleCandidateQuoteRequestV2::new(merkle_address, QUOTE_DATA_SIZE, timestamp);
    stale_merkle.settlement_version = below_minimum_version();
    let refused_merkle = merkle_quote(
        &client,
        &storer_peer,
        next_request_id(),
        ChunkMessageBody::MerkleCandidateQuoteRequestV2(stale_merkle),
    )
    .await;

    // -- A client newer than this node. Refused, with a verdict about the
    //    *node*, so an up-to-date user is never told to upgrade.
    let mut ahead = ChunkQuoteRequestV2::new(address, QUOTE_DATA_SIZE);
    ahead.settlement_version = above_current_version();
    let node_behind = single_node_quote(
        &client,
        &storer_peer,
        next_request_id(),
        ChunkMessageBody::QuoteRequestV2(ahead),
    )
    .await;

    let mut ahead_merkle =
        MerkleCandidateQuoteRequestV2::new(merkle_address, QUOTE_DATA_SIZE, timestamp);
    ahead_merkle.settlement_version = above_current_version();
    let node_behind_merkle = merkle_quote(
        &client,
        &storer_peer,
        next_request_id(),
        ChunkMessageBody::MerkleCandidateQuoteRequestV2(ahead_merkle),
    )
    .await;

    println!(
        "SETTLEMENT-GATE-RESULT min={MIN_SUPPORTED_SETTLEMENT_VERSION} \
         current={CURRENT_SETTLEMENT_VERSION}"
    );

    assert!(
        matches!(legacy, ChunkQuoteResponse::Success { .. }),
        "REGRESSION: an unversioned single-node quote was not served: {legacy:?}"
    );
    assert!(
        matches!(
            &legacy_merkle,
            MerkleCandidateQuoteResponse::Error(ProtocolError::QuoteFailed(reason))
                if reason == BATCH_PAYMENTS_REFUSED
        ),
        "an unversioned merkle quote was not refused as a batch payment: {legacy_merkle:?}"
    );
    assert!(
        matches!(current, ChunkQuoteResponse::Success { .. }),
        "REGRESSION: the gate is not invisible to a current client: {current:?}"
    );
    assert!(
        matches!(
            &current_merkle,
            MerkleCandidateQuoteResponse::Error(ProtocolError::QuoteFailed(reason))
                if reason == BATCH_PAYMENTS_REFUSED
        ),
        "a current merkle quote was not refused as a batch payment: {current_merkle:?}"
    );

    match refused {
        ChunkQuoteResponse::Error(ProtocolError::ClientUpdateRequired {
            client_settlement_version,
            min_settlement_version,
        }) => {
            assert_eq!(client_settlement_version, below_minimum_version());
            assert_eq!(min_settlement_version, MIN_SUPPORTED_SETTLEMENT_VERSION);
        }
        other => panic!("a stale single-node client was not refused as its own fault: {other:?}"),
    }
    match refused_merkle {
        MerkleCandidateQuoteResponse::Error(ProtocolError::ClientUpdateRequired {
            client_settlement_version,
            min_settlement_version,
        }) => {
            assert_eq!(client_settlement_version, below_minimum_version());
            assert_eq!(min_settlement_version, MIN_SUPPORTED_SETTLEMENT_VERSION);
        }
        other => panic!("a stale merkle client was not refused as its own fault: {other:?}"),
    }
    match rootless {
        ChunkQuoteResponse::Error(ProtocolError::StorerUpdateRequired {
            client_settlement_version,
            node_settlement_version,
        }) => {
            assert_eq!(client_settlement_version, CURRENT_SETTLEMENT_VERSION);
            assert_eq!(node_settlement_version, honoured_version());
        }
        other => panic!("a node with no root vault quoted a root-paying client: {other:?}"),
    }
    match node_behind {
        ChunkQuoteResponse::Error(ProtocolError::StorerUpdateRequired {
            client_settlement_version,
            node_settlement_version,
        }) => {
            assert_eq!(client_settlement_version, above_current_version());
            assert_eq!(node_settlement_version, honoured_version());
        }
        other => panic!("a lagging node blamed the client instead of itself: {other:?}"),
    }
    match node_behind_merkle {
        MerkleCandidateQuoteResponse::Error(ProtocolError::StorerUpdateRequired {
            client_settlement_version,
            node_settlement_version,
        }) => {
            assert_eq!(client_settlement_version, above_current_version());
            assert_eq!(node_settlement_version, honoured_version());
        }
        other => panic!("a lagging node blamed the merkle client instead of itself: {other:?}"),
    }

    harness.teardown().await.expect("teardown");
}

/// An unversioned client pays every record as a full chunk, at today's
/// price, and a node that settles at version 2 (this testnet's nodes know no
/// root vault) stores what it paid for, whatever the record's size: no
/// release ever refuses today's price.
#[tokio::test]
#[serial]
async fn a_client_paying_todays_price_is_stored_by_a_size_pricing_node() {
    use super::TestAnvil;
    use ant_node::ant_protocol::CLOSE_GROUP_MAJORITY;
    use paid::paid_upload;

    assert_eq!(CURRENT_SETTLEMENT_VERSION, ROOT_SETTLEMENT_VERSION);
    assert_eq!(honoured_version(), 2);
    assert_eq!(MIN_SUPPORTED_SETTLEMENT_VERSION, 1);

    // Anvil first, so the nodes verify payments on the chain the client pays on.
    let mut anvil = TestAnvil::new().await.expect("start anvil");
    let wallet = anvil.create_funded_wallet().expect("funded wallet");
    let config = TestNetworkConfig::small()
        .with_payment_enforcement()
        .with_evm_network(anvil.to_network());
    let harness = TestHarness::setup_with_config(config)
        .await
        .expect("setup paid network");
    harness.warmup_dht().await.expect("warmup");

    let client = harness.node(CLIENT_INDEX).expect("client node");
    let client_peer = *client.peer_id();
    // The smallest class, a middle one and a full chunk.
    for size in [4096, 300 * 1024, 4 * 1024 * 1024] {
        let mut data = vec![0u8; size];
        rand::Rng::fill(&mut rand::thread_rng(), &mut data[..]);
        let (outcome, address) = paid_upload(&client, &client_peer, &wallet, &data)
            .await
            .unwrap_or_else(|e| panic!("a {size}-byte record paid at today's price: {e}"));
        assert!(
            outcome.stored_acks >= CLOSE_GROUP_MAJORITY,
            "a {size}-byte record paid at today's price was stored by {} storers \
             ({})",
            outcome.stored_acks,
            hex::encode(address)
        );
    }

    harness.teardown().await.expect("teardown");
    anvil.shutdown().await;
}

/// A paid upload as an unversioned client sends it, kept from the A/B load
/// driver (`first_audit_ab.rs`) when that file was removed.
mod paid {
    use ant_node::ant_protocol::{
        ChunkMessage, ChunkMessageBody, ChunkPutRequest, ChunkPutResponse, ChunkQuoteRequest,
        ChunkQuoteResponse, CLOSE_GROUP_SIZE,
    };
    use ant_node::client::send_and_await_chunk_response;
    use ant_node::payment::{serialize_single_node_proof, PaymentProof};
    use evmlib::common::Amount;
    use evmlib::{EncodedPeerId, ProofOfPayment};
    use rand::Rng;
    use saorsa_core::identity::PeerId;
    use saorsa_core::P2PNode;
    use std::sync::Arc;
    use std::time::Duration;

    /// The verifier accepts a settlement of at least 3x the median quote
    /// price. Mirrors `PAID_QUOTE_PAYMENT_MULTIPLIER` (private to the
    /// verifier).
    const PAYMENT_MULTIPLIER: u64 = 3;

    /// Per-request timeout for quote and PUT round trips.
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

    /// One collected quote: the issuing peer, the decoded quote, and the raw
    /// commitment sidecar blob (when the quote pinned a commitment).
    struct CollectedQuote {
        peer: PeerId,
        quote: evmlib::PaymentQuote,
        sidecar: Option<Vec<u8>>,
    }

    /// Request a signed, unversioned quote (plus its commitment sidecar) from
    /// one peer.
    async fn request_quote(
        client: &Arc<P2PNode>,
        peer: &PeerId,
        address: [u8; 32],
        data_size: u64,
    ) -> Result<CollectedQuote, String> {
        let request_id: u64 = rand::thread_rng().gen();
        let message = ChunkMessage {
            request_id,
            body: ChunkMessageBody::QuoteRequest(ChunkQuoteRequest {
                address,
                data_size,
                data_type: 0,
            }),
        };
        let message_bytes = message
            .encode()
            .map_err(|e| format!("encode quote request: {e}"))?;

        let (quote_bytes, sidecar) = send_and_await_chunk_response(
            client,
            peer,
            message_bytes,
            request_id,
            REQUEST_TIMEOUT,
            &[],
            |body| match body {
                ChunkMessageBody::QuoteResponse(ChunkQuoteResponse::Success {
                    quote,
                    already_stored: _,
                    commitment,
                }) => Some(Ok((quote, commitment))),
                ChunkMessageBody::QuoteResponse(ChunkQuoteResponse::Error(e)) => {
                    Some(Err(format!("quote error: {e:?}")))
                }
                _ => None,
            },
            |e| format!("send quote request: {e}"),
            || "quote request timed out".to_string(),
        )
        .await?;

        let quote: evmlib::PaymentQuote =
            rmp_serde::from_slice(&quote_bytes).map_err(|e| format!("decode quote: {e}"))?;
        Ok(CollectedQuote {
            peer: *peer,
            quote,
            sidecar,
        })
    }

    /// Send a paid PUT to one peer; `Ok(true)` when it stored (or already
    /// had it).
    async fn paid_put(
        client: &Arc<P2PNode>,
        peer: &PeerId,
        address: [u8; 32],
        data: &[u8],
        proof_bytes: Vec<u8>,
    ) -> Result<bool, String> {
        let request_id: u64 = rand::thread_rng().gen();
        let message = ChunkMessage {
            request_id,
            body: ChunkMessageBody::PutRequest(ChunkPutRequest::with_payment(
                address,
                bytes::Bytes::copy_from_slice(data),
                proof_bytes,
            )),
        };
        let message_bytes = message
            .encode()
            .map_err(|e| format!("encode put request: {e}"))?;

        send_and_await_chunk_response(
            client,
            peer,
            message_bytes,
            request_id,
            REQUEST_TIMEOUT,
            &[],
            |body| match body {
                ChunkMessageBody::PutResponse(
                    ChunkPutResponse::Success { .. } | ChunkPutResponse::AlreadyExists { .. },
                ) => Some(Ok(true)),
                ChunkMessageBody::PutResponse(ChunkPutResponse::PaymentRequired { message }) => {
                    Some(Err(format!("payment required: {message}")))
                }
                ChunkMessageBody::PutResponse(ChunkPutResponse::Error(e)) => {
                    Some(Err(format!("put error: {e:?}")))
                }
                _ => None,
            },
            |e| format!("send put: {e}"),
            || "put timed out".to_string(),
        )
        .await
    }

    /// Outcome of one full paid upload.
    pub struct UploadOutcome {
        /// How many storers accepted the PUT (stored, or held it already).
        pub stored_acks: usize,
    }

    /// One production-shaped paid upload: collect quotes from the close
    /// group, pay the median issuer 3x on chain, then PUT the chunk (with the
    /// full quote bundle and sidecars) to every close-group peer. The quotes
    /// are unversioned, as a client released before settlement versions asks
    /// them, so they are priced and paid at today's price whatever the
    /// chunk's size.
    pub async fn paid_upload(
        client: &Arc<P2PNode>,
        client_peer: &PeerId,
        wallet: &evmlib::wallet::Wallet,
        data: &[u8],
    ) -> Result<(UploadOutcome, [u8; 32]), String> {
        let address = ant_node::compute_address(data);

        let closest = client
            .dht()
            .find_closest_nodes(&address, CLOSE_GROUP_SIZE + 1)
            .await
            .map_err(|e| format!("find closest: {e}"))?;
        let storers: Vec<PeerId> = closest
            .iter()
            .map(|n| n.peer_id)
            .filter(|p| p != client_peer)
            .take(CLOSE_GROUP_SIZE)
            .collect();
        if storers.is_empty() {
            return Err("no close-group peers found".to_string());
        }

        let mut quotes: Vec<CollectedQuote> = Vec::with_capacity(storers.len());
        for peer in &storers {
            match request_quote(client, peer, address, data.len() as u64).await {
                Ok(q) => quotes.push(q),
                Err(e) => eprintln!("paid upload: quote from {peer} failed: {e}"),
            }
        }
        if quotes.is_empty() {
            return Err("no quotes collected".to_string());
        }

        // Median selection mirrors the node verifier: sort by price, index
        // len/2, settle 3x that price to the median issuer.
        quotes.sort_by_key(|q| q.quote.price);
        let median = quotes
            .get(quotes.len() / 2)
            .ok_or_else(|| "median index out of range".to_string())?;
        let amount = median
            .quote
            .price
            .checked_mul(Amount::from(PAYMENT_MULTIPLIER))
            .ok_or_else(|| "payment amount overflow".to_string())?;
        let (tx_by_quote, _gas) = wallet
            .pay_for_quotes(vec![(
                median.quote.hash(),
                median.quote.rewards_address,
                amount,
            )])
            .await
            .map_err(|e| format!("pay_for_quotes: {e:?}"))?;
        let tx_hashes: Vec<evmlib::common::TxHash> = tx_by_quote.values().copied().collect();

        let proof = PaymentProof {
            proof_of_payment: ProofOfPayment {
                peer_quotes: quotes
                    .iter()
                    .map(|q| (EncodedPeerId::new(*q.peer.as_bytes()), q.quote.clone()))
                    .collect(),
            },
            tx_hashes,
            commitment_sidecars: quotes.iter().filter_map(|q| q.sidecar.clone()).collect(),
        };
        let proof_bytes =
            serialize_single_node_proof(&proof).map_err(|e| format!("serialize proof: {e}"))?;

        let mut stored_acks = 0usize;
        for peer in &storers {
            match paid_put(client, peer, address, data, proof_bytes.clone()).await {
                Ok(true) => stored_acks += 1,
                Ok(false) => {}
                Err(e) => eprintln!("paid upload: put to {peer} failed: {e}"),
            }
        }
        if stored_acks == 0 {
            return Err("no storer accepted the paid put".to_string());
        }

        Ok((UploadOutcome { stored_acks }, address))
    }
}

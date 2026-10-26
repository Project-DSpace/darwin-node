//! Chunk data type tests.
//!
//! Chunks are immutable, content-addressed data blocks (up to 4MB).
//! The address is derived from the content hash (BLAKE3 -> `XorName`).
//!
//! ## Test Coverage
//!
//! - Store and retrieve through a node's handler, up to the 4 MiB limit, and
//!   a larger chunk refused (no network)
//! - Chunks persist across a restart of the handler (no network)
//! - A 4 MiB chunk stored on a remote node over QUIC (a 5-node network)
//! - Every PUT without a valid payment refused, nothing stored and no
//!   payment read (no network)

#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::{TestData, MAX_CHUNK_SIZE};

/// Size of small test data (1KB).
const SMALL_CHUNK_SIZE: usize = 1024;

/// Size of medium test data (1MB).
const MEDIUM_CHUNK_SIZE: usize = 1024 * 1024;

/// Test fixture for chunk operations.
#[allow(clippy::struct_field_names)]
pub struct ChunkTestFixture {
    /// Small test data (1KB).
    pub small: Vec<u8>,
    /// Medium test data (1MB).
    pub medium: Vec<u8>,
    /// Large test data (4MB - max size).
    pub large: Vec<u8>,
}

impl Default for ChunkTestFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkTestFixture {
    /// Create a new test fixture with pre-generated data.
    #[must_use]
    pub fn new() -> Self {
        Self {
            small: TestData::generate(SMALL_CHUNK_SIZE),
            medium: TestData::generate(MEDIUM_CHUNK_SIZE),
            large: TestData::generate(MAX_CHUNK_SIZE),
        }
    }

    /// Compute content address for data (BLAKE3 hash).
    #[must_use]
    pub fn compute_address(data: &[u8]) -> [u8; 32] {
        ant_node::compute_address(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::{TestHarness, TestNetwork};
    use ant_node::ant_protocol::{
        ChunkGetRequest, ChunkGetResponse, ChunkMessage, ChunkMessageBody, ChunkPutRequest,
        ChunkPutResponse, ProtocolError, PROOF_TAG_MERKLE,
    };
    use ant_node::payment::{
        deserialize_merkle_proof, serialize_merkle_proof, PaymentProof, BATCH_PAYMENTS_REFUSED,
        MAX_PAYMENT_PROOF_SIZE_BYTES, MIN_PAYMENT_PROOF_SIZE_BYTES,
    };
    use ant_node::storage::AntProtocol;
    use bytes::Bytes;
    use evmlib::merkle_payments::{
        MerklePaymentCandidateNode, MerklePaymentCandidatePool, MerklePaymentProof, MerkleTree,
        OnChainPaymentInfo, CANDIDATES_PER_POOL,
    };
    use evmlib::{ProofOfPayment, RewardsAddress};
    use rand::seq::SliceRandom;
    use saorsa_core::MlDsa65;
    use saorsa_pqc::pqc::types::MlDsaSecretKey;
    use saorsa_pqc::pqc::MlDsaOperations;

    /// A node's chunk handler on its own, with no network: a fresh store in
    /// `dir`, payments checked on `evm` (the test default chain if `None`).
    async fn handler(dir: &std::path::Path, evm: Option<evmlib::Network>) -> AntProtocol {
        let identity = saorsa_core::identity::NodeIdentity::generate().expect("identity");
        TestNetwork::create_ant_protocol(dir, evm, &identity)
            .await
            .expect("chunk handler")
    }

    /// The handler's answer to one request, or its error.
    async fn try_ask(
        protocol: &AntProtocol,
        body: ChunkMessageBody,
    ) -> Result<ChunkMessageBody, String> {
        let message = ChunkMessage {
            request_id: rand::random(),
            body,
        };
        let response = protocol
            .try_handle_request(&message.encode().expect("encode"))
            .await
            .map_err(|e| e.to_string())?
            .expect("a response");
        Ok(ChunkMessage::decode(&response).expect("decode").body)
    }

    /// The handler's answer to one request.
    async fn ask(protocol: &AntProtocol, body: ChunkMessageBody) -> ChunkMessageBody {
        try_ask(protocol, body).await.expect("handle")
    }

    /// Stores `data` as a paid-for chunk (its payment cached, as a node that
    /// has verified it), returning the handler's answer or its error.
    async fn try_put_paid(protocol: &AntProtocol, data: &[u8]) -> Result<ChunkMessageBody, String> {
        let address = ChunkTestFixture::compute_address(data);
        protocol.payment_verifier().cache_insert(address);
        let request = ChunkPutRequest::new(address, Bytes::copy_from_slice(data));
        try_ask(protocol, ChunkMessageBody::PutRequest(request)).await
    }

    /// Stores `data` as a paid-for chunk, returning the handler's answer.
    async fn put_paid(protocol: &AntProtocol, data: &[u8]) -> ChunkMessageBody {
        try_put_paid(protocol, data).await.expect("handle")
    }

    /// The chunk at `address`, if the handler has it.
    async fn get(protocol: &AntProtocol, address: [u8; 32]) -> Option<Vec<u8>> {
        let request = ChunkGetRequest::new(address);
        match ask(protocol, ChunkMessageBody::GetRequest(request)).await {
            ChunkMessageBody::GetResponse(ChunkGetResponse::Success {
                address: at,
                content,
            }) => {
                assert_eq!(at, address, "the chunk comes back at its address");
                Some(content.to_vec())
            }
            ChunkMessageBody::GetResponse(ChunkGetResponse::NotFound { .. }) => None,
            other => panic!("unexpected GET answer: {other:?}"),
        }
    }

    /// Small and 4 MiB chunks are stored at their content address and read
    /// back whole; a chunk over 4 MiB is refused and not stored. Upstream ran
    /// each case on its own 5-node network, through this same handler path.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_chunk_store_retrieve_and_size_limit() {
        let dir = tempfile::tempdir().expect("temp dir");
        let protocol = handler(dir.path(), None).await;
        let fixture = ChunkTestFixture::new();

        for (label, data) in [("1 KiB", &fixture.small), ("4 MiB", &fixture.large)] {
            let address = ChunkTestFixture::compute_address(data);
            match put_paid(&protocol, data).await {
                ChunkMessageBody::PutResponse(ChunkPutResponse::Success { address: at }) => {
                    assert_eq!(at, address, "{label}: stored at its content address");
                }
                other => panic!("{label}: expected Success, got {other:?}"),
            }
            assert_eq!(
                get(&protocol, address).await.as_deref(),
                Some(data.as_slice()),
                "{label}: read back whole"
            );
        }

        let oversized = TestData::generate(MAX_CHUNK_SIZE * 2);
        // Refused as an error from the handler (the message is over its size
        // limit) or as an error answer.
        let answer = try_put_paid(&protocol, &oversized).await;
        assert!(
            !matches!(
                answer,
                Ok(ChunkMessageBody::PutResponse(
                    ChunkPutResponse::Success { .. } | ChunkPutResponse::AlreadyExists { .. }
                ))
            ),
            "a chunk over 4 MiB is refused, got {answer:?}"
        );
        assert_eq!(
            get(&protocol, ChunkTestFixture::compute_address(&oversized)).await,
            None,
            "the oversized chunk is not stored"
        );
    }

    // =========================================================================
    // Cross-Node Tests (require P2P network)
    // =========================================================================

    /// Test 8: One node asks another to store a max-size chunk (4 MiB) via P2P.
    ///
    /// This test validates the full cross-node protocol flow with the largest
    /// allowed payload, exercising QUIC stream flow-control limits:
    /// 1. Spins up a minimal 5-node local testnet
    /// 2. A regular node (node 3) discovers connected peers
    /// 3. Picks a random peer and sends a `ChunkPutRequest` with a 4 MiB chunk
    /// 4. The target node stores the chunk and responds with success
    /// 5. The regular node then sends a `ChunkGetRequest` to retrieve it
    /// 6. Verifies the 4 MiB data round-trips correctly
    #[tokio::test(flavor = "multi_thread")]
    async fn test_chunk_store_on_remote_node() {
        let harness = TestHarness::setup_minimal()
            .await
            .expect("Failed to setup test harness");

        let fixture = ChunkTestFixture::new();

        // Node 3 (regular) discovers its connected peers and picks a random one
        let requester = harness.test_node(3).expect("Node 3 should exist");
        let peers = requester.connected_peers().await;
        assert!(
            !peers.is_empty(),
            "Node 3 should have at least one connected peer"
        );

        let mut rng = rand::thread_rng();
        let target_peer_id = peers.choose(&mut rng).expect("peers is non-empty");

        // Pre-populate payment cache on the target node so the store is accepted
        let expected_address = ChunkTestFixture::compute_address(&fixture.large);
        harness.prepopulate_payment_cache_for_peer(target_peer_id, &expected_address);

        // Use the max-size (4 MiB) chunk to exercise QUIC stream limits
        let address = requester
            .store_chunk_on_peer(target_peer_id, &fixture.large)
            .await
            .expect("Failed to store max-size chunk on remote node");

        // Verify the returned address matches the expected content hash
        assert_eq!(
            address, expected_address,
            "Returned address should match computed content address"
        );

        // Retrieve the chunk back from the same remote peer via P2P
        let retrieved = requester
            .get_chunk_from_peer(target_peer_id, &address)
            .await
            .expect("Failed to retrieve max-size chunk from remote node");

        let chunk = retrieved.expect("Max-size chunk should exist on remote storage node");
        assert_eq!(
            chunk.content.len(),
            fixture.large.len(),
            "Retrieved chunk size should match original (4 MiB)"
        );
        assert_eq!(
            chunk.content.as_ref(),
            fixture.large.as_slice(),
            "Retrieved data should match original"
        );
        assert_eq!(
            chunk.address, address,
            "Chunk address should match the stored address"
        );

        harness
            .teardown()
            .await
            .expect("Failed to teardown harness");
    }

    /// Chunks persist across a restart: chunks of 1 KiB, 1 MiB and 4 MiB are
    /// stored, the handler and its store are dropped, and a new handler on
    /// the same folder serves them all. Upstream did this on node 0 of a
    /// 5-node network; the network played no part in it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_chunk_persist_across_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let fixture = ChunkTestFixture::new();
        let chunks: [(&str, &[u8]); 3] = [
            ("small", &fixture.small),
            ("medium", &fixture.medium),
            ("large", &fixture.large),
        ];

        let protocol = handler(dir.path(), None).await;
        for (label, data) in chunks {
            assert!(
                matches!(
                    put_paid(&protocol, data).await,
                    ChunkMessageBody::PutResponse(ChunkPutResponse::Success { .. })
                ),
                "{label}: stored"
            );
        }
        drop(protocol);

        let restarted = handler(dir.path(), None).await;
        for (label, data) in chunks {
            assert_eq!(
                get(&restarted, ChunkTestFixture::compute_address(data))
                    .await
                    .as_deref(),
                Some(data),
                "{label} chunk is served after the restart"
            );
        }
    }

    /// An RPC endpoint that only counts the connections made to it: any
    /// payment read reaches it as a connection.
    struct CountingRpc {
        url: String,
        connections: Arc<AtomicUsize>,
    }

    fn counting_rpc() -> CountingRpc {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let connections = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&connections);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                counted.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });
        CountingRpc { url, connections }
    }

    /// A merkle (batch) proof with real ML-DSA-65 signatures for the first of
    /// four chunks, as upstream's merkle attack tests built it: the chunk's
    /// content, and the tagged proof bytes.
    fn signed_merkle_proof() -> (Vec<u8>, Vec<u8>) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_secs();
        let contents: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i; 64]).collect();
        let addresses: Vec<xor_name::XorName> = contents
            .iter()
            .map(|c| xor_name::XorName(ant_node::compute_address(c)))
            .collect();
        let tree = MerkleTree::from_xornames(addresses.clone()).expect("tree");
        let price = ant_node::payment::pricing::calculate_price(0);
        let candidate_nodes: [MerklePaymentCandidateNode; CANDIDATES_PER_POOL] =
            std::array::from_fn(|i| {
                let ml_dsa = MlDsa65::new();
                let (pub_key, secret_key) = ml_dsa.generate_keypair().expect("keygen");
                #[allow(clippy::cast_possible_truncation)]
                let reward_address = RewardsAddress::new([i as u8; 20]);
                let msg = MerklePaymentCandidateNode::bytes_to_sign(
                    &price,
                    &reward_address,
                    timestamp,
                    0,
                    &None,
                );
                let sk = MlDsaSecretKey::from_bytes(secret_key.as_bytes()).expect("sk");
                MerklePaymentCandidateNode {
                    pub_key: pub_key.as_bytes().to_vec(),
                    price,
                    reward_address,
                    merkle_payment_timestamp: timestamp,
                    committed_key_count: 0,
                    commitment_pin: None,
                    signature: ml_dsa.sign(&sk, &msg).expect("sign").as_bytes().to_vec(),
                }
            });
        let midpoint_proof = tree
            .reward_candidates(timestamp)
            .expect("reward candidates")
            .first()
            .expect("a candidate")
            .clone();
        let pool = MerklePaymentCandidatePool {
            midpoint_proof,
            candidate_nodes,
        };
        let address_proof = tree
            .generate_address_proof(0, addresses[0])
            .expect("address proof");
        let proof = MerklePaymentProof::new(addresses[0], address_proof, pool);
        (
            contents[0].clone(),
            serialize_merkle_proof(&proof).expect("serialize merkle proof"),
        )
    }

    /// A PUT without a valid payment is refused, the chunk is not stored, and
    /// the refusal reads nothing from the chain. One handler with payments
    /// checked on an RPC that counts connections, and no network: each case
    /// below was upstream's `security_attacks` or `merkle_payment` e2e test,
    /// on a 5- or 14-node network with Anvil.
    ///
    /// - no proof, an empty one, garbage, a proof with no quotes: refused
    ///   (`PaymentRequired`, or a payment error);
    /// - a proof one byte over `MAX_PAYMENT_PROOF_SIZE_BYTES`: refused by the
    ///   size gate, before it is parsed;
    /// - merkle-tagged garbage, and a well-formed, signed merkle proof whose
    ///   payment is on record (upstream's pay-yourself pool): refused as a
    ///   batch payment (`BATCH_PAYMENTS_REFUSED`, this fork's merkle refusal).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_chunk_rejected_without_payment() {
        let rpc = counting_rpc();
        let evm = evmlib::Network::new_custom(
            &rpc.url,
            "0x5FbDB2315678afecb367f032d93F642f64180aa3",
            "0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512",
        );
        let dir = tempfile::tempdir().expect("temp dir");
        let protocol = handler(dir.path(), Some(evm)).await;

        let empty_quotes = rmp_serde::to_vec(&PaymentProof {
            proof_of_payment: ProofOfPayment {
                peer_quotes: vec![],
            },
            tx_hashes: vec![],
            commitment_sidecars: vec![],
        })
        .expect("serialize");
        let mut tagged_garbage = vec![PROOF_TAG_MERKLE, 0xDE, 0xAD, 0xBE, 0xEF];
        tagged_garbage.resize(MIN_PAYMENT_PROOF_SIZE_BYTES.max(5), 0);

        // The pay-yourself pool: its payment put on record in the pool cache,
        // so only the merkle refusal can turn it away.
        let (merkle_content, merkle_proof) = signed_merkle_proof();
        let parsed = deserialize_merkle_proof(&merkle_proof).expect("parse merkle proof");
        let timestamp = parsed.winner_pool.candidate_nodes[0].merkle_payment_timestamp;
        let per_node =
            ant_node::payment::pricing::calculate_price(0) * evmlib::common::Amount::from(6u64);
        protocol.payment_verifier().pool_cache_insert(
            parsed.winner_pool_hash(),
            OnChainPaymentInfo {
                depth: 2,
                merkle_payment_timestamp: timestamp,
                paid_node_addresses: vec![
                    (RewardsAddress::new([0u8; 20]), 0, per_node),
                    (RewardsAddress::new([1u8; 20]), 1, per_node),
                ],
            },
        );

        /// What a case must be refused with.
        enum Refusal {
            Payment,
            TooLarge,
            Batch,
        }
        let cases: Vec<(&str, Vec<u8>, Option<Vec<u8>>, Refusal)> = vec![
            ("no proof", b"no proof".to_vec(), None, Refusal::Payment),
            (
                "0 bytes",
                b"empty proof".to_vec(),
                Some(vec![]),
                Refusal::Payment,
            ),
            (
                "garbage",
                b"garbage proof".to_vec(),
                Some((0..64).map(|_| rand::random()).collect()),
                Refusal::Payment,
            ),
            (
                "empty quotes",
                b"empty quotes".to_vec(),
                Some(empty_quotes),
                Refusal::Payment,
            ),
            (
                "MAX+1 bytes",
                b"oversized proof".to_vec(),
                Some(vec![0xAA; MAX_PAYMENT_PROOF_SIZE_BYTES + 1]),
                Refusal::TooLarge,
            ),
            (
                "merkle-tagged garbage",
                b"merkle-tagged garbage".to_vec(),
                Some(tagged_garbage),
                Refusal::Batch,
            ),
            (
                "merkle proof paid on record",
                merkle_content,
                Some(merkle_proof),
                Refusal::Batch,
            ),
        ];

        for (case, content, proof, refusal) in cases {
            let address = ChunkTestFixture::compute_address(&content);
            let request = match proof {
                None => ChunkPutRequest::new(address, Bytes::from(content)),
                Some(proof) => ChunkPutRequest::with_payment(address, Bytes::from(content), proof),
            };
            let answer = ask(&protocol, ChunkMessageBody::PutRequest(request)).await;
            let refused = match (&refusal, &answer) {
                (
                    Refusal::Payment,
                    ChunkMessageBody::PutResponse(
                        ChunkPutResponse::PaymentRequired { .. }
                        | ChunkPutResponse::Error(ProtocolError::PaymentFailed(_)),
                    ),
                ) => true,
                (
                    Refusal::TooLarge,
                    ChunkMessageBody::PutResponse(ChunkPutResponse::Error(
                        ProtocolError::PaymentFailed(message),
                    )),
                ) => message.contains("too large"),
                (
                    Refusal::Batch,
                    ChunkMessageBody::PutResponse(ChunkPutResponse::Error(
                        ProtocolError::PaymentFailed(message),
                    )),
                ) => message.contains(BATCH_PAYMENTS_REFUSED),
                _ => false,
            };
            assert!(refused, "{case}: wrong answer {answer:?}");
            assert_eq!(get(&protocol, address).await, None, "{case}: not stored");
        }

        assert_eq!(
            rpc.connections.load(Ordering::SeqCst),
            0,
            "no refusal reads a payment from the chain"
        );
    }
}

//! Pointer replication across real nodes (ADR-0016).
//!
//! Each test runs a live network whose nodes carry the same pointer service and
//! replication engine a production node does, over real QUIC. Payment is
//! pre-marked in each node's verifier cache — the chain is not what is under
//! test here — but every other check a node makes runs for real: signatures,
//! responsibility, the merge rule, quorum and the capability gate.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::testnet::{TestNetworkConfig, TestNode};
use super::TestHarness;
use ant_node::ant_protocol::chunk::{
    ChunkMessage, ChunkMessageBody, PointerPutRequest, PointerPutResponse, PROOF_TAG_MERKLE,
};
use ant_node::pointer::{FinalStateWitness, PointerStore};
use ant_node::replication::audit::AuditTickResult;
use ant_node::replication::commitment::pointer_leaf_hash;
use ant_node::replication::commitment_state::{BuiltCommitment, ResponderCommitmentState};
use ant_node::replication::config::SUBTREE_AUDIT_PROTOCOL_ID;
use ant_node::replication::pointer::{PointerFreshWrite, PointerReplication};
use ant_node::replication::protocol::{
    ReplicationMessage, ReplicationMessageBody, SubtreeAuditChallenge, SubtreeAuditResponse,
    SubtreeSliceChallenge, SubtreeSliceItem, SubtreeSliceOpening, SubtreeSliceResponse,
};
use ant_node::replication::retention::{DiskRetention, DiskSpace};
use ant_node::replication::slice::nonced_block_root;
use ant_node::storage::XorName;
use ant_node::ReplicationConfig;
use ant_protocol::pointer::{
    Pointer, PointerState, PointerTarget, PointerTargetKind, FINAL_COUNTER,
};
use bytes::Bytes;
use saorsa_core::identity::PeerId;
use saorsa_pqc::api::sig::{ml_dsa_65, MlDsaPublicKey, MlDsaSecretKey};
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;

/// How long to wait for replication to reach a node.
const SETTLE: Duration = Duration::from_secs(30);

/// How often to look while waiting.
const POLL: Duration = Duration::from_millis(200);

/// The id a hand-driven storage audit uses for both of its rounds.
const CHALLENGE_ID: u64 = 0x5EED;

/// A proof the receivers never parse: the state is pre-marked as paid in each
/// verifier's cache, so verification answers from the cache.
const DUMMY_PROOF: [u8; 64] = [0x01; 64];

fn owner() -> (MlDsaPublicKey, MlDsaSecretKey) {
    ml_dsa_65().generate_keypair().expect("keypair")
}

fn signed(pk: &MlDsaPublicKey, sk: &MlDsaSecretKey, counter: u64, target: u8) -> Pointer {
    Pointer::sign(
        sk,
        pk,
        counter,
        PointerTarget::new(PointerTargetKind::Chunk, [target; 32]),
    )
    .expect("sign")
}

/// The final state that hands `pk`'s pointer over to the pointer at
/// `recipient` (ADR-0018).
fn transfer(pk: &MlDsaPublicKey, sk: &MlDsaSecretKey, recipient: u8) -> Pointer {
    Pointer::sign(
        sk,
        pk,
        FINAL_COUNTER,
        PointerTarget::new(PointerTargetKind::Pointer, [recipient; 32]),
    )
    .expect("sign")
}

fn store(node: &TestNode) -> PointerStore {
    node.ant_protocol
        .as_ref()
        .expect("protocol")
        .pointer_service()
        .expect("pointer service")
        .store()
        .clone()
}

fn replication(node: &TestNode) -> &Arc<PointerReplication> {
    node.replication_engine
        .as_ref()
        .expect("engine")
        .pointer_replication()
        .expect("pointer replication")
}

fn commitments(node: &TestNode) -> &ResponderCommitmentState {
    node.replication_engine
        .as_ref()
        .expect("engine")
        .commitment_state()
}

fn peer(node: &TestNode) -> PeerId {
    *node.p2p_node.as_ref().expect("p2p").peer_id()
}

fn held(node: &TestNode, record: &Pointer) -> Option<PointerState> {
    store(node).state(&record.address())
}

fn holds(node: &TestNode, record: &Pointer) -> bool {
    held(node, record).is_some_and(|state| state.state_id == record.state_id())
}

/// Mark `record`'s state as paid on every node, as a settled payment would.
fn mark_paid(harness: &TestHarness, record: &Pointer) {
    for i in 0..harness.node_count() {
        if let Some(protocol) = harness.test_node(i).and_then(|n| n.ant_protocol.as_ref()) {
            protocol
                .payment_verifier()
                .cache_insert_pointer(record.address(), record.state_id());
        }
    }
}

/// Every node's index, except `except`.
fn others(harness: &TestHarness, except: &[usize]) -> Vec<usize> {
    (0..harness.node_count())
        .filter(|i| !except.contains(i))
        .collect()
}

/// Wait until `node` holds exactly `record`'s state.
async fn wait_for(harness: &TestHarness, index: usize, record: &Pointer) -> bool {
    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        if harness
            .test_node(index)
            .is_some_and(|node| holds(node, record))
        {
            return true;
        }
        tokio::time::sleep(POLL).await;
    }
    false
}

/// A single-node proof the storer's slim builder forwards for `record`: one
/// quote on the pricing curve, at the baseline count, naming the record's
/// state. Its signature is a stand-in and it pays nothing on chain: every
/// node accepts it from the payment cache, which `mark_paid` fills. A proof
/// the builder declines would send no pointer offer.
fn forwardable_proof(record: &Pointer) -> Vec<u8> {
    use ant_node::payment::{calculate_price, serialize_single_node_proof, PaymentProof};
    use evmlib::{EncodedPeerId, PaymentQuote, ProofOfPayment, RewardsAddress};

    let quote = PaymentQuote {
        content: xor_name::XorName(record.state_id()),
        timestamp: std::time::SystemTime::now(),
        price: calculate_price(0),
        rewards_address: RewardsAddress::new([0x5A; 20]),
        committed_key_count: 0,
        commitment_pin: None,
        pub_key: vec![0x5B; 64],
        signature: vec![0x5C; 64],
    };
    serialize_single_node_proof(&PaymentProof {
        proof_of_payment: ProofOfPayment {
            peer_quotes: vec![(EncodedPeerId::new([0x5D; 32]), quote)],
        },
        tx_hashes: vec![],
        commitment_sidecars: vec![],
    })
    .expect("serialize the proof")
}

/// Send a paid pointer PUT to one node through its request handler, as a
/// client's PUT arrives, and return its answer.
async fn put(node: &TestNode, record: &Pointer) -> PointerPutResponse {
    let message = ChunkMessage {
        request_id: 1,
        body: ChunkMessageBody::PointerPutRequest(PointerPutRequest::with_payment(
            Bytes::from(record.to_bytes()),
            forwardable_proof(record),
        )),
    };
    let reply = node
        .ant_protocol
        .as_ref()
        .expect("protocol")
        .try_handle_request(&message.encode().expect("encode"))
        .await
        .expect("handle")
        .expect("answered");
    match ChunkMessage::decode(&reply).expect("decode").body {
        ChunkMessageBody::PointerPutResponse(response) => response,
        other => panic!("expected a pointer PUT response, got {other:?}"),
    }
}

/// Have every node in `from` push its hints to every node in `to`, so each
/// learns the others understand pointers and hears what they hold.
async fn exchange_hints(harness: &TestHarness, from: &[usize], to: &[usize]) {
    let targets: Vec<PeerId> = to
        .iter()
        .filter_map(|i| harness.test_node(*i).map(peer))
        .collect();
    for i in from {
        let node = harness.test_node(*i).expect("node");
        let own = peer(node);
        let peers: Vec<PeerId> = targets.iter().copied().filter(|p| *p != own).collect();
        replication(node).push_hints(&peers).await;
    }
    // One-way messages: give them a moment to land.
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// A client's paid PUT reaches one node, and replication carries it to every
/// other member of the close group: the node that accepted it offers it on,
/// with the proof, and each receiver verifies and stores it.
#[tokio::test]
#[serial]
async fn a_paid_put_to_one_node_reaches_its_whole_close_group() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let record = signed(&pk, &sk, 0, 1);
    mark_paid(&harness, &record);

    let entry = 3;
    match put(harness.test_node(entry).expect("node"), &record).await {
        PointerPutResponse::Success { state_id, .. } => assert_eq!(state_id, record.state_id()),
        other => panic!("the entry node refused the PUT: {other:?}"),
    }

    // In a five-node network every node is in every close group.
    for i in others(&harness, &[entry]) {
        assert!(
            wait_for(&harness, i, &record).await,
            "node {i} never received the pointer"
        );
    }

    harness.teardown().await.expect("teardown");
}

/// An update written to one node replaces the old state on every node.
#[tokio::test]
#[serial]
async fn an_update_replicates_and_replaces_the_old_state_everywhere() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let created = signed(&pk, &sk, 0, 1);
    let updated = signed(&pk, &sk, 4, 2);
    mark_paid(&harness, &created);
    mark_paid(&harness, &updated);

    put(harness.test_node(2).expect("node"), &created).await;
    for i in 0..harness.node_count() {
        assert!(
            wait_for(&harness, i, &created).await,
            "node {i} lacks the create"
        );
    }
    put(harness.test_node(4).expect("node"), &updated).await;
    for i in 0..harness.node_count() {
        assert!(
            wait_for(&harness, i, &updated).await,
            "node {i} still holds the old state"
        );
    }

    harness.teardown().await.expect("teardown");
}

/// A node that missed an update is brought level by neighbour sync: the
/// holders hint the newer state, it asks the group which state each holds,
/// adopts the one a quorum hold, and fetches it from one of them.
#[tokio::test]
#[serial]
async fn a_node_that_missed_an_update_is_repaired_by_neighbour_sync() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let created = signed(&pk, &sk, 0, 1);
    let missed = signed(&pk, &sk, 3, 2);
    let lagging = 4;
    let current = others(&harness, &[lagging]);

    // Everyone holds the create; everyone but the laggard holds the update,
    // written straight into their stores so nothing replicates it.
    for i in 0..harness.node_count() {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&created.to_bytes())
            .await
            .expect("put");
    }
    for i in &current {
        store(harness.test_node(*i).expect("node"))
            .put_bytes(&missed.to_bytes())
            .await
            .expect("put");
    }
    assert!(holds(harness.test_node(lagging).expect("node"), &created));

    // Nothing but hints reaches the laggard: no fresh write carries the
    // update, so holding it afterwards means it was repaired. The engine's
    // own verification loop runs too; driving a round here only saves waiting.
    exchange_hints(&harness, &current, &[lagging]).await;
    replication(harness.test_node(lagging).expect("node"))
        .verify_due()
        .await;

    assert!(
        wait_for(&harness, lagging, &missed).await,
        "the lagging node never caught up"
    );

    harness.teardown().await.expect("teardown");
}

/// A node that joins after a pointer was written obtains it, with nothing but
/// the engine's own loops: its bootstrap sync reaches its neighbours, they
/// push their hints back, and it verifies and fetches.
#[tokio::test]
#[serial]
async fn a_node_that_joins_later_obtains_the_existing_pointers() {
    let mut harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let record = signed(&pk, &sk, 2, 1);
    for i in 0..harness.node_count() {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&record.to_bytes())
            .await
            .expect("put");
    }

    let joined = harness.add_node().await.expect("add node");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut obtained = false;
    while tokio::time::Instant::now() < deadline {
        if harness
            .test_node(joined)
            .is_some_and(|node| holds(node, &record))
        {
            obtained = true;
            break;
        }
        // Stand in for the periodic timer, which runs every ten minutes.
        for i in 0..harness.node_count() {
            if let Some(engine) = harness
                .test_node(i)
                .and_then(|n| n.replication_engine.as_ref())
            {
                engine.trigger_neighbor_sync();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(obtained, "the joining node never obtained the pointer");

    harness.teardown().await.expect("teardown");
}

/// A newer state that only one node holds is not adopted by the rest, however
/// well signed: one peer's word is not the network's. It is exactly what an
/// owner who got one node to store an unpaid state would try.
#[tokio::test]
#[serial]
async fn a_state_only_one_node_holds_is_not_adopted() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let agreed = signed(&pk, &sk, 1, 1);
    let lone = signed(&pk, &sk, 9, 9);
    let lone_holder = 0;
    for i in 0..harness.node_count() {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&agreed.to_bytes())
            .await
            .expect("put");
    }
    store(harness.test_node(lone_holder).expect("node"))
        .put_bytes(&lone.to_bytes())
        .await
        .expect("put");

    let rest = others(&harness, &[lone_holder]);
    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    exchange_hints(&harness, &everyone, &everyone).await;
    for i in &rest {
        replication(harness.test_node(*i).expect("node"))
            .verify_due()
            .await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let lone_peer = peer(harness.test_node(lone_holder).expect("node"));
    for i in &rest {
        let node = harness.test_node(*i).expect("node");
        // The lone holder's hints did arrive, so what follows is the quorum
        // refusing, not a hint that never came.
        assert!(
            replication(node).is_capable(&lone_peer),
            "node {i} never heard from the lone holder"
        );
        assert!(
            holds(node, &agreed),
            "node {i} adopted a state only one node holds"
        );
    }

    harness.teardown().await.expect("teardown");
}

/// A fresh offer whose state was never paid for is refused by every receiver.
#[tokio::test]
#[serial]
async fn a_fresh_offer_that_was_not_paid_for_is_refused() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let unpaid = signed(&pk, &sk, 0, 1);
    let source = 3;
    let node = harness.test_node(source).expect("node");
    store(node)
        .put_bytes(&unpaid.to_bytes())
        .await
        .expect("put");
    replication(node)
        .replicate_fresh(PointerFreshWrite {
            record: unpaid.to_bytes(),
            payment_proof: DUMMY_PROOF.to_vec(),
        })
        .await;
    tokio::time::sleep(Duration::from_secs(5)).await;

    for i in others(&harness, &[source]) {
        assert!(
            held(harness.test_node(i).expect("node"), &unpaid).is_none(),
            "node {i} stored an unpaid pointer"
        );
    }

    harness.teardown().await.expect("teardown");
}

/// Some minutes after offering a fresh state, the offering node asks each
/// close-group member for it. A member that cannot produce it is penalised,
/// and one that can is not.
#[tokio::test]
#[serial]
async fn the_possession_check_penalises_only_a_member_that_dropped_the_record() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let record = signed(&pk, &sk, 0, 1);
    let checker = 3;
    let dropper = 1;
    let keeper = 2;
    for i in 0..harness.node_count() {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&record.to_bytes())
            .await
            .expect("put");
    }
    store(harness.test_node(dropper).expect("node"))
        .delete(&record.address())
        .await
        .expect("delete");

    // The checker only asks peers that understand pointers.
    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    exchange_hints(&harness, &everyone, &[checker]).await;

    let checker_node = harness.test_node(checker).expect("node");
    let checker_p2p = checker_node.p2p_node.as_ref().expect("p2p");
    let dropper_peer = peer(harness.test_node(dropper).expect("node"));
    let keeper_peer = peer(harness.test_node(keeper).expect("node"));
    let dropper_before = checker_p2p.peer_trust(&dropper_peer);
    let keeper_before = checker_p2p.peer_trust(&keeper_peer);

    replication(checker_node)
        .check_possession(record.state(), &[dropper_peer, keeper_peer], None)
        .await;

    assert!(
        checker_p2p.peer_trust(&dropper_peer) < dropper_before,
        "the member that dropped the record was not penalised"
    );
    assert!(
        checker_p2p.peer_trust(&keeper_peer) >= keeper_before,
        "the member that holds the record was penalised"
    );

    harness.teardown().await.expect("teardown");
}

/// Full, not faulty: a new pointer written near a full node (one whose "full"
/// notice is fresh, and that refused the offer, so it holds no state of the
/// pointer) costs that node nothing in the possession check after the write,
/// when it wasn't paid for the write. A member that isn't full and lacks the
/// pointer is still penalised, and so is the full one if it was paid.
#[tokio::test]
#[serial]
async fn the_pointer_possession_check_does_not_penalise_a_full_member_for_a_new_pointer() {
    use ant_node::replication::full;

    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let record = signed(&pk, &sk, 0, 1);
    let checker = 3;
    let full_member = 1;
    let lacking = 2;
    for i in others(&harness, &[full_member, lacking]) {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&record.to_bytes())
            .await
            .expect("put");
    }

    // The checker only asks peers that understand pointers.
    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    exchange_hints(&harness, &everyone, &[checker]).await;

    let checker_node = harness.test_node(checker).expect("node");
    let checker_p2p = checker_node.p2p_node.as_ref().expect("p2p");
    let full_peer = peer(harness.test_node(full_member).expect("node"));
    let lacking_peer = peer(harness.test_node(lacking).expect("node"));
    full::view_for(&peer(checker_node)).marks.note_notice(
        full_peer,
        true,
        std::time::Instant::now(),
    );
    let full_before = checker_p2p.peer_trust(&full_peer);
    let lacking_before = checker_p2p.peer_trust(&lacking_peer);

    // Neither was among the write's paid quotes.
    let paid: Vec<PeerId> = Vec::new();
    replication(checker_node)
        .check_possession(record.state(), &[full_peer, lacking_peer], Some(&paid))
        .await;
    assert!(
        checker_p2p.peer_trust(&full_peer) >= full_before,
        "the full member was penalised for a new pointer it couldn't take"
    );
    assert!(
        checker_p2p.peer_trust(&lacking_peer) < lacking_before,
        "the member that isn't full and lacks the pointer was not penalised"
    );

    // Paid for the write, the full member is charged like any other.
    let full_before = checker_p2p.peer_trust(&full_peer);
    replication(checker_node)
        .check_possession(record.state(), &[full_peer], Some(&[full_peer]))
        .await;
    assert!(
        checker_p2p.peer_trust(&full_peer) < full_before,
        "the full member paid for the write was not penalised"
    );

    harness.teardown().await.expect("teardown");
}

/// A transfer written to one node reaches the whole group, and after that no
/// node takes a second final state — not even one whose target sorts first,
/// which the previous merge rule let displace the first everywhere (ADR-0018).
#[tokio::test]
#[serial]
async fn a_transfer_reaches_the_group_and_no_node_takes_a_second_one() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let created = signed(&pk, &sk, 0, 1);
    let handed_over = transfer(&pk, &sk, 0x77);
    let take_back = transfer(&pk, &sk, 0x01);
    assert!(take_back.target().to_bytes() < handed_over.target().to_bytes());
    for record in [&created, &handed_over, &take_back] {
        mark_paid(&harness, record);
    }

    put(harness.test_node(1).expect("node"), &created).await;
    for i in 0..harness.node_count() {
        assert!(
            wait_for(&harness, i, &created).await,
            "node {i} lacks the create"
        );
    }
    match put(harness.test_node(2).expect("node"), &handed_over).await {
        PointerPutResponse::Success { state_id, .. } => {
            assert_eq!(state_id, handed_over.state_id());
        }
        other => panic!("the transfer was refused: {other:?}"),
    }
    for i in 0..harness.node_count() {
        assert!(
            wait_for(&harness, i, &handed_over).await,
            "node {i} never received the transfer"
        );
    }

    // The former owner, paid up, tries to take it back on every node.
    for i in 0..harness.node_count() {
        match put(harness.test_node(i).expect("node"), &take_back).await {
            PointerPutResponse::Stale { state_id, .. } => assert_eq!(
                state_id,
                handed_over.state_id(),
                "node {i} must name the transfer it holds"
            ),
            other => panic!("node {i} took a second final state: {other:?}"),
        }
        assert!(holds(harness.test_node(i).expect("node"), &handed_over));
    }

    harness.teardown().await.expect("teardown");
}

/// A node that never heard of a transfer — one that joined after it, or lost
/// its copy — would take any final state on the merge rule alone. Before it
/// does, it asks its group, and a peer that serves the transfer it holds is
/// proof enough to refuse the second one.
#[tokio::test]
#[serial]
async fn a_node_that_missed_the_transfer_refuses_another_its_group_proves() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let created = signed(&pk, &sk, 0, 1);
    let handed_over = transfer(&pk, &sk, 0x77);
    let second = transfer(&pk, &sk, 0x01);
    mark_paid(&harness, &second);
    mark_paid(&harness, &handed_over);

    // Everyone holds the create, and learns everyone else understands
    // pointers while there is nothing newer to hint.
    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    for i in &everyone {
        store(harness.test_node(*i).expect("node"))
            .put_bytes(&created.to_bytes())
            .await
            .expect("put");
    }
    exchange_hints(&harness, &everyone, &everyone).await;

    // The transfer lands everywhere but one node, written straight into the
    // stores so nothing replicates or hints it there.
    let unaware = 4;
    for i in others(&harness, &[unaware]) {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&handed_over.to_bytes())
            .await
            .expect("put");
    }
    let node = harness.test_node(unaware).expect("node");
    assert!(
        holds(node, &created),
        "the unaware node has only the create"
    );

    match put(node, &second).await {
        PointerPutResponse::Stale { state_id, .. } => assert_eq!(
            state_id,
            handed_over.state_id(),
            "the refusal names the transfer the group proved"
        ),
        other => panic!("the unaware node took a second final state: {other:?}"),
    }
    assert!(holds(node, &created), "nothing was written");

    // The transfer the group holds is not refused: it is no conflict.
    match put(node, &handed_over).await {
        PointerPutResponse::Success { state_id, .. } => {
            assert_eq!(state_id, handed_over.state_id());
        }
        other => panic!("the group's own transfer was refused: {other:?}"),
    }

    harness.teardown().await.expect("teardown");
}

/// A fresh offer of a final state is paid for before its receiver asks the
/// close group about it, as a client's PUT is. An offer paid with a merkle
/// proof, or not paid at all, is refused without a look: the receiver proves
/// nothing about the rival its group holds. Paid, the same offer is looked
/// for, and the rival is proven.
#[tokio::test]
#[serial]
async fn a_final_state_offer_is_paid_for_before_the_group_is_asked() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let created = signed(&pk, &sk, 0, 1);
    let handed_over = transfer(&pk, &sk, 0x77);
    let second = transfer(&pk, &sk, 0x01);

    // Everyone holds the create, and learns everyone else understands
    // pointers while there is nothing newer to hint.
    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    for i in &everyone {
        store(harness.test_node(*i).expect("node"))
            .put_bytes(&created.to_bytes())
            .await
            .expect("put");
    }
    exchange_hints(&harness, &everyone, &everyone).await;

    // The rival lands everywhere but one node, written straight into the
    // stores so nothing replicates or hints it there.
    let unaware = 4;
    let source = 3;
    for i in others(&harness, &[unaware]) {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&handed_over.to_bytes())
            .await
            .expect("put");
    }
    let offer = |payment_proof: Vec<u8>| PointerFreshWrite {
        record: second.to_bytes(),
        payment_proof,
    };

    let mut merkle_proof = vec![PROOF_TAG_MERKLE];
    merkle_proof.resize(DUMMY_PROOF.len(), 0);
    for proof in [merkle_proof, DUMMY_PROOF.to_vec()] {
        replication(harness.test_node(source).expect("node"))
            .replicate_fresh(offer(proof))
            .await;
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let node = harness.test_node(unaware).expect("node");
    assert!(
        holds(node, &created),
        "an offer paid with a merkle proof, or not paid, was stored"
    );
    assert_eq!(
        replication(node).proven_conflict(&second.state()),
        None,
        "the group was asked about an offer before its payment was checked"
    );

    // Control: paid, the same offer gets as far as the look, which proves
    // the rival and refuses it.
    mark_paid(&harness, &second);
    replication(harness.test_node(source).expect("node"))
        .replicate_fresh(offer(DUMMY_PROOF.to_vec()))
        .await;
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut proven = None;
    while tokio::time::Instant::now() < deadline {
        proven = replication(node).proven_conflict(&second.state());
        if proven.is_some() {
            break;
        }
        tokio::time::sleep(POLL).await;
    }
    assert_eq!(
        proven.map(|state| state.state_id),
        Some(handed_over.state_id()),
        "the paid offer never reached the look"
    );
    assert!(holds(node, &created), "the proven loser was stored");

    harness.teardown().await.expect("teardown");
}

/// Two different final states are a fork only the owner can make, by racing
/// them. A member holding the other side took what reached it first, as the
/// merge rule says; the possession check does not penalise it for the owner's
/// fork, while it still penalises a member that holds nothing.
#[tokio::test]
#[serial]
async fn the_possession_check_does_not_penalise_the_other_side_of_a_fork() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let one_side = transfer(&pk, &sk, 0x77);
    let other_side = transfer(&pk, &sk, 0x01);
    let checker = 3;
    let forked = 1;
    let dropper = 2;
    for i in others(&harness, &[forked, dropper]) {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&one_side.to_bytes())
            .await
            .expect("put");
    }
    store(harness.test_node(forked).expect("node"))
        .put_bytes(&other_side.to_bytes())
        .await
        .expect("put");

    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    exchange_hints(&harness, &everyone, &[checker]).await;

    let checker_node = harness.test_node(checker).expect("node");
    let checker_p2p = checker_node.p2p_node.as_ref().expect("p2p");
    let forked_peer = peer(harness.test_node(forked).expect("node"));
    let dropper_peer = peer(harness.test_node(dropper).expect("node"));
    let forked_before = checker_p2p.peer_trust(&forked_peer);
    let dropper_before = checker_p2p.peer_trust(&dropper_peer);

    replication(checker_node)
        .check_possession(one_side.state(), &[forked_peer, dropper_peer], None)
        .await;

    assert!(
        checker_p2p.peer_trust(&forked_peer) >= forked_before,
        "the member holding the other final state was penalised"
    );
    assert!(
        checker_p2p.peer_trust(&dropper_peer) < dropper_before,
        "the member holding nothing was not penalised"
    );

    harness.teardown().await.expect("teardown");
}

/// A network whose close group is two nodes, so that in five nodes some node
/// is always outside the retention width (two plus the margin of two) for any
/// address, with no pruning hysteresis.
fn prune_network(paid_width: usize) -> TestNetworkConfig {
    TestNetworkConfig {
        replication_config: Some(ReplicationConfig {
            close_group_size: 2,
            // The quorum may not exceed the group.
            quorum_threshold: 2,
            paid_list_close_group_size: paid_width,
            prune_hysteresis_duration: Duration::ZERO,
            ..ReplicationConfig::default()
        }),
        ..TestNetworkConfig::minimal()
    }
}

/// Disk-pressure state for an explicit prune pass, fixed so the test does not
/// depend on the host's disk: under pressure (5% free) or not (50% free), with
/// every out-of-range address beyond the erase floor. The floor is fixed
/// because in a harness this small ten times the distance to a node's
/// `close_group_size`-th nearest peer is usually past the end of the key
/// space, which would leave nothing eligible.
fn disk_state(under_pressure: bool) -> DiskRetention {
    disk_state_with_floor(under_pressure, Some([0u8; 32]))
}

/// As [`disk_state`], with the erase floor at `floor`.
fn disk_state_with_floor(under_pressure: bool, floor: Option<XorName>) -> DiskRetention {
    let retention = DiskRetention::new(std::env::temp_dir());
    let available = if under_pressure { 5 } else { 50 };
    retention.set_space_for_test(Some(DiskSpace::bytes(available, 100)));
    retention.set_floor_for_test(floor);
    retention
}

/// One less than `distance`, which is not zero.
fn one_less(mut distance: XorName) -> XorName {
    for byte in distance.iter_mut().rev() {
        if *byte > 0 {
            *byte -= 1;
            break;
        }
        *byte = u8::MAX;
    }
    distance
}

/// The node, out of five, that is outside the retention width for `record`.
async fn out_of_range_node(harness: &TestHarness, record: &Pointer) -> usize {
    let view = harness.test_node(0).expect("node");
    let p2p = view.p2p_node.as_ref().expect("p2p");
    let inside: Vec<PeerId> = p2p
        .dht_manager()
        .find_closest_nodes_local_with_self(&record.address(), 4)
        .await
        .into_iter()
        .map(|node| node.peer_id)
        .collect();
    (0..harness.node_count())
        .find(|i| {
            harness
                .test_node(*i)
                .is_some_and(|node| !inside.contains(&peer(node)))
        })
        .expect("one node is outside a width of four in five")
}

/// A node keeps a record it is no longer responsible for while its disk has
/// room, even when the close group holds it. Under disk pressure it erases the
/// record once the current close group prove they hold it, and keeps it while
/// they do not.
#[tokio::test]
#[serial]
async fn pruning_deletes_only_once_the_close_group_proves_it_holds_the_record() {
    let harness = TestHarness::setup_with_config(prune_network(20))
        .await
        .expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let record = signed(&pk, &sk, 1, 1);
    let pruner = out_of_range_node(&harness, &record).await;
    store(harness.test_node(pruner).expect("node"))
        .put_bytes(&record.to_bytes())
        .await
        .expect("put");
    let pressured = disk_state(true);

    // Nobody else holds it yet: nothing proves it is safe to drop.
    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    exchange_hints(&harness, &everyone, &[pruner]).await;
    let pruner_node = harness.test_node(pruner).expect("node");
    let pruning = replication(pruner_node);
    let unproven = pruning
        .prune_pass(true, Some(commitments(pruner_node)), &pressured)
        .await;
    assert!(unproven.disk_pressure);
    assert_eq!(unproven.pruned, 0);
    assert!(
        holds(harness.test_node(pruner).expect("node"), &record),
        "the only copy was pruned"
    );

    // The rest hold it now. While the disk has room, the record is kept.
    for i in others(&harness, &[pruner]) {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&record.to_bytes())
            .await
            .expect("put");
    }
    let roomy = pruning
        .prune_pass(true, Some(commitments(pruner_node)), &disk_state(false))
        .await;
    assert!(!roomy.disk_pressure);
    assert_eq!(roomy.candidates, 1);
    assert_eq!(roomy.beyond_floor, 1);
    assert_eq!(roomy.pruned, 0);
    assert!(
        holds(harness.test_node(pruner).expect("node"), &record),
        "a node with room erased a record it keeps"
    );

    // While bootstrapping it asks no one, so under disk pressure too it keeps
    // the record.
    let bootstrapping = pruning
        .prune_pass(false, Some(commitments(pruner_node)), &pressured)
        .await;
    assert!(bootstrapping.disk_pressure);
    assert_eq!(bootstrapping.pruned, 0);
    assert!(holds(harness.test_node(pruner).expect("node"), &record));

    // Under disk pressure it keeps the record with no floor, and at exactly
    // the floor: only records strictly farther may go.
    let distance = ant_node::xor_distance(peer(pruner_node).as_bytes(), &record.address());
    for floor in [None, Some(distance)] {
        let floored = pruning
            .prune_pass(
                true,
                Some(commitments(pruner_node)),
                &disk_state_with_floor(true, floor),
            )
            .await;
        assert_eq!(floored.beyond_floor, 0, "floor {floor:?}");
        assert_eq!(floored.pruned, 0, "floor {floor:?}");
        assert!(holds(harness.test_node(pruner).expect("node"), &record));
    }

    // Beyond the floor and under disk pressure, the close group proves it and
    // the pruner drops it.
    let erased = pruning
        .prune_pass(
            true,
            Some(commitments(pruner_node)),
            &disk_state_with_floor(true, Some(one_less(distance))),
        )
        .await;
    assert_eq!(erased.beyond_floor, 1);
    assert_eq!(erased.pruned, 1);
    assert!(
        held(harness.test_node(pruner).expect("node"), &record).is_none(),
        "the record was not pruned though the close group holds it"
    );

    harness.teardown().await.expect("teardown");
}

/// Under disk pressure the pointer pass erases the eligible record farthest
/// from the node first, measures free space again before the next one, and
/// stops once 15% is free, keeping the rest though their close groups hold
/// them.
#[tokio::test]
#[serial]
async fn pointer_erasure_goes_farthest_first_and_stops_once_fifteen_percent_is_free() {
    let harness = TestHarness::setup_with_config(prune_network(20))
        .await
        .expect("setup");
    harness.warmup_dht().await.expect("warmup");

    // Two records the same node is out of range of.
    let (pk, sk) = owner();
    let first = signed(&pk, &sk, 1, 1);
    let pruner = out_of_range_node(&harness, &first).await;
    let mut second = None;
    for _ in 0..500 {
        let (pk, sk) = owner();
        let candidate = signed(&pk, &sk, 1, 1);
        if out_of_range_node(&harness, &candidate).await == pruner {
            second = Some(candidate);
            break;
        }
    }
    let second = second.expect("a second record the same node is out of range of");
    for i in 0..harness.node_count() {
        for record in [&first, &second] {
            store(harness.test_node(i).expect("node"))
                .put_bytes(&record.to_bytes())
                .await
                .expect("put");
        }
    }
    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    exchange_hints(&harness, &everyone, &[pruner]).await;

    let pruner_node = harness.test_node(pruner).expect("node");
    let distance =
        |record: &Pointer| ant_node::xor_distance(peer(pruner_node).as_bytes(), &record.address());
    let (farther, nearer) = if distance(&first) > distance(&second) {
        (first, second)
    } else {
        (second, first)
    };

    // 9% free while the farther record is held; erasing it frees 6%.
    let disk = DiskRetention::new(std::env::temp_dir());
    disk.set_floor_for_test(Some([0u8; 32]));
    let pruner_store = store(pruner_node);
    let farther_address = farther.address();
    disk.set_space_source_for_test(move || {
        let available = if pruner_store.state(&farther_address).is_some() {
            9
        } else {
            15
        };
        Some(DiskSpace::bytes(available, 100))
    });

    let result = replication(pruner_node)
        .prune_pass(true, Some(commitments(pruner_node)), &disk)
        .await;
    assert!(result.disk_pressure);
    assert_eq!(result.candidates, 2);
    assert_eq!(result.beyond_floor, 2);
    assert_eq!(result.pruned, 1, "erasing must stop at 15% free");
    assert!(
        held(pruner_node, &farther).is_none(),
        "the farther record goes first"
    );
    assert!(holds(pruner_node, &nearer), "the nearer one is kept");
    assert!(!disk.under_pressure());

    harness.teardown().await.expect("teardown");
}

/// A node outside even a complete paid-width group still needs the close
/// group's proof: it never drops a record without asking, under disk pressure
/// or not, and while bootstrapping it asks nobody, so it keeps the record.
#[tokio::test]
#[serial]
async fn a_node_far_outside_the_group_keeps_the_record_without_the_groups_proof() {
    let harness = TestHarness::setup_with_config(prune_network(3))
        .await
        .expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let record = signed(&pk, &sk, 1, 1);
    let pruner = out_of_range_node(&harness, &record).await;
    store(harness.test_node(pruner).expect("node"))
        .put_bytes(&record.to_bytes())
        .await
        .expect("put");

    let pruner_node = harness.test_node(pruner).expect("node");
    let pressured = disk_state(true);
    let bootstrapping = replication(pruner_node)
        .prune_pass(false, Some(commitments(pruner_node)), &pressured)
        .await;
    assert_eq!(bootstrapping.pruned, 0);
    assert!(
        holds(pruner_node, &record),
        "a far-away record was dropped without the close group's proof"
    );

    // Allowed to ask, but nobody else holds it: still kept.
    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    exchange_hints(&harness, &everyone, &[pruner]).await;
    let unproven = replication(pruner_node)
        .prune_pass(true, Some(commitments(pruner_node)), &pressured)
        .await;
    assert_eq!(unproven.pruned, 0);
    assert!(
        holds(pruner_node, &record),
        "a far-away record was dropped though no one else holds it"
    );

    harness.teardown().await.expect("teardown");
}

/// A pointer a retained storage commitment still holds is never pruned, as a
/// chunk is not, even under disk pressure with the close group holding it: a
/// peer pinning that commitment may yet audit it, and a node that had deleted
/// it would fail. Once no retained commitment holds it, it goes.
#[tokio::test]
#[serial]
async fn pruning_keeps_a_pointer_a_retained_commitment_still_holds() {
    let harness = TestHarness::setup_with_config(prune_network(3))
        .await
        .expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let record = signed(&pk, &sk, 1, 1);
    let pruner = out_of_range_node(&harness, &record).await;
    let pruner_node = harness.test_node(pruner).expect("node");
    store(pruner_node)
        .put_bytes(&record.to_bytes())
        .await
        .expect("put");
    for i in others(&harness, &[pruner]) {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&record.to_bytes())
            .await
            .expect("put");
    }
    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    exchange_hints(&harness, &everyone, &[pruner]).await;
    let pressured = disk_state(true);

    // The commitment the node gossiped while it was still responsible.
    let (node_pk, node_sk) = owner();
    let committed = BuiltCommitment::build(
        vec![(record.address(), pointer_leaf_hash(&record.address()))],
        peer(pruner_node).as_bytes(),
        &node_sk,
        &node_pk.to_bytes(),
    )
    .expect("commitment");
    let state = commitments(pruner_node);
    state.rotate(committed);

    let vetoed = replication(pruner_node)
        .prune_pass(true, Some(state), &pressured)
        .await;
    assert_eq!(vetoed.held_by_commitment, 1);
    assert_eq!(vetoed.pruned, 0);
    assert!(
        holds(pruner_node, &record),
        "a pointer a retained commitment holds was pruned"
    );

    state.clear_all();
    let erased = replication(pruner_node)
        .prune_pass(true, Some(state), &pressured)
        .await;
    assert_eq!(erased.pruned, 1);
    assert!(
        held(pruner_node, &record).is_none(),
        "the pointer was kept after no commitment held it"
    );

    harness.teardown().await.expect("teardown");
}

/// Store `count` pointers on node `holder`, have it commit to them, and hand
/// that commitment to node `auditor`, as its gossip would. Returns what the
/// holder committed to.
async fn commit_pointers(
    harness: &TestHarness,
    holder: usize,
    auditor: usize,
    count: usize,
) -> Vec<Pointer> {
    let records: Vec<Pointer> = (0..count)
        .map(|_| {
            let (pk, sk) = owner();
            signed(&pk, &sk, 1, 1)
        })
        .collect();
    commit_records(harness, holder, auditor, records).await
}

/// [`commit_pointers`] over records the caller signed.
async fn commit_records(
    harness: &TestHarness,
    holder: usize,
    auditor: usize,
    records: Vec<Pointer>,
) -> Vec<Pointer> {
    let holder_node = harness.test_node(holder).expect("holder");
    for record in &records {
        store(holder_node)
            .put_bytes(&record.to_bytes())
            .await
            .expect("put");
    }

    let engine = holder_node.replication_engine.as_ref().expect("engine");
    engine.rebuild_commitment_now().await.expect("rebuild");
    let committed = engine
        .commitment_state()
        .current()
        .expect("a current commitment");
    let pointers = committed.pointer_leaf_keys();
    assert!(
        !pointers.is_empty(),
        "the holder committed to none of the pointers it is responsible for"
    );
    assert_eq!(
        committed.leaf_keys(),
        pointers,
        "the holder has no chunks, so every leaf audited is a pointer"
    );

    harness
        .test_node(auditor)
        .expect("auditor")
        .replication_engine
        .as_ref()
        .expect("engine")
        .inject_peer_commitment_for_test(&peer(holder_node), committed.commitment().clone())
        .await;
    records
        .into_iter()
        .filter(|record| pointers.contains(&record.address()))
        .collect()
}

/// A node holding the pointers it committed to passes the storage audit over
/// the wire, proving each opened one with its signed record.
#[tokio::test]
#[serial]
async fn a_node_holding_its_committed_pointers_passes_the_storage_audit() {
    let harness = TestHarness::setup_small().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    let (holder, auditor) = (3, 4);
    commit_pointers(&harness, holder, auditor, 48).await;

    let holder_peer = peer(harness.test_node(holder).expect("holder"));
    let result = harness
        .test_node(auditor)
        .expect("auditor")
        .replication_engine
        .as_ref()
        .expect("engine")
        .audit_peer_now(&holder_peer)
        .await;
    assert!(
        matches!(result, AuditTickResult::Passed { keys_checked, .. } if keys_checked >= 1),
        "an honest pointer holder must pass, got {result:?}"
    );

    harness.teardown().await.expect("teardown");
}

/// Several paid updates between the two rounds of a storage audit do not fail
/// the node holding the pointer: round 2 serves the record round 1 read, found
/// by the root round 1 reported over it (ADR-0019). Driven one round at a time
/// against the holder's live engine, so it is the round-1 session that carries
/// what round 1 bound across to round 2.
#[tokio::test]
#[serial]
async fn round_two_serves_the_record_round_one_read_across_several_updates() {
    let harness = TestHarness::setup_small().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    let (holder, auditor) = (7, 8);
    let owners: Vec<(MlDsaPublicKey, MlDsaSecretKey)> = (0..24).map(|_| owner()).collect();
    let records = owners.iter().map(|(pk, sk)| signed(pk, sk, 1, 1)).collect();
    commit_records(&harness, holder, auditor, records).await;

    let holder_node = harness.test_node(holder).expect("holder");
    let holder_peer = peer(holder_node);
    let committed = commitments(holder_node)
        .current()
        .expect("a current commitment");
    let pointer_keys = committed.pointer_leaf_keys();
    let auditor_p2p = harness
        .test_node(auditor)
        .expect("auditor")
        .p2p_node
        .as_ref()
        .expect("p2p")
        .clone();
    let ask = |body: ReplicationMessageBody| {
        let auditor_p2p = Arc::clone(&auditor_p2p);
        async move {
            let request = ReplicationMessage {
                request_id: CHALLENGE_ID,
                body,
            }
            .encode()
            .expect("encode");
            let response = auditor_p2p
                .send_request(
                    &holder_peer,
                    SUBTREE_AUDIT_PROTOCOL_ID,
                    request,
                    Duration::from_secs(60),
                )
                .await
                .expect("a response");
            ReplicationMessage::decode_subtree_audit_response(&response.data)
                .expect("decode")
                .body
        }
    };

    // Round 1: the holder binds the record it holds for each pointer.
    let nonce = [0x5A; 32];
    let round1 = ask(ReplicationMessageBody::SubtreeAuditChallenge(
        SubtreeAuditChallenge {
            challenge_id: CHALLENGE_ID,
            nonce,
            challenged_peer_id: *holder_peer.as_bytes(),
            expected_commitment_hash: committed.hash(),
        },
    ))
    .await;
    let ReplicationMessageBody::SubtreeAuditResponse(SubtreeAuditResponse::Proof { proof, .. }) =
        round1
    else {
        panic!("expected a round-1 proof, got {round1:?}");
    };
    let opened: Vec<_> = proof
        .leaves
        .iter()
        .filter(|leaf| pointer_keys.contains(&leaf.key))
        .take(5)
        .cloned()
        .collect();
    assert!(!opened.is_empty(), "round 1 proved a pointer");

    // Between the rounds, the owner of every pointer about to be opened
    // updates it three times, each a state the holder accepts.
    let holder_store = store(holder_node);
    for leaf in &opened {
        let (pk, sk) = owners
            .iter()
            .find(|(pk, sk)| signed(pk, sk, 1, 1).address() == leaf.key)
            .expect("an owner for every committed pointer");
        for counter in 2..=4 {
            holder_store
                .put_bytes(&signed(pk, sk, counter, 2).to_bytes())
                .await
                .expect("update");
        }
    }

    // Round 2: each opened pointer is proved by the record round 1 read.
    let round2 = ask(ReplicationMessageBody::SubtreeSliceChallenge(
        SubtreeSliceChallenge {
            challenge_id: CHALLENGE_ID,
            nonce,
            challenged_peer_id: *holder_peer.as_bytes(),
            expected_commitment_hash: committed.hash(),
            openings: opened
                .iter()
                .map(|leaf| SubtreeSliceOpening {
                    key: leaf.key,
                    block_index: 0,
                })
                .collect(),
        },
    ))
    .await;
    let ReplicationMessageBody::SubtreeSliceResponse(SubtreeSliceResponse::Items { items, .. }) =
        round2
    else {
        panic!("expected round-2 items, got {round2:?}");
    };
    for leaf in &opened {
        let served = items
            .iter()
            .find_map(|item| match item {
                SubtreeSliceItem::PointerRecord { key, records } if *key == leaf.key => {
                    Some(records.as_slice())
                }
                _ => None,
            })
            .expect("a record for every opened pointer");
        assert!(
            served.iter().any(|record| {
                nonced_block_root(&nonce, holder_peer.as_bytes(), &leaf.key, record)
                    == leaf.nonced_root
                    && Pointer::from_bytes(record).is_ok_and(|p| p.address() == leaf.key)
            }),
            "round 2 must serve the record round 1 bound, after three updates"
        );
    }

    harness.teardown().await.expect("teardown");
}

/// A node that dropped the pointers it committed to fails the storage audit,
/// exactly as a node that dropped its chunks does.
#[tokio::test]
#[serial]
async fn a_node_that_dropped_its_committed_pointers_fails_the_storage_audit() {
    let harness = TestHarness::setup_small().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    let (holder, auditor) = (5, 6);
    let committed = commit_pointers(&harness, holder, auditor, 48).await;

    let holder_node = harness.test_node(holder).expect("holder");
    for record in &committed {
        assert!(store(holder_node)
            .delete(&record.address())
            .await
            .expect("delete"));
    }

    let result = harness
        .test_node(auditor)
        .expect("auditor")
        .replication_engine
        .as_ref()
        .expect("engine")
        .audit_peer_now(&peer(holder_node))
        .await;
    assert!(
        matches!(result, AuditTickResult::Failed { .. }),
        "a node that dropped its committed pointers must fail, got {result:?}"
    );

    harness.teardown().await.expect("teardown");
}

/// A pointer hint reaches a stand-in for full peers, and the stand-in takes
/// it: senders widen the hint's targets past the peers they mark full, and
/// the receiver admits it across the storage admission width widened the
/// same way, not the base width alone.
///
/// Five nodes and a close group of two, so the admission width is four: the
/// fifth nearest stands in once the three between it and the nearest are
/// marked full. While only the sender marks them, the stand-in drops the
/// hint; once it marks them too, it takes the pointer.
#[tokio::test]
#[serial]
async fn a_stand_in_for_full_peers_takes_a_pointer_hint() {
    use ant_node::replication::full;

    let harness = TestHarness::setup_with_config(prune_network(20))
        .await
        .expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let record = signed(&pk, &sk, 1, 1);
    mark_paid(&harness, &record);
    let view = harness.test_node(0).expect("node");
    let nearest: Vec<PeerId> = view
        .p2p_node
        .as_ref()
        .expect("p2p")
        .dht_manager()
        .find_closest_nodes_local_with_self(&record.address(), 5)
        .await
        .into_iter()
        .map(|node| node.peer_id)
        .collect();
    assert_eq!(nearest.len(), 5);
    let index_of = |peer: &PeerId| {
        (0..harness.node_count())
            .find(|i| {
                harness
                    .test_node(*i)
                    .is_some_and(|node| self::peer(node) == *peer)
            })
            .expect("a node of the harness")
    };
    let sender = index_of(&nearest[0]);
    let stand_in = index_of(&nearest[4]);
    // Held by the four nearest, so the stand-in's check finds it.
    for peer in &nearest[..4] {
        store(harness.test_node(index_of(peer)).expect("node"))
            .put_bytes(&record.to_bytes())
            .await
            .expect("put");
    }
    let mark = |index: usize| {
        let node = harness.test_node(index).expect("node");
        let marks = &full::view_for(&self::peer(node)).marks;
        for peer in &nearest[1..4] {
            marks.mark_for_test(*peer, std::time::Instant::now());
        }
    };
    // Each learns the others understand pointers. Nobody is marked yet, so
    // nobody hints the stand-in the record.
    let everyone: Vec<usize> = (0..harness.node_count()).collect();
    exchange_hints(&harness, &everyone, &everyone).await;
    assert_eq!(
        replication(harness.test_node(stand_in).expect("node")).pending_len(),
        0
    );

    // Only the sender marks them: it hints the stand-in, which drops it.
    mark(sender);
    exchange_hints(&harness, &[sender], &[stand_in]).await;
    let stand_in_node = harness.test_node(stand_in).expect("node");
    assert_eq!(replication(stand_in_node).pending_len(), 0);
    assert!(held(stand_in_node, &record).is_none());

    // The stand-in marks them too: it takes the hint and fetches the record.
    mark(stand_in);
    exchange_hints(&harness, &[sender], &[stand_in]).await;
    assert!(
        replication(stand_in_node).pending_len() > 0 || held(stand_in_node, &record).is_some(),
        "a stand-in for full peers takes the pointer hint"
    );
    replication(stand_in_node).verify_due().await;
    assert!(
        wait_for(&harness, stand_in, &record).await,
        "and stores the record its close group holds"
    );

    harness.teardown().await.expect("teardown");
}

/// A node's answer to a quote request through its own request handler, as a
/// client's quote arrives.
async fn quote_through(
    node: &TestNode,
    address: XorName,
    data_type: u32,
    data_size: u64,
) -> ant_node::ant_protocol::ChunkQuoteResponse {
    use ant_node::ant_protocol::ChunkQuoteRequest;
    let message = ChunkMessage {
        request_id: 7,
        body: ChunkMessageBody::QuoteRequest(ChunkQuoteRequest {
            address,
            data_size,
            data_type,
        }),
    };
    let reply = node
        .ant_protocol
        .as_ref()
        .expect("protocol")
        .try_handle_request(&message.encode().expect("encode"))
        .await
        .expect("handle")
        .expect("answered");
    match ChunkMessage::decode(&reply).expect("decode").body {
        ChunkMessageBody::QuoteResponse(response) => response,
        other => panic!("expected a quote response, got {other:?}"),
    }
}

/// A pointer update near full nodes, over each node's real quote path: with
/// 3 of the 7 nearest full and not holding the pointer, only 4 quote, fewer
/// than the 5 of 7 a pointer write needs, so a client stops before paying
/// (the stop itself is the client's); with the same 3 full but
/// holding it, all 7 quote the update, and it goes through.
#[tokio::test]
#[serial]
async fn a_pointer_update_near_full_nodes_is_quoted_only_by_those_that_can_take_it() {
    use ant_node::ant_protocol::ChunkQuoteResponse;
    use ant_node::pointer::{DATA_TYPE_POINTER, POINTER_WIRE_LEN};
    use ant_node::storage::FullTrigger;

    let harness = TestHarness::setup_small().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let record = signed(&pk, &sk, 1, 1);
    let nearest: Vec<PeerId> = harness
        .test_node(0)
        .expect("node")
        .p2p_node
        .as_ref()
        .expect("p2p")
        .dht_manager()
        .find_closest_nodes_local_with_self(&record.address(), 7)
        .await
        .into_iter()
        .map(|node| node.peer_id)
        .collect();
    assert_eq!(nearest.len(), 7);
    let index_of = |peer: &PeerId| {
        (0..harness.node_count())
            .find(|i| {
                harness
                    .test_node(*i)
                    .is_some_and(|node| self::peer(node) == *peer)
            })
            .expect("a node of the harness")
    };
    let group: Vec<usize> = nearest.iter().map(index_of).collect();
    let chunks = |index: usize| {
        harness
            .test_node(index)
            .expect("node")
            .ant_protocol
            .as_ref()
            .expect("protocol")
            .storage()
    };
    let quoting = |answers: &[ChunkQuoteResponse]| {
        answers
            .iter()
            .filter(|answer| matches!(answer, ChunkQuoteResponse::Success { .. }))
            .count()
    };
    let ask_all = || async {
        let mut answers = Vec::new();
        for index in &group {
            answers.push(
                quote_through(
                    harness.test_node(*index).expect("node"),
                    record.address(),
                    DATA_TYPE_POINTER,
                    POINTER_WIRE_LEN as u64,
                )
                .await,
            );
        }
        answers
    };

    // Held by the four not full; three full nodes that don't hold it.
    for index in &group[3..] {
        store(harness.test_node(*index).expect("node"))
            .put_bytes(&record.to_bytes())
            .await
            .expect("stored");
    }
    for index in &group[..3] {
        chunks(*index).set_full_for_test(Some(FullTrigger::Bytes));
    }
    let answers = ask_all().await;
    assert_eq!(
        quoting(&answers),
        4,
        "only the 4 not full quote: {answers:?}"
    );
    for answer in &answers[..3] {
        assert!(
            matches!(answer, ChunkQuoteResponse::Error(_)),
            "a full node that doesn't hold the pointer prices no new data: {answer:?}"
        );
    }

    // The three full nodes now hold it: they quote its update too.
    for index in &group[..3] {
        store(harness.test_node(*index).expect("node"))
            .put_bytes(&record.to_bytes())
            .await
            .expect("stored");
    }
    let answers = ask_all().await;
    assert_eq!(
        quoting(&answers),
        7,
        "a full holder quotes an update of what it holds: {answers:?}"
    );
    for index in &group[..3] {
        chunks(*index).set_full_for_test(None);
    }

    harness.teardown().await.expect("teardown");
}

/// A full node repairs a pointer it holds and doesn't look for one it
/// doesn't: a newer state of a held pointer takes no new space, a new pointer
/// would (`may_repair_address`). Both hints are queued; the round fetches only
/// the held one, and the other is fetched once the node is no longer full,
/// which shows its hint was sound and only the full gate held it back.
#[tokio::test]
#[serial]
async fn a_full_node_repairs_a_pointer_it_holds_and_fetches_none_it_does_not() {
    use ant_node::storage::FullTrigger;

    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");

    let (pk, sk) = owner();
    let created = signed(&pk, &sk, 0, 1);
    let updated = signed(&pk, &sk, 3, 2);
    let (other_pk, other_sk) = owner();
    let unheld = signed(&other_pk, &other_sk, 0, 3);
    let full_node = 4;
    let current = others(&harness, &[full_node]);

    // Everyone holds the create; everyone but the full node holds the update
    // and a second pointer, written straight into their stores so nothing
    // replicates them.
    for i in 0..harness.node_count() {
        store(harness.test_node(i).expect("node"))
            .put_bytes(&created.to_bytes())
            .await
            .expect("put");
    }
    for i in &current {
        let node_store = store(harness.test_node(*i).expect("node"));
        node_store
            .put_bytes(&updated.to_bytes())
            .await
            .expect("put");
        node_store.put_bytes(&unheld.to_bytes()).await.expect("put");
    }

    let node = harness.test_node(full_node).expect("node");
    let chunks = node.ant_protocol.as_ref().expect("protocol").storage();
    chunks.set_full_for_test(Some(FullTrigger::Bytes));

    // The engine's own verification loop may take the held pointer's hint
    // before this round does; either way only that one is fetched.
    exchange_hints(&harness, &current, &[full_node]).await;
    replication(node).verify_due().await;

    assert!(
        wait_for(&harness, full_node, &updated).await,
        "the full node didn't repair the pointer it holds"
    );
    // Give a fetch of the unheld pointer, were one started, time to land;
    // the engine's own verification loop runs meanwhile too.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        held(node, &unheld).is_none(),
        "the full node fetched a pointer it doesn't hold"
    );
    assert_eq!(
        replication(node).pending_len(),
        1,
        "the unheld pointer's hint was queued and waits"
    );

    // No longer full, it fetches the hint it held back.
    chunks.set_full_for_test(None);
    replication(node).verify_due().await;
    assert!(
        wait_for(&harness, full_node, &unheld).await,
        "the hint held back while full was not sound"
    );

    harness.teardown().await.expect("teardown");
}

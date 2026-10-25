//! Paged neighbour sync between real nodes.
//!
//! A responder builds its hints for a requester once, at the exchange's first
//! page, and serves each page from them; both hint lists run over several
//! pages here through the page-size seam (`ANT_TEST_SEAM_SYNC_PAGE_BYTES`, in
//! `src/test_seams.rs`). A page it can't take is answered busy, never left
//! unanswered, and the busy answer costs it none of the requester's trust. The
//! repair proofs of a page wait for the requester's next page. Whether a node
//! pages to a peer follows the release the peer's user agent names.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::testnet::TestNetworkConfig;
use super::TestHarness;
use ant_node::replication::config::{NEIGHBOR_SYNC_PAGES_FROM_RELEASE, REPLICATION_PROTOCOL_ID};
use ant_node::replication::protocol::{
    NeighborSyncPageRequest, NeighborSyncPageResponse, NeighborSyncRequest, ReplicationMessage,
    ReplicationMessageBody,
};
use ant_node::storage::XorName;
use ant_node::test_seams::{ANNOUNCED_RELEASE, SYNC_PAGES_FROM_RELEASE, SYNC_PAGE_BYTES};
use ant_node::ReplicationConfig;
use saorsa_core::identity::PeerId;
use saorsa_core::P2PNode;
use serial_test::serial;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

/// Two keys a page.
const SMALL_PAGE_BYTES: &str = "64";
/// Keys on a page that size.
const SMALL_PAGE_KEYS: usize = 2;
/// The responder of the tests below, and its requester.
const RESPONDER: usize = 3;
const REQUESTER: usize = 4;
/// How long the tests' own requests wait for an answer.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
/// How often a test page answered busy (the requester's own background sync
/// may hold its slot at the responder for a moment) is asked again.
const BUSY_RETRIES: usize = 20;
const BUSY_RETRY_PAUSE: Duration = Duration::from_millis(250);
/// Well inside the requester's wait: a busy answer is sent at once.
const BUSY_ANSWERED_WITHIN: Duration = Duration::from_secs(5);
/// How far a trust score may move in a test with no event against the peer:
/// its decay toward neutral over a few seconds is far less, and one charged
/// timeout moves it by about 0.06.
const TRUST_TOLERANCE: f64 = 1e-3;
/// How long a test waits for repair proofs recorded in the background.
const PROOFS_WITHIN: Duration = Duration::from_secs(10);

/// Sets a seam for the test, and clears it when the test ends, passing or not.
struct Seam(&'static str);

impl Seam {
    fn set(name: &'static str, value: &str) -> Self {
        std::env::set_var(name, value);
        Self(name)
    }
}

impl Drop for Seam {
    fn drop(&mut self) {
        std::env::remove_var(self.0);
    }
}

async fn ask(
    sender: &P2PNode,
    target: &PeerId,
    body: ReplicationMessageBody,
) -> ReplicationMessageBody {
    let encoded = ReplicationMessage {
        request_id: rand::random(),
        body,
    }
    .encode()
    .expect("encode");
    let response = sender
        .send_request(target, REPLICATION_PROTOCOL_ID, encoded, REQUEST_TIMEOUT)
        .await
        .expect("every request is answered");
    ReplicationMessage::decode(&response.data)
        .expect("decode the answer")
        .body
}

fn page_request(cycle: u64, page: u32) -> ReplicationMessageBody {
    ReplicationMessageBody::NeighborSyncPageRequest(NeighborSyncPageRequest {
        cycle,
        page,
        pages: 1,
        replica_hints: Vec::new(),
        paid_hints: Vec::new(),
        bootstrapping: false,
        commitment: None,
    })
}

/// This build's release: `n` of its `+ds.<n>` version.
fn own_release() -> u64 {
    env!("CARGO_PKG_VERSION")
        .split("+ds.")
        .nth(1)
        .and_then(|release| release.split('.').next())
        .and_then(|release| release.parse().ok())
        .expect("a +ds version")
}

/// Whether `node` holds a repair proof that it hinted `key` to `peer`, waiting
/// up to [`PROOFS_WITHIN`] for proofs recorded in the background when
/// `expected`.
async fn proof_held(
    harness: &TestHarness,
    node: usize,
    peer: &PeerId,
    key: &XorName,
    expected: bool,
) -> bool {
    let engine = harness
        .test_node(node)
        .and_then(|node| node.replication_engine.as_ref())
        .expect("engine");
    let deadline = Instant::now()
        + if expected {
            PROOFS_WITHIN
        } else {
            Duration::ZERO
        };
    loop {
        let held = engine.has_repair_proof_for_test(peer, key).await;
        if held == expected || Instant::now() >= deadline {
            return held;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn as_page_answer(body: ReplicationMessageBody) -> NeighborSyncPageResponse {
    match body {
        ReplicationMessageBody::NeighborSyncPageResponse(answer) => answer,
        other => panic!("a page request answered with {other:?}"),
    }
}

/// Ask for `page`, asking again while it is answered busy.
async fn page_answer(
    sender: &P2PNode,
    target: &PeerId,
    cycle: u64,
    page: u32,
) -> NeighborSyncPageResponse {
    for _ in 0..BUSY_RETRIES {
        let answer = as_page_answer(ask(sender, target, page_request(cycle, page)).await);
        if !matches!(answer, NeighborSyncPageResponse::Busy { .. }) {
            return answer;
        }
        tokio::time::sleep(BUSY_RETRY_PAUSE).await;
    }
    panic!("page {page} answered busy {BUSY_RETRIES} times");
}

fn peer_of(harness: &TestHarness, index: usize) -> PeerId {
    *harness
        .test_node(index)
        .and_then(|node| node.p2p_node.as_ref())
        .expect("node")
        .peer_id()
}

/// Store `count` records on node `index`, and list `paid` keys in its paid
/// list; returns the records' keys and the paid keys.
async fn give_keys(
    harness: &TestHarness,
    index: usize,
    count: usize,
    paid: usize,
) -> (BTreeSet<XorName>, BTreeSet<XorName>) {
    let node = harness.test_node(index).expect("node");
    let storage = node.ant_protocol.as_ref().expect("protocol").storage();
    let mut stored = BTreeSet::new();
    for n in 0..count {
        let content = format!("paged sync record {index}-{n}").into_bytes();
        let address = ant_node::client::compute_address(&content);
        storage.put(&address, &content).await.expect("put");
        stored.insert(address);
    }
    let engine = node.replication_engine.as_ref().expect("engine");
    let mut listed = BTreeSet::new();
    for n in 0..paid {
        let key = ant_node::client::compute_address(format!("paid key {index}-{n}").as_bytes());
        engine.paid_list().insert(&key).await.expect("paid list");
        listed.insert(key);
    }
    (stored, listed)
}

/// With five nodes every node is in every key's close group, so a node's
/// hints for another are every record it holds and every key it lists as
/// paid. Here each list takes more than one page, and every page is served
/// from the hints the responder built at the first, with no other scan.
#[tokio::test]
#[serial]
async fn a_responder_serves_both_lists_over_pages_from_one_scan() {
    let _page = Seam::set(SYNC_PAGE_BYTES, SMALL_PAGE_BYTES);
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    let (records, paid) = give_keys(&harness, RESPONDER, 5, 6).await;
    let responder = peer_of(&harness, RESPONDER);
    let requester = harness
        .test_node(REQUESTER)
        .and_then(|node| node.p2p_node.as_ref())
        .expect("requester");
    let engine = harness
        .test_node(RESPONDER)
        .and_then(|node| node.replication_engine.as_ref())
        .expect("engine");
    let built_before = engine.served_hint_sets_for_test().2;

    let cycle = 0x5EED;
    let mut replica_seen = Vec::new();
    let mut paid_seen = Vec::new();
    let mut replica_pages = 0;
    let mut paid_pages = 0;
    let mut pages = None;
    let mut page = 0u32;
    while pages.is_none_or(|pages| page < pages) {
        let NeighborSyncPageResponse::Page {
            cycle: echoed,
            page: echoed_page,
            pages: of,
            replica_hints,
            paid_hints,
            ..
        } = page_answer(requester, &responder, cycle, page).await
        else {
            unreachable!("busy answers are asked again")
        };
        assert_eq!((echoed, echoed_page), (cycle, page));
        assert!(replica_hints.len() + paid_hints.len() <= SMALL_PAGE_KEYS);
        if let Some(pages) = pages {
            assert_eq!(of, pages, "the page count is the same on every page");
        }
        pages = Some(of);
        replica_pages += usize::from(!replica_hints.is_empty());
        paid_pages += usize::from(!paid_hints.is_empty());
        replica_seen.extend(replica_hints);
        paid_seen.extend(paid_hints);
        page += 1;
    }

    assert_eq!(
        replica_seen.iter().copied().collect::<BTreeSet<_>>(),
        records
    );
    assert_eq!(replica_seen.len(), records.len(), "each record on one page");
    assert!(paid.iter().all(|key| paid_seen.contains(key)));
    assert!(
        replica_pages > 1,
        "the replica hints took {replica_pages} page(s)"
    );
    assert!(paid_pages > 1, "the paid hints took {paid_pages} page(s)");
    let (held, _, built) = engine.served_hint_sets_for_test();
    assert_eq!(
        held, 0,
        "the hints are dropped after the exchange's last page"
    );
    assert_eq!(
        built,
        built_before + 1,
        "one scan for every page of the exchange"
    );

    // A page of an exchange the responder no longer holds is answered busy.
    let late = as_page_answer(ask(requester, &responder, page_request(cycle, 1)).await);
    assert!(
        matches!(late, NeighborSyncPageResponse::Busy { cycle: echoed, page: 1 } if echoed == cycle),
        "{late:?}"
    );

    harness.teardown().await.expect("teardown");
}

/// A requester pages its own hints, strictly one page after another, and
/// takes every page of the answer; the responder takes the requester's hints
/// from each page.
#[tokio::test]
#[serial]
async fn a_requester_pages_its_hints_and_reads_every_page_of_the_answer() {
    let _page = Seam::set(SYNC_PAGE_BYTES, SMALL_PAGE_BYTES);
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    let (responder_records, _) = give_keys(&harness, RESPONDER, 4, 3).await;
    let (requester_records, _) = give_keys(&harness, REQUESTER, 5, 0).await;
    let responder = peer_of(&harness, RESPONDER);
    let requester_engine = harness
        .test_node(REQUESTER)
        .and_then(|node| node.replication_engine.as_ref())
        .expect("engine");

    let mut report = requester_engine.sync_with_peer_now(&responder, true).await;
    for _ in 0..BUSY_RETRIES {
        if !report.busy {
            break;
        }
        tokio::time::sleep(BUSY_RETRY_PAUSE).await;
        report = requester_engine.sync_with_peer_now(&responder, true).await;
    }
    assert!(report.complete, "{report:?}");
    assert!(report.pages_sent > 1 && report.peer_pages > 1, "{report:?}");
    assert_eq!(
        report.pages_answered,
        report.pages_sent.max(report.peer_pages)
    );
    assert_eq!(
        report
            .replica_hints_received
            .iter()
            .copied()
            .collect::<BTreeSet<_>>(),
        responder_records
    );
    assert_eq!(report.replica_hints_answered, requester_records.len());

    // The responder took the requester's records from its pages: each is
    // stored there already, or on its way.
    let responder_node = harness.test_node(RESPONDER).expect("node");
    let responder_engine = responder_node.replication_engine.as_ref().expect("engine");
    let storage = responder_node
        .ant_protocol
        .as_ref()
        .expect("protocol")
        .storage();
    for key in &requester_records {
        assert!(
            storage.exists(key).unwrap_or(false)
                || responder_engine.fetch_pipeline_contains_for_test(key).await,
            "a hinted record was taken from its page"
        );
    }

    harness.teardown().await.expect("teardown");
}

/// A page refused at admission, the responder busy with other requesters, is
/// answered busy at once: the request gets an answer, so the requester
/// charges the responder nothing for silence (its trust in the responder is
/// unchanged), and syncs again later.
#[tokio::test]
#[serial]
async fn a_page_refused_at_admission_is_answered_busy_at_once() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    let responder = peer_of(&harness, RESPONDER);
    let requester = harness
        .test_node(REQUESTER)
        .and_then(|node| node.p2p_node.as_ref())
        .expect("requester");
    let engine = harness
        .test_node(RESPONDER)
        .and_then(|node| node.replication_engine.as_ref())
        .expect("engine");

    let held = engine.hold_neighbor_sync_admission();
    assert!(!held.is_empty());
    let trust_before = requester.peer_trust(&responder);
    let asked = Instant::now();
    let answer = as_page_answer(ask(requester, &responder, page_request(3, 0)).await);
    assert!(
        matches!(answer, NeighborSyncPageResponse::Busy { cycle: 3, page: 0 }),
        "{answer:?}"
    );
    assert!(
        asked.elapsed() < BUSY_ANSWERED_WITHIN,
        "answered at once, not left to time out: {:?}",
        asked.elapsed()
    );
    let trust_after = requester.peer_trust(&responder);
    assert!(
        (trust_after - trust_before).abs() < TRUST_TOLERANCE,
        "the requester's trust in the responder moved from {trust_before} to {trust_after}"
    );

    // Admission free again: the next exchange is served.
    drop(held);
    assert!(matches!(
        page_answer(requester, &responder, 4, 0).await,
        NeighborSyncPageResponse::Page { cycle: 4, .. }
    ));

    harness.teardown().await.expect("teardown");
}

/// A requester whose own paged exchange is answered busy, the responder's
/// admission full, ends the exchange and leaves the peer to a later cycle:
/// its engine charges the responder nothing, so its trust in the responder
/// is no lower afterwards.
#[tokio::test]
#[serial]
async fn a_busy_answer_to_a_requesters_exchange_costs_the_responder_no_trust() {
    let _page = Seam::set(SYNC_PAGE_BYTES, SMALL_PAGE_BYTES);
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    give_keys(&harness, RESPONDER, 4, 3).await;
    give_keys(&harness, REQUESTER, 5, 0).await;
    let responder = peer_of(&harness, RESPONDER);
    let requester_node = harness.test_node(REQUESTER).expect("node");
    let requester = requester_node.p2p_node.as_ref().expect("p2p");
    let requester_engine = requester_node.replication_engine.as_ref().expect("engine");
    let responder_engine = harness
        .test_node(RESPONDER)
        .and_then(|node| node.replication_engine.as_ref())
        .expect("engine");

    let held = responder_engine.hold_neighbor_sync_admission();
    assert!(!held.is_empty());
    let trust_before = requester.peer_trust(&responder);
    let report = requester_engine.sync_with_peer_now(&responder, true).await;
    assert!(report.busy && !report.complete, "{report:?}");
    assert_eq!(report.pages_answered, 0, "{report:?}");
    let trust_after = requester.peer_trust(&responder);
    assert!(
        trust_after > trust_before - TRUST_TOLERANCE,
        "the requester's trust in the responder fell from {trust_before} to {trust_after}"
    );
    drop(held);

    harness.teardown().await.expect("teardown");
}

/// The repair proofs of a page's replica hints wait for the requester's next
/// request of the exchange, which shows the page arrived: recorded then, even
/// when that request is answered busy; the last page's get none.
#[tokio::test]
#[serial]
async fn a_pages_proofs_wait_for_the_requesters_next_page() {
    let _page = Seam::set(SYNC_PAGE_BYTES, SMALL_PAGE_BYTES);
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    // Five records: three pages of two keys.
    give_keys(&harness, RESPONDER, 5, 0).await;
    let responder = peer_of(&harness, RESPONDER);
    let requester_id = peer_of(&harness, REQUESTER);
    let requester = harness
        .test_node(REQUESTER)
        .and_then(|node| node.p2p_node.as_ref())
        .expect("requester");
    let engine = harness
        .test_node(RESPONDER)
        .and_then(|node| node.replication_engine.as_ref())
        .expect("engine");
    let page_keys = |answer: NeighborSyncPageResponse| match answer {
        NeighborSyncPageResponse::Page {
            pages,
            replica_hints,
            ..
        } => (pages, replica_hints),
        NeighborSyncPageResponse::Busy { .. } => unreachable!("busy answers are asked again"),
    };

    let cycle = 0xB0B;
    let (pages, first) = page_keys(page_answer(requester, &responder, cycle, 0).await);
    assert_eq!((pages, first.len()), (3, SMALL_PAGE_KEYS));
    for key in &first {
        assert!(
            !proof_held(&harness, RESPONDER, &requester_id, key, false).await,
            "no proof before the requester shows the page arrived"
        );
    }

    // The next page, refused at admission: answered busy, and the first
    // page's proofs recorded all the same.
    let admission = engine.hold_neighbor_sync_admission();
    let busy = as_page_answer(ask(requester, &responder, page_request(cycle, 1)).await);
    assert!(
        matches!(busy, NeighborSyncPageResponse::Busy { page: 1, .. }),
        "{busy:?}"
    );
    drop(admission);
    for key in &first {
        assert!(
            proof_held(&harness, RESPONDER, &requester_id, key, true).await,
            "the page arrived: its proofs are recorded"
        );
    }

    // The rest of the exchange: the middle page's proofs come with the last
    // page's request; the last page's keys get none.
    let (_, middle) = page_keys(page_answer(requester, &responder, cycle, 1).await);
    let (_, last) = page_keys(page_answer(requester, &responder, cycle, 2).await);
    assert_eq!((middle.len(), last.len()), (SMALL_PAGE_KEYS, 1));
    for key in &middle {
        assert!(proof_held(&harness, RESPONDER, &requester_id, key, true).await);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !proof_held(&harness, RESPONDER, &requester_id, &last[0], false).await,
        "nothing follows the last page, so its keys get no proof"
    );

    harness.teardown().await.expect("teardown");
}

/// A page that waited past half the requester's wait for a worker is answered
/// busy, while the requester still waits. Here every page waits too long.
#[tokio::test]
#[serial]
async fn a_page_that_waited_too_long_is_answered_busy() {
    let config = TestNetworkConfig {
        replication_config: Some(ReplicationConfig {
            neighbor_sync_page_timeout: Duration::from_nanos(2),
            ..ReplicationConfig::default()
        }),
        ..TestNetworkConfig::minimal()
    };
    let harness = TestHarness::setup_with_config(config).await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    let responder = peer_of(&harness, RESPONDER);
    let requester = harness
        .test_node(REQUESTER)
        .and_then(|node| node.p2p_node.as_ref())
        .expect("requester");

    let answer = as_page_answer(ask(requester, &responder, page_request(9, 0)).await);
    assert!(
        matches!(answer, NeighborSyncPageResponse::Busy { cycle: 9, page: 0 }),
        "{answer:?}"
    );

    harness.teardown().await.expect("teardown");
}

/// Whether a node pages to a peer follows the release the peer's user agent
/// names, decided again for each exchange: these nodes' agents name this
/// build's release, so they get pages when pages start at that release, and
/// single messages when they start at the next.
#[tokio::test]
#[serial]
async fn a_node_pages_to_a_peer_whose_agent_names_a_paging_release() {
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    let responder = peer_of(&harness, RESPONDER);
    let requester_node = harness.test_node(REQUESTER).expect("node");
    let requester = requester_node.p2p_node.as_ref().expect("p2p");
    let engine = requester_node.replication_engine.as_ref().expect("engine");

    // Connected, so the peer's agent is known.
    let answered = ask(
        requester,
        &responder,
        ReplicationMessageBody::NeighborSyncRequest(NeighborSyncRequest {
            replica_hints: Vec::new(),
            paid_hints: Vec::new(),
            bootstrapping: false,
            commitment: None,
        }),
    )
    .await;
    assert!(matches!(
        answered,
        ReplicationMessageBody::NeighborSyncResponse(_)
    ));

    let own = own_release();
    {
        let _from = Seam::set(SYNC_PAGES_FROM_RELEASE, &(own + 1).to_string());
        assert!(
            !engine.pages_to_for_test(&responder).await,
            "a peer of the release before pages gets single messages"
        );
    }
    {
        let _from = Seam::set(SYNC_PAGES_FROM_RELEASE, &own.to_string());
        assert!(engine.pages_to_for_test(&responder).await);
    }
    {
        let _from = Seam::set(SYNC_PAGES_FROM_RELEASE, &(own + 1).to_string());
        assert!(
            !engine.pages_to_for_test(&responder).await,
            "decided again for each exchange"
        );
    }

    harness.teardown().await.expect("teardown");
}

/// Nodes that announce the release nodes page from, through the
/// announced-release seam, page to each other with no other seam: the agent
/// each announces names that release.
#[tokio::test]
#[serial]
async fn nodes_announcing_the_paging_release_page_to_each_other() {
    let _announced = Seam::set(
        ANNOUNCED_RELEASE,
        &NEIGHBOR_SYNC_PAGES_FROM_RELEASE.to_string(),
    );
    let harness = TestHarness::setup_minimal().await.expect("setup");
    harness.warmup_dht().await.expect("warmup");
    let responder = peer_of(&harness, RESPONDER);
    let requester_node = harness.test_node(REQUESTER).expect("node");
    let requester = requester_node.p2p_node.as_ref().expect("p2p");
    let engine = requester_node.replication_engine.as_ref().expect("engine");

    // Connected, so the peer's agent is known.
    let answered = ask(
        requester,
        &responder,
        ReplicationMessageBody::NeighborSyncRequest(NeighborSyncRequest {
            replica_hints: Vec::new(),
            paid_hints: Vec::new(),
            bootstrapping: false,
            commitment: None,
        }),
    )
    .await;
    assert!(matches!(
        answered,
        ReplicationMessageBody::NeighborSyncResponse(_)
    ));
    let agent = requester
        .transport()
        .peer_user_agent(&responder)
        .await
        .expect("connected");
    assert!(
        agent.ends_with(&format!("+ds.{NEIGHBOR_SYNC_PAGES_FROM_RELEASE}")),
        "{agent}"
    );
    assert!(engine.pages_to_for_test(&responder).await);

    harness.teardown().await.expect("teardown");
}

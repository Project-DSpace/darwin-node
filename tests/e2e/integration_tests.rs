//! The e2e harness's network: messages between nodes, and a port range
//! that is taken.
//!
//! Every network test here forms a network, so formation itself has no
//! test of its own.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::{TestHarness, TestNetworkConfig};
use saorsa_core::P2PEvent;
use serial_test::serial;
use std::net::UdpSocket;
use std::time::Duration;

/// Test that a node can send a message to a connected peer and the peer
/// receives it.
///
/// This validates the fundamental `send_message` / `subscribe_events` layer
/// that all higher-level protocols (chunk, etc.) are built on.
#[tokio::test(flavor = "multi_thread")]
async fn test_node_to_node_messaging() {
    const TEST_TOPIC: &str = "test/ping/v1";
    const PAYLOAD: &[u8] = b"hello from node 3";
    // With the reduced transport recv timeout (2s), messages should arrive
    // within a few seconds. 10s gives ample headroom.
    const DELIVERY_TIMEOUT_SECS: u64 = 10;

    let harness = TestHarness::setup_minimal()
        .await
        .expect("Failed to setup test harness");

    // Subscribe on every node's event stream *before* sending, so we can
    // confirm exactly which node receives the message.
    let all_nodes = harness.all_nodes();
    let mut all_rx: Vec<_> = all_nodes.iter().map(|n| n.subscribe_events()).collect();

    // Pick node 3 (regular) and send to one of its connected peers.
    let sender = harness.test_node(3).expect("Node 3 should exist");
    let peers = sender.connected_peers().await;
    assert!(
        !peers.is_empty(),
        "Node 3 should have at least one connected peer"
    );
    let target_peer_id = peers[0];

    let sender_p2p = sender.p2p_node.as_ref().expect("Node 3 should be running");

    sender_p2p
        .send_message(&target_peer_id, TEST_TOPIC, PAYLOAD.to_vec(), &[])
        .await
        .expect("Failed to send message to connected peer");

    // Poll all event streams until the message arrives or we time out.
    // Note: P2PEvent::Message.source is a transport-level peer ID (hex),
    // which differs from P2PNode::peer_id() (app-level "peer_…" format),
    // so we match on topic + payload instead of source identity.
    let mut received = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(DELIVERY_TIMEOUT_SECS);

    // Race all receivers concurrently instead of polling sequentially.
    // Pin the deadline sleep once so it tracks cumulative time across loop
    // iterations — otherwise select_all always wins the race against a
    // freshly-created sleep and the timeout never fires. Pinning prevents the
    // timeout from being recreated on each loop iteration, which would reset
    // its deadline.
    let timeout = tokio::time::sleep_until(deadline);
    tokio::pin!(timeout);

    while !received {
        let futs: Vec<_> = all_rx
            .iter_mut()
            .map(|rx| Box::pin(async move { rx.recv().await }))
            .collect();

        tokio::select! {
            (result, _idx, _remaining) = futures::future::select_all(futs) => {
                if let Ok(P2PEvent::Message { ref topic, ref data, .. }) = result {
                    if topic == TEST_TOPIC && data.as_slice() == PAYLOAD {
                        received = true;
                    }
                }
            }
            () = &mut timeout => {
                break;
            }
        }
    }

    assert!(
        received,
        "No node received the message on topic '{TEST_TOPIC}'"
    );

    // Drop event subscribers before teardown — holding broadcast receivers
    // can prevent background tasks from terminating during shutdown.
    drop(all_rx);
    drop(all_nodes);

    harness
        .teardown()
        .await
        .expect("Failed to teardown test harness");
}

/// A network whose first node cannot bind its port is brought up on a fresh
/// port range rather than failing the test that asked for it.
#[tokio::test]
#[serial]
async fn a_network_whose_port_is_taken_moves_to_a_fresh_range() {
    let config = TestNetworkConfig::minimal();
    let taken = config.base_port;
    let _holder = UdpSocket::bind(("127.0.0.1", taken)).expect("hold the first node's port");

    let harness = TestHarness::setup_with_config(config)
        .await
        .expect("the network comes up on another range");
    assert_ne!(
        harness.network().config().base_port,
        taken,
        "the network moved off the port it could not bind"
    );

    harness.teardown().await.expect("teardown");
}

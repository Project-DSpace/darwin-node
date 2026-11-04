// SPDX-License-Identifier: MIT OR Apache-2.0

//! Regression coverage for the owned-buffer send path (`P2pEndpoint::send_bytes`).
//!
//! `send_bytes` hands the caller's `Bytes` to the QUIC stream through
//! `write_chunks`, which advances the buffer in place across partial writes.
//! These tests prove, over a real loopback connection, that both entry points
//! deliver identical payloads at the sizes that exercise the interesting
//! branches (empty, single byte, either side of the large-send delivery-ack
//! threshold, and a multi-megabyte frame that needs many partial writes), and
//! that a send to a peer that goes away mid-transfer fails instead of hanging.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use bytes::Bytes;
use saorsa_transport::{EndpointError, NatConfig, P2pConfig, P2pEndpoint, PqcConfig};
use tokio::time::timeout;

/// Large-send delivery-ack threshold in `p2p_endpoint.rs`; sizes straddle it.
const DELIVERY_ACK_THRESHOLD: usize = 16 * 1024;
/// Bigger than any single QUIC flow-control grant, so the write loop must
/// make many partial writes.
const LARGE_FRAME: usize = 8 * 1024 * 1024;
/// Receiver-side message ceiling, comfortably above `LARGE_FRAME`.
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
/// Payload sizes covered by the delivery tests.
const PAYLOAD_SIZES: [usize; 5] = [
    0,
    1,
    DELIVERY_ACK_THRESHOLD - 1,
    DELIVERY_ACK_THRESHOLD,
    LARGE_FRAME,
];
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// Give the server's accept a head start before the client dials.
const ACCEPT_HEAD_START: Duration = Duration::from_millis(100);

fn node_config(known_peers: Vec<SocketAddr>) -> P2pConfig {
    P2pConfig::builder()
        .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .known_peers(known_peers)
        .max_message_size(MAX_MESSAGE_SIZE)
        .nat(NatConfig {
            enable_relay_fallback: false,
            ..Default::default()
        })
        .pqc(PqcConfig::default())
        .build()
        .expect("build test config")
}

async fn node(known_peers: Vec<SocketAddr>) -> P2pEndpoint {
    P2pEndpoint::new(node_config(known_peers))
        .await
        .expect("create endpoint")
}

async fn shutdown(endpoint: P2pEndpoint) {
    let _ = timeout(SHUTDOWN_TIMEOUT, endpoint.shutdown()).await;
}

/// A connected server/client pair; the server has accepted the connection so
/// its reader task is running.
struct Pair {
    server: P2pEndpoint,
    client: P2pEndpoint,
    server_addr: SocketAddr,
}

async fn connected_pair() -> Pair {
    let server = node(vec![]).await;
    let server_addr = server.local_addr().expect("server address");
    let accepting = server.clone();
    let accept = tokio::spawn(async move { timeout(CONNECT_TIMEOUT, accepting.accept()).await });
    tokio::time::sleep(ACCEPT_HEAD_START).await;

    let client = node(vec![server_addr]).await;
    let peer = timeout(CONNECT_TIMEOUT, client.connect(server_addr))
        .await
        .expect("connect did not time out")
        .expect("connect succeeded");
    let _ = accept.await;
    let server_addr = peer.remote_addr.as_socket_addr().expect("udp address");
    Pair {
        server,
        client,
        server_addr,
    }
}

fn payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Send `data` with either entry point and return what the server received
/// (`None` for an empty send, which produces no message to receive).
async fn round_trip(pair: &Pair, data: &[u8], owned: bool) -> Option<Vec<u8>> {
    let sent = if owned {
        timeout(
            TRANSFER_TIMEOUT,
            pair.client
                .send_bytes(&pair.server_addr, Bytes::from(data.to_vec())),
        )
        .await
    } else {
        timeout(TRANSFER_TIMEOUT, pair.client.send(&pair.server_addr, data)).await
    };
    sent.expect("send did not time out")
        .expect("send succeeded");
    if data.is_empty() {
        return None;
    }
    let (_, received) = timeout(TRANSFER_TIMEOUT, pair.server.recv())
        .await
        .expect("recv did not time out")
        .expect("recv succeeded");
    Some(received)
}

#[tokio::test]
async fn send_bytes_delivers_every_size_identically_to_send() {
    let pair = connected_pair().await;
    for &size in &PAYLOAD_SIZES {
        let data = payload(size);
        let via_owned = round_trip(&pair, &data, true).await;
        let via_borrowed = round_trip(&pair, &data, false).await;
        if size == 0 {
            assert!(via_owned.is_none() && via_borrowed.is_none());
            continue;
        }
        assert_eq!(
            via_owned.as_deref(),
            Some(data.as_slice()),
            "send_bytes payload mismatch at {size} bytes"
        );
        assert_eq!(
            via_borrowed.as_deref(),
            Some(data.as_slice()),
            "send payload mismatch at {size} bytes"
        );
    }
    shutdown(pair.client).await;
    shutdown(pair.server).await;
}

#[tokio::test]
async fn send_bytes_reports_a_peer_that_goes_away_mid_transfer() {
    let pair = connected_pair().await;
    let data = Bytes::from(payload(LARGE_FRAME));

    // Tear the receiver down while the multi-megabyte frame is in flight; the
    // send must surface a `SendFailed` whose byte count reflects what the
    // owned write loop actually handed to the stream, rather than wait
    // forever for progress or the delivery ack.
    let server = pair.server.clone();
    let teardown = tokio::spawn(async move {
        tokio::time::sleep(ACCEPT_HEAD_START).await;
        shutdown(server).await;
    });
    let result = timeout(
        TRANSFER_TIMEOUT,
        pair.client.send_bytes(&pair.server_addr, data),
    )
    .await;
    let _ = teardown.await;

    let outcome = result.expect("send_bytes must complete once the peer is gone");
    match outcome {
        Err(EndpointError::SendFailed { bytes_written, .. }) => assert!(
            bytes_written <= LARGE_FRAME,
            "bytes_written {bytes_written} exceeds the {LARGE_FRAME}-byte frame"
        ),
        other => panic!("expected SendFailed for a peer that went away, got {other:?}"),
    }
    shutdown(pair.client).await;
}

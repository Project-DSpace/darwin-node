// Copyright 2026 Saorsa Labs Ltd.
//
// This software is licensed under the MIT license <LICENSE-MIT or
// https://opensource.org/licenses/MIT> or the Apache License, Version 2.0
// <LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0>, at your
// option. This file may not be copied, modified, or distributed except
// according to those terms.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//! A sender whose peer moves to a new address mid-transfer must not keep
//! per-packet congestion-control state for the packets it sends afterwards.
//!
//! The peer's NAT rebinding is played by a UDP proxy that starts forwarding
//! the receiver's packets from a second socket, so the sender sees the
//! connection arrive from a new address and migrates to it. While the new
//! path is validated its ACKs are withheld from the congestion controller.
//! BBRv2 used to keep those packets tracked until that path's controller was
//! discarded, pinning a sampler-map slot for every later ack-eliciting packet.

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use tokio::net::UdpSocket;
use tokio::time::{Instant, sleep, timeout};

use crate::high_level::Connection;
use crate::nat_traversal_api::CongestionAlgorithm;
use crate::{NatConfig, P2pConfig, P2pEndpoint, PqcConfig};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const MESSAGE: usize = 512 * 1024;
const WARM_UP_MESSAGES: usize = 4;
const MESSAGES_AFTER_REBIND: usize = 16;
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
/// A connection that has gone quiet holds per-packet state only for the few
/// packets still in flight. Before the fix, it held an entry for every
/// ack-eliciting packet sent since the migration, thousands for this transfer.
const MAX_TRACKED_WHEN_IDLE: usize = 64;

fn loopback() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

fn node_config() -> TestResult<P2pConfig> {
    Ok(P2pConfig::builder()
        .bind_addr(loopback())
        .nat(NatConfig {
            enable_relay_fallback: false,
            // The only controller that keeps per-packet state, so the only one
            // this test can check.
            congestion_algorithm: CongestionAlgorithm::Bbr2,
            ..Default::default()
        })
        .pqc(PqcConfig::default())
        .build()?)
}

/// Relays datagrams between the receiver and the sender. Setting `rebound`
/// moves the receiver→sender direction to a second socket, which is what the
/// sender sees when the receiver's NAT mapping changes.
struct RebindingProxy {
    receiver_facing: SocketAddr,
    rebound: Arc<AtomicBool>,
}

async fn spawn_proxy(sender: SocketAddr) -> TestResult<RebindingProxy> {
    let to_receiver = Arc::new(UdpSocket::bind(loopback()).await?);
    let first = Arc::new(UdpSocket::bind(loopback()).await?);
    let second = Arc::new(UdpSocket::bind(loopback()).await?);
    let receiver_facing = to_receiver.local_addr()?;
    let rebound = Arc::new(AtomicBool::new(false));
    let receiver: Arc<OnceLock<SocketAddr>> = Arc::new(OnceLock::new());

    {
        let (to_receiver, first, second, rebound, receiver) = (
            Arc::clone(&to_receiver),
            Arc::clone(&first),
            Arc::clone(&second),
            Arc::clone(&rebound),
            Arc::clone(&receiver),
        );
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65_536];
            while let Ok((len, from)) = to_receiver.recv_from(&mut buf).await {
                let _ = receiver.set(from);
                let out = if rebound.load(Ordering::Relaxed) {
                    &second
                } else {
                    &first
                };
                if let Some(datagram) = buf.get(..len) {
                    let _ = out.send_to(datagram, sender).await;
                }
            }
        });
    }
    for socket in [first, second] {
        let (to_receiver, receiver) = (Arc::clone(&to_receiver), Arc::clone(&receiver));
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65_536];
            while let Ok((len, _)) = socket.recv_from(&mut buf).await {
                if let (Some(&target), Some(datagram)) = (receiver.get(), buf.get(..len)) {
                    let _ = to_receiver.send_to(datagram, target).await;
                }
            }
        });
    }
    Ok(RebindingProxy {
        receiver_facing,
        rebound,
    })
}

async fn send_messages(
    sender: &P2pEndpoint,
    to: SocketAddr,
    count: usize,
    received: &AtomicUsize,
) -> TestResult {
    let target = received.load(Ordering::Relaxed) + count * MESSAGE;
    for _ in 0..count {
        let message = Bytes::from(vec![7u8; MESSAGE]);
        timeout(STEP_TIMEOUT, sender.send_bytes(&to, message)).await??;
    }
    let deadline = Instant::now() + STEP_TIMEOUT;
    while received.load(Ordering::Relaxed) < target {
        if Instant::now() >= deadline {
            return Err("receiver did not get every message".into());
        }
        sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// Waits for the last ACKs to land: polls the per-packet state of the
/// connection's current path until it is down to `MAX_TRACKED_WHEN_IDLE` or
/// `STEP_TIMEOUT` passes, and returns the last count seen.
async fn tracked_once_idle(connection: &Connection) -> TestResult<usize> {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let tracked = connection
            .congestion_tracked_packets()
            .ok_or("the sender's congestion controller keeps no per-packet state")?;
        if tracked <= MAX_TRACKED_WHEN_IDLE || Instant::now() >= deadline {
            return Ok(tracked);
        }
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn migration_mid_transfer_does_not_pin_congestion_state() -> TestResult {
    let sender = P2pEndpoint::new(node_config()?).await?;
    let sender_addr = sender.local_addr().ok_or("sender has no local address")?;
    let proxy = spawn_proxy(sender_addr).await?;

    let accepted = {
        let sender = sender.clone();
        tokio::spawn(async move { sender.accept().await })
    };
    let receiver = P2pEndpoint::new(node_config()?).await?;
    timeout(STEP_TIMEOUT, receiver.connect(proxy.receiver_facing)).await??;
    let peer = timeout(STEP_TIMEOUT, accepted)
        .await??
        .ok_or("sender never accepted the connection")?;
    let to = peer
        .remote_addr
        .as_socket_addr()
        .ok_or("accepted connection has no socket address")?;

    let received = Arc::new(AtomicUsize::new(0));
    {
        let (receiver, received) = (receiver.clone(), Arc::clone(&received));
        tokio::spawn(async move {
            while let Ok((_, data)) = receiver.recv().await {
                received.fetch_add(data.len(), Ordering::Relaxed);
            }
        });
    }

    send_messages(&sender, to, WARM_UP_MESSAGES, &received).await?;
    proxy.rebound.store(true, Ordering::Relaxed);
    send_messages(&sender, to, MESSAGES_AFTER_REBIND, &received).await?;

    let connection = sender
        .get_quic_connection(&to)
        .await?
        .ok_or("sender lost the connection")?;
    assert_ne!(
        connection.current_remote_address(),
        to,
        "the sender never migrated, so the test did not exercise path validation"
    );
    let tracked = tracked_once_idle(&connection).await?;
    assert!(
        tracked <= MAX_TRACKED_WHEN_IDLE,
        "an idle connection still tracks {tracked} packets after migrating mid-transfer"
    );

    sender.shutdown().await;
    receiver.shutdown().await;
    Ok(())
}

// Copyright 2024 Saorsa Labs Ltd.
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

//! Process-global traffic accumulators (V2-834).
//!
//! The V2-623 counters are attached to individual objects (an endpoint's
//! connection table, a relay server's stats block). Production showed that a
//! single node process holds many more UDP sockets than it holds endpoint
//! summaries — per-dial relay carriers, rebind `prev_socket`s, relay-server
//! per-session sockets — so any object-scoped counter leaves bytes uncovered.
//! The accumulators here sit at the boundaries that *every* byte in the
//! process crosses, regardless of which object owns the socket, so their
//! totals match the host NIC by construction:
//!
//! * [`SOCKET_TRAFFIC`] — bumped inside the one real
//!   [`AsyncUdpSocket`](crate::high_level::AsyncUdpSocket) implementation
//!   (every QUIC socket) and at the relay server's per-session plain UDP
//!   sockets (its `recv_from` / `send_to`), i.e. every real UDP socket the
//!   process owns. The virtual `MasqueRelaySocket` is excluded without any
//!   per-call-site discrimination: its bytes ride a carrier connection that
//!   *is* counted here.
//! * [`FAILED_DIAL_TRAFFIC`] — the handshake bytes of dials that never became
//!   a registered connection. Folded from inside `Connecting`, because callers
//!   never hold a `Connection` for a failed or timed-out dial.
//! * [`RELAY_CLIENT_TRAFFIC`] — the client-side relay tunnel legs (stream
//!   bytes, keepalives, control frames, silent drops) across every
//!   `MasqueRelaySocket` the process creates.
//!
//! All counters are relaxed `AtomicU64`s bumped on hot paths and read by the
//! periodic summary task in `nat_traversal_api`; they are never reset.

use std::sync::atomic::{AtomicU64, Ordering};

/// Bytes and datagrams crossing every real UDP socket the process owns: the
/// QUIC endpoints' sockets and the relay server's per-session plain sockets.
/// Invariant: per process, `tx_bytes`/`rx_bytes` plus link-layer headers ≈
/// the NIC's UDP share.
#[derive(Debug)]
pub struct SocketTraffic {
    /// Bytes handed to `sendto` and accepted by the kernel (QUIC sockets and
    /// the relay's forwards to targets).
    pub tx_bytes: AtomicU64,
    /// Datagrams accepted by the kernel.
    pub tx_datagrams: AtomicU64,
    /// Datagrams the kernel refused with a non-`WouldBlock` error (dropped).
    pub tx_errors: AtomicU64,
    /// Bytes returned by `recvfrom`, before any connection matching — this is
    /// true ingress including scanner, malformed and unmatched datagrams, and
    /// the relay's receives from targets.
    pub rx_bytes: AtomicU64,
    /// Datagrams returned by `recvfrom`.
    pub rx_datagrams: AtomicU64,
    /// Bytes of stateless endpoint responses (Version Negotiation, Retry,
    /// Stateless Reset, refusal CLOSE). Already included in `tx_bytes`; kept
    /// separately so the endpoint total can be itemised.
    pub stateless_tx_bytes: AtomicU64,
    /// Count of stateless endpoint responses.
    pub stateless_tx_count: AtomicU64,
}

impl SocketTraffic {
    const fn new() -> Self {
        Self {
            tx_bytes: AtomicU64::new(0),
            tx_datagrams: AtomicU64::new(0),
            tx_errors: AtomicU64::new(0),
            rx_bytes: AtomicU64::new(0),
            rx_datagrams: AtomicU64::new(0),
            stateless_tx_bytes: AtomicU64::new(0),
            stateless_tx_count: AtomicU64::new(0),
        }
    }

    /// Record a datagram accepted by the kernel.
    #[inline]
    pub fn record_tx(&self, bytes: usize) {
        self.tx_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.tx_datagrams.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a datagram the kernel refused (not `WouldBlock`).
    #[inline]
    pub fn record_tx_error(&self) {
        self.tx_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a datagram returned by the kernel.
    #[inline]
    pub fn record_rx(&self, bytes: usize) {
        self.rx_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.rx_datagrams.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a stateless endpoint response handed to the socket.
    #[inline]
    pub fn record_stateless_tx(&self, bytes: usize) {
        self.stateless_tx_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.stateless_tx_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Process-wide socket-boundary totals. See the module docs.
pub static SOCKET_TRAFFIC: SocketTraffic = SocketTraffic::new();

/// Handshake traffic of dials that never became a registered connection.
///
/// Covers dials that errored, timed out, were cancelled (happy-eyeballs
/// losers, aborted tasks) or completed the handshake but were dropped before
/// the caller registered them. These bytes are on the wire — and therefore in
/// [`SOCKET_TRAFFIC`] — but invisible to the connection-table fold, so they
/// are itemised here. Hole-punch attempts are the expected dominant share.
#[derive(Debug)]
pub struct FailedDialTraffic {
    /// UDP bytes sent by abandoned dials.
    pub tx_bytes: AtomicU64,
    /// UDP bytes received by abandoned dials.
    pub rx_bytes: AtomicU64,
    /// Number of abandoned dials.
    pub count: AtomicU64,
}

impl FailedDialTraffic {
    const fn new() -> Self {
        Self {
            tx_bytes: AtomicU64::new(0),
            rx_bytes: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Fold one abandoned dial's cumulative UDP totals.
    #[inline]
    pub fn record(&self, tx_bytes: u64, rx_bytes: u64) {
        self.tx_bytes.fetch_add(tx_bytes, Ordering::Relaxed);
        self.rx_bytes.fetch_add(rx_bytes, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Process-wide abandoned-dial totals. See [`FailedDialTraffic`].
pub static FAILED_DIAL_TRAFFIC: FailedDialTraffic = FailedDialTraffic::new();

/// Client-side relay tunnel legs, summed over every `MasqueRelaySocket`.
///
/// These bytes travel inside the carrier QUIC connection to the relay and are
/// therefore already part of [`SOCKET_TRAFFIC`]; they are itemised so the
/// relay leg of a NAT'd node's traffic can be separated from its direct
/// traffic, and so the silent-drop paths in the virtual socket stop being
/// invisible.
#[derive(Debug)]
pub struct RelayClientTraffic {
    /// Stream bytes written to relay control streams (frames, length
    /// prefixes and keepalives, exactly as handed to `write_all`).
    pub stream_tx_bytes: AtomicU64,
    /// Stream bytes read from relay control streams (frames, length prefixes,
    /// keepalives and control frames).
    pub stream_rx_bytes: AtomicU64,
    /// Zero-length keepalive frames sent.
    pub keepalive_tx_count: AtomicU64,
    /// Zero-length keepalive frames received.
    pub keepalive_rx_count: AtomicU64,
    /// Bytes of tunnel control frames received (marker + length + body).
    pub control_rx_bytes: AtomicU64,
    /// Tunnel control frames received.
    pub control_rx_count: AtomicU64,
    /// Bytes quinn believes were sent but were dropped because the segment
    /// exceeded the per-target relay MTU. Subtract from a relayed connection's
    /// `udp_tx` to get bytes that actually reached the relay.
    pub dropped_oversized_bytes: AtomicU64,
    /// Transmits dropped for exceeding the per-target relay MTU.
    pub dropped_oversized_count: AtomicU64,
    /// Bytes silently dropped because the tunnel writer had already stopped.
    pub dropped_writer_stopped_bytes: AtomicU64,
    /// Transmits silently dropped because the tunnel writer had stopped.
    pub dropped_writer_stopped_count: AtomicU64,
    /// Inbound relay frames dropped for exceeding the receive buffer.
    pub dropped_recv_oversized_count: AtomicU64,
}

impl RelayClientTraffic {
    const fn new() -> Self {
        Self {
            stream_tx_bytes: AtomicU64::new(0),
            stream_rx_bytes: AtomicU64::new(0),
            keepalive_tx_count: AtomicU64::new(0),
            keepalive_rx_count: AtomicU64::new(0),
            control_rx_bytes: AtomicU64::new(0),
            control_rx_count: AtomicU64::new(0),
            dropped_oversized_bytes: AtomicU64::new(0),
            dropped_oversized_count: AtomicU64::new(0),
            dropped_writer_stopped_bytes: AtomicU64::new(0),
            dropped_writer_stopped_count: AtomicU64::new(0),
            dropped_recv_oversized_count: AtomicU64::new(0),
        }
    }
}

/// Process-wide client-side relay leg totals. See [`RelayClientTraffic`].
pub static RELAY_CLIENT_TRAFFIC: RelayClientTraffic = RelayClientTraffic::new();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_traffic_records_are_additive() {
        let t = SocketTraffic::new();
        t.record_tx(100);
        t.record_tx(50);
        t.record_tx_error();
        t.record_rx(7);
        t.record_stateless_tx(40);
        assert_eq!(t.tx_bytes.load(Ordering::Relaxed), 150);
        assert_eq!(t.tx_datagrams.load(Ordering::Relaxed), 2);
        assert_eq!(t.tx_errors.load(Ordering::Relaxed), 1);
        assert_eq!(t.rx_bytes.load(Ordering::Relaxed), 7);
        assert_eq!(t.rx_datagrams.load(Ordering::Relaxed), 1);
        assert_eq!(t.stateless_tx_bytes.load(Ordering::Relaxed), 40);
        assert_eq!(t.stateless_tx_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn failed_dial_record_counts_each_dial_once() {
        let t = FailedDialTraffic::new();
        t.record(1200, 0);
        t.record(2400, 1200);
        assert_eq!(t.tx_bytes.load(Ordering::Relaxed), 3600);
        assert_eq!(t.rx_bytes.load(Ordering::Relaxed), 1200);
        assert_eq!(t.count.load(Ordering::Relaxed), 2);
    }
}

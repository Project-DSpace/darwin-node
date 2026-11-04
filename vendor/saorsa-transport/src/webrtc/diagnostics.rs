// Copyright 2026 Saorsa Labs Limited
//
// Licensed under the MIT license <https://opensource.org/licenses/MIT> or the
// Apache License, Version 2.0 <https://www.apache.org/licenses/LICENSE-2.0>,
// at your option.

//! Local diagnostics for native WebRTC associations; no peer credentials are retained.

use super::WebRtcDirectError;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use webrtc_stack::peer_connection::peer_connection_state::RTCPeerConnectionState;

/// Statistics for one live native WebRTC association.
#[derive(Clone, Debug)]
pub struct WebRtcConnectionMetrics {
    /// Listener-local identifier, independent of ICE credentials and remote ports.
    pub connection_id: u64,
    /// Observed remote UDP address.
    pub remote_addr: SocketAddr,
    /// Current ICE/DTLS peer-connection state.
    pub state: RTCPeerConnectionState,
    /// Time association construction began.
    pub created_at: Instant,
    /// First successful ICE/DTLS connection, if established.
    pub connected_at: Option<Instant>,
    /// Bytes written to DataChannels, excluding UDP/DTLS/SCTP overhead.
    pub bytes_sent: u64,
    /// Bytes read from DataChannels, excluding UDP/DTLS/SCTP overhead.
    pub bytes_received: u64,
    /// Most recent nonempty DataChannel read or write.
    pub last_activity: Option<Instant>,
    /// Unavailable: upstream currently supplies placeholder ICE RTT statistics.
    pub rtt: Option<Duration>,
    /// Unavailable: upstream does not expose DataChannel packet-loss statistics.
    pub packet_loss: Option<f64>,
}

/// A coherent lifecycle snapshot plus independently sampled traffic counters.
#[derive(Clone, Debug, Default)]
pub struct WebRtcDiagnosticsSnapshot {
    /// Whether the UDP driver is alive; does not assert external reachability.
    pub running: bool,
    /// Associations currently in the connected state.
    pub active_connections: usize,
    /// Associations still constructing or negotiating ICE/DTLS.
    pub connecting_connections: usize,
    /// Association construction attempts, excluding pre-ICE admission rejections.
    pub connection_attempts: u64,
    /// Associations that reached connected, counted once even after reconnection.
    pub successful_connections: u64,
    /// Associations that failed or ended before connecting, counted once each.
    pub failed_connections: u64,
    /// Associations closed or dropped, counted once each.
    pub closed_connections: u64,
    /// Transitions into disconnected, including repeat transitions after recovery.
    pub disconnections: u64,
    /// Proven incoming associations rejected by transport capacity limits.
    pub rejected_connections: u64,
    /// UDP receive or routing errors.
    pub listener_errors: u64,
    /// Cumulative DataChannel payload bytes sent, including closed associations.
    pub bytes_sent: u64,
    /// Cumulative DataChannel payload bytes received, including closed associations.
    pub bytes_received: u64,
    /// Open owned associations; closed history is represented only by counters.
    pub connections: Vec<WebRtcConnectionMetrics>,
}

#[derive(Default)]
struct Traffic {
    sent: AtomicU64,
    received: AtomicU64,
    last_activity: Mutex<Option<Instant>>,
}

struct Entry {
    remote_addr: SocketAddr,
    state: RTCPeerConnectionState,
    created_at: Instant,
    connected_at: Option<Instant>,
    failed: bool,
    traffic: Arc<Traffic>,
}

#[derive(Default)]
struct State {
    next_id: u64,
    counters: WebRtcDiagnosticsSnapshot,
    connections: HashMap<u64, Entry>,
}

#[derive(Default)]
struct Inner {
    state: Mutex<State>,
    running: AtomicBool,
    traffic: Traffic,
}

/// Cloneable local diagnostics handle, valid after listener shutdown.
///
/// Snapshots retain only open owned connections, so history cannot grow
/// with traffic. This API neither probes peers nor exports a network endpoint.
#[derive(Clone, Default)]
pub struct WebRtcDiagnostics(Arc<Inner>);

impl WebRtcDiagnostics {
    /// Snapshot lifecycle counts and currently owned connection traffic.
    pub fn snapshot(&self) -> WebRtcDiagnosticsSnapshot {
        let state = self.0.state.lock();
        let mut snapshot = state.counters.clone();
        snapshot.running = self.0.running.load(Ordering::Acquire);
        snapshot.bytes_sent = self.0.traffic.sent.load(Ordering::Relaxed);
        snapshot.bytes_received = self.0.traffic.received.load(Ordering::Relaxed);
        for (&id, entry) in &state.connections {
            match entry.state {
                RTCPeerConnectionState::Connected => snapshot.active_connections += 1,
                RTCPeerConnectionState::New | RTCPeerConnectionState::Connecting => {
                    snapshot.connecting_connections += 1
                }
                _ => {}
            }
            snapshot.connections.push(metrics(id, entry));
        }
        snapshot
            .connections
            .sort_unstable_by_key(|connection| connection.connection_id);
        snapshot
    }

    pub(super) fn begin(
        &self,
        remote_addr: SocketAddr,
    ) -> Result<ConnectionTracker, WebRtcDirectError> {
        let mut state = self.0.state.lock();
        let id = state.next_id.checked_add(1).ok_or_else(|| {
            WebRtcDirectError::Session("WebRTC diagnostic identifiers exhausted".into())
        })?;
        state.next_id = id;
        state.counters.connection_attempts += 1;
        let traffic = Arc::new(Traffic::default());
        state.connections.insert(
            id,
            Entry {
                remote_addr,
                state: RTCPeerConnectionState::New,
                created_at: Instant::now(),
                connected_at: None,
                failed: false,
                traffic: Arc::clone(&traffic),
            },
        );
        Ok(ConnectionTracker(ConnectionObserver {
            diagnostics: self.clone(),
            id,
            traffic,
        }))
    }

    pub(super) fn reject(&self) {
        self.0.state.lock().counters.rejected_connections += 1;
    }

    pub(super) fn listener_error(&self) {
        self.0.state.lock().counters.listener_errors += 1;
    }

    pub(super) fn driver_guard(&self, shutdown: CancellationToken) -> DriverGuard {
        self.0.running.store(true, Ordering::Release);
        DriverGuard {
            diagnostics: self.clone(),
            shutdown,
        }
    }
}

fn metrics(id: u64, entry: &Entry) -> WebRtcConnectionMetrics {
    WebRtcConnectionMetrics {
        connection_id: id,
        remote_addr: entry.remote_addr,
        state: entry.state,
        created_at: entry.created_at,
        connected_at: entry.connected_at,
        bytes_sent: entry.traffic.sent.load(Ordering::Relaxed),
        bytes_received: entry.traffic.received.load(Ordering::Relaxed),
        last_activity: *entry.traffic.last_activity.lock(),
        rtt: None,
        packet_loss: None,
    }
}

pub(super) struct DriverGuard {
    diagnostics: WebRtcDiagnostics,
    shutdown: CancellationToken,
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        self.diagnostics.0.running.store(false, Ordering::Release);
        self.shutdown.cancel();
    }
}

pub(super) struct ConnectionTracker(ConnectionObserver);

impl ConnectionTracker {
    pub(super) fn observer(&self) -> ConnectionObserver {
        self.0.clone()
    }
    pub(super) fn metrics(&self) -> Option<WebRtcConnectionMetrics> {
        self.0
            .diagnostics
            .0
            .state
            .lock()
            .connections
            .get(&self.0.id)
            .map(|entry| metrics(self.0.id, entry))
    }
}

impl Drop for ConnectionTracker {
    fn drop(&mut self) {
        self.0.transition(RTCPeerConnectionState::Closed);
        self.0
            .diagnostics
            .0
            .state
            .lock()
            .connections
            .remove(&self.0.id);
    }
}

#[derive(Clone)]
pub(super) struct ConnectionObserver {
    diagnostics: WebRtcDiagnostics,
    id: u64,
    traffic: Arc<Traffic>,
}

impl ConnectionObserver {
    pub(super) fn transition(&self, next: RTCPeerConnectionState) {
        let mut state = self.diagnostics.0.state.lock();
        let State {
            counters,
            connections,
            ..
        } = &mut *state;
        let Some(entry) = connections.get_mut(&self.id) else {
            return;
        };
        if entry.state == next {
            return;
        }
        if next == RTCPeerConnectionState::Connected && entry.connected_at.is_none() {
            entry.connected_at = Some(Instant::now());
            counters.successful_connections += 1;
        }
        if next == RTCPeerConnectionState::Failed
            || (next == RTCPeerConnectionState::Closed && entry.connected_at.is_none())
        {
            if !entry.failed {
                entry.failed = true;
                counters.failed_connections += 1;
            }
        }
        if next == RTCPeerConnectionState::Disconnected {
            counters.disconnections += 1;
        }
        if next == RTCPeerConnectionState::Closed {
            counters.closed_connections += 1;
        }
        tracing::debug!(connection_id = self.id, remote = %entry.remote_addr, previous = ?entry.state, state = ?next, "WebRTC connection state changed");
        entry.state = next;
        if next == RTCPeerConnectionState::Closed {
            connections.remove(&self.id);
        }
    }

    pub(super) fn sent(&self, length: usize) {
        if length == 0 {
            return;
        }
        self.traffic
            .sent
            .fetch_add(length as u64, Ordering::Relaxed);
        self.diagnostics
            .0
            .traffic
            .sent
            .fetch_add(length as u64, Ordering::Relaxed);
        *self.traffic.last_activity.lock() = Some(Instant::now());
    }

    pub(super) fn received(&self, length: usize) {
        if length == 0 {
            return;
        }
        self.traffic
            .received
            .fetch_add(length as u64, Ordering::Relaxed);
        self.diagnostics
            .0
            .traffic
            .received
            .fetch_add(length as u64, Ordering::Relaxed);
        *self.traffic.last_activity.lock() = Some(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_is_counted_once_and_closed_entries_are_released() {
        let diagnostics = WebRtcDiagnostics::default();
        let address = "127.0.0.1:1234".parse().unwrap();
        let first = diagnostics.begin(address).unwrap();
        let second = diagnostics.begin(address).unwrap();
        assert_ne!(first.0.id, second.0.id);
        assert_eq!(diagnostics.snapshot().connecting_connections, 2);
        let observer = first.observer();
        observer.transition(RTCPeerConnectionState::Connected);
        observer.transition(RTCPeerConnectionState::Connected);
        observer.sent(13);
        observer.received(17);
        observer.transition(RTCPeerConnectionState::Disconnected);
        observer.transition(RTCPeerConnectionState::Connected);
        let live = diagnostics.snapshot();
        assert_eq!(live.active_connections, 1);
        assert_eq!(live.successful_connections, 1);
        assert_eq!(live.disconnections, 1);
        assert_eq!(live.bytes_sent, 13);
        assert_eq!(live.bytes_received, 17);
        assert!(first.metrics().unwrap().last_activity.is_some());
        assert!(first.metrics().unwrap().rtt.is_none());
        assert!(first.metrics().unwrap().packet_loss.is_none());
        observer.transition(RTCPeerConnectionState::Closed);
        assert!(first.metrics().is_none());
        drop(first);
        // A late upstream callback cannot resurrect a dropped association.
        observer.transition(RTCPeerConnectionState::Connected);
        second.observer().transition(RTCPeerConnectionState::Failed);
        drop(second);
        let closed = diagnostics.snapshot();
        assert!(closed.connections.is_empty());
        assert_eq!(closed.active_connections, 0);
        assert_eq!(closed.connecting_connections, 0);
        assert_eq!(closed.connection_attempts, 2);
        assert_eq!(closed.successful_connections, 1);
        assert_eq!(closed.failed_connections, 1);
        assert_eq!(closed.closed_connections, 2);
        assert_eq!(closed.bytes_sent, 13);
    }

    #[tokio::test]
    async fn aborted_setup_and_driver_do_not_leave_live_diagnostics() {
        let diagnostics = WebRtcDiagnostics::default();
        let attempt = diagnostics
            .begin("127.0.0.1:4321".parse().unwrap())
            .unwrap();
        let shutdown = CancellationToken::new();
        let guard = diagnostics.driver_guard(shutdown.clone());
        let task = tokio::spawn(async move {
            let (_attempt, _guard) = (attempt, guard);
            std::future::pending::<()>().await;
        });
        assert!(diagnostics.snapshot().running);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let snapshot = diagnostics.snapshot();
        assert!(!snapshot.running);
        assert!(shutdown.is_cancelled());
        assert_eq!(snapshot.failed_connections, 1);
        assert_eq!(snapshot.closed_connections, 1);
        assert!(snapshot.connections.is_empty());
    }
}

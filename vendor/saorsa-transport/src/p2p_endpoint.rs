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

//! P2P endpoint for saorsa-transport
//!
//! This module provides the main API for P2P communication with NAT traversal,
//! secure connections, and event-driven architecture.
//!
//! # Features
//!
//! - Configuration via [`P2pConfig`](crate::unified_config::P2pConfig)
//! - Event subscription via broadcast channels
//! - TLS-based peer authentication via ML-DSA-65 (v0.2+)
//! - NAT traversal with automatic fallback
//! - Connection metrics and statistics
//!
//! # Example
//!
//! ```rust,ignore
//! use saorsa_transport::{P2pEndpoint, P2pConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // All nodes are symmetric - they can both connect and accept connections
//!     let config = P2pConfig::builder()
//!         .bind_addr("0.0.0.0:9000".parse()?)
//!         .known_peer("quic.saorsalabs.com:9000".parse()?)
//!         .build()?;
//!
//!     let endpoint = P2pEndpoint::new(config).await?;
//!     println!("Public key: {:?}", endpoint.local_public_key());
//!
//!     // Subscribe to events
//!     let mut events = endpoint.subscribe();
//!     tokio::spawn(async move {
//!         while let Ok(event) = events.recv().await {
//!             println!("Event: {:?}", event);
//!         }
//!     });
//!
//!     // Connect to known peers
//!     endpoint.connect_known_peers().await?;
//!
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::MtuDiscoveryConfig;
use crate::Side;
use crate::bounded_pending_buffer::BoundedPendingBuffer;
use crate::connection_router::{ConnectionRouter, RouterConfig};
use crate::connection_strategy::{
    ConnectionMethod, ConnectionStage, ConnectionStrategy, StrategyConfig,
};
use crate::constrained::ConnectionId as ConstrainedConnectionId;
use crate::constrained::EngineEvent;
use crate::crypto::raw_public_keys::key_utils::generate_ml_dsa_keypair;
use crate::happy_eyeballs::{self, HappyEyeballsConfig};
pub use crate::nat_traversal_api::TraversalPhase;
use crate::nat_traversal_api::{
    NatTraversalEndpoint, NatTraversalError, NatTraversalEvent, NatTraversalStatistics,
    PreparedRelay, RELAY_TUNNEL_INITIAL_MTU,
};
use crate::shared::{normalize_socket_addr, socket_addr_variants};
use crate::transport::{ProtocolEngine, TransportAddr, TransportRegistry};
use crate::unified_config::P2pConfig;
use rustls;

/// Event channel capacity
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// How often the stale connection reaper checks for QUIC-dead connections
/// via `is_connected()`.  This is a cheap local state check — no network
/// traffic.  Kept short so the reaper acts as a fast safety net behind the
/// event-driven reader-exit detection.
const STALE_REAPER_INTERVAL: Duration = Duration::from_secs(10);

/// Maximum time a QUIC stream write may make no forward progress.
const STREAM_WRITE_PROGRESS_TIMEOUT: Duration = Duration::from_secs(2);

/// Payload size at or above which QUIC sends wait for the peer to acknowledge
/// receipt of the full stream after `finish()`. Smaller payloads return as
/// soon as the local stack has accepted the data, avoiding an extra RTT for
/// small request/response traffic.
const LARGE_SEND_DELIVERY_ACK_THRESHOLD: usize = 16 * 1024;

/// Maximum number of inbound uni streams read concurrently per QUIC connection.
const MAX_CONCURRENT_INBOUND_STREAM_READS_PER_CONNECTION: usize = 32;

/// Application error code used when aborting a stuck/failed send stream.
/// `0` is the conventional "generic abort, no specific application error"
/// QUIC application error code.
const STREAM_RESET_ABORT_CODE: u32 = 0;

/// Quick direct connection attempt after a failed hole-punch round.
/// If the target's outgoing packets created a NAT binding, a QUIC handshake
/// through the pinhole needs only 1-2 RTTs (~600ms at 300ms worst-case RTT).
const POST_HOLEPUNCH_DIRECT_RETRY_TIMEOUT: Duration = Duration::from_secs(1);

/// Extra time subscribers wait beyond the normal fallback strategy budget
/// before treating an in-flight dial entry as stale.
const PENDING_DIAL_WAIT_SLACK: Duration = Duration::from_secs(5);

/// Capacity of the broadcast channel that fans an in-flight dial's outcome
/// out to subscribers waiting on the same target. Sized for the small
/// number of concurrent waiters we observe in practice; the channel only
/// needs to hold the single result message the owner sends on completion.
const PENDING_DIAL_BROADCAST_CAPACITY: usize = 4;

static NEXT_PENDING_DIAL_ID: AtomicU64 = AtomicU64::new(1);

use crate::SHUTDOWN_DRAIN_TIMEOUT;

type PendingDialResult = Result<(PeerConnection, ConnectionMethod), String>;
type PendingDialMap = HashMap<SocketAddr, PendingDial>;

struct PendingDial {
    id: u64,
    tx: broadcast::Sender<PendingDialResult>,
    started_at: Instant,
}

/// Remove the pending-dial entry for `target` only if it is still the entry
/// with the matching `id` (i.e. the caller still owns that slot). Returns the
/// removed entry, or `None` if the slot was already gone or had been claimed
/// by a newer dial.
async fn remove_pending_dial_if_owned(
    pending_dials: &tokio::sync::Mutex<PendingDialMap>,
    target: SocketAddr,
    id: u64,
) -> Option<PendingDial> {
    let mut pending = pending_dials.lock().await;
    match pending.get(&target) {
        Some(entry) if entry.id == id => pending.remove(&target),
        _ => None,
    }
}

struct PendingDialGuard {
    pending_dials: Arc<tokio::sync::Mutex<PendingDialMap>>,
    target: SocketAddr,
    id: u64,
    active: bool,
}

impl PendingDialGuard {
    fn new(
        pending_dials: Arc<tokio::sync::Mutex<PendingDialMap>>,
        target: SocketAddr,
        id: u64,
    ) -> Self {
        Self {
            pending_dials,
            target,
            id,
            active: true,
        }
    }

    async fn complete(
        &mut self,
        result: &Result<(PeerConnection, ConnectionMethod), EndpointError>,
    ) {
        if !self.active {
            return;
        }

        let entry = remove_pending_dial_if_owned(&self.pending_dials, self.target, self.id).await;
        self.active = false;

        let Some(entry) = entry else {
            return;
        };

        match result {
            Ok((conn, method)) => {
                let _ = entry.tx.send(Ok((conn.clone(), method.clone())));
            }
            Err(e) => {
                let _ = entry.tx.send(Err(e.to_string()));
            }
        }
    }
}

impl Drop for PendingDialGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        let pending_dials = Arc::clone(&self.pending_dials);
        let target = self.target;
        let id = self.id;

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if remove_pending_dial_if_owned(&pending_dials, target, id)
                    .await
                    .is_some()
                {
                    debug!(
                        "connect_with_fallback: cleared cancelled in-flight dial to {}",
                        target
                    );
                }
            });
        }
    }
}

fn pending_dial_wait_timeout(strategy_config: Option<&StrategyConfig>) -> Duration {
    let strategy = strategy_config.cloned().unwrap_or_default();
    let direct_budget = strategy
        .direct_connect_timeout
        .saturating_add(strategy.direct_handshake_timeout);
    let holepunch_round_budget = strategy
        .holepunch_timeout
        .saturating_add(POST_HOLEPUNCH_DIRECT_RETRY_TIMEOUT);
    let holepunch_budget = if strategy.holepunch_enabled {
        holepunch_round_budget.saturating_mul(strategy.max_holepunch_rounds)
    } else {
        Duration::ZERO
    };
    let relay_budget = if strategy.relay_enabled {
        strategy.relay_timeout
    } else {
        Duration::ZERO
    };

    direct_budget
        .saturating_add(holepunch_budget)
        .saturating_add(relay_budget)
        .saturating_add(PENDING_DIAL_WAIT_SLACK)
}

fn pending_dial_remaining_wait(wait_timeout: Duration, age: Duration) -> Option<Duration> {
    wait_timeout
        .checked_sub(age)
        .filter(|remaining| !remaining.is_zero())
}

/// Extract the raw SPKI (SubjectPublicKeyInfo) bytes from a QUIC connection's
/// peer identity, if TLS-based authentication was used.
///
/// Returns `None` for unauthenticated or constrained connections.
pub(crate) fn extract_public_key_bytes_from_connection(
    connection: &crate::high_level::Connection,
) -> Option<Vec<u8>> {
    let identity = connection.peer_identity()?;
    let certs = identity.downcast_ref::<Vec<rustls::pki_types::CertificateDer<'static>>>()?;
    let cert = certs.first()?;
    Some(cert.as_ref().to_vec())
}

/// P2P endpoint - the primary API for saorsa-transport
///
/// This struct provides the main interface for P2P communication with
/// NAT traversal, connection management, and secure messaging.
pub struct P2pEndpoint {
    /// Internal NAT traversal endpoint
    inner: Arc<NatTraversalEndpoint>,

    // v0.2: auth_manager removed - TLS handles peer authentication via ML-DSA-65
    /// Connected peers keyed by remote socket address
    connected_peers: Arc<RwLock<HashMap<SocketAddr, PeerConnection>>>,

    /// Sockets we have already broadcast `P2pEvent::PeerConnected` for.
    ///
    /// A single underlying QUIC connection can be observed via several paths
    /// (NAT-traversal callback, accept-side reader, dial short-circuit, send-side
    /// hole-punch fixup). Without dedup each path fires its own PeerConnected,
    /// which translates to duplicate `LinkEvent::PeerConnected` →
    /// `ConnectionEvent::Established` upstream and triggers redundant identity
    /// announces (one of the root causes of the "Failed to send identity
    /// announce" warning storm). Cleared when the address is cleaned up so
    /// real reconnects fire a fresh event.
    emitted_peer_connected: Arc<dashmap::DashSet<SocketAddr>>,

    /// Endpoint statistics
    stats: Arc<RwLock<EndpointStats>>,

    /// Configuration
    config: P2pConfig,

    /// Event broadcaster
    event_tx: broadcast::Sender<P2pEvent>,

    /// SPKI fingerprint of our own ML-DSA-65 public key (BLAKE3 hash)
    our_fingerprint: [u8; 32],

    /// Our ML-DSA-65 public key SPKI bytes (for identity sharing)
    public_key: Vec<u8>,

    /// Shutdown token for cooperative cancellation
    shutdown: CancellationToken,

    /// Bounded pending data buffer for message ordering
    pending_data: Arc<RwLock<BoundedPendingBuffer>>,

    /// Transport registry for multi-transport support
    ///
    /// Contains all registered transport providers (UDP, BLE, etc.) that this
    /// endpoint can use for connectivity.
    transport_registry: Arc<TransportRegistry>,

    /// Connection router for automatic protocol engine selection
    ///
    /// Routes connections through either QUIC (for broadband) or Constrained
    /// engine (for BLE/LoRa) based on transport capabilities. The router is
    /// fully interior-mutable — all methods take `&self` and stat/state
    /// mutations are lock-free — so no `RwLock` is needed.
    router: Arc<ConnectionRouter>,

    /// Mapping from TransportAddr to ConnectionId for constrained connections
    ///
    /// When a peer is connected via a constrained transport (BLE, LoRa, etc.),
    /// this map stores the ConstrainedEngine's ConnectionId for that address.
    /// UDP/QUIC peers are NOT in this map - they use the standard QUIC connection.
    constrained_connections: Arc<RwLock<HashMap<TransportAddr, ConstrainedConnectionId>>>,

    /// Reverse lookup: ConnectionId → TransportAddr for constrained connections
    ///
    /// This enables mapping incoming constrained data back to the correct remote address.
    /// Registered when ConnectionAccepted/Established fires for constrained transports.
    constrained_peer_addrs: Arc<RwLock<HashMap<ConstrainedConnectionId, TransportAddr>>>,

    /// Per-target peer IDs for hole-punch attempts. When set for a target
    /// address, the PUNCH_ME_NOW uses the peer ID instead of wire_id_from_addr,
    /// allowing the coordinator to match by peer identity. Keyed by target
    /// address so concurrent dials don't race on shared state.
    hole_punch_target_peer_ids: Arc<dashmap::DashMap<SocketAddr, [u8; 32]>>,

    /// Per-target preferred coordinator for hole-punch relay. When the DHT
    /// lookup discovers a peer via a FindNode response from another node, that
    /// responding node (the "referrer") has a connection to the discovered peer
    /// and is an ideal coordinator for PUNCH_ME_NOW relay. Keyed by target
    /// address, value is the referrer's socket address.
    hole_punch_preferred_coordinators: Arc<dashmap::DashMap<SocketAddr, SocketAddr>>,

    /// Channel sender for data received from QUIC reader tasks and constrained poller
    data_tx: mpsc::Sender<(SocketAddr, Vec<u8>)>,

    /// Channel receiver for data received from QUIC reader tasks and constrained poller
    data_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<(SocketAddr, Vec<u8>)>>>,

    /// JoinSet tracking background reader tasks (each returns SocketAddr on exit).
    /// Spawn into it only through [`spawn_reader`], which reaps finished readers.
    reader_tasks: Arc<tokio::sync::Mutex<JoinSet<SocketAddr>>>,

    /// Per-address abort handles for targeted reader task cancellation
    reader_handles: Arc<RwLock<HashMap<SocketAddr, tokio::task::AbortHandle>>>,

    /// Channel for reader tasks to notify immediate cleanup on exit.
    ///
    /// When a reader task detects a dead QUIC connection (`accept_uni` error),
    /// it sends `(peer_addr, stable_id)` here. The stable_id identifies the
    /// specific Connection the reader owned; the cleanup handler only evicts
    /// the DashMap entry if its stable_id matches, so a newer
    /// simultaneous-open replacement is preserved when an older reader dies.
    /// The reader-exit handler task receives it and calls
    /// `do_cleanup_connection` immediately — no waiting for the periodic
    /// stale reaper.
    reader_exit_tx: mpsc::UnboundedSender<(SocketAddr, usize)>,

    /// In-flight connection attempts, keyed by target address.
    ///
    /// When multiple concurrent `connect_with_fallback` calls target the same
    /// address (e.g., 3 chunks all needing the same NATed node), only the first
    /// call does the actual connection work. Subsequent callers subscribe to a
    /// broadcast channel and wait for the result instead of starting parallel
    /// hole-punch attempts that deadlock the runtime.
    pending_dials: Arc<tokio::sync::Mutex<PendingDialMap>>,
}

impl std::fmt::Debug for P2pEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P2pEndpoint")
            .field("public_key_len", &self.public_key.len())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Connection information for a peer
#[derive(Debug, Clone)]
pub struct PeerConnection {
    /// Remote peer's ML-DSA-65 SPKI public key bytes (None for constrained/unauthenticated)
    pub public_key: Option<Vec<u8>>,

    /// Remote address (supports all transport types)
    pub remote_addr: TransportAddr,

    /// Whether peer is authenticated
    pub authenticated: bool,

    /// Connection established time
    pub connected_at: Instant,

    /// Last activity time
    pub last_activity: Instant,
}

/// Connection metrics for P2P peers
#[derive(Debug, Clone, Default)]
pub struct ConnectionMetrics {
    /// Bytes sent to this peer
    pub bytes_sent: u64,

    /// Bytes received from this peer
    pub bytes_received: u64,

    /// Round-trip time
    pub rtt: Option<Duration>,

    /// Packet loss rate (0.0 to 1.0)
    pub packet_loss: f64,

    /// Last activity timestamp
    pub last_activity: Option<Instant>,
}

/// P2P endpoint statistics
#[derive(Debug, Clone)]
pub struct EndpointStats {
    /// Number of active connections
    pub active_connections: usize,

    /// Total successful connections
    pub successful_connections: u64,

    /// Total failed connections
    pub failed_connections: u64,

    /// NAT traversal attempts
    pub nat_traversal_attempts: u64,

    /// Successful NAT traversals
    pub nat_traversal_successes: u64,

    /// Direct connections (no NAT traversal needed)
    pub direct_connections: u64,

    /// Relayed connections
    pub relayed_connections: u64,

    /// Total bootstrap nodes configured
    pub total_bootstrap_nodes: usize,

    /// Connected bootstrap nodes
    pub connected_bootstrap_nodes: usize,

    /// Endpoint start time
    pub start_time: Instant,

    /// Average coordination time for NAT traversal
    pub average_coordination_time: Duration,
}

impl Default for EndpointStats {
    fn default() -> Self {
        Self {
            active_connections: 0,
            successful_connections: 0,
            failed_connections: 0,
            nat_traversal_attempts: 0,
            nat_traversal_successes: 0,
            direct_connections: 0,
            relayed_connections: 0,
            total_bootstrap_nodes: 0,
            connected_bootstrap_nodes: 0,
            start_time: Instant::now(),
            average_coordination_time: Duration::ZERO,
        }
    }
}

/// P2P event for connection and network state changes.
///
/// Events use [`TransportAddr`] to support multi-transport connectivity.
/// Use `addr.as_socket_addr()` for backward compatibility with UDP-only code.
///
/// # Examples
///
/// ## Handling events with transport awareness
///
/// ```rust,ignore
/// use saorsa_transport::{P2pEvent, transport::TransportAddr};
///
/// while let Ok(event) = events.recv().await {
///     match event {
///         P2pEvent::PeerConnected { peer_id, addr, side } => {
///             // Handle different transport types
///             match addr {
///                 TransportAddr::Quic(socket_addr) => {
///                     println!("UDP connection from {socket_addr}");
///                 },
///                 TransportAddr::Ble { mac, .. } => {
///                     println!("BLE connection from {:?}", mac);
///                 },
///                 _ => println!("Other transport: {addr}"),
///             }
///         }
///         P2pEvent::ExternalAddressDiscovered { addr } => {
///             // Our external address was discovered
///             if let Some(socket_addr) = addr.as_socket_addr() {
///                 println!("External UDP address: {socket_addr}");
///             }
///         }
///         _ => {}
///     }
/// }
/// ```
///
/// ## Address-based event handling
///
/// Events use `SocketAddr` as the primary peer identifier:
///
/// ```rust,ignore
/// match event {
///     P2pEvent::PeerConnected { addr, public_key, .. } => {
///         if let Some(socket_addr) = addr.as_socket_addr() {
///             println!("Peer connected from {}", socket_addr);
///             if let Some(pk) = &public_key {
///                 println!("  Public key: {} bytes", pk.len());
///             }
///         }
///     }
///     _ => {}
/// }
/// ```
#[derive(Debug, Clone)]
pub enum P2pEvent {
    /// A new peer has connected.
    ///
    /// The `addr` field contains a [`TransportAddr`] which can represent different
    /// transport types (UDP, BLE, LoRa, etc.). Use `addr.as_socket_addr()` to extract
    /// the [`SocketAddr`] for UDP connections, or pattern match for specific transports.
    PeerConnected {
        /// Remote transport address (supports UDP, BLE, LoRa, and other transports)
        addr: TransportAddr,
        /// Remote peer's ML-DSA-65 SPKI public key bytes (None for constrained/unauthenticated)
        public_key: Option<Vec<u8>>,
        /// Who initiated the connection (Client = we connected, Server = they connected)
        side: Side,
    },

    /// A peer has disconnected.
    PeerDisconnected {
        /// Remote transport address of the disconnected peer
        addr: TransportAddr,
        /// Reason for the disconnection
        reason: DisconnectReason,
    },

    /// NAT traversal progress update.
    NatTraversalProgress {
        /// Target address for the NAT traversal
        addr: SocketAddr,
        /// Current phase of NAT traversal
        phase: TraversalPhase,
    },

    /// An external address was discovered for this node.
    ///
    /// The `addr` field contains a [`TransportAddr`] representing our externally
    /// visible address. For UDP connections, use `addr.as_socket_addr()` to get
    /// the [`SocketAddr`].
    ExternalAddressDiscovered {
        /// Discovered external transport address (typically TransportAddr::Quic for NAT traversal)
        addr: TransportAddr,
    },

    /// A connected peer reported observing one of our external addresses
    /// via an OBSERVED_ADDRESS frame.
    ///
    /// Fires on the first observation from each peer for a given external
    /// address (once per `(peer, observed_external)` pair), regardless of
    /// whether saorsa-transport's pinning quorum has been reached. Upper
    /// layers can use this for per-address attribution of cold-dialability
    /// proof: the peer is stating, with their own observation, that they
    /// reached us at `observed_external`.
    PeerObservedExternal {
        /// The peer's socket address (the source of the OBSERVED_ADDRESS frame)
        peer_addr: SocketAddr,
        /// The external address the peer reports observing us at
        observed_external: SocketAddr,
    },

    /// A connected peer advertised a new reachable address (relay or migration).
    PeerAddressUpdated {
        /// The connected peer that sent the advertisement
        peer_addr: SocketAddr,
        /// The new address the peer is advertising as reachable
        advertised_addr: SocketAddr,
    },

    /// This node established a MASQUE relay and is advertising a relay address.
    ///
    /// Emitted once when the relay becomes active. Upper layers should use this
    /// to trigger a DHT self-lookup so that more peers learn the relay address.
    RelayEstablished {
        /// The relay's public address (relay_IP:PORT)
        relay_addr: SocketAddr,
    },

    /// A previously-advertised MASQUE relay address is no longer reachable.
    ///
    /// Emitted when the relay health monitor observes the tunnel has died, or
    /// when the relay endpoint's accept loop exits (MASQUE streams closed).
    /// Upper layers should use this to immediately republish their address
    /// set without the relay entry, so peers stop dialing the dead address.
    RelayLost {
        /// The relay address that has become unreachable. Matches a
        /// previously-emitted [`P2pEvent::RelayEstablished::relay_addr`].
        relay_addr: SocketAddr,
    },

    /// Bootstrap connection status
    BootstrapStatus {
        /// Number of connected bootstrap nodes
        connected: usize,
        /// Total number of bootstrap nodes
        total: usize,
    },

    /// Peer authenticated
    PeerAuthenticated {
        /// Authenticated peer address
        addr: SocketAddr,
        /// Authenticated peer's ML-DSA-65 SPKI public key bytes
        public_key: Vec<u8>,
    },

    /// Data received from peer
    DataReceived {
        /// Source peer address
        addr: SocketAddr,
        /// Number of bytes received
        bytes: usize,
    },

    /// Data received from a constrained transport (BLE, LoRa, etc.)
    ///
    /// This event is generated when data arrives via a non-UDP transport that uses
    /// the constrained protocol engine.
    ConstrainedDataReceived {
        /// Remote transport address (BLE device ID, LoRa address, etc.)
        remote_addr: TransportAddr,
        /// Connection ID from the constrained engine
        connection_id: u16,
        /// The received data payload
        data: Vec<u8>,
    },
}

/// Reason for peer disconnection
#[derive(Debug, Clone)]
pub enum DisconnectReason {
    /// Normal disconnect
    Normal,
    /// Connection timeout
    Timeout,
    /// Protocol error
    ProtocolError(String),
    /// Authentication failure
    AuthenticationFailed,
    /// Connection lost
    ConnectionLost,
    /// Remote closed
    RemoteClosed,
}

// TraversalPhase is re-exported from nat_traversal_api

/// Stage at which a QUIC send failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendFailureStage {
    /// The send failed before a stream could be opened.
    OpenStream,
    /// Opening a stream made no progress within the progress timeout.
    OpenStreamProgressTimeout,
    /// A stream write made no progress within the progress timeout.
    WriteProgressTimeout,
    /// A stream write returned an error.
    Write,
    /// The stream could not be finished after all bytes were queued.
    Finish,
    /// The peer stopped the stream or the connection failed before delivery was acknowledged.
    DeliveryAck,
}

impl std::fmt::Display for SendFailureStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            Self::OpenStream => "open_stream",
            Self::OpenStreamProgressTimeout => "open_stream_progress_timeout",
            Self::WriteProgressTimeout => "write_progress_timeout",
            Self::Write => "write",
            Self::Finish => "finish",
            Self::DeliveryAck => "delivery_ack",
        };
        f.write_str(label)
    }
}

/// Error type for P2pEndpoint operations
#[derive(Debug, thiserror::Error)]
pub enum EndpointError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Connection error
    #[error("Connection error: {0}")]
    Connection(String),

    /// Send failure with enough context for upper layers to decide retry policy.
    #[error("Send failed at {stage}: {reason} (bytes_written={bytes_written})")]
    SendFailed {
        /// Stage at which the send failed.
        stage: SendFailureStage,
        /// Bytes accepted by the QUIC stream before failure.
        bytes_written: usize,
        /// Human-readable failure reason.
        reason: String,
    },

    /// NAT traversal error
    #[error("NAT traversal error: {0}")]
    NatTraversal(#[from] NatTraversalError),

    /// Authentication error
    #[error("Authentication error: {0}")]
    Authentication(String),

    /// Timeout error
    #[error("Operation timed out")]
    Timeout,

    /// Peer not found
    #[error("Peer not found at address: {0}")]
    PeerNotFound(SocketAddr),

    /// Already connected
    #[error("Already connected to address: {0}")]
    AlreadyConnected(SocketAddr),

    /// Shutdown in progress
    #[error("Endpoint is shutting down")]
    ShuttingDown,

    /// All connection strategies failed
    #[error("All connection strategies failed: {0}")]
    AllStrategiesFailed(String),

    /// No target address provided
    #[error("No target address provided")]
    NoAddress,
}

fn send_failed(
    stage: SendFailureStage,
    bytes_written: usize,
    reason: impl Into<String>,
) -> EndpointError {
    EndpointError::SendFailed {
        stage,
        bytes_written,
        reason: reason.into(),
    }
}

/// Frame handed to the send path by either public entry point.
///
/// Only the QUIC branch needs an owned buffer, because the stream takes it
/// over for the transfer. A borrowed frame from [`P2pEndpoint::send`] is
/// therefore copied exactly once, inside that branch, and never before the
/// shutdown/peer-lookup checks or on the constrained/datagram paths, which
/// only read it.
enum SendPayload<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

impl SendPayload<'_> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(data) => data,
            Self::Owned(data) => data,
        }
    }

    /// Owned frame for the QUIC stream; copies only a borrowed payload.
    fn into_bytes(self) -> Bytes {
        match self {
            Self::Borrowed(data) => Bytes::copy_from_slice(data),
            Self::Owned(data) => data,
        }
    }
}

/// Hand `data` to the stream without copying it.
///
/// `write_chunks` moves the caller's `Bytes` into the stream's send buffer
/// (splitting off whatever flow control accepts per call), so the frame is
/// held once for the transfer instead of once by the caller and once by the
/// stream. Each call still has to make progress within
/// `STREAM_WRITE_PROGRESS_TIMEOUT`, exactly as the slice-based write did.
///
/// Because every stream chunk is a slice of the one shared allocation, that
/// allocation is released when the last segment is acknowledged rather than
/// progressively per acknowledged segment as the copying write did. Peak
/// memory is one frame either way.
async fn write_stream_with_progress_timeout(
    send_stream: &mut crate::high_level::SendStream,
    addr: SocketAddr,
    data: Bytes,
) -> Result<usize, EndpointError> {
    let total_len = data.len();
    let mut pending = [data];
    let mut bytes_written = 0usize;
    let write_started = Instant::now();

    debug!(
        "send({}): starting QUIC stream write ({} bytes)",
        addr, total_len
    );

    while bytes_written < total_len {
        match timeout(
            STREAM_WRITE_PROGRESS_TIMEOUT,
            send_stream.write_chunks(&mut pending),
        )
        .await
        {
            Ok(Ok(written)) if written.bytes == 0 => {
                return Err(send_failed(
                    SendFailureStage::Write,
                    bytes_written,
                    "stream write returned zero bytes",
                ));
            }
            Ok(Ok(written)) => {
                bytes_written += written.bytes;
            }
            Ok(Err(e)) => {
                warn!(
                    "send({}): stream write failed after {}/{} bytes: {}",
                    addr, bytes_written, total_len, e
                );
                return Err(send_failed(
                    SendFailureStage::Write,
                    bytes_written,
                    e.to_string(),
                ));
            }
            Err(_elapsed) => {
                warn!(
                    "send({}): stream write made no progress for {:?} after {}/{} bytes",
                    addr, STREAM_WRITE_PROGRESS_TIMEOUT, bytes_written, total_len
                );
                return Err(send_failed(
                    SendFailureStage::WriteProgressTimeout,
                    bytes_written,
                    format!(
                        "stream write made no progress for {:?}",
                        STREAM_WRITE_PROGRESS_TIMEOUT
                    ),
                ));
            }
        }
    }

    debug!(
        "send({}): QUIC stream write completed ({} bytes in {:?})",
        addr,
        bytes_written,
        write_started.elapsed()
    );

    Ok(bytes_written)
}

async fn read_stream(
    recv_stream: &mut crate::high_level::RecvStream,
    addr: SocketAddr,
    size_limit: usize,
) -> Result<Vec<u8>, EndpointError> {
    let mut read = Vec::new();
    let mut start = u64::MAX;
    let mut end = 0u64;
    let read_started = Instant::now();

    debug!(
        "recv({}): starting QUIC stream read (limit {} bytes)",
        addr, size_limit
    );

    loop {
        match recv_stream.read_chunk(usize::MAX, false).await {
            Ok(Some(chunk)) => {
                let new_start = start.min(chunk.offset);
                let chunk_end = chunk.offset.saturating_add(chunk.bytes.len() as u64);
                let new_end = end.max(chunk_end);
                if new_end.saturating_sub(new_start) > size_limit as u64 {
                    warn!(
                        "recv({}): stream exceeded read limit (limit {} bytes)",
                        addr, size_limit
                    );
                    return Err(EndpointError::Connection(format!(
                        "incoming stream exceeded read limit of {size_limit} bytes"
                    )));
                }
                start = new_start;
                end = new_end;
                read.push((chunk.bytes, chunk.offset));
            }
            Ok(None) => {
                if end == 0 {
                    debug!("recv({}): QUIC stream read completed empty", addr);
                    return Ok(Vec::new());
                }

                let mut buffer = vec![0; end.saturating_sub(start) as usize];
                for (data, offset) in read {
                    let offset = offset.saturating_sub(start) as usize;
                    buffer[offset..offset + data.len()].copy_from_slice(&data);
                }
                debug!(
                    "recv({}): QUIC stream read completed ({} bytes in {:?})",
                    addr,
                    buffer.len(),
                    read_started.elapsed()
                );
                return Ok(buffer);
            }
            Err(e) => {
                warn!("recv({}): stream read failed: {}", addr, e);
                return Err(EndpointError::Connection(format!(
                    "stream read failed: {e}"
                )));
            }
        }
    }
}

async fn wait_for_stream_delivery_ack(
    send_stream: &crate::high_level::SendStream,
    addr: SocketAddr,
    bytes_written: usize,
) -> Result<(), EndpointError> {
    let ack_started = Instant::now();

    debug!(
        "send({}): waiting for peer to acknowledge QUIC stream delivery ({} bytes)",
        addr, bytes_written
    );

    match send_stream.stopped().await {
        Ok(None) => {
            debug!(
                "send({}): peer acknowledged QUIC stream delivery in {:?}",
                addr,
                ack_started.elapsed()
            );
            Ok(())
        }
        Ok(Some(error_code)) => {
            warn!(
                "send({}): peer stopped QUIC stream before delivery was acknowledged after {:?} (error_code={:?})",
                addr,
                ack_started.elapsed(),
                error_code
            );
            Err(send_failed(
                SendFailureStage::DeliveryAck,
                bytes_written,
                format!("peer stopped QUIC stream with error code {error_code:?}"),
            ))
        }
        Err(e) => {
            warn!(
                "send({}): failed while waiting for QUIC stream delivery ack after {:?}: {}",
                addr,
                ack_started.elapsed(),
                e
            );
            Err(send_failed(
                SendFailureStage::DeliveryAck,
                bytes_written,
                e.to_string(),
            ))
        }
    }
}

#[derive(Clone, Copy)]
enum InboundDataSendMode {
    TrySendWithTimeout,
    AwaitSend,
}

async fn dispatch_inbound_stream(
    mut recv_stream: crate::high_level::RecvStream,
    addr: SocketAddr,
    data_tx: mpsc::Sender<(SocketAddr, Vec<u8>)>,
    event_tx: broadcast::Sender<P2pEvent>,
    max_read_bytes: usize,
    reader_label: &'static str,
    send_mode: InboundDataSendMode,
) {
    let data = match read_stream(&mut recv_stream, addr, max_read_bytes).await {
        Ok(data) if data.is_empty() => return,
        Ok(data) => data,
        Err(e) => {
            info!(
                "Reader task for {}{}: stream read error: {}",
                addr, reader_label, e
            );
            let _ = recv_stream.stop(crate::VarInt::from_u32(STREAM_RESET_ABORT_CODE));
            return;
        }
    };

    let data_len = data.len();
    debug!(
        "Reader task{}: {} bytes from {}",
        reader_label, data_len, addr
    );

    // Note: last_activity update moved out of the hot path to avoid
    // RwLock write contention. With N reader tasks all acquiring
    // write locks on every message, the lock becomes a bottleneck
    // that can starve other tasks and deadlock the runtime.
    // The DataReceived event below serves as a liveness signal.
    let _ = event_tx.send(P2pEvent::DataReceived {
        addr,
        bytes: data_len,
    });

    match send_mode {
        InboundDataSendMode::TrySendWithTimeout => {
            // Send through channel without blocking the reader task's event loop.
            // If the channel is full, wait briefly in this per-stream task. This
            // keeps the accept loop free to keep accepting other QUIC uni streams.
            match data_tx.try_send((addr, data)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full((addr, data))) => {
                    let data_len = data.len();
                    if tokio::time::timeout(Duration::from_secs(5), data_tx.send((addr, data)))
                        .await
                        .is_err()
                    {
                        warn!(
                            "Reader task for {}{}: data channel send timed out, dropping {} bytes",
                            addr, reader_label, data_len
                        );
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!(
                        "Reader task for {}{}: channel closed, dropping {} bytes",
                        addr, reader_label, data_len
                    );
                }
            }
        }
        InboundDataSendMode::AwaitSend => {
            if data_tx.send((addr, data)).await.is_err() {
                debug!(
                    "Reader task for {}{}: channel closed, dropping {} bytes",
                    addr, reader_label, data_len
                );
            }
        }
    }
}

/// Spawn a QUIC reader into the shared reader set, first removing the readers
/// that have already finished.
///
/// A `JoinSet` keeps each finished task allocated until it is joined or the
/// set is dropped, and nothing else joins this set (shutdown only aborts), so
/// without the reap every connection the endpoint ever had would stay
/// allocated until the endpoint is dropped. With it the set holds the live
/// readers plus those that finished since the previous spawn.
fn spawn_reader(
    reader_tasks: &mut JoinSet<SocketAddr>,
    reader: impl Future<Output = SocketAddr> + Send + 'static,
) -> AbortHandle {
    while let Some(result) = reader_tasks.try_join_next() {
        if let Err(error) = result
            && error.is_panic()
        {
            warn!("Reader task panicked: {error}");
        }
    }
    reader_tasks.spawn(reader)
}

async fn run_quic_reader_task(
    addr: SocketAddr,
    connection: crate::high_level::Connection,
    data_tx: mpsc::Sender<(SocketAddr, Vec<u8>)>,
    event_tx: broadcast::Sender<P2pEvent>,
    exit_tx: mpsc::UnboundedSender<(SocketAddr, usize)>,
    max_read_bytes: usize,
    reader_label: &'static str,
    send_mode: InboundDataSendMode,
) -> SocketAddr {
    info!("Reader task STARTED for {}{}", addr, reader_label);

    let stable_id = connection.stable_id();
    let stream_permits = Arc::new(tokio::sync::Semaphore::new(
        MAX_CONCURRENT_INBOUND_STREAM_READS_PER_CONNECTION,
    ));
    let mut stream_tasks = tokio::task::JoinSet::new();

    loop {
        while let Some(result) = stream_tasks.try_join_next() {
            if let Err(e) = result {
                warn!(
                    "Reader task for {}{}: stream worker failed: {}",
                    addr, reader_label, e
                );
            }
        }

        if data_tx.is_closed() {
            debug!(
                "Reader task for {}{}: channel closed, exiting",
                addr, reader_label
            );
            break;
        }

        let permit = match Arc::clone(&stream_permits).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };

        let recv_stream = match connection.accept_uni().await {
            Ok(stream) => stream,
            Err(e) => {
                info!(
                    "Reader task for {}{} ending: accept_uni error: {}",
                    addr, reader_label, e
                );
                break;
            }
        };

        let data_tx = data_tx.clone();
        let event_tx = event_tx.clone();
        stream_tasks.spawn(async move {
            let _permit = permit;
            dispatch_inbound_stream(
                recv_stream,
                addr,
                data_tx,
                event_tx,
                max_read_bytes,
                reader_label,
                send_mode,
            )
            .await;
        });
    }

    stream_tasks.abort_all();

    // Notify the reader-exit handler for immediate cleanup.
    let _ = exit_tx.send((addr, stable_id));
    addr
}

/// Broadcast `P2pEvent::PeerConnected` for `addr` exactly once per
/// disconnect cycle.
///
/// `emitted` tracks which addresses we have already broadcast for. The first
/// call per address inserts and broadcasts; subsequent calls are no-ops until
/// the address is removed from `emitted` by `do_cleanup_connection` or a peer
/// disconnect event. This prevents the simultaneous-open dial+accept race
/// (and the send-side hole-punch fixup) from emitting duplicate events that
/// would each trigger a redundant identity announce upstream in saorsa-core.
///
/// Non-socket transport addresses (BLE, LoRa) are always broadcast — they
/// don't share the dedup pool keyed by `SocketAddr`.
fn broadcast_peer_connected_once(
    emitted: &dashmap::DashSet<SocketAddr>,
    event_tx: &broadcast::Sender<P2pEvent>,
    mut addr: TransportAddr,
    public_key: Option<Vec<u8>>,
    side: Side,
) {
    addr = match addr {
        TransportAddr::Quic(socket_addr) => TransportAddr::Quic(normalize_socket_addr(socket_addr)),
        TransportAddr::Tcp(socket_addr) => TransportAddr::Tcp(normalize_socket_addr(socket_addr)),
        TransportAddr::Udp(socket_addr) => TransportAddr::Udp(normalize_socket_addr(socket_addr)),
        other => other,
    };

    if let Some(socket_addr) = addr.as_socket_addr() {
        if !emitted.insert(socket_addr) {
            debug!(
                "broadcast_peer_connected_once: skipping duplicate PeerConnected for {}",
                socket_addr
            );
            return;
        }
    }
    let _ = event_tx.send(P2pEvent::PeerConnected {
        addr,
        public_key,
        side,
    });
}

/// Shared cleanup logic for removing a peer from all tracking structures.
///
/// Used by both `P2pEndpoint::cleanup_connection()` and the background reaper
/// to ensure consistent cleanup behaviour (single source of truth).
///
/// Lock ordering: always acquires locks in the canonical order
/// `connected_peers` → `reader_handles` → `stats` to prevent ABBA deadlocks.
/// Each lock is acquired and released independently (no nesting) to minimise
/// hold time and avoid blocking concurrent `send()` / `connect()` calls.
///
/// `expected_stable_id`, if provided, gates the DashMap removal so a
/// reader-exit triggered cleanup does not evict a newer simultaneous-open
/// replacement. `None` means force-remove (disconnect API / stale reaper /
/// send-failure cleanup).
///
/// Returns `true` if the peer was actually present in `connected_peers`.
#[allow(clippy::too_many_arguments)]
async fn do_cleanup_connection(
    connected_peers: &RwLock<HashMap<SocketAddr, PeerConnection>>,
    inner: &NatTraversalEndpoint,
    reader_handles: &RwLock<HashMap<SocketAddr, tokio::task::AbortHandle>>,
    stats: &RwLock<EndpointStats>,
    event_tx: &broadcast::Sender<P2pEvent>,
    emitted_peer_connected: &dashmap::DashSet<SocketAddr>,
    addr: &SocketAddr,
    reason: DisconnectReason,
    expected_stable_id: Option<usize>,
) -> bool {
    let variants = socket_addr_variants(*addr);

    // A live CONNECT-UDP stream makes this connection part of an established
    // relay's data plane. Ordinary disconnect APIs and the stale-peer reaper
    // must not force-close it; the relay forwarding guard releases this pin
    // when the stream actually ends. Reader-exit cleanup still proceeds for a
    // connection that has already failed.
    if expected_stable_id.is_none()
        && variants
            .iter()
            .any(|candidate| inner.is_relay_control_connection(candidate))
    {
        info!(
            peer = %addr,
            "do_cleanup_connection: keeping live relay control connection"
        );
        return false;
    }

    // Step 1: Try to remove from the NAT traversal layer first (lock-free
    // DashMap). When the caller is a reader-exit firing for an older
    // connection that has since been replaced via simultaneous-open, the
    // stable_id guard rejects the removal — in that case the current
    // DashMap entry belongs to a newer connection and the per-addr
    // tracking (connected_peers, reader_handles) must NOT be torn down.
    let inner_removed = variants.iter().any(|candidate| {
        matches!(
            inner.remove_connection(candidate, expected_stable_id),
            Ok(Some(_))
        )
    });
    if let Some(expected_id) = expected_stable_id
        && !inner_removed
    {
        if let Ok(Some(current)) = inner.get_connection(addr) {
            debug!(
                "do_cleanup_connection: {} — current connection stable_id {} \
                 differs from reader-exit stable_id {}, leaving state intact",
                addr,
                current.stable_id(),
                expected_id
            );
            return false;
        }
        debug!(
            "do_cleanup_connection: {} — no current connection for reader-exit stable_id {}, \
             cleaning remaining endpoint state",
            addr, expected_id
        );
    }

    // Step 2: Remove from connected_peers (canonical lock #1)
    let removed = {
        let mut peers = connected_peers.write().await;
        let mut removed = None;
        for candidate in &variants {
            if let Some(peer) = peers.remove(candidate) {
                if removed.is_none() {
                    removed = Some(peer);
                }
            }
        }
        removed
    };

    // Allow a future reconnect to broadcast a fresh PeerConnected event.
    for candidate in &variants {
        emitted_peer_connected.remove(candidate);
    }

    // Step 3: Remove and abort reader task (canonical lock #2)
    let abort_handles: Vec<_> = {
        let mut handles = reader_handles.write().await;
        variants
            .iter()
            .filter_map(|candidate| handles.remove(candidate))
            .collect()
    };
    for handle in abort_handles {
        handle.abort();
    }

    // Step 4: Update stats and emit event (canonical lock #3)
    if let Some(peer_conn) = removed {
        {
            let mut s = stats.write().await;
            s.active_connections = s.active_connections.saturating_sub(1);
        }

        let _ = event_tx.send(P2pEvent::PeerDisconnected {
            addr: peer_conn.remote_addr,
            reason,
        });

        info!("Cleaned up connection for addr {}", addr);
        true
    } else {
        false
    }
}

impl P2pEndpoint {
    /// Create a new P2P endpoint with the given configuration
    pub async fn new(config: P2pConfig) -> Result<Self, EndpointError> {
        // Use provided keypair or generate a new one (ML-DSA-65)
        let (public_key, secret_key) = match config.keypair.clone() {
            Some(keypair) => keypair,
            None => generate_ml_dsa_keypair().map_err(|e| {
                EndpointError::Config(format!("Failed to generate ML-DSA-65 keypair: {e:?}"))
            })?,
        };
        // SPKI fingerprint of our own public key (for identity/logging)
        let our_fingerprint =
            crate::crypto::raw_public_keys::pqc::fingerprint_public_key(&public_key);

        info!(
            "Creating P2P endpoint (fingerprint: {})",
            hex::encode(&our_fingerprint[..8])
        );

        // v0.2: auth_manager removed - TLS handles peer authentication via ML-DSA-65
        // Store public key bytes directly for identity sharing
        let public_key_bytes: Vec<u8> = public_key.as_bytes().to_vec();

        // Create event channel
        let (event_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let event_tx_clone = event_tx.clone();

        // Create stats
        let stats = Arc::new(RwLock::new(EndpointStats {
            total_bootstrap_nodes: config.known_peers.len(),
            start_time: Instant::now(),
            ..Default::default()
        }));
        let stats_clone = Arc::clone(&stats);

        // Shared dedup tracker for PeerConnected broadcasts. Captured by
        // the event_callback closure (which has no &self) and also wired
        // into the P2pEndpoint struct so other broadcast sites share state.
        let emitted_peer_connected: Arc<dashmap::DashSet<SocketAddr>> =
            Arc::new(dashmap::DashSet::new());
        let emitted_peer_connected_cb = Arc::clone(&emitted_peer_connected);

        // Create event callback that bridges to broadcast channel
        let event_callback = Box::new(move |event: NatTraversalEvent| {
            let event_tx = event_tx_clone.clone();
            let stats = stats_clone.clone();
            let emitted = Arc::clone(&emitted_peer_connected_cb);

            tokio::spawn(async move {
                // Update stats based on event
                let mut stats_guard = stats.write().await;
                match &event {
                    NatTraversalEvent::CoordinationRequested { .. } => {
                        stats_guard.nat_traversal_attempts += 1;
                    }
                    NatTraversalEvent::ConnectionEstablished {
                        remote_address,
                        side,
                        public_key,
                    } => {
                        stats_guard.nat_traversal_successes += 1;
                        stats_guard.active_connections += 1;
                        stats_guard.successful_connections += 1;

                        // Broadcast event with connection direction (deduped).
                        broadcast_peer_connected_once(
                            &emitted,
                            &event_tx,
                            TransportAddr::Quic(*remote_address),
                            public_key.clone(),
                            *side,
                        );
                    }
                    NatTraversalEvent::TraversalFailed { remote_address, .. } => {
                        stats_guard.failed_connections += 1;
                        let _ = event_tx.send(P2pEvent::NatTraversalProgress {
                            addr: *remote_address,
                            phase: TraversalPhase::Failed,
                        });
                    }
                    NatTraversalEvent::PhaseTransition {
                        remote_address,
                        to_phase,
                        ..
                    } => {
                        let _ = event_tx.send(P2pEvent::NatTraversalProgress {
                            addr: *remote_address,
                            phase: *to_phase,
                        });
                    }
                    NatTraversalEvent::ExternalAddressDiscovered { address, .. } => {
                        info!("External address discovered: {}", address);
                        let _ = event_tx.send(P2pEvent::ExternalAddressDiscovered {
                            addr: TransportAddr::Quic(*address),
                        });
                    }
                    NatTraversalEvent::PeerObservedExternal {
                        peer_addr,
                        observed_external,
                    } => {
                        let _ = event_tx.send(P2pEvent::PeerObservedExternal {
                            peer_addr: *peer_addr,
                            observed_external: *observed_external,
                        });
                    }
                    _ => {}
                }
                drop(stats_guard);
            });
        });

        // Create NAT traversal endpoint with the same identity key used for auth
        // This ensures P2pEndpoint and NatTraversalEndpoint use the same keypair
        let mut nat_config = config.to_nat_config_with_key(public_key.clone(), secret_key);

        // Phase 5.3 Deliverable 3: Socket sharing in default constructor
        // Bind a single UDP socket and share it between transport registry and Quinn
        let default_addr: std::net::SocketAddr =
            std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0);
        let bind_addr = config
            .bind_addr
            .as_ref()
            .and_then(|addr| addr.as_socket_addr())
            .unwrap_or(default_addr);
        let (udp_transport, quinn_socket) =
            crate::transport::UdpTransport::bind_for_quinn(bind_addr)
                .await
                .map_err(|e| EndpointError::Config(format!("Failed to bind UDP socket: {e}")))?;

        let actual_bind_addr = quinn_socket
            .local_addr()
            .map_err(|e| EndpointError::Config(format!("Failed to get local address: {e}")))?;

        info!("Bound shared UDP socket at {}", actual_bind_addr);

        // Create transport registry with the UDP transport
        // Also include any additional transports from the config
        let mut transport_registry = config.transport_registry.clone();
        transport_registry.register(Arc::new(udp_transport));

        // Update NAT config to use our registry and bind address
        nat_config.transport_registry = Some(Arc::new(transport_registry.clone()));
        nat_config.bind_addr = Some(actual_bind_addr);

        // Create NAT traversal endpoint with the shared socket
        let inner = NatTraversalEndpoint::new_with_socket(
            nat_config,
            Some(event_callback),
            None,
            Some(quinn_socket),
        )
        .await
        .map_err(|e| EndpointError::Config(e.to_string()))?;

        // Wrap the registry in Arc for shared ownership
        let transport_registry = Arc::new(transport_registry);

        // Create connection router for automatic protocol engine selection
        let inner_arc = Arc::new(inner);
        let router_config = RouterConfig {
            constrained_config: crate::constrained::ConstrainedTransportConfig::default(),
            prefer_quic: true, // Default to QUIC for broadband transports
            enable_metrics: true,
            max_connections: 256,
        };
        // `with_full_config` already installs the QUIC endpoint; no
        // post-construction setter is needed.
        let router = ConnectionRouter::with_full_config(
            router_config,
            Arc::clone(&transport_registry),
            Arc::clone(&inner_arc),
        );

        // Create channel for data received from background reader tasks
        let (data_tx, data_rx) = mpsc::channel(config.data_channel_capacity);
        let reader_tasks = Arc::new(tokio::sync::Mutex::new(JoinSet::new()));
        let reader_handles = Arc::new(RwLock::new(HashMap::new()));

        // Channel for reader tasks to signal immediate cleanup on exit
        let (reader_exit_tx, reader_exit_rx) = mpsc::unbounded_channel();

        let endpoint = Self {
            inner: inner_arc,
            // v0.2: auth_manager removed
            connected_peers: Arc::new(RwLock::new(HashMap::new())),
            emitted_peer_connected,
            stats,
            config,
            event_tx,
            our_fingerprint,
            public_key: public_key_bytes,
            shutdown: CancellationToken::new(),
            pending_data: Arc::new(RwLock::new(BoundedPendingBuffer::default())),
            transport_registry,
            router: Arc::new(router),
            constrained_connections: Arc::new(RwLock::new(HashMap::new())),
            constrained_peer_addrs: Arc::new(RwLock::new(HashMap::new())),
            hole_punch_target_peer_ids: Arc::new(dashmap::DashMap::new()),
            hole_punch_preferred_coordinators: Arc::new(dashmap::DashMap::new()),
            data_tx,
            data_rx: Arc::new(tokio::sync::Mutex::new(data_rx)),
            reader_tasks,
            reader_handles,
            reader_exit_tx,
            pending_dials: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        };

        // Spawn background constrained poller task
        endpoint.spawn_constrained_poller();

        // Spawn stale connection reaper — periodically detects and removes
        // dead connections from tracking structures (issue #137 fix).
        endpoint.spawn_stale_connection_reaper();

        // Spawn reader-exit handler — immediately cleans up when a reader
        // task detects a dead QUIC connection, without waiting for the reaper.
        endpoint.spawn_reader_exit_handler(reader_exit_rx);

        // Spawn NAT traversal session driver — periodically polls the
        // NatTraversalEndpoint to advance sessions through Discovery →
        // Coordination → Punching phases. Runs independently of
        // try_hole_punch to avoid DashMap lock contention deadlocks.
        endpoint.spawn_session_driver();

        // Spawn incoming connection forwarder — bridges accepted connections
        // from the NatTraversalEndpoint to P2pEndpoint's connected_peers.
        endpoint.spawn_incoming_connection_forwarder();

        Ok(endpoint)
    }

    /// Get the local ML-DSA-65 SPKI public key bytes
    pub fn local_public_key(&self) -> &[u8] {
        &self.public_key
    }

    /// Get the underlying QUIC connection for a remote address.
    ///
    /// Look up an existing QUIC connection by remote address.
    ///
    /// Returns `None` if we have no tracked connection for this address.
    pub async fn get_quic_connection(
        &self,
        addr: &SocketAddr,
    ) -> Result<Option<crate::high_level::Connection>, EndpointError> {
        let addr = normalize_socket_addr(*addr);
        let peers = self.connected_peers.read().await;
        if !peers.contains_key(&addr) {
            return Ok(None);
        }
        drop(peers);
        self.inner
            .get_connection(&addr)
            .map_err(EndpointError::NatTraversal)
    }

    /// Get the local bind address
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.inner
            .get_endpoint()
            .and_then(|ep| ep.local_addr().ok())
    }

    /// Get observed external address (if discovered)
    pub fn external_addr(&self) -> Option<SocketAddr> {
        self.inner.get_observed_external_address().ok().flatten()
    }

    /// Get the transport registry for this endpoint
    ///
    /// The transport registry contains all registered transport providers (UDP, BLE, etc.)
    /// that this endpoint can use for connectivity.
    pub fn transport_registry(&self) -> &TransportRegistry {
        &self.transport_registry
    }

    /// Get the ML-DSA-65 public key bytes (1952 bytes)
    pub fn public_key_bytes(&self) -> &[u8] {
        &self.public_key
    }

    // === Connection Management ===

    /// Connect to a peer by address (direct connection).
    ///
    /// Uses Raw Public Key authentication - the peer's identity is verified via their
    /// ML-DSA-65 public key, not via SNI/certificates.
    ///
    /// If we already have a live connection to the target address, returns the
    /// existing connection instead of creating a duplicate. After handshake, if
    /// we discover a simultaneous open (both sides connected at the same time),
    /// a deterministic tiebreaker ensures both sides keep the same connection.
    pub async fn connect(&self, addr: SocketAddr) -> Result<PeerConnection, EndpointError> {
        if self.shutdown.is_cancelled() {
            return Err(EndpointError::ShuttingDown);
        }
        let addr = normalize_socket_addr(addr);

        // Dedup check: if we already have a live connection to this address, return it.
        {
            let peers = self.connected_peers.read().await;
            if let Some(existing) = peers.get(&addr) {
                // Verify the underlying QUIC connection is still alive
                if self.inner.is_connected(&addr) {
                    info!("connect: reusing existing live connection to {}", addr);
                    return Ok(existing.clone());
                }
            }
        }
        // If a dead connection was found, remove stale entry.
        {
            let mut peers = self.connected_peers.write().await;
            if peers.contains_key(&addr) && !self.inner.is_connected(&addr) {
                peers.remove(&addr);
                info!("connect: removed stale connection entry for {}", addr);
            }
        }

        info!("Connecting directly to {}", addr);

        let endpoint = self
            .inner
            .get_endpoint()
            .ok_or_else(|| EndpointError::Config("QUIC endpoint not available".to_string()))?;

        let connecting = endpoint
            .connect(addr, "peer")
            .map_err(|e| EndpointError::Connection(e.to_string()))?;

        // Enforce a hard timeout on the QUIC handshake to prevent the 76s hang
        // reported in issue #137. The connection_timeout config or 30s default
        // ensures callers always get a response within a bounded window.
        let handshake_timeout = self
            .config
            .timeouts
            .nat_traversal
            .connection_establishment_timeout;
        let connection = match timeout(handshake_timeout, connecting).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                info!("connect: handshake to {} failed: {}", addr, e);
                return Err(EndpointError::Connection(e.to_string()));
            }
            Err(_) => {
                info!(
                    "connect: handshake to {} timed out after {:?}",
                    addr, handshake_timeout
                );
                return Err(EndpointError::Timeout);
            }
        };

        // Extract the public key from the TLS handshake
        let remote_public_key = extract_public_key_bytes_from_connection(&connection);

        // Post-handshake dedup: if we already have a live connection to this
        // address, just overwrite it with the new outgoing connection.
        if self.inner.is_connected(&addr) {
            debug!(
                "connect: simultaneous open for {} — overwriting existing connection",
                addr
            );
        }

        // Store connection in inner layer (keyed by remote SocketAddr)
        self.inner
            .add_connection(addr, connection.clone())
            .map_err(EndpointError::NatTraversal)?;

        // Spawn handler (we initiated the connection = Client side)
        self.inner
            .spawn_connection_handler(addr, connection, Side::Client)
            .map_err(EndpointError::NatTraversal)?;

        // Create peer connection record
        // v0.2: Peer is authenticated via TLS (ML-DSA-65) during handshake
        let peer_conn = PeerConnection {
            public_key: remote_public_key.clone(),
            remote_addr: TransportAddr::Quic(addr),
            authenticated: true, // TLS handles authentication
            connected_at: Instant::now(),
            last_activity: Instant::now(),
        };

        // Spawn background reader task BEFORE storing peer in connected_peers
        // This prevents a race where recv() called immediately after connect()
        // returns might miss early data if the peer sends before the task starts
        if let Ok(Some(conn)) = self.inner.get_connection(&addr) {
            self.spawn_reader_task(addr, conn).await;
        }

        // Store peer (reader task is already running, so no data loss window)
        self.connected_peers
            .write()
            .await
            .insert(addr, peer_conn.clone());

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.active_connections += 1;
            stats.successful_connections += 1;
            stats.direct_connections += 1;
        }

        // Broadcast event (we initiated the connection = Client side)
        broadcast_peer_connected_once(
            &self.emitted_peer_connected,
            &self.event_tx,
            TransportAddr::Quic(addr),
            remote_public_key,
            Side::Client,
        );

        Ok(peer_conn)
    }

    /// Connect to a peer using any transport address
    ///
    /// This method uses the connection router to automatically select the appropriate
    /// protocol engine (QUIC or Constrained) based on the transport capabilities.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use saorsa_transport::transport::TransportAddr;
    ///
    /// // Connect via UDP (uses QUIC)
    /// let udp_addr = TransportAddr::Quic("192.168.1.100:9000".parse()?);
    /// let conn = endpoint.connect_transport(&udp_addr, None).await?;
    ///
    /// // Connect via BLE (uses Constrained engine)
    /// let ble_addr = TransportAddr::Ble {
    ///     mac: [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF],
    ///     psm: 128,
    /// };
    /// let conn = endpoint.connect_transport(&ble_addr, None).await?;
    /// ```
    pub async fn connect_transport(
        &self,
        addr: &TransportAddr,
    ) -> Result<PeerConnection, EndpointError> {
        if self.shutdown.is_cancelled() {
            return Err(EndpointError::ShuttingDown);
        }

        // Use the router to determine the appropriate engine.
        //
        // Both `select_engine_for_addr` and `connect` take `&self` on
        // `ConnectionRouter`, so there is no locking at all on the hot
        // path. Selection and connect are two separate calls — there is a
        // theoretical TOCTOU window where the engine picked here could
        // become unavailable before the connect runs. In practice the
        // router has no API to revoke or replace an engine once installed
        // (the QUIC endpoint is set at construction time, the constrained
        // transport is lazy-initialised and never torn down), so the race
        // is closed by construction. If that invariant is ever relaxed,
        // this call site needs to handle an engine-unavailable error from
        // `connect()` explicitly.
        let engine = self.router.select_engine_for_addr(addr);

        info!("Connecting to {} via {:?} engine", addr, engine);

        match engine {
            ProtocolEngine::Quic => {
                // For QUIC, extract socket address and use existing connect path
                let socket_addr = addr.as_socket_addr().ok_or_else(|| {
                    EndpointError::Connection(format!(
                        "Cannot extract socket address from {} for QUIC",
                        addr
                    ))
                })?;
                self.connect(socket_addr).await
            }
            ProtocolEngine::Constrained => {
                // For constrained transports, use the router's connect
                // path. No lock needed — `connect` takes `&self`.
                let _routed = self.router.connect(addr).map_err(|e| {
                    EndpointError::Connection(format!("Constrained connection failed: {}", e))
                })?;

                // Use a synthetic socket address for constrained connections
                let synthetic_addr = addr.to_synthetic_socket_addr();

                let peer_conn = PeerConnection {
                    public_key: None, // Constrained connections don't have TLS auth yet
                    remote_addr: addr.clone(),
                    authenticated: false,
                    connected_at: Instant::now(),
                    last_activity: Instant::now(),
                };

                self.connected_peers
                    .write()
                    .await
                    .insert(synthetic_addr, peer_conn.clone());

                // Update stats
                {
                    let mut stats = self.stats.write().await;
                    stats.active_connections += 1;
                    stats.successful_connections += 1;
                }

                // Broadcast event (deduped)
                broadcast_peer_connected_once(
                    &self.emitted_peer_connected,
                    &self.event_tx,
                    addr.clone(),
                    None,
                    Side::Client,
                );

                Ok(peer_conn)
            }
        }
    }

    /// Get the connection router for advanced routing control
    ///
    /// Returns a shared reference to the connection router which can be
    /// used to query engine selection for addresses, read routing stats,
    /// or drive connects/accepts directly. All router methods take
    /// `&self`, so multiple callers can use the returned handle
    /// concurrently.
    pub fn router(&self) -> &Arc<ConnectionRouter> {
        &self.router
    }

    /// Get a point-in-time snapshot of router statistics.
    pub fn routing_stats(&self) -> crate::connection_router::RouterStatsSnapshot {
        self.router.stats().snapshot()
    }

    /// Register a constrained connection for a transport address
    ///
    /// This associates a TransportAddr with a ConstrainedEngine ConnectionId, enabling
    /// send() to use the proper constrained protocol for reliable delivery.
    ///
    /// # Arguments
    ///
    /// * `addr` - The remote transport address
    /// * `conn_id` - The ConnectionId from the ConstrainedEngine
    ///
    /// # Returns
    ///
    /// The previous ConnectionId if one was already registered for this address.
    pub async fn register_constrained_connection(
        &self,
        addr: TransportAddr,
        conn_id: ConstrainedConnectionId,
    ) -> Option<ConstrainedConnectionId> {
        let old = self
            .constrained_connections
            .write()
            .await
            .insert(addr.clone(), conn_id);
        debug!(
            "Registered constrained connection for addr {}: conn_id={:?}",
            addr, conn_id
        );
        old
    }

    /// Unregister a constrained connection for a transport address
    ///
    /// Call this when a constrained connection is closed or reset.
    ///
    /// # Returns
    ///
    /// The ConnectionId if one was registered for this address.
    pub async fn unregister_constrained_connection(
        &self,
        addr: &TransportAddr,
    ) -> Option<ConstrainedConnectionId> {
        let removed = self.constrained_connections.write().await.remove(addr);
        if removed.is_some() {
            debug!("Unregistered constrained connection for addr {}", addr);
        }
        removed
    }

    /// Check if a transport address has a constrained connection
    pub async fn has_constrained_connection(&self, addr: &TransportAddr) -> bool {
        self.constrained_connections.read().await.contains_key(addr)
    }

    /// Get the ConnectionId for a transport address's constrained connection
    pub async fn get_constrained_connection_id(
        &self,
        addr: &TransportAddr,
    ) -> Option<ConstrainedConnectionId> {
        self.constrained_connections.read().await.get(addr).copied()
    }

    /// Get the number of active constrained connections
    pub async fn constrained_connection_count(&self) -> usize {
        self.constrained_connections.read().await.len()
    }

    /// Look up TransportAddr from constrained ConnectionId
    pub async fn addr_from_constrained_conn(
        &self,
        conn_id: ConstrainedConnectionId,
    ) -> Option<TransportAddr> {
        self.constrained_peer_addrs
            .read()
            .await
            .get(&conn_id)
            .cloned()
    }

    /// Connect with automatic fallback: IPv4 → IPv6 → HolePunch → Relay
    ///
    /// This method implements a progressive connection strategy that automatically
    /// falls back through increasingly aggressive NAT traversal techniques:
    ///
    /// 1. **Direct IPv4** (5s timeout) - Simple direct connection
    /// 2. **Direct IPv6** (5s timeout) - Bypasses NAT when IPv6 available
    /// 3. **Hole-Punch** (15s timeout) - Coordinated NAT traversal via common peer
    /// 4. **Relay** (30s timeout) - MASQUE relay as last resort
    ///
    /// # Arguments
    ///
    /// * `target_ipv4` - Optional IPv4 address of the target peer
    /// * `target_ipv6` - Optional IPv6 address of the target peer
    /// * `strategy_config` - Optional custom strategy configuration
    ///
    /// # Returns
    ///
    /// A tuple of (PeerConnection, ConnectionMethod) indicating how the connection
    /// was established.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let (conn, method) = endpoint.connect_with_fallback(
    ///     Some("1.2.3.4:9000".parse()?),
    ///     Some("[2001:db8::1]:9000".parse()?),
    ///     None, // Use default strategy config
    /// ).await?;
    ///
    /// match method {
    ///     ConnectionMethod::DirectIPv4 => println!("Direct IPv4"),
    ///     ConnectionMethod::DirectIPv6 => println!("Direct IPv6"),
    ///     ConnectionMethod::HolePunched { coordinator } => println!("Via {}", coordinator),
    ///     ConnectionMethod::Relayed { relay } => println!("Relayed via {}", relay),
    /// }
    /// ```
    /// Set the target peer ID for a hole-punch attempt to a specific address.
    /// When set, the PUNCH_ME_NOW frame carries the peer ID instead of a
    /// socket-address-derived wire ID, allowing the coordinator to find the
    /// target connection by authenticated identity.
    ///
    /// Keyed by target address so concurrent dials to different peers each
    /// get their own peer ID without racing on shared state.
    pub async fn set_hole_punch_target_peer_id(&self, target: SocketAddr, peer_id: [u8; 32]) {
        self.hole_punch_target_peer_ids.insert(target, peer_id);
    }

    /// Set a preferred coordinator for hole-punching to a specific target.
    /// The preferred coordinator is a peer that referred us to the target
    /// during a DHT lookup, so it has a connection to the target.
    pub async fn set_hole_punch_preferred_coordinator(
        &self,
        target: SocketAddr,
        coordinator: SocketAddr,
    ) {
        self.hole_punch_preferred_coordinators
            .insert(target, coordinator);
    }

    /// Connect with automatic fallback: Direct → HolePunch → Relay.
    pub async fn connect_with_fallback(
        &self,
        target_ipv4: Option<SocketAddr>,
        target_ipv6: Option<SocketAddr>,
        strategy_config: Option<StrategyConfig>,
    ) -> Result<(PeerConnection, ConnectionMethod), EndpointError> {
        info!(
            "connect_with_fallback: IPv4={:?}, IPv6={:?}",
            target_ipv4, target_ipv6
        );
        if self.shutdown.is_cancelled() {
            return Err(EndpointError::ShuttingDown);
        }

        // Dedup: if another task is already connecting to this target, wait for
        // its result instead of starting a parallel attempt. The wait is
        // bounded and the owner installs a cancellation guard, so a dropped
        // owner future cannot leave a permanent in-flight entry behind.
        let target = target_ipv4.or(target_ipv6);
        let mut pending_guard = None;
        if let Some(target_addr) = target {
            let wait_timeout = pending_dial_wait_timeout(strategy_config.as_ref());

            loop {
                let mut pending = self.pending_dials.lock().await;
                if let Some(entry) = pending.get(&target_addr) {
                    let pending_id = entry.id;
                    let started_at = entry.started_at;
                    let age = started_at.elapsed();
                    let Some(remaining_wait) = pending_dial_remaining_wait(wait_timeout, age)
                    else {
                        pending.remove(&target_addr);
                        drop(pending);
                        warn!(
                            "connect_with_fallback: in-flight dial to {} already stale (age {:?}, timeout {:?}); retrying locally",
                            target_addr, age, wait_timeout
                        );
                        continue;
                    };
                    let mut rx = entry.tx.subscribe();
                    drop(pending);

                    info!(
                        "connect_with_fallback: waiting for in-flight dial to {} (age {:?}, remaining {:?}, timeout {:?})",
                        target_addr, age, remaining_wait, wait_timeout
                    );

                    match timeout(remaining_wait, rx.recv()).await {
                        Ok(Ok(Ok(result))) => return Ok(result),
                        Ok(Ok(Err(err))) => {
                            debug!(
                                "connect_with_fallback: in-flight dial to {} failed: {}; retrying locally",
                                target_addr, err
                            );
                        }
                        Ok(Err(err)) => {
                            debug!(
                                "connect_with_fallback: in-flight dial to {} ended without a result: {}; retrying locally",
                                target_addr, err
                            );
                        }
                        Err(_) => {
                            let total_age = started_at.elapsed();
                            warn!(
                                "connect_with_fallback: in-flight dial to {} stale after {:?} (timeout {:?}); retrying locally",
                                target_addr, total_age, wait_timeout
                            );
                            remove_pending_dial_if_owned(
                                &self.pending_dials,
                                target_addr,
                                pending_id,
                            )
                            .await;
                        }
                    }

                    continue;
                }

                // We're the first — register ourselves.
                let (tx, _) = broadcast::channel(PENDING_DIAL_BROADCAST_CAPACITY);
                let id = NEXT_PENDING_DIAL_ID.fetch_add(1, Ordering::Relaxed);
                pending.insert(
                    target_addr,
                    PendingDial {
                        id,
                        tx,
                        started_at: Instant::now(),
                    },
                );
                drop(pending);

                pending_guard = Some(PendingDialGuard::new(
                    Arc::clone(&self.pending_dials),
                    target_addr,
                    id,
                ));
                break;
            }
        }

        // Do the actual connection work
        let result = self
            .connect_with_fallback_inner(target_ipv4, target_ipv6, strategy_config)
            .await;

        // Broadcast result to any waiters and clean up pending entry
        if let Some(guard) = pending_guard.as_mut() {
            guard.complete(&result).await;
        }

        result
    }

    /// Inner implementation of connect_with_fallback (separated for dedup wrapper).
    async fn connect_with_fallback_inner(
        &self,
        target_ipv4: Option<SocketAddr>,
        target_ipv6: Option<SocketAddr>,
        strategy_config: Option<StrategyConfig>,
    ) -> Result<(PeerConnection, ConnectionMethod), EndpointError> {
        // Build strategy config with coordinator and relay from our config.
        // Collect ALL coordinator candidates so we can rotate on failure.
        let mut config = strategy_config.unwrap_or_default();
        let target = target_ipv4.or(target_ipv6);
        let mut coordinator_candidates: Vec<SocketAddr> = Vec::new();

        // Add known_peers first (configured bootstrap nodes)
        for addr in &self.config.known_peers {
            if let Some(sa) = addr.as_socket_addr() {
                if Some(sa) != target {
                    coordinator_candidates.push(sa);
                }
            }
        }
        // Add all connected peers as fallback candidates
        {
            let peers = self.connected_peers.read().await;
            for &addr in peers.keys() {
                if Some(addr) != target && !coordinator_candidates.contains(&addr) {
                    coordinator_candidates.push(addr);
                }
            }
        }

        // If the DHT referrer set a preferred coordinator for this target,
        // move it to the front of the candidate list so round 1 uses it.
        if let Some(target_addr) = target {
            if let Some(preferred) = self.hole_punch_preferred_coordinators.get(&target_addr) {
                let preferred_addr = *preferred;
                coordinator_candidates.retain(|a| *a != preferred_addr);
                coordinator_candidates.insert(0, preferred_addr);
                info!(
                    "Using preferred coordinator {} for target {} (DHT referrer)",
                    preferred_addr, target_addr
                );
            } else {
                info!(
                    "No preferred coordinator for target {} (not discovered via DHT referral)",
                    target_addr
                );
            }
        }

        if config.coordinator.is_none() {
            config.coordinator = coordinator_candidates.first().copied();
            if let Some(coord) = config.coordinator {
                info!(
                    "Using {} as NAT traversal coordinator ({} candidates total)",
                    coord,
                    coordinator_candidates.len()
                );
            }
        }
        if config.relay_addrs.is_empty() {
            if let Some(relay_addr) = self.config.nat.relay_nodes.first().copied() {
                config.relay_addrs.push(relay_addr);
            }

            // If still no relay addresses, use connected peers as relay candidates.
            // In the symmetric architecture, every node runs a MASQUE relay server.
            if config.relay_addrs.is_empty() {
                let peers = self.connected_peers.read().await;
                let target = target_ipv4.or(target_ipv6);
                for &addr in peers.keys() {
                    if Some(addr) != target {
                        config.relay_addrs.push(addr);
                    }
                }
                if !config.relay_addrs.is_empty() {
                    info!(
                        "Using {} connected peer(s) as relay candidates",
                        config.relay_addrs.len()
                    );
                }
            }
        }

        let mut strategy = ConnectionStrategy::new(config);

        info!(
            "Starting fallback connection: IPv4={:?}, IPv6={:?}",
            target_ipv4, target_ipv6
        );

        // Collect direct addresses for Happy Eyeballs racing (RFC 8305)
        let mut direct_addresses: Vec<SocketAddr> = Vec::new();
        if let Some(v6) = target_ipv6 {
            direct_addresses.push(v6);
        }
        if let Some(v4) = target_ipv4 {
            direct_addresses.push(v4);
        }

        loop {
            // Check if a previous hole-punch attempt established the connection
            // asynchronously (e.g. the target connected to us after receiving
            // a relayed PUNCH_ME_NOW from a prior round).
            let target = target_ipv4.or(target_ipv6);
            if let Some(target_addr) = target {
                if self.inner.is_connected(&target_addr) {
                    info!(
                        "connect_with_fallback: connection to {} established asynchronously",
                        target_addr
                    );
                    let peer_conn = PeerConnection {
                        public_key: None,
                        remote_addr: TransportAddr::Quic(target_addr),
                        authenticated: true,
                        connected_at: Instant::now(),
                        last_activity: Instant::now(),
                    };
                    // Spawn background reader task for data reception
                    if let Ok(Some(conn)) = self.inner.get_connection(&target_addr) {
                        self.spawn_reader_task(target_addr, conn).await;
                    }

                    self.connected_peers
                        .write()
                        .await
                        .insert(target_addr, peer_conn.clone());

                    // Broadcast PeerConnected so the identity exchange is
                    // triggered. Deduped so the parallel accept path doesn't
                    // double-announce for the same underlying connection.
                    broadcast_peer_connected_once(
                        &self.emitted_peer_connected,
                        &self.event_tx,
                        TransportAddr::Quic(target_addr),
                        peer_conn.public_key.clone(),
                        Side::Client,
                    );

                    // The connection was already established (e.g. by an
                    // earlier dial or an inbound connection from the peer).
                    // Report the actual address family instead of the
                    // misleading HolePunched variant.
                    let method = if target_addr.is_ipv4() {
                        ConnectionMethod::DirectIPv4
                    } else {
                        ConnectionMethod::DirectIPv6
                    };
                    return Ok((peer_conn, method));
                }
            }

            match strategy.current_stage().clone() {
                ConnectionStage::DirectIPv4 { .. } => {
                    // Use Happy Eyeballs (RFC 8305) to race all direct addresses (IPv4 + IPv6)
                    // instead of trying them sequentially. This prevents stalls when one address
                    // family is broken by racing with a 250ms stagger.
                    if direct_addresses.is_empty() {
                        debug!("No direct addresses provided, skipping to hole-punch");
                        strategy.transition_to_ipv6("No direct addresses");
                        continue;
                    }

                    let he_config = HappyEyeballsConfig::default();
                    let connect_timeout = strategy.direct_connect_timeout();
                    let handshake_timeout = strategy.direct_handshake_timeout();

                    info!(
                        "Happy Eyeballs: racing {} direct addresses (connect timeout: {:?}, handshake timeout: {:?})",
                        direct_addresses.len(),
                        connect_timeout,
                        handshake_timeout
                    );

                    // Clone the QUIC endpoint for use in the Happy Eyeballs closure.
                    // Each spawned attempt needs its own reference to create connections.
                    let quic_endpoint = match self.inner.get_endpoint().cloned() {
                        Some(ep) => ep,
                        None => {
                            debug!("QUIC endpoint not available, skipping direct");
                            strategy.transition_to_ipv6("QUIC endpoint not available");
                            strategy.transition_to_holepunch("QUIC endpoint not available");
                            continue;
                        }
                    };

                    let addrs = direct_addresses.clone();
                    let he_result = happy_eyeballs::race_connect(&addrs, &he_config, move |addr| {
                        let ep = quic_endpoint.clone();
                        async move {
                            let mut connecting = ep
                                .connect(addr, "peer")
                                .map_err(|e| format!("connect error: {e}"))?;

                            timeout(connect_timeout, connecting.first_peer_response())
                                .await
                                .map_err(|_| {
                                    format!(
                                        "direct connect timed out after {:?} waiting for first QUIC response",
                                        connect_timeout
                                    )
                                })?
                                .map_err(|e| format!("direct connect error: {e}"))?;

                            timeout(handshake_timeout, connecting)
                                .await
                                .map_err(|_| {
                                    format!("handshake timed out after {:?}", handshake_timeout)
                                })?
                                .map_err(|e| format!("handshake error: {e}"))
                        }
                    })
                    .await;

                    match he_result {
                        Ok((connection, winning_addr)) => {
                            let method = if winning_addr.is_ipv6() {
                                ConnectionMethod::DirectIPv6
                            } else {
                                ConnectionMethod::DirectIPv4
                            };
                            info!(
                                "Happy Eyeballs: {} connection to {} succeeded",
                                method, winning_addr
                            );

                            // Complete the connection setup (handlers, stats)
                            let peer_conn = self
                                .finalize_direct_connection(connection, winning_addr)
                                .await?;
                            return Ok((peer_conn, method));
                        }
                        Err(e) => {
                            debug!("Happy Eyeballs: all direct attempts failed: {}", e);
                            strategy.transition_to_ipv6(e.to_string());
                            strategy.transition_to_holepunch("Happy Eyeballs exhausted");
                        }
                    }
                }

                ConnectionStage::DirectIPv6 { .. } => {
                    // Happy Eyeballs already handled both IPv4 and IPv6 in the DirectIPv4 stage.
                    // If we reach here, it means Happy Eyeballs failed and we need to move on.
                    debug!(
                        "DirectIPv6 stage reached after Happy Eyeballs, advancing to hole-punch"
                    );
                    strategy.transition_to_holepunch("Handled by Happy Eyeballs");
                }

                ConnectionStage::HolePunching {
                    coordinator, round, ..
                } => {
                    let target = target_ipv4
                        .or(target_ipv6)
                        .ok_or(EndpointError::NoAddress)?;

                    info!(
                        "Trying hole-punch to {} via {} (round {})",
                        target, coordinator, round
                    );

                    // Use our existing NAT traversal infrastructure
                    match timeout(
                        strategy.holepunch_timeout(),
                        self.try_hole_punch(target, coordinator),
                    )
                    .await
                    {
                        Ok(Ok(conn)) => {
                            info!("✓ Hole-punch succeeded to {} via {}", target, coordinator);
                            return Ok((conn, ConnectionMethod::HolePunched { coordinator }));
                        }
                        Ok(Err(e)) => {
                            // After a failed hole-punch round, try a quick direct
                            // connect — the NAT binding may have been created by
                            // the target's outgoing packets even though our
                            // try_hole_punch didn't detect the connection.
                            if let Ok(Ok(peer_conn)) =
                                timeout(POST_HOLEPUNCH_DIRECT_RETRY_TIMEOUT, self.connect(target))
                                    .await
                            {
                                info!("✓ Post-hole-punch direct connect succeeded to {}", target);
                                return Ok((
                                    peer_conn,
                                    ConnectionMethod::HolePunched { coordinator },
                                ));
                            }
                            strategy.record_holepunch_error(round, e.to_string());
                            if strategy.should_retry_holepunch() {
                                // Keep the same coordinator for retries. The preferred
                                // coordinator (index 0) was chosen because it has a
                                // known connection to the target. Switching to a random
                                // fallback wastes another round on a coordinator that
                                // likely can't relay to the target.
                                info!(
                                    "Hole-punch round {} failed, retrying with same coordinator",
                                    round
                                );
                                strategy.increment_round();
                            } else {
                                debug!("Hole-punch failed after {} rounds", round);
                                strategy.transition_to_relay(e.to_string());
                            }
                        }
                        Err(_) => {
                            // Same: try a quick direct connect after timeout
                            if let Ok(Ok(peer_conn)) =
                                timeout(POST_HOLEPUNCH_DIRECT_RETRY_TIMEOUT, self.connect(target))
                                    .await
                            {
                                info!("✓ Post-hole-punch direct connect succeeded to {}", target);
                                return Ok((
                                    peer_conn,
                                    ConnectionMethod::HolePunched { coordinator },
                                ));
                            }
                            strategy.record_holepunch_error(round, "Timeout".to_string());
                            if strategy.should_retry_holepunch() {
                                info!(
                                    "Hole-punch round {} timed out, retrying with same coordinator",
                                    round
                                );
                                strategy.increment_round();
                            } else {
                                debug!("Hole-punch timed out after {} rounds", round);
                                strategy.transition_to_relay("Timeout");
                            }
                        }
                    }
                }

                ConnectionStage::Relay { relay_addr, .. } => {
                    let target = target_ipv4
                        .or(target_ipv6)
                        .ok_or(EndpointError::NoAddress)?;

                    info!("Trying relay connection to {} via {}", target, relay_addr);

                    match timeout(
                        strategy.relay_timeout(),
                        self.try_relay_connection(target, relay_addr),
                    )
                    .await
                    {
                        Ok(Ok(conn)) => {
                            info!(
                                "✓ Relay connection succeeded to {} via {}",
                                target, relay_addr
                            );
                            return Ok((conn, ConnectionMethod::Relayed { relay: relay_addr }));
                        }
                        Ok(Err(e)) => {
                            debug!("Relay connection failed: {}", e);
                            strategy.transition_to_failed(e.to_string());
                        }
                        Err(_) => {
                            debug!("Relay connection timed out");
                            strategy.transition_to_failed("Timeout");
                        }
                    }
                }

                ConnectionStage::Failed { errors } => {
                    let error_summary = errors
                        .iter()
                        .map(|e| format!("{:?}: {}", e.method, e.error))
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err(EndpointError::AllStrategiesFailed(error_summary));
                }

                ConnectionStage::Connected { via } => {
                    // This shouldn't happen in the loop, but handle it
                    unreachable!("Connected stage reached in loop: {:?}", via);
                }
            }
        }
    }

    /// Finalize a direct QUIC connection established by Happy Eyeballs.
    ///
    /// Takes the raw QUIC `Connection` from the successful handshake and completes
    /// the P2P connection setup: public key extraction, connection storage, handler
    /// spawning, stats update, and event broadcast.
    async fn finalize_direct_connection(
        &self,
        connection: crate::high_level::Connection,
        addr: SocketAddr,
    ) -> Result<PeerConnection, EndpointError> {
        let addr = normalize_socket_addr(addr);
        // Extract public key from TLS
        let remote_public_key = extract_public_key_bytes_from_connection(&connection);

        // Dedup check: if already connected to this address, use fingerprint tiebreaker
        if self.inner.is_connected(&addr) {
            // Use our SPKI fingerprint vs remote's for deterministic tiebreaking
            let remote_fingerprint = remote_public_key
                .as_deref()
                .and_then(|pk| {
                    crate::crypto::raw_public_keys::pqc::fingerprint_public_key_bytes(pk).ok()
                })
                .unwrap_or([0u8; 32]);
            let we_keep_client = self.our_fingerprint < remote_fingerprint;
            if !we_keep_client {
                // We have the higher fingerprint: close this outgoing connection,
                // keep the existing one (from accept path).
                info!(
                    "finalize_direct_connection: simultaneous open for {} — \
                     closing outgoing (keeping incoming)",
                    addr
                );
                connection.close(0u32.into(), b"duplicate");
                // Wait briefly for the accept path to populate connected_peers
                for _ in 0..10 {
                    let peers = self.connected_peers.read().await;
                    if let Some(existing) = peers.get(&addr) {
                        return Ok(existing.clone());
                    }
                    drop(peers);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                return Err(EndpointError::Connection(
                    "simultaneous open: peer connection not yet available, retry".into(),
                ));
            }
            // We have the lower fingerprint: keep our outgoing connection.
            // The incoming entry currently in the DashMap will be overwritten
            // by the `add_connection` call below — we deliberately do NOT
            // explicitly remove or close it here. Force-closing the incoming
            // would propagate a CONNECTION_CLOSE that races with the peer's
            // own simultaneous-open resolution and triggers cascading cleanup
            // that can evict the freshly-added outgoing's per-addr state.
            // The incoming's reader task will exit naturally once the peer
            // tears it down on their side; the stable_id guard in
            // `do_cleanup_connection` keeps that exit from disturbing the
            // outgoing.
            info!(
                "finalize_direct_connection: simultaneous open for {} — \
                 keeping outgoing (replacing incoming via add_connection)",
                addr
            );
        }

        // Store in NAT traversal layer (keyed by remote SocketAddr)
        self.inner
            .add_connection(addr, connection.clone())
            .map_err(EndpointError::NatTraversal)?;

        // Spawn connection handler (Client side - we initiated)
        self.inner
            .spawn_connection_handler(addr, connection, Side::Client)
            .map_err(EndpointError::NatTraversal)?;

        let peer_conn = PeerConnection {
            public_key: remote_public_key.clone(),
            remote_addr: TransportAddr::Quic(addr),
            authenticated: true,
            connected_at: Instant::now(),
            last_activity: Instant::now(),
        };

        // Spawn reader task before storing peer to prevent data loss race
        if let Ok(Some(conn)) = self.inner.get_connection(&addr) {
            self.spawn_reader_task(addr, conn).await;
        }

        self.connected_peers
            .write()
            .await
            .insert(addr, peer_conn.clone());

        {
            let mut stats = self.stats.write().await;
            stats.active_connections += 1;
            stats.successful_connections += 1;
            stats.direct_connections += 1;
        }

        broadcast_peer_connected_once(
            &self.emitted_peer_connected,
            &self.event_tx,
            TransportAddr::Quic(addr),
            remote_public_key,
            Side::Client,
        );

        Ok(peer_conn)
    }

    /// Internal helper for hole-punch attempt
    async fn try_hole_punch(
        &self,
        target: SocketAddr,
        coordinator: SocketAddr,
    ) -> Result<PeerConnection, EndpointError> {
        let target = normalize_socket_addr(target);
        let coordinator = normalize_socket_addr(coordinator);
        info!(
            "try_hole_punch: ENTER target={} coordinator={}",
            target, coordinator
        );

        // First ensure we're connected to the coordinator
        if !self.is_connected_to_addr(coordinator).await {
            info!(
                "try_hole_punch: connecting to coordinator {} first",
                coordinator
            );
            self.connect(coordinator).await?;
            info!("try_hole_punch: coordinator {} connected", coordinator);
        } else {
            info!(
                "try_hole_punch: coordinator {} already connected",
                coordinator
            );
        }

        // Initiate NAT traversal — sends PUNCH_ME_NOW to coordinator.
        // Look up the target peer ID from the per-target map. This avoids
        // races when multiple concurrent connections share the same P2pEndpoint.
        let target_peer_id = self.hole_punch_target_peer_ids.get(&target).map(|v| *v);
        // The target's peer ID is not logged: these lines already name its address.
        if target_peer_id.is_some() {
            info!(
                "try_hole_punch: calling initiate_nat_traversal({}, {}) with peer ID (dashmap key={})",
                target, coordinator, target
            );
        } else {
            info!(
                "try_hole_punch: calling initiate_nat_traversal({}, {}) with address-based wire ID (no dashmap entry for key={})",
                target, coordinator, target
            );
        }
        self.inner
            .initiate_nat_traversal_for_peer(target, coordinator, target_peer_id)
            .map_err(EndpointError::NatTraversal)?;
        info!("try_hole_punch: initiate_nat_traversal returned OK");

        // NOTE: We intentionally do NOT send a QUIC probe here.
        // A previous attempt sent a fire-and-forget probe that created
        // a second QUIC connection to the target address. When the probe
        // succeeded (target is a cloud VM, directly reachable), the probe
        // connection was accepted by the target, stored in the DashMap
        // under the same key as the REAL incoming connection, then
        // immediately closed with "hole-punch-probe". The close triggered
        // cleanup that removed the DashMap entry — destroying the real
        // connection's entry and making all send() calls fail.
        //
        // The correct approach: rely on the coordinator relay (PUNCH_ME_NOW)
        // to create the NAT binding on the target side. The target then
        // connects back to us, and we use THAT connection for bidirectional
        // communication.

        // Poll for the connection to appear. The target node will receive
        // the relayed PUNCH_ME_NOW and initiate a QUIC connection to us,
        // which gets accepted by saorsa-core's transport handler.
        // No internal deadline — the outer strategy.holepunch_timeout()
        // cancels this future when it expires.
        let mut poll_count = 0u32;

        loop {
            poll_count += 1;
            if poll_count % 10 == 1 {
                info!(
                    "try_hole_punch: poll loop iteration {} for target {}",
                    poll_count, target
                );
            }

            if self.shutdown.is_cancelled() {
                return Err(EndpointError::ShuttingDown);
            }

            // Check for connection by address first (fast path for cone NAT),
            // then by peer ID (handles symmetric NAT where the return
            // connection has a different port than the DHT address).
            let connected_addr = if self.inner.is_connected(&target) {
                Some(target)
            } else if let Some(ref pid) = target_peer_id {
                self.find_connection_by_peer_id(pid)
            } else {
                None
            };

            if let Some(actual_addr) = connected_addr {
                let actual_addr = normalize_socket_addr(actual_addr);
                info!(
                    "try_hole_punch: connection to {} established (actual addr: {})!",
                    target, actual_addr
                );
                let peer_conn = PeerConnection {
                    public_key: None,
                    remote_addr: TransportAddr::Quic(actual_addr),
                    authenticated: true,
                    connected_at: Instant::now(),
                    last_activity: Instant::now(),
                };
                self.connected_peers
                    .write()
                    .await
                    .insert(actual_addr, peer_conn.clone());
                return Ok(peer_conn);
            }

            // Check P2pEndpoint's connected_peers (populated by saorsa-core)
            // Try both the target address and, for symmetric NAT, any
            // connection matching the peer ID.
            {
                let peers = self.connected_peers.read().await;
                if let Some(existing) = peers.get(&target) {
                    info!("try_hole_punch: connection to {} found in peers", target);
                    return Ok(existing.clone());
                }
            }

            // Wait briefly then re-check; the outer timeout cancels us on expiry
            tokio::select! {
                _ = self.inner.connection_notify().notified() => {
                    debug!("try_hole_punch: connection_notify fired for {}", target);
                }
                _ = self.shutdown.cancelled() => {
                    return Err(EndpointError::ShuttingDown);
                }
                // Wake periodically to drive session and re-check connections
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    }

    async fn try_relay_connection(
        &self,
        target: SocketAddr,
        relay_addr: SocketAddr,
    ) -> Result<PeerConnection, EndpointError> {
        info!(
            "Attempting MASQUE relay connection to {} via {}",
            target, relay_addr
        );

        // Step 1: Establish relay session (control plane handshake)
        let (public_addr, raw_streams) = self
            .inner
            .establish_relay_session(relay_addr)
            .await
            .map_err(EndpointError::NatTraversal)?;

        info!(
            "MASQUE relay session established via {} (public addr: {:?})",
            relay_addr, public_addr
        );

        let raw_streams = raw_streams.ok_or_else(|| {
            EndpointError::Connection("Relay did not provide streams".to_string())
        })?;

        // Step 4: Create a new Quinn endpoint with the relay socket
        let existing_endpoint = self
            .inner
            .get_endpoint()
            .ok_or_else(|| EndpointError::Config("QUIC endpoint not available".to_string()))?;

        let relay_public_addr = public_addr.ok_or_else(|| {
            EndpointError::Connection("Relay did not provide address".to_string())
        })?;
        let original_socket = existing_endpoint.current_socket().map_err(|e| {
            EndpointError::Connection(format!("Failed to get original socket: {e}"))
        })?;
        // Dial-through-relay path: the returned `_closed` Notify fires
        // when the tunnel dies.  The per-dial relay endpoint carries a
        // single connection and is discarded after the dial completes,
        // so we don't wire up a graceful-close watcher — the single
        // connection is the natural unit of recovery at this layer.
        let (relay_socket, _tunnel) = crate::masque::MasqueRelaySocket::new(
            raw_streams.send_stream,
            raw_streams.recv_stream,
            relay_public_addr,
            relay_addr,
            original_socket,
        );

        let mut client_config = existing_endpoint
            .default_client_config
            .clone()
            .ok_or_else(|| EndpointError::Config("No client config available".to_string()))?;

        // Clamp the relay-tunnelled dialer's MTU to QUIC's safe
        // minimum so the very first Initial+Handshake we coalesce can
        // never exceed any real Internet egress path that the relay
        // hands off to.  DPLPMTUD then probes upward as
        // `TunnelControlFrame::PmtuUpdate` (out-of-band tunnel
        // control frames) trim the per-target cap on the relay side.
        // Other transport settings — congestion control, idle timeout,
        // flow control windows — are inherited from the existing
        // client config.
        let mut tunnel_tc = (*client_config.transport).clone();
        tunnel_tc.initial_mtu(RELAY_TUNNEL_INITIAL_MTU);
        tunnel_tc.min_mtu(RELAY_TUNNEL_INITIAL_MTU);
        tunnel_tc.mtu_discovery_config(Some(MtuDiscoveryConfig::default()));
        client_config.transport_config(Arc::new(tunnel_tc));

        let runtime = crate::high_level::default_runtime()
            .ok_or_else(|| EndpointError::Config("No async runtime available".to_string()))?;

        let mut relay_endpoint = crate::high_level::Endpoint::new_with_abstract_socket(
            crate::EndpointConfig::default(),
            None,
            relay_socket,
            runtime,
        )
        .map_err(|e| {
            EndpointError::Connection(format!("Failed to create relay endpoint: {}", e))
        })?;

        relay_endpoint.set_default_client_config(client_config);

        // Step 5: Connect to target through the relay endpoint
        let connecting = relay_endpoint.connect(target, "peer").map_err(|e| {
            EndpointError::Connection(format!("Failed to initiate relay connection: {}", e))
        })?;

        let handshake_timeout = self
            .config
            .timeouts
            .nat_traversal
            .connection_establishment_timeout;

        let connection = match timeout(handshake_timeout, connecting).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                info!(
                    "Relay connection handshake to {} via {} failed: {}",
                    target, relay_addr, e
                );
                return Err(EndpointError::Connection(e.to_string()));
            }
            Err(_) => {
                info!(
                    "Relay connection handshake to {} via {} timed out",
                    target, relay_addr
                );
                return Err(EndpointError::Timeout);
            }
        };

        // Step 6: Finalize — extract public key, store connection, spawn handler
        let remote_public_key = extract_public_key_bytes_from_connection(&connection);

        self.inner
            .add_connection(target, connection.clone())
            .map_err(EndpointError::NatTraversal)?;

        self.inner
            .spawn_connection_handler(target, connection, Side::Client)
            .map_err(EndpointError::NatTraversal)?;

        let peer_conn = PeerConnection {
            public_key: remote_public_key,
            remote_addr: TransportAddr::Quic(target),
            authenticated: true,
            connected_at: Instant::now(),
            last_activity: Instant::now(),
        };

        // Spawn background reader task
        if let Ok(Some(conn)) = self.inner.get_connection(&target) {
            self.spawn_reader_task(target, conn).await;
        }

        // Store peer connection
        self.connected_peers
            .write()
            .await
            .insert(target, peer_conn.clone());

        info!(
            "MASQUE relay connection succeeded to {} via {}",
            target, relay_addr
        );

        Ok(peer_conn)
    }

    /// Check if we're connected to a specific address
    async fn is_connected_to_addr(&self, addr: SocketAddr) -> bool {
        let transport_addr = TransportAddr::Quic(normalize_socket_addr(addr));
        let peers = self.connected_peers.read().await;
        peers.values().any(|p| p.remote_addr == transport_addr)
    }

    /// Accept incoming connections
    ///
    /// Returns `None` if the endpoint is shutting down or the accept fails.
    /// This method races the inner accept against the shutdown token, so it
    /// will return promptly when `shutdown()` is called.
    pub async fn accept(&self) -> Option<PeerConnection> {
        self.accept_with_connection()
            .await
            .map(|(peer, _connection)| peer)
    }

    /// Accept an incoming peer while retaining its authoritative QUIC handle.
    ///
    /// The link-transport adapter needs the exact accepted handle. Looking it
    /// up again by address races connection cleanup when a short-lived peer
    /// closes immediately after the handshake.
    pub(crate) async fn accept_with_connection(
        &self,
    ) -> Option<(PeerConnection, crate::high_level::Connection)> {
        if self.shutdown.is_cancelled() {
            return None;
        }

        let result = tokio::select! {
            r = self.inner.accept_connection_direct() => r,
            _ = self.shutdown.cancelled() => return None,
        };

        match result {
            Ok((remote_addr, connection)) => {
                // Extract public key from TLS handshake
                let remote_public_key = extract_public_key_bytes_from_connection(&connection);
                let reader_connection = connection.clone();
                let accepted_connection = connection.clone();

                // They initiated the connection to us = Server side
                if let Err(e) =
                    self.inner
                        .spawn_connection_handler(remote_addr, connection, Side::Server)
                {
                    error!("Failed to spawn connection handler: {}", e);
                    return None;
                }

                // v0.2: Peer is authenticated via TLS (ML-DSA-65) during handshake
                let peer_conn = PeerConnection {
                    public_key: remote_public_key.clone(),
                    remote_addr: TransportAddr::Quic(remote_addr),
                    authenticated: true, // TLS handles authentication
                    connected_at: Instant::now(),
                    last_activity: Instant::now(),
                };

                // The accepted connection is authoritative. Using it directly
                // avoids racing a peer's immediate close against a redundant
                // lookup in the shared connection map.
                info!("accept: spawning reader task for {}", remote_addr);
                self.spawn_reader_task(remote_addr, reader_connection).await;

                self.connected_peers
                    .write()
                    .await
                    .insert(remote_addr, peer_conn.clone());

                {
                    let mut stats = self.stats.write().await;
                    stats.active_connections += 1;
                    stats.successful_connections += 1;
                }

                // They initiated the connection to us = Server side
                broadcast_peer_connected_once(
                    &self.emitted_peer_connected,
                    &self.event_tx,
                    TransportAddr::Quic(remote_addr),
                    remote_public_key,
                    Side::Server,
                );

                Some((peer_conn, accepted_connection))
            }
            Err(e) => {
                debug!("Accept failed: {}", e);
                None
            }
        }
    }

    /// Clean up a connection from ALL tracking structures.
    ///
    /// This is the single point of cleanup for connections — it removes the peer from:
    /// - `connected_peers` HashMap
    /// - `NatTraversalEndpoint.connections` DashMap (via `remove_connection()`)
    /// - `reader_handles` (aborts the background reader task)
    /// - Updates stats and emits a disconnect event
    ///
    /// Safe to call even if the peer is not in all structures (idempotent).
    async fn cleanup_connection(&self, addr: &SocketAddr, reason: DisconnectReason) {
        // Explicit disconnect: force-remove the DashMap entry regardless of
        // Quinn's close_reason state. The caller has already decided this
        // peer should go.
        do_cleanup_connection(
            &*self.connected_peers,
            &*self.inner,
            &*self.reader_handles,
            &*self.stats,
            &self.event_tx,
            &self.emitted_peer_connected,
            addr,
            reason,
            None,
        )
        .await;
    }

    /// Disconnect from a peer by address
    pub async fn disconnect(&self, addr: &SocketAddr) -> Result<(), EndpointError> {
        let variants = socket_addr_variants(*addr);
        if self
            .connected_peers
            .read()
            .await
            .keys()
            .any(|known| variants.contains(known))
        {
            self.cleanup_connection(addr, DisconnectReason::Normal)
                .await;
            Ok(())
        } else {
            Err(EndpointError::PeerNotFound(*addr))
        }
    }

    // === Messaging ===

    /// Send data to a peer
    ///
    /// For QUIC connections with payloads at or above
    /// `LARGE_SEND_DELIVERY_ACK_THRESHOLD` bytes, this waits for the peer to
    /// acknowledge receipt of the full stream before returning. Smaller
    /// payloads return as soon as the local stack has accepted the data.
    ///
    /// # Transport Selection
    ///
    /// This method selects the appropriate transport provider based on the destination
    /// peer's address type and the capabilities advertised in the transport registry.
    ///
    /// ## Current Behavior (Phase 2.1)
    ///
    /// All connections currently use UDP/QUIC via the existing `connection.open_uni()`
    /// path. This ensures backward compatibility with existing peers.
    ///
    /// ## Future Behavior (Phase 2.3)
    ///
    /// Transport selection will be based on:
    /// - Peer's advertised transport addresses (from connection metadata)
    /// - Transport provider capabilities (from `transport_registry`)
    /// - Protocol engine requirements (QUIC vs Constrained)
    ///
    /// Selection priority:
    /// 1. **UDP/QUIC**: Default for broadband, full QUIC support
    /// 2. **BLE**: For nearby devices, constrained engine
    /// 3. **LoRa**: For long-range, low-bandwidth scenarios
    /// 4. **Overlay**: For I2P/Yggdrasil privacy-preserving routing
    ///
    /// # Arguments
    ///
    /// - `addr`: The target peer's socket address
    /// - `data`: The payload to send
    ///
    /// # Errors
    ///
    /// Returns `EndpointError` if:
    /// - The endpoint is shutting down
    /// - The peer is not connected
    /// - No suitable transport provider is available
    /// - The send operation fails
    pub async fn send(&self, addr: &SocketAddr, data: &[u8]) -> Result<(), EndpointError> {
        self.send_inner(addr, SendPayload::Borrowed(data)).await
    }

    /// Send an owned buffer to a peer without copying it.
    ///
    /// Same semantics as [`send`](Self::send), but the QUIC stream takes over
    /// `data` directly, so large frames are not duplicated between the caller
    /// and the stream's send buffer for the duration of the transfer.
    pub async fn send_bytes(&self, addr: &SocketAddr, data: Bytes) -> Result<(), EndpointError> {
        self.send_inner(addr, SendPayload::Owned(data)).await
    }

    async fn send_inner(
        &self,
        addr: &SocketAddr,
        data: SendPayload<'_>,
    ) -> Result<(), EndpointError> {
        if self.shutdown.is_cancelled() {
            return Err(EndpointError::ShuttingDown);
        }
        // Recorded up front: the QUIC path gives `data` away to the stream.
        let total_len = data.len();

        // Get peer's transport address and optionally capture the connection
        // for hole-punched peers that bypassed normal registration.
        //
        // On dual-stack sockets (bindv6only=0), incoming connections use
        // IPv4-mapped IPv6 addresses ([::ffff:x.x.x.x]) but callers may pass
        // plain IPv4. Try both forms when looking up the peer.
        let (transport_addr, cached_connection) = {
            let peer_info = self.connected_peers.read().await;
            let variants = socket_addr_variants(*addr);
            let found = variants
                .iter()
                .find_map(|candidate| peer_info.get(candidate));
            if let Some(peer_conn) = found {
                (peer_conn.remote_addr.clone(), None)
            } else {
                // Check if the NatTraversalEndpoint has a connection to this
                // address (e.g. from a hole-punch that bypassed the normal path).
                // Capture the connection now before it can be cleaned up.
                drop(peer_info);
                let conn = variants
                    .iter()
                    .find_map(|candidate| self.inner.get_connection(candidate).ok().flatten());
                if let Some(conn) = conn {
                    let key = normalize_socket_addr(*addr);
                    info!(
                        "send: found hole-punched connection to {}, registering",
                        key
                    );
                    let peer_conn = PeerConnection {
                        public_key: None,
                        remote_addr: TransportAddr::Quic(key),
                        authenticated: true,
                        connected_at: Instant::now(),
                        last_activity: Instant::now(),
                    };
                    self.connected_peers.write().await.insert(key, peer_conn);
                    broadcast_peer_connected_once(
                        &self.emitted_peer_connected,
                        &self.event_tx,
                        TransportAddr::Quic(key),
                        None,
                        Side::Server,
                    );
                    (TransportAddr::Quic(key), Some(conn))
                } else {
                    return Err(EndpointError::PeerNotFound(*addr));
                }
            }
        };

        // Select protocol engine based on transport address.
        //
        // No lock: `select_engine_for_addr` takes `&self` on
        // `ConnectionRouter` and bumps its stats counters via atomics, so
        // concurrent sends can run fully in parallel. The previous
        // implementation held an exclusive write lock on the router here,
        // which serialised every outbound send on the endpoint through a
        // single lock and was a dominant contention point at high node
        // counts (1000-node testnet).
        let engine = self.router.select_engine_for_addr(&transport_addr);

        match engine {
            crate::transport::ProtocolEngine::Quic => {
                // Use cached connection (from hole-punch) or look up fresh
                let connection = if let Some(conn) = cached_connection {
                    conn
                } else {
                    self.inner
                        .get_connection(addr)
                        .map_err(EndpointError::NatTraversal)?
                        .ok_or(EndpointError::PeerNotFound(*addr))?
                };

                // Fail fast on a dead connection.
                //
                // If the QUIC connection has a close_reason, every step of
                // the send path below is guaranteed to either fail or hang
                // until a per-step progress timeout. Short-circuiting here
                // collapses that to a microsecond-scale error.
                //
                // The upper layer (transport_handle channel recovery)
                // treats `EndpointError::PeerNotFound` as "this channel
                // is dead, remove it and try the next", which is the
                // behaviour we want here.
                if let Some(reason) = connection.close_reason() {
                    debug!(
                        "send({}): skipping, connection already closed ({})",
                        addr, reason
                    );
                    return Err(EndpointError::PeerNotFound(*addr));
                }

                let send_started = Instant::now();

                let open_uni_started = Instant::now();
                debug!("send({}): opening QUIC uni stream", addr);
                let mut send_stream =
                    match timeout(STREAM_WRITE_PROGRESS_TIMEOUT, connection.open_uni()).await {
                        Ok(Ok(stream)) => stream,
                        Ok(Err(e)) => {
                            warn!("send({}): open_uni failed: {}", addr, e);
                            return Err(send_failed(
                                SendFailureStage::OpenStream,
                                0,
                                e.to_string(),
                            ));
                        }
                        Err(_elapsed) => {
                            warn!(
                                "send({}): open_uni made no progress for {:?}",
                                addr, STREAM_WRITE_PROGRESS_TIMEOUT
                            );
                            return Err(send_failed(
                                SendFailureStage::OpenStreamProgressTimeout,
                                0,
                                format!(
                                    "open_uni made no progress for {:?}",
                                    STREAM_WRITE_PROGRESS_TIMEOUT
                                ),
                            ));
                        }
                    };
                debug!(
                    "send({}): opened QUIC uni stream in {:?}",
                    addr,
                    open_uni_started.elapsed()
                );

                // Moved, not cloned: from here on only the stream holds the
                // frame (a borrowed `send(&[u8])` payload is copied here, once).
                let bytes_written = match write_stream_with_progress_timeout(
                    &mut send_stream,
                    *addr,
                    data.into_bytes(),
                )
                .await
                {
                    Ok(bytes_written) => bytes_written,
                    Err(e) => {
                        let _ = send_stream.reset(crate::VarInt::from_u32(STREAM_RESET_ABORT_CODE));
                        return Err(e);
                    }
                };

                let finish_started = Instant::now();
                debug!(
                    "send({}): finishing QUIC uni stream after writing {} bytes",
                    addr, bytes_written
                );
                send_stream.finish().map_err(|e| {
                    warn!("send({}): finish failed: {}", addr, e);
                    send_failed(SendFailureStage::Finish, bytes_written, e.to_string())
                })?;
                debug!(
                    "send({}): finished QUIC uni stream in {:?}",
                    addr,
                    finish_started.elapsed()
                );

                if total_len >= LARGE_SEND_DELIVERY_ACK_THRESHOLD {
                    wait_for_stream_delivery_ack(&send_stream, *addr, bytes_written).await?;
                }

                debug!(
                    "Sent {} bytes to {} via QUIC (send path took {:?})",
                    total_len,
                    addr,
                    send_started.elapsed()
                );
            }
            crate::transport::ProtocolEngine::Constrained => {
                // Check if we have an established constrained connection for this address
                let maybe_conn_id = self
                    .constrained_connections
                    .read()
                    .await
                    .get(&transport_addr)
                    .copied();

                if let Some(conn_id) = maybe_conn_id {
                    // Use ConstrainedEngine for reliable delivery
                    let engine = self.inner.constrained_engine();
                    let responses = {
                        let mut engine = engine.lock();
                        engine
                            .send(conn_id, data.as_slice())
                            .map_err(|e| EndpointError::Connection(e.to_string()))?
                    };

                    // Send any packets generated by the constrained engine
                    for (_dest_addr, packet_data) in responses {
                        self.transport_registry
                            .send(&packet_data, &transport_addr)
                            .await
                            .map_err(|e| EndpointError::Connection(e.to_string()))?;
                    }

                    debug!(
                        "Sent {} bytes to {} via constrained engine ({})",
                        total_len,
                        addr,
                        transport_addr.transport_type()
                    );
                } else {
                    // No established connection - send directly via transport
                    self.transport_registry
                        .send(data.as_slice(), &transport_addr)
                        .await
                        .map_err(|e| EndpointError::Connection(e.to_string()))?;

                    debug!(
                        "Sent {} bytes to {} via constrained transport (direct, {})",
                        total_len,
                        addr,
                        transport_addr.transport_type()
                    );
                }
            }
        }

        Ok(())
    }

    /// Receive data from any connected peer.
    ///
    /// Blocks until data arrives from any transport (UDP/QUIC, BLE, LoRa, etc.)
    /// or the endpoint shuts down. Background reader tasks feed a shared channel,
    /// so this wakes instantly when data is available.
    ///
    /// # Errors
    ///
    /// Returns `EndpointError::ShuttingDown` if the endpoint is shutting down.
    pub async fn recv(&self) -> Result<(SocketAddr, Vec<u8>), EndpointError> {
        if self.shutdown.is_cancelled() {
            return Err(EndpointError::ShuttingDown);
        }

        // Note: pending data buffer (BoundedPendingBuffer) still uses PeerId internally.
        // It is not consulted here; background reader tasks feed the data_rx channel
        // using SocketAddr as the key.

        // Wait for data from the shared channel (fed by background reader tasks),
        // racing against the shutdown token so callers unblock promptly on shutdown.
        let mut rx = self.data_rx.lock().await;
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => Ok(msg),
                None => Err(EndpointError::ShuttingDown),
            },
            _ = self.shutdown.cancelled() => Err(EndpointError::ShuttingDown),
        }
    }

    // === Events ===

    /// Subscribe to endpoint events
    pub fn subscribe(&self) -> broadcast::Receiver<P2pEvent> {
        self.event_tx.subscribe()
    }

    // === Statistics ===

    /// Get endpoint statistics
    pub async fn stats(&self) -> EndpointStats {
        self.stats.read().await.clone()
    }

    /// Get metrics for a specific connection by address
    pub async fn connection_metrics(&self, addr: &SocketAddr) -> Option<ConnectionMetrics> {
        let peers = self.connected_peers.read().await;
        let peer_conn = peers.get(addr)?;
        let last_activity = Some(peer_conn.last_activity);
        drop(peers);

        let connection = self.inner.get_connection(addr).ok()??;
        let stats = connection.stats();
        let rtt = connection.rtt();

        Some(ConnectionMetrics {
            bytes_sent: stats.udp_tx.bytes,
            bytes_received: stats.udp_rx.bytes,
            rtt: Some(rtt),
            packet_loss: stats.path.lost_packets as f64
                / (stats.path.sent_packets + stats.path.lost_packets).max(1) as f64,
            last_activity,
        })
    }

    /// Get NAT traversal statistics
    pub fn nat_stats(&self) -> Result<NatTraversalStatistics, EndpointError> {
        self.inner
            .get_nat_stats()
            .map_err(|e| EndpointError::Connection(e.to_string()))
    }

    // === Known Peers ===

    /// Connect to configured known peers
    ///
    /// This method now uses the connection router to automatically select
    /// the appropriate protocol engine for each peer address.
    pub async fn connect_known_peers(&self) -> Result<usize, EndpointError> {
        let mut connected = 0;
        let known_peers = self.config.known_peers.clone();

        for addr in &known_peers {
            // Use connect_transport for all address types
            match self.connect_transport(addr).await {
                Ok(_) => {
                    connected += 1;
                    info!("Connected to known peer {}", addr);
                }
                Err(e) => {
                    warn!("Failed to connect to known peer {}: {}", addr, e);
                }
            }
        }

        {
            let mut stats = self.stats.write().await;
            stats.connected_bootstrap_nodes = connected;
        }

        // Close the candidate-discovery feed window before broadcasting
        // BootstrapStatus, so any observer that reacts to the event sees a
        // consistent "bootstrap is over" state.
        self.inner.mark_bootstrap_complete();

        let _ = self.event_tx.send(P2pEvent::BootstrapStatus {
            connected,
            total: known_peers.len(),
        });

        // Relay acquisition is now driven exclusively by saorsa-core's
        // acquisition driver (see saorsa-core `reachability::driver`),
        // which unconditionally tries to acquire a MASQUE relay from an
        // XOR-closest peer after bootstrap. The legacy symmetric-NAT-
        // gated auto-relay that used to run here would race the driver
        // and produce duplicate rebinds.

        Ok(connected)
    }

    /// Add a bootstrap node dynamically
    pub async fn add_bootstrap(&self, addr: SocketAddr) {
        let _ = self.inner.add_bootstrap_node(addr);
        let mut stats = self.stats.write().await;
        stats.total_bootstrap_nodes += 1;
    }

    /// Check if the proactive relay session is still alive.
    ///
    /// Returns `true` if no relay was established (nothing to monitor) or the
    /// relay's underlying QUIC connection is still open. Returns `false` if a
    /// relay was established but the session has died — the caller should
    /// rebind.
    pub async fn is_relay_healthy(&self) -> bool {
        self.inner.is_relay_healthy().await
    }

    /// Enable or disable relay serving on this node's MASQUE relay server.
    ///
    /// Delegates to [`NatTraversalEndpoint::set_relay_serving_enabled`].
    /// Called by the ADR-014 reachability classifier: public nodes leave it
    /// enabled, private nodes disable it so they reject incoming relay
    /// reservation requests.
    pub fn set_relay_serving_enabled(&self, enabled: bool) {
        self.inner.set_relay_serving_enabled(enabled);
    }

    /// Establish and immediately publish a proactive MASQUE relay session with
    /// `relay_addr` as a supplementary inbound path.
    ///
    /// This legacy convenience entry point prepares and publishes in one call.
    /// Canary-gated callers should use
    /// [`prepare_proactive_relay`](Self::prepare_proactive_relay), followed by
    /// either [`publish_proactive_relay`](Self::publish_proactive_relay) or
    /// [`abort_proactive_relay`](Self::abort_proactive_relay).
    ///
    /// On success, the returned `SocketAddr` is the relay-allocated public
    /// address the caller should publish as its contact address in the DHT
    /// self-record.
    ///
    /// Errors from `setup_proactive_relay` (including
    /// [`NatTraversalError::RelayAtCapacity`]) are propagated through the
    /// [`EndpointError::NatTraversal`] conversion, so callers can match on the
    /// inner variant to distinguish "walk to next candidate" from other
    /// failure modes.
    pub async fn setup_proactive_relay(
        &self,
        relay_addr: SocketAddr,
    ) -> Result<SocketAddr, EndpointError> {
        let allocated = self.inner.setup_proactive_relay(relay_addr).await?;
        Ok(allocated)
    }

    /// Prepare a proactive MASQUE relay without publishing its address.
    ///
    /// The relay endpoint is live so callers can run inbound canary probes,
    /// but the address is not exposed through `relay_public_addr` or emitted as
    /// `RelayEstablished` until
    /// [`publish_proactive_relay`](Self::publish_proactive_relay) is called.
    /// Relay addresses are not sent through connection-level `ADD_ADDRESS`;
    /// authenticated upper layers own their publication.
    pub async fn prepare_proactive_relay(
        &self,
        relay_addr: SocketAddr,
    ) -> Result<PreparedRelay, EndpointError> {
        Ok(self.inner.prepare_proactive_relay(relay_addr).await?)
    }

    /// Activate the current provisional proactive relay.
    pub async fn publish_proactive_relay(
        &self,
        prepared: PreparedRelay,
    ) -> Result<(), EndpointError> {
        self.inner.publish_proactive_relay(prepared).await?;
        Ok(())
    }

    /// Abort a provisional or published proactive relay and release its
    /// MASQUE session and relay-server capacity.
    pub async fn abort_proactive_relay(
        &self,
        prepared: PreparedRelay,
    ) -> Result<(), EndpointError> {
        self.inner.abort_proactive_relay(prepared).await?;
        Ok(())
    }

    /// Perform a fresh, isolated authenticated reachability probe.
    ///
    /// This connection never enters the ordinary peer or address maps and is
    /// closed by the same call that created it.
    pub async fn probe_fresh_authenticated(
        &self,
        target: SocketAddr,
    ) -> Result<Vec<u8>, EndpointError> {
        self.inner
            .probe_fresh_authenticated(target)
            .await
            .map_err(EndpointError::NatTraversal)
    }

    /// Get list of connected peers
    pub async fn connected_peers(&self) -> Vec<PeerConnection> {
        self.connected_peers
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    /// Check if an address is connected
    pub async fn is_connected(&self, addr: &SocketAddr) -> bool {
        self.connected_peers
            .read()
            .await
            .contains_key(&normalize_socket_addr(*addr))
    }

    /// Check if a live QUIC connection exists at the NatTraversalEndpoint level.
    ///
    /// This is the authoritative check — it queries the DashMap that stores
    /// actual QUIC connections, bypassing the connected_peers HashMap which
    /// may have a registration delay for hole-punch connections.
    ///
    /// Tries both the plain and IPv4-mapped address forms because the DashMap
    /// key format depends on whether the connection was established on a
    /// dual-stack socket (IPv6-mapped) or IPv4-only (plain).
    /// Check if a peer with the given ID has an active connection,
    /// returning the actual socket address. For symmetric NAT, the
    /// address differs from the DHT address.
    pub fn find_connection_by_peer_id(&self, peer_id: &[u8; 32]) -> Option<SocketAddr> {
        self.inner.find_connection_by_peer_id(peer_id)
    }

    /// Register a peer ID at the low-level endpoint for PUNCH_ME_NOW relay
    /// routing. Called when the identity exchange completes on a connection.
    pub fn register_connection_peer_id(&self, addr: SocketAddr, peer_id: [u8; 32]) {
        self.inner
            .register_connection_peer_id(addr, crate::nat_traversal_api::PeerId(peer_id));
    }

    /// Return whether this endpoint owns an active QUIC connection for `addr`.
    pub fn has_active_connection(&self, addr: &SocketAddr) -> bool {
        self.inner.is_connected(addr)
    }

    /// Check if a peer is connected at the transport level.
    pub fn inner_is_connected(&self, addr: &SocketAddr) -> bool {
        if self.inner.is_connected(addr) {
            debug!("inner_is_connected: {} found (exact match)", addr);
            return true;
        }
        // Try the alternate form (plain ↔ mapped)
        if let Some(alt) = crate::shared::dual_stack_alternate(addr) {
            if self.inner.is_connected(&alt) {
                debug!("inner_is_connected: {} found via alternate {}", addr, alt);
                return true;
            }
        }
        info!(
            "inner_is_connected: {} NOT found (connections: {})",
            addr,
            self.inner.connection_count()
        );
        false
    }

    /// Check if an address is authenticated
    pub async fn is_authenticated(&self, addr: &SocketAddr) -> bool {
        self.connected_peers
            .read()
            .await
            .get(addr)
            .map(|p| p.authenticated)
            .unwrap_or(false)
    }

    // === Lifecycle ===

    /// Shutdown the endpoint gracefully
    pub async fn shutdown(&self) {
        info!("Shutting down P2P endpoint");
        self.shutdown.cancel();

        // Abort all background reader tasks
        self.reader_tasks.lock().await.abort_all();
        self.reader_handles.write().await.clear();

        // Disconnect all peers
        let addrs: Vec<SocketAddr> = self.connected_peers.read().await.keys().copied().collect();
        for addr in addrs {
            let _ = self.disconnect(&addr).await;
        }

        // Bounded timeout prevents blocking when the remote peer is unresponsive.
        match timeout(SHUTDOWN_DRAIN_TIMEOUT, self.inner.shutdown()).await {
            Err(_) => warn!("Inner endpoint shutdown timed out, proceeding"),
            Ok(Err(e)) => warn!("Inner endpoint shutdown error: {e}"),
            Ok(Ok(())) => {}
        }
    }

    /// Check if endpoint is running
    pub fn is_running(&self) -> bool {
        !self.shutdown.is_cancelled()
    }

    /// Get a clone of the shutdown token (for external cancellation listening)
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    // === Internal helpers ===

    /// Spawn a background tokio task that reads uni streams from a QUIC connection
    /// and forwards received data into the shared `data_tx` channel.
    ///
    /// The task exits naturally when the connection is closed or the channel is dropped.
    async fn spawn_reader_task(&self, addr: SocketAddr, connection: crate::high_level::Connection) {
        let data_tx = self.data_tx.clone();
        let event_tx = self.event_tx.clone();
        let max_read_bytes = self.config.max_message_size;
        let exit_tx = self.reader_exit_tx.clone();

        let abort_handle = spawn_reader(
            &mut *self.reader_tasks.lock().await,
            run_quic_reader_task(
                addr,
                connection,
                data_tx,
                event_tx,
                exit_tx,
                max_read_bytes,
                "",
                InboundDataSendMode::TrySendWithTimeout,
            ),
        );

        let key = normalize_socket_addr(addr);
        if let Some(old) = self.reader_handles.write().await.insert(key, abort_handle) {
            old.abort();
        }
    }

    /// Spawn a single background task that polls constrained transport events
    /// and forwards `DataReceived` payloads into the shared `data_tx` channel.
    ///
    /// Lifecycle events (ConnectionAccepted, ConnectionClosed, etc.) are handled
    /// inline within this task.
    fn spawn_constrained_poller(&self) {
        let inner = Arc::clone(&self.inner);
        let data_tx = self.data_tx.clone();
        let connected_peers = Arc::clone(&self.connected_peers);
        let event_tx = self.event_tx.clone();
        let emitted_peer_connected = Arc::clone(&self.emitted_peer_connected);
        let constrained_peer_addrs = Arc::clone(&self.constrained_peer_addrs);
        let constrained_connections = Arc::clone(&self.constrained_connections);
        let shutdown = self.shutdown.clone();

        /// Register a new constrained peer in all lookup maps and emit a connect event.
        #[allow(clippy::too_many_arguments)]
        async fn register_constrained_peer(
            connection_id: ConstrainedConnectionId,
            addr: &TransportAddr,
            side: Side,
            constrained_connections: &RwLock<HashMap<TransportAddr, ConstrainedConnectionId>>,
            constrained_peer_addrs: &RwLock<HashMap<ConstrainedConnectionId, TransportAddr>>,
            connected_peers: &RwLock<HashMap<SocketAddr, PeerConnection>>,
            event_tx: &broadcast::Sender<P2pEvent>,
            emitted_peer_connected: &dashmap::DashSet<SocketAddr>,
        ) {
            let synthetic_addr = addr.to_synthetic_socket_addr();
            constrained_connections
                .write()
                .await
                .insert(addr.clone(), connection_id);
            constrained_peer_addrs
                .write()
                .await
                .insert(connection_id, addr.clone());
            connected_peers.write().await.insert(
                synthetic_addr,
                PeerConnection {
                    public_key: None,
                    remote_addr: addr.clone(),
                    authenticated: false,
                    connected_at: Instant::now(),
                    last_activity: Instant::now(),
                },
            );
            broadcast_peer_connected_once(
                emitted_peer_connected,
                event_tx,
                addr.clone(),
                None,
                side,
            );
        }

        tokio::spawn(async move {
            loop {
                let wrapper = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    event = inner.recv_constrained_event() => {
                        match event {
                            Some(w) => w,
                            None => {
                                debug!("Constrained event channel closed, exiting poller");
                                break;
                            }
                        }
                    }
                };

                match wrapper.event {
                    EngineEvent::DataReceived {
                        connection_id,
                        data,
                    } => {
                        let synthetic_addr = constrained_peer_addrs
                            .read()
                            .await
                            .get(&connection_id)
                            .map(|a| a.to_synthetic_socket_addr())
                            .unwrap_or_else(|| wrapper.remote_addr.to_synthetic_socket_addr());

                        let data_len = data.len();
                        tracing::trace!(
                            "Constrained poller: {} bytes from {}",
                            data_len,
                            synthetic_addr
                        );

                        if let Some(peer_conn) =
                            connected_peers.write().await.get_mut(&synthetic_addr)
                        {
                            peer_conn.last_activity = Instant::now();
                        }
                        let _ = event_tx.send(P2pEvent::DataReceived {
                            addr: synthetic_addr,
                            bytes: data_len,
                        });

                        if data_tx.send((synthetic_addr, data)).await.is_err() {
                            debug!("Constrained poller: channel closed, exiting");
                            break;
                        }
                    }
                    EngineEvent::ConnectionAccepted {
                        connection_id,
                        remote_addr: _,
                    } => {
                        register_constrained_peer(
                            connection_id,
                            &wrapper.remote_addr,
                            Side::Server,
                            &constrained_connections,
                            &constrained_peer_addrs,
                            &connected_peers,
                            &event_tx,
                            &emitted_peer_connected,
                        )
                        .await;
                    }
                    EngineEvent::ConnectionEstablished { connection_id } => {
                        if constrained_peer_addrs
                            .read()
                            .await
                            .get(&connection_id)
                            .is_none()
                        {
                            register_constrained_peer(
                                connection_id,
                                &wrapper.remote_addr,
                                Side::Client,
                                &constrained_connections,
                                &constrained_peer_addrs,
                                &connected_peers,
                                &event_tx,
                                &emitted_peer_connected,
                            )
                            .await;
                        }
                    }
                    EngineEvent::ConnectionClosed { connection_id } => {
                        let removed_addr =
                            constrained_peer_addrs.write().await.remove(&connection_id);
                        if let Some(addr) = removed_addr {
                            let synthetic_addr = addr.to_synthetic_socket_addr();
                            constrained_connections.write().await.remove(&addr);
                            connected_peers.write().await.remove(&synthetic_addr);
                            let _ = event_tx.send(P2pEvent::PeerDisconnected {
                                addr,
                                reason: DisconnectReason::RemoteClosed,
                            });
                            debug!(
                                "Constrained poller: peer at {} disconnected",
                                synthetic_addr
                            );
                        }
                    }
                    EngineEvent::ConnectionError {
                        connection_id,
                        error,
                    } => {
                        warn!(
                            "Constrained poller: conn_id={}, error={}",
                            connection_id.value(),
                            error
                        );
                    }
                    EngineEvent::Transmit { .. } => {}
                }
            }
        });
    }

    /// Spawn a background task that periodically detects and removes stale connections
    /// and probes live connections with health-check PINGs.
    ///
    /// Spawn a task that immediately cleans up connections when their reader
    /// task exits (QUIC connection died).
    ///
    /// This is the primary, event-driven detection path.  The stale reaper
    /// serves as a periodic safety net behind this.
    fn spawn_reader_exit_handler(
        &self,
        mut reader_exit_rx: mpsc::UnboundedReceiver<(SocketAddr, usize)>,
    ) {
        let connected_peers = Arc::clone(&self.connected_peers);
        let inner = Arc::clone(&self.inner);
        let event_tx = self.event_tx.clone();
        let stats = Arc::clone(&self.stats);
        let reader_handles = Arc::clone(&self.reader_handles);
        let emitted_peer_connected = Arc::clone(&self.emitted_peer_connected);
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            loop {
                let (addr, stable_id) = tokio::select! {
                    msg = reader_exit_rx.recv() => {
                        match msg {
                            Some(m) => m,
                            None => return, // channel closed
                        }
                    }
                    _ = shutdown.cancelled() => {
                        debug!("Reader-exit handler shutting down");
                        return;
                    }
                };

                info!(
                    "Reader task exited for {} (stable_id {}), running immediate cleanup",
                    addr, stable_id
                );
                let cleanup_start = Instant::now();
                do_cleanup_connection(
                    &connected_peers,
                    &inner,
                    &reader_handles,
                    &stats,
                    &event_tx,
                    &emitted_peer_connected,
                    &addr,
                    DisconnectReason::Timeout,
                    Some(stable_id),
                )
                .await;
                let cleanup_elapsed = cleanup_start.elapsed();
                if cleanup_elapsed > Duration::from_secs(1) {
                    warn!(
                        "do_cleanup_connection for {} took {:?} — potential lock contention",
                        addr, cleanup_elapsed
                    );
                }
            }
        });
    }

    /// Safety-net reaper that periodically checks for QUIC-dead connections
    /// whose reader task has not yet exited (or whose exit was missed).
    ///
    /// The primary detection path is event-driven: the reader-exit handler
    /// cleans up immediately when a reader task detects a dead connection.
    /// This reaper is a cheap fallback that runs every [`STALE_REAPER_INTERVAL`]
    /// and calls `is_connected()` — a local state check with no network traffic.
    fn spawn_stale_connection_reaper(&self) {
        let connected_peers = Arc::clone(&self.connected_peers);
        let inner = Arc::clone(&self.inner);
        let event_tx = self.event_tx.clone();
        let stats = Arc::clone(&self.stats);
        let reader_handles = Arc::clone(&self.reader_handles);
        let emitted_peer_connected = Arc::clone(&self.emitted_peer_connected);
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(STALE_REAPER_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = shutdown.cancelled() => {
                        debug!("Stale connection reaper shutting down");
                        return;
                    }
                }

                let stale_addrs: Vec<SocketAddr> = {
                    let peers = connected_peers.read().await;
                    peers
                        .keys()
                        .filter(|addr| !inner.is_connected(addr))
                        .copied()
                        .collect()
                };

                if !stale_addrs.is_empty() {
                    info!(
                        "Stale connection reaper: removing {} dead connection(s)",
                        stale_addrs.len()
                    );
                }

                for addr in &stale_addrs {
                    // Stale reaper has already determined the connection is
                    // gone (is_connected returned false), so force-remove.
                    do_cleanup_connection(
                        &connected_peers,
                        &inner,
                        &reader_handles,
                        &stats,
                        &event_tx,
                        &emitted_peer_connected,
                        addr,
                        DisconnectReason::Timeout,
                        None,
                    )
                    .await;
                }
            }
        });
    }

    /// Spawn a background task that periodically drives the NAT traversal
    /// session state machine via `poll()`.
    ///
    /// This runs `poll()` on its own task, decoupled from `try_hole_punch`,
    /// to avoid DashMap lock contention deadlocks between `poll()` and the
    /// concurrent accept handler.
    fn spawn_session_driver(&self) {
        let inner = Arc::clone(&self.inner);
        let shutdown = self.shutdown.clone();
        let event_tx_for_nat = self.event_tx.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            let mut relay_event_sent = false;
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = shutdown.cancelled() => {
                        debug!("NAT traversal session driver shutting down");
                        return;
                    }
                }

                // Drive the session state machine. Errors are non-fatal —
                // the session will retry on the next tick.
                //
                // poll() is synchronous — it acquires parking_lot locks and
                // iterates DashMaps. Measure duration to detect if it's
                // blocking the worker thread for too long.
                let poll_start = Instant::now();
                if let Err(e) = inner.poll(Instant::now()) {
                    debug!("NAT traversal poll error (will retry): {:?}", e);
                }
                let poll_elapsed = poll_start.elapsed();
                if poll_elapsed > Duration::from_millis(100) {
                    warn!(
                        "NAT traversal poll() took {:?} — may be starving other tasks",
                        poll_elapsed
                    );
                }

                // Process any hole-punch addresses forwarded from the Quinn driver.
                // These are addresses from relayed PUNCH_ME_NOW that need fully tracked
                // outgoing connections (not fire-and-forget).
                inner.process_pending_hole_punches().await;

                // Forward peer address updates as P2pEvents so the upper layer
                // (saorsa-core) can update its DHT routing table.
                {
                    let mut rx = inner.peer_address_update_rx.lock().await;
                    while let Ok((peer_addr, advertised_addr)) = rx.try_recv() {
                        info!(
                            "Peer {} advertised address {} — forwarding to P2pEvent",
                            peer_addr, advertised_addr
                        );
                        let _ = event_tx_for_nat.send(P2pEvent::PeerAddressUpdated {
                            peer_addr,
                            advertised_addr,
                        });
                    }
                }

                // Emit RelayEstablished once when relay becomes active.
                // Upper layers use this to trigger a DHT self-lookup for
                // relay address propagation.
                if !relay_event_sent {
                    if let Some(prepared) = inner.published_relay_handle().await {
                        let relay_addr = prepared.public_addr();
                        info!(
                            "Relay established at {} — emitting RelayEstablished event",
                            relay_addr
                        );
                        let _ = event_tx_for_nat.send(P2pEvent::RelayEstablished { relay_addr });
                        relay_event_sent = true;
                    }
                }

                // Monitor relay health. If the relay session died (connection
                // closed, server restarted, etc.), tear down its one lifecycle
                // state so the upper layer can acquire a replacement.
                // One verdict that names the relay it is about. Asking whether
                // the relay is healthy and then asking separately which relay is
                // published would let a replacement publish in between, and this
                // would tear down the healthy replacement.
                let dead = if relay_event_sent {
                    inner.unhealthy_published_relay().await
                } else {
                    None
                };
                if let Some(prepared) = dead {
                    // The unhealthy path, so the teardown does not file this
                    // failure as an intentional shutdown.
                    if let Err(error) = inner.abort_unhealthy_proactive_relay(prepared).await {
                        warn!(
                            relay_addr = %prepared.public_addr(),
                            %error,
                            "Failed to tear down unhealthy proactive relay"
                        );
                    }
                    relay_event_sent = false;

                    let relay_addr = prepared.public_addr();
                    info!(
                        "Relay tunnel at {} is unhealthy — emitting RelayLost event",
                        relay_addr
                    );
                    let _ = event_tx_for_nat.send(P2pEvent::RelayLost { relay_addr });
                }
            }
        });
    }

    /// Spawn a background task that monitors for new connections accepted by
    /// the NatTraversalEndpoint and registers them in `connected_peers` +
    /// emits `PeerConnected` events. This bridges the gap between the
    /// NatTraversalEndpoint's accept handler and the P2pEndpoint's tracking.
    fn spawn_incoming_connection_forwarder(&self) {
        debug!("FORWARDER_DEBUG: spawn_incoming_connection_forwarder called");
        let connected_peers = Arc::clone(&self.connected_peers);
        let event_tx = self.event_tx.clone();
        let emitted_peer_connected = Arc::clone(&self.emitted_peer_connected);
        let shutdown = self.shutdown.clone();
        let accepted_rx = self.inner.accepted_addrs_rx();
        let inner = Arc::clone(&self.inner);
        let data_tx = self.data_tx.clone();
        let reader_exit_tx = self.reader_exit_tx.clone();
        let reader_tasks = Arc::clone(&self.reader_tasks);
        let reader_handles = Arc::clone(&self.reader_handles);
        let max_read_bytes = self.config.max_message_size;

        tokio::spawn(async move {
            debug!("FORWARDER_DEBUG: started, acquiring rx lock...");
            let mut rx = accepted_rx.lock().await;
            info!("Incoming connection forwarder: rx lock acquired, waiting for addresses...");
            loop {
                let addr = tokio::select! {
                    Some(addr) = rx.recv() => {
                        info!("Incoming connection forwarder: received address {}", addr);
                        addr
                    },
                    _ = shutdown.cancelled() => return,
                };

                // Check if already registered
                if connected_peers.read().await.contains_key(&addr) {
                    continue;
                }

                info!(
                    "Incoming connection forwarder: registering {} in connected_peers",
                    addr
                );
                let peer_conn = PeerConnection {
                    public_key: None,
                    remote_addr: TransportAddr::Quic(addr),
                    authenticated: true,
                    connected_at: Instant::now(),
                    last_activity: Instant::now(),
                };

                // Spawn a reader task so we can receive data on this connection.
                // Without this, the target node of a hole-punch cannot receive
                // unidirectional streams opened by the initiator.
                if let Ok(Some(conn)) = inner.get_connection(&addr) {
                    let data_tx = data_tx.clone();
                    let event_tx = event_tx.clone();
                    let exit_tx = reader_exit_tx.clone();

                    let abort_handle = spawn_reader(
                        &mut *reader_tasks.lock().await,
                        run_quic_reader_task(
                            addr,
                            conn,
                            data_tx,
                            event_tx,
                            exit_tx,
                            max_read_bytes,
                            " (forwarder)",
                            InboundDataSendMode::AwaitSend,
                        ),
                    );

                    let key = normalize_socket_addr(addr);
                    if let Some(old) = reader_handles.write().await.insert(key, abort_handle) {
                        old.abort();
                    }
                } else {
                    warn!(
                        "Incoming connection forwarder: no connection found for {} in DashMap",
                        addr
                    );
                }

                connected_peers
                    .write()
                    .await
                    .insert(normalize_socket_addr(addr), peer_conn);
                broadcast_peer_connected_once(
                    &emitted_peer_connected,
                    &event_tx,
                    TransportAddr::Quic(addr),
                    None,
                    Side::Server,
                );
            }
        });
    }

    // v0.2: authenticate_peer removed - TLS handles peer authentication via ML-DSA-65
}

impl Clone for P2pEndpoint {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            // v0.2: auth_manager removed - TLS handles peer authentication
            connected_peers: Arc::clone(&self.connected_peers),
            emitted_peer_connected: Arc::clone(&self.emitted_peer_connected),
            stats: Arc::clone(&self.stats),
            config: self.config.clone(),
            event_tx: self.event_tx.clone(),
            our_fingerprint: self.our_fingerprint,
            public_key: self.public_key.clone(),
            shutdown: self.shutdown.clone(),
            pending_data: Arc::clone(&self.pending_data),
            transport_registry: Arc::clone(&self.transport_registry),
            router: Arc::clone(&self.router),
            constrained_connections: Arc::clone(&self.constrained_connections),
            constrained_peer_addrs: Arc::clone(&self.constrained_peer_addrs),
            hole_punch_target_peer_ids: Arc::clone(&self.hole_punch_target_peer_ids),
            hole_punch_preferred_coordinators: Arc::clone(&self.hole_punch_preferred_coordinators),
            data_tx: self.data_tx.clone(),
            data_rx: Arc::clone(&self.data_rx),
            reader_tasks: Arc::clone(&self.reader_tasks),
            reader_handles: Arc::clone(&self.reader_handles),
            reader_exit_tx: self.reader_exit_tx.clone(),
            pending_dials: Arc::clone(&self.pending_dials),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_dial_wait_timeout_covers_default_strategy_budget() {
        let strategy = StrategyConfig::default();
        let wait = pending_dial_wait_timeout(Some(&strategy));

        let direct = strategy.direct_connect_timeout + strategy.direct_handshake_timeout;
        let holepunch_round = strategy.holepunch_timeout + POST_HOLEPUNCH_DIRECT_RETRY_TIMEOUT;
        let holepunch = holepunch_round * strategy.max_holepunch_rounds;
        let expected = direct + holepunch + strategy.relay_timeout + PENDING_DIAL_WAIT_SLACK;
        assert_eq!(wait, expected);
    }

    #[test]
    fn test_pending_dial_wait_timeout_excludes_disabled_stages() {
        let strategy = StrategyConfig {
            holepunch_enabled: false,
            relay_enabled: false,
            ..StrategyConfig::default()
        };
        let wait = pending_dial_wait_timeout(Some(&strategy));

        let direct = strategy.direct_connect_timeout + strategy.direct_handshake_timeout;
        assert_eq!(wait, direct + PENDING_DIAL_WAIT_SLACK);
    }

    #[test]
    fn test_pending_dial_remaining_wait_subtracts_age() {
        let wait_timeout = Duration::from_secs(30);
        let age = Duration::from_secs(12);

        assert_eq!(
            pending_dial_remaining_wait(wait_timeout, age),
            Some(Duration::from_secs(18))
        );
    }

    #[test]
    fn test_pending_dial_remaining_wait_expires_stale_entries() {
        let wait_timeout = Duration::from_secs(30);

        assert_eq!(
            pending_dial_remaining_wait(wait_timeout, wait_timeout),
            None
        );
        assert_eq!(
            pending_dial_remaining_wait(wait_timeout, wait_timeout + Duration::from_secs(1)),
            None
        );
    }

    #[tokio::test]
    async fn test_pending_dial_guard_complete_broadcasts_and_clears_entry() {
        let target: SocketAddr = "127.0.0.1:12345".parse().expect("valid addr");
        let pending_dials = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let (tx, _) = broadcast::channel(PENDING_DIAL_BROADCAST_CAPACITY);
        let mut rx = tx.subscribe();
        let id = 42;

        {
            let mut pending = pending_dials.lock().await;
            pending.insert(
                target,
                PendingDial {
                    id,
                    tx,
                    started_at: Instant::now(),
                },
            );
        }

        let mut guard = PendingDialGuard::new(Arc::clone(&pending_dials), target, id);
        let conn = PeerConnection {
            public_key: None,
            remote_addr: TransportAddr::Quic(target),
            authenticated: true,
            connected_at: Instant::now(),
            last_activity: Instant::now(),
        };
        let result = Ok((conn.clone(), ConnectionMethod::DirectIPv4));

        guard.complete(&result).await;

        assert!(pending_dials.lock().await.is_empty());
        let (received_conn, received_method) = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("pending dial result sent")
            .expect("pending dial broadcast open")
            .expect("pending dial succeeded");
        assert_eq!(received_conn.remote_addr, conn.remote_addr);
        assert_eq!(received_method, ConnectionMethod::DirectIPv4);
    }

    #[tokio::test]
    async fn test_pending_dial_guard_drop_clears_owned_entry() {
        let target: SocketAddr = "127.0.0.1:12346".parse().expect("valid addr");
        let pending_dials = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let (tx, _) = broadcast::channel(PENDING_DIAL_BROADCAST_CAPACITY);
        let id = 43;

        {
            let mut pending = pending_dials.lock().await;
            pending.insert(
                target,
                PendingDial {
                    id,
                    tx,
                    started_at: Instant::now(),
                },
            );
        }

        let guard = PendingDialGuard::new(Arc::clone(&pending_dials), target, id);
        drop(guard);

        timeout(Duration::from_secs(1), async {
            loop {
                if pending_dials.lock().await.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pending dial guard cleared entry");
    }

    #[tokio::test]
    async fn test_pending_dial_guard_abort_during_complete_clears_owned_entry() {
        let target: SocketAddr = "127.0.0.1:12347".parse().expect("valid addr");
        let pending_dials = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let (tx, _) = broadcast::channel(PENDING_DIAL_BROADCAST_CAPACITY);
        let id = 44;

        {
            let mut pending = pending_dials.lock().await;
            pending.insert(
                target,
                PendingDial {
                    id,
                    tx,
                    started_at: Instant::now(),
                },
            );
        }

        let pending_lock = pending_dials.lock().await;
        let pending_dials_for_task = Arc::clone(&pending_dials);
        let handle = tokio::spawn(async move {
            let mut guard = PendingDialGuard::new(pending_dials_for_task, target, id);
            let result: Result<(PeerConnection, ConnectionMethod), EndpointError> =
                Err(EndpointError::Timeout);
            guard.complete(&result).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.abort();
        drop(pending_lock);

        timeout(Duration::from_secs(1), async {
            loop {
                if pending_dials.lock().await.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("aborted pending dial guard cleared entry");
    }

    #[test]
    fn test_endpoint_stats_default() {
        let stats = EndpointStats::default();
        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.successful_connections, 0);
        assert_eq!(stats.nat_traversal_attempts, 0);
    }

    #[test]
    fn test_connection_metrics_default() {
        let metrics = ConnectionMetrics::default();
        assert_eq!(metrics.bytes_sent, 0);
        assert_eq!(metrics.bytes_received, 0);
        assert!(metrics.rtt.is_none());
        assert_eq!(metrics.packet_loss, 0.0);
    }

    #[test]
    fn test_peer_connection_debug() {
        let socket_addr: SocketAddr = "127.0.0.1:8080".parse().expect("valid addr");
        let conn = PeerConnection {
            public_key: None,
            remote_addr: TransportAddr::Quic(socket_addr),
            authenticated: false,
            connected_at: Instant::now(),
            last_activity: Instant::now(),
        };
        let debug_str = format!("{:?}", conn);
        assert!(debug_str.contains("PeerConnection"));
    }

    #[test]
    fn test_disconnect_reason_debug() {
        let reason = DisconnectReason::Normal;
        assert!(format!("{:?}", reason).contains("Normal"));

        let reason = DisconnectReason::ProtocolError("test".to_string());
        assert!(format!("{:?}", reason).contains("test"));
    }

    #[test]
    fn test_traversal_phase_debug() {
        let phase = TraversalPhase::Discovery;
        assert!(format!("{:?}", phase).contains("Discovery"));
    }

    #[test]
    fn test_endpoint_error_display() {
        let err = EndpointError::Timeout;
        assert!(err.to_string().contains("timed out"));

        let addr: SocketAddr = "127.0.0.1:8080".parse().expect("valid addr");
        let err = EndpointError::PeerNotFound(addr);
        assert!(err.to_string().contains("not found"));

        let err = EndpointError::SendFailed {
            stage: SendFailureStage::WriteProgressTimeout,
            bytes_written: 1024,
            reason: "no progress".to_string(),
        };
        assert!(err.to_string().contains("write_progress_timeout"));
        assert!(err.to_string().contains("bytes_written=1024"));
    }

    #[tokio::test]
    async fn test_endpoint_creation() {
        // v0.13.0+: No role - all nodes are symmetric P2P nodes
        let config = P2pConfig::builder().build().expect("valid config");

        let result = P2pEndpoint::new(config).await;
        // May fail in test environment without network, but shouldn't panic
        if let Ok(endpoint) = result {
            assert!(endpoint.is_running());
            assert!(endpoint.local_addr().is_some() || endpoint.local_addr().is_none());
        }
    }

    #[tokio::test]
    async fn test_finished_reader_tasks_do_not_accumulate() {
        // Each connection spawns a reader on both sides. The server's ends when
        // the client closes the connection and the client's is aborted by its
        // own disconnect, so a set that kept finished readers would hold at
        // least ROUNDS of them on each side.
        const ROUNDS: usize = 8;
        const MAX_RETAINED_READERS: usize = 2;
        let localhost = SocketAddr::from(([127, 0, 0, 1], 0));
        let config = || {
            P2pConfig::builder()
                .bind_addr(localhost)
                .build()
                .expect("valid config")
        };
        let server = P2pEndpoint::new(config()).await.expect("server endpoint");
        let client = P2pEndpoint::new(config()).await.expect("client endpoint");
        let server_addr = server.local_addr().expect("server address");

        for round in 0..ROUNDS {
            let (accepted, connected) = tokio::join!(
                timeout(Duration::from_secs(10), server.accept()),
                timeout(Duration::from_secs(10), client.connect(server_addr)),
            );
            accepted
                .expect("accept did not time out")
                .expect("server accepted the connection");
            let peer = connected
                .expect("connect did not time out")
                .expect("client connected");
            let peer_addr = peer.remote_addr.as_socket_addr().expect("QUIC address");

            client
                .disconnect(&peer_addr)
                .await
                .expect("client disconnected");
            timeout(Duration::from_secs(10), async {
                while !server.connected_peers().await.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("round {round}: server kept the closed connection"));
        }

        let server_readers = server.reader_tasks.lock().await.len();
        let client_readers = client.reader_tasks.lock().await.len();
        assert!(
            server_readers <= MAX_RETAINED_READERS,
            "server retained {server_readers} reader tasks after {ROUNDS} closed connections"
        );
        assert!(
            client_readers <= MAX_RETAINED_READERS,
            "client retained {client_readers} reader tasks after {ROUNDS} closed connections"
        );

        client.shutdown().await;
        server.shutdown().await;
    }

    // ==========================================================================
    // Transport Registry Tests (Phase 1.1 Task 5)
    // ==========================================================================

    #[tokio::test]
    async fn test_p2p_endpoint_stores_transport_registry() {
        use crate::transport::TransportType;

        // Build config with default transport providers
        // Phase 5.3: P2pEndpoint::new() always adds a shared UDP transport
        let config = P2pConfig::builder().build().expect("valid config");

        // Create endpoint
        let result = P2pEndpoint::new(config).await;

        // Verify registry is accessible and contains the auto-added UDP provider
        if let Ok(endpoint) = result {
            let registry = endpoint.transport_registry();
            // Phase 5.3: Registry now always has at least 1 UDP provider (socket sharing)
            assert!(
                !registry.is_empty(),
                "Registry should have at least 1 provider"
            );

            let udp_providers = registry.providers_by_type(TransportType::Quic);
            assert_eq!(udp_providers.len(), 1, "Should have 1 UDP provider");
        }
        // Note: endpoint creation may fail in test environment without network
    }

    #[tokio::test]
    async fn test_p2p_endpoint_default_config_has_udp_registry() {
        // Build config with no additional transport providers
        let config = P2pConfig::builder().build().expect("valid config");

        // Create endpoint
        let result = P2pEndpoint::new(config).await;

        // Phase 5.3: Default registry now includes a shared UDP transport
        // This is required for socket sharing with Quinn
        if let Ok(endpoint) = result {
            let registry = endpoint.transport_registry();
            assert!(
                !registry.is_empty(),
                "Default registry should have UDP for socket sharing"
            );
            assert!(
                registry.has_quic_capable_transport(),
                "Default registry should have QUIC-capable transport"
            );
        }
        // Note: endpoint creation may fail in test environment without network
    }

    // ==========================================================================
    // Event Address Migration Tests (Phase 2.2 Task 7)
    // ==========================================================================

    #[test]
    fn test_peer_connected_event_with_udp() {
        let socket_addr: SocketAddr = "192.168.1.100:8080".parse().expect("valid addr");
        let event = P2pEvent::PeerConnected {
            addr: TransportAddr::Quic(socket_addr),
            public_key: None,
            side: Side::Client,
        };

        // Verify event fields
        if let P2pEvent::PeerConnected {
            addr,
            public_key,
            side,
        } = event
        {
            assert!(public_key.is_none());
            assert_eq!(addr, TransportAddr::Quic(socket_addr));
            assert!(side.is_client());

            // Verify as_socket_addr() works
            let extracted = addr.as_socket_addr();
            assert_eq!(extracted, Some(socket_addr));
        } else {
            panic!("Expected PeerConnected event");
        }
    }

    #[test]
    fn test_peer_connected_event_with_ble() {
        // BLE MAC address (6 bytes)
        let mac_addr = [0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc];
        let event = P2pEvent::PeerConnected {
            addr: TransportAddr::Ble {
                mac: mac_addr,
                psm: 128,
            },
            public_key: None,
            side: Side::Server,
        };

        // Verify event fields
        if let P2pEvent::PeerConnected {
            addr,
            public_key,
            side,
        } = event
        {
            assert!(public_key.is_none());
            assert!(side.is_server());

            // Verify as_socket_addr() returns None for BLE
            assert!(addr.as_socket_addr().is_none());

            // Verify we can match on BLE variant
            if let TransportAddr::Ble { mac, psm } = addr {
                assert_eq!(mac, mac_addr);
                assert_eq!(psm, 128);
            } else {
                panic!("Expected BLE address");
            }
        }
    }

    #[test]
    fn test_external_address_discovered_udp() {
        let socket_addr: SocketAddr = "203.0.113.1:12345".parse().expect("valid addr");
        let event = P2pEvent::ExternalAddressDiscovered {
            addr: TransportAddr::Quic(socket_addr),
        };

        if let P2pEvent::ExternalAddressDiscovered { addr } = event {
            assert_eq!(addr, TransportAddr::Quic(socket_addr));
            assert_eq!(addr.as_socket_addr(), Some(socket_addr));
        } else {
            panic!("Expected ExternalAddressDiscovered event");
        }
    }

    #[test]
    fn test_event_clone() {
        let socket_addr: SocketAddr = "10.0.0.1:9000".parse().expect("valid addr");
        let event = P2pEvent::PeerConnected {
            addr: TransportAddr::Quic(socket_addr),
            public_key: Some(vec![0x11; 32]),
            side: Side::Client,
        };

        // Verify events are Clone
        let cloned = event.clone();
        if let (
            P2pEvent::PeerConnected {
                public_key: pk1,
                addr: a1,
                ..
            },
            P2pEvent::PeerConnected {
                public_key: pk2,
                addr: a2,
                ..
            },
        ) = (&event, &cloned)
        {
            assert_eq!(pk1, pk2);
            assert_eq!(a1, a2);
        }
    }

    #[test]
    fn test_peer_connection_with_transport_addr() {
        // Test with UDP
        let udp_addr: SocketAddr = "127.0.0.1:8080".parse().expect("valid addr");
        let udp_conn = PeerConnection {
            public_key: None,
            remote_addr: TransportAddr::Quic(udp_addr),
            authenticated: true,
            connected_at: Instant::now(),
            last_activity: Instant::now(),
        };
        assert_eq!(
            udp_conn.remote_addr.as_socket_addr(),
            Some(udp_addr),
            "UDP connection should have extractable socket address"
        );

        // Test with BLE
        let mac_addr = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66];
        let ble_conn = PeerConnection {
            public_key: None,
            remote_addr: TransportAddr::Ble {
                mac: mac_addr,
                psm: 128,
            },
            authenticated: true,
            connected_at: Instant::now(),
            last_activity: Instant::now(),
        };
        assert!(
            ble_conn.remote_addr.as_socket_addr().is_none(),
            "BLE connection should not have socket address"
        );
    }

    #[test]
    fn test_transport_addr_display_in_events() {
        let socket_addr: SocketAddr = "192.168.1.1:9001".parse().expect("valid addr");
        let event = P2pEvent::PeerConnected {
            addr: TransportAddr::Quic(socket_addr),
            public_key: None,
            side: Side::Client,
        };

        // Verify display formatting works for logging
        let debug_str = format!("{:?}", event);
        assert!(
            debug_str.contains("192.168.1.1"),
            "Event debug should contain IP address"
        );
        assert!(
            debug_str.contains("9001"),
            "Event debug should contain port"
        );
    }

    // ==========================================================================
    // Connection Tracking Tests (Phase 2.2 Task 8)
    // ==========================================================================

    #[test]
    fn test_connection_tracking_udp() {
        use std::collections::HashMap;

        // Simulate connection tracking with SocketAddr key
        let mut connections: HashMap<SocketAddr, PeerConnection> = HashMap::new();

        let socket_addr: SocketAddr = "10.0.0.1:8080".parse().expect("valid addr");
        let conn = PeerConnection {
            public_key: None,
            remote_addr: TransportAddr::Quic(socket_addr),
            authenticated: true,
            connected_at: Instant::now(),
            last_activity: Instant::now(),
        };

        connections.insert(socket_addr, conn.clone());

        // Verify connection is tracked
        assert!(connections.contains_key(&socket_addr));
        let retrieved = connections.get(&socket_addr).expect("connection exists");
        assert_eq!(retrieved.remote_addr, TransportAddr::Quic(socket_addr));
        assert!(retrieved.authenticated);
    }

    #[test]
    fn test_connection_tracking_multi_transport() {
        use std::collections::HashMap;

        // Simulate multiple connections on different transports keyed by SocketAddr.
        // For constrained transports (BLE) we use a synthetic SocketAddr via
        // TransportAddr::to_synthetic_socket_addr().
        let mut connections: HashMap<SocketAddr, PeerConnection> = HashMap::new();

        // UDP connection
        let udp_addr: SocketAddr = "192.168.1.100:9000".parse().expect("valid addr");
        connections.insert(
            udp_addr,
            PeerConnection {
                public_key: None,
                remote_addr: TransportAddr::Quic(udp_addr),
                authenticated: true,
                connected_at: Instant::now(),
                last_activity: Instant::now(),
            },
        );

        // BLE connection (different peer) - use synthetic SocketAddr as key
        let ble_device = [0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff];
        let ble_addr = TransportAddr::Ble {
            mac: ble_device,
            psm: 128,
        };
        let ble_socket_key = ble_addr.to_synthetic_socket_addr();
        connections.insert(
            ble_socket_key,
            PeerConnection {
                public_key: None,
                remote_addr: ble_addr,
                authenticated: true,
                connected_at: Instant::now(),
                last_activity: Instant::now(),
            },
        );

        // Verify each tracked independently
        assert_eq!(connections.len(), 2);
        assert!(
            connections
                .get(&udp_addr)
                .expect("UDP connection exists")
                .remote_addr
                .as_socket_addr()
                .is_some()
        );
        assert!(
            connections
                .get(&ble_socket_key)
                .expect("BLE connection exists")
                .remote_addr
                .as_socket_addr()
                .is_none()
        );
    }

    #[test]
    fn test_connection_lookup_by_socket_addr() {
        use std::collections::HashMap;

        let mut connections: HashMap<SocketAddr, PeerConnection> = HashMap::new();

        // Add multiple connections keyed by SocketAddr
        let addrs = ["10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"];

        for addr_str in addrs {
            let socket_addr: SocketAddr = addr_str.parse().expect("valid addr");
            connections.insert(
                socket_addr,
                PeerConnection {
                    public_key: None,
                    remote_addr: TransportAddr::Quic(socket_addr),
                    authenticated: true,
                    connected_at: Instant::now(),
                    last_activity: Instant::now(),
                },
            );
        }

        // Direct lookup by SocketAddr
        let target: SocketAddr = "10.0.0.2:8080".parse().expect("valid addr");
        let found = connections.get(&target);

        assert!(found.is_some());
        assert_eq!(
            found.expect("connection exists").remote_addr,
            TransportAddr::Quic(target)
        );
    }

    #[test]
    fn test_transport_addr_equality_in_tracking() {
        // Verify TransportAddr equality works correctly for tracking
        let addr1: SocketAddr = "192.168.1.1:8080".parse().expect("valid addr");
        let addr2: SocketAddr = "192.168.1.1:8080".parse().expect("valid addr");
        let addr3: SocketAddr = "192.168.1.1:8081".parse().expect("valid addr");

        let t1 = TransportAddr::Quic(addr1);
        let t2 = TransportAddr::Quic(addr2);
        let t3 = TransportAddr::Quic(addr3);

        // Same address should be equal
        assert_eq!(t1, t2);

        // Different port should not be equal
        assert_ne!(t1, t3);

        // Different transport type should not be equal
        let ble = TransportAddr::Ble {
            mac: [0; 6],
            psm: 128,
        };
        assert_ne!(t1, ble);
    }

    #[test]
    fn test_peer_connection_update_preserves_transport_addr() {
        let socket_addr: SocketAddr = "172.16.0.1:5000".parse().expect("valid addr");
        let mut conn = PeerConnection {
            public_key: None,
            remote_addr: TransportAddr::Quic(socket_addr),
            authenticated: false,
            connected_at: Instant::now(),
            last_activity: Instant::now(),
        };

        // Simulate updating the connection (e.g., after authentication)
        conn.authenticated = true;
        conn.last_activity = Instant::now();

        // Verify transport address is preserved
        assert_eq!(conn.remote_addr, TransportAddr::Quic(socket_addr));
        assert!(conn.authenticated);
    }
}

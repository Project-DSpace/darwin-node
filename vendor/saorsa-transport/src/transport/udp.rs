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

//! UDP transport provider implementation
//!
//! This module implements the [`TransportProvider`] trait for UDP/IP sockets,
//! providing high-bandwidth, low-latency transport for standard Internet connectivity.
//!
//! The UDP transport is the default and most capable transport, supporting:
//! - Full QUIC protocol
//! - IPv4 and IPv6 dual-stack
//! - Broadcast on local networks
//! - No link-layer acknowledgements (QUIC handles reliability)

use async_trait::async_trait;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use super::addr::{TransportAddr, TransportType};
use super::capabilities::TransportCapabilities;
use super::provider::{
    InboundDatagram, LinkQuality, TransportError, TransportProvider, TransportStats,
};

/// UDP transport provider for standard Internet connectivity
///
/// This is the primary transport for saorsa-transport, providing high-bandwidth,
/// low-latency connectivity over UDP/IP.
pub struct UdpTransport {
    socket: Arc<UdpSocket>,
    capabilities: TransportCapabilities,
    local_addr: SocketAddr,
    online: AtomicBool,
    /// Whether the socket has been delegated to Quinn (recv handled externally)
    delegated_to_quinn: AtomicBool,
    stats: Arc<UdpTransportStats>,
    inbound_tx: mpsc::Sender<InboundDatagram>,
    shutdown_tx: mpsc::Sender<()>,
}

/// Live counters for one [`UdpTransport`] socket (V2-834 Part A.5).
///
/// Relaxed atomics bumped on the send and receive paths; a snapshot is
/// available through [`TransportProvider::stats`] and the live handle through
/// [`UdpTransport::raw_stats`]. When the socket has been delegated to the QUIC
/// endpoint the receive side is driven by the endpoint instead, so the
/// `*_received` counters stay at zero and the bytes appear in the endpoint's
/// socket totals.
#[derive(Debug)]
pub struct UdpTransportStats {
    /// Datagrams accepted by the kernel on `send_to`.
    pub datagrams_sent: AtomicU64,
    /// Datagrams returned by `recv_from` on the transport's own recv loop.
    pub datagrams_received: AtomicU64,
    /// Bytes accepted by the kernel on `send_to`.
    pub bytes_sent: AtomicU64,
    /// Bytes returned by `recv_from` on the transport's own recv loop.
    pub bytes_received: AtomicU64,
    /// Failed `send_to` calls.
    pub send_errors: AtomicU64,
    /// Failed `recv_from` calls.
    pub receive_errors: AtomicU64,
}

impl Default for UdpTransportStats {
    fn default() -> Self {
        Self {
            datagrams_sent: AtomicU64::new(0),
            datagrams_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            send_errors: AtomicU64::new(0),
            receive_errors: AtomicU64::new(0),
        }
    }
}

impl UdpTransport {
    /// Bind a new UDP transport to the specified address
    ///
    /// # Arguments
    ///
    /// * `addr` - The socket address to bind to. Use `0.0.0.0:0` for automatic port selection.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be bound.
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        let local_addr = socket.local_addr()?;
        let socket = Arc::new(socket);

        let (inbound_tx, _) = mpsc::channel(1024);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let transport = Self {
            socket: socket.clone(),
            capabilities: TransportCapabilities::broadband(),
            local_addr,
            online: AtomicBool::new(true),
            delegated_to_quinn: AtomicBool::new(false),
            stats: Arc::new(UdpTransportStats::default()),
            inbound_tx,
            shutdown_tx,
        };

        // Spawn receive loop
        transport.spawn_recv_loop(socket, shutdown_rx);

        Ok(transport)
    }

    /// Bind a new UDP transport for use with Quinn (no recv loop)
    ///
    /// This creates a transport where the socket will be shared with Quinn's
    /// QUIC endpoint. The transport can still send, but receiving is handled
    /// by Quinn's internal polling.
    ///
    /// # Arguments
    ///
    /// * `addr` - The socket address to bind to. Use `0.0.0.0:0` for automatic port selection.
    ///
    /// # Returns
    ///
    /// Returns a tuple of:
    /// - The `UdpTransport` for use in the transport registry
    /// - The `std::net::UdpSocket` for Quinn's endpoint
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be bound.
    pub async fn bind_for_quinn(addr: SocketAddr) -> io::Result<(Self, std::net::UdpSocket)> {
        let socket = UdpSocket::bind(addr).await?;
        let local_addr = socket.local_addr()?;

        // Convert to std socket for Quinn
        let std_socket = socket.into_std()?;

        // Recreate tokio socket from the std socket (they share the underlying fd)
        let std_socket_for_transport = std_socket.try_clone()?;
        let tokio_socket = UdpSocket::from_std(std_socket_for_transport)?;
        let socket_arc = Arc::new(tokio_socket);

        let (inbound_tx, _) = mpsc::channel(1024);
        let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);

        let transport = Self {
            socket: socket_arc,
            capabilities: TransportCapabilities::broadband(),
            local_addr,
            online: AtomicBool::new(true),
            delegated_to_quinn: AtomicBool::new(true), // Quinn handles recv
            stats: Arc::new(UdpTransportStats::default()),
            inbound_tx,
            shutdown_tx,
        };

        // Do NOT spawn recv loop - Quinn will handle packet reception

        Ok((transport, std_socket))
    }

    /// Create a UDP transport from an existing socket
    ///
    /// This is useful when you want to share a socket with other components.
    /// Note: This spawns a recv loop, so don't use this if Quinn will handle recv.
    /// Use `bind_for_quinn()` instead for Quinn integration.
    pub fn from_socket(socket: Arc<UdpSocket>, local_addr: SocketAddr) -> Self {
        let (inbound_tx, _) = mpsc::channel(1024);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let transport = Self {
            socket: socket.clone(),
            capabilities: TransportCapabilities::broadband(),
            local_addr,
            online: AtomicBool::new(true),
            delegated_to_quinn: AtomicBool::new(false),
            stats: Arc::new(UdpTransportStats::default()),
            inbound_tx,
            shutdown_tx,
        };

        transport.spawn_recv_loop(socket, shutdown_rx);
        transport
    }

    /// Check if this transport's recv is delegated to Quinn
    ///
    /// When true, the socket is shared with Quinn's QUIC endpoint and
    /// packet reception is handled by Quinn, not this transport.
    pub fn is_delegated_to_quinn(&self) -> bool {
        self.delegated_to_quinn.load(Ordering::SeqCst)
    }

    /// Live counters for this socket.
    pub fn raw_stats(&self) -> Arc<UdpTransportStats> {
        Arc::clone(&self.stats)
    }

    fn spawn_recv_loop(&self, socket: Arc<UdpSocket>, mut shutdown_rx: mpsc::Receiver<()>) {
        let inbound_tx = self.inbound_tx.clone();
        let stats = Arc::clone(&self.stats);
        let online = self.online.load(Ordering::SeqCst);

        if !online {
            return;
        }

        // Note: This is a simplified receive loop for the transport abstraction.
        // In practice, the actual packet reception is handled by the QUIC endpoint's
        // polling mechanism, not this transport directly.
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];

            loop {
                tokio::select! {
                    result = socket.recv_from(&mut buf) => {
                        match result {
                            Ok((len, source)) => {
                                stats.datagrams_received.fetch_add(1, Ordering::Relaxed);
                                stats.bytes_received.fetch_add(len as u64, Ordering::Relaxed);
                                let datagram = InboundDatagram {
                                    data: buf[..len].to_vec(),
                                    source: TransportAddr::Quic(source),
                                    received_at: Instant::now(),
                                    link_quality: None,
                                };

                                // Best-effort send; drop if channel is full
                                let _ = inbound_tx.try_send(datagram);
                            }
                            Err(_) => {
                                // Receive error, but continue trying
                                stats.receive_errors.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
    }

    /// Get the underlying UDP socket
    pub fn socket(&self) -> &Arc<UdpSocket> {
        &self.socket
    }

    /// Get the local address this transport is bound to
    pub fn local_address(&self) -> SocketAddr {
        self.local_addr
    }
}

#[async_trait]
impl TransportProvider for UdpTransport {
    fn name(&self) -> &str {
        "UDP"
    }

    fn transport_type(&self) -> TransportType {
        TransportType::Quic
    }

    fn capabilities(&self) -> &TransportCapabilities {
        &self.capabilities
    }

    fn local_addr(&self) -> Option<TransportAddr> {
        Some(TransportAddr::Quic(self.local_addr))
    }

    async fn send(&self, data: &[u8], dest: &TransportAddr) -> Result<(), TransportError> {
        if !self.online.load(Ordering::SeqCst) {
            return Err(TransportError::Offline);
        }

        let socket_addr = match dest {
            TransportAddr::Quic(addr) => *addr,
            _ => {
                return Err(TransportError::AddressMismatch {
                    expected: TransportType::Quic,
                    actual: dest.transport_type(),
                });
            }
        };

        if data.len() > self.capabilities.mtu {
            return Err(TransportError::MessageTooLarge {
                size: data.len(),
                mtu: self.capabilities.mtu,
            });
        }

        match self.socket.send_to(data, socket_addr).await {
            Ok(sent) => {
                self.stats.datagrams_sent.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .bytes_sent
                    .fetch_add(sent as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats.send_errors.fetch_add(1, Ordering::Relaxed);
                Err(TransportError::SendFailed {
                    reason: e.to_string(),
                })
            }
        }
    }

    fn inbound(&self) -> mpsc::Receiver<InboundDatagram> {
        // Create a new receiver from the same channel
        // Note: In a real implementation, you might want to use a broadcast channel
        // or have the endpoint subscribe to the transport's inbound stream.
        let (_, rx) = mpsc::channel(1024);
        rx
    }

    fn is_online(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        self.online.store(false, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(()).await;
        Ok(())
    }

    async fn broadcast(&self, data: &[u8]) -> Result<(), TransportError> {
        // UDP supports broadcast
        if !self.capabilities.broadcast {
            return Err(TransportError::BroadcastNotSupported);
        }

        // Broadcast to 255.255.255.255 on the same port
        let broadcast_addr = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::BROADCAST),
            self.local_addr.port(),
        );

        self.send(data, &TransportAddr::Quic(broadcast_addr)).await
    }

    async fn link_quality(&self, _peer: &TransportAddr) -> Option<LinkQuality> {
        // UDP doesn't provide link quality metrics directly
        None
    }

    fn stats(&self) -> TransportStats {
        TransportStats {
            datagrams_sent: self.stats.datagrams_sent.load(Ordering::Relaxed),
            datagrams_received: self.stats.datagrams_received.load(Ordering::Relaxed),
            bytes_sent: self.stats.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.stats.bytes_received.load(Ordering::Relaxed),
            send_errors: self.stats.send_errors.load(Ordering::Relaxed),
            receive_errors: self.stats.receive_errors.load(Ordering::Relaxed),
            current_rtt: None,
        }
    }

    fn socket(&self) -> Option<&Arc<UdpSocket>> {
        Some(&self.socket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_udp_transport_bind() {
        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        assert!(transport.is_online());
        assert_eq!(transport.transport_type(), TransportType::Quic);
        assert!(transport.capabilities().supports_full_quic());

        let local_addr = transport.local_addr();
        assert!(local_addr.is_some());
        if let Some(TransportAddr::Quic(addr)) = local_addr {
            assert_eq!(
                addr.ip(),
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
            );
            assert_ne!(addr.port(), 0);
        }
    }

    #[tokio::test]
    async fn test_udp_transport_send() {
        let transport1 = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let transport2 = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let dest = transport2.local_addr().unwrap();
        let result = transport1.send(b"hello", &dest).await;
        assert!(result.is_ok());

        let stats = transport1.stats();
        assert_eq!(stats.datagrams_sent, 1);
        assert_eq!(stats.bytes_sent, 5);
    }

    #[tokio::test]
    async fn test_udp_transport_address_mismatch() {
        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let ble_addr = TransportAddr::ble([0x00, 0x11, 0x22, 0x33, 0x44, 0x55], 128);
        let result = transport.send(b"hello", &ble_addr).await;

        match result {
            Err(TransportError::AddressMismatch { expected, actual }) => {
                assert_eq!(expected, TransportType::Quic);
                assert_eq!(actual, TransportType::Ble);
            }
            _ => panic!("expected AddressMismatch error"),
        }
    }

    #[tokio::test]
    async fn test_udp_transport_shutdown() {
        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        assert!(transport.is_online());
        transport.shutdown().await.unwrap();
        assert!(!transport.is_online());

        // Sending after shutdown should fail
        let dest = TransportAddr::Quic("127.0.0.1:9000".parse().unwrap());
        let result = transport.send(b"hello", &dest).await;
        assert!(matches!(result, Err(TransportError::Offline)));
    }

    #[test]
    fn test_udp_capabilities() {
        let caps = TransportCapabilities::broadband();

        assert!(caps.supports_full_quic());
        assert!(!caps.half_duplex);
        assert!(caps.broadcast);
        assert!(!caps.metered);
        assert!(!caps.power_constrained);
    }

    #[tokio::test]
    async fn test_udp_transport_socket_accessor() {
        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        // Test the inherent socket() method
        let socket_ref = transport.socket();
        assert!(socket_ref.local_addr().is_ok());

        // Test the trait method via TransportProvider
        let provider: &dyn TransportProvider = &transport;
        let socket_opt = provider.socket();
        assert!(socket_opt.is_some());
        assert!(socket_opt.unwrap().local_addr().is_ok());
    }
}

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

//! Saorsa's signaling-free WebRTC Direct transport.
//!
//! This module deliberately exposes no libp2p types or wire protocols. It uses
//! WebRTC ICE-lite, DTLS, SCTP, and reliable ordered DataChannels directly.
//! A browser learns the listener's literal IP address, UDP port, and pinned
//! certificate hash from a bootstrap record, so no DNS or signaling service is
//! required.
//!
//! The browser keeps the ICE credentials generated for its local SDP offer and
//! synthesizes the listener's SDP answer from the bootstrap endpoint. The
//! answer's username fragment uses [`ICE_CREDENTIAL_PREFIX`] and carries the
//! browser-generated ICE password. The listener recovers that password, the
//! browser's username fragment, and its observed address from the first STUN
//! binding request, then constructs the corresponding peer connection locally.

#[path = "admission.rs"]
mod admission;
#[path = "diagnostics.rs"]
mod diagnostics;
use diagnostics::{ConnectionObserver, ConnectionTracker};
pub use diagnostics::{WebRtcConnectionMetrics, WebRtcDiagnostics, WebRtcDiagnosticsSnapshot};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rand::distributions::{Alphanumeric, DistString};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use stun::attributes::ATTR_USERNAME;
use stun::message::{Message as StunMessage, is_message as is_stun_message};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use webrtc_rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
use webrtc_stack::api::setting_engine::SettingEngine;
use webrtc_stack::api::{APIBuilder, interceptor_registry::register_default_interceptors};
use webrtc_stack::data::data_channel::DataChannel;
use webrtc_stack::data_channel::RTCDataChannel;
use webrtc_stack::dtls::extension::extension_use_srtp::SrtpProtectionProfile;
use webrtc_stack::dtls_transport::RTCDtlsTransport;
use webrtc_stack::dtls_transport::dtls_role::DTLSRole;
use webrtc_stack::dtls_transport::dtls_transport_state::RTCDtlsTransportState;
use webrtc_stack::ice::mdns::MulticastDnsMode;
use webrtc_stack::ice::network_type::NetworkType;
use webrtc_stack::ice::udp_mux::{UDPMux, UDPMuxConn, UDPMuxConnParams, UDPMuxWriter};
use webrtc_stack::ice::udp_network::UDPNetwork;
use webrtc_stack::interceptor::registry::Registry;
use webrtc_stack::peer_connection::RTCPeerConnection;
use webrtc_stack::peer_connection::certificate::RTCCertificate;
use webrtc_stack::peer_connection::configuration::RTCConfiguration;
use webrtc_stack::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc_stack::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc_stack::util::{Conn, Error as WebRtcUtilError};

use crate::transport::WebRtcDirectAddr;

/// Prefix identifying the legacy SDP-munging Saorsa WebRTC Direct profile.
pub const ICE_CREDENTIAL_PREFIX_V1: &str = "saorsa+webrtc+v1/";

/// Prefix identifying the no-SDP-mutation Saorsa WebRTC Direct profile.
pub const ICE_CREDENTIAL_PREFIX_V2: &str = "saorsa+webrtc+v2/";

/// Prefix used by new Saorsa WebRTC Direct dials.
pub const ICE_CREDENTIAL_PREFIX: &str = ICE_CREDENTIAL_PREFIX_V2;

/// Maximum binary message size supported by this transport profile.
///
/// Large application frames must be split into messages no larger than this
/// value. The DataChannel itself remains reliable and ordered.
pub const MAX_DATA_CHANNEL_MESSAGE_SIZE: usize = 16 * 1024;

const MAX_PENDING_ASSOCIATIONS: usize = 256;

// How long teardown waits for a pending DTLS handshake to end before closing.
const PENDING_HANDSHAKE_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

// A plaintext DTLS 1.2 close_notify alert record: content type 21 (alert),
// version 254.253, epoch 0, sequence number 2^48 - 1, length 2, then level 1
// (warning) and description 0 (close_notify). A browser's handshake records
// count up from zero, so the replay window has not seen the last number.
const DTLS_CLOSE_NOTIFY: [u8; 15] = [21, 254, 253, 0, 0, 255, 255, 255, 255, 255, 255, 0, 2, 1, 0];

/// Resource limits applied after address validation and before RTC allocation.
#[derive(Clone, Copy, Debug)]
pub struct WebRtcAdmissionLimits {
    /// Maximum simultaneous associations, including handshakes.
    pub max_connections: usize,
    /// Maximum simultaneous associations from one IPv4 address or IPv6 /64.
    pub max_connections_per_ip: usize,
}

impl Default for WebRtcAdmissionLimits {
    fn default() -> Self {
        Self {
            max_connections: MAX_PENDING_ASSOCIATIONS,
            max_connections_per_ip: 4,
        }
    }
}

/// Errors produced by the WebRTC Direct transport.
#[derive(Debug, Error)]
pub enum WebRtcDirectError {
    /// The listener or its underlying channel has closed.
    #[error("WebRTC Direct transport is closed")]
    Closed,
    /// A socket operation failed.
    #[error("WebRTC Direct socket error: {0}")]
    Io(#[from] std::io::Error),
    /// A certificate could not be generated, loaded, or inspected.
    #[error("WebRTC Direct certificate error: {0}")]
    Certificate(String),
    /// WebRTC session setup or DataChannel I/O failed.
    #[error("WebRTC Direct session error: {0}")]
    Session(String),
    /// A DataChannel message violated the Saorsa transport profile.
    #[error("WebRTC Direct protocol error: {0}")]
    Protocol(String),
}

/// Persistent P-256 certificate used to authenticate a WebRTC Direct listener.
#[derive(Clone, Debug, PartialEq)]
pub struct WebRtcCertificate {
    inner: RTCCertificate,
}

impl WebRtcCertificate {
    /// Generate a new P-256 certificate accepted by current web browsers.
    pub fn generate() -> Result<Self, WebRtcDirectError> {
        ensure_crypto_provider();
        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
            .map_err(|error| WebRtcDirectError::Certificate(error.to_string()))?;
        let mut params = webrtc_rcgen::CertificateParams::default();
        params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(365 * 5);
        let certificate = params
            .self_signed(&key_pair)
            .map_err(|error| WebRtcDirectError::Certificate(error.to_string()))?;
        let private_key = webrtc_stack::dtls::crypto::CryptoPrivateKey::try_from(&key_pair)
            .map_err(|error| WebRtcDirectError::Certificate(error.to_string()))?;
        Self::from_dtls(webrtc_stack::dtls::crypto::Certificate {
            certificate: vec![certificate.der().clone()],
            private_key,
        })
    }

    /// Load persistent key material, deriving expiry from the signed certificate.
    /// Legacy ARM files may contain an artificial two-day EXPIRES header; that
    /// advisory header is ignored without changing the certificate or its pin.
    pub fn from_pem(pem: &str) -> Result<Self, WebRtcDirectError> {
        ensure_crypto_provider();
        let (_, material) = pem
            .split_once("-----END EXPIRES-----")
            .ok_or_else(|| WebRtcDirectError::Certificate("missing EXPIRES PEM header".into()))?;
        let certificate = webrtc_stack::dtls::crypto::Certificate::from_pem(material.trim())
            .map_err(|error| WebRtcDirectError::Certificate(error.to_string()))?;
        Self::from_dtls(certificate)
    }

    fn from_dtls(
        certificate: webrtc_stack::dtls::crypto::Certificate,
    ) -> Result<Self, WebRtcDirectError> {
        let der = certificate
            .certificate
            .first()
            .ok_or_else(|| WebRtcDirectError::Certificate("missing X.509 certificate".into()))?;
        let (_, parsed) = x509_parser::parse_x509_certificate(der.as_ref())
            .map_err(|error| WebRtcDirectError::Certificate(error.to_string()))?;
        if !parsed.validity().is_valid() {
            return Err(WebRtcDirectError::Certificate(
                "X.509 certificate is expired or not yet valid".into(),
            ));
        }
        let seconds = u64::try_from(parsed.validity().not_after.timestamp())
            .map_err(|error| WebRtcDirectError::Certificate(error.to_string()))?;
        let expires = std::time::UNIX_EPOCH
            .checked_add(std::time::Duration::from_secs(seconds))
            .ok_or_else(|| {
                WebRtcDirectError::Certificate("X.509 expiry exceeds platform clock range".into())
            })?;
        Ok(Self {
            inner: RTCCertificate::from_existing(certificate, expires),
        })
    }

    /// Serialize the certificate and private key for persistent storage.
    pub fn serialize_pem(&self) -> String {
        self.inner.serialize_pem()
    }

    /// Return the SHA-256 digest browsers pin through the endpoint certhash.
    pub fn sha256_digest(&self) -> Result<[u8; 32], WebRtcDirectError> {
        let fingerprint = self
            .inner
            .get_fingerprints()
            .into_iter()
            .find(|fingerprint| fingerprint.algorithm.eq_ignore_ascii_case("sha-256"))
            .ok_or_else(|| {
                WebRtcDirectError::Certificate(
                    "certificate does not contain a SHA-256 fingerprint".to_string(),
                )
            })?;
        let bytes = hex::decode(fingerprint.value.replace(':', ""))
            .map_err(|error| WebRtcDirectError::Certificate(error.to_string()))?;
        bytes.try_into().map_err(|bytes: Vec<u8>| {
            WebRtcDirectError::Certificate(format!(
                "SHA-256 fingerprint has {} bytes instead of 32",
                bytes.len()
            ))
        })
    }
}

fn ensure_crypto_provider() {
    // saorsa-transport's native QUIC stack uses aws-lc-rs. Installation is a
    // process-wide one-time choice; an already-installed provider is valid.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// A signaling-free WebRTC listener bound to one UDP socket.
pub struct WebRtcDirectListener {
    local_addr: SocketAddr,
    certificate: WebRtcCertificate,
    mux: Arc<DirectUdpMux>,
    incoming: mpsc::Receiver<IncomingAssociation>,
    driver: JoinHandle<()>,
    accepting: Option<JoinHandle<Result<WebRtcDirectConnection, WebRtcDirectError>>>,
}

impl WebRtcDirectListener {
    /// Bind a listener using a persistent certificate.
    pub async fn bind(
        bind_addr: SocketAddr,
        certificate: WebRtcCertificate,
    ) -> Result<Self, WebRtcDirectError> {
        Self::bind_with_limits(bind_addr, certificate, WebRtcAdmissionLimits::default()).await
    }

    /// Bind with application connection limits enforced before RTC allocation.
    pub async fn bind_with_limits(
        bind_addr: SocketAddr,
        certificate: WebRtcCertificate,
        limits: WebRtcAdmissionLimits,
    ) -> Result<Self, WebRtcDirectError> {
        if limits.max_connections == 0
            || limits.max_connections > MAX_PENDING_ASSOCIATIONS
            || limits.max_connections_per_ip == 0
        {
            return Err(WebRtcDirectError::Protocol(
                "invalid WebRTC admission limits".into(),
            ));
        }
        let socket = Arc::new(UdpSocket::bind(bind_addr).await?);
        let local_addr = socket.local_addr()?;
        let (incoming_tx, incoming) = mpsc::channel(MAX_PENDING_ASSOCIATIONS);
        let mux = DirectUdpMux::with_limits(Arc::clone(&socket), local_addr, incoming_tx, limits);
        let driver_mux = Arc::clone(&mux);
        let status = mux.diagnostics.driver_guard(mux.shutdown.clone());
        let driver = tokio::spawn(async move {
            let _status = status;
            driver_mux.run(socket).await;
        });
        Ok(Self {
            local_addr,
            certificate,
            mux,
            incoming,
            driver,
            accepting: None,
        })
    }

    /// Return the bound UDP socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Return local listener and connection diagnostics.
    pub fn diagnostics(&self) -> WebRtcDiagnostics {
        self.mux.diagnostics.clone()
    }

    /// Return the listener certificate.
    pub fn certificate(&self) -> &WebRtcCertificate {
        &self.certificate
    }

    /// Accept the next browser association discovered through STUN.
    pub async fn accept(&mut self) -> Result<WebRtcDirectConnection, WebRtcDirectError> {
        if self.accepting.is_none() {
            let association = loop {
                let association = tokio::select! {
                    biased;
                    () = self.mux.shutdown.cancelled() => return Err(WebRtcDirectError::Closed),
                    association = self.incoming.recv() => association.ok_or(WebRtcDirectError::Closed)?,
                };
                if self.mux.is_pending(&association) {
                    break association;
                }
            };
            let mux = Arc::clone(&self.mux);
            let certificate = self.certificate.clone();
            // Own construction independently of the caller's select! future.
            // Cancelling accept leaves this task available to the next accept.
            self.accepting = Some(tokio::spawn(async move {
                let mut owner = AssociationOwner {
                    mux: Arc::clone(&mux),
                    credential: association.server_ufrag.clone(),
                    remote_addr: association.remote_addr,
                    generation: association.generation,
                    peer: None,
                };
                create_inbound_connection(&association, mux, &certificate, &mut owner)
                    .await
                    .map(|mut connection| {
                        connection.owner = Some(owner);
                        connection
                    })
            }));
        }
        let task = self.accepting.as_mut().ok_or(WebRtcDirectError::Closed)?;
        let result = tokio::select! {
            biased;
            () = self.mux.shutdown.cancelled() => return Err(WebRtcDirectError::Closed),
            result = task => result.map_err(|error| WebRtcDirectError::Session(error.to_string())),
        };
        self.accepting = None;
        result?
    }

    /// Stop accepting connections and release the shared UDP mux.
    pub async fn close(&self) -> Result<(), WebRtcDirectError> {
        if let Some(task) = &self.accepting {
            task.abort();
        }
        self.mux
            .close()
            .await
            .map_err(|error| WebRtcDirectError::Session(error.to_string()))
    }
}

impl Drop for WebRtcDirectListener {
    fn drop(&mut self) {
        if let Some(task) = self.accepting.take() {
            task.abort();
        }
        self.mux.shutdown.cancel();
        self.driver.abort();
    }
}

// Give upstream ICE a session-scoped mux. Its callbacks carry the generation
// captured at admission instead of looking up whichever session is current.
struct AssociationMux {
    mux: Arc<DirectUdpMux>,
    credential: String,
    generation: u64,
}

#[async_trait]
impl UDPMux for AssociationMux {
    async fn close(&self) -> Result<(), WebRtcUtilError> {
        self.mux
            .remove_owned_connection(&self.credential, Some(self.generation))
            .await;
        Ok(())
    }

    async fn get_conn(
        self: Arc<Self>,
        credential: &str,
    ) -> Result<Arc<dyn Conn + Send + Sync>, WebRtcUtilError> {
        if credential != self.credential {
            return Err(WebRtcUtilError::ErrUseClosedNetworkConn);
        }
        let writer: Arc<dyn UDPMuxWriter + Send + Sync> = self.clone();
        Arc::clone(&self.mux)
            .get_owned_conn(credential, Some(self.generation), writer)
            .await
            .map(|connection| Arc::new(connection) as Arc<dyn Conn + Send + Sync>)
    }

    async fn remove_conn_by_ufrag(&self, credential: &str) {
        if credential == self.credential {
            self.mux
                .remove_owned_connection(credential, Some(self.generation))
                .await;
        }
    }
}

#[async_trait]
impl UDPMuxWriter for AssociationMux {
    async fn register_conn_for_address(&self, connection: &UDPMuxConn, addr: SocketAddr) {
        if connection.key() == self.credential {
            self.mux
                .register_owned_address(connection, addr, Some(self.generation));
        }
    }

    async fn send_to(&self, packet: &[u8], target: &SocketAddr) -> Result<usize, WebRtcUtilError> {
        if self.mux.admitted.read().get(&self.credential) != Some(&(*target, self.generation)) {
            return Err(WebRtcUtilError::ErrUseClosedNetworkConn);
        }
        self.mux.send_to(packet, target).await
    }
}

// Own cleanup on setup errors, rejected connections, and caller cancellation.
struct AssociationOwner {
    mux: Arc<DirectUdpMux>,
    credential: String,
    remote_addr: SocketAddr,
    peer: Option<(Arc<RTCPeerConnection>, UDPMuxConn)>,
    generation: Option<u64>,
}

impl Drop for AssociationOwner {
    fn drop(&mut self) {
        self.mux
            .release_pending(self.remote_addr, &self.credential, self.generation);
        let mux = Arc::clone(&self.mux);
        let credential = self.credential.clone();
        let remote_addr = self.remote_addr;
        let peer = self.peer.take();
        let generation = self.generation;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some((peer, ice)) = peer {
                    let _ = close_inbound_peer(&peer, &ice, remote_addr).await;
                }
                mux.remove_owned_connection(&credential, generation).await;
            });
        }
    }
}

/// Close an inbound peer connection without stranding its DTLS handshake.
///
/// webrtc 0.14 cannot cancel a DTLS server handshake that is still waiting for
/// the browser: `RTCDtlsTransport::stop` only closes a finished connection, and
/// stopping ICE drops the DTLS endpoint without closing its buffer. The
/// handshake and its reader and writer tasks then wait forever and keep the
/// peer connection's internals alive, about 62 KiB for every browser that
/// gives up after ICE connects. While ICE still delivers packets, hand the
/// handshake a close_notify from the browser's address so it fails and
/// releases everything, then close as for any failed connection.
async fn close_inbound_peer(
    peer: &RTCPeerConnection,
    ice: &UDPMuxConn,
    remote_addr: SocketAddr,
) -> Result<(), webrtc_stack::Error> {
    let dtls = peer.dtls_transport();
    // A start that already passed the New check is handled below only if the
    // next read sees Connecting; ADR-017 records the instant in which it does
    // not.
    stop_unstarted_dtls(&dtls).await;
    if dtls.state() == RTCDtlsTransportState::Connecting {
        match ice.write_packet(&DTLS_CLOSE_NOTIFY, remote_addr).await {
            Ok(()) => {
                let ended = tokio::time::timeout(PENDING_HANDSHAKE_CLOSE_TIMEOUT, async {
                    while dtls.state() == RTCDtlsTransportState::Connecting {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await;
                if ended.is_err() {
                    tracing::debug!(%remote_addr, "pending DTLS handshake outlived close_notify");
                }
            }
            Err(error) => {
                tracing::debug!(%remote_addr, %error, "failed to end pending DTLS handshake");
            }
        }
    }
    peer.close().await
}

/// Stop a DTLS transport whose handshake has not started.
///
/// A handshake only starts from New, so ICE connecting after this, for
/// example while the peer connection closes, can no longer start one that
/// nothing would end.
async fn stop_unstarted_dtls(dtls: &RTCDtlsTransport) {
    if dtls.state() == RTCDtlsTransportState::New {
        let _ = dtls.stop().await;
    }
}

/// One browser WebRTC association that can carry application DataChannels.
pub struct WebRtcDirectConnection {
    diagnostics: ConnectionTracker,
    remote_addr: SocketAddr,
    peer_connection: Arc<RTCPeerConnection>,
    ice: UDPMuxConn,
    incoming: mpsc::Receiver<WebRtcDataChannel>,
    closed: watch::Receiver<bool>,
    owner: Option<AssociationOwner>,
}

impl WebRtcDirectConnection {
    /// Snapshot this association's state and DataChannel traffic.
    pub fn metrics(&self) -> Option<WebRtcConnectionMetrics> {
        self.diagnostics.metrics()
    }

    /// Return the browser's observed UDP address.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Accept the next reliable ordered DataChannel opened by the browser.
    pub async fn accept_data_channel(&mut self) -> Result<WebRtcDataChannel, WebRtcDirectError> {
        loop {
            if *self.closed.borrow() {
                return Err(WebRtcDirectError::Closed);
            }
            tokio::select! {
                biased;
                changed = self.closed.changed() => {
                    if changed.is_err() || *self.closed.borrow() {
                        return Err(WebRtcDirectError::Closed);
                    }
                }
                channel = self.incoming.recv() => {
                    return channel.ok_or(WebRtcDirectError::Closed);
                }
            }
        }
    }

    /// Close the WebRTC association.
    pub async fn close(&self) -> Result<(), WebRtcDirectError> {
        close_inbound_peer(&self.peer_connection, &self.ice, self.remote_addr)
            .await
            .map_err(|error| WebRtcDirectError::Session(error.to_string()))
    }
}

/// Native diagnostic client for the Saorsa WebRTC Direct wire profile.
///
/// Browser applications use `RTCPeerConnection` directly. This type exists so
/// native integration tests and troubleshooting tools can verify listeners
/// through the same ICE-lite, DTLS, SCTP, and DataChannel path.
pub struct WebRtcDirectClient {
    diagnostics: ConnectionTracker,
    local_addr: SocketAddr,
    peer_connection: Arc<RTCPeerConnection>,
    channel: WebRtcDataChannel,
    mux: Arc<DirectUdpMux>,
    _driver: UdpDriver,
    _owner: OutboundAssociationOwner,
}

/// Close the entire RTC stack on cancellation, setup failure, or client drop.
struct OutboundAssociationOwner {
    runtime: tokio::runtime::Handle,
    peer: Arc<RTCPeerConnection>,
    mux: Arc<DirectUdpMux>,
}

impl Drop for OutboundAssociationOwner {
    fn drop(&mut self) {
        let peer = Arc::clone(&self.peer);
        let mux = Arc::clone(&self.mux);
        self.runtime.spawn(async move {
            let _ = peer.close().await;
            let _ = mux.close().await;
        });
    }
}

/// Own the receive task even while a dial future is still being polled.
struct UdpDriver {
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl Drop for UdpDriver {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

impl WebRtcDirectClient {
    /// Dial a pinned direct endpoint and open one reliable ordered DataChannel.
    pub async fn dial(
        endpoint: &WebRtcDirectAddr,
        data_channel_label: &str,
    ) -> Result<Self, WebRtcDirectError> {
        if data_channel_label.is_empty() {
            return Err(WebRtcDirectError::Protocol(
                "DataChannel label must not be empty".to_string(),
            ));
        }
        let bind_addr = match endpoint.ip() {
            IpAddr::V4(_) => SocketAddr::from(([0, 0, 0, 0], 0)),
            IpAddr::V6(_) => SocketAddr::from(([0_u16; 8], 0)),
        };
        let socket = Arc::new(UdpSocket::bind(bind_addr).await?);
        let local_addr = socket.local_addr()?;
        let (unused_incoming, incoming) = mpsc::channel(1);
        drop(incoming);
        let mux = DirectUdpMux::new(Arc::clone(&socket), local_addr, unused_incoming);
        let driver_mux = Arc::clone(&mux);
        let status = mux.diagnostics.driver_guard(mux.shutdown.clone());
        let driver = UdpDriver {
            shutdown: mux.shutdown.clone(),
            task: tokio::spawn(async move {
                let _status = status;
                driver_mux.run(socket).await;
            }),
        };

        let (owner, channel, diagnostics) =
            create_outbound_client(endpoint, data_channel_label, local_addr, Arc::clone(&mux))
                .await?;
        Ok(Self {
            local_addr,
            peer_connection: Arc::clone(&owner.peer),
            channel,
            mux,
            _driver: driver,
            _owner: owner,
            diagnostics,
        })
    }

    /// Return the local UDP address used for this association.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Snapshot this association's state and DataChannel traffic.
    pub fn metrics(&self) -> Option<WebRtcConnectionMetrics> {
        self.diagnostics.metrics()
    }

    /// Return local connection diagnostics, including closed totals.
    pub fn diagnostics(&self) -> WebRtcDiagnostics {
        self.mux.diagnostics.clone()
    }

    /// Return the open application DataChannel.
    pub fn data_channel(&self) -> &WebRtcDataChannel {
        &self.channel
    }

    /// Close the DataChannel, peer connection, and UDP mux.
    pub async fn close(&self) -> Result<(), WebRtcDirectError> {
        // Attempt every layer even if an earlier close reports an error.
        let channel = self.channel.close().await;
        let peer = self
            .peer_connection
            .close()
            .await
            .map_err(|error| WebRtcDirectError::Session(error.to_string()));
        let mux = self
            .mux
            .close()
            .await
            .map_err(|error| WebRtcDirectError::Session(error.to_string()));
        channel.and(peer).and(mux)
    }
}

/// A reliable ordered WebRTC DataChannel carrying binary application messages.
#[derive(Clone)]
pub struct WebRtcDataChannel {
    diagnostics: ConnectionObserver,
    inner: Arc<DataChannel>,
    label: String,
    id: u16,
}

impl WebRtcDataChannel {
    /// Return the application label chosen by the browser.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Return the SCTP stream identifier.
    pub fn id(&self) -> u16 {
        self.id
    }

    /// Receive one binary DataChannel message.
    ///
    /// An empty vector means the browser closed or reset the channel.
    pub async fn receive(&self) -> Result<Vec<u8>, WebRtcDirectError> {
        let mut buffer = vec![0_u8; MAX_DATA_CHANNEL_MESSAGE_SIZE];
        let (length, is_string) = self
            .inner
            .read_data_channel(&mut buffer)
            .await
            .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
        self.diagnostics.received(length);
        if is_string {
            return Err(WebRtcDirectError::Protocol(
                "text DataChannel messages are not supported".to_string(),
            ));
        }
        buffer.truncate(length);
        Ok(buffer)
    }

    /// Send one binary DataChannel message.
    pub async fn send(&self, message: &[u8]) -> Result<(), WebRtcDirectError> {
        if message.len() > MAX_DATA_CHANNEL_MESSAGE_SIZE {
            return Err(WebRtcDirectError::Protocol(format!(
                "message has {} bytes; maximum is {MAX_DATA_CHANNEL_MESSAGE_SIZE}",
                message.len()
            )));
        }
        let written = self
            .inner
            .write(&Bytes::copy_from_slice(message))
            .await
            .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
        self.diagnostics.sent(written);
        if written != message.len() {
            return Err(WebRtcDirectError::Session(format!(
                "DataChannel wrote {written} of {} bytes",
                message.len()
            )));
        }
        Ok(())
    }

    /// Close this DataChannel.
    pub async fn close(&self) -> Result<(), WebRtcDirectError> {
        self.inner
            .close()
            .await
            .map_err(|error| WebRtcDirectError::Session(error.to_string()))
    }
}

async fn create_inbound_connection(
    association: &IncomingAssociation,
    udp_mux: Arc<DirectUdpMux>,
    certificate: &WebRtcCertificate,
    owner: &mut AssociationOwner,
) -> Result<WebRtcDirectConnection, WebRtcDirectError> {
    let remote_addr = association.remote_addr;
    let server_ufrag = association.server_ufrag.as_str();
    let client_ufrag = association.client_ufrag.as_str();
    let client_pwd = association.client_pwd.as_str();
    if parse_profile_credentials(server_ufrag, client_ufrag)
        .is_none_or(|credentials| credentials.client_pwd != client_pwd)
    {
        return Err(WebRtcDirectError::Protocol(
            "invalid Saorsa WebRTC Direct ICE credentials".to_string(),
        ));
    }

    let diagnostics = udp_mux.diagnostics.begin(remote_addr)?;
    let mut settings = SettingEngine::default();
    // Direct peers use literal IP candidates; never open an mDNS socket.
    settings.set_ice_multicast_dns_mode(MulticastDnsMode::Disabled);
    settings.set_lite(true);
    settings.disable_certificate_fingerprint_verification(true);
    settings
        .set_answering_dtls_role(DTLSRole::Server)
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    settings.set_ice_credentials(server_ufrag.to_string(), server_ufrag.to_string());
    let generation = owner.generation.ok_or(WebRtcDirectError::Closed)?;
    let association_mux = Arc::new(AssociationMux {
        mux: Arc::clone(&udp_mux),
        credential: server_ufrag.to_string(),
        generation,
    });
    // Admission consumed the authenticated Binding request that proved the
    // return path. Deliver it to this generation's ICE agent before it starts,
    // so nomination does not depend on the client's next retransmission.
    // Keep this same writer alive in SettingEngine: UDPMuxConn holds it weakly.
    let writer: Arc<dyn UDPMuxWriter + Send + Sync> = association_mux.clone();
    let initial_conn = udp_mux
        .get_owned_conn(server_ufrag, Some(generation), writer)
        .await
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    initial_conn
        .write_packet(&association.initial_packet, remote_addr)
        .await
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    settings.set_udp_network(UDPNetwork::Muxed(association_mux));
    settings.detach_data_channels();
    settings.set_srtp_protection_profiles(vec![
        SrtpProtectionProfile::Srtp_Aead_Aes_128_Gcm,
        SrtpProtectionProfile::Srtp_Aes128_Cm_Hmac_Sha1_80,
        SrtpProtectionProfile::Srtp_Aes128_Cm_Hmac_Sha1_32,
    ]);
    settings.set_network_types(vec![match remote_addr {
        SocketAddr::V4(_) => NetworkType::Udp4,
        SocketAddr::V6(_) => NetworkType::Udp6,
    }]);
    let first_ip = AtomicBool::new(true);
    settings.set_ip_filter(Box::new(move |_| first_ip.swap(false, Ordering::Relaxed)));

    let mut media_engine = webrtc_stack::api::media_engine::MediaEngine::default();
    media_engine
        .register_default_codecs()
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    let registry = register_default_interceptors(Registry::new(), &mut media_engine)
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    let api = APIBuilder::new()
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .with_setting_engine(settings)
        .build();
    let peer_connection = Arc::new(
        api.new_peer_connection(RTCConfiguration {
            certificates: vec![certificate.inner.clone()],
            ..RTCConfiguration::default()
        })
        .await
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?,
    );

    owner.peer = Some((Arc::clone(&peer_connection), initial_conn.clone()));
    let setup_peer = Arc::downgrade(&peer_connection);
    let setup_ice = initial_conn.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        if let Some(peer) = setup_peer.upgrade() {
            if peer.connection_state() != webrtc_stack::peer_connection::peer_connection_state::RTCPeerConnectionState::Connected {
                let _ = close_inbound_peer(&peer, &setup_ice, remote_addr).await;
            }
        }
    });
    let (incoming_tx, incoming) = mpsc::channel(16);
    register_data_channel_handler(&peer_connection, incoming_tx, diagnostics.observer());
    let (closed_tx, closed) = watch::channel(false);
    let observer = diagnostics.observer();
    // Teardown stops a DTLS transport that is still New, and webrtc then reports
    // Connected if ICE connects before the close completes. Only a peer
    // connection whose DTLS handshake completed counts as connected: the
    // transport stores the remote certificate when it completes, before
    // publishing Connected, and never clears it.
    let dtls = Arc::downgrade(&peer_connection.dtls_transport());
    peer_connection.on_peer_connection_state_change(Box::new(move |state| {
        let closed_tx = closed_tx.clone();
        let observer = observer.clone();
        let dtls = dtls.clone();
        Box::pin(async move {
            let handshake_done = match dtls.upgrade() {
                Some(dtls) if state == RTCPeerConnectionState::Connected => {
                    !dtls.get_remote_certificate().await.is_empty()
                }
                _ => false,
            };
            if state != RTCPeerConnectionState::Connected || handshake_done {
                observer.transition(state);
            }
            if matches!(
                state,
                RTCPeerConnectionState::Failed
                    | RTCPeerConnectionState::Disconnected
                    | RTCPeerConnectionState::Closed
            ) {
                let _ = closed_tx.send(true);
            }
        })
    }));

    let offer = RTCSessionDescription::offer(client_offer(remote_addr, client_ufrag, client_pwd))
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    peer_connection
        .set_remote_description(offer)
        .await
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    let answer = peer_connection
        .create_answer(None)
        .await
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    peer_connection
        .set_local_description(answer)
        .await
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;

    Ok(WebRtcDirectConnection {
        diagnostics,
        remote_addr,
        owner: None,
        peer_connection,
        ice: initial_conn,
        incoming,
        closed,
    })
}

async fn create_outbound_client(
    endpoint: &WebRtcDirectAddr,
    data_channel_label: &str,
    local_addr: SocketAddr,
    udp_mux: Arc<DirectUdpMux>,
) -> Result<
    (
        OutboundAssociationOwner,
        WebRtcDataChannel,
        ConnectionTracker,
    ),
    WebRtcDirectError,
> {
    let diagnostics = udp_mux.diagnostics.begin(endpoint.socket_addr())?;
    let client_ufrag = random_ice_string(32);
    let client_pwd = random_ice_string(32);
    let server_ufrag = format!("{ICE_CREDENTIAL_PREFIX_V2}{client_pwd}");
    let client_certificate = WebRtcCertificate::generate()?;
    let mut settings = SettingEngine::default();
    // Direct peers use literal IP candidates; never open an mDNS socket.
    settings.set_ice_multicast_dns_mode(MulticastDnsMode::Disabled);
    settings.set_ice_credentials(client_ufrag, client_pwd);
    settings.set_udp_network(UDPNetwork::Muxed(
        Arc::clone(&udp_mux) as Arc<dyn UDPMux + Send + Sync>
    ));
    settings.detach_data_channels();
    settings.set_srtp_protection_profiles(vec![
        SrtpProtectionProfile::Srtp_Aead_Aes_128_Gcm,
        SrtpProtectionProfile::Srtp_Aes128_Cm_Hmac_Sha1_80,
        SrtpProtectionProfile::Srtp_Aes128_Cm_Hmac_Sha1_32,
    ]);
    settings.set_network_types(vec![match endpoint.socket_addr() {
        SocketAddr::V4(_) => NetworkType::Udp4,
        SocketAddr::V6(_) => NetworkType::Udp6,
    }]);
    let first_ip = AtomicBool::new(true);
    settings.set_ip_filter(Box::new(move |_| first_ip.swap(false, Ordering::Relaxed)));

    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    let peer_connection = Arc::new(
        APIBuilder::new()
            .with_setting_engine(settings)
            .build()
            .new_peer_connection(RTCConfiguration {
                certificates: vec![client_certificate.inner],
                ..RTCConfiguration::default()
            })
            .await
            .map_err(|error| WebRtcDirectError::Session(error.to_string()))?,
    );
    let owner = OutboundAssociationOwner {
        runtime,
        peer: Arc::clone(&peer_connection),
        mux: udp_mux,
    };
    let observer = diagnostics.observer();
    peer_connection.on_peer_connection_state_change(Box::new(move |state| {
        observer.transition(state);
        Box::pin(async {})
    }));
    let rtc_channel = peer_connection
        .create_data_channel(data_channel_label, None)
        .await
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    let (channel_tx, mut channel_rx) = mpsc::channel(1);
    let label = data_channel_label.to_string();
    let channel_id = rtc_channel.id();
    // The channel owns this callback, even if setup ends before it opens.
    // A strong capture would keep the channel and its mux alive after close.
    let open_channel = Arc::downgrade(&rtc_channel);
    let observer = diagnostics.observer();
    rtc_channel.on_open(Box::new(move || {
        let open_channel = open_channel.upgrade();
        let channel_tx = channel_tx.clone();
        let label = label.clone();
        let diagnostics = observer.clone();
        Box::pin(async move {
            let Some(open_channel) = open_channel else {
                return;
            };
            match open_channel.detach().await {
                Ok(inner) => {
                    let _ = channel_tx
                        .send(WebRtcDataChannel {
                            inner,
                            label,
                            id: channel_id,
                            diagnostics,
                        })
                        .await;
                }
                Err(error) => {
                    tracing::debug!(%error, "failed to detach outbound WebRTC DataChannel");
                }
            }
        })
    }));

    let offer = peer_connection
        .create_offer(None)
        .await
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    peer_connection
        .set_local_description(offer)
        .await
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    let answer = RTCSessionDescription::answer(server_answer(
        endpoint.socket_addr(),
        endpoint.certificate_hash().as_bytes(),
        &server_ufrag,
    ))
    .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    peer_connection
        .set_remote_description(answer)
        .await
        .map_err(|error| WebRtcDirectError::Session(error.to_string()))?;
    let channel = tokio::time::timeout(std::time::Duration::from_secs(10), channel_rx.recv())
        .await
        .map_err(|_| WebRtcDirectError::Session("DataChannel opening timed out".to_string()))?
        .ok_or_else(|| {
            WebRtcDirectError::Session("DataChannel closed before opening".to_string())
        })?;
    tracing::debug!(%local_addr, remote = %endpoint.socket_addr(), "WebRTC Direct dial completed");
    Ok((owner, channel, diagnostics))
}

fn register_data_channel_handler(
    peer_connection: &RTCPeerConnection,
    incoming: mpsc::Sender<WebRtcDataChannel>,
    diagnostics: ConnectionObserver,
) {
    peer_connection.on_data_channel(Box::new(move |channel: Arc<RTCDataChannel>| {
        let incoming = incoming.clone();
        let diagnostics = diagnostics.clone();
        Box::pin(async move {
            let reliable = channel.ordered()
                && channel.max_retransmits().is_none()
                && channel.max_packet_lifetime().is_none();
            let label = channel.label().to_string();
            let id = channel.id();
            let open_channel = Arc::downgrade(&channel);
            channel.on_open(Box::new(move || {
                let incoming = incoming.clone();
                let open_channel = open_channel.upgrade();
                let label = label.clone();
                let diagnostics = diagnostics.clone();
                Box::pin(async move {
                    let Some(open_channel) = open_channel else {
                        return;
                    };
                    match open_channel.detach().await {
                        Ok(inner) => {
                            // on_data_channel runs before the SCTP stream is
                            // attached. Close after opening so rejection also
                            // resets the underlying stream at the remote peer.
                            if !reliable {
                                inner.close().await.ok();
                                return;
                            }
                            let channel = WebRtcDataChannel {
                                inner,
                                label,
                                id,
                                diagnostics,
                            };
                            if let Err(error) = incoming.try_send(channel) {
                                let channel = error.into_inner();
                                channel.close().await.ok();
                            }
                        }
                        Err(error) => {
                            tracing::debug!(%error, id, "failed to detach WebRTC DataChannel");
                        }
                    }
                })
            }));
        })
    }));
}

fn client_offer(remote_addr: SocketAddr, client_ufrag: &str, client_pwd: &str) -> String {
    let (ip_version, ip) = match remote_addr.ip() {
        IpAddr::V4(ip) => ("IP4", ip.to_string()),
        IpAddr::V6(ip) => ("IP6", ip.to_string()),
    };
    format!(
        "v=0\n\
o=- 0 0 IN {ip_version} {ip}\n\
s=-\n\
c=IN {ip_version} {ip}\n\
t=0 0\n\
m=application {} UDP/DTLS/SCTP webrtc-datachannel\n\
a=mid:0\n\
a=ice-options:ice2\n\
a=ice-ufrag:{client_ufrag}\n\
a=ice-pwd:{client_pwd}\n\
a=fingerprint:sha-256 FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF:FF\n\
a=setup:actpass\n\
a=sctp-port:5000\n\
a=max-message-size:{MAX_DATA_CHANNEL_MESSAGE_SIZE}\n",
        remote_addr.port()
    )
}

fn server_answer(
    remote_addr: SocketAddr,
    certificate_hash: &[u8; 32],
    ice_credential: &str,
) -> String {
    let (ip_version, ip) = match remote_addr.ip() {
        IpAddr::V4(ip) => ("IP4", ip.to_string()),
        IpAddr::V6(ip) => ("IP6", ip.to_string()),
    };
    let fingerprint = certificate_hash
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":");
    format!(
        "v=0\n\
o=- 0 0 IN {ip_version} {ip}\n\
s=-\n\
t=0 0\n\
a=ice-lite\n\
m=application {} UDP/DTLS/SCTP webrtc-datachannel\n\
c=IN {ip_version} {ip}\n\
a=mid:0\n\
a=ice-options:ice2\n\
a=ice-ufrag:{ice_credential}\n\
a=ice-pwd:{ice_credential}\n\
a=fingerprint:sha-256 {fingerprint}\n\
a=setup:passive\n\
a=sctp-port:5000\n\
a=max-message-size:{MAX_DATA_CHANNEL_MESSAGE_SIZE}\n\
a=candidate:1467250027 1 UDP 1467250027 {ip} {} typ host\n\
a=end-of-candidates\n",
        remote_addr.port(),
        remote_addr.port()
    )
}

fn random_ice_string(length: usize) -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), length)
}

fn is_ice_char_string(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
}

fn is_valid_ice_ufrag(value: &str) -> bool {
    (4..=256).contains(&value.len()) && is_ice_char_string(value)
}

fn is_valid_ice_pwd(value: &str) -> bool {
    (22..=256).contains(&value.len()) && is_ice_char_string(value)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProfileCredentials {
    server_ufrag: String,
    client_ufrag: String,
    client_pwd: String,
}

fn parse_profile_credentials(server_ufrag: &str, client_ufrag: &str) -> Option<ProfileCredentials> {
    if !is_valid_ice_ufrag(server_ufrag) || !is_valid_ice_ufrag(client_ufrag) {
        return None;
    }

    let client_pwd = if let Some(client_pwd) = server_ufrag.strip_prefix(ICE_CREDENTIAL_PREFIX_V2) {
        if !is_valid_ice_pwd(client_pwd) {
            return None;
        }
        client_pwd
    } else if server_ufrag.starts_with(ICE_CREDENTIAL_PREFIX_V1) {
        if !is_valid_ice_pwd(client_ufrag) {
            return None;
        }
        client_ufrag
    } else {
        return None;
    };

    Some(ProfileCredentials {
        server_ufrag: server_ufrag.to_string(),
        client_ufrag: client_ufrag.to_string(),
        client_pwd: client_pwd.to_string(),
    })
}

/// Answer a proven association that found no free slot.
///
/// Silence would leave the browser retransmitting checks until its ICE timeout
/// and then treating a busy node as unreachable. An answered check fails at
/// once, so the client can try another holder of the same record.
async fn refuse(
    socket: &UdpSocket,
    association: &IncomingAssociation,
    refusal: admission::Refusal,
) {
    let remote_addr = association.remote_addr;
    let Some(response) = admission::refusal_response(
        &association.initial_packet,
        &association.server_ufrag,
        refusal,
    ) else {
        tracing::debug!(%remote_addr, ?refusal, "WebRTC Direct refusal would exceed its request");
        return;
    };
    if let Err(error) = socket.send_to(&response, remote_addr).await {
        tracing::debug!(%remote_addr, %error, "failed to send WebRTC Direct refusal");
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IncomingAssociation {
    remote_addr: SocketAddr,
    server_ufrag: String,
    client_ufrag: String,
    client_pwd: String,
    // Bounded by the listener receive buffer and retained only after admission.
    initial_packet: Vec<u8>,
    generation: Option<u64>,
}

struct DirectUdpMux {
    diagnostics: WebRtcDiagnostics,
    local_addr: SocketAddr,
    conns: Mutex<HashMap<String, UDPMuxConn>>,
    address_map: RwLock<HashMap<SocketAddr, UDPMuxConn>>,
    pending: RwLock<HashMap<String, (SocketAddr, u64)>>,
    admitted: RwLock<HashMap<String, (SocketAddr, u64)>>,
    limits: WebRtcAdmissionLimits,
    next_generation: AtomicU64,
    admission: parking_lot::Mutex<admission::Admission>,
    incoming: mpsc::Sender<IncomingAssociation>,
    socket: Weak<UdpSocket>,
    shutdown: CancellationToken,
    closed: AtomicBool,
}

impl DirectUdpMux {
    fn new(
        socket: Arc<UdpSocket>,
        local_addr: SocketAddr,
        incoming: mpsc::Sender<IncomingAssociation>,
    ) -> Arc<Self> {
        Self::with_limits(
            socket,
            local_addr,
            incoming,
            WebRtcAdmissionLimits::default(),
        )
    }

    fn with_limits(
        socket: Arc<UdpSocket>,
        local_addr: SocketAddr,
        incoming: mpsc::Sender<IncomingAssociation>,
        limits: WebRtcAdmissionLimits,
    ) -> Arc<Self> {
        Arc::new(Self {
            diagnostics: WebRtcDiagnostics::default(),
            local_addr,
            conns: Mutex::new(HashMap::new()),
            address_map: RwLock::new(HashMap::new()),
            pending: RwLock::new(HashMap::new()),
            admitted: RwLock::new(HashMap::new()),
            limits,
            next_generation: AtomicU64::new(1),
            admission: parking_lot::Mutex::new(admission::Admission::default()),
            incoming,
            socket: Arc::downgrade(&socket),
            shutdown: CancellationToken::new(),
            closed: AtomicBool::new(false),
        })
    }

    async fn run(self: Arc<Self>, socket: Arc<UdpSocket>) {
        let mut buffer = [0_u8; MAX_DATA_CHANNEL_MESSAGE_SIZE];
        let mut expiry = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            let received = tokio::select! {
                () = self.shutdown.cancelled() => break,
                _ = expiry.tick() => {
                    self.admission.lock().expire(tokio::time::Instant::now());
                    continue;
                }
                received = socket.recv_from(&mut buffer) => received,
            };
            let (length, remote_addr) = match received {
                Ok(received) => received,
                Err(error) if error.kind() == ErrorKind::ConnectionReset => continue,
                Err(error) => {
                    self.diagnostics.listener_error();
                    tracing::warn!(%error, "WebRTC Direct UDP listener failed");
                    break;
                }
            };
            let packet = &buffer[..length];
            // Probe responses must bypass stale source mappings when a browser
            // replaces an association while reusing its UDP port.
            let probe_response = self.admission.lock().expects_response(packet, remote_addr);
            let connection = if probe_response {
                None
            } else {
                self.connection_for_packet(packet, remote_addr).await
            };
            if let Some(connection) = connection {
                if let Err(error) = connection.write_packet(packet, remote_addr).await {
                    tracing::debug!(%error, %remote_addr, "failed to route WebRTC UDP packet");
                }
                continue;
            }

            let decision =
                self.admission
                    .lock()
                    .receive(packet, remote_addr, tokio::time::Instant::now());
            let mut association = match decision {
                admission::Decision::Ignore => continue,
                admission::Decision::Challenge(bytes) => {
                    let _ = socket.send_to(&bytes, remote_addr).await;
                    continue;
                }
                admission::Decision::Admit(association) => association,
            };
            let Ok(generation) =
                self.next_generation
                    .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                        value.checked_add(1)
                    })
            else {
                continue;
            };
            association.generation = Some(generation);
            let refusal = {
                let mut admitted = self.admitted.write();
                if admitted.contains_key(&association.server_ufrag) {
                    continue;
                }
                let mut pending = self.pending.write();
                // Pending credentials are also admitted, so this only guards
                // that invariant. Never refuse a live association's credential.
                if pending.contains_key(&association.server_ufrag) {
                    self.diagnostics.reject();
                    continue;
                }
                let refusal = self.refusal(&admitted, pending.len(), remote_addr);
                if refusal.is_none() {
                    admitted.insert(association.server_ufrag.clone(), (remote_addr, generation));
                    pending.insert(
                        association.server_ufrag.clone(),
                        (association.remote_addr, generation),
                    );
                }
                refusal
            };
            if let Some(refusal) = refusal {
                self.diagnostics.reject();
                refuse(&socket, &association, refusal).await;
                continue;
            }
            let expiry_mux = Arc::downgrade(&self);
            let expiry_credential = association.server_ufrag.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                if let Some(mux) = expiry_mux.upgrade() {
                    if mux
                        .admitted
                        .read()
                        .get(&expiry_credential)
                        .is_some_and(|(_, created)| *created == generation)
                        && mux.pending.read().get(&expiry_credential)
                            == Some(&(remote_addr, generation))
                    {
                        mux.remove_owned_connection(&expiry_credential, Some(generation))
                            .await;
                    }
                }
            });
            if let Err(error) = self.incoming.try_send(association.clone()) {
                self.diagnostics.reject();
                self.pending.write().remove(&association.server_ufrag);
                self.admitted.write().remove(&association.server_ufrag);
                tracing::debug!(%remote_addr, %error, "WebRTC Direct accept queue is unavailable");
                // A closed queue means the listener is shutting down, not busy.
                if matches!(error, mpsc::error::TrySendError::Full(_)) {
                    refuse(&socket, &association, admission::Refusal::Capacity).await;
                }
            }
        }
    }

    /// The refusal owed to a proven association from `remote_addr`, or `None`
    /// while the listener and that source both have a free slot.
    fn refusal(
        &self,
        admitted: &HashMap<String, (SocketAddr, u64)>,
        pending: usize,
        remote_addr: SocketAddr,
    ) -> Option<admission::Refusal> {
        if admitted.len() >= self.limits.max_connections || pending >= MAX_PENDING_ASSOCIATIONS {
            return Some(admission::Refusal::Capacity);
        }
        let source = super::source_ip_bucket(remote_addr.ip());
        let source_connections = admitted
            .values()
            .filter(|addr| super::source_ip_bucket(addr.0.ip()) == source)
            .count();
        (source_connections >= self.limits.max_connections_per_ip)
            .then_some(admission::Refusal::SourceQuota)
    }

    async fn connection_for_packet(
        &self,
        packet: &[u8],
        remote_addr: SocketAddr,
    ) -> Option<UDPMuxConn> {
        // A browser may reuse the same UDP source port for a replacement peer
        // connection. Binding requests carry the new association's local ICE
        // credential, so it must take precedence over a stale address mapping.
        // Binding responses do not carry USERNAME and therefore continue to use
        // the source-address mapping registered when the ICE agent sent its
        // request. DTLS and SCTP packets use that mapping as well.
        if let Some(local_credential) = local_ice_credential(packet) {
            // An existing association is bound to the source that completed
            // our reachability transaction. Public ICE credentials cannot move it.
            if self
                .admitted
                .read()
                .get(&local_credential)
                .is_some_and(|(source, _)| *source != remote_addr)
            {
                return None;
            }
            if !self.admitted.read().contains_key(&local_credential)
                && !self.address_map.read().contains_key(&remote_addr)
            {
                return None;
            }
            return self.conns.lock().await.get(&local_credential).cloned();
        }
        self.address_map.read().get(&remote_addr).cloned()
    }

    async fn remove_owned_connection(&self, credential: &str, expected: Option<u64>) {
        let mut connections = self.conns.lock().await;
        let mut admitted = self.admitted.write();
        if admitted.get(credential).map(|(_, at)| *at) != expected {
            return;
        }
        let removed = connections.remove(credential);
        self.pending.write().remove(credential);
        admitted.remove(credential);
        if let Some(connection) = removed {
            // ICE may already have closed the connection and cleared its
            // address list. Remove ownership from our authoritative map instead.
            // The admission lock and generation check above protect replacements.
            self.address_map
                .write()
                .retain(|_, current| current.key() != credential);
            connection.close();
        }
    }

    async fn get_owned_conn(
        self: Arc<Self>,
        ice_credential: &str,
        expected: Option<u64>,
        writer: Arc<dyn UDPMuxWriter + Send + Sync>,
    ) -> Result<UDPMuxConn, WebRtcUtilError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WebRtcUtilError::ErrUseClosedNetworkConn);
        }
        let mut connections = self.conns.lock().await;
        if self.closed.load(Ordering::Acquire)
            || self.admitted.read().get(ice_credential).map(|(_, id)| *id) != expected
        {
            return Err(WebRtcUtilError::ErrUseClosedNetworkConn);
        }
        if let Some(connection) = connections.get(ice_credential) {
            return Ok(connection.clone());
        }
        let connection = UDPMuxConn::new(UDPMuxConnParams {
            local_addr: self.local_addr,
            key: ice_credential.to_string(),
            udp_mux: Arc::downgrade(&writer),
        });
        let mut closed = connection.close_rx();
        let mux = Arc::clone(&self);
        let credential = ice_credential.to_string();
        let generation = expected;
        tokio::spawn(async move {
            let _ = closed.changed().await;
            mux.remove_owned_connection(&credential, generation).await;
        });
        connections.insert(ice_credential.to_string(), connection.clone());
        Ok(connection)
    }

    fn register_owned_address(
        &self,
        connection: &UDPMuxConn,
        addr: SocketAddr,
        generation: Option<u64>,
    ) {
        let admitted = self.admitted.read();
        if self.closed.load(Ordering::Acquire)
            || admitted.get(connection.key()).map(|(_, id)| *id) != generation
        {
            return;
        }
        if admitted
            .get(connection.key())
            .is_some_and(|(source, _)| *source != addr)
        {
            return;
        }
        let key = connection.key();
        self.address_map
            .write()
            .entry(addr)
            .and_modify(|current| {
                if current.key() != key {
                    current.remove_address(&addr);
                    *current = connection.clone();
                }
            })
            .or_insert_with(|| connection.clone());
        self.pending.write().remove(connection.key());
    }

    fn is_pending(&self, association: &IncomingAssociation) -> bool {
        let Some(generation) = association.generation else {
            return false;
        };
        let admitted = self.admitted.read();
        let expected = (association.remote_addr, generation);
        admitted.get(&association.server_ufrag) == Some(&expected)
            && self.pending.read().get(&association.server_ufrag) == Some(&expected)
    }

    fn release_pending(&self, remote_addr: SocketAddr, credential: &str, generation: Option<u64>) {
        let admitted = self.admitted.read();
        if admitted.get(credential).map(|(_, id)| *id) == generation {
            let mut pending = self.pending.write();
            if pending
                .get(credential)
                .is_some_and(|(addr, id)| *addr == remote_addr && Some(*id) == generation)
            {
                pending.remove(credential);
            }
        }
    }
}

#[async_trait]
impl UDPMux for DirectUdpMux {
    async fn close(&self) -> Result<(), WebRtcUtilError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.shutdown.cancel();
        let connections = std::mem::take(&mut *self.conns.lock().await);
        for (_, connection) in connections {
            connection.close();
        }
        self.address_map.write().clear();
        self.pending.write().clear();
        self.admitted.write().clear();
        Ok(())
    }

    async fn get_conn(
        self: Arc<Self>,
        ice_credential: &str,
    ) -> Result<Arc<dyn Conn + Send + Sync>, WebRtcUtilError> {
        let writer: Arc<dyn UDPMuxWriter + Send + Sync> = self.clone();
        self.get_owned_conn(ice_credential, None, writer)
            .await
            .map(|connection| Arc::new(connection) as Arc<dyn Conn + Send + Sync>)
    }

    async fn remove_conn_by_ufrag(&self, ice_credential: &str) {
        self.remove_owned_connection(ice_credential, None).await;
    }
}

#[async_trait]
impl UDPMuxWriter for DirectUdpMux {
    async fn register_conn_for_address(&self, connection: &UDPMuxConn, addr: SocketAddr) {
        self.register_owned_address(connection, addr, None);
    }

    async fn send_to(&self, packet: &[u8], target: &SocketAddr) -> Result<usize, WebRtcUtilError> {
        let socket = self
            .socket
            .upgrade()
            .ok_or(WebRtcUtilError::ErrUseClosedNetworkConn)?;
        socket
            .send_to(packet, target)
            .await
            .map_err(|error| WebRtcUtilError::Io(error.into()))
    }
}

fn local_ice_credential(packet: &[u8]) -> Option<String> {
    stun_ice_credentials(packet).map(|(local, _)| local)
}

fn stun_ice_credentials(packet: &[u8]) -> Option<(String, String)> {
    if !is_stun_message(packet) {
        return None;
    }
    let mut message = StunMessage::new();
    message.unmarshal_binary(packet).ok()?;
    let (attribute, found) = message.attributes.get(ATTR_USERNAME);
    if !found {
        return None;
    }
    let username = String::from_utf8(attribute.value).ok()?;
    let (local, remote) = username.split_once(':')?;
    Some((local.to_string(), remote.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;
    use std::pin::pin;
    use std::task::Poll;
    use stun::agent::TransactionId;
    use stun::attributes::ATTR_USE_CANDIDATE;
    use stun::message::{BINDING_REQUEST, BINDING_SUCCESS};
    use stun::textattrs::Username;
    use tokio::task::JoinSet;
    use webrtc_stack::ice_transport::ice_connection_state::RTCIceConnectionState;
    use webrtc_stack::peer_connection::signaling_state::RTCSignalingState;

    // Relay STUN both ways and drop everything else, from one upstream port.
    // ICE connects and stays up, but the listener's DTLS server never sees a
    // ClientHello: what a browser that gives up after ICE leaves behind.
    // While `nominate` is false, the client's nominating checks are dropped as
    // well, so ICE does not connect and DTLS does not start.
    async fn spawn_stun_only_relay(
        listener: SocketAddr,
        nominate: Arc<AtomicBool>,
    ) -> std::io::Result<(SocketAddr, JoinSet<()>)> {
        let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        let back = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        let relay_addr = front.local_addr()?;
        let client = Arc::new(parking_lot::Mutex::new(None));
        let mut tasks = JoinSet::new();
        {
            let (front, back, client) =
                (Arc::clone(&front), Arc::clone(&back), Arc::clone(&client));
            tasks.spawn(async move {
                let mut buffer = [0u8; 2048];
                while let Ok((length, source)) = front.recv_from(&mut buffer).await {
                    *client.lock() = Some(source);
                    if let Some(packet) = buffer
                        .get(..length)
                        .filter(|packet| is_stun_message(packet))
                        .filter(|packet| nominate.load(Ordering::Relaxed) || !is_nomination(packet))
                    {
                        let _ = back.send_to(packet, listener).await;
                    }
                }
            });
        }
        tasks.spawn(async move {
            let mut buffer = [0u8; 2048];
            while let Ok((length, _)) = back.recv_from(&mut buffer).await {
                let packet = buffer
                    .get(..length)
                    .filter(|packet| is_stun_message(packet));
                let target = *client.lock();
                if let (Some(packet), Some(target)) = (packet, target) {
                    let _ = front.send_to(packet, target).await;
                }
            }
        });
        Ok((relay_addr, tasks))
    }

    fn is_nomination(packet: &[u8]) -> bool {
        let mut message = StunMessage::new();
        message.unmarshal_binary(packet).is_ok() && message.contains(ATTR_USE_CANDIDATE)
    }

    #[derive(Clone, Copy, Debug)]
    enum Teardown {
        // The application closes the connection before ICE connects, as
        // ant-node does when it rejects one on accept. DTLS never starts.
        CloseOnAccept,
        // The application closes the connection, as ant-node does on errors.
        Close,
        // The application drops the connection without closing it.
        Drop,
        // Nobody acts until the listener's setup deadline closes it.
        SetupDeadline,
        // The application closes the connection twice at once, then drops it.
        ConcurrentClose,
        // The application gives up on a close part-way, then drops the
        // connection, so the drop's teardown runs where the close stopped.
        CancelledClose,
        // The setup deadline's teardown runs while the application's close is
        // part-way through.
        CloseDuringSetupDeadline,
    }

    // A listener with one accepted connection from a native client behind a
    // STUN-only relay, which forwards nominations while `nominate` is true.
    async fn accept_through_stun_only_relay(
        nominate: Arc<AtomicBool>,
    ) -> anyhow::Result<(
        WebRtcDirectListener,
        WebRtcDirectConnection,
        JoinSet<()>,
        JoinSet<()>,
    )> {
        let certificate = WebRtcCertificate::generate()?;
        let certificate_hash = certificate.sha256_digest()?;
        let mut listener = WebRtcDirectListener::bind("127.0.0.1:0".parse()?, certificate).await?;
        let (relay, relay_tasks) = spawn_stun_only_relay(listener.local_addr(), nominate).await?;
        let endpoint = WebRtcDirectAddr::new(
            relay,
            crate::transport::WebRtcCertificateHash::new(certificate_hash),
        )?;
        let mut client = JoinSet::new();
        client.spawn(async move {
            let _ = WebRtcDirectClient::dial(&endpoint, "pending-dtls-test").await;
        });
        let connection = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await??;
        Ok((listener, connection, relay_tasks, client))
    }

    async fn assert_teardown_releases_pending_dtls_handshake(
        teardown: Teardown,
    ) -> anyhow::Result<()> {
        let nominate = !matches!(teardown, Teardown::CloseOnAccept);
        let (_listener, connection, _relay, _client) =
            accept_through_stun_only_relay(Arc::new(AtomicBool::new(nominate))).await?;
        let dtls = connection.peer_connection.dtls_transport();
        if nominate {
            // ICE is connected and the DTLS server is waiting for a ClientHello.
            tokio::time::timeout(Duration::from_secs(5), async {
                while dtls.state() != RTCDtlsTransportState::Connecting {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
        } else {
            // Nothing nominated a pair, so ICE has not connected and DTLS has
            // not started.
            assert_eq!(dtls.state(), RTCDtlsTransportState::New);
        }
        let transport = Arc::downgrade(&dtls);
        drop(dtls);

        match teardown {
            Teardown::CloseOnAccept | Teardown::Close => connection.close().await?,
            Teardown::Drop => {}
            Teardown::SetupDeadline => {
                tokio::time::timeout(Duration::from_secs(15), async {
                    while connection.peer_connection.connection_state()
                        != RTCPeerConnectionState::Closed
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await?;
            }
            Teardown::ConcurrentClose => {
                let (first, second) = tokio::join!(connection.close(), connection.close());
                first?;
                second?;
            }
            Teardown::CancelledClose => {
                // One poll of the close delivers the close_notify and leaves
                // it waiting for the handshake to end. Abandon it there.
                let mut close = pin!(connection.close());
                let first = poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx))).await;
                assert!(
                    first.is_pending(),
                    "close finished on its first poll, so nothing was abandoned"
                );
                // Nothing else delivers a close_notify before the drop, so the
                // handshake ending shows the abandoned close delivered one.
                tokio::time::timeout(Duration::from_secs(5), async {
                    while connection.peer_connection.dtls_transport().state()
                        == RTCDtlsTransportState::Connecting
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await?;
            }
            Teardown::CloseDuringSetupDeadline => {
                // Suspend a close in its wait for the handshake to end, as in
                // CancelledClose, and finish it only once the setup deadline's
                // teardown has begun closing the peer connection: webrtc marks
                // the signaling state Closed before the first await of close.
                let mut close = pin!(connection.close());
                let first = poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx))).await;
                assert!(first.is_pending(), "close finished on its first poll");
                tokio::time::timeout(Duration::from_secs(15), async {
                    while connection.peer_connection.signaling_state() != RTCSignalingState::Closed
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await?;
                close.await?;
            }
        }
        drop(connection);

        // The handshake future owns the peer connection's internals. If it
        // never ends, neither does the transport it is negotiating for.
        let released = tokio::time::timeout(Duration::from_secs(5), async {
            while transport.strong_count() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            released.is_ok(),
            "{teardown:?} left the pending DTLS handshake holding its peer connection"
        );
        Ok(())
    }

    #[test]
    fn close_notify_record_decodes_as_a_plaintext_close_notify() -> anyhow::Result<()> {
        use webrtc_stack::dtls::content::{Content, ContentType};
        use webrtc_stack::dtls::record_layer::RecordLayer;
        use webrtc_stack::dtls::record_layer::record_layer_header::{
            MAX_SEQUENCE_NUMBER, PROTOCOL_VERSION1_2,
        };

        let record = RecordLayer::unmarshal(&mut DTLS_CLOSE_NOTIFY.as_slice())?;
        let header = record.record_layer_header;
        assert_eq!(header.content_type, ContentType::Alert);
        assert_eq!(header.protocol_version, PROTOCOL_VERSION1_2);
        assert_eq!(header.epoch, 0);
        assert_eq!(header.sequence_number, MAX_SEQUENCE_NUMBER);
        let Content::Alert(alert) = record.content else {
            anyhow::bail!("expected an alert, got {:?}", record.content);
        };
        assert_eq!(alert.to_string(), "Alert LevelWarning: CloseNotify");
        Ok(())
    }

    #[tokio::test]
    async fn close_on_accept_releases_dtls_transport() -> anyhow::Result<()> {
        assert_teardown_releases_pending_dtls_handshake(Teardown::CloseOnAccept).await
    }

    #[tokio::test]
    async fn close_releases_pending_dtls_handshake() -> anyhow::Result<()> {
        assert_teardown_releases_pending_dtls_handshake(Teardown::Close).await
    }

    #[tokio::test]
    async fn drop_releases_pending_dtls_handshake() -> anyhow::Result<()> {
        assert_teardown_releases_pending_dtls_handshake(Teardown::Drop).await
    }

    #[tokio::test]
    async fn setup_deadline_releases_pending_dtls_handshake() -> anyhow::Result<()> {
        assert_teardown_releases_pending_dtls_handshake(Teardown::SetupDeadline).await
    }

    #[tokio::test]
    async fn concurrent_close_releases_pending_dtls_handshake() -> anyhow::Result<()> {
        assert_teardown_releases_pending_dtls_handshake(Teardown::ConcurrentClose).await
    }

    #[tokio::test]
    async fn cancelled_close_releases_pending_dtls_handshake() -> anyhow::Result<()> {
        assert_teardown_releases_pending_dtls_handshake(Teardown::CancelledClose).await
    }

    #[tokio::test]
    async fn close_during_setup_deadline_releases_pending_dtls_handshake() -> anyhow::Result<()> {
        assert_teardown_releases_pending_dtls_handshake(Teardown::CloseDuringSetupDeadline).await
    }

    // Teardown stops a DTLS transport that is still New before it closes the
    // peer connection. ICE can connect in between; it must not start a
    // handshake, and webrtc's Connected for ICE connected with DTLS closed must
    // not count as a successful connection.
    #[tokio::test]
    async fn ice_connecting_after_dtls_stop_starts_no_handshake() -> anyhow::Result<()> {
        let nominate = Arc::new(AtomicBool::new(false));
        let (listener, connection, _relay, _client) =
            accept_through_stun_only_relay(Arc::clone(&nominate)).await?;
        let diagnostics = listener.diagnostics();
        let peer = Arc::clone(&connection.peer_connection);
        let dtls = peer.dtls_transport();
        assert_eq!(dtls.state(), RTCDtlsTransportState::New);

        stop_unstarted_dtls(&dtls).await;
        nominate.store(true, Ordering::Relaxed);
        tokio::time::timeout(Duration::from_secs(5), async {
            while peer.ice_connection_state() != RTCIceConnectionState::Connected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        // A stopped transport stays Closed, so this wait only bounds how long a
        // wrongly started handshake has to show; webrtc tries the DTLS start
        // right after ICE connects.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            dtls.state(),
            RTCDtlsTransportState::Closed,
            "ICE connecting after the stop started a DTLS handshake"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while peer.connection_state() != RTCPeerConnectionState::Connected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;

        // webrtc runs each call of the state handler to completion under one
        // lock, and close publishes Closed through it only after stopping the
        // transports, so once close returns the diagnostics have seen the
        // Connected call too. The Closed count shows the handler recorded the
        // close, not the connection's drop below.
        connection.close().await?;
        let snapshot = diagnostics.snapshot();
        assert_eq!(snapshot.closed_connections, 1);
        assert_eq!(
            snapshot.successful_connections, 0,
            "ICE connected with DTLS stopped counted as a successful connection"
        );
        assert_eq!(snapshot.failed_connections, 1);

        let transport = Arc::downgrade(&dtls);
        drop((dtls, peer, connection));
        let released = tokio::time::timeout(Duration::from_secs(5), async {
            while transport.strong_count() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            released.is_ok(),
            "the closed connection kept its DTLS transport"
        );
        Ok(())
    }

    #[test]
    fn parses_only_supported_saorsa_profile_credentials() {
        let v1 = "saorsa+webrtc+v1/0123456789abcdefghijklmnopqrstuv";
        assert_eq!(
            parse_profile_credentials(v1, v1),
            Some(ProfileCredentials {
                server_ufrag: v1.to_string(),
                client_ufrag: v1.to_string(),
                client_pwd: v1.to_string(),
            })
        );

        let client_pwd = "browserClientPassword1234";
        let server_ufrag = format!("{ICE_CREDENTIAL_PREFIX_V2}{client_pwd}");
        assert_eq!(
            parse_profile_credentials(&server_ufrag, "browserClientUfrag"),
            Some(ProfileCredentials {
                server_ufrag,
                client_ufrag: "browserClientUfrag".to_string(),
                client_pwd: client_pwd.to_string(),
            })
        );

        assert!(parse_profile_credentials("unknown+webrtc+v2/abcd", "browserClient").is_none());
        assert!(parse_profile_credentials("saorsa+webrtc+v2/too-short", "browserClient").is_none());
        assert!(
            parse_profile_credentials(
                "saorsa+webrtc+v2/abcdefghijklmnopqrstuv\r\na=candidate:x",
                "browserClient"
            )
            .is_none()
        );
        assert!(parse_profile_credentials(&"a".repeat(257), "browserClient").is_none());
    }

    #[test]
    fn generated_certificate_round_trips_and_has_sha256_pin() {
        let certificate = WebRtcCertificate::generate().unwrap();
        let digest = certificate.sha256_digest().unwrap();
        assert_ne!(digest, [0_u8; 32]);

        let loaded = WebRtcCertificate::from_pem(&certificate.serialize_pem()).unwrap();
        assert_eq!(loaded.sha256_digest().unwrap(), digest);
    }

    #[tokio::test]
    async fn expired_arm_metadata_does_not_expire_a_valid_certificate_or_change_its_pin() {
        let certificate = WebRtcCertificate::generate().unwrap();
        let pem = certificate.serialize_pem();
        let (_, material) = pem.split_once("-----END EXPIRES-----").unwrap();
        // Simulate the old ARM expiry already in the past, and a malformed short
        // header that the upstream loader would panic while indexing.
        for expiry in ["AAAAAAAAAAA=", "AA=="] {
            let old = format!("-----BEGIN EXPIRES-----\n{expiry}\n-----END EXPIRES-----{material}");
            let loaded = WebRtcCertificate::from_pem(&old).unwrap();
            assert_eq!(
                loaded.sha256_digest().unwrap(),
                certificate.sha256_digest().unwrap()
            );
            let api = APIBuilder::new().build();
            let peer = api
                .new_peer_connection(RTCConfiguration {
                    certificates: vec![loaded.inner],
                    ..Default::default()
                })
                .await
                .unwrap();
            peer.close().await.unwrap();
        }
    }

    #[test]
    fn expired_signed_certificate_is_rejected() {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let mut params = webrtc_rcgen::CertificateParams::default();
        params.not_after = time::OffsetDateTime::now_utc() - time::Duration::days(1);
        let certificate = params.self_signed(&key).unwrap();
        let dtls = webrtc_stack::dtls::crypto::Certificate {
            certificate: vec![certificate.der().clone()],
            private_key: webrtc_stack::dtls::crypto::CryptoPrivateKey::try_from(&key).unwrap(),
        };
        assert!(WebRtcCertificate::from_dtls(dtls).is_err());
    }

    #[test]
    fn client_offer_contains_observed_address_and_distinct_v2_credentials() {
        let address: SocketAddr = "192.0.2.4:49152".parse().unwrap();
        let client_ufrag = "browserClientUfrag";
        let client_pwd = "browserClientPassword1234";
        let offer = client_offer(address, client_ufrag, client_pwd);
        assert!(offer.contains("m=application 49152 UDP/DTLS/SCTP webrtc-datachannel"));
        assert!(offer.contains("c=IN IP4 192.0.2.4"));
        assert!(offer.contains(&format!("a=ice-ufrag:{client_ufrag}")));
        assert!(offer.contains(&format!("a=ice-pwd:{client_pwd}")));
        assert!(RTCSessionDescription::offer(offer).is_ok());
    }

    #[tokio::test]
    async fn dropped_outbound_owner_closes_rtc_even_with_other_peer_references() {
        use webrtc_stack::peer_connection::peer_connection_state::RTCPeerConnectionState;
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let (incoming, _) = mpsc::channel(1);
        let mux = DirectUdpMux::new(Arc::clone(&socket), socket.local_addr().unwrap(), incoming);
        let peer = Arc::new(
            APIBuilder::new()
                .build()
                .new_peer_connection(RTCConfiguration::default())
                .await
                .unwrap(),
        );
        let owner = OutboundAssociationOwner {
            runtime: tokio::runtime::Handle::try_current().unwrap(),
            peer: Arc::clone(&peer),
            mux: Arc::clone(&mux),
        };
        std::thread::spawn(move || drop(owner)).join().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if peer.connection_state() == RTCPeerConnectionState::Closed
                    && mux.shutdown.is_cancelled()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("outbound owner retained live RTC state");
    }

    #[tokio::test]
    async fn cancelled_dial_releases_udp_socket() {
        use std::time::Duration;

        let remote = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = WebRtcDirectAddr::new(
            remote.local_addr().unwrap(),
            crate::transport::WebRtcCertificateHash::new([1; 32]),
        )
        .unwrap();
        let dial = tokio::spawn(async move {
            WebRtcDirectClient::dial(&endpoint, "cancelled-dial-test").await
        });
        let mut buffer = [0; 2048];
        let (_, local_addr) =
            tokio::time::timeout(Duration::from_secs(2), remote.recv_from(&mut buffer))
                .await
                .unwrap()
                .unwrap();
        dial.abort();
        assert!(matches!(dial.await, Err(error) if error.is_cancelled()));

        // Task cancellation is scheduled asynchronously; wait for the driver
        // to drop its socket, rather than relying on a fixed scheduler delay.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match UdpSocket::bind(local_addr).await {
                    Ok(_) => break,
                    Err(error) if error.kind() == ErrorKind::AddrInUse => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("could not rebind cancelled dial socket: {error}"),
                }
            }
        })
        .await
        .expect("cancelled dial retained its UDP socket");
    }

    async fn assert_incomplete_dial_releases_mux(cancel: bool) {
        use std::time::Duration;

        let remote = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = WebRtcDirectAddr::new(remote.local_addr().unwrap(), [1; 32].into()).unwrap();
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();
        let (incoming, _) = mpsc::channel(1);
        let mux = DirectUdpMux::new(Arc::clone(&socket), local_addr, incoming);
        let weak_mux = Arc::downgrade(&mux);
        let dial = tokio::spawn(async move {
            create_outbound_client(&endpoint, "incomplete-dial-test", local_addr, mux).await
        });

        // Wait until setup has installed the channel callback and started ICE.
        // The silent endpoint never lets that callback run.
        let mut buffer = [0; 2048];
        tokio::time::timeout(Duration::from_secs(2), remote.recv_from(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        if cancel {
            dial.abort();
            assert!(matches!(dial.await, Err(error) if error.is_cancelled()));
        } else {
            let result = tokio::time::timeout(Duration::from_secs(12), dial)
                .await
                .expect("dial did not time out")
                .unwrap();
            assert!(matches!(result, Err(WebRtcDirectError::Session(message))
                if message == "DataChannel opening timed out"));
        }

        // Closing the socket alone is insufficient: a channel callback cycle
        // also retains its settings and mux after asynchronous cleanup finishes.
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak_mux.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("incomplete dial retained its channel settings and UDP mux");
    }

    #[tokio::test]
    async fn cancelled_dial_releases_channel_and_mux() {
        assert_incomplete_dial_releases_mux(true).await;
    }

    #[tokio::test]
    async fn timed_out_dial_releases_channel_and_mux() {
        assert_incomplete_dial_releases_mux(false).await;
    }

    #[tokio::test]
    async fn native_v2_dial_opens_a_bidirectional_data_channel() {
        let certificate = WebRtcCertificate::generate().unwrap();
        let certificate_hash = certificate.sha256_digest().unwrap();
        let mut listener = WebRtcDirectListener::bind("127.0.0.1:0".parse().unwrap(), certificate)
            .await
            .unwrap();
        let endpoint = WebRtcDirectAddr::new(
            listener.local_addr(),
            crate::transport::WebRtcCertificateHash::new(certificate_hash),
        )
        .unwrap();
        let listener_diagnostics = listener.diagnostics();
        let accepted = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            let channel = connection.accept_data_channel().await.unwrap();
            assert_eq!(channel.label(), "saorsa-v2-test");
            assert_eq!(channel.receive().await.unwrap(), b"client-to-server");
            channel.send(b"server-to-client").await.unwrap();
            assert_eq!(channel.receive().await.unwrap(), b"client-finished");
            let metrics = connection.metrics().unwrap();
            assert_eq!(metrics.bytes_sent, b"server-to-client".len() as u64);
            assert_eq!(
                metrics.bytes_received,
                (b"client-to-server".len() + b"client-finished".len()) as u64
            );
            assert!(metrics.connected_at.is_some());
            assert!(metrics.last_activity.is_some());
            connection.close().await.unwrap();
        });

        let client = WebRtcDirectClient::dial(&endpoint, "saorsa-v2-test")
            .await
            .unwrap();
        client
            .data_channel()
            .send(b"client-to-server")
            .await
            .unwrap();
        assert_eq!(
            client.data_channel().receive().await.unwrap(),
            b"server-to-client"
        );
        client
            .data_channel()
            .send(b"client-finished")
            .await
            .unwrap();
        let client_metrics = client.metrics().unwrap();
        assert_eq!(
            client_metrics.bytes_received,
            b"server-to-client".len() as u64
        );
        assert!(client_metrics.connected_at.is_some());
        accepted.await.unwrap();
        let listener_metrics = listener_diagnostics.snapshot();
        assert_eq!(listener_metrics.successful_connections, 1);
        assert_eq!(listener_metrics.failed_connections, 0);
        assert_eq!(listener_metrics.closed_connections, 1);
        assert_eq!(listener_metrics.active_connections, 0);
        assert!(listener_metrics.connections.is_empty());
        assert_eq!(
            listener_metrics.bytes_sent,
            b"server-to-client".len() as u64
        );
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn rejects_unordered_and_partially_reliable_channels() {
        use std::time::Duration;
        use webrtc_stack::data_channel::data_channel_init::RTCDataChannelInit;

        let certificate = WebRtcCertificate::generate().unwrap();
        let hash = certificate.sha256_digest().unwrap();
        let mut listener = WebRtcDirectListener::bind("127.0.0.1:0".parse().unwrap(), certificate)
            .await
            .unwrap();
        let endpoint = WebRtcDirectAddr::new(listener.local_addr(), hash.into()).unwrap();
        let accepted = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            let channel = connection.accept_data_channel().await.unwrap();
            (listener, connection, channel)
        });
        let client = WebRtcDirectClient::dial(&endpoint, "reliable-control")
            .await
            .unwrap();
        let (listener, mut connection, control) = accepted.await.unwrap();

        for options in [
            RTCDataChannelInit {
                ordered: Some(false),
                ..Default::default()
            },
            RTCDataChannelInit {
                max_retransmits: Some(0),
                ..Default::default()
            },
            RTCDataChannelInit {
                max_packet_life_time: Some(1000),
                ..Default::default()
            },
        ] {
            let channel = client
                .peer_connection
                .create_data_channel("unsupported-channel", Some(options))
                .await
                .unwrap();
            let (opened_tx, mut opened_rx) = mpsc::channel(1);
            let open_channel = Arc::clone(&channel);
            channel.on_open(Box::new(move || {
                let channel = Arc::clone(&open_channel);
                let opened = opened_tx.clone();
                Box::pin(async move {
                    opened.send(channel.detach().await.unwrap()).await.unwrap();
                })
            }));
            let detached = tokio::time::timeout(Duration::from_secs(2), opened_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let mut buffer = [0; 16];
            let closed = tokio::time::timeout(
                Duration::from_secs(2),
                detached.read_data_channel(&mut buffer),
            )
            .await
            .expect("rejected channel was not reset");
            assert!(matches!(closed, Ok((0, _)) | Err(_)));
        }

        // The association still accepts traffic on its reliable channel, and
        // none of the rejected channels reached the application accept queue.
        client.data_channel().send(b"still reliable").await.unwrap();
        assert_eq!(control.receive().await.unwrap(), b"still reliable");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), connection.accept_data_channel())
                .await
                .is_err()
        );
        connection.close().await.unwrap();
        client.close().await.unwrap();
        listener.close().await.unwrap();
    }

    #[tokio::test]
    async fn retired_generation_cannot_mutate_replacement() {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let (tx, _) = mpsc::channel(1);
        let mux = DirectUdpMux::new(Arc::clone(&socket), socket.local_addr().unwrap(), tx);
        let credential = "replacement".to_string();
        let remote = "127.0.0.1:45678".parse().unwrap();
        let retired = Arc::new(AssociationMux {
            mux: mux.clone(),
            credential: credential.clone(),
            generation: 1,
        });
        let owner = AssociationOwner {
            mux: mux.clone(),
            credential: credential.clone(),
            remote_addr: remote,
            peer: None,
            generation: Some(1),
        };
        mux.admitted.write().insert(credential.clone(), (remote, 2));
        mux.pending.write().insert(credential.clone(), (remote, 2));
        drop(owner);
        retired.remove_conn_by_ufrag(&credential).await;
        retired.close().await.unwrap();
        assert!(retired.clone().get_conn(&credential).await.is_err());
        assert!(retired.send_to(b"stale", &remote).await.is_err());
        tokio::task::yield_now().await;
        assert_eq!(mux.pending.read().get(&credential), Some(&(remote, 2)));
        assert_eq!(mux.admitted.read().get(&credential), Some(&(remote, 2)));
        assert!(mux.conns.lock().await.is_empty());
        mux.close().await.unwrap();
    }

    #[tokio::test]
    async fn ice_closed_connection_releases_routing_and_preserves_replacement() {
        use std::time::Duration;

        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let (incoming, _) = mpsc::channel(1);
        let mux = DirectUdpMux::new(Arc::clone(&socket), socket.local_addr().unwrap(), incoming);
        let credential = "reused-credential";
        let remote = "127.0.0.1:45678".parse().unwrap();

        for generation in 1..=2 {
            mux.admitted
                .write()
                .insert(credential.into(), (remote, generation));
            mux.pending
                .write()
                .insert(credential.into(), (remote, generation));
            let association = Arc::new(AssociationMux {
                mux: Arc::clone(&mux),
                credential: credential.into(),
                generation,
            });
            let _connection = association.clone().get_conn(credential).await.unwrap();
            let connection = mux.conns.lock().await.get(credential).unwrap().clone();
            connection.add_address(remote).await;

            if generation == 2 {
                // Late and repeated cleanup from the first generation must
                // leave the replacement's source mapping and connection alive.
                for _ in 0..2 {
                    mux.remove_owned_connection(credential, Some(1)).await;
                }
                let routed = mux.connection_for_packet(&[0], remote).await.unwrap();
                assert!(!routed.is_closed());
                assert_eq!(mux.admitted.read().get(credential), Some(&(remote, 2)));
            }

            // ICE failure closes candidates before notifying the mux. Upstream
            // clears the connection's address list as part of that close.
            connection.close();
            assert!(connection.get_addresses().is_empty());
            tokio::time::timeout(Duration::from_secs(2), async {
                while mux.conns.lock().await.contains_key(credential) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("closed connection was not removed");
            assert!(mux.address_map.read().is_empty());
            assert!(mux.pending.read().is_empty());
            assert!(mux.admitted.read().is_empty());
            assert!(mux.connection_for_packet(&[0], remote).await.is_none());
            mux.remove_owned_connection(credential, Some(generation))
                .await;
        }
        mux.close().await.unwrap();
    }

    #[tokio::test]
    async fn queued_associations_require_their_original_source_and_generation() {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let (tx, _) = mpsc::channel(1);
        let mux = DirectUdpMux::new(Arc::clone(&socket), socket.local_addr().unwrap(), tx);
        let current = "127.0.0.1:45679".parse().unwrap();
        let credential = "replacement".to_string();
        mux.pending.write().insert(credential.clone(), (current, 2));
        mux.admitted
            .write()
            .insert(credential.clone(), (current, 2));
        for (source, generation, accepted) in [
            (current, 1, false),
            ("127.0.0.1:45678".parse().unwrap(), 2, false),
            (current, 2, true),
        ] {
            let association = IncomingAssociation {
                remote_addr: source,
                server_ufrag: credential.clone(),
                client_ufrag: "client".into(),
                client_pwd: "password".into(),
                initial_packet: Vec::new(),
                generation: Some(generation),
            };
            assert_eq!(mux.is_pending(&association), accepted);
        }
        mux.close().await.unwrap();
    }

    async fn prove_association(
        sender: &UdpSocket,
        listener: &WebRtcDirectListener,
        password: &str,
    ) -> Vec<u8> {
        let packet = admission::tests::request(password);
        sender
            .send_to(&packet, listener.local_addr())
            .await
            .unwrap();
        let mut buffer = [0u8; 2048];
        let length = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let (length, _) = sender.recv_from(&mut buffer).await.unwrap();
                let mut message = StunMessage::new();
                message.unmarshal_binary(&buffer[..length]).unwrap();
                // A previous association can have already answered its admitted
                // request. Wait for this association's admission challenge.
                if message.typ == BINDING_REQUEST
                    && stun::integrity::MessageIntegrity::new_short_term_integrity(
                        password.to_string(),
                    )
                    .check(&mut message)
                    .is_ok()
                {
                    break length;
                }
            }
        })
        .await
        .unwrap();
        let response =
            admission::tests::response(&buffer[..length], password, sender.local_addr().unwrap());
        sender
            .send_to(&response, listener.local_addr())
            .await
            .unwrap();
        sender
            .send_to(&packet, listener.local_addr())
            .await
            .unwrap();
        packet
    }

    #[tokio::test]
    async fn admitted_binding_request_reaches_ice_without_another_client_packet() {
        use std::time::Duration;

        let mut listener = WebRtcDirectListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            WebRtcCertificate::generate().unwrap(),
        )
        .await
        .unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let password = "browserPassword0123456789";
        let admitted = prove_association(&sender, &listener, password).await;
        let mut request = StunMessage::new();
        request.unmarshal_binary(&admitted).unwrap();
        let connection = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();

        // No retransmit: ICE must answer the exact request consumed by admission.
        // The generous deadline is for slow CI, not a throughput assertion.
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut buffer = [0u8; 2048];
            loop {
                let (length, _) = sender.recv_from(&mut buffer).await.unwrap();
                let mut response = StunMessage::new();
                response.unmarshal_binary(&buffer[..length]).unwrap();
                if response.typ == BINDING_SUCCESS {
                    assert_eq!(response.transaction_id, request.transaction_id);
                    stun::integrity::MessageIntegrity::new_short_term_integrity(format!(
                        "{ICE_CREDENTIAL_PREFIX_V2}{password}"
                    ))
                    .check(&mut response)
                    .unwrap();
                    break;
                }
            }
        })
        .await
        .expect("ICE waited for a retransmission after admission");
        connection.close().await.unwrap();
        listener.close().await.unwrap();
    }

    #[tokio::test]
    async fn listener_accept_returns_closed_even_with_queued_associations() {
        use std::time::Duration;

        for queued in [false, true] {
            let mut listener = WebRtcDirectListener::bind(
                "127.0.0.1:0".parse().unwrap(),
                WebRtcCertificate::generate().unwrap(),
            )
            .await
            .unwrap();
            if queued {
                listener
                    .mux
                    .incoming
                    .try_send(IncomingAssociation {
                        remote_addr: "127.0.0.1:55555".parse().unwrap(),
                        server_ufrag: format!(
                            "{ICE_CREDENTIAL_PREFIX_V2}browserPassword0123456789"
                        ),
                        client_ufrag: "browserClientUfrag".to_string(),
                        client_pwd: "browserPassword0123456789".to_string(),
                        initial_packet: Vec::new(),
                        generation: None,
                    })
                    .unwrap();
            }
            listener.close().await.unwrap();
            for _ in 0..2 {
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(1), listener.accept()).await,
                    Ok(Err(WebRtcDirectError::Closed))
                ));
            }
        }
    }

    #[tokio::test]
    async fn listener_shutdown_wakes_pending_accept() {
        use std::time::Duration;

        let mut listener = WebRtcDirectListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            WebRtcCertificate::generate().unwrap(),
        )
        .await
        .unwrap();
        let mux = Arc::clone(&listener.mux);
        let accept = listener.accept();
        tokio::pin!(accept);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut accept)
                .await
                .is_err()
        );
        mux.close().await.unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), accept).await,
            Ok(Err(WebRtcDirectError::Closed))
        ));
    }

    #[tokio::test]
    async fn closed_association_rejects_repeated_channel_accepts() {
        use std::time::Duration;

        let mut listener = WebRtcDirectListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            WebRtcCertificate::generate().unwrap(),
        )
        .await
        .unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        prove_association(&sender, &listener, "browserPassword0123456789").await;
        let mut connection = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        connection.close().await.unwrap();
        for _ in 0..2 {
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(1), connection.accept_data_channel())
                    .await,
                Ok(Err(WebRtcDirectError::Closed))
            ));
        }
        listener.close().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_wakes_accept_with_construction_in_flight() {
        let mut listener = WebRtcDirectListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            WebRtcCertificate::generate().unwrap(),
        )
        .await
        .unwrap();
        let (release, waiting) = tokio::sync::oneshot::channel();
        listener.accepting = Some(tokio::spawn(async move {
            let _ = waiting.await;
            Err(WebRtcDirectError::Closed)
        }));
        listener.close().await.unwrap();
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept()).await;
        release.send(()).unwrap();
        assert!(matches!(result, Ok(Err(WebRtcDirectError::Closed))));
    }

    #[tokio::test]
    async fn cancelled_accept_preserves_construction_and_drop_releases_slot() {
        let certificate = WebRtcCertificate::generate().unwrap();
        let mut listener = WebRtcDirectListener::bind("127.0.0.1:0".parse().unwrap(), certificate)
            .await
            .unwrap();
        let address = "127.0.0.1:45678".parse().unwrap();
        let credential = "cancelled-test".to_string();
        listener
            .mux
            .pending
            .write()
            .insert(credential.clone(), (address, 1));
        listener
            .mux
            .admitted
            .write()
            .insert(credential.clone(), (address, 1));
        let owner = AssociationOwner {
            mux: Arc::clone(&listener.mux),
            credential,
            remote_addr: address,
            generation: Some(1),
            peer: None,
        };
        let (release, wait) = tokio::sync::oneshot::channel();
        listener.accepting = Some(tokio::spawn(async move {
            let _owner = owner;
            let _ = wait.await;
            Err(WebRtcDirectError::Closed)
        }));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), listener.accept())
                .await
                .is_err()
        );
        assert!(listener.accepting.is_some());
        assert_eq!(listener.mux.pending.read().len(), 1);
        release.send(()).unwrap();
        assert!(matches!(
            listener.accept().await,
            Err(WebRtcDirectError::Closed)
        ));
        assert!(listener.mux.pending.read().is_empty());
        assert!(listener.accepting.is_none());
        listener.close().await.unwrap();
    }

    #[tokio::test]
    async fn dropping_incomplete_connection_closes_peer_and_mux() {
        let certificate = WebRtcCertificate::generate().unwrap();
        let mut listener = WebRtcDirectListener::bind("127.0.0.1:0".parse().unwrap(), certificate)
            .await
            .unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        prove_association(&sender, &listener, "browserPassword0123456789").await;
        let connection = listener.accept().await.unwrap();
        let peer = Arc::clone(&connection.peer_connection);
        drop(connection);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while peer.connection_state() != webrtc_stack::peer_connection::peer_connection_state::RTCPeerConnectionState::Closed {
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert!(listener.mux.pending.read().is_empty());
        listener.close().await.unwrap();
    }

    #[tokio::test]
    async fn closing_incomplete_associations_releases_pending_credentials() {
        let certificate = WebRtcCertificate::generate().unwrap();
        let mut listener = WebRtcDirectListener::bind("127.0.0.1:0".parse().unwrap(), certificate)
            .await
            .unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // Reachability is established, but no DTLS handshake follows.
        // Rejection and first-channel timeout both close this same association.
        // Reuse its credential to prove that cleanup also permits a later retry.
        for index in 0..8 {
            prove_association(&sender, &listener, &format!("browserPassword{index:016}")).await;
            let connection =
                tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
            // ICE can now consume the admitted request and register its source
            // before accept returns, releasing the pending reservation early.
            assert_eq!(listener.mux.admitted.read().len(), 1);
            connection.close().await.unwrap();
            assert!(listener.mux.pending.read().is_empty());
            assert!(listener.mux.admitted.read().is_empty());
            assert!(listener.mux.conns.lock().await.is_empty());
        }
        listener.close().await.unwrap();
    }

    #[tokio::test]
    async fn unreturned_probes_never_allocate_or_queue_associations() {
        let listener = WebRtcDirectListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            WebRtcCertificate::generate().unwrap(),
        )
        .await
        .unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for index in 0..MAX_PENDING_ASSOCIATIONS {
            sender
                .send_to(
                    &admission::tests::request(&format!("browserPassword{index:016}")),
                    listener.local_addr(),
                )
                .await
                .unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(listener.mux.pending.read().is_empty());
        assert!(listener.mux.admitted.read().is_empty());
        assert!(listener.mux.conns.lock().await.is_empty());
        assert!(listener.incoming.is_empty());
        listener.close().await.unwrap();
    }

    #[tokio::test]
    async fn validated_queue_respects_source_limit_and_expires_without_accept() {
        let listener = WebRtcDirectListener::bind_with_limits(
            "127.0.0.1:0".parse().unwrap(),
            WebRtcCertificate::generate().unwrap(),
            WebRtcAdmissionLimits {
                max_connections: 2,
                max_connections_per_ip: 1,
            },
        )
        .await
        .unwrap();
        let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        prove_association(&first, &listener, "browserPasswordFirst012345").await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        prove_association(&second, &listener, "browserPasswordSecond01234").await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(listener.mux.admitted.read().len(), 1);
        assert_eq!(listener.incoming.len(), 1);
        assert!(listener.mux.conns.lock().await.is_empty());
        tokio::time::timeout(std::time::Duration::from_secs(12), async {
            while !listener.mux.admitted.read().is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(listener.mux.pending.read().is_empty());
        listener.close().await.unwrap();
    }

    async fn receive_refusal(receiver: &UdpSocket) -> StunMessage {
        let mut buffer = [0u8; 2048];
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let (length, _) = receiver.recv_from(&mut buffer).await.unwrap();
                let mut message = StunMessage::new();
                message.unmarshal_binary(&buffer[..length]).unwrap();
                if message.typ == stun::message::BINDING_ERROR {
                    break message;
                }
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn full_listener_answers_a_proven_check_with_a_refusal() {
        for (limits, reason) in [
            (
                WebRtcAdmissionLimits {
                    max_connections: 1,
                    max_connections_per_ip: 1,
                },
                "Insufficient Capacity",
            ),
            (
                WebRtcAdmissionLimits {
                    max_connections: 2,
                    max_connections_per_ip: 1,
                },
                "Source Quota Reached",
            ),
        ] {
            let listener = WebRtcDirectListener::bind_with_limits(
                "127.0.0.1:0".parse().unwrap(),
                WebRtcCertificate::generate().unwrap(),
                limits,
            )
            .await
            .unwrap();
            let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            prove_association(&first, &listener, "browserPasswordFirst012345").await;
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while listener.mux.admitted.read().is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            let password = "browserPasswordSecond01234";
            let request = prove_association(&second, &listener, password).await;
            let mut refusal = receive_refusal(&second).await;
            let mut original = StunMessage::new();
            original.unmarshal_binary(&request).unwrap();
            assert_eq!(refusal.transaction_id, original.transaction_id);
            let mut error = stun::error_code::ErrorCodeAttribute::default();
            stun::message::Getter::get_from(&mut error, &refusal).unwrap();
            assert_eq!(error.code.0, stun::error_code::CODE_ALLOC_QUOTA_REACHED.0);
            assert_eq!(error.reason, reason.as_bytes());
            stun::integrity::MessageIntegrity::new_short_term_integrity(format!(
                "{ICE_CREDENTIAL_PREFIX_V2}{password}"
            ))
            .check(&mut refusal)
            .unwrap();
            assert_eq!(listener.mux.admitted.read().len(), 1);
            assert_eq!(listener.diagnostics().snapshot().rejected_connections, 1);
            listener.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn binding_request_credential_overrides_stale_source_address_mapping() {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local_addr = socket.local_addr().unwrap();
        let (incoming, _) = mpsc::channel(1);
        let mux = DirectUdpMux::new(socket, local_addr, incoming);
        let remote_addr: SocketAddr = "127.0.0.1:49152".parse().unwrap();
        let old_credential = "saorsa+webrtc+v1/oldoldoldoldoldoldoldoldoldold12";
        let new_credential = "saorsa+webrtc+v1/newnewnewnewnewnewnewnewnewnew12";

        let writer: Arc<dyn UDPMuxWriter + Send + Sync> = mux.clone();
        let old_connection = UDPMuxConn::new(UDPMuxConnParams {
            local_addr,
            key: old_credential.to_string(),
            udp_mux: Arc::downgrade(&writer),
        });
        let new_connection = UDPMuxConn::new(UDPMuxConnParams {
            local_addr,
            key: new_credential.to_string(),
            udp_mux: Arc::downgrade(&writer),
        });
        mux.conns
            .lock()
            .await
            .insert(new_credential.to_string(), new_connection);
        mux.address_map
            .write()
            .insert(remote_addr, old_connection.clone());

        let mut request = StunMessage::new();
        request
            .build(&[
                Box::new(BINDING_REQUEST),
                Box::new(TransactionId::new()),
                Box::new(Username::new(
                    ATTR_USERNAME,
                    format!("{new_credential}:browser"),
                )),
            ])
            .unwrap();
        let routed = mux
            .connection_for_packet(&request.raw, remote_addr)
            .await
            .unwrap();
        assert_eq!(routed.key(), new_credential);

        let mut unknown_request = StunMessage::new();
        unknown_request
            .build(&[
                Box::new(BINDING_REQUEST),
                Box::new(TransactionId::new()),
                Box::new(Username::new(
                    ATTR_USERNAME,
                    "saorsa+webrtc+v1/unknownunknownunknownunknown12:browser".to_string(),
                )),
            ])
            .unwrap();
        assert!(
            mux.connection_for_packet(&unknown_request.raw, remote_addr)
                .await
                .is_none()
        );

        let mut response = StunMessage::new();
        response
            .build(&[Box::new(BINDING_SUCCESS), Box::new(TransactionId::new())])
            .unwrap();
        let routed = mux
            .connection_for_packet(&response.raw, remote_addr)
            .await
            .unwrap();
        assert_eq!(routed.key(), old_credential);
        mux.admitted
            .write()
            .insert(new_credential.to_string(), (remote_addr, 1));
        let other_addr = "127.0.0.2:49152".parse().unwrap();
        assert!(
            mux.connection_for_packet(&request.raw, other_addr)
                .await
                .is_none()
        );
        assert_eq!(
            mux.admitted.read().get(new_credential).unwrap().0,
            remote_addr
        );
    }
}

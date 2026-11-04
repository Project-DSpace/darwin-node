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

//! MASQUE Relay Socket
//!
//! A virtual UDP socket backed entirely by a MASQUE relay tunnel.
//!
//! Implements [`AsyncUdpSocket`] so it can back a standalone Quinn
//! endpoint that accepts connections arriving through the relay.  The
//! node's **main** endpoint keeps its original UDP socket and is never
//! touched — this socket powers a **second** endpoint that provides an
//! additional inbound path.
//!
//! ## Routing
//!
//! - **Outgoing** → encoded as length-prefixed
//!   [`UncompressedDatagram`]s and written to the relay QUIC stream.
//! - **Incoming** → read from the relay QUIC stream, decoded, and
//!   queued for Quinn's `poll_recv`.
//!
//! ## Backpressure & buffering
//!
//! Both the send and receive paths use **bounded** `tokio::sync::mpsc`
//! channels rather than unbounded ones.  The receiver path gets natural
//! backpressure from `Sender::send().await` in the reader task: if
//! Quinn stops consuming, the reader stalls on the channel and QUIC
//! flow control eventually pauses the peer.  The sender path propagates
//! backpressure up into Quinn: when the send channel is full,
//! `try_send` returns [`io::ErrorKind::WouldBlock`] and the
//! [`TunnelPoller`] blocks on a [`Notify`] until the stream writer
//! task drains an item and frees a slot.  This preserves the
//! reliable-stream invariant of the MASQUE tunnel — packets are never
//! silently dropped — at the cost of pausing the inner Quinn endpoint
//! when the tunnel cannot keep up.

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex as PlMutex;
use std::fmt;
use std::future::Future;
use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};

use quinn_udp::{RecvMeta, Transmit};

use crate::VarInt;
use crate::high_level::{AsyncUdpSocket, UdpPoller};
use crate::masque::UncompressedDatagram;
use crate::masque::tunnel_control::{
    CONTROL_FRAME_MARKER, MAX_CONTROL_FRAME_BODY, TunnelControlFrame,
};
use crate::traffic::RELAY_CLIENT_TRAFFIC;

/// Interval at which the relay client sends a zero-length keepalive
/// frame through the relay stream.  Must be shorter than the NAT
/// conntrack UDP stream timeout (typically 120 s on Linux) to prevent
/// the mapping from expiring while the relay is idle.
const RELAY_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Upper bound on pending outbound packets queued for the stream writer.
/// A full channel causes `try_send` to drop packets (see module-level
/// docs), which matches UDP's lossy semantics.  8192 × ~1200 B ≈ 10 MB
/// of worst-case buffering before drops begin.
const SEND_QUEUE_CAPACITY: usize = 8192;
const RELAY_STREAM_BATCH_MAX_FRAMES: usize = 64;
const RELAY_STREAM_BATCH_MAX_BYTES: usize = 64 * 1024;

/// Upper bound on decoded inbound packets queued for `poll_recv`.
/// The reader task awaits on `Sender::send`, so when this fills up the
/// reader naturally backpressures the relay stream.
const RECV_QUEUE_CAPACITY: usize = 8192;

/// Safety cap on individual frame length read from the relay stream.
/// Legitimate QUIC packets are ≤65535 bytes; anything above this is a
/// framing error or corruption and closes the session.
const MAX_RELAY_FRAME: usize = 512 * 1024;

fn append_relay_frame(out: &mut Vec<u8>, encoded: &Bytes) {
    let frame_len = encoded.len() as u32;
    out.extend_from_slice(&frame_len.to_be_bytes());
    out.extend_from_slice(encoded);
}

/// Raw QUIC streams from a relay session, before socket construction.
///
/// Returned by `establish_relay_session` so the caller can construct a
/// [`MasqueRelaySocket`] with the additional context it needs.
pub struct RawRelayStreams {
    /// Send half of the relay QUIC stream (length-prefixed datagrams).
    pub send_stream: crate::high_level::SendStream,
    /// Receive half of the relay QUIC stream.
    pub recv_stream: crate::high_level::RecvStream,
}

/// Owns the background tasks and CONNECT-UDP streams backing a relay socket.
///
/// Calling [`shutdown`](Self::shutdown) aborts and joins every tunnel task.
/// Aborting the reader and writer tasks drops their QUIC stream halves, which
/// promptly tells the relay server to close the associated MASQUE session and
/// release its capacity slot. Shutdown is idempotent.
///
/// A tunnel can end because it broke or because we dismantled it, and the layers
/// above need to tell those apart: the first is a transport failure the backing
/// endpoint must hear about, the second is routine. [`is_closed`](Self::is_closed)
/// is true either way; [`shutdown_requested`](Self::shutdown_requested) only for
/// the second.
#[derive(Debug)]
pub(crate) struct RelayTunnelControl {
    tasks: PlMutex<Vec<tokio::task::JoinHandle<()>>>,
    state: Arc<TunnelState>,
}

/// The tunnel facts that outlive any one owner.
///
/// Held by the control, by the [`MasqueRelaySocket`] it owns, and by the tunnel
/// tasks. Sharing it strongly rather than reaching back through a
/// `Weak<RelayTunnelControl>` matters: the dial-through path in `p2p_endpoint`
/// drops its control as soon as the dial completes while the socket and its
/// tasks live on, and a task that could not record its exit there would leave a
/// parked poller waiting forever.
#[derive(Debug)]
struct TunnelState {
    /// Why the tunnel stopped carrying traffic. Holds a [`TunnelCause`].
    cause: AtomicU8,
    /// Woken once `cause` settles.
    closed: Notify,
    /// Whether the writer has stopped, so nothing more can leave the tunnel.
    /// Distinct from the tunnel being closed: the relay's two stream halves are
    /// independent, so a peer that resets only its server-to-client half leaves
    /// the writer able to flush a queued CONNECTION_CLOSE.
    writer_stopped: AtomicBool,
    /// Woken when the outbound queue frees a slot, and when nothing will ever
    /// free one again. A [`TunnelPoller`] parked on a full send queue is
    /// normally released by the writer draining a slot; when the writer is
    /// aborted instead, shutdown and the writer's own guard wake it here.
    send_capacity_freed: Notify,
}

/// Why a tunnel stopped carrying traffic.
///
/// Settled by compare-exchange, so the **first** transition out of
/// [`TunnelCause::Live`] wins. A tunnel that broke and was then cleaned up stays
/// classified as a failure; otherwise cleanup arriving a moment later would
/// silence the fault that triggered it.
///
/// A `u8` so it can live in an `AtomicU8` alongside the rest of [`TunnelState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum TunnelCause {
    Live = 0,
    Failed = 1,
    ShutdownRequested = 2,
}

impl TunnelState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            cause: AtomicU8::new(TunnelCause::Live as u8),
            closed: Notify::new(),
            writer_stopped: AtomicBool::new(false),
            send_capacity_freed: Notify::new(),
        })
    }

    /// Settle the terminal cause, if it has not already settled.
    ///
    /// Losing the race is normal and not an error: it means something else ended
    /// the tunnel first, and that first cause is the true one.
    fn settle(&self, cause: TunnelCause) {
        if self
            .cause
            .compare_exchange(
                TunnelCause::Live as u8,
                cause as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.closed.notify_waiters();
        }
    }

    fn is_closed(&self) -> bool {
        self.cause.load(Ordering::Acquire) != TunnelCause::Live as u8
    }

    fn shutdown_requested(&self) -> bool {
        self.cause.load(Ordering::Acquire) == TunnelCause::ShutdownRequested as u8
    }

    fn writer_stopped(&self) -> bool {
        self.writer_stopped.load(Ordering::Acquire)
    }

    /// Record that the writer has stopped and release anything waiting on it.
    ///
    /// The cause settles first so that a writer which broke on its own is filed
    /// as a failure before a concurrent teardown can claim the tunnel as
    /// intentionally dismantled.
    fn mark_writer_stopped(&self, cause: TunnelCause) {
        self.settle(cause);
        self.writer_stopped.store(true, Ordering::Release);
        self.send_capacity_freed.notify_waiters();
    }
}

/// Records the writer's exit exactly once, on whatever path ends it.
///
/// Held by the writer future, so it also runs when the task is aborted, even
/// before its first poll, since the guard is created before the spawn.
struct WriterExit(Arc<TunnelState>);

impl Drop for WriterExit {
    fn drop(&mut self) {
        // `Failed` loses to a cause already settled, which is the point: a
        // writer aborted by shutdown must not overwrite `ShutdownRequested`, and
        // one aborted after a reader failure must not overwrite `Failed`.
        self.0.mark_writer_stopped(TunnelCause::Failed);
    }
}

impl RelayTunnelControl {
    fn new(state: Arc<TunnelState>) -> Arc<Self> {
        Arc::new(Self {
            tasks: PlMutex::new(Vec::new()),
            state,
        })
    }

    #[cfg(test)]
    pub(crate) fn detached() -> Arc<Self> {
        Self::new(TunnelState::new())
    }

    fn register(&self, handle: tokio::task::JoinHandle<()>) {
        if self.is_closed() {
            handle.abort();
            return;
        }

        let mut tasks = self.tasks.lock();
        if self.is_closed() {
            handle.abort();
        } else {
            tasks.push(handle);
        }
    }

    /// Record that the tunnel broke rather than being dismantled on request.
    ///
    /// Called by the tunnel tasks when their stream fails, and by the relay
    /// health monitor before it tears down a relay it has found dead.
    pub(crate) fn mark_failed(&self) {
        self.state.settle(TunnelCause::Failed);
    }

    /// Returns whether the tunnel has failed or has been explicitly shut down.
    pub(crate) fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    /// Returns whether this tunnel was dismantled by a local shutdown request
    /// rather than by a transport failure.
    pub(crate) fn shutdown_requested(&self) -> bool {
        self.state.shutdown_requested()
    }

    /// Wait until the tunnel reader exits or shutdown is requested.
    pub(crate) async fn closed(&self) {
        loop {
            if self.is_closed() {
                return;
            }
            let notified = self.state.closed.notified();
            if self.is_closed() {
                return;
            }
            notified.await;
        }
    }

    /// Stop the tunnel and wait for all task-owned QUIC streams to be dropped.
    pub(crate) async fn shutdown(&self) {
        let handles = self.abort_tasks();
        for handle in handles {
            let _ = handle.await;
        }
    }

    /// Stop the tunnel without waiting for task cancellation to complete.
    ///
    /// This is the cancellation-safe fallback used by relay ownership guards:
    /// `Drop` cannot await, but it must still ensure the task-owned QUIC stream
    /// halves are scheduled for prompt release.
    pub(crate) fn shutdown_now(&self) {
        drop(self.abort_tasks());
    }

    fn abort_tasks(&self) -> Vec<tokio::task::JoinHandle<()>> {
        // Record everything before aborting anything. `settle` releases the
        // tunnel-death watcher and unblocks `poll_recv`, and the writer state
        // releases a parked `TunnelPoller`; all three must already be able to
        // see that this teardown was asked for rather than suffered.
        //
        // The writer's own `WriterExit` guard does the same, but only once the
        // abort lands, and `JoinHandle::abort` is asynchronous. Doing it here
        // means a parked poller is not waiting on a cancellation to complete: it
        // re-checks the writer state, not the send channel, which is typically
        // still open at this point.
        self.state
            .mark_writer_stopped(TunnelCause::ShutdownRequested);
        let handles = {
            let mut tasks = self.tasks.lock();
            std::mem::take(&mut *tasks)
        };
        for handle in &handles {
            handle.abort();
        }
        handles
    }
}

/// A virtual UDP socket backed entirely by a MASQUE relay tunnel.
///
/// All traffic — both outgoing and incoming — flows through the relay
/// QUIC stream.  This socket is intended for a **second** Quinn endpoint
/// dedicated to relay traffic, leaving the main endpoint and its
/// original UDP socket completely untouched.
pub struct MasqueRelaySocket {
    /// The relay's public address (returned as our local address).
    relay_public_addr: SocketAddr,
    /// Bounded MPSC receiver of decoded inbound packets.
    ///
    /// Wrapped in a parking_lot mutex purely for interior mutability
    /// (`Receiver::poll_recv` needs `&mut`).  Only Quinn's single I/O
    /// driver task polls `poll_recv` on this socket, so the lock is
    /// effectively uncontested at runtime.
    recv_rx: PlMutex<mpsc::Receiver<(Bytes, SocketAddr)>>,
    /// Bounded channel for outbound packets (drained by the background
    /// writer task into the relay send stream).
    send_tx: mpsc::Sender<Bytes>,
    /// Per-target maximum payload size enforced by [`Self::try_send`],
    /// populated by [`TunnelControlFrame::PmtuUpdate`] frames decoded by
    /// the reader task.  When a destination has an entry, any
    /// [`Transmit`] whose `contents.len()` exceeds the cap is silently
    /// dropped at try_send time, simulating packet loss for Quinn's
    /// DPLPMTUD machinery so the inner connection's MTU estimate
    /// converges to the true egress path MTU.  Targets without an
    /// entry are unconstrained by this layer (Quinn governs sizing).
    target_mtu: Arc<DashMap<SocketAddr, u16>>,
    /// The tunnel facts, shared with the [`RelayTunnelControl`] that owns the
    /// tunnel tasks. `poll_recv` reads the cause to tell "we dismantled this"
    /// from "this broke", and the poller reads the writer state to know whether
    /// waiting for send capacity is still worth anything.
    state: Arc<TunnelState>,
    /// The original socket is kept alive so the relay connection's own
    /// QUIC traffic (keepalives, ACKs, stream data) continues to flow
    /// directly.  Without this reference the OS may reclaim the socket.
    _original_socket: Arc<dyn AsyncUdpSocket>,
}

impl fmt::Debug for MasqueRelaySocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MasqueRelaySocket")
            .field("relay_public_addr", &self.relay_public_addr)
            .field("send_capacity", &self.send_tx.capacity())
            .finish()
    }
}

impl MasqueRelaySocket {
    /// Create a new tunnel-only relay socket.
    ///
    /// All I/O flows through the relay QUIC stream.  `original_socket`
    /// is held alive (but not used for I/O) to prevent the OS from
    /// reclaiming the underlying file descriptor while the relay
    /// connection's own QUIC traffic still needs it.
    ///
    /// Spawns three background tasks:
    /// - A reader that decodes length-prefixed frames from
    ///   `recv_stream` and pushes `(Bytes, SocketAddr)` to the bounded
    ///   recv channel for [`poll_recv`] to drain.
    /// - A writer that drains the send channel and writes
    ///   length-prefixed frames to `send_stream`.
    /// - A keepalive ticker that injects zero-length frames so the
    ///   NAT conntrack entry stays alive on idle connections.
    ///
    /// Returns the socket alongside a [`RelayTunnelControl`] that reports
    /// tunnel failure and provides explicit teardown. Callers that own the
    /// backing endpoint should use it to trigger a **graceful** close
    /// — `Endpoint::close(code, reason)` sends CONNECTION_CLOSE frames
    /// to every connection before the endpoint driver future is dropped.
    /// Without this, the driver's `Drop` impl fires last and cascades
    /// a cryptic `"endpoint driver future was dropped"` into every
    /// connection accepted through this tunnel.
    pub(crate) fn new(
        mut send_stream: crate::high_level::SendStream,
        mut recv_stream: crate::high_level::RecvStream,
        relay_public_addr: SocketAddr,
        _relay_server_addr: SocketAddr,
        original_socket: Arc<dyn AsyncUdpSocket>,
    ) -> (Arc<Self>, Arc<RelayTunnelControl>) {
        let (send_tx, mut send_rx) = mpsc::channel::<Bytes>(SEND_QUEUE_CAPACITY);
        let (recv_tx, recv_rx) = mpsc::channel::<(Bytes, SocketAddr)>(RECV_QUEUE_CAPACITY);
        let state = TunnelState::new();
        let control = RelayTunnelControl::new(Arc::clone(&state));

        let target_mtu: Arc<DashMap<SocketAddr, u16>> = Arc::new(DashMap::new());
        let target_mtu_reader = Arc::clone(&target_mtu);

        let socket = Arc::new(Self {
            relay_public_addr,
            recv_rx: PlMutex::new(recv_rx),
            send_tx: send_tx.clone(),
            state: Arc::clone(&state),
            target_mtu,
            _original_socket: original_socket,
        });

        // Background task: read length-prefixed frames from relay stream
        // and forward decoded (payload, source) pairs to `poll_recv`.
        // Holds the payload as `Bytes` throughout — no Vec round-trip.
        let weak_control: Weak<RelayTunnelControl> = Arc::downgrade(&control);
        let reader_handle = tokio::spawn(async move {
            loop {
                let mut len_buf = [0u8; 4];
                if let Err(e) = recv_stream.read_exact(&mut len_buf).await {
                    tracing::debug!(error = %e, "MasqueRelaySocket: stream read error (length)");
                    break;
                }
                let frame_len = u32::from_be_bytes(len_buf);
                // V2-834 C.4: exact stream bytes read, prefix included.
                RELAY_CLIENT_TRAFFIC
                    .stream_rx_bytes
                    .fetch_add(len_buf.len() as u64, Ordering::Relaxed);

                // Zero-length frame = keepalive ping from the relay
                // server, skip without trying to decode a datagram.
                if frame_len == 0 {
                    RELAY_CLIENT_TRAFFIC
                        .keepalive_rx_count
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // Sentinel marker for an out-of-band control frame.
                // Wire layout:
                //   [4-byte BE CONTROL_FRAME_MARKER]
                //   [4-byte BE body_len]
                //   [body_len bytes body]
                if frame_len == CONTROL_FRAME_MARKER {
                    let mut body_len_buf = [0u8; 4];
                    if let Err(e) = recv_stream.read_exact(&mut body_len_buf).await {
                        tracing::debug!(error = %e, "MasqueRelaySocket: control frame read error (body_len)");
                        break;
                    }
                    let body_len = u32::from_be_bytes(body_len_buf);
                    if body_len > MAX_CONTROL_FRAME_BODY {
                        tracing::warn!(
                            body_len,
                            cap = MAX_CONTROL_FRAME_BODY,
                            "MasqueRelaySocket: control frame body too large, closing"
                        );
                        break;
                    }
                    let mut body = vec![0u8; body_len as usize];
                    if let Err(e) = recv_stream.read_exact(&mut body).await {
                        tracing::debug!(error = %e, "MasqueRelaySocket: control frame read error (body)");
                        break;
                    }
                    // Marker was already counted as the length prefix.
                    let control_len = (body_len_buf.len() + body.len()) as u64;
                    RELAY_CLIENT_TRAFFIC
                        .stream_rx_bytes
                        .fetch_add(control_len, Ordering::Relaxed);
                    RELAY_CLIENT_TRAFFIC
                        .control_rx_bytes
                        .fetch_add(len_buf.len() as u64 + control_len, Ordering::Relaxed);
                    RELAY_CLIENT_TRAFFIC
                        .control_rx_count
                        .fetch_add(1, Ordering::Relaxed);
                    match TunnelControlFrame::decode_body(&body) {
                        Some(TunnelControlFrame::PmtuUpdate { target, mtu }) => {
                            tracing::debug!(
                                relay = %relay_public_addr,
                                target = %target,
                                mtu,
                                "RELAY_TUNNEL[clt]: PmtuUpdate received → clamping per-target MTU"
                            );
                            target_mtu_reader.insert(target, mtu);
                        }
                        None => {
                            tracing::debug!(
                                relay = %relay_public_addr,
                                body_len,
                                "RELAY_TUNNEL[clt]: unknown / malformed control frame, ignoring"
                            );
                        }
                    }
                    continue;
                }

                let frame_len = frame_len as usize;
                if frame_len > MAX_RELAY_FRAME {
                    tracing::warn!(frame_len, "MasqueRelaySocket: corrupt frame length");
                    break;
                }

                let mut frame_buf = vec![0u8; frame_len];
                if let Err(e) = recv_stream.read_exact(&mut frame_buf).await {
                    tracing::debug!(error = %e, "MasqueRelaySocket: stream read error (data)");
                    break;
                }
                RELAY_CLIENT_TRAFFIC
                    .stream_rx_bytes
                    .fetch_add(frame_len as u64, Ordering::Relaxed);

                let mut cursor = Bytes::from(frame_buf);
                match UncompressedDatagram::decode(&mut cursor) {
                    Ok(datagram) => {
                        // `datagram.payload` is a zero-copy slice of
                        // the original frame buffer — no clone needed.
                        if recv_tx
                            .send((datagram.payload, datagram.target))
                            .await
                            .is_err()
                        {
                            // Receiver dropped — socket is gone.
                            break;
                        }
                    }
                    Err(_) => {
                        tracing::trace!("MasqueRelaySocket: failed to decode frame");
                    }
                }
            }
            // Dropping `recv_tx` here wakes any pending `poll_recv`
            // with Poll::Ready(None), signalling end-of-stream.
            //
            // Signal the owner before the endpoint driver is dropped so it
            // can gracefully close connections accepted through the tunnel.
            if let Some(control) = weak_control.upgrade() {
                control.mark_failed();
            }
        });
        control.register(reader_handle);

        // Background task: write queued outbound packets to relay stream.
        let writer_capacity = Arc::clone(&state);
        // Created before the spawn so an abort that lands before the future's
        // first poll still records the exit.
        let writer_exit = WriterExit(Arc::clone(&state));
        let writer_handle = tokio::spawn(async move {
            let _writer_exit = writer_exit;
            while let Some(encoded) = send_rx.recv().await {
                // `recv` completing means the channel just freed a
                // slot.  Wake any poller parked on full-queue
                // backpressure before proceeding with the (potentially
                // slow) stream write — so the Quinn endpoint can start
                // assembling the next packet concurrently with this
                // frame going out on the wire.
                let mut batch =
                    Vec::with_capacity(encoded.len().saturating_add(std::mem::size_of::<u32>()));
                append_relay_frame(&mut batch, &encoded);
                writer_capacity.send_capacity_freed.notify_one();
                // Zero-length frames are keepalives (V2-834 C.4).
                let mut keepalives: u64 = u64::from(encoded.is_empty());

                let mut frames = 1usize;
                while frames < RELAY_STREAM_BATCH_MAX_FRAMES
                    && batch.len() < RELAY_STREAM_BATCH_MAX_BYTES
                {
                    match send_rx.try_recv() {
                        Ok(next) => {
                            keepalives += u64::from(next.is_empty());
                            append_relay_frame(&mut batch, &next);
                            writer_capacity.send_capacity_freed.notify_one();
                            frames += 1;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }

                if let Err(e) = send_stream.write_all(&batch).await {
                    tracing::debug!(error = %e, frames, bytes = batch.len(), "MasqueRelaySocket: stream batch write error");
                    break;
                }
                // V2-834 C.4: exact stream bytes written to the carrier.
                RELAY_CLIENT_TRAFFIC
                    .stream_tx_bytes
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
                if keepalives > 0 {
                    RELAY_CLIENT_TRAFFIC
                        .keepalive_tx_count
                        .fetch_add(keepalives, Ordering::Relaxed);
                }
            }
            // Writer exited (stream error or receiver dropped). Dropping
            // `send_rx` closes the channel so subsequent `try_send` calls fail
            // fast with `Closed` instead of filling a queue nobody will drain.
            // `_writer_exit` then records the exit and wakes parked pollers.
            drop(send_rx);
        });
        control.register(writer_handle);

        // Background task: periodic keepalive pings.
        // Sends a zero-length frame through the writer channel to keep
        // the NAT conntrack entry alive for the underlying QUIC
        // connection.  The writer encodes it as a 4-byte `[0,0,0,0]`
        // length prefix with no payload; the relay server skips it.
        let keepalive_tx = send_tx;
        let keepalive_handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(RELAY_KEEPALIVE_INTERVAL);
            tick.tick().await; // skip immediate first tick
            loop {
                tick.tick().await;
                // Use `send` (not `try_send`) — a momentarily-full queue
                // must not cause us to lose liveness of the keepalive.
                if keepalive_tx.send(Bytes::new()).await.is_err() {
                    break; // channel closed — relay dead
                }
            }
        });
        control.register(keepalive_handle);

        (socket, control)
    }

    /// Whether a [`TunnelPoller`] should stop waiting for send capacity.
    ///
    /// The writer state is checked alongside the channel state because shutdown
    /// publishes it *before* aborting the writer, whereas the channel's closure
    /// trails the abort and arrives with no notification of its own. A poller
    /// that consulted only the channel could wake to one still full and open,
    /// re-park, and never be woken again.
    ///
    /// Every `true` here must be matched by a non-`WouldBlock` result from
    /// [`enqueue_outbound`](Self::enqueue_outbound): Quinn retries a `WouldBlock`
    /// immediately and without yielding, so claiming writability and then
    /// refusing the datagram spins the connection driver instead of parking it.
    fn writable_or_finished(&self) -> bool {
        self.send_tx.capacity() > 0 || self.send_tx.is_closed() || self.state.writer_stopped()
    }

    /// Whether the outbound send channel is still open. Lets the teardown test
    /// assert that a poller was released by the writer state rather than by the
    /// channel closing.
    #[cfg(test)]
    pub(crate) fn send_channel_open(&self) -> bool {
        !self.send_tx.is_closed()
    }

    /// Remaining capacity in the outbound send channel.  Exposed for
    /// tests and metrics — a sustained value of 0 means the tunnel
    /// stream can't keep up with Quinn's offered load and the poller
    /// is serialising sends.
    pub fn send_capacity(&self) -> usize {
        self.send_tx.capacity()
    }

    /// Internal helper: enqueue an already-encoded outbound frame.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] when the send channel is
    /// full so the Quinn UDP driver re-polls
    /// [`UdpPoller::poll_writable`] instead of dropping the packet.
    /// This preserves the reliable-stream invariant of the MASQUE
    /// tunnel — packets never silently disappear — at the cost of
    /// pausing the inner Quinn endpoint until the stream writer
    /// drains a slot.
    fn enqueue_outbound(&self, encoded: Bytes) -> io::Result<()> {
        match self.send_tx.try_send(encoded) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(encoded)) => {
                if self.state.writer_stopped() {
                    // This queue will never drain, and `writable_or_finished`
                    // has already told Quinn the socket is writable, so
                    // `WouldBlock` here would put it into an immediate,
                    // unyielding retry. Drop the datagram instead — which is
                    // what an undeliverable packet is.
                    RELAY_CLIENT_TRAFFIC
                        .dropped_writer_stopped_bytes
                        .fetch_add(encoded.len() as u64, Ordering::Relaxed);
                    RELAY_CLIENT_TRAFFIC
                        .dropped_writer_stopped_count
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "relay send queue full",
                ))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "relay stream closed",
            )),
        }
    }
}

impl AsyncUdpSocket for MasqueRelaySocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(TunnelPoller {
            socket: self,
            wait: None,
        })
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        // When Quinn uses GSO (Generic Segmentation Offload),
        // transmit.contents contains multiple concatenated QUIC packets
        // of `segment_size` bytes.  Each segment must be sent as its
        // own tunnel frame — the relay server has a per-frame size
        // limit and cannot handle the entire batch as one.
        // Per-target MTU enforcement: if a previous PmtuUpdate control
        // frame told us the egress path to this destination caps at
        // `mtu` bytes, drop oversized packets here so they never reach
        // the relay-server's fragmentation-rejecting socket.  Returning
        // `Ok(())` makes Quinn treat the packet as successfully sent;
        // its loss-detection then observes the missing ACK and lowers
        // the connection's MTU estimate via DPLPMTUD's normal path.
        // We do NOT return an Err here because that would skip Quinn's
        // PMTUD machinery entirely and leave the size unchanged.
        if let Some(cap) = self.target_mtu.get(&transmit.destination) {
            let segment = transmit.segment_size.unwrap_or(transmit.contents.len());
            if segment > usize::from(*cap) {
                tracing::debug!(
                    relay = %self.relay_public_addr,
                    destination = %transmit.destination,
                    segment,
                    cap = *cap,
                    "RELAY_TUNNEL[clt]: try_send dropping oversized packet (per-target MTU exceeded)"
                );
                // V2-834 C.2: quinn counts this as sent; nothing reaches
                // the wire. Itemise so `udp_tx` can be corrected.
                RELAY_CLIENT_TRAFFIC
                    .dropped_oversized_bytes
                    .fetch_add(transmit.contents.len() as u64, Ordering::Relaxed);
                RELAY_CLIENT_TRAFFIC
                    .dropped_oversized_count
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }

        if let Some(segment_size) = transmit.segment_size {
            for chunk in transmit.contents.chunks(segment_size) {
                let datagram = UncompressedDatagram::new(
                    VarInt::from_u32(0),
                    transmit.destination,
                    Bytes::copy_from_slice(chunk),
                );
                self.enqueue_outbound(datagram.encode())?;
            }
            return Ok(());
        }

        let datagram = UncompressedDatagram::new(
            VarInt::from_u32(0),
            transmit.destination,
            Bytes::copy_from_slice(transmit.contents),
        );
        self.enqueue_outbound(datagram.encode())
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let capacity = bufs.len().min(meta.len());
        let mut filled = 0;

        // Single lock acquisition per Quinn poll — unlike the previous
        // design there is no separate waker mutex, and no risk of
        // losing a wakeup: `Receiver::poll_recv` registers `cx.waker()`
        // itself when it returns `Pending`.
        let mut rx = self.recv_rx.lock();
        while filled < capacity {
            match rx.poll_recv(cx) {
                Poll::Ready(Some((payload, source))) => {
                    if payload.len() > bufs[filled].len() {
                        tracing::warn!(
                            payload_len = payload.len(),
                            buf_len = bufs[filled].len(),
                            "MasqueRelaySocket: payload exceeds receive buffer; dropping packet"
                        );
                        RELAY_CLIENT_TRAFFIC
                            .dropped_recv_oversized_count
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let len = payload.len();
                    // Single copy — Bytes → Quinn-owned slice.
                    bufs[filled][..len].copy_from_slice(&payload);

                    let mut recv_meta = RecvMeta::default();
                    recv_meta.len = len;
                    recv_meta.stride = len;
                    recv_meta.addr = source;
                    recv_meta.ecn = None;
                    recv_meta.dst_ip = None;
                    meta[filled] = recv_meta;

                    filled += 1;
                }
                Poll::Ready(None) => {
                    // Channel closed — reader task exited.  Surface as
                    // end-of-stream only if we haven't collected any
                    // packets in this poll; otherwise deliver what we
                    // have and let the next poll see the closed state.
                    if filled == 0 {
                        if self.state.shutdown_requested() {
                            // We dismantled this tunnel ourselves, so nothing
                            // failed. An I/O error here would end the endpoint
                            // driver through its failure path: logged at ERROR,
                            // and its `Drop` clears the connection senders
                            // instead of letting the endpoint retire once its
                            // last handle goes away.
                            //
                            // Park instead. Nothing can arrive on a torn-down
                            // tunnel, and the driver keeps its other wakers —
                            // the endpoint-event channel and the explicit wake
                            // `EndpointRef::drop` issues at refcount zero — so
                            // it still retires with `Ok(())`.
                            return Poll::Pending;
                        }
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "relay recv stream closed",
                        )));
                    }
                    break;
                }
                Poll::Pending => {
                    break;
                }
            }
        }

        if filled > 0 {
            Poll::Ready(Ok(filled))
        } else {
            Poll::Pending
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.relay_public_addr)
    }

    fn may_fragment(&self) -> bool {
        false
    }

    fn is_virtual(&self) -> bool {
        true
    }
}

/// Poller for the tunnel socket.
///
/// Backpressure model: when the outbound send channel is full,
/// [`poll_writable`](UdpPoller::poll_writable) parks on the socket's
/// `send_capacity_freed` [`Notify`] and wakes when the stream writer drains
/// a slot.  Each wake re-checks remaining capacity because multiple
/// pollers may race against the same notification and because the
/// keepalive task can refill the slot before we observe it.
///
/// The inner `wait` future captures an `Arc<MasqueRelaySocket>` so it
/// owns its own keep-alive reference to the `Notify`; the boxed future
/// is kept alive across polls (following the same pattern as
/// `UdpPollHelper` in `high_level::runtime`) so the registered waker
/// is not lost between calls.
struct TunnelPoller {
    socket: Arc<MasqueRelaySocket>,
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send + Sync>>>,
}

impl fmt::Debug for TunnelPoller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunnelPoller")
            .field("socket", &self.socket)
            .field("waiting", &self.wait.is_some())
            .finish()
    }
}

impl UdpPoller for TunnelPoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        // `TunnelPoller` is `Unpin` (all fields are `Unpin`), so we can
        // freely take `&mut self` out of the `Pin`.
        let this = self.get_mut();

        // Fast path: capacity is available right now, the channel is closed
        // (writer task exited — return Ready so Quinn attempts a `try_send`,
        // which surfaces the failure as `ConnectionAborted`), or the tunnel has
        // ended and no drain is ever coming.
        if this.socket.writable_or_finished() {
            this.wait = None;
            return Poll::Ready(Ok(()));
        }

        // Slow path: park on the `send_capacity_freed` notify and re-check
        // capacity after each wake.  The future is created once and
        // kept alive until it resolves, so the waker registered via
        // `Notified::enable()` survives across polls — discarding the
        // future after each poll would deregister the waker and lead
        // to lost wakeups.
        let fut = this.wait.get_or_insert_with(|| {
            let socket = Arc::clone(&this.socket);
            Box::pin(async move {
                loop {
                    // Register interest BEFORE re-checking capacity.
                    // If the writer task calls `notify_one` between our
                    // last check and `enable`, `enable` stashes the
                    // permit and the subsequent `.await` returns
                    // immediately.
                    let notified = socket.state.send_capacity_freed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();

                    if socket.writable_or_finished() {
                        return;
                    }
                    notified.await;
                    if socket.writable_or_finished() {
                        return;
                    }
                    // Spurious wake (e.g., another poller consumed the
                    // freed slot before we saw it).  Loop and wait for
                    // the next drain.
                }
            })
        });

        match fut.as_mut().poll(cx) {
            Poll::Ready(()) => {
                this.wait = None;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod relay_tunnel_control_tests {
    use super::{RelayTunnelControl, TunnelState, WriterExit};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DropMarker(Arc<AtomicBool>);

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn shutdown_aborts_registered_tasks_and_wakes_waiters() {
        let control = RelayTunnelControl::detached();
        let dropped = Arc::new(AtomicBool::new(false));
        let marker = DropMarker(Arc::clone(&dropped));
        control.register(tokio::spawn(async move {
            let _marker = marker;
            std::future::pending::<()>().await;
        }));

        let waiter_control = Arc::clone(&control);
        let waiter = tokio::spawn(async move {
            waiter_control.closed().await;
        });

        control.shutdown().await;
        let waiter_result = waiter.await;

        assert!(waiter_result.is_ok());
        assert!(control.is_closed());
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn shutdown_is_idempotent() {
        let control = RelayTunnelControl::detached();

        control.shutdown().await;
        control.shutdown().await;

        assert!(control.is_closed());
    }

    #[tokio::test]
    async fn shutdown_now_aborts_registered_tasks_without_an_await() {
        let control = RelayTunnelControl::detached();
        let dropped = Arc::new(AtomicBool::new(false));
        let marker = DropMarker(Arc::clone(&dropped));
        control.register(tokio::spawn(async move {
            let _marker = marker;
            std::future::pending::<()>().await;
        }));

        tokio::task::yield_now().await;
        control.shutdown_now();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !dropped.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("aborted tunnel task should release its owned stream state");
        assert!(control.is_closed());
    }

    #[test]
    fn writer_exit_marks_the_tunnel_failed_not_shut_down() {
        let control = RelayTunnelControl::detached();

        drop(WriterExit(Arc::clone(&control.state)));

        assert!(control.is_closed(), "a writer exit ends the tunnel");
        assert!(
            !control.shutdown_requested(),
            "a tunnel that broke on its own was not dismantled by us"
        );
    }

    #[tokio::test]
    async fn shutdown_records_that_the_teardown_was_requested() {
        let control = RelayTunnelControl::detached();

        control.shutdown().await;

        assert!(control.is_closed());
        assert!(control.shutdown_requested());
    }

    #[tokio::test]
    async fn a_writer_exit_is_recorded_even_after_its_control_is_dropped() {
        // The dial-through path in `p2p_endpoint` discards its control as soon
        // as the dial completes, while the socket and the tunnel tasks live on.
        // A writer that could not record its exit there would leave a poller
        // parked on a full send queue waiting forever.
        let control = RelayTunnelControl::detached();
        let state: Arc<TunnelState> = Arc::clone(&control.state);
        let exit = WriterExit(Arc::clone(&state));

        let waiter = tokio::spawn({
            let state = Arc::clone(&state);
            async move { state.send_capacity_freed.notified().await }
        });
        tokio::task::yield_now().await;

        drop(control);
        drop(exit);

        assert!(state.writer_stopped(), "the writer's exit is recorded");
        assert!(state.is_closed(), "and it ends the tunnel");
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("a parked capacity waiter must be released by the writer's exit")
            .expect("waiter task");
    }

    #[tokio::test]
    async fn cleanup_after_a_failure_does_not_reclassify_it_as_requested() {
        // Cleanup routinely arrives after a tunnel has already broken — the
        // health monitor calls `shutdown` on tunnels it finds dead. That must
        // not rewrite the record, or it silences the fault that triggered it.
        let control = RelayTunnelControl::detached();

        control.mark_failed();
        control.shutdown().await;

        assert!(control.is_closed());
        assert!(
            !control.shutdown_requested(),
            "cleanup arriving after a failure must not silence the failure"
        );
    }
}

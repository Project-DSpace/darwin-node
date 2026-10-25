# Vendored from crates.io (webrtc 0.14.0, sha256 08fd686c...; MIT OR Apache-2.0, https://github.com/webrtc-rs/webrtc).
# The only change from upstream is in src/dtls_transport/mod.rs: RTCDtlsTransport::stop() cancels a
# DTLS handshake still in progress. Without it, a node waiting as DTLS server for a browser that left
# after ICE waited forever, and every such session kept its peer connection (about 60 KB) after closing.
# tests/webrtc_dtls_cleanup.rs covers it. Drop this copy once an upstream release cancels the handshake.
# Still needed with saorsa-transport 0.38.2, checked again for it: 0.38.2 ends a pending handshake by handing
# it a DTLS close_notify through ICE before the peer connection closes (its ADR-017), best effort. Its own ADR
# leaves cases that this copy still covers, since closing the peer connection stops the DTLS transport: a
# handshake that starts in the instant teardown stops the transport, a close_notify that can't be written or
# that the handshake outlives by a second, and every handshake still pending when the listener itself closes.
# tests/webrtc_dtls_cleanup.rs and the fork pins pass with both.

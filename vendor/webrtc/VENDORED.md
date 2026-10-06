# Vendored from crates.io (webrtc 0.14.0, sha256 08fd686c...; MIT OR Apache-2.0, https://github.com/webrtc-rs/webrtc).
# The only change from upstream is in src/dtls_transport/mod.rs: RTCDtlsTransport::stop() cancels a
# DTLS handshake still in progress. Without it, a node waiting as DTLS server for a browser that left
# after ICE waited forever, and every such session kept its peer connection (about 60 KB) after closing.
# tests/webrtc_dtls_cleanup.rs covers it. Drop this copy once an upstream release cancels the handshake.

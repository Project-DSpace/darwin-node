# saorsa-transport

QUIC transport for peer-to-peer networks. NAT traversal is done natively inside QUIC
([draft-seemann-quic-nat-traversal-02](docs/rfcs/draft-seemann-quic-nat-traversal-02.txt))
together with address discovery
([draft-ietf-quic-address-discovery-00](docs/rfcs/draft-ietf-quic-address-discovery-00.txt)),
with a MASQUE CONNECT-UDP relay as the fallback when hole punching fails. Connections use
post-quantum cryptography: ML-KEM-768 key exchange (currently negotiated as the
X25519MLKEM768 hybrid) and ML-DSA-65 authentication with raw public keys (no X.509, no CAs).
Every node is a symmetric peer: there are no client or server roles, and any node can connect,
accept, and help other peers with traversal.

[![Documentation](https://docs.rs/saorsa-transport/badge.svg)](https://docs.rs/saorsa-transport/)
[![Crates.io](https://img.shields.io/crates/v/saorsa-transport.svg)](https://crates.io/crates/saorsa-transport)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE-MIT)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE-APACHE)
[![CI Status](https://github.com/WithAutonomi/saorsa-transport/actions/workflows/ci.yml/badge.svg)](https://github.com/WithAutonomi/saorsa-transport/actions/workflows/ci.yml)
[![Security Audit](https://github.com/WithAutonomi/saorsa-transport/actions/workflows/security.yml/badge.svg)](https://github.com/WithAutonomi/saorsa-transport/actions/workflows/security.yml)

## Quick Start

```sh
cargo add saorsa-transport
```

`Node` is the zero-configuration entry point. `P2pEndpoint` with `P2pConfig::builder()` gives
full control.

```rust,no_run
use saorsa_transport::Node;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Bind a random port and remember one known peer (any node in the network will do).
    let node = Node::with_peers(vec!["203.0.113.10:9000".parse()?]).await?;
    let connected = node.connect_known_peers().await?;
    println!("connected to {connected} known peer(s)");

    // Learned from what connected peers observe about us.
    if let Some(addr) = node.external_addr() {
        println!("external address: {addr}");
    }

    let peer = "203.0.113.10:9000".parse()?;
    node.send(&peer, b"hello").await?;
    let (from, data) = node.recv().await?;
    println!("{} bytes from {from}", data.len());
    Ok(())
}
```

Peer identity is a 32-byte value: the BLAKE3 hash of `b"AUTONOMI_PEER_ID_V2:"` followed by the
peer's ML-DSA-65 public key (`fingerprint_public_key`). Keys are generated with
`generate_ml_dsa_keypair()`.

## Feature flags

Defaults are `native`, `platform-verifier`, `network-discovery` and `upnp`.

| Feature | Meaning |
|---------|---------|
| `native` | Native QUIC, sockets, OS integration and the background runtime. Without it only the portable address and capability types remain. |
| `platform-verifier` | Platform-specific certificate verification. |
| `network-discovery` | Enhanced network interface discovery. |
| `upnp` | Best-effort UPnP IGD port mapping. Failure is silent and non-fatal. |
| `ble` | Bluetooth Low Energy transport (Linux via BlueZ, macOS via Core Bluetooth). |
| `webrtc` | Shared browser/native wire profile and post-quantum application session. Does not enable the native QUIC or WebRTC socket runtimes. |
| `webrtc-direct` | Signaling-free WebRTC DataChannel listener for direct browser clients (implies `native` and `webrtc`). |
| `log` | Route `tracing` events to `log` when no `tracing` subscriber exists. |
| `trace`, `trace-full` | Zero-cost tracing system (`trace-full` implies `trace`). |
| `arbitrary`, `fuzzing`, `property_testing` | Fuzzing and property-test support. |

### WebRTC and WASM

`default-features = false` exposes the shared transport addresses and capability model on
`wasm32-unknown-unknown`. The `webrtc` feature adds `saorsa_transport::webrtc` (the shared
session and frame contract); `webrtc-direct` adds the native listener in `webrtc::direct`.
Its `chunk_protocol` frame allows 5 MiB for serialized requests/proofs, while ordinary record
frames keep a 4 MiB limit. CI checks:

```sh
cargo check --lib --no-default-features --target wasm32-unknown-unknown
cargo check --lib --no-default-features --features webrtc --target wasm32-unknown-unknown
cargo test --lib --no-default-features --features webrtc webrtc::
```

## Documentation

- [API reference and module map on docs.rs](https://docs.rs/saorsa-transport)
- [Protocol extensions](docs/architecture/PROTOCOL_EXTENSIONS.md): wire reference for the QUIC extension frames and transport parameters
- [Architecture decision records](docs/adr/): why each part is built the way it is
- [Specification copies](docs/rfcs/): the drafts and standards this crate follows
- [PQC authentication specification](docs/rfcs/saorsa-transport-pqc-authentication.md) (being revised)
- [Changelog](CHANGELOG.md)
- [Contributing](.github/CONTRIBUTING.md)
- [Security policy](SECURITY.md)

## Examples

Run with `cargo run --example <name>`:

| Example | Shows |
|---------|-------|
| `simple_p2p` | Basic symmetric peer API |
| `simple_chat` | Chat message serialization and handling |
| `simple_transfer` | Data transfer with throughput measurement |
| `throughput_test` | Throughput and efficiency measurement |
| `port_configuration` | Port binding strategies |
| `pqc_config_demo` | Post-quantum configuration |
| `pqc_verification` | Post-quantum verification |
| `ml_kem_usage` | ML-KEM-768 via saorsa-pqc |
| `test_network_discovery_simple` | Network interface discovery |
| `trace_demo` | The tracing system |
| `verify_quic_endpoints` | QUIC endpoint checks |
| `ble_chat` | Chat over BLE; needs `--features ble` |

## Platforms

CI builds and tests on Linux, macOS and Windows (including Linux aarch64 cross builds and
macOS on both x86_64 and aarch64), and checks the library for `wasm32-unknown-unknown` with
`--no-default-features` and with `--features webrtc`. The minimum supported Rust version is
the `rust-version` in [Cargo.toml](Cargo.toml).

## Development

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

The full set of checks CI runs is listed in [AGENTS.md](AGENTS.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT), at your option.

The QUIC core began as a fork of [Quinn](https://github.com/quinn-rs/quinn). Post-quantum primitives come from
[saorsa-pqc](https://crates.io/crates/saorsa-pqc). Developed for the [Autonomi](https://autonomi.com) network.

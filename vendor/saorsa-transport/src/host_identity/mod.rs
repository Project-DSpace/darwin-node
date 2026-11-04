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

//! Local-only HostKey for key hierarchy and local state encryption
//!
//! This module provides a host-scoped identity system where:
//! - A single HostKey (root secret) exists only on the local machine
//! - The HostKey is NEVER transmitted on the wire
//! - All endpoint keys and local state encryption keys are derived from the HostKey
//!
//! ## Architecture (ADR-007)
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                        LOCAL MACHINE ONLY                           │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │  HostKey (32 bytes)                                                 │
//! │    │                                                                │
//! │    ├── derive_endpoint_encryption_key(network_id)                   │
//! │    │     └── Used to encrypt/decrypt per-network ML-DSA-65 keypair │
//! │    │                                                                │
//! │    └── derive_cache_key()                                           │
//! │          └── Used to encrypt local cache state at rest              │
//! └─────────────────────────────────────────────────────────────────────┘
//!
//!                              │ (encrypted storage)
//!                              ▼
//!
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                      NETWORK-VISIBLE                                │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │  EndpointId (per-network)                                           │
//! │    └── Derived from ML-DSA-65 public key                           │
//! │                                                                     │
//! │  PeerId (32 bytes)                                                  │
//! │    └── SHA-256 hash of ML-DSA-65 public key                        │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Decisions
//!
//! 1. **Privacy by Default**: Per-network endpoint keys prevent cross-overlay correlation
//! 2. **No Sybil Resistance**: HostKey is local-only; Sybil resistance belongs at overlay layer
//! 3. **Encrypted Storage**: Endpoint keypairs and local state encrypted at rest
//! 4. **Platform Storage**: Uses OS keychain when available, encrypted file fallback
//!
//! ## Usage
//!
//! ```ignore
//! use saorsa_transport::host_identity::{HostIdentity, EndpointKeyPolicy};
//!
//! // Generate a new host identity (or load from storage)
//! let host = HostIdentity::generate();
//!
//! // Derive encryption key for a network's endpoint keypair
//! let encryption_key = host.derive_endpoint_encryption_key(b"my-network");
//!
//! // Derive a local cache encryption key
//! let cache_key = host.derive_cache_key();
//!
//! // Display-safe fingerprint (not the actual secret)
//! println!("Host fingerprint: {}", host.fingerprint());
//! ```

pub mod derivation;
pub mod storage;

pub use derivation::{EndpointKeyPolicy, HOSTKEY_VERSION, HostIdentity};
pub use storage::{
    HostKeyStorage, KeyringStorage, PlainFileStorage, StorageError, StorageResult,
    StorageSecurityLevel, StorageSelection, auto_storage,
};

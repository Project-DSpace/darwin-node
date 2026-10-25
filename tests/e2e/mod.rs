//! E2E test infrastructure for ant-node.
//!
//! This module provides a complete testing framework for running E2E tests
//! against a local testnet of 25 ant nodes with optional EVM payment
//! verification via a local Anvil testnet.
//!
//! ## Architecture
//!
//! ```text
//! TestHarness
//!     ├── TestNetwork (25 nodes)
//!     │       ├── Nodes 0-2: Bootstrap
//!     │       └── Nodes 3-24: Regular
//!     ├── TestAnvil (EVM testnet)
//!     └── PaymentHelpers
//! ```
//!
//! ## Usage
//!
//! ```rust,no_run
//! use ant_node::tests::e2e::TestHarness;
//!
//! #[tokio::test]
//! async fn test_chunk_storage() {
//!     let harness = TestHarness::setup().await.unwrap();
//!
//!     // Store data via node 5
//!     let data = b"test data";
//!     let address = harness.node(5).unwrap().store(data).await.unwrap();
//!
//!     // Retrieve from node 20
//!     let retrieved = harness.node(20).unwrap().retrieve(&address).await.unwrap();
//!     assert_eq!(data, retrieved);
//!
//!     harness.teardown().await.unwrap();
//! }
//! ```

mod anvil;
mod data_types;
mod harness;
#[allow(clippy::missing_errors_doc, clippy::match_same_arms)]
mod testnet;

#[cfg(test)]
mod integration_tests;

// Listings across real nodes: a paid envelope served by its close group.
#[cfg(test)]
mod listings;

#[cfg(test)]
mod merkle_payment;

#[cfg(test)]
mod replication;

// Pointer replication across real nodes (ADR-0016).
mod pointer_replication;

#[cfg(test)]
mod security_attacks;

#[cfg(test)]
mod subtree_audit_testnet;

#[cfg(test)]
mod fetch_local_write_guard;

#[cfg(test)]
mod fetch_responsibility_recheck;

#[cfg(test)]
mod fresh_offer_capacity;

#[cfg(test)]
mod verify_storage_gate;

#[cfg(test)]
mod settlement_version_gate;

pub use anvil::TestAnvil;
pub use harness::TestHarness;
pub use testnet::{NetworkState, NodeState, TestNetwork, TestNetworkConfig, TestNode};

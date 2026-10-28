//! Storage subsystem for chunk persistence.
//!
//! This module provides content-addressed storage for chunks, one immutable file per
//! chunk, along with a protocol handler that integrates with saorsa-core's `Protocol`
//! trait for automatic message routing.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │        AntProtocol (implements Protocol trait)        │
//! ├─────────────────────────────────────────────────────────┤
//! │  protocol_id() = "autonomi.ant.chunk.v1"                  │
//! │                                                         │
//! │  handle(peer_id, data) ──▶ decode AntProtocolMessage │
//! │                                   │                     │
//! │         ┌─────────────────────────┼─────────────────┐  │
//! │         ▼                         ▼                 ▼  │
//! │   QuoteRequest           ChunkPutRequest    ChunkGetRequest
//! │         │                         │                 │  │
//! │         ▼                         ▼                 ▼  │
//! │   QuoteGenerator          PaymentVerifier    ChunkStore│
//! │         │                         │                 │  │
//! │         └─────────────────────────┴─────────────────┘  │
//! │                           │                             │
//! │                 return Ok(Some(response_bytes))         │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use ant_node::storage::{AntProtocol, ChunkStore, ChunkStoreConfig};
//!
//! // Create storage
//! let config = ChunkStoreConfig::default();
//! let storage = Arc::new(ChunkStore::new(config).await?);
//!
//! // Create protocol handler
//! let protocol = AntProtocol::new(storage, Arc::new(payment_verifier), Arc::new(quote_generator));
//!
//! // Register with saorsa-core
//! listener.register_protocol(protocol).await?;
//! ```

// `test-utils` makes this module public so integration tests and downstream harnesses can
// reach the store directly, and that is not a narrow door: with the feature on, every `pub`
// item inside it — `StoreLayout`, `CapacityVerdict`, every inherent method on `ChunkStore` —
// is importable from outside the crate. The re-export list below is the boundary for the
// DEFAULT build only. Anything relying on the wider surface is relying on a test feature, and
// this comment is here so that is a decision rather than a discovery.
#[cfg(any(test, feature = "test-utils"))]
pub mod chunk_store;
#[cfg(not(any(test, feature = "test-utils")))]
pub(crate) mod chunk_store;
mod handler;
// This crate's own business: the cleanup is called once, from the node builder.
pub(crate) mod legacy_artifacts;
pub(crate) mod traffic;

pub use crate::ant_protocol::XorName;
pub(crate) use chunk_store::FULL_LINE_READING_MAX_AGE;
pub use chunk_store::{
    full_line_reading, register_full_line, ChunkStore, ChunkStoreConfig, FullState, FullTrigger,
    SpaceReading,
};
pub(crate) use chunk_store::{rename_with_retry, Reservation};
// Crate-private, as it was before the two stores became one: `CapacityVerdict` was
// `pub(crate)` on the old store and has no caller outside this crate.
pub(crate) use chunk_store::CapacityVerdict;
/// The handler's test protocol, for the browser listener's quote tests.
#[cfg(test)]
pub(crate) use handler::tests as handler_tests;
pub use handler::AntProtocol;
pub(crate) use handler::ChunkRequestContext;
pub use handler::SELF_CLOSENESS_GATE_WIDTH;

/// Bytes in one MiB.
pub const MIB: u64 = 1024 * 1024;

/// Bytes in one GiB.
pub const GIB: u64 = 1024 * MIB;

/// The full line: the free space a node keeps unused on its storage
/// partition unless its configuration sets another (`disk_reserve_mb`).
///
/// At or below it, the node stores no new chunk or pointer, and its log files
/// give way (`crate::log_files`). It is the warning level of the node manager
/// that images before darwin-nodes ran, which deleted a node's data at
/// 500 MiB free; nodes up to ds.15 kept 500 MiB.
pub const DEFAULT_DISK_RESERVE: u64 = GIB;

/// The free space at which the node manager of images before darwin-nodes
/// deleted a node's data.
///
/// A node's own writes keep clear of it: its upgrades leave room above it (`crate::upgrade::UPGRADE_SPACE_MARGIN`).
pub const OLD_MANAGER_LINE: u64 = 500 * MIB;

/// Free bytes on the partition holding `path`, as the node's disk checks read
/// them: the chunk store's capacity check and the listing store's map size.
/// In a `test-utils` build a test may set them instead
/// ([`crate::test_seams::FREE_SPACE_FILE`]).
pub(crate) fn available_space(path: &std::path::Path) -> std::io::Result<u64> {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(space) = crate::test_seams::free_space() {
        return space.map(|space| space.free);
    }
    fs2::available_space(path)
}

/// Free and total inodes of a partition whose filesystem counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Inodes {
    /// Inodes a write by the node may still take.
    pub free: u64,
    /// The partition's inodes.
    pub total: u64,
}

/// Free inodes below this share of the partition's inodes, in tenths of a
/// percent, count as being at the full line: the falling floor's last step
/// (2.5%), so the inode guard adds no value of its own.
pub const INODES_AT_THE_LINE_PER_MILLE: u64 = 25;

impl Inodes {
    /// Whether free inodes are below `per_mille` tenths of a percent of the
    /// partition's.
    #[must_use]
    pub fn below_per_mille(self, per_mille: u64) -> bool {
        u128::from(self.free) * 1000 < u128::from(self.total) * u128::from(per_mille)
    }

    /// Whether free inodes count as being at the full line.
    #[must_use]
    pub fn at_the_line(self) -> bool {
        self.below_per_mille(INODES_AT_THE_LINE_PER_MILLE)
    }
}

/// Free inodes on the partition holding `path`, or `None` where they can't
/// be counted: the filesystem reports no inodes (btrfs, some overlay, FUSE
/// and network filesystems report 0), the query failed, or the platform has
/// no `statvfs`. Bytes alone count then. In a `test-utils` build a test may
/// set them instead ([`crate::test_seams::FREE_SPACE_FILE`]).
pub(crate) fn free_inodes(path: &std::path::Path) -> Option<Inodes> {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(space) = crate::test_seams::free_space() {
        return space
            .ok()?
            .inodes
            .map(|(free, total)| Inodes { free, total })
            .filter(|inodes| inodes.total > 0);
    }
    statvfs_inodes(path)
}

#[cfg(unix)]
fn statvfs_inodes(path: &std::path::Path) -> Option<Inodes> {
    let stats = rustix::fs::statvfs(path).ok()?;
    let total = stats.f_files;
    (total > 0).then_some(Inodes {
        free: stats.f_favail.min(total),
        total,
    })
}

#[cfg(not(unix))]
fn statvfs_inodes(_path: &std::path::Path) -> Option<Inodes> {
    None
}

/// Statistics about storage operations.
///
/// Counters other than `current_chunks` are cumulative for the lifetime of the
/// process; `current_chunks` is the live count.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Total number of chunks stored.
    pub chunks_stored: u64,
    /// Total number of chunks retrieved.
    pub chunks_retrieved: u64,
    /// Total bytes stored.
    pub bytes_stored: u64,
    /// Total bytes retrieved.
    pub bytes_retrieved: u64,
    /// Number of duplicate writes (already exists).
    pub duplicates: u64,
    /// Number of verification failures on read.
    pub verification_failures: u64,
    /// Number of chunks currently persisted.
    pub current_chunks: u64,
}

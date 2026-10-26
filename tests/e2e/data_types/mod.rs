//! Data type E2E tests for ant-node.
//!
//! This module contains tests for the chunk data type:
//! - **Chunk**: Immutable, content-addressed data (up to 4MB)

mod chunk;

/// Test data generator for consistent test fixtures.
pub struct TestData;

impl TestData {
    /// Generate test data of specified size.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn generate(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 256) as u8).collect()
    }
}

/// Maximum chunk size (4MB).
pub const MAX_CHUNK_SIZE: usize = 4 * 1024 * 1024;

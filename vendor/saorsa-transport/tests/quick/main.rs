// This software is licensed under the MIT license <LICENSE-MIT or
// https://opensource.org/licenses/MIT> or the Apache License, Version 2.0
// <LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0>, at your
// option. This file may not be copied, modified, or distributed except
// according to those terms.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//! Quick tests - Execute in <30 seconds total
//!
//! This test suite contains fast unit and integration tests that provide
//! rapid feedback during development. These tests are run on every push.

#![allow(clippy::unwrap_used, clippy::expect_used)]

// Re-export test modules
// v0.2: auth_tests removed - TLS handles peer authentication via ML-DSA-65
mod auto_binding_integration;
mod binding_stream_tests;
mod connect_topologies;
mod connection_tests;
mod crypto_tests;
mod frame_tests;
mod pure_pq_rpk_tests;
mod token_binding_tests;
mod token_v2_server_side_tests;

// Quick test utilities
pub mod utils {
    use std::time::{Duration, Instant};

    /// Ensures a test completes within the specified duration
    pub fn assert_duration<F, R>(max_duration: Duration, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        let elapsed = start.elapsed();

        assert!(
            elapsed <= max_duration,
            "Test exceeded time limit: {elapsed:?} > {max_duration:?}"
        );

        result
    }

    /// Maximum duration for a quick test
    pub const QUICK_TEST_TIMEOUT: Duration = Duration::from_secs(5);
}

// This software is licensed under the MIT license <LICENSE-MIT or
// https://opensource.org/licenses/MIT> or the Apache License, Version 2.0
// <LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0>, at your
// option. This file may not be copied, modified, or distributed except
// according to those terms.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//! Standard test suite for saorsa-transport
//! These tests run in < 5 minutes and include integration and protocol tests

#![allow(clippy::unwrap_used, clippy::expect_used)]

pub mod utils {
    use std::time::Duration;

    pub const STANDARD_TEST_TIMEOUT: Duration = Duration::from_secs(30);

    // Add common test utilities here
    pub fn setup_test_logger() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("saorsa_transport=debug,warn")
            .try_init();
    }
}

// Test modules
pub mod integration_tests;
pub mod nat_basic_tests;
pub mod protocol_tests;

// Re-export test utilities
pub use utils::*;

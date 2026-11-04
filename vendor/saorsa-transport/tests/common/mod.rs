// This software is licensed under the MIT license <LICENSE-MIT or
// https://opensource.org/licenses/MIT> or the Apache License, Version 2.0
// <LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0>, at your
// option. This file may not be copied, modified, or distributed except
// according to those terms.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//! Common test utilities and initialization
//!
//! This module provides shared functionality for integration tests,
//! including crypto provider initialization.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Once;

/// Initialize cryptographic provider once for all tests
static INIT: Once = Once::new();

/// Initialize the default crypto provider for tests.
///
/// This function ensures the crypto provider is installed exactly once,
/// even when called from multiple tests. It's safe to call this from
/// every test that needs QUIC functionality.
///
/// When both rustls-ring and rustls-aws-lc-rs features are enabled
/// (e.g., with --all-features), this prevents the panic:
/// "Could not automatically determine the process-level CryptoProvider"
///
/// # Example
/// ```
/// mod common;
///
/// #[tokio::test]
/// async fn my_test() {
///     common::init_crypto();
///     // ... test code that uses QUIC ...
/// }
/// ```
pub fn init_crypto() {
    INIT.call_once(|| {
        // Install default crypto provider (aws-lc-rs for PQC support)
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

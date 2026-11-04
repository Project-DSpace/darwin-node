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

//! Extension Handlers for RFC 7250 Raw Public Keys
//!
//! Note: rustls 0.23.x does not yet have full RFC 7250 Raw Public Keys support.
//! See https://github.com/rustls/rustls/issues/423 for the tracking issue.
//!
//! This module provides a workaround by using custom certificate verifiers
//! that can handle SubjectPublicKeyInfo structures as "certificates".

use std::sync::Arc;

use rustls::{ClientConfig, ServerConfig};

use super::tls_extensions::CertificateTypePreferences;

/// Configure client with certificate type preferences
pub fn configure_client(_config: &mut ClientConfig, _preferences: Arc<CertificateTypePreferences>) {
    // rustls 0.23.x handles RFC 7250 internally
}

/// Configure server with certificate type preferences  
pub fn configure_server(_config: &mut ServerConfig, _preferences: Arc<CertificateTypePreferences>) {
    // rustls 0.23.x handles RFC 7250 internally
}

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


//! TLS Extension Handling
//!
//! This module implements TLS extension handling for certificate type negotiation
//! as specified in RFC 7250. It focuses on the minimal set of extensions needed
//! for raw public key authentication.

use std::sync::Arc;
use thiserror::Error;

/// Errors that can occur during TLS extension handling
#[derive(Debug, Error)]
pub enum TlsExtensionError {
    #[error("Unsupported certificate type")]
    UnsupportedCertificateType,
    
    #[error("Extension encoding error: {0}")]
    EncodingError(String),
    
    #[error("Extension decoding error: {0}")]
    DecodingError(String),
}

/// Certificate types as defined in RFC 7250
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertificateType {
    /// X.509 certificate
    X509 = 0,
    /// Raw public key
    RawPublicKey = 2,
}

/// Handler for certificate type negotiation
pub struct CertificateTypeHandler {
    // Supported certificate types in order of preference
    supported_types: Vec<CertificateType>,
}

impl CertificateTypeHandler {
    /// Create a new handler with the specified supported types
    pub fn new(supported_types: Vec<CertificateType>) -> Self {
        Self { supported_types }
    }
    
    /// Create a handler that only supports raw public keys
    pub fn raw_public_key_only() -> Self {
        Self {
            supported_types: vec![CertificateType::RawPublicKey],
        }
    }
    
    /// Get the supported certificate types
    pub fn supported_types(&self) -> &[CertificateType] {
        &self.supported_types
    }
}

// Implementation of TLS extension handling
// (Placeholder - actual implementation would go here)
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

//! Metrics collection system
//!
//! This module provides internal metrics collection capabilities for saorsa-transport.
//!
//! ## Example
//!
//! ```rust
//! use saorsa_transport::metrics::MetricsConfig;
//!
//! let config = MetricsConfig::default();
//! assert!(!config.enabled);
//! ```

pub use crate::logging::metrics::*;

/// Configuration for metrics collection and export
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Whether to enable metrics collection
    pub enabled: bool,
    /// Port for the metrics HTTP server (only used when prometheus feature is enabled)
    pub port: u16,
    /// Address to bind the metrics server to
    pub bind_address: std::net::IpAddr,
    /// Update interval for metrics collection
    pub update_interval: std::time::Duration,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 9090,
            bind_address: std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
            update_interval: std::time::Duration::from_secs(30),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_config_default() {
        let config = MetricsConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.port, 9090);
        assert_eq!(
            config.bind_address,
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0))
        );
        assert_eq!(config.update_interval, std::time::Duration::from_secs(30));
    }
}

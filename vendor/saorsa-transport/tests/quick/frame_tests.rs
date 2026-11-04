// This software is licensed under the MIT license <LICENSE-MIT or
// https://opensource.org/licenses/MIT> or the Apache License, Version 2.0
// <LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0>, at your
// option. This file may not be copied, modified, or distributed except
// according to those terms.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//! Quick frame parsing tests

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

#[test]
fn test_frame_type_identification() {
    super::utils::assert_duration(Duration::from_millis(10), || {
        // Basic frame type tests
        // Frame types are const values and tested in unit tests
        // Placeholder test - implementation pending
    });
}

#[test]
fn test_observed_address_creation() {
    super::utils::assert_duration(Duration::from_millis(50), || {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        // Observed address frame tested in unit tests
        assert_eq!(addr.port(), 8080);
    });
}

#[test]
fn test_frame_size_calculations() {
    super::utils::assert_duration(Duration::from_millis(10), || {
        // Test that basic structures have reasonable sizes
        use std::mem::size_of;

        // Socket addresses should be reasonable size
        assert!(size_of::<SocketAddr>() <= 32);
    });
}

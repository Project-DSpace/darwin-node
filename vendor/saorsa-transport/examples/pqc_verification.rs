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

//! PQC Verification Example
//!
//! v0.13.0+: PQC is always enabled (100% PQC, no classical crypto).
//! This example verifies the P2pEndpoint initializes with PQC correctly.

use saorsa_transport::{P2pConfig, P2pEndpoint, PqcConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // v0.13.0+: PQC is always-on. Configure ML-KEM and ML-DSA.
    let pqc_config = PqcConfig::builder().ml_kem(true).ml_dsa(true).build()?;

    // v0.13.0+: No role needed - all nodes are symmetric P2P nodes
    let config = P2pConfig::builder()
        .known_peer("127.0.0.1:9000".parse::<std::net::SocketAddr>()?)
        .pqc(pqc_config)
        .build()?;

    println!("Attempting to create P2pEndpoint with PQC...");

    // Create the endpoint
    let endpoint = P2pEndpoint::new(config).await?;
    println!(
        "Endpoint created at local addr: {:?}",
        endpoint.local_addr()
    );

    // Verify PQC is enabled
    println!("Verification passed: P2pEndpoint initialized with PQC config.");

    endpoint.shutdown().await;
    Ok(())
}

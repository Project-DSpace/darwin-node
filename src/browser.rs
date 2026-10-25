//! Shared browser-client discovery types.
//!
//! These types describe the public read and paid immutable-write capabilities
//! exposed by browser-enabled nodes. Wallet secrets never form part of these
//! records: browsers sign EVM transactions locally and send only payment
//! receipts to nodes.

pub use saorsa_transport::webrtc::{BrowserEndpoint, BrowserPaymentNetwork, WebRtcDirectEndpoint};
use serde::{Deserialize, Serialize};

/// Version of the local browser bootstrap manifest.
pub const BROWSER_MANIFEST_VERSION: u16 = 6;

/// A bootstrap node that a browser can authenticate and contact directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserBootstrapNode {
    /// Self-contained browser endpoint for this node.
    #[serde(flatten)]
    pub endpoint: BrowserEndpoint,
}

/// Metadata for immutable content published into a browser-enabled devnet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserPublicFile {
    /// Human-readable filename suggested to the browser.
    pub name: String,
    /// Address of the publicly stored `MessagePack` `DataMap`.
    pub address: String,
    /// Plaintext content length in bytes.
    pub size: usize,
    /// MIME type used by the browser when saving the content.
    pub content_type: String,
    /// BLAKE3 hash of the fully reconstructed plaintext file.
    pub blake3: String,
    /// Size of the publicly stored `MessagePack` `DataMap` chunk.
    pub data_map_size: usize,
    /// Resolved root `DataMap` used to reconstruct the file.
    pub chunks: Vec<BrowserChunkInfo>,
    /// Minimum number of devnet nodes that admitted every required record.
    pub replicas: usize,
}

/// One resolved self-encryption chunk descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserChunkInfo {
    /// Zero-based plaintext order.
    pub index: usize,
    /// Address of the encrypted chunk stored by nodes.
    pub dst_hash: String,
    /// BLAKE3 hash of the plaintext chunk and self-encryption key input.
    pub src_hash: String,
    /// Expected plaintext chunk size.
    pub src_size: usize,
}

/// Resolve the public payment identity without exposing the verifier's RPC URL.
#[cfg(any(feature = "webrtc-direct", test))]
pub(crate) async fn browser_payment_network(
    network: &evmlib::Network,
) -> crate::Result<BrowserPaymentNetwork> {
    let chain_id = match network {
        evmlib::Network::ArbitrumOne => 42_161,
        evmlib::Network::ArbitrumSepoliaTest => 421_614,
        evmlib::Network::Custom(_) => payment_chain_id_retrying(network.rpc_url()).await?,
    };
    Ok(BrowserPaymentNetwork {
        chain_id,
        payment_token_address: format!("{:?}", network.payment_token_address()),
        payment_vault_address: format!("{:?}", network.payment_vault_address()),
    })
}

/// Rounds of chain ID reads at start before the node gives up: one failed
/// read must not keep it down.
#[cfg(any(feature = "webrtc-direct", test))]
const CHAIN_ID_ROUNDS: u32 = 4;

/// The chain ID through `rpc_url`, then through the other public RPCs of the
/// same chain ([`crate::listings::listings_rpc_urls`]), in rounds 2, 4 and 8 s
/// apart; the last error if no RPC answers in any round.
#[cfg(any(feature = "webrtc-direct", test))]
async fn payment_chain_id_retrying(rpc_url: &reqwest::Url) -> crate::Result<u64> {
    let urls: Vec<reqwest::Url> = crate::listings::listings_rpc_urls(rpc_url.as_str())
        .iter()
        .filter_map(|url| url.parse().ok())
        .collect();
    let mut last = None;
    for round in 0..CHAIN_ID_ROUNDS {
        if round > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(1 << round)).await;
        }
        for url in &urls {
            match payment_chain_id(url).await {
                Ok(chain_id) => return Ok(chain_id),
                Err(e) => last = Some(e),
            }
        }
    }
    Err(last.unwrap_or_else(|| {
        crate::Error::Config("no RPC to read the custom EVM chain ID from".to_string())
    }))
}

#[cfg(any(feature = "webrtc-direct", test))]
async fn payment_chain_id(rpc_url: &reqwest::Url) -> crate::Result<u64> {
    // Neither provider errors nor response bodies may escape into errors: they
    // can contain the operator's credentials, API keys, or internal addresses.
    let unavailable = || {
        crate::Error::Config(
            "could not resolve the custom EVM chain ID for browser payment metadata".to_string(),
        )
    };
    let invalid = || {
        crate::Error::Config("custom EVM RPC returned an invalid eth_chainId response".to_string())
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| unavailable())?;
    let mut response = client
        .post(rpc_url.clone())
        .json(
            &serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}),
        )
        .send()
        .await
        .map_err(|_| unavailable())?
        .error_for_status()
        .map_err(|_| unavailable())?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
        if body.len() + chunk.len() > 4096 {
            return Err(invalid());
        }
        body.extend_from_slice(&chunk);
    }
    let response: serde_json::Value = serde_json::from_slice(&body).map_err(|_| invalid())?;
    if response["jsonrpc"] != "2.0" || response["id"] != 1 || response.get("error").is_some() {
        return Err(invalid());
    }
    let hex = response["result"]
        .as_str()
        .and_then(|value| value.strip_prefix("0x"))
        .filter(|hex| {
            !hex.is_empty()
                && (hex.len() == 1 || !hex.starts_with('0'))
                && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        .ok_or_else(invalid)?;
    u64::from_str_radix(hex, 16).map_err(|_| invalid())
}

/// Local-devnet handoff consumed by the browser application.
///
/// This manifest is intentionally a local testnet bootstrap artifact. The
/// production design replaces it with the ML-DSA-signed endpoint records from
/// ADR-0015.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserDevnetManifest {
    /// Manifest schema version.
    pub version: u16,
    /// Opaque identifier that distinguishes concurrent local devnets.
    pub network_id: String,
    /// Creation time in RFC 3339 form.
    pub created_at: String,
    /// Direct node endpoints available as initial browser contacts.
    pub endpoints: Vec<BrowserBootstrapNode>,
    /// Public payment chain and contracts used by browser uploads.
    pub payment: BrowserPaymentNetwork,
    /// Immutable files published when the devnet started.
    pub files: Vec<BrowserPublicFile>,
}

impl BrowserDevnetManifest {
    /// Construct a versioned local browser manifest.
    #[must_use]
    pub fn new(
        network_id: String,
        created_at: String,
        endpoints: Vec<BrowserBootstrapNode>,
        payment: BrowserPaymentNetwork,
        files: Vec<BrowserPublicFile>,
    ) -> Self {
        Self {
            version: BROWSER_MANIFEST_VERSION,
            network_id,
            created_at,
            endpoints,
            payment,
            files,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{browser_payment_network, payment_chain_id, payment_chain_id_retrying};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// An RPC answering `eth_chainId` with 4663, after failing its first
    /// `failures` requests with HTTP 503; its URL and the request count.
    async fn flaky_rpc(failures: usize) -> (reqwest::Url, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&count);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buffer = vec![0u8; 4096];
                let _ = socket.read(&mut buffer).await;
                let reply = if seen.fetch_add(1, Ordering::SeqCst) < failures {
                    "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n".to_string()
                } else {
                    let body = r#"{"jsonrpc":"2.0","id":1,"result":"0x1237"}"#;
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    )
                };
                let _ = socket.write_all(reply.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        (url, count)
    }

    #[tokio::test]
    async fn a_failed_chain_id_read_at_start_is_tried_again() {
        let (url, count) = flaky_rpc(1).await;
        assert_eq!(payment_chain_id_retrying(&url).await.unwrap(), 4663);
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn builtin_payment_identity_contains_only_chain_and_contracts(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (network, chain_id) in [
            (evmlib::Network::ArbitrumOne, 42_161),
            (evmlib::Network::ArbitrumSepoliaTest, 421_614),
        ] {
            let identity = browser_payment_network(&network).await?;
            assert_eq!(identity.chain_id, chain_id);
            assert_eq!(
                serde_json::to_value(identity)?,
                serde_json::json!({
                    "chain_id": chain_id,
                    "payment_token_address": format!("{:?}", network.payment_token_address()),
                    "payment_vault_address": format!("{:?}", network.payment_vault_address()),
                })
            );
        }
        Ok(())
    }

    /// A chain ID that isn't one (zero-padded, over 64 bits, under another
    /// request id) or a provider error is refused, and the error names
    /// neither the provider's words nor the RPC's URL, whose user, password,
    /// path and query can hold the operator's keys. Was the devnet test
    /// `invalid_chain_identity_fails_without_exposing_provider_response`
    /// (tests/webrtc_direct_devnet.rs), which started a devnet for each case
    /// to reach this read.
    #[tokio::test]
    async fn an_invalid_chain_identity_is_refused_without_the_providers_words() {
        for body in [
            r#"{"jsonrpc":"2.0","id":1,"result":"0x01"}"#,
            r#"{"jsonrpc":"2.0","id":1,"result":"0x10000000000000000"}"#,
            r#"{"jsonrpc":"2.0","id":2,"result":"0x1"}"#,
            r#"{"jsonrpc":"2.0","id":1,"error":{"message":"dummy-private-url"}}"#,
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url: reqwest::Url = format!(
                "http://dummy-user:dummy-password@{}/v2/dummy-path-key?api_key=dummy-query-key",
                listener.local_addr().unwrap()
            )
            .parse()
            .unwrap();
            tokio::spawn(async move {
                while let Ok((mut socket, _)) = listener.accept().await {
                    let mut buffer = vec![0u8; 4096];
                    let _ = socket.read(&mut buffer).await;
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                    let _ = socket.shutdown().await;
                }
            });
            let error = payment_chain_id(&url).await.expect_err(body).to_string();
            assert!(!error.contains("dummy"), "{body}: {error}");
            assert!(!error.contains(url.as_str()), "{body}: {error}");
        }
    }
}

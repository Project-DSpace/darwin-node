//! Values this fork sets on purpose, each of which a merge from upstream could
//! quietly put back: the repository nodes upgrade from and the key its
//! releases are signed with, the bootstrap peers that ship (none), the
//! allowance of new browser sessions per source, and the penalty for not
//! holding a close-group chunk, which nothing can withhold. And how the
//! payment check reads the chain (`payment_reads` below), which a release
//! that rewrites the read path must keep: a failed read is made again in a
//! second round, an empty record is read once more, a payment to another
//! payee is refused, an overpayment taken, and a merkle proof refused before
//! anything is read.
//!
//! Upstream has no such file, so a merge can't drop it while resolving a
//! conflict. Each test fails if its value changes. The payment reads need the
//! `test-utils` feature, with which CI runs every test.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ant_node::config::{BootstrapPeersConfig, NodeConfig, UpgradeConfig, WebRtcDirectConfig};
use sha2::{Digest, Sha256};
use std::path::Path;

/// The repository whose releases nodes install by themselves.
const UPGRADE_REPO: &str = "Project-DSpace/darwin-node";

/// SHA-256 of `sign/release-signing-key.pub`, the ML-DSA-65 public key every
/// release is signed for.
const RELEASE_KEY_SHA256: &str = "695bea4c91d0046dfacc48edc293584485b242d30ba534bb61eebb485ce97288";

/// New browser sessions one source may open at once, then per minute.
const NEW_SESSIONS_BURST: usize = 12;
const NEW_SESSIONS_PER_MINUTE: usize = 6;

fn repo_path(path: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn repo_text(path: &str) -> String {
    std::fs::read_to_string(repo_path(path)).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn production_toml() -> toml::Value {
    // Read as a plain document: the file as a whole doesn't deserialize as a
    // NodeConfig (its `evm_network` is a bare string).
    toml::from_str(&repo_text("config/production.toml")).expect("production.toml is TOML")
}

#[test]
fn nodes_upgrade_from_this_forks_releases() {
    assert_eq!(UpgradeConfig::default().github_repo, UPGRADE_REPO);
    assert_eq!(NodeConfig::default().upgrade.github_repo, UPGRADE_REPO);

    let empty: NodeConfig = toml::from_str("").unwrap();
    assert_eq!(empty.upgrade.github_repo, UPGRADE_REPO);
    let partial: NodeConfig = toml::from_str("[upgrade]\nchannel = \"beta\"").unwrap();
    assert_eq!(partial.upgrade.github_repo, UPGRADE_REPO);

    let production = production_toml();
    let upgrade = production
        .get("upgrade")
        .expect("production.toml has an [upgrade] section");
    assert_eq!(
        upgrade.get("github_repo").and_then(toml::Value::as_str),
        Some(UPGRADE_REPO)
    );
    let upgrade: UpgradeConfig = upgrade.clone().try_into().unwrap();
    assert_eq!(upgrade.github_repo, UPGRADE_REPO);
}

/// The key bytes in `RELEASE_SIGNING_KEY`, read from the source as
/// release.yml reads them: from the line that starts the constant to the line
/// that closes it, every `0x` followed by two lowercase hex digits.
fn embedded_release_key(source: &str) -> Vec<u8> {
    let lines = source
        .lines()
        .skip_while(|line| !line.starts_with("const RELEASE_SIGNING_KEY"));
    let mut bytes = Vec::new();
    for line in lines {
        let mut rest = line;
        while let Some(at) = rest.find("0x") {
            let digits = rest.get(at + 2..at + 4).unwrap_or_default();
            if digits.len() == 2
                && digits
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                bytes.push(u8::from_str_radix(digits, 16).unwrap());
            }
            rest = &rest[at + 2..];
        }
        if line.starts_with("];") {
            return bytes;
        }
    }
    panic!("RELEASE_SIGNING_KEY not found, or not closed with a line starting \"];\"");
}

#[test]
fn releases_are_checked_against_the_published_key() {
    let published = std::fs::read(repo_path("sign/release-signing-key.pub")).unwrap();
    assert_eq!(hex::encode(Sha256::digest(&published)), RELEASE_KEY_SHA256);
    assert_eq!(published.len(), ant_node::upgrade::PUBLIC_KEY_SIZE);

    let embedded = embedded_release_key(&repo_text("src/upgrade/signature.rs"));
    assert!(
        embedded == published,
        "RELEASE_SIGNING_KEY in src/upgrade/signature.rs ({} bytes) is not \
         sign/release-signing-key.pub",
        embedded.len()
    );
}

#[test]
fn no_bootstrap_peers_ship() {
    let shipped = repo_text("config/bootstrap_peers.toml");
    let file: BootstrapPeersConfig = toml::from_str(&shipped).unwrap();
    assert!(file.peers.is_empty(), "{:?}", file.peers);
    // Every list in the file, whatever it is called.
    let document: toml::Value = toml::from_str(&shipped).unwrap();
    for (key, value) in document.as_table().expect("a table") {
        if let Some(list) = value.as_array() {
            assert!(
                list.is_empty(),
                "config/bootstrap_peers.toml: {key} = {list:?}"
            );
        }
    }

    let empty: NodeConfig = toml::from_str("").unwrap();
    for (preset, config) in [
        ("default", NodeConfig::default()),
        ("testnet", NodeConfig::testnet()),
        ("development", NodeConfig::development()),
        ("an empty config file", empty),
    ] {
        assert!(
            config.bootstrap.is_empty(),
            "the {preset} preset names bootstrap peers: {:?}",
            config.bootstrap
        );
    }
    let production = production_toml();
    let bootstrap = production
        .get("bootstrap")
        .and_then(toml::Value::as_array)
        .expect("production.toml has a bootstrap list");
    assert!(
        bootstrap.is_empty(),
        "production.toml names bootstrap peers: {bootstrap:?}"
    );
}

#[test]
fn a_browser_source_opens_12_sessions_then_6_a_minute() {
    let empty: NodeConfig = toml::from_str("").unwrap();
    let partial: NodeConfig = toml::from_str("[webrtc_direct]\nmax_connections = 48").unwrap();
    for (from, config) in [
        (
            "WebRtcDirectConfig::default()",
            WebRtcDirectConfig::default(),
        ),
        ("NodeConfig::default()", NodeConfig::default().webrtc_direct),
        ("an empty config file", empty.webrtc_direct),
        ("a partial [webrtc_direct]", partial.webrtc_direct),
    ] {
        assert_eq!(
            config.new_sessions_burst_per_ip, NEW_SESSIONS_BURST,
            "{from}"
        );
        assert_eq!(
            config.new_sessions_per_minute_per_ip, NEW_SESSIONS_PER_MINUTE,
            "{from}"
        );
    }
}

/// The names the switch that withheld the unheld-chunk penalty went by, while
/// nodes moved off the LMDB chunk store: its environment override, the build
/// constant, and the helper every lane went through.
const PENALTY_SWITCH: &[&str] = &[
    "ANT_SUSPEND_UNHELD_CHUNK_PENALTY",
    "RELEASE_SUSPEND_CLOSE_GROUP_STORAGE_PENALTY",
    "close_group_storage_penalty_suspended",
    "penalise_unheld_close_group_chunk",
];

/// Every `.rs` file under `dir`, recursively.
fn rust_files(dir: &Path, found: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display())) {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, found);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            found.push(path);
        }
    }
}

/// A peer that doesn't hold a close-group chunk it should is penalised, as in
/// upstream v0.23.0, and nothing in the node or the image can withhold it: no
/// switch and no environment override. The census
/// (src/replication/retention/census.rs) pins the lanes that charge it, and a
/// build whose trust weights fall to zero doesn't compile (src/replication/mod.rs).
#[test]
fn nothing_withholds_the_unheld_chunk_penalty() {
    // The census names the switch's parts to pin them away, in test-only code.
    let census = repo_path("src/replication/retention/census.rs");
    let mut sources = Vec::new();
    rust_files(&repo_path("src"), &mut sources);
    sources.retain(|path| *path != census);
    assert!(
        sources.len() > 50,
        "found only {} source files",
        sources.len()
    );
    let image = ["docker/Dockerfile", "docker/entrypoint.sh"].map(repo_path);
    for path in sources.iter().chain(image.iter()) {
        let text = std::fs::read_to_string(path).unwrap();
        for name in PENALTY_SWITCH {
            assert!(
                !text.contains(name),
                "{} names {name}: the penalty for an unheld close-group chunk is applied \
                 on every lane, and nothing may withhold it",
                path.display()
            );
        }
    }
}

/// How the payment check reads the chain, through the path a node takes
/// (`PaymentVerifier::verify_payment`) to a JSON-RPC provider on 127.0.0.1
/// that answers as the vault's `completedPayments` does: no test override
/// stands in for the vault here, as it does in the verifier's unit tests. A
/// release that reads payments another way (pooled, batched, cached) keeps
/// these behaviours or fails here. The provider answers one call or a JSON-RPC
/// batch of them.
///
/// The paid quote's issuer must be among the node's closest peers, which a
/// node checks against its routing table; the tests name it as close through
/// the `test-utils` setter, so this module needs that feature.
#[cfg(feature = "test-utils")]
mod payment_reads {
    use ant_node::ant_protocol::PROOF_TAG_MERKLE;
    use ant_node::payment::{
        calculate_price, serialize_single_node_proof, EvmVerifierConfig, PaymentProof,
        PaymentStatus, PaymentVerifier, PaymentVerifierConfig, PriceFloorConfig,
        VerificationContext, BATCH_PAYMENTS_REFUSED, MIN_PAYMENT_PROOF_SIZE_BYTES,
    };
    use ant_node::CLOSE_GROUP_SIZE;
    use evmlib::common::Amount;
    use evmlib::{EncodedPeerId, PaymentQuote, ProofOfPayment, RewardsAddress};
    use saorsa_pqc::api::sig::ml_dsa_65;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant, SystemTime};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A payment read that fails is made again in a second round, half a
    /// second later (`PAYMENT_READ_ROUNDS`, `PAYMENT_READ_BACKOFF` in
    /// src/payment/verifier.rs). A node with one RPC reads twice in all.
    const READ_ROUNDS: usize = 2;
    const READ_BACKOFF: Duration = Duration::from_millis(500);

    /// A payment record that reads empty is read once more, 1.5 s later
    /// (`EMPTY_PAYMENT_RECHECK_DELAY`).
    const EMPTY_RECHECK: Duration = Duration::from_millis(1500);

    /// The vault and token the verifier is told of. Any addresses will do: the
    /// provider below answers for the vault whatever its address.
    const VAULT: &str = "0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512";
    const TOKEN: &str = "0x5FbDB2315678afecb367f032d93F642f64180aa3";

    /// The contexts a payment is checked in: a client's PUT, a fresh offer
    /// from a close-group peer, and a paid-list notice.
    const CONTEXTS: [VerificationContext; 3] = [
        VerificationContext::ClientPut,
        VerificationContext::FreshReplication,
        VerificationContext::PaidListAdmission,
    ];

    /// What the provider does with one `completedPayments` call.
    #[derive(Clone, Copy, Debug)]
    enum Answer {
        /// The vault's record: the payee's leading 16 bytes and the amount.
        Record([u8; 16], u128),
        /// `-32005 the network is busy`, as a busy public RPC answers.
        Busy,
        /// The connection closes before any answer.
        Dropped,
    }

    /// A `completedPayments` call the provider got: the quote hash it asked
    /// about, and when.
    #[derive(Clone, Debug)]
    struct Call {
        quote_hash: String,
        at: Instant,
    }

    /// A JSON-RPC provider on 127.0.0.1.
    struct Provider {
        url: String,
        calls: Arc<Mutex<Vec<Call>>>,
        /// Requests for anything but `eth_call`, answered "method not found".
        other_methods: Arc<Mutex<Vec<String>>>,
    }

    impl Provider {
        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }
    }

    /// One HTTP request's body, or `None` once the connection closes.
    async fn read_http_body(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
            let read = socket.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            buffer.extend_from_slice(&chunk[..read]);
        };
        let headers = String::from_utf8_lossy(&buffer[..header_end]).to_lowercase();
        let length: usize = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .map_or(0, |value| value.trim().parse().unwrap_or(0));
        while buffer.len() < header_end + length {
            let read = socket.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            buffer.extend_from_slice(&chunk[..read]);
        }
        Some(buffer[header_end..header_end + length].to_vec())
    }

    /// The ABI words of a `completedPayments` record:
    /// `(bytes16 rewardsAddress, uint128 amount)`.
    fn record_words(payee_prefix: [u8; 16], amount: u128) -> String {
        let mut words = [0u8; 64];
        words[..16].copy_from_slice(&payee_prefix);
        words[48..].copy_from_slice(&amount.to_be_bytes());
        format!("0x{}", hex::encode(words))
    }

    /// The answer to one JSON-RPC request object, or `None` to drop the
    /// connection.
    fn respond<F>(
        request: &serde_json::Value,
        answer: &F,
        calls: &Mutex<Vec<Call>>,
        other_methods: &Mutex<Vec<String>>,
    ) -> Option<serde_json::Value>
    where
        F: Fn(usize, &str) -> Answer,
    {
        let id = request["id"].clone();
        let method = request["method"].as_str().unwrap_or_default();
        if method != "eth_call" {
            other_methods.lock().unwrap().push(method.to_string());
            return Some(serde_json::json!({ "jsonrpc": "2.0", "id": id,
                "error": { "code": -32601, "message": "method not found" } }));
        }
        let input = request["params"][0]["input"]
            .as_str()
            .or_else(|| request["params"][0]["data"].as_str())
            .unwrap_or_default();
        let quote_hash = input
            .get(input.len().saturating_sub(64)..)
            .unwrap_or_default()
            .to_lowercase();
        let count = {
            let mut calls = calls.lock().unwrap();
            calls.push(Call {
                quote_hash: quote_hash.clone(),
                at: Instant::now(),
            });
            calls.len()
        };
        match answer(count, &quote_hash) {
            Answer::Record(prefix, amount) => Some(serde_json::json!({ "jsonrpc": "2.0",
                "id": id, "result": record_words(prefix, amount) })),
            Answer::Busy => Some(serde_json::json!({ "jsonrpc": "2.0", "id": id,
                "error": { "code": -32005, "message": "the network is busy" } })),
            Answer::Dropped => None,
        }
    }

    /// A provider that answers its `n`th `completedPayments` call (from 1),
    /// for a quote hash, with `answer(n, quote_hash)`.
    async fn provider<F>(answer: F) -> Provider
    where
        F: Fn(usize, &str) -> Answer + Send + Sync + 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let calls: Arc<Mutex<Vec<Call>>> = Arc::default();
        let other_methods: Arc<Mutex<Vec<String>>> = Arc::default();
        let (seen, others) = (Arc::clone(&calls), Arc::clone(&other_methods));
        let answer = Arc::new(answer);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let (seen, others, answer) =
                    (Arc::clone(&seen), Arc::clone(&others), Arc::clone(&answer));
                tokio::spawn(async move {
                    while let Some(body) = read_http_body(&mut socket).await {
                        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let response = request.as_array().map_or_else(
                            || respond(&request, answer.as_ref(), &seen, &others),
                            |batch| {
                                batch
                                    .iter()
                                    .map(|one| respond(one, answer.as_ref(), &seen, &others))
                                    .collect::<Option<Vec<_>>>()
                                    .map(serde_json::Value::from)
                            },
                        );
                        // A dropped call: the connection closes unanswered.
                        let Some(response) = response else {
                            return;
                        };
                        let body = response.to_string();
                        let reply = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                             content-length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        if socket.write_all(reply.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        Provider {
            url,
            calls,
            other_methods,
        }
    }

    /// A proof of 7 signed quotes for one address, as a client sends it, and
    /// the quote a client pays: the median, at 3 times its price.
    struct Bundle {
        xorname: [u8; 32],
        proof: Vec<u8>,
        median: PaymentQuote,
        median_peer: [u8; 32],
        required: u128,
    }

    impl Bundle {
        fn quote_hash(&self) -> String {
            hex::encode(self.median.hash())
        }

        /// The leading 16 bytes of the paid quote's rewards address, which is
        /// how the vault records the payee.
        fn payee_prefix(&self) -> [u8; 16] {
            let mut prefix = [0u8; 16];
            prefix.copy_from_slice(&self.median.rewards_address.as_slice()[..16]);
            prefix
        }
    }

    /// A quote for `xorname` from a node holding `records` committed keys,
    /// priced as that node prices it and signed with its key.
    fn signed_quote(xorname: [u8; 32], records: usize) -> (EncodedPeerId, PaymentQuote) {
        let (public_key, secret_key) = ml_dsa_65().generate_keypair().unwrap();
        let pub_key = public_key.to_bytes();
        let peer_id = EncodedPeerId::new(*blake3::hash(&pub_key).as_bytes());
        let mut quote = PaymentQuote {
            content: xor_name::XorName(xorname),
            timestamp: SystemTime::now(),
            price: calculate_price(records),
            rewards_address: RewardsAddress::new([u8::try_from(records + 1).unwrap(); 20]),
            committed_key_count: u32::try_from(records).unwrap(),
            commitment_pin: (records > 0).then_some([0xA5; 32]),
            pub_key,
            signature: Vec::new(),
        };
        quote.signature = ml_dsa_65()
            .sign(&secret_key, &quote.bytes_for_sig())
            .unwrap()
            .to_bytes();
        (peer_id, quote)
    }

    fn bundle(seed: u8) -> Bundle {
        let xorname = [seed; 32];
        // Prices rise with the records held, so the median is the middle one.
        let quotes: Vec<_> = (0..CLOSE_GROUP_SIZE)
            .map(|records| signed_quote(xorname, records))
            .collect();
        let (median_peer, median) = quotes[CLOSE_GROUP_SIZE / 2].clone();
        let required = u128::try_from(median.price * Amount::from(3u64)).unwrap();
        let proof = serialize_single_node_proof(&PaymentProof {
            proof_of_payment: ProofOfPayment {
                peer_quotes: quotes,
            },
            tx_hashes: Vec::new(),
            commitment_sidecars: Vec::new(),
        })
        .unwrap();
        Bundle {
            xorname,
            proof,
            median,
            median_peer: *median_peer.as_bytes(),
            required,
        }
    }

    /// A node's payment check, reading payments through `provider` alone.
    fn verifier(provider: &Provider, bundle: &Bundle) -> PaymentVerifier {
        let verifier = PaymentVerifier::new(PaymentVerifierConfig {
            evm: EvmVerifierConfig {
                network: evmlib::Network::new_custom(&provider.url, TOKEN, VAULT),
            },
            cache_capacity: 100,
            close_group_size: CLOSE_GROUP_SIZE,
            local_rewards_address: RewardsAddress::new([0xD1; 20]),
            price_floor: PriceFloorConfig::default(),
        });
        verifier.set_paid_quote_k_closest_for_tests(vec![bundle.median_peer]);
        verifier
    }

    /// What one payment check came to: its outcome, the calls the provider
    /// got, and how long it took.
    struct Checked {
        context: VerificationContext,
        outcome: Result<PaymentStatus, String>,
        calls: Vec<Call>,
        took: Duration,
        quote_hash: String,
    }

    /// Checks a fresh bundle's payment in `context`, the provider answering
    /// its `n`th call with `answer(n, bundle, quote_hash)`.
    async fn check<F>(seed: u8, context: VerificationContext, answer: F) -> Checked
    where
        F: Fn(usize, &Bundle, &str) -> Answer + Send + Sync + 'static,
    {
        let bundle = Arc::new(bundle(seed));
        let for_provider = Arc::clone(&bundle);
        let provider = provider(move |n, hash| answer(n, &for_provider, hash)).await;
        let verifier = verifier(&provider, &bundle);
        let started = Instant::now();
        let outcome = verifier
            .verify_payment(&bundle.xorname, Some(&bundle.proof), context)
            .await
            .map_err(|e| e.to_string());
        let took = started.elapsed();
        let other_methods = provider.other_methods.lock().unwrap().clone();
        assert!(
            other_methods.is_empty(),
            "{context:?}: the provider was asked for more than completedPayments: {other_methods:?}"
        );
        Checked {
            context,
            outcome,
            calls: provider.calls(),
            took,
            quote_hash: bundle.quote_hash(),
        }
    }

    /// The same check in every context at once, each with its own bundle,
    /// provider and node.
    async fn check_everywhere<F>(seed: u8, answer: F) -> Vec<Checked>
    where
        F: Fn(usize, &Bundle, &str) -> Answer + Clone + Send + Sync + 'static,
    {
        futures::future::join_all(
            CONTEXTS
                .iter()
                .zip(seed..)
                .map(|(context, seed)| check(seed, *context, answer.clone())),
        )
        .await
    }

    fn paid(bundle: &Bundle) -> Answer {
        Answer::Record(bundle.payee_prefix(), bundle.required)
    }

    /// The node's RPC fails the first read, busy or by dropping the call,
    /// and the payment still verifies: the second round, half a second
    /// later, reads the record. Upstream read once, and refused a chunk that
    /// was paid for.
    #[tokio::test]
    async fn a_read_that_fails_once_is_made_again_half_a_second_later() {
        for first in [Answer::Busy, Answer::Dropped] {
            let checked = check_everywhere(
                0x10,
                move |n, bundle, _| {
                    if n == 1 {
                        first
                    } else {
                        paid(bundle)
                    }
                },
            )
            .await;
            for c in checked {
                let case = format!("{:?}, first read {first:?}", c.context);
                assert_eq!(c.outcome, Ok(PaymentStatus::PaymentVerified), "{case}");
                let hashes: Vec<&str> = c
                    .calls
                    .iter()
                    .map(|call| call.quote_hash.as_str())
                    .collect();
                assert_eq!(
                    hashes,
                    [c.quote_hash.as_str(); 2],
                    "{case}: the paid quote's record, read twice"
                );
                assert!(
                    c.calls[1].at.duration_since(c.calls[0].at) >= READ_BACKOFF,
                    "{case}: the second round waits {READ_BACKOFF:?}"
                );
            }
        }
    }

    /// A read that fails in every round refuses the store, after one read in
    /// each round and no more.
    #[tokio::test]
    async fn a_read_that_keeps_failing_is_refused_after_two_rounds() {
        let checked = check_everywhere(0x20, |_, _, _| Answer::Busy).await;
        for c in checked {
            let context = c.context;
            assert!(c.outcome.is_err(), "{context:?}: {:?}", c.outcome);
            assert_eq!(
                c.calls.len(),
                READ_ROUNDS,
                "{context:?}: one read in each of {READ_ROUNDS} rounds: {:?}",
                c.calls
            );
            assert!(
                c.calls[1].at.duration_since(c.calls[0].at) >= READ_BACKOFF,
                "{context:?}: the second round waits {READ_BACKOFF:?}"
            );
        }
    }

    /// The record of a payment made a moment ago reads empty on an RPC a
    /// block behind, is read once more 1.5 s later, and holds the payment.
    #[tokio::test]
    async fn an_empty_record_is_read_again_and_verifies() {
        let checked = check_everywhere(0x30, |n, bundle, _| {
            if n == 1 {
                Answer::Record([0u8; 16], 0)
            } else {
                paid(bundle)
            }
        })
        .await;
        for c in checked {
            let context = c.context;
            assert_eq!(c.outcome, Ok(PaymentStatus::PaymentVerified), "{context:?}");
            assert_eq!(c.calls.len(), 2, "{context:?}: {:?}", c.calls);
            assert!(
                c.calls[1].at.duration_since(c.calls[0].at) >= EMPTY_RECHECK,
                "{context:?}: read again after {EMPTY_RECHECK:?}"
            );
        }
    }

    /// A record still empty when read again is refused, after one pause: a
    /// store nobody paid for waits no longer than that.
    #[tokio::test]
    async fn a_record_empty_twice_is_refused_after_one_pause() {
        let checked = check_everywhere(0x40, |_, _, _| Answer::Record([0u8; 16], 0)).await;
        for c in checked {
            let context = c.context;
            assert!(c.outcome.is_err(), "{context:?}: {:?}", c.outcome);
            assert_eq!(
                c.calls.len(),
                2,
                "{context:?}: read once, then once more: {:?}",
                c.calls
            );
            assert!(
                c.took >= EMPTY_RECHECK && c.took < EMPTY_RECHECK * 2,
                "{context:?}: one pause in all, took {:?}",
                c.took
            );
        }
    }

    /// A payment recorded for another payee is refused, however much it paid:
    /// a client can't pay itself for a store. The record isn't empty, so it is
    /// refused at once, after one read.
    #[tokio::test]
    async fn a_payment_to_another_payee_is_refused() {
        let checked = check_everywhere(0x50, |_, bundle, _| {
            Answer::Record([0xEE; 16], bundle.required * 10)
        })
        .await;
        for c in checked {
            let context = c.context;
            let err = c.outcome.expect_err("a redirected payment is refused");
            assert!(err.contains("redirected"), "{context:?}: {err}");
            assert_eq!(c.calls.len(), 1, "{context:?}: {:?}", c.calls);
            assert!(c.took < EMPTY_RECHECK, "{context:?}: took {:?}", c.took);
        }
    }

    /// The vault's record must hold at least 3 times the median quote's price,
    /// for its payee: more is taken (a client may overpay), exactly that is
    /// taken, and a wei less is refused at once.
    #[tokio::test]
    async fn an_overpayment_is_taken_and_an_underpayment_refused() {
        for (times, less, verified) in [(10, 0, true), (1, 0, true), (1, 1, false)] {
            let checked = check_everywhere(0x60, move |_, bundle, _| {
                Answer::Record(bundle.payee_prefix(), bundle.required * times - less)
            })
            .await;
            for c in checked {
                let case = format!("{:?}, {times} x the amount due, less {less} wei", c.context);
                if verified {
                    assert_eq!(c.outcome, Ok(PaymentStatus::PaymentVerified), "{case}");
                } else {
                    assert!(c.outcome.is_err(), "{case}: {:?}", c.outcome);
                    assert!(c.took < EMPTY_RECHECK, "{case}: took {:?}", c.took);
                }
                assert_eq!(c.calls.len(), 1, "{case}: {:?}", c.calls);
            }
        }
    }

    /// A merkle (batch) proof is refused in every context before anything is
    /// read from the chain.
    #[tokio::test]
    async fn a_merkle_proof_is_refused_before_any_read() {
        for context in CONTEXTS {
            let bundle = bundle(0x70);
            let provider = provider(|_, _| Answer::Busy).await;
            let verifier = verifier(&provider, &bundle);
            let mut proof = vec![0u8; MIN_PAYMENT_PROOF_SIZE_BYTES.max(64)];
            proof[0] = PROOF_TAG_MERKLE;
            let err = verifier
                .verify_payment(&bundle.xorname, Some(&proof), context)
                .await
                .expect_err("a merkle proof is refused");
            assert!(
                err.to_string().contains(BATCH_PAYMENTS_REFUSED),
                "{context:?}: {err}"
            );
            assert!(
                provider.calls().is_empty(),
                "{context:?}: {:?}",
                provider.calls()
            );
        }
    }
}

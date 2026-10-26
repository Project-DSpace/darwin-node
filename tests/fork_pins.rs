//! Values this fork sets on purpose, each of which a merge from upstream could
//! quietly put back: the repository nodes upgrade from and the key its
//! releases are signed with, the bootstrap peers that ship (none), the
//! allowance of new browser sessions per source, browser limits a starting
//! node accepts and binds, within the most sessions the transport holds
//! (`browser_listener` below), and the penalty for not holding a close-group
//! chunk, which nothing withholds but one bounded case: a full peer (its own
//! "full" notice fresh) answering that it lacks a key it was never seen
//! proving, in a check not pinned to its commitment and, after a write, not
//! paid for it ("full, not faulty", `unheld_chunk_penalty` below). And how the
//! payment check reads the chain (`payment_reads` below), which a release
//! that rewrites the read path must keep: the node's own RPC is read first,
//! then, if that is one of Robinhood Chain's public RPCs, the others; a failed
//! read moves on to the next RPC at once and is made again in a second round,
//! while an answer, even an empty record, ends the read; an empty record is
//! read once more; a payment to another payee is refused, an overpayment
//! taken, and a merkle proof refused before anything is read. A receipt of a
//! transaction the proof names can settle a quote it shows paid in full to
//! the quote's payee, with no read of the record, and can do nothing else: it
//! never refuses, so a receipt that doesn't pay the quote leaves the record
//! read to decide. A store's whole payment check ends inside the time a
//! released client waits for its answer. And the bytes of
//! the payment proofs nodes pass each other in fresh offers and paid notices,
//! and of the quotes inside them, with each quote's hash
//! (`payment_encodings` below). And the census of test files, so a merge
//! can't bring back a test this fork removed, or add one, unreviewed
//! (`every_test_file_is_reviewed`).
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

/// The browser listener's defaults, which every operator runs unless their
/// node config file lowers them. A default the node refused would stop every
/// node that offers browser access from starting, before it could upgrade
/// itself out of the fault.
#[cfg(feature = "webrtc-direct")]
mod browser_listener {
    use ant_node::config::{NodeConfig, WEBRTC_DIRECT_SESSION_CEILING};
    use saorsa_transport::webrtc_direct::{
        WebRtcAdmissionLimits, WebRtcCertificate, WebRtcDirectListener,
    };

    #[tokio::test]
    async fn the_default_config_passes_the_nodes_check_and_binds_a_listener() {
        let config = NodeConfig::default().webrtc_direct;
        let bound = ant_node::check_browser_listener(&config, "127.0.0.1:0".parse().unwrap())
            .await
            .expect("the default browser limits pass the check and bind");
        assert!(bound.ip().is_loopback());
        assert_ne!(bound.port(), 0);
    }

    /// The ceiling the node holds `max_connections` to is the transport's: a
    /// listener binds with it and refuses one more, and so does the node. A
    /// transport upgrade that moves it fails here, so the ceiling is checked
    /// again at every bump.
    #[tokio::test]
    async fn the_session_ceiling_is_the_transports() {
        assert_eq!(WEBRTC_DIRECT_SESSION_CEILING, 256);
        for (sessions, binds) in [
            (WEBRTC_DIRECT_SESSION_CEILING, true),
            (WEBRTC_DIRECT_SESSION_CEILING + 1, false),
        ] {
            let listener = WebRtcDirectListener::bind_with_limits(
                "127.0.0.1:0".parse().unwrap(),
                WebRtcCertificate::generate().unwrap(),
                WebRtcAdmissionLimits {
                    max_connections: sessions,
                    max_connections_per_ip: 1,
                },
            )
            .await;
            assert_eq!(listener.is_ok(), binds, "a listener of {sessions} sessions");
            if let Ok(listener) = listener {
                listener.close().await.unwrap();
            }
        }
        let mut config = NodeConfig::default().webrtc_direct;
        config.max_connections = WEBRTC_DIRECT_SESSION_CEILING + 1;
        config.max_channels = config.max_channels.max(2 * config.max_connections);
        let error = ant_node::check_browser_listener(&config, "127.0.0.1:0".parse().unwrap())
            .await
            .expect_err("a node refuses more sessions than the transport holds")
            .to_string();
        assert!(error.contains("at most 256"), "{error}");
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
/// upstream v0.23.0, and nothing in the node or the image can withhold it but
/// the one bounded case of "full, not faulty" (`unheld_chunk_penalty` below):
/// no switch and no environment override. The census
/// (src/replication/retention/census.rs) pins the lanes that charge it and the
/// one waiver each lane asks, and a build whose trust weights fall to zero
/// doesn't compile (src/replication/mod.rs).
#[test]
fn nothing_but_a_fresh_full_notice_withholds_the_unheld_chunk_penalty() {
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
                 on every lane, and nothing may withhold it but the waiver for a full peer",
                path.display()
            );
        }
    }
}

/// Every file under `tests/`, reviewed on 2026-10-25 when the suite was cut
/// back to the tests that protect something. A file that isn't here, from an
/// upstream merge or a new branch, fails the census below until it is
/// reviewed: kept and listed, or left out and listed in `REMOVED_TEST_FILES`.
const TEST_FILES: &[&str] = &[
    "tests/chunk_store_crash_safety.rs",
    "tests/darwin_image.rs",
    "tests/darwin_network.rs",
    "tests/e2e/anvil.rs",
    "tests/e2e/data_types/chunk.rs",
    "tests/e2e/data_types/mod.rs",
    "tests/e2e/fetch_local_write_guard.rs",
    "tests/e2e/fetch_responsibility_recheck.rs",
    "tests/e2e/fresh_offer_capacity.rs",
    "tests/e2e/harness.rs",
    "tests/e2e/integration_tests.rs",
    "tests/e2e/listings.rs",
    "tests/e2e/mod.rs",
    "tests/e2e/pointer_replication.rs",
    "tests/e2e/replication.rs",
    "tests/e2e/settlement_version_gate.rs",
    "tests/e2e/subtree_audit_testnet.rs",
    "tests/e2e/sync_pages.rs",
    "tests/e2e/testnet.rs",
    "tests/e2e/verify_storage_gate.rs",
    "tests/fork_pins.rs",
    "tests/full_line.rs",
    "tests/poc_audit_handler_live.rs",
    "tests/poc_bootstrap_stall.rs",
    "tests/poc_commitment_audit_attacks.rs",
    "tests/poc_d1_bounded_queues.rs",
    "tests/poc_price_floor_live.rs",
    "tests/pointer_convergence.rs",
    "tests/price_multiplier.rs",
    "tests/record_weight.rs",
    "tests/small_disks.rs",
    "tests/storage_scale.rs",
    "tests/test_seams.rs",
    "tests/webrtc_direct_devnet.rs",
    "tests/webrtc_dtls_cleanup.rs",
    "tests/wire_format.rs",
];

/// Upstream's test files this fork removed, and where what they checked is
/// checked now. An upstream merge that brings one back fails the census.
const REMOVED_TEST_FILES: &[(&str, &str)] = &[
    (
        "tests/e2e/payment_flow.rs",
        "tested only its own helper; paid stores on live nodes: e2e listings, \
         poc_price_floor_live",
    ),
    ("tests/e2e/complete_payment_e2e.rs", "had no tests"),
    (
        "tests/e2e/live_testnet.rs",
        "ignored, disabled since saorsa-core 0.16",
    ),
    (
        "tests/e2e/first_audit_ab.rs",
        "an A/B load driver behind FIRST_AUDIT_AB=1, never run in CI",
    ),
    (
        "tests/e2e/security_attacks.rs",
        "the refusal table, e2e data_types::chunk::test_chunk_rejected_without_payment",
    ),
    (
        "tests/e2e/merkle_payment.rs",
        "merkle refusal: payment_reads::a_merkle_proof_is_refused_before_any_read here, \
         the verifier's merkle_proofs_are_refused_in_every_context and _for_pointers, \
         and the refusal table",
    ),
];

/// The census of test files: every file under `tests/` is one reviewed
/// (`TEST_FILES`), and none of the removed ones is back.
#[test]
fn every_test_file_is_reviewed() {
    let root = repo_path("");
    let mut found = Vec::new();
    rust_files(&repo_path("tests"), &mut found);
    let mut found: Vec<String> = found
        .iter()
        .map(|path| {
            path.strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect();
    found.sort();
    let mut listed: Vec<String> = TEST_FILES.iter().map(|s| (*s).to_string()).collect();
    listed.sort();
    for (path, now) in REMOVED_TEST_FILES {
        assert!(
            !repo_path(path).exists(),
            "{path} is back: it was removed (its checks: {now}); review it, then \
             list it in TEST_FILES or delete it"
        );
    }
    let unreviewed: Vec<_> = found.iter().filter(|f| !listed.contains(f)).collect();
    let missing: Vec<_> = listed.iter().filter(|f| !found.contains(f)).collect();
    assert!(
        unreviewed.is_empty() && missing.is_empty(),
        "test files not in TEST_FILES (review each, then list it): {unreviewed:?}; \
         listed but gone (take them off the list): {missing:?}"
    );
}

/// "Full, not faulty": the one case in which the unheld-chunk penalty is
/// waived, on each lane that charges it, through the one function every lane
/// asks (`FullView::waives_unheld_penalty`). The bound on how long a node may
/// answer "full" without the penalty is how long one "full" notice stays
/// fresh: a node that keeps renewing it is never charged for being full, and
/// one that stops is charged again once its last notice is that old.
mod unheld_chunk_penalty {
    use ant_node::replication::full::{
        FullView, Lane, OwnQuote, UnheldAnswer, UnheldCase, FULL_NOTICE_FRESHNESS,
    };
    use saorsa_core::identity::PeerId;
    use std::time::{Duration, Instant};

    /// How long one "full" notice stays fresh: 40 minutes, two of the
    /// longest neighbour-sync intervals.
    const FRESHNESS: Duration = Duration::from_mins(40);

    fn case<'a>(lane: Lane, peer: &'a PeerId, key: &'a [u8; 32]) -> UnheldCase<'a> {
        UnheldCase {
            lane,
            peer,
            key,
            answer: UnheldAnswer::Absent,
            commitment_pinned: false,
            own_quote: if matches!(
                lane,
                Lane::PossessionFailed | Lane::PossessionNoAnswer | Lane::PointerPossession
            ) {
                OwnQuote::NotPaid
            } else {
                OwnQuote::NoProof
            },
        }
    }

    #[test]
    fn a_full_notice_stays_fresh_for_forty_minutes() {
        assert_eq!(FULL_NOTICE_FRESHNESS, FRESHNESS);
    }

    /// On every lane: no notice, a stale notice, a check pinned to the peer's
    /// commitment, a key the peer was seen holding, the peer's own paid quote,
    /// and an answer other than "absent" (no answer among them) are all
    /// charged; a fresh notice for any other key is not.
    #[test]
    fn only_a_fresh_notice_for_a_key_never_seen_held_and_not_paid_for_is_waived() {
        let view = FullView::default();
        let peer = PeerId::from_bytes([1; 32]);
        let other = PeerId::from_bytes([2; 32]);
        let key = [7u8; 32];
        let held = [8u8; 32];
        let now = Instant::now();
        for lane in Lane::ALL {
            assert!(
                !view.waives_unheld_penalty(&case(lane, &peer, &key), now),
                "{lane:?}: no notice, charged"
            );
        }
        view.marks.note_notice(peer, true, now);
        view.seen.record(&peer, &held);
        for lane in Lane::ALL {
            let fresh = case(lane, &peer, &key);
            assert!(view.waives_unheld_penalty(&fresh, now), "{lane:?}: waived");
            assert!(
                view.waives_unheld_penalty(
                    &fresh,
                    now + FRESHNESS.saturating_sub(Duration::from_secs(1))
                ),
                "{lane:?}: still fresh"
            );
            assert!(
                !view.waives_unheld_penalty(&fresh, now + FRESHNESS),
                "{lane:?}: a stale notice is charged"
            );
            assert!(
                !view.waives_unheld_penalty(&case(lane, &other, &key), now),
                "{lane:?}: another peer's notice is no notice"
            );
            let pinned = UnheldCase {
                commitment_pinned: true,
                ..fresh
            };
            assert!(
                !view.waives_unheld_penalty(&pinned, now),
                "{lane:?}: a commitment-pinned check is charged"
            );
            assert!(
                !view.waives_unheld_penalty(&case(lane, &peer, &held), now),
                "{lane:?}: a key the peer was seen holding is charged"
            );
            let paid = UnheldCase {
                own_quote: OwnQuote::PaidOrUnknown,
                ..fresh
            };
            assert!(
                !view.waives_unheld_penalty(&paid, now),
                "{lane:?}: a peer paid for the record is charged"
            );
            let unanswered = UnheldCase {
                answer: UnheldAnswer::Other,
                ..fresh
            };
            assert!(
                !view.waives_unheld_penalty(&unanswered, now),
                "{lane:?}: no answer, or any answer but absent, is charged"
            );
        }
        // A peer that says it has left "full" is charged at once.
        view.marks.note_notice(peer, false, now);
        for lane in Lane::ALL {
            assert!(!view.waives_unheld_penalty(&case(lane, &peer, &key), now));
        }
    }
}

/// How the payment check reads the chain, through the path a node takes
/// (`PaymentVerifier::verify_payment`) to a JSON-RPC provider on 127.0.0.1
/// that answers as the vault's `completedPayments` does: no test override
/// stands in for the vault here, as it does in the verifier's unit tests. A
/// release that reads payments another way (pooled, batched, cached) keeps
/// these behaviours or fails here. The provider answers one call or a JSON-RPC
/// batch of them. Where a node reads through two RPCs, both are providers on
/// 127.0.0.1, named through the `test-utils` setter for the list of RPCs
/// every payment read takes its RPCs from (`set_payment_rpc_urls_for_tests`).
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
    /// src/payment/settlement.rs). A node with one RPC reads twice in all.
    const READ_ROUNDS: usize = 2;
    const READ_BACKOFF: Duration = Duration::from_millis(500);

    /// A payment record that reads empty is read once more, 1.5 s later
    /// (`EMPTY_PAYMENT_RECHECK_DELAY` in src/payment/settlement.rs).
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
        quotes: Vec<(EncodedPeerId, PaymentQuote)>,
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

        /// The paid quote's rewards address.
        fn median_payee(&self) -> [u8; 20] {
            self.median.rewards_address.as_slice().try_into().unwrap()
        }

        /// The proof a client sends, naming `tx_hashes`.
        fn proof_naming(&self, tx_hashes: &[[u8; 32]]) -> Vec<u8> {
            serialize_single_node_proof(&PaymentProof {
                proof_of_payment: ProofOfPayment {
                    peer_quotes: self.quotes.clone(),
                },
                tx_hashes: tx_hashes
                    .iter()
                    .map(|tx_hash| evmlib::common::TxHash::from(*tx_hash))
                    .collect(),
                commitment_sidecars: Vec::new(),
            })
            .unwrap()
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
                peer_quotes: quotes.clone(),
            },
            tx_hashes: Vec::new(),
            commitment_sidecars: Vec::new(),
        })
        .unwrap();
        Bundle {
            xorname,
            proof,
            quotes,
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

    /// The RPCs a node reads payments through, as the verifier lists them:
    /// its own first, then, if that is one of Robinhood Chain's public RPCs,
    /// the chain's others; a node on rpc.ordofi.network, no longer one of
    /// them, still falls back to both, and no node falls back to it; any
    /// other RPC (an operator's own, a local chain) alone.
    #[test]
    fn payments_are_read_through_the_nodes_rpc_then_the_chains_other_public_ones() {
        const MAINNET: &str = "https://rpc.mainnet.chain.robinhood.com";
        const PUBLICNODE: &str = "https://robinhood-rpc.publicnode.com";
        const ORDOFI: &str = "https://rpc.ordofi.network";
        let urls = |rpc: &str| -> Vec<String> {
            PaymentVerifier::new(PaymentVerifierConfig {
                evm: EvmVerifierConfig {
                    network: evmlib::Network::new_custom(rpc, TOKEN, VAULT),
                },
                cache_capacity: 100,
                close_group_size: CLOSE_GROUP_SIZE,
                local_rewards_address: RewardsAddress::new([0xD1; 20]),
                price_floor: PriceFloorConfig::default(),
            })
            .payment_rpc_urls_for_tests()
            .iter()
            .map(|url| url.as_str().trim_end_matches('/').to_string())
            .collect()
        };
        assert_eq!(urls(MAINNET), [MAINNET, PUBLICNODE]);
        assert_eq!(urls(&format!("{PUBLICNODE}/")), [PUBLICNODE, MAINNET]);
        assert_eq!(urls(ORDOFI), [ORDOFI, PUBLICNODE, MAINNET]);
        assert_eq!(urls("http://127.0.0.1:8545"), ["http://127.0.0.1:8545"]);
    }

    /// What one payment check on a node with two RPCs came to: its outcome,
    /// and the calls its own RPC and the next one got.
    struct CheckedTwo {
        context: VerificationContext,
        outcome: Result<PaymentStatus, String>,
        own: Vec<Call>,
        next: Vec<Call>,
        took: Duration,
    }

    impl CheckedTwo {
        /// Every call, in the order made: which RPC it went to (0, the
        /// node's own; 1, the next), and when.
        fn calls(&self) -> Vec<(usize, Instant)> {
            let mut calls: Vec<(usize, Instant)> = self
                .own
                .iter()
                .map(|call| (0, call.at))
                .chain(self.next.iter().map(|call| (1, call.at)))
                .collect();
            calls.sort_by_key(|&(_, at)| at);
            calls
        }
    }

    /// Checks a fresh bundle's payment in `context` on a node that reads
    /// payments through two providers, its own (`own`) and then the next
    /// (`next`), each answering its `n`th call with `answer(n, bundle)`. The
    /// two are set as the node's RPC list (`set_payment_rpc_urls_for_tests`),
    /// which every payment read takes its RPCs from.
    async fn check_two<F, G>(
        seed: u8,
        context: VerificationContext,
        own_answer: F,
        next_answer: G,
    ) -> CheckedTwo
    where
        F: Fn(usize, &Bundle) -> Answer + Send + Sync + 'static,
        G: Fn(usize, &Bundle) -> Answer + Send + Sync + 'static,
    {
        let bundle = Arc::new(bundle(seed));
        let (for_own, for_next) = (Arc::clone(&bundle), Arc::clone(&bundle));
        let own = provider(move |n, _| own_answer(n, &for_own)).await;
        let next = provider(move |n, _| next_answer(n, &for_next)).await;
        let verifier = verifier(&own, &bundle);
        verifier.set_payment_rpc_urls_for_tests(vec![
            own.url.parse().unwrap(),
            next.url.parse().unwrap(),
        ]);
        let started = Instant::now();
        let outcome = verifier
            .verify_payment(&bundle.xorname, Some(&bundle.proof), context)
            .await
            .map_err(|e| e.to_string());
        let took = started.elapsed();
        for provider in [&own, &next] {
            let other_methods = provider.other_methods.lock().unwrap().clone();
            assert!(
                other_methods.is_empty(),
                "{context:?}: a provider was asked for more than completedPayments: {other_methods:?}"
            );
        }
        CheckedTwo {
            context,
            outcome,
            own: own.calls(),
            next: next.calls(),
            took,
        }
    }

    /// The same check in every context at once, each with its own bundle,
    /// providers and node.
    async fn check_two_everywhere<F, G>(seed: u8, own: F, next: G) -> Vec<CheckedTwo>
    where
        F: Fn(usize, &Bundle) -> Answer + Clone + Send + Sync + 'static,
        G: Fn(usize, &Bundle) -> Answer + Clone + Send + Sync + 'static,
    {
        futures::future::join_all(
            CONTEXTS
                .iter()
                .zip(seed..)
                .map(|(context, seed)| check_two(seed, *context, own.clone(), next.clone())),
        )
        .await
    }

    /// The node's own RPC fails, busy or by dropping the call, and the next
    /// one is asked at once, in the same round: the payment verifies after one
    /// call on each, with no pause between them.
    #[tokio::test]
    async fn a_failed_read_moves_on_to_the_next_rpc_at_once() {
        for failure in [Answer::Busy, Answer::Dropped] {
            let checked =
                check_two_everywhere(0x80, move |_, _| failure, |_, bundle| paid(bundle)).await;
            for c in checked {
                let case = format!("{:?}, the node's RPC {failure:?}", c.context);
                assert_eq!(c.outcome, Ok(PaymentStatus::PaymentVerified), "{case}");
                assert_eq!(
                    (c.own.len(), c.next.len()),
                    (1, 1),
                    "{case}: one call on each RPC: {:?}",
                    c.calls()
                );
                assert!(
                    c.own[0].at <= c.next[0].at
                        && c.next[0].at.duration_since(c.own[0].at) < READ_BACKOFF,
                    "{case}: the next RPC is asked at once, not after a round's pause: {:?}",
                    c.calls()
                );
            }
        }
    }

    /// What the node's own RPC answers: its name, the answer, and whether the
    /// payment then verifies.
    type AnsweredCase = (&'static str, fn(&Bundle) -> Answer, bool);

    /// An answer from the node's own RPC settles the read, whatever it says:
    /// the payment, a payment to another payee, or a wei too little. The next
    /// RPC, which would answer that the payment is there, is never asked.
    #[tokio::test]
    async fn an_answer_settles_the_read_and_the_next_rpc_is_not_asked() {
        let answers: [AnsweredCase; 3] = [
            ("the payment", paid, true),
            (
                "a payment to another payee",
                |bundle| Answer::Record([0xEE; 16], bundle.required * 10),
                false,
            ),
            (
                "a wei too little",
                |bundle| Answer::Record(bundle.payee_prefix(), bundle.required - 1),
                false,
            ),
        ];
        for (answer_name, answer, verified) in answers {
            let checked = check_two_everywhere(
                0x90,
                move |_, bundle| answer(bundle),
                |_, bundle| paid(bundle),
            )
            .await;
            for c in checked {
                let case = format!("{:?}, the node's RPC answers {answer_name}", c.context);
                if verified {
                    assert_eq!(c.outcome, Ok(PaymentStatus::PaymentVerified), "{case}");
                } else {
                    assert!(c.outcome.is_err(), "{case}: {:?}", c.outcome);
                }
                assert_eq!(c.own.len(), 1, "{case}: {:?}", c.calls());
                assert!(
                    c.next.is_empty(),
                    "{case}: the next RPC was asked: {:?}",
                    c.calls()
                );
            }
        }
    }

    /// An empty record from the node's own RPC, which is a block or two
    /// behind, ends the read it answers: the next RPC isn't asked at once.
    /// The record is read once more after the pause (`EMPTY_RECHECK`), a read
    /// of its own, which may go to either RPC, and that answer decides: the
    /// payment, or an empty record again, refused. Two calls in all.
    #[tokio::test]
    async fn an_empty_record_ends_its_read_and_is_read_again_after_the_pause() {
        for (later_name, later_paid) in [("holds the payment", true), ("is empty again", false)] {
            let own = move |n: usize, bundle: &Bundle| {
                if n > 1 && later_paid {
                    paid(bundle)
                } else {
                    Answer::Record([0u8; 16], 0)
                }
            };
            let next = move |_: usize, bundle: &Bundle| {
                if later_paid {
                    paid(bundle)
                } else {
                    Answer::Record([0u8; 16], 0)
                }
            };
            let checked = check_two_everywhere(0xA0, own, next).await;
            for c in checked {
                let case = format!("{:?}, the record read again {later_name}", c.context);
                if later_paid {
                    assert_eq!(c.outcome, Ok(PaymentStatus::PaymentVerified), "{case}");
                } else {
                    assert!(c.outcome.is_err(), "{case}: {:?}", c.outcome);
                    assert!(
                        c.took < EMPTY_RECHECK * 2,
                        "{case}: one pause in all, took {:?}",
                        c.took
                    );
                }
                let calls = c.calls();
                assert_eq!(
                    calls.len(),
                    2,
                    "{case}: read, then read once more: {calls:?}"
                );
                assert_eq!(
                    calls[0].0, 0,
                    "{case}: the first read asks the node's own RPC: {calls:?}"
                );
                assert!(
                    calls[1].1.duration_since(calls[0].1) >= EMPTY_RECHECK,
                    "{case}: nothing is asked until the pause is over: {calls:?}"
                );
            }
        }
    }

    /// A read that fails on every RPC asks each once a round, the node's own
    /// first and the next at once after it, in two rounds half a second apart,
    /// and then refuses the store.
    #[tokio::test]
    async fn a_read_failing_on_every_rpc_asks_each_once_a_round_then_refuses() {
        let checked = check_two_everywhere(0xB0, |_, _| Answer::Busy, |_, _| Answer::Busy).await;
        for c in checked {
            let context = c.context;
            assert!(c.outcome.is_err(), "{context:?}: {:?}", c.outcome);
            assert_eq!(
                (c.own.len(), c.next.len()),
                (READ_ROUNDS, READ_ROUNDS),
                "{context:?}: each RPC once in each of {READ_ROUNDS} rounds: {:?}",
                c.calls()
            );
            let calls = c.calls();
            assert_eq!(
                calls[0].0, 0,
                "{context:?}: the node's own RPC first: {calls:?}"
            );
            for (at, pair) in calls.windows(2).enumerate() {
                let gap = pair[1].1.duration_since(pair[0].1);
                if at % 2 == 0 {
                    assert!(
                        pair[0].0 != pair[1].0 && gap < READ_BACKOFF,
                        "{context:?}: within a round, the other RPC at once: {calls:?}"
                    );
                } else {
                    assert!(
                        gap >= READ_BACKOFF,
                        "{context:?}: the next round waits {READ_BACKOFF:?}: {calls:?}"
                    );
                }
            }
        }
    }

    /// A transaction a provider below knows: its receipt's status and the
    /// vault payment logs it shows, each `(emitter, payee, amount)` for the
    /// bundle's quote.
    struct Receipt {
        success: bool,
        logs: Vec<([u8; 20], [u8; 20], u128)>,
    }

    /// The vault's address, as its logs carry it.
    fn vault_address() -> [u8; 20] {
        hex::decode(VAULT.trim_start_matches("0x"))
            .unwrap()
            .try_into()
            .unwrap()
    }

    /// `keccak256("DataPaymentMade(address,uint256,bytes32)")`.
    const DATA_PAYMENT_MADE: &str =
        "0xf998960b1c6f0e0e89b7bbe6b6fbf3e03e6f08eee5b8430877d8adb8e149d580";

    /// The receipt of transaction `0x77…77` paying `bundle`'s quote as
    /// `receipt` says, as JSON.
    fn receipt_json(bundle: &Bundle, receipt: &Receipt) -> serde_json::Value {
        let logs: Vec<serde_json::Value> = receipt
            .logs
            .iter()
            .enumerate()
            .map(|(index, (emitter, payee, amount))| {
                serde_json::json!({
                    "address": format!("0x{}", hex::encode(emitter)),
                    "topics": [
                        DATA_PAYMENT_MADE,
                        format!("0x{}{}", "00".repeat(12), hex::encode(payee)),
                        format!("0x{amount:064x}"),
                        format!("0x{}", bundle.quote_hash()),
                    ],
                    "data": "0x",
                    "logIndex": format!("0x{index:x}"),
                })
            })
            .collect();
        serde_json::json!({
            "transactionHash": format!("0x{}", "77".repeat(32)),
            "status": if receipt.success { "0x1" } else { "0x0" },
            "blockNumber": "0x64",
            "logs": logs,
        })
    }

    /// A count of calls a provider got.
    type Counted = Arc<Mutex<usize>>;

    /// A provider that answers receipt reads with `receipt` and record reads
    /// with `record`, and counts each.
    async fn chain_provider(
        receipt: serde_json::Value,
        record: Answer,
    ) -> (String, Counted, Counted) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (receipts, records): (Counted, Counted) = Default::default();
        let (receipts_seen, records_seen) = (Arc::clone(&receipts), Arc::clone(&records));
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let (receipt, receipts, records) = (
                    receipt.clone(),
                    Arc::clone(&receipts_seen),
                    Arc::clone(&records_seen),
                );
                tokio::spawn(async move {
                    while let Some(body) = read_http_body(&mut socket).await {
                        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let calls = match &request {
                            serde_json::Value::Array(calls) => calls.clone(),
                            call => vec![call.clone()],
                        };
                        let answers: Vec<serde_json::Value> = calls
                            .iter()
                            .map(|call| {
                                let result = match call["method"].as_str().unwrap_or_default() {
                                    "eth_getTransactionReceipt" => {
                                        *receipts.lock().unwrap() += 1;
                                        receipt.clone()
                                    }
                                    "eth_call" => {
                                        *records.lock().unwrap() += 1;
                                        let Answer::Record(prefix, amount) = record else {
                                            panic!("the record answers");
                                        };
                                        serde_json::json!(record_words(prefix, amount))
                                    }
                                    other => panic!("asked for {other}"),
                                };
                                serde_json::json!({ "jsonrpc": "2.0", "id": call["id"],
                                    "result": result })
                            })
                            .collect();
                        let response = if request.is_array() {
                            serde_json::Value::from(answers)
                        } else {
                            answers.into_iter().next().unwrap()
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
        (url, receipts, records)
    }

    /// What a check of a proof naming one transaction came to, its receipt
    /// as `receipt` says and the vault's record as `record`: the outcome, and
    /// how many receipt and record reads it made.
    async fn check_naming(
        seed: u8,
        context: VerificationContext,
        receipt: fn(&Bundle) -> Receipt,
        record: fn(&Bundle) -> Answer,
    ) -> (Result<PaymentStatus, String>, usize, usize) {
        let bundle = bundle(seed);
        let (url, receipts, records) =
            chain_provider(receipt_json(&bundle, &receipt(&bundle)), record(&bundle)).await;
        let verifier = verifier(
            &Provider {
                url,
                calls: Arc::default(),
                other_methods: Arc::default(),
            },
            &bundle,
        );
        let outcome = verifier
            .verify_payment(
                &bundle.xorname,
                Some(&bundle.proof_naming(&[[0x77; 32]])),
                context,
            )
            .await
            .map_err(|e| e.to_string());
        let receipt_reads = *receipts.lock().unwrap();
        let record_reads = *records.lock().unwrap();
        (outcome, receipt_reads, record_reads)
    }

    /// The receipt of a payment of the quote to its payee, `times` the
    /// amount due.
    fn paying(bundle: &Bundle, times: u128) -> Receipt {
        Receipt {
            success: true,
            logs: vec![(
                vault_address(),
                bundle.median_payee(),
                bundle.required * times,
            )],
        }
    }

    /// What a receipt shows paid, and the receipt.
    type PaidCase = (&'static str, fn(&Bundle) -> Receipt);

    /// A receipt of a transaction the proof names, showing the vault's
    /// payment of the quote to its payee, of at least the amount due,
    /// settles it with no read of the record, in every context.
    #[tokio::test]
    async fn a_receipt_showing_the_quote_paid_settles_it_without_a_record_read() {
        let receipts: [PaidCase; 2] = [
            ("the amount due", |bundle| paying(bundle, 1)),
            ("10 x the amount due", |bundle| paying(bundle, 10)),
        ];
        for (paid, receipt) in receipts {
            for context in CONTEXTS {
                let (outcome, receipt_reads, record_reads) = check_naming(
                    0xC0,
                    context,
                    receipt,
                    // Read, the record would refuse: nothing is read.
                    |_| Answer::Record([0u8; 16], 0),
                )
                .await;
                let case = format!("{context:?}, {paid}");
                assert_eq!(outcome, Ok(PaymentStatus::PaymentVerified), "{case}");
                assert_eq!((receipt_reads, record_reads), (1, 0), "{case}");
            }
        }
    }

    /// What the receipt names, what the record holds, and whether the
    /// payment then verifies, with how many reads of the record.
    type FactCase = (
        &'static str,
        fn(&Bundle) -> Receipt,
        fn(&Bundle) -> Answer,
        bool,
        usize,
    );

    /// A receipt that doesn't pay the quote in full to its payee, of a
    /// transaction that reverted, or with another contract's log shaped as
    /// the vault's, settles nothing: the record read decides, and only it
    /// refuses. A record that pays the quote verifies it; one that doesn't
    /// is refused, an empty one after it is read once more.
    #[tokio::test]
    async fn a_receipt_that_doesnt_pay_the_quote_leaves_the_record_read_to_decide() {
        let cases: [FactCase; 4] = [
            (
                "a payment to another payee",
                |bundle| Receipt {
                    success: true,
                    logs: vec![(vault_address(), [0xEE; 20], bundle.required * 10)],
                },
                |bundle| Answer::Record([0xEE; 16], bundle.required * 10),
                false,
                1,
            ),
            (
                "a wei too little",
                |bundle| Receipt {
                    success: true,
                    logs: vec![(vault_address(), bundle.median_payee(), bundle.required - 1)],
                },
                |bundle| Answer::Record(bundle.payee_prefix(), bundle.required - 1),
                false,
                1,
            ),
            (
                "a reverted transaction showing a payment",
                |bundle| Receipt {
                    success: false,
                    logs: vec![(vault_address(), bundle.median_payee(), bundle.required)],
                },
                |_| Answer::Record([0u8; 16], 0),
                false,
                2,
            ),
            (
                "another contract's log, the quote paid elsewhere",
                |bundle| Receipt {
                    success: true,
                    logs: vec![([0x99; 20], bundle.median_payee(), bundle.required)],
                },
                |bundle| Answer::Record(bundle.payee_prefix(), bundle.required),
                true,
                1,
            ),
        ];
        for (name, receipt, record, verified, reads) in cases {
            for context in CONTEXTS {
                let (outcome, receipt_reads, record_reads) =
                    check_naming(0xD0, context, receipt, record).await;
                let case = format!("{context:?}, {name}");
                if verified {
                    assert_eq!(outcome, Ok(PaymentStatus::PaymentVerified), "{case}");
                } else {
                    assert!(outcome.is_err(), "{case}: {outcome:?}");
                }
                assert_eq!(receipt_reads, 1, "{case}");
                assert_eq!(record_reads, reads, "{case}: the record read decides");
            }
        }
    }

    /// A store's whole payment check ends before a released client stops
    /// waiting for the store's answer (its `STORE_RESPONSE_TIMEOUT`, 10 s), so
    /// the node answers with its payment error rather than leave the client
    /// to time out, and the reads still in flight fill the payment cache for
    /// the client's next try.
    #[test]
    fn a_stores_payment_check_ends_inside_the_clients_wait() {
        use ant_node::payment::{CLIENT_STORE_WAIT, PUT_PAYMENT_DEADLINE};
        assert_eq!(CLIENT_STORE_WAIT, Duration::from_secs(10));
        assert_eq!(PUT_PAYMENT_DEADLINE, Duration::from_secs(7));
        assert!(PUT_PAYMENT_DEADLINE < CLIENT_STORE_WAIT);
    }
}

/// The payment proofs nodes pass each other, and the quotes inside them, as
/// ds.15 encodes them. A proof travels in fresh replication offers, paid-list
/// notices and fresh pointer offers (`proof_of_payment`), which
/// `tests/wire_format.rs` pins as opaque bytes; every node that receives one
/// decodes it, hashes each quote, and reads the vault by those hashes. A
/// vendored ant-protocol change or an evmlib bump that moves a field, or
/// hashes a quote differently, fails here: nodes up to ds.15 would no longer
/// decode, or would find unpaid, the proofs a newer node sends them.
///
/// The keys and signatures are short stand-ins, and the peer ids and
/// transaction hash made-up bytes: only the encoding is under test. On a
/// mismatch the test prints the bytes as they are now, for review.
mod payment_encodings {
    use ant_node::payment::proof::deserialize_single_node_proof;
    use ant_node::payment::{serialize_single_node_proof, PaymentProof};
    use evmlib::common::{Amount, TxHash};
    use evmlib::{EncodedPeerId, PaymentQuote, ProofOfPayment, RewardsAddress};
    use std::fmt::Write as _;
    use std::time::{Duration, SystemTime};

    /// A quote, with its fields made from `byte`: one bound to a storage
    /// commitment (its key count and pin), as a node with a commitment
    /// quotes, or a baseline quote, as a node without one does. Its time is a
    /// whole number of 100 ns, the finest a Windows clock keeps, so the bytes
    /// are the same on every platform.
    fn quote(byte: u8, committed: bool) -> PaymentQuote {
        PaymentQuote {
            content: xor_name::XorName([byte; 32]),
            timestamp: SystemTime::UNIX_EPOCH + Duration::new(1_792_000_000, 123_456_700),
            price: Amount::from(3_200_000_000_000_000_u64 + u64::from(byte)),
            rewards_address: RewardsAddress::new([byte + 1; 20]),
            pub_key: vec![byte + 2; 3],
            signature: vec![byte + 3; 4],
            committed_key_count: if committed { 70_000 } else { 0 },
            commitment_pin: committed.then_some([byte + 4; 32]),
        }
    }

    /// A single-node proof, as a client sends it with a PUT and nodes pass it
    /// on: here two of the close group's quotes, enough to pin how the list
    /// is laid out, with the payment's transaction and the commitment the
    /// first quote is bound to (an opaque sidecar).
    fn proof() -> PaymentProof {
        PaymentProof {
            proof_of_payment: ProofOfPayment {
                peer_quotes: vec![
                    (EncodedPeerId::new([0x51; 32]), quote(0x10, true)),
                    (EncodedPeerId::new([0x52; 32]), quote(0x20, false)),
                ],
            },
            tx_hashes: vec![TxHash::from([0x7A; 32])],
            commitment_sidecars: vec![vec![0xC0, 0xC1, 0xC2]],
        }
    }

    /// `proof()`, as `serialize_single_node_proof` writes it: the single-node
    /// tag (01), then the proof in `MessagePack`, each struct an array of its
    /// fields in order: the quotes with their peer ids, the transaction
    /// hashes, the commitment sidecars.
    const PINNED_PROOF: &str = "0193919292dc0020515151515151515151515151515151515151515151515151515151515151515198dc0020101010101010101010101010101010101010101010101010101010101010101092ce6acfc000ce075bccbcc420000000000000000000000000000000000000000000000000000b5e620f480010c4141111111111111111111111111111111111111111931212129413131313ce00011170dc0020141414141414141414141414141414141414141414141414141414141414141492dc0020525252525252525252525252525252525252525252525252525252525252525298dc0020202020202020202020202020202020202020202020202020202020202020202092ce6acfc000ce075bccbcc420000000000000000000000000000000000000000000000000000b5e620f480020c414212121212121212121212121212121212121212193222222942323232300c091c4207a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a9193ccc0ccc1ccc2";

    /// Each quote of `proof()`, as `MessagePack` writes it (an array of its 8
    /// fields; the time as seconds and nanoseconds, the price as 32 bytes, an
    /// absent pin as nil), and its hash: the key the vault records its
    /// payment under.
    const PINNED_QUOTES: &[(&str, &str, &str)] = &[
        (
            "committed",
            "98dc0020101010101010101010101010101010101010101010101010101010101010101092ce6acfc000ce075bccbcc420000000000000000000000000000000000000000000000000000b5e620f480010c4141111111111111111111111111111111111111111931212129413131313ce00011170dc00201414141414141414141414141414141414141414141414141414141414141414",
            "0807c76ec29a88205006aebd8b2f8e361ff83fc93343c4547e642e8aef6f6e90",
        ),
        (
            "baseline",
            "98dc0020202020202020202020202020202020202020202020202020202020202020202092ce6acfc000ce075bccbcc420000000000000000000000000000000000000000000000000000b5e620f480020c414212121212121212121212121212121212121212193222222942323232300c0",
            "4f79e437123b8ad4b75fd1704ed1ba0effffebd62c91d97e119a618decec8640",
        ),
    ];

    #[test]
    fn a_payment_proof_keeps_its_bytes() {
        let sample = proof();
        let bytes = hex::encode(serialize_single_node_proof(&sample).unwrap());
        assert!(
            bytes == PINNED_PROOF,
            "\nA single-node payment proof no longer matches its pinned bytes in \
             tests/fork_pins.rs. Nodes up to ds.15 decode these bytes in every fresh offer \
             and paid notice. Its bytes now:\n\n{bytes}\n"
        );

        // The pinned bytes decode to the same proof, and encode back to them.
        let pinned = hex::decode(PINNED_PROOF).unwrap();
        let decoded = deserialize_single_node_proof(&pinned)
            .unwrap_or_else(|e| panic!("the pinned proof doesn't decode: {e}"));
        assert_eq!(decoded.proof_of_payment, sample.proof_of_payment);
        assert_eq!(decoded.tx_hashes, sample.tx_hashes);
        assert_eq!(decoded.commitment_sidecars, sample.commitment_sidecars);
        assert_eq!(serialize_single_node_proof(&decoded).unwrap(), pinned);
    }

    #[test]
    fn a_payment_quote_keeps_its_bytes_and_its_hash() {
        let samples = [
            ("committed", quote(0x10, true)),
            ("baseline", quote(0x20, false)),
        ];
        let found: Vec<(String, String, String)> = samples
            .iter()
            .map(|(name, quote)| {
                (
                    (*name).to_string(),
                    hex::encode(rmp_serde::to_vec(quote).unwrap()),
                    hex::encode(quote.hash()),
                )
            })
            .collect();
        let pinned: Vec<(String, String, String)> = PINNED_QUOTES
            .iter()
            .map(|(name, bytes, hash)| {
                (
                    (*name).to_string(),
                    (*bytes).to_string(),
                    (*hash).to_string(),
                )
            })
            .collect();
        let mut table = String::new();
        for (name, bytes, hash) in &found {
            writeln!(
                table,
                "    (\n        \"{name}\",\n        \"{bytes}\",\n        \"{hash}\",\n    ),"
            )
            .unwrap();
        }
        assert!(
            found == pinned,
            "\nA payment quote no longer matches its pinned bytes or hash in tests/fork_pins.rs. \
             Nodes up to ds.15 decode these bytes in every proof, and read the vault by the \
             hash. Each quote's bytes and hash now:\n\n{table}"
        );

        // The pinned bytes decode to the same quote, which hashes as pinned.
        for ((name, quote), (_, bytes, hash)) in samples.iter().zip(PINNED_QUOTES) {
            let decoded: PaymentQuote = rmp_serde::from_slice(&hex::decode(bytes).unwrap())
                .unwrap_or_else(|e| panic!("{name}: the pinned quote doesn't decode: {e}"));
            assert_eq!(&decoded, quote, "{name}");
            assert_eq!(hex::encode(decoded.hash()), *hash, "{name}");
        }
    }
}

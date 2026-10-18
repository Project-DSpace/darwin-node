//! Values this fork sets on purpose, each of which a merge from upstream could
//! quietly put back: the repository nodes upgrade from and the key its
//! releases are signed with, the bootstrap peers that ship (none), the
//! allowance of new browser sessions per source, and the suspended penalty for
//! not holding a close-group chunk.
//!
//! Upstream has no such file, so a merge can't drop it while resolving a
//! conflict. Each test fails if its value changes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ant_node::config::{BootstrapPeersConfig, NodeConfig, UpgradeConfig, WebRtcDirectConfig};
use ant_node::replication::config::{
    apply_close_group_storage_penalty_policy, close_group_storage_penalty_suspended,
    set_close_group_storage_penalty_suspended, SUSPEND_CLOSE_GROUP_STORAGE_PENALTY_ENV,
};
use serial_test::serial;
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

/// Serial, and the variable put back afterwards: the switch and the
/// environment are process-wide.
#[test]
#[serial]
fn a_started_node_withholds_the_unheld_chunk_penalty() {
    let saved = std::env::var_os(SUSPEND_CLOSE_GROUP_STORAGE_PENALTY_ENV);
    std::env::remove_var(SUSPEND_CLOSE_GROUP_STORAGE_PENALTY_ENV);
    // Start from the other state, so the result is the policy's doing.
    set_close_group_storage_penalty_suspended(false);
    apply_close_group_storage_penalty_policy();
    let suspended = close_group_storage_penalty_suspended();
    if let Some(saved) = saved {
        std::env::set_var(SUSPEND_CLOSE_GROUP_STORAGE_PENALTY_ENV, saved);
    }
    assert!(
        suspended,
        "with {SUSPEND_CLOSE_GROUP_STORAGE_PENALTY_ENV} unset, a node must withhold the \
         penalty for not holding a close-group chunk while this fork keeps the LMDB migration"
    );
}

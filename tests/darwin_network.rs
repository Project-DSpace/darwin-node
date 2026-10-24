//! The storage-node image's network settings, checked against the binary it
//! packages.
//!
//! The image (docker/Dockerfile) sets the payment token and vault, the RPCs,
//! the bootstrap servers and the network mode, and docker/entrypoint.sh hands
//! them to every node in `ANT_*` variables. The binary reads those variables
//! only by name, so a name that changed in a merge would leave nodes on
//! upstream's defaults (Arbitrum One, Autonomi's contracts) with nothing
//! failing. Upstream has no such file, so a merge can't drop it while
//! resolving a conflict. The vault's deployment block the binary has built
//! in for the image's token and vault is checked against the image's too.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;

/// DARWIN, the token nodes are paid in, on Robinhood Chain.
const DARWIN_TOKEN: &str = "0x511f76A3De41F000EC11EA4fD212fb3587c6cef3";
/// The payment vault storage payments go through.
const DARWIN_VAULT: &str = "0xE0f627986c0438De0E314B644f3C8aA21D3f4f6B";
/// The block that vault was deployed in: no payment to it is in an earlier
/// one.
const DARWIN_VAULT_DEPLOYED: &str = "81897121";
/// Robinhood Chain's own public RPC.
const ROBINHOOD_RPC: &str = "https://rpc.mainnet.chain.robinhood.com";
/// The network's three bootstrap servers.
const BOOTSTRAP_PEERS: [&str; 3] = [
    "142.93.32.90:10000",
    "165.22.193.90:10000",
    "142.93.96.232:10000",
];

fn repo_text(path: &str) -> String {
    let full = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

/// The Dockerfile's instructions, one per entry, with continued lines joined
/// and comment lines left out.
fn dockerfile_instructions(dockerfile: &str) -> Vec<String> {
    let mut instructions = Vec::new();
    let mut current = String::new();
    for line in dockerfile.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || (trimmed.is_empty() && current.is_empty()) {
            continue;
        }
        let (text, continued) = trimmed
            .strip_suffix('\\')
            .map_or((trimmed, false), |text| (text, true));
        current.push_str(text.trim_end());
        current.push(' ');
        if !continued {
            instructions.push(current.trim().to_string());
            current.clear();
        }
    }
    if !current.trim().is_empty() {
        instructions.push(current.trim().to_string());
    }
    instructions
}

/// Every variable the Dockerfile's `ENV` instructions set, with its value.
/// Values may be double-quoted.
fn dockerfile_env(dockerfile: &str) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    for instruction in dockerfile_instructions(dockerfile) {
        let Some(assignments) = instruction.strip_prefix("ENV ") else {
            continue;
        };
        let mut rest = assignments.trim_start();
        while !rest.is_empty() {
            let (name, after) = rest
                .split_once('=')
                .unwrap_or_else(|| panic!("ENV without '=': {rest}"));
            let (value, after) = after.strip_prefix('"').map_or_else(
                || after.split_at(after.find(char::is_whitespace).unwrap_or(after.len())),
                |quoted| {
                    quoted
                        .split_once('"')
                        .unwrap_or_else(|| panic!("unclosed quote: {after}"))
                },
            );
            env.insert(name.trim().to_string(), value.to_string());
            rest = after.trim_start();
        }
    }
    env
}

/// Every `ANT_*` name in `text`.
fn ant_variable_names(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("ANT_") {
        // A name starts here only if no letter, digit or underscore comes
        // before it, so RELEVANT_X is not one.
        let starts_a_name = rest[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
        let length = rest[at..]
            .find(|c: char| !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
            .unwrap_or(rest.len() - at);
        let name = &rest[at..at + length];
        if starts_a_name && name.len() > "ANT_".len() && !names.iter().any(|n| n == name) {
            names.push(name.to_string());
        }
        rest = &rest[at + length..];
    }
    names
}

#[test]
fn the_image_runs_the_darwin_network() {
    let env = dockerfile_env(&repo_text("docker/Dockerfile"));
    let get = |name: &str| {
        env.get(name)
            .unwrap_or_else(|| panic!("the Dockerfile sets no {name}: {env:?}"))
            .as_str()
    };

    assert!(
        get("ANT_EVM_PAYMENT_TOKEN").eq_ignore_ascii_case(DARWIN_TOKEN),
        "ANT_EVM_PAYMENT_TOKEN={}",
        get("ANT_EVM_PAYMENT_TOKEN")
    );
    assert!(
        get("ANT_EVM_PAYMENT_VAULT").eq_ignore_ascii_case(DARWIN_VAULT),
        "ANT_EVM_PAYMENT_VAULT={}",
        get("ANT_EVM_PAYMENT_VAULT")
    );
    assert_eq!(
        get("ANT_EVM_PAYMENT_VAULT_DEPLOYMENT_BLOCK"),
        DARWIN_VAULT_DEPLOYED
    );
    // The image's token and vault are the network a node takes as the
    // production one, whose vault's deployment block it has built in: the
    // same block, so a node that upgrades its binary alone, under an image
    // that doesn't give it, has it too.
    let without_the_block = ant_node::config::EvmNetworkConfig::Custom {
        rpc_url: ROBINHOOD_RPC.to_string(),
        payment_token_address: get("ANT_EVM_PAYMENT_TOKEN").to_string(),
        payment_vault_address: get("ANT_EVM_PAYMENT_VAULT").to_string(),
        payment_vault_deployment_block: None,
    };
    assert_eq!(
        without_the_block.vault_deployment_block(),
        ant_node::config::VaultDeploymentBlock::BuiltIn(DARWIN_VAULT_DEPLOYED.parse().unwrap())
    );
    let rpcs: Vec<&str> = get("RPC_URLS").split_whitespace().collect();
    assert!(rpcs.contains(&ROBINHOOD_RPC), "RPC_URLS={rpcs:?}");
    let peers: Vec<&str> = get("BOOTSTRAP_PEERS").split_whitespace().collect();
    assert_eq!(peers, BOOTSTRAP_PEERS);
    for peer in &peers {
        assert!(peer.parse::<SocketAddr>().is_ok(), "{peer}");
    }
    assert_eq!(get("ANT_NETWORK_MODE"), "production");
    assert_eq!(get("IPV4_ONLY"), "true");
}

/// Variables the image gives every node before the node reads them, each
/// with what it is for. A node ignores a variable it doesn't read. Once the
/// node reads one, the test below fails until it is taken off this list, so
/// the test then checks it as it checks the rest.
const GIVEN_BEFORE_THE_NODE_READS_THEM: &[(&str, &str)] = &[(
    "ANT_IMAGE_VERSION",
    "the image marker: the commit the image was built from, so a node can \
     tell that its node manager deletes no node's data",
)];

#[test]
fn every_variable_the_image_sets_is_one_the_node_reads() {
    // The ARG IMAGE_VERSION is the image marker, which the entrypoint passes
    // on as ANT_IMAGE_VERSION; only ENV instructions and the entrypoint's
    // variables reach the nodes.
    let image_env = dockerfile_env(&repo_text("docker/Dockerfile"));
    let mut names: Vec<String> = image_env
        .keys()
        .filter(|name| name.starts_with("ANT_"))
        .cloned()
        .collect();
    for name in ant_variable_names(&repo_text("docker/entrypoint.sh")) {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    // The payment settings at least, so an empty list can't pass.
    for expected in [
        "ANT_EVM_RPC_URL",
        "ANT_EVM_PAYMENT_TOKEN",
        "ANT_EVM_PAYMENT_VAULT",
        "ANT_EVM_PAYMENT_VAULT_DEPLOYMENT_BLOCK",
    ] {
        assert!(names.iter().any(|n| n == expected), "{expected}: {names:?}");
    }

    let output = Command::new(env!("CARGO_BIN_EXE_ant-node"))
        .arg("--help")
        .output()
        .expect("run ant-node --help");
    assert!(output.status.success(), "ant-node --help: {output:?}");
    let help = String::from_utf8_lossy(&output.stdout);
    let reads = |name: &str| help.contains(&format!("[env: {name}="));
    for (name, what) in GIVEN_BEFORE_THE_NODE_READS_THEM {
        assert!(
            names.iter().any(|n| n == name),
            "the image no longer gives {name} ({what}): take it off GIVEN_BEFORE_THE_NODE_READS_THEM"
        );
        assert!(
            !reads(name),
            "ant-node reads {name} now: take it off GIVEN_BEFORE_THE_NODE_READS_THEM"
        );
    }
    let unread: Vec<&String> = names
        .iter()
        .filter(|name| !reads(name))
        .filter(|name| {
            !GIVEN_BEFORE_THE_NODE_READS_THEM
                .iter()
                .any(|(given, _)| given == name)
        })
        .collect();
    assert!(
        unread.is_empty(),
        "the image sets {unread:?}, which ant-node doesn't read:\n{help}"
    );
}

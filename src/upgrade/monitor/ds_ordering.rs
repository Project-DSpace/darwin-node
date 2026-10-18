//! This fork's releases are versioned `<upstream version>+ds.<build>`, and a
//! node upgrades between builds of one upstream version by their build
//! numbers. semver's `Version` orders build metadata, numeric parts as
//! numbers, though the SemVer specification gives it no precedence; these
//! tests fail if the monitor stops doing so, for example if a merge compares
//! versions by precedence alone, which would leave every node on its build.
//!
//! A file of its own, which upstream doesn't have, so a merge can't drop it
//! while resolving a conflict in the monitor's tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::{
    select_upgrade_from_releases, version_from_tag, version_matches_channel, Asset, GitHubRelease,
};
use crate::config::UpgradeChannel;
use semver::Version;

/// A published release with a binary for this platform and its signature.
/// On 64-bit x86 Linux, the archive this fork's release workflow publishes.
fn release(tag: &str) -> GitHubRelease {
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;
    let binary = if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        "ant-node-cli-linux-x64.tar.gz".to_string()
    } else if cfg!(windows) {
        format!("ant-node-{arch}-{os}.exe")
    } else {
        format!("ant-node-{arch}-{os}")
    };
    GitHubRelease {
        tag_name: tag.to_string(),
        name: tag.to_string(),
        body: format!("notes for {tag}"),
        prerelease: false,
        assets: vec![
            Asset {
                name: binary.clone(),
                browser_download_url: format!("https://example.com/{tag}"),
            },
            Asset {
                name: format!("{binary}.sig"),
                browser_download_url: format!("https://example.com/{tag}.sig"),
            },
        ],
    }
}

/// The version a node running `current` upgrades to, given `tags`.
fn upgrade(current: &str, tags: &[&str], channel: UpgradeChannel) -> Option<Version> {
    let current = Version::parse(current).unwrap();
    let releases: Vec<GitHubRelease> = tags.iter().map(|tag| release(tag)).collect();
    select_upgrade_from_releases(&releases, &current, channel, None).map(|info| info.version)
}

fn version(text: &str) -> Version {
    Version::parse(text).unwrap()
}

#[test]
fn ds_builds_of_one_upstream_version_upgrade_in_order() {
    let ds13 = version_from_tag("v0.23.0+ds.13").expect("a +ds tag parses");
    assert_eq!((ds13.major, ds13.minor, ds13.patch), (0, 23, 0));
    assert!(ds13.pre.is_empty(), "a build is not a pre-release");
    assert_eq!(ds13.build.as_str(), "ds.13");
    assert_eq!(ds13, version("0.23.0+ds.13"));
    assert!(version("0.23.1+ds.1") > version("0.23.0+ds.99"));

    for channel in [UpgradeChannel::Stable, UpgradeChannel::Beta] {
        assert!(version_matches_channel(&ds13, channel), "{channel:?}");

        assert_eq!(
            upgrade("0.23.0+ds.12", &["v0.23.0+ds.13"], channel),
            Some(version("0.23.0+ds.13")),
            "{channel:?}: ds.12 takes ds.13"
        );
        assert_eq!(
            upgrade("0.23.0+ds.9", &["v0.23.0+ds.10"], channel),
            Some(version("0.23.0+ds.10")),
            "{channel:?}: build numbers compare as numbers, not as text"
        );
        assert_eq!(
            upgrade(
                "0.23.0+ds.9",
                &[
                    "v0.23.0+ds.11",
                    "v0.23.0+ds.13",
                    "v0.21.0+ds.11",
                    "v0.23.0+ds.10",
                    "v0.23.0+ds.12",
                ],
                channel
            ),
            Some(version("0.23.0+ds.13")),
            "{channel:?}: the newest of several, in any order"
        );
        assert_eq!(
            upgrade(
                "0.23.0+ds.13",
                &[
                    "v0.23.0+ds.12",
                    "v0.23.0+ds.13",
                    "v0.21.0+ds.11",
                    "v0.23.0",
                    "v0.22.9+ds.99",
                ],
                channel
            ),
            None,
            "{channel:?}: no downgrade, and no reinstall of the running build"
        );
        assert_eq!(
            upgrade("0.23.0+ds.99", &["v0.23.0+ds.100", "v0.23.1+ds.1"], channel),
            Some(version("0.23.1+ds.1")),
            "{channel:?}: a newer upstream version beats any build of an older one"
        );
    }
}

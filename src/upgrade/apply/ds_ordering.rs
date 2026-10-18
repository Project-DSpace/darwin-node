//! The last guard before a node replaces its own binary: it applies only a
//! version newer than the one it runs, with this fork's `+ds.<build>` builds
//! ordered by build number. A file of its own, which upstream doesn't have, so
//! a merge can't drop it while resolving a conflict in the upgrader's tests.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::AutoApplyUpgrader;
use crate::upgrade::{UpgradeInfo, UpgradeResult};
use semver::Version;

/// A URL no download can use, so the offer fails without touching a network.
const BAD_URL: &str = "not a url";

fn upgrader_at(version: &str) -> AutoApplyUpgrader {
    let mut upgrader = AutoApplyUpgrader::new();
    upgrader.current_version = Version::parse(version).unwrap();
    upgrader
}

fn offer(version: &str) -> UpgradeInfo {
    UpgradeInfo {
        version: Version::parse(version).unwrap(),
        download_url: BAD_URL.to_string(),
        signature_url: BAD_URL.to_string(),
        release_notes: String::new(),
    }
}

#[tokio::test]
async fn a_node_at_ds_12_applies_only_a_later_build() {
    let upgrader = upgrader_at("0.23.0+ds.12");
    for older in ["0.23.0+ds.12", "0.23.0+ds.11", "0.21.0+ds.11"] {
        let result = upgrader.apply_upgrade(&offer(older)).await.unwrap();
        assert!(
            matches!(result, UpgradeResult::NoUpgrade),
            "{older}: {result:?}"
        );
    }

    // ds.13 gets past the guard: the download is tried, fails, and nothing is
    // replaced or backed up.
    let binary = AutoApplyUpgrader::current_binary_path().unwrap();
    let backup = binary.with_file_name(format!(
        "{}.backup",
        binary.file_name().unwrap().to_string_lossy()
    ));
    let had_backup = backup.exists();
    let before = std::fs::metadata(&binary).unwrap();

    let result = upgrader
        .apply_upgrade(&offer("0.23.0+ds.13"))
        .await
        .unwrap();
    match result {
        UpgradeResult::RolledBack { reason } => {
            assert!(reason.contains("Download failed"), "{reason}");
        }
        other => panic!("ds.13 must reach the download and fail there: {other:?}"),
    }

    let after = std::fs::metadata(&binary).unwrap();
    assert_eq!(before.len(), after.len());
    assert_eq!(before.modified().ok(), after.modified().ok());
    assert_eq!(backup.exists(), had_backup, "no backup was made");
}

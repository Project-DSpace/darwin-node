//! After a clean start, a node prunes the shared upgrade cache to the version
//! it runs, the one before, and any newer one. A file of its own, which
//! upstream doesn't have, so a merge can't drop it while resolving a conflict
//! in the cache's tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::{cached_version, BinaryCache};
use semver::Version;
use std::fs;
use std::path::Path;

fn version(text: &str) -> Version {
    Version::parse(text).unwrap()
}

/// The files the cache keeps for `version`.
fn cache_files(dir: &Path, version: &str) {
    for suffix in ["archive", "sig", "meta.json"] {
        fs::write(dir.join(format!("ant-node-{version}.{suffix}")), version).unwrap();
    }
}

fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[test]
fn a_cache_file_names_its_version() {
    for (name, expected) in [
        ("ant-node-0.23.0+ds.15.archive", Some("0.23.0+ds.15")),
        ("ant-node-0.23.0+ds.15.sig", Some("0.23.0+ds.15")),
        ("ant-node-0.23.0+ds.15.meta.json", Some("0.23.0+ds.15")),
        (".ant-node-0.23.0+ds.15.archive.tmp", Some("0.23.0+ds.15")),
        (".ant-node-0.23.0+ds.15.meta.tmp", Some("0.23.0+ds.15")),
        ("ant-node-1.2.0-beta.1.archive", Some("1.2.0-beta.1")),
        ("download.lock", None),
        ("releases.json", None),
        ("ant-node-not-a-version.archive", None),
        ("ant-node-0.23.0+ds.15.exe", None),
    ] {
        assert_eq!(cached_version(name), expected.map(version), "{name}");
    }
}

#[test]
fn the_running_version_the_one_before_and_newer_ones_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    for v in [
        "0.23.0+ds.13",
        "0.23.0+ds.14",
        "0.23.0+ds.15",
        "0.23.0+ds.16",
    ] {
        cache_files(dir.path(), v);
    }
    fs::write(
        dir.path().join(".ant-node-0.23.0+ds.12.archive.tmp"),
        "left",
    )
    .unwrap();
    fs::write(dir.path().join("download.lock"), "").unwrap();
    fs::write(dir.path().join("releases.json"), "{}").unwrap();
    let cache = BinaryCache::new(dir.path().to_path_buf());

    let deleted = cache.prune(&version("0.23.0+ds.15")).unwrap();
    assert_eq!(deleted, [version("0.23.0+ds.12"), version("0.23.0+ds.13")]);
    let left = names(dir.path());
    assert!(left
        .iter()
        .all(|name| !name.contains("ds.12") && !name.contains("ds.13")));
    for v in ["0.23.0+ds.14", "0.23.0+ds.15", "0.23.0+ds.16"] {
        for suffix in ["archive", "sig", "meta.json"] {
            assert!(
                left.contains(&format!("ant-node-{v}.{suffix}")),
                "{v}.{suffix}"
            );
        }
    }
    assert!(left.contains(&"download.lock".to_string()));
    assert!(left.contains(&"releases.json".to_string()));

    // A second prune finds nothing more to delete.
    assert_eq!(cache.prune(&version("0.23.0+ds.15")).unwrap(), []);
}

#[test]
fn without_the_running_version_cached_the_newest_older_one_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    for v in ["0.22.0+ds.12", "0.23.0+ds.13", "0.23.0+ds.14"] {
        cache_files(dir.path(), v);
    }
    let cache = BinaryCache::new(dir.path().to_path_buf());
    let deleted = cache.prune(&version("0.23.0+ds.15")).unwrap();
    assert_eq!(deleted, [version("0.22.0+ds.12"), version("0.23.0+ds.13")]);
    assert_eq!(
        names(dir.path()).len(),
        3 + 1,
        "ds.14's three files and the lock"
    );
}

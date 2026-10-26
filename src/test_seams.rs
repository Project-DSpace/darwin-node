//! Test-only seams: values a test sets from outside a node's process.
//!
//! They reach states a short run on a roomy disk never does. The
//! mixed-version harness (`scripts/test_mixed_versions.py`) runs nodes of two
//! releases as separate processes and sets them through the environment.
//!
//! This module is compiled only into builds with the `test-utils` feature (and
//! into the crate's own unit tests). No release is built with that feature, so
//! an operator can't set a seam: the Build workflow checks that the node it
//! builds holds no seam's variable. Every seam's variable starts with
//! [`PREFIX`].
//!
//! A seam for a value the node doesn't have yet is added here in the change
//! that brings the value, with the harness's runs that set it: paged
//! neighbour sync's page size, so a run's few records take more than one
//! page; the release a node announces, so this tree's nodes, which announce
//! the last release's version until a release is cut, name the release that
//! reads pages; and the requests a node admits at once, so a few nodes push a
//! responder past its limit; how long a "full" notice stays fresh
//! ([`FULL_FRESHNESS_SECS`]), for a run in which a node's mark as full must
//! lapse within the run, and the first release a full node tells it is full
//! ([`FULL_NOTICE_FROM_RELEASE`]), which the harness's `full` run sets so that
//! nodes of a tree between releases tell each other; the neighbour-sync
//! interval ([`NEIGHBOR_SYNC_SECS`]) and the out-of-range wait
//! ([`PRUNE_HYSTERESIS_SECS`]), for a run in which erasure in two phases must
//! go ahead within the run. Each is compiled only with `test-utils` and named
//! under [`PREFIX`], so the Build workflow's check covers it, and the harness
//! refuses a run that needs a seam with a binary that lacks it, as it refuses
//! the `full` run without [`FREE_SPACE_FILE`].

use std::io;
use std::path::Path;

/// Every seam's environment variable starts with this.
pub const PREFIX: &str = "ANT_TEST_SEAM_";

/// The bytes of keys one neighbour-sync page carries.
///
/// In place of
/// [`NEIGHBOR_SYNC_PAGE_BYTES`](crate::replication::config::NEIGHBOR_SYNC_PAGE_BYTES):
/// a whole number, at least one key's 32 bytes. A small page makes the few
/// records of a test run take more than one page. A value that isn't such a
/// number is ignored.
pub const SYNC_PAGE_BYTES: &str = "ANT_TEST_SEAM_SYNC_PAGE_BYTES";

/// The first release whose nodes this node pages its hints to.
///
/// Its `+ds.<n>` build number, in place of
/// [`NEIGHBOR_SYNC_PAGES_FROM_RELEASE`](crate::replication::config::NEIGHBOR_SYNC_PAGES_FROM_RELEASE).
/// Nodes built from a tree between releases announce the last release's
/// version; a run of such nodes alone sets this to that release, or lower, so
/// that they page to each other. A value that isn't a whole number is ignored.
pub const SYNC_PAGES_FROM_RELEASE: &str = "ANT_TEST_SEAM_SYNC_PAGES_FROM_RELEASE";

/// The release this node announces in its user agent.
///
/// Its `+ds.<n>` build number, in place of the one in this build's version:
/// the agent reads `node/<version>+ds.<n>`. Only the agent: the node's
/// version everywhere else, its upgrades and what it logs and reports, stays
/// this build's. Nodes built from a tree between releases announce the last
/// release's version, which reads no pages; the harness sets this to the
/// release that reads pages on this tree's nodes, so they page to each other
/// and to no node of the release before. A value that isn't a whole number is
/// ignored.
pub const ANNOUNCED_RELEASE: &str = "ANT_TEST_SEAM_ANNOUNCED_RELEASE";

/// Neighbour-sync requests a node admits at once, across peers.
///
/// In place of the responder's limit of 8: a whole number, at least 1. A small
/// limit has a few requesters push a responder past it, so its pages are
/// answered busy. A value that isn't such a number is ignored.
pub const SYNC_ADMISSION_LIMIT: &str = "ANT_TEST_SEAM_SYNC_ADMISSION_LIMIT";

/// One key's bytes: the smallest page the page-size seam takes.
const KEY_BYTES: usize = 32;

/// The page size a test set for this process, if it set a valid one.
pub(crate) fn sync_page_bytes() -> Option<usize> {
    let value = std::env::var(SYNC_PAGE_BYTES).ok()?;
    parse_sync_page_bytes(&value)
}

fn parse_sync_page_bytes(value: &str) -> Option<usize> {
    value
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|bytes| *bytes >= KEY_BYTES)
}

/// The paging release a test set for this process, if it set a valid one.
pub(crate) fn sync_pages_from_release() -> Option<u64> {
    let value = std::env::var(SYNC_PAGES_FROM_RELEASE).ok()?;
    value.trim().parse::<u64>().ok()
}

/// `version` as this node announces it: with the release a test set for this
/// process ([`ANNOUNCED_RELEASE`]) as its `+ds.<n>`, or as it is.
#[must_use]
pub fn announced_version(version: &str) -> String {
    std::env::var(ANNOUNCED_RELEASE)
        .ok()
        .and_then(|release| release.trim().parse::<u64>().ok())
        .map_or_else(
            || version.to_string(),
            |release| with_release(version, release),
        )
}

/// `version` with `release` as its `+ds.<n>`, in place of any build part.
fn with_release(version: &str, release: u64) -> String {
    let base = version.split('+').next().unwrap_or(version);
    format!("{base}+ds.{release}")
}

/// The admission limit a test set for this process, if it set a valid one.
pub(crate) fn sync_admission_limit() -> Option<usize> {
    let value = std::env::var(SYNC_ADMISSION_LIMIT).ok()?;
    value
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|limit| *limit >= 1)
}

/// Names a file holding the free space the node reads for its partition.
///
/// Every reading of the free space on the node's storage partition returns
/// it in place of the partition's own: the chunk store's capacity check, the
/// listing store's map size, and the disk-pressure state. The file holds the
/// free bytes, then, optionally, the partition's size in bytes (otherwise its
/// real size is read), then, optionally, the free inodes and the partition's
/// inodes (otherwise the reading counts no inodes), separated by white space.
/// The test rewrites the file to fill or free the disk while the node runs. A
/// file that can't be read or parsed is a failed measurement, as a failed
/// `statvfs` is.
pub const FREE_SPACE_FILE: &str = "ANT_TEST_SEAM_FREE_SPACE_FILE";

/// How long, in whole seconds, a "full" notice from a peer stays fresh.
///
/// In place of
/// [`FULL_NOTICE_FRESHNESS`](crate::replication::full::FULL_NOTICE_FRESHNESS),
/// on the node that holds the marks; a full node renews its notice at a
/// quarter of the value it reads here, so a run sets the same on every node.
/// A short value lets a run see a mark lapse once the full node stops renewing
/// it. A value that isn't a whole number of seconds, at least 4, is ignored.
pub const FULL_FRESHNESS_SECS: &str = "ANT_TEST_SEAM_FULL_FRESHNESS_SECS";

/// The neighbour-sync interval, in whole seconds: the shortest wait between
/// cycles, with twice it the longest and it the cooldown before the same
/// peer is synced again.
///
/// In place of the 10 to 20 minutes and the hour of
/// [`ReplicationConfig`](crate::replication::config::ReplicationConfig)'s
/// defaults, so a run sees the several sync cycles (and so the erasure
/// epochs) a record's erasure needs. A value that isn't a whole number of
/// seconds, at least 1, is ignored.
pub const NEIGHBOR_SYNC_SECS: &str = "ANT_TEST_SEAM_NEIGHBOR_SYNC_SECS";

/// How long, in whole seconds, a record or pointer must have been out of a
/// node's range before it may be erased, in place of the default 3 days. A
/// value that isn't a whole number is ignored.
pub const PRUNE_HYSTERESIS_SECS: &str = "ANT_TEST_SEAM_PRUNE_HYSTERESIS_SECS";

/// `config` with the timing seams a test set for this process applied
/// ([`NEIGHBOR_SYNC_SECS`], [`PRUNE_HYSTERESIS_SECS`]).
#[must_use]
pub fn replication_timing(
    config: crate::replication::config::ReplicationConfig,
) -> crate::replication::config::ReplicationConfig {
    apply_replication_timing(
        config,
        std::env::var(NEIGHBOR_SYNC_SECS).ok().as_deref(),
        std::env::var(PRUNE_HYSTERESIS_SECS).ok().as_deref(),
    )
}

fn apply_replication_timing(
    mut config: crate::replication::config::ReplicationConfig,
    sync: Option<&str>,
    hysteresis: Option<&str>,
) -> crate::replication::config::ReplicationConfig {
    use std::time::Duration;
    if let Some(secs) = sync
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|secs| *secs >= 1)
    {
        config.neighbor_sync_interval_min = Duration::from_secs(secs);
        config.neighbor_sync_interval_max = Duration::from_secs(secs.saturating_mul(2));
        config.neighbor_sync_cooldown = Duration::from_secs(secs);
    }
    if let Some(secs) = hysteresis.and_then(|value| value.trim().parse::<u64>().ok()) {
        config.prune_hysteresis_duration = Duration::from_secs(secs);
    }
    config
}

/// Free and total bytes a test set for the storage partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreeSpace {
    /// Bytes free.
    pub free: u64,
    /// The partition's size, if the test set one.
    pub total: Option<u64>,
    /// Free and total inodes, if the test set them.
    pub inodes: Option<(u64, u64)>,
}

/// The first release whose nodes this node sends its "full" notice to.
///
/// Its `+ds.<n>` build number, in place of
/// [`FULL_NOTICE_FROM_RELEASE`](crate::replication::full::FULL_NOTICE_FROM_RELEASE).
/// Nodes built from a tree between releases announce the last release's
/// version; a run of such nodes alone sets this to that release, or lower, so
/// that a full one tells the others. A value that isn't a whole number is
/// ignored.
pub const FULL_NOTICE_FROM_RELEASE: &str = "ANT_TEST_SEAM_FULL_NOTICE_FROM_RELEASE";

/// The notice's release a test set for this process, if it set a valid one.
pub(crate) fn full_notice_from_release() -> Option<u64> {
    let value = std::env::var(FULL_NOTICE_FROM_RELEASE).ok()?;
    value.trim().parse::<u64>().ok()
}

/// The shortest "full" freshness the seam takes, in seconds: a renewal every
/// second at a quarter of it.
const MIN_FULL_FRESHNESS_SECS: u64 = 4;

/// How long a "full" notice stays fresh, if a test set a valid value.
pub(crate) fn full_freshness() -> Option<std::time::Duration> {
    let value = std::env::var(FULL_FRESHNESS_SECS).ok()?;
    parse_full_freshness(&value)
}

fn parse_full_freshness(value: &str) -> Option<std::time::Duration> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|secs| *secs >= MIN_FULL_FRESHNESS_SECS)
        .map(std::time::Duration::from_secs)
}

/// The free space a test set for this process: `None` when the seam isn't
/// set, otherwise what the file it names holds, or why it couldn't be read.
pub(crate) fn free_space() -> Option<io::Result<FreeSpace>> {
    let path = std::env::var_os(FREE_SPACE_FILE)?;
    Some(read_free_space(Path::new(&path)))
}

/// Reads a free-space file (see [`FREE_SPACE_FILE`]).
///
/// # Errors
///
/// When the file can't be read, or holds anything but one or two
/// whole numbers.
pub fn read_free_space(path: &Path) -> io::Result<FreeSpace> {
    let text = std::fs::read_to_string(path)?;
    parse_free_space(&text).map_err(|reason| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{FREE_SPACE_FILE} names {}: {reason}", path.display()),
        )
    })
}

fn parse_free_space(text: &str) -> Result<FreeSpace, String> {
    let number = |field: &str| {
        field
            .parse::<u64>()
            .map_err(|e| format!("'{field}' is not a number of bytes: {e}"))
    };
    let fields: Vec<&str> = text.split_whitespace().collect();
    match fields.as_slice() {
        [free] => Ok(FreeSpace {
            free: number(free)?,
            total: None,
            inodes: None,
        }),
        [free, total] => Ok(FreeSpace {
            free: number(free)?,
            total: Some(number(total)?),
            inodes: None,
        }),
        [free, total, free_inodes, inodes] => Ok(FreeSpace {
            free: number(free)?,
            total: Some(number(total)?),
            inodes: Some((number(free_inodes)?, number(inodes)?)),
        }),
        _ => Err(format!(
            "expected the free bytes and, optionally, the partition's size, then its free \
             and total inodes; found {} field(s)",
            fields.len()
        )),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn a_free_space_file_holds_the_free_bytes_and_optionally_the_size() {
        assert_eq!(
            parse_free_space("1048576\n"),
            Ok(FreeSpace {
                free: 1_048_576,
                total: None,
                inodes: None,
            })
        );
        assert_eq!(
            parse_free_space(" 0  21474836480 "),
            Ok(FreeSpace {
                free: 0,
                total: Some(21_474_836_480),
                inodes: None,
            })
        );
        assert_eq!(
            parse_free_space("5 10 100 4000"),
            Ok(FreeSpace {
                free: 5,
                total: Some(10),
                inodes: Some((100, 4000)),
            })
        );
        for bad in ["", "full", "-1", "1 2 3", "1.5", "1 2 3 4 5", "1 2 x 4"] {
            assert!(parse_free_space(bad).is_err(), "{bad:?} was read");
        }
    }

    #[test]
    fn an_unreadable_free_space_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        assert!(read_free_space(&missing).is_err());
        let garbled = dir.path().join("garbled");
        std::fs::write(&garbled, "lots").unwrap();
        let err = read_free_space(&garbled).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains(FREE_SPACE_FILE), "{err}");
    }

    #[test]
    fn every_seam_starts_with_the_prefix() {
        for seam in [
            FREE_SPACE_FILE,
            SYNC_PAGE_BYTES,
            SYNC_PAGES_FROM_RELEASE,
            ANNOUNCED_RELEASE,
            SYNC_ADMISSION_LIMIT,
            FULL_FRESHNESS_SECS,
            FULL_NOTICE_FROM_RELEASE,
            NEIGHBOR_SYNC_SECS,
            PRUNE_HYSTERESIS_SECS,
        ] {
            assert!(seam.starts_with(PREFIX), "{seam}");
        }
    }

    /// The announced release replaces only the build part of the version, so
    /// the agent still parses as a node's of that release.
    #[test]
    fn an_announced_release_replaces_the_build_part_only() {
        assert_eq!(with_release("0.23.0+ds.15", 17), "0.23.0+ds.17");
        assert_eq!(with_release("0.23.0", 17), "0.23.0+ds.17");
        assert_eq!(with_release("0.24.1+ds.15.2", 18), "0.24.1+ds.18");
        let agent = format!("node/{}", with_release(env!("CARGO_PKG_VERSION"), 17));
        assert!(crate::replication::sync_pages::agent_reads_pages(
            &agent, 17
        ));
        assert!(!crate::replication::sync_pages::agent_reads_pages(
            &agent, 18
        ));
    }

    #[test]
    fn a_sync_page_holds_at_least_one_key() {
        assert_eq!(parse_sync_page_bytes("64"), Some(64));
        assert_eq!(parse_sync_page_bytes(" 32\n"), Some(32));
        for ignored in ["", "31", "0", "-64", "64 bytes", "6.4"] {
            assert_eq!(parse_sync_page_bytes(ignored), None, "{ignored:?}");
        }
    }

    #[test]
    fn the_timing_seams_shorten_sync_and_the_out_of_range_wait() {
        use crate::replication::config::ReplicationConfig;
        use std::time::Duration;
        let config = apply_replication_timing(ReplicationConfig::default(), Some("30"), Some("60"));
        assert_eq!(config.neighbor_sync_interval_min, Duration::from_secs(30));
        assert_eq!(config.neighbor_sync_interval_max, Duration::from_secs(60));
        assert_eq!(config.neighbor_sync_cooldown, Duration::from_secs(30));
        assert_eq!(config.prune_hysteresis_duration, Duration::from_secs(60));
        assert!(config.validate().is_ok());
        let unset = apply_replication_timing(ReplicationConfig::default(), None, None);
        let default = ReplicationConfig::default();
        assert_eq!(
            unset.neighbor_sync_interval_min,
            default.neighbor_sync_interval_min
        );
        assert_eq!(
            unset.prune_hysteresis_duration,
            default.prune_hysteresis_duration
        );
        for ignored in ["", "0", "-1", "1.5", "30s"] {
            let config = apply_replication_timing(
                ReplicationConfig::default(),
                Some(ignored),
                Some(ignored),
            );
            assert_eq!(
                config.neighbor_sync_interval_min, default.neighbor_sync_interval_min,
                "{ignored:?}"
            );
        }
    }

    #[test]
    fn a_full_freshness_is_whole_seconds_of_at_least_four() {
        assert_eq!(
            parse_full_freshness("8"),
            Some(std::time::Duration::from_secs(8))
        );
        assert_eq!(
            parse_full_freshness(" 4\n"),
            Some(std::time::Duration::from_secs(4))
        );
        for ignored in ["", "3", "0", "-8", "8s", "8.5"] {
            assert_eq!(parse_full_freshness(ignored), None, "{ignored:?}");
        }
    }
}

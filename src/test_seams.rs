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
//! that brings the value: how long a "full" notice stays fresh
//! ([`FULL_FRESHNESS_SECS`]), for a run in which a node's mark as full must
//! lapse within the run, and the first release a full node tells it is full
//! ([`FULL_NOTICE_FROM_RELEASE`]), which the harness's `full` run sets so that
//! nodes of a tree between releases tell each other; still to come is the
//! size of a neighbour-sync page, for a run whose lists take more than one
//! page. Each is compiled only with `test-utils` and named under [`PREFIX`],
//! so the Build workflow's check covers it, and the harness refuses a run that
//! needs a seam with a binary that lacks it, as it refuses the `full` run
//! without [`FREE_SPACE_FILE`].

use std::io;
use std::path::Path;

/// Every seam's environment variable starts with this.
pub const PREFIX: &str = "ANT_TEST_SEAM_";

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
            FULL_FRESHNESS_SECS,
            FULL_NOTICE_FROM_RELEASE,
        ] {
            assert!(seam.starts_with(PREFIX), "{seam}");
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

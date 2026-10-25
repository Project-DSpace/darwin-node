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
//! that brings the value, with the harness's runs that set it: the size of a
//! neighbour-sync page, for a run whose lists take more than one page, and how
//! long a "full" notice stays fresh, for a run in which a node's mark as full
//! lapses. Each is compiled only with `test-utils` and named under
//! [`PREFIX`], so the Build workflow's check covers it, and the harness
//! refuses such a run with a binary that lacks the seam, as it refuses the
//! `full` run without [`FREE_SPACE_FILE`].

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
/// real size is read), separated by white space. The test rewrites the file
/// to fill or free the disk while the node runs. A file that can't be read or
/// parsed is a failed measurement, as a failed `statvfs` is.
pub const FREE_SPACE_FILE: &str = "ANT_TEST_SEAM_FREE_SPACE_FILE";

/// Free and total bytes a test set for the storage partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreeSpace {
    /// Bytes free.
    pub free: u64,
    /// The partition's size, if the test set one.
    pub total: Option<u64>,
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
        }),
        [free, total] => Ok(FreeSpace {
            free: number(free)?,
            total: Some(number(total)?),
        }),
        _ => Err(format!(
            "expected the free bytes and, optionally, the partition's size; found {} field(s)",
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
                total: None
            })
        );
        assert_eq!(
            parse_free_space(" 0  21474836480 "),
            Ok(FreeSpace {
                free: 0,
                total: Some(21_474_836_480)
            })
        );
        for bad in ["", "full", "-1", "1 2 3", "1.5"] {
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
        assert!(FREE_SPACE_FILE.starts_with(PREFIX));
    }
}

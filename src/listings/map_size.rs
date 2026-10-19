//! The size of the listing store's LMDB map, from the disk it is on.
//!
//! The listing store keeps its own LMDB environment and sizes its map from the
//! free space on the partition, as the node's LMDB chunk store did. That store
//! is gone; these are its sizing rules, unchanged, for the listing store alone.

use crate::error::{Error, Result};
#[cfg(windows)]
use crate::storage::GIB;
use std::path::Path;

/// Absolute minimum LMDB map size.
///
/// Even on a nearly-full disk the database must be able to open.
/// Set to 256 MiB — enough for millions of LMDB pages.
const MIN_MAP_SIZE: usize = 256 * 1024 * 1024;

/// Maximum head-room (beyond the current data footprint) to reserve for the
/// LMDB map **on Windows**.
///
/// On Windows a node's committed / private memory scales with the *mapped*
/// size rather than with the data actually stored. Measured on a live node:
/// ~7.4 GB of extra commit for a ~3.7 TiB map, versus ~181 MB for a ~55 GiB
/// map — roughly 0.2% of the mapped size, resident and attributed to *no*
/// user-space allocation. That size-proportional cost is consistent with
/// kernel page tables / section metadata for the mapping; the exact kernel
/// structure was inferred from the scaling rather than measured directly, but
/// the size-proportional *effect* is what this cap targets. Linux keeps the
/// mapping sparse, so a disk-sized map is nearly free there — the overhead is
/// Windows-specific, which is why it only surfaced in Windows reports.
///
/// Sizing the map to the whole disk therefore costs ~0.2% of *free disk* per
/// node, multiplied by every node sharing the host (e.g. a 10 TiB partition ≈
/// 20 GiB). We instead cap the head-room on Windows and lean on the
/// listing store growing the map on demand as data accumulates,
/// keeping the overhead proportional to *stored data* rather than *disk
/// capacity*. At 32 GiB the extra commit is ~100 MB, and a resize happens at
/// most once per 32 GiB written.
#[cfg(windows)]
const WINDOWS_MAP_HEADROOM: u64 = 32 * GIB;

/// Compute the LMDB map size from the disk hosting `db_dir`.
///
/// The result covers **all existing data** plus all remaining usable disk
/// space:
///
/// ```text
/// map_size = current_db_file_size + max(0, available_space − reserve)
/// ```
///
/// `available_space` (from `statvfs`) reports only the *free* bytes on the
/// partition — the DB file's own footprint is **not** included, so adding
/// it back ensures the map is always large enough for the data already
/// stored.
///
/// On Windows the disk-headroom term is additionally capped at
/// `WINDOWS_MAP_HEADROOM` to bound the map-proportional commit overhead (see
/// that constant); the listing store grows the map on demand as data grows.
///
/// The result is page-aligned and never falls below [`MIN_MAP_SIZE`].
pub fn compute_map_size(db_dir: &Path, reserve: u64) -> Result<usize> {
    let available = fs2::available_space(db_dir)
        .map_err(|e| Error::Storage(format!("Failed to query available disk space: {e}")))?;

    // The MDB data file may not exist yet on first run, and that is the only reason to
    // read zero here. Any other failure is a question that was not answered, and answering
    // it with zero sizes the map as though the database were empty, which on a node with a
    // large one is a map far too small to open it.
    let mdb_file = db_dir.join("data.mdb");
    let current_db_bytes = match std::fs::metadata(&mdb_file) {
        Ok(meta) => meta.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => {
            return Err(Error::Storage(format!(
                "Failed to measure {}: {e}. Refusing to size the map as though it were \
                 empty.",
                mdb_file.display()
            )))
        }
    };

    let target = map_target_bytes(current_db_bytes, available, reserve);

    // Align up to system page size (required by heed's resize).
    let page = page_size::get() as u64;
    let aligned = target.div_ceil(page) * page;

    let result = usize::try_from(aligned).unwrap_or(usize::MAX);
    Ok(result.max(MIN_MAP_SIZE))
}

/// Head-room policy for the LMDB map, split out from [`compute_map_size`] so it
/// is unit-testable without touching the real filesystem.
///
/// `map = current_db_bytes + max(0, available − reserve)`, with the head-room
/// term capped at `WINDOWS_MAP_HEADROOM` on Windows. Existing data
/// (`current_db_bytes`) is always covered so a resize can never truncate the
/// database, even when the head-room cap or a nearly-full disk drives the
/// growth term to zero.
fn map_target_bytes(current_db_bytes: u64, available: u64, reserve: u64) -> u64 {
    // available_space excludes the DB file, so we add it back to get the
    // total space the DB could occupy while still leaving `reserve` free.
    let growth_room = available.saturating_sub(reserve);

    // On Windows, bound the head-room so the mapped size (and its
    // size-proportional commit overhead) stays proportional to stored data
    // rather than disk capacity. Elsewhere, use all reachable space.
    #[cfg(windows)]
    let growth_room = growth_room.min(WINDOWS_MAP_HEADROOM);

    current_db_bytes.saturating_add(growth_room)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{GIB, MIB};

    #[test]
    fn map_target_covers_existing_data_and_headroom() {
        // A partition with far more free space than any node needs.
        let huge_free = 100 * 1024 * GIB; // 100 TiB
        let reserve = 500 * MIB;

        // Fresh node, no data yet.
        let fresh = map_target_bytes(0, huge_free, reserve);
        // Node already holding 16 GiB of chunks.
        let with_data = map_target_bytes(16 * GIB, huge_free, reserve);

        #[cfg(windows)]
        {
            // Windows: head-room is capped, so the map (and thus page tables)
            // stay bounded regardless of disk size. Existing data always sits
            // on top of the capped head-room.
            assert_eq!(fresh, WINDOWS_MAP_HEADROOM);
            assert_eq!(with_data, 16 * GIB + WINDOWS_MAP_HEADROOM);
            // Sanity: page-table cost (~map/512) is tens of MiB, not tens of GiB.
            assert!(with_data / 512 < 128 * MIB);
        }

        #[cfg(not(windows))]
        {
            // Other platforms keep the disk-sized map (lazy page tables cost
            // nothing), so head-room is the full free span minus reserve.
            assert_eq!(fresh, huge_free - reserve);
            assert_eq!(with_data, 16 * GIB + (huge_free - reserve));
        }
    }

    #[test]
    fn map_target_never_truncates_data_when_disk_nearly_full() {
        // Free space below the reserve → head-room saturates to 0 on every
        // platform, but the existing 4 GiB of data must still be covered.
        assert_eq!(map_target_bytes(4 * GIB, 100 * MIB, 500 * MIB), 4 * GIB);
    }
}

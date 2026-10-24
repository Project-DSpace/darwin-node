//! The free-space seam (src/test_seams.rs) reaches the node's readings of
//! its free space: the chunk store's capacity check and the disk-pressure
//! state, so a test can make a node's disk read as full on a disk with room.
//! One test, alone in its process, because the seam is read from the
//! environment.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ant_node::replication::retention::DiskRetention;
use ant_node::storage::chunk_store::CapacityVerdict;
use ant_node::storage::{ChunkStore, ChunkStoreConfig, GIB, MIB};
use ant_node::test_seams::FREE_SPACE_FILE;

#[tokio::test]
async fn the_free_space_seam_reaches_the_free_space_readings() {
    let dir = tempfile::tempdir().unwrap();
    let seam = dir.path().join("free");
    let store = ChunkStore::new(ChunkStoreConfig {
        root_dir: dir.path().join("node"),
        verify_on_read: true,
        disk_reserve: 500 * MIB,
    })
    .await
    .unwrap();
    let retention = DiskRetention::new(store.chunks_dir().to_path_buf());

    // 100 MiB free of 20 GiB: below the 500 MiB reserve, and under disk
    // pressure, whatever the real disk has.
    std::fs::write(&seam, format!("{} {}", 100 * MIB, 20 * GIB)).unwrap();
    std::env::set_var(FREE_SPACE_FILE, &seam);
    assert_eq!(store.capacity_verdict(), CapacityVerdict::Full);
    let refused = store.check_capacity().unwrap_err().to_string();
    assert!(refused.contains("Insufficient disk space"), "{refused}");
    assert!(retention.refresh(), "0.5% free is disk pressure");

    // Freed: writable again at once, since a failing check isn't kept.
    std::fs::write(&seam, format!("{} {}", 10 * GIB, 20 * GIB)).unwrap();
    assert_eq!(store.capacity_verdict(), CapacityVerdict::Writable);
    store.check_capacity().unwrap();
    assert!(
        !retention.refresh(),
        "half the partition free is no pressure"
    );

    // Free space only, the partition's size read from the disk.
    std::fs::write(&seam, "0\n").unwrap();
    assert_eq!(store.capacity_verdict(), CapacityVerdict::Full);
    assert!(retention.refresh(), "nothing free is disk pressure");

    // A seam that can't be read is a failed measurement, never room.
    std::fs::write(&seam, "plenty").unwrap();
    assert_eq!(store.capacity_verdict(), CapacityVerdict::Unknown);
    assert!(
        !retention.refresh(),
        "a failed measurement ends the pressure, as a failed statvfs does"
    );

    // Unset: the disk's own reading.
    std::env::remove_var(FREE_SPACE_FILE);
    assert_ne!(store.capacity_verdict(), CapacityVerdict::Unknown);
}

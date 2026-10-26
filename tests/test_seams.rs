//! The test seams (`src/test_seams.rs`) reach the node's values. The
//! free-space seam reaches its readings of its free space: the chunk store's
//! capacity check and the disk-pressure state, so a test can make a node's
//! disk read as full on a disk with room. The "full" seams reach how long a
//! peer's "full" notice stays fresh, so a run can see a mark lapse, and the
//! first release a full node tells it is full. Each seam is set by one test
//! alone, in this process of its own, because the seams are read from the
//! environment.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ant_node::replication::full::{
    full_notice_freshness, full_notice_from_release, full_notice_renewal, FullMarks,
    FULL_NOTICE_FRESHNESS, FULL_NOTICE_FROM_RELEASE,
};
use ant_node::replication::retention::DiskRetention;
use ant_node::storage::chunk_store::CapacityVerdict;
use ant_node::storage::{ChunkStore, ChunkStoreConfig, GIB, MIB};
use ant_node::test_seams::{
    FREE_SPACE_FILE, FULL_FRESHNESS_SECS, FULL_NOTICE_FROM_RELEASE as FROM_RELEASE_SEAM,
};
use saorsa_core::identity::PeerId;
use std::time::{Duration, Instant};

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

#[test]
fn the_full_seams_reach_the_freshness_and_the_release() {
    assert_eq!(full_notice_freshness(), FULL_NOTICE_FRESHNESS);
    assert_eq!(full_notice_from_release(), FULL_NOTICE_FROM_RELEASE);

    // Eight seconds: a mark lapses eight seconds after the peer's last
    // notice, and a full node renews every two.
    std::env::set_var(FULL_FRESHNESS_SECS, "8");
    assert_eq!(full_notice_freshness(), Duration::from_secs(8));
    assert_eq!(full_notice_renewal(), Duration::from_secs(2));
    let marks = FullMarks::default();
    let peer = PeerId::from_bytes([5; 32]);
    let now = Instant::now();
    marks.note_notice(peer, true, now);
    assert!(marks.is_marked(&peer, now + Duration::from_secs(7)));
    assert!(
        !marks.is_marked(&peer, now + Duration::from_secs(8)),
        "the mark lapses at the seam's freshness"
    );

    // Under four seconds is ignored: the shipped freshness.
    std::env::set_var(FULL_FRESHNESS_SECS, "3");
    assert_eq!(full_notice_freshness(), FULL_NOTICE_FRESHNESS);
    std::env::remove_var(FULL_FRESHNESS_SECS);
    assert_eq!(full_notice_freshness(), FULL_NOTICE_FRESHNESS);

    // The first release told: 0 tells nodes of a tree between releases.
    std::env::set_var(FROM_RELEASE_SEAM, "0");
    assert_eq!(full_notice_from_release(), 0);
    std::env::set_var(FROM_RELEASE_SEAM, "soon");
    assert_eq!(full_notice_from_release(), FULL_NOTICE_FROM_RELEASE);
    std::env::remove_var(FROM_RELEASE_SEAM);
    assert_eq!(full_notice_from_release(), FULL_NOTICE_FROM_RELEASE);
}

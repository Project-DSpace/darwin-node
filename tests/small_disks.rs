//! Small disks with the shipped full line: the falling floor and "full, not
//! faulty" on simulated 15, 20, 40 and 80 GB partitions, with one and with two
//! nodes on a partition.
//!
//! Each node is a chunk store opened with the shipped settings (the 1 GiB
//! full line, never the zero reserve of `ChunkStoreConfig::test_default()`)
//! and the disk-pressure state the prune passes share. The partition is the
//! free-space seam (`src/test_seams.rs`): the test writes the free bytes, the
//! partition's size and, where it says so, its inodes, and every reading the
//! nodes take sees them. Free space is walked down from 12% to nothing:
//!
//! - the floor's steps fire in order and never go back, each at the larger of
//!   its share of the partition and the line plus one 4 MiB chunk, so on the
//!   small partitions they collapse onto the line and the last one is reached
//!   no later than the line;
//! - at the line a node whose last passes found something to erase is not
//!   full; one whose passes found nothing is, refuses every new record and
//!   keeps what it holds, and leaves "full" only 256 MiB above the line;
//! - inodes short with bytes plentiful put a node at the line and refuse new
//!   records; a filesystem that counts no inodes never does.
//!
//! One test, alone in its process, because the seam is read from the
//! environment.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ant_node::replication::erasure::{ClockKind, ErasureGate};
use ant_node::replication::full;
use ant_node::replication::retention::{
    step_clamp, DiskRetention, FLOOR_STEPS, LAST_FLOOR_STEP, LEAVE_FULL_INODE_MARGIN,
    LEAVE_FULL_MARGIN,
};
use ant_node::storage::{
    ChunkStore, ChunkStoreConfig, FullTrigger, XorName, DEFAULT_DISK_RESERVE, GIB, MIB,
};
use ant_node::test_seams::FREE_SPACE_FILE;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Bytes in a decimal gigabyte, as partitions are sold.
const GB: u64 = 1_000_000_000;

/// The partitions simulated, the operator guide's 20 GB minimum among them.
const PARTITIONS: [u64; 4] = [15 * GB, 20 * GB, 40 * GB, 80 * GB];

/// The free-space seam's file.
struct Seam {
    path: PathBuf,
}

impl Seam {
    /// Free bytes and the partition's size, inodes not counted.
    fn bytes(&self, free: u64, total: u64) {
        self.write(&format!("{free} {total}"));
    }

    /// Free bytes, the partition's size, free inodes and the partition's.
    fn with_inodes(&self, free: u64, total: u64, free_inodes: u64, inodes: u64) {
        self.write(&format!("{free} {total} {free_inodes} {inodes}"));
    }

    /// Written under another name and renamed into place, so a reading never
    /// sees half a line.
    fn write(&self, line: &str) {
        let next = self.path.with_extension("next");
        std::fs::write(&next, line).unwrap();
        std::fs::rename(&next, &self.path).unwrap();
    }
}

/// One node: its chunk store, with the shipped line, and its disk-pressure
/// state.
struct Node {
    store: Arc<ChunkStore>,
    retention: DiskRetention,
}

impl Node {
    async fn open(root: &Path) -> Self {
        let store = Arc::new(
            ChunkStore::new(ChunkStoreConfig {
                root_dir: root.to_path_buf(),
                verify_on_read: false,
                ..ChunkStoreConfig::default()
            })
            .await
            .unwrap(),
        );
        assert_eq!(store.full_line(), DEFAULT_DISK_RESERVE, "the shipped line");
        let retention = DiskRetention::for_store(&store);
        Self { store, retention }
    }

    /// Measure again, as a prune pass does at its start, and the store's
    /// capacity guard with it.
    fn measure(&self) {
        self.retention.refresh();
        let _ = self.store.capacity_verdict();
    }

    /// Store a new chunk: its error's text if refused.
    async fn put_new(&self, seed: u64) -> Result<bool, String> {
        let content = format!("a new record {seed}").into_bytes();
        let address = ChunkStore::compute_address(&content);
        self.store
            .put(&address, &content)
            .await
            .map_err(|e| e.to_string())
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn on_small_disks_the_floor_falls_onto_the_line_and_full_nodes_keep_what_they_hold() {
    let dir = tempfile::tempdir().unwrap();
    let seam = Seam {
        path: dir.path().join("free"),
    };
    seam.bytes(10 * GB, 20 * GB);
    std::env::set_var(FREE_SPACE_FILE, &seam.path);

    let line = DEFAULT_DISK_RESERVE;
    let clamp = step_clamp(line);
    assert_eq!(clamp, line + 4 * MIB);
    let mut seed = 0u64;

    for total in PARTITIONS {
        for count in [1usize, 2] {
            let root = dir.path().join(format!("p{}-n{count}", total / GB));
            let mut nodes = Vec::new();
            for index in 0..count {
                nodes.push(Node::open(&root.join(format!("node-{index}"))).await);
            }
            let what = format!("{} GB, {count} node(s)", total / GB);

            // Each node holds a record while the disk has room.
            seam.bytes(total / 2, total);
            let held: Vec<(XorName, Vec<u8>)> = nodes
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    let content = format!("held {what} {index}").into_bytes();
                    (ChunkStore::compute_address(&content), content)
                })
                .collect();
            for (node, (address, content)) in nodes.iter().zip(&held) {
                node.measure();
                node.store.put(address, content).await.unwrap();
            }

            // Walk free space down to the line: the steps fire in order, each
            // at the larger of its share and the clamp, and the last no later
            // than the line.
            let mut first_at: [Option<u64>; FLOOR_STEPS.len()] = [None; FLOOR_STEPS.len()];
            let mut last_step: Vec<Option<usize>> = vec![None; count];
            let mut line_first_at = None;
            let mut available = total * 12 / 100;
            loop {
                seam.bytes(available, total);
                for (index, node) in nodes.iter().enumerate() {
                    node.measure();
                    let step = node.retention.step();
                    assert!(
                        step >= last_step[index],
                        "{what}: the floor rose at {available}"
                    );
                    last_step[index] = step;
                    if let Some(step) = step {
                        for reached in first_at.iter_mut().take(step + 1) {
                            reached.get_or_insert(available);
                        }
                    }
                    if node.retention.at_the_line().is_some() {
                        line_first_at.get_or_insert(available);
                        assert_eq!(
                            step,
                            Some(LAST_FLOOR_STEP),
                            "{what}: at the line, the last step runs"
                        );
                    }
                }
                if available <= clamp {
                    break;
                }
                let next = if available > clamp + GIB {
                    64 * MIB
                } else {
                    MIB
                };
                available = available.saturating_sub(next).max(clamp);
            }
            for (index, step) in FLOOR_STEPS.iter().enumerate() {
                let share =
                    u64::try_from(u128::from(total) * u128::from(step.below_per_mille) / 1000)
                        .unwrap();
                let level = share.max(clamp);
                let fired = first_at[index].unwrap_or_else(|| panic!("{what}: step {index}"));
                assert!(
                    fired <= level && fired + 64 * MIB >= level,
                    "{what}: step {index} first fired at {fired}, its level is {level}"
                );
            }
            let line_first_at = line_first_at.expect("the line is reached");
            assert_eq!(line_first_at, clamp, "{what}");
            assert!(
                first_at[LAST_FLOOR_STEP].unwrap() >= line_first_at,
                "{what}"
            );

            // At the line, passes that erased or selected something leave the
            // node as it is: it still has copies to give up.
            for node in &nodes {
                assert_eq!(full::after_passes(&node.store, &node.retention, 1, 0), None);
                assert_eq!(full::after_passes(&node.store, &node.retention, 0, 3), None);
                assert!(!node.store.is_full(), "{what}");
            }
            // A key found ready but deferred for lack of audit budget, hinted
            // again meanwhile, still keeps the node from being full.
            let gate = ErasureGate::default();
            let deferred = [0xD7; 32];
            gate.select(ClockKind::Records, &[deferred], 1, 1024);
            let ready = gate.select(ClockKind::Records, &[deferred], 3, 1024);
            assert_eq!(ready, vec![deferred]);
            gate.release_untried(ClockKind::Records, &ready, &HashSet::new());
            assert!(!gate.is_being_erased(&deferred));
            for node in &nodes {
                assert_eq!(
                    full::after_passes(&node.store, &node.retention, 0, gate.not_yet_erased()),
                    None,
                    "{what}"
                );
                assert!(!node.store.is_full(), "{what}");
            }
            // Once it is no longer eligible, nothing is owed.
            gate.select(ClockKind::Records, &[], 4, 1024);
            assert_eq!(gate.not_yet_erased(), 0);
            // Passes at the last step that found nothing: full.
            for node in &nodes {
                assert_eq!(
                    full::after_passes(&node.store, &node.retention, 0, gate.not_yet_erased()),
                    Some(true),
                    "{what}"
                );
                let state = node.store.full_state().expect("full");
                assert_eq!(state.trigger, FullTrigger::Bytes);
                // Entering again changes nothing.
                assert_eq!(full::after_passes(&node.store, &node.retention, 0, 0), None);
            }
            // A full node refuses every new record, even with room for it,
            // and keeps what it holds.
            seam.bytes(clamp + MIB, total);
            for (node, (address, content)) in nodes.iter().zip(&held) {
                node.measure();
                seed += 1;
                let refused = node
                    .put_new(seed)
                    .await
                    .expect_err("a new record is refused");
                assert!(refused.contains("full"), "{what}: {refused}");
                assert!(node.store.exists(address).unwrap(), "{what}: still held");
                assert!(
                    node.store.put(address, content).await.is_ok(),
                    "{what}: a record it holds is no new data"
                );
            }
            // It leaves "full" only with the margin above the line.
            seam.bytes(line + LEAVE_FULL_MARGIN - 1, total);
            for node in &nodes {
                assert_eq!(full::leave_if_room(&node.store, &node.retention), None);
                assert!(node.store.is_full(), "{what}: no flapping");
            }
            seam.bytes(line + LEAVE_FULL_MARGIN, total);
            for node in &nodes {
                assert_eq!(
                    full::leave_if_room(&node.store, &node.retention),
                    Some(false),
                    "{what}"
                );
                assert!(!node.store.is_full());
                node.measure();
                seed += 1;
                assert!(node.put_new(seed).await.is_ok(), "{what}: takes data again");
            }
        }
    }

    // Inodes short and bytes plentiful: at the line by inodes, new records
    // refused before they are written, full once nothing is left to erase.
    let total = 20 * GB;
    let node = Node::open(&dir.path().join("inodes")).await;
    let inodes = 1_000_000u64;
    seam.with_inodes(total / 2, total, inodes * 24 / 1000, inodes);
    node.measure();
    assert_eq!(node.retention.at_the_line(), Some(FullTrigger::Inodes));
    assert_eq!(node.retention.step(), Some(LAST_FLOOR_STEP));
    seed += 1;
    let refused = node.put_new(seed).await.expect_err("out of inodes");
    assert!(refused.contains("inodes"), "{refused}");
    assert_eq!(
        full::after_passes(&node.store, &node.retention, 0, 0),
        Some(true)
    );
    assert_eq!(
        node.store.full_state().map(|state| state.trigger),
        Some(FullTrigger::Inodes)
    );
    // Leaving needs the inode margin too.
    let at_the_line = inodes * 25 / 1000;
    seam.with_inodes(
        total / 2,
        total,
        at_the_line + LEAVE_FULL_INODE_MARGIN - 1,
        inodes,
    );
    assert_eq!(full::leave_if_room(&node.store, &node.retention), None);
    let more = 10_000_000u64;
    seam.with_inodes(
        total / 2,
        total,
        more * 25 / 1000 + LEAVE_FULL_INODE_MARGIN,
        more,
    );
    assert_eq!(
        full::leave_if_room(&node.store, &node.retention),
        Some(false)
    );

    // A filesystem that counts no inodes (f_files 0): bytes alone count, and
    // a node is never at the line by inodes.
    seam.with_inodes(total / 2, total, 0, 0);
    node.measure();
    assert_eq!(node.retention.at_the_line(), None);
    assert_eq!(node.retention.step(), None);
    seed += 1;
    assert!(node.put_new(seed).await.is_ok());

    std::env::remove_var(FREE_SPACE_FILE);
}

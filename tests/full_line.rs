//! Two nodes on one small partition at the full line, their logs flooded at
//! debug level, while their paid lists, listings and commitment snapshots are
//! written and an upgrade goes through its free-space check: the partition
//! never gets down to 500 MiB free, the line at which the node manager of
//! images before darwin-nodes deleted a node's data, and every one of those
//! writes succeeds. The nodes' log files give way at the line, each writer
//! says what it dropped, and the chunk stores take no new chunk below it.
//!
//! The partition is the free-space seam (`src/test_seams.rs`): a thread keeps
//! the file it names at the partition's size less every byte under the test's
//! folder and what the test says other writers hold, so every reading the
//! nodes take sees their own and each other's writes. The upgrade can't be
//! applied here, since a release must be signed with the release key: it goes
//! through its free-space check to its download, and its writes are made at a
//! release's sizes. One test, alone in its process, because the seam is read
//! from the environment.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ant_listings::{Flags, Listing, ListingPayment, Metadata, ShownFields};
use ant_node::listings::ListingService;
use ant_node::log_files::{FreeSpace, LineFormat, LogFiles, TagLevels};
use ant_node::replication::paid_list::PaidList;
use ant_node::storage::{
    ChunkStore, ChunkStoreConfig, SpaceReading, DEFAULT_DISK_RESERVE, GIB, MIB, OLD_MANAGER_LINE,
};
use ant_node::test_seams::FREE_SPACE_FILE;
use ant_node::upgrade::{
    AutoApplyUpgrader, BinaryCache, UpgradeInfo, UpgradeResult, UPGRADE_SPACE_MARGIN,
};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing_subscriber::prelude::*;

/// The partition's size.
const PARTITION: u64 = 4 * GIB;

/// How often the partition's free space is published to the seam.
const PUBLISH_EVERY: Duration = Duration::from_millis(5);

/// How fast each node logs: a busy node at debug level.
const LOG_BYTES_PER_SECOND: u64 = 4 * MIB;

/// How long the nodes stay at the line.
const AT_THE_LINE: Duration = Duration::from_secs(45);

/// The upgrade's own writes, at a release's sizes: the archive twice (the
/// download, and its copy in the shared cache) and the binary twice
/// (extracted, and the backup of the running one).
const ARCHIVE: u64 = 20 * MIB;
const BINARY: u64 = 60 * MIB;
const UPGRADE_WRITES: u64 = 2 * ARCHIVE + 2 * BINARY;

/// A partition of [`PARTITION`] bytes holding everything under `root`, and
/// `others` bytes that other writers hold.
struct Partition {
    root: PathBuf,
    seam: PathBuf,
    others: AtomicU64,
    lowest: AtomicU64,
    stop: AtomicBool,
}

impl Partition {
    fn free(&self) -> u64 {
        PARTITION
            .saturating_sub(bytes_under(&self.root))
            .saturating_sub(self.others.load(Ordering::SeqCst))
    }

    /// Writes the free space to the seam, through a file renamed into place
    /// so a node never reads half a number.
    fn publish(&self) {
        let free = self.free();
        self.lowest.fetch_min(free, Ordering::SeqCst);
        let next = self.seam.with_extension("next");
        // A reader holding the file open can make the rename fail on Windows
        // for a moment; the next round publishes again.
        if std::fs::write(&next, format!("{free} {PARTITION}")).is_ok() {
            let _ = std::fs::rename(&next, &self.seam);
        }
    }

    /// Other writers fill or free the disk until `free` bytes are left.
    fn fill_to(&self, free: u64) {
        let ours = bytes_under(&self.root);
        self.others.store(PARTITION - ours - free, Ordering::SeqCst);
        self.publish();
    }

    fn lowest(&self) -> u64 {
        self.lowest.load(Ordering::SeqCst)
    }
}

fn bytes_under(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => bytes_under(&entry.path()),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        })
        .sum()
}

/// A node's chunk store, as its log files read it.
struct StoreSpace(Arc<ChunkStore>);

impl FreeSpace for StoreSpace {
    fn reading(&mut self) -> Option<SpaceReading> {
        self.0.space_reading(Duration::from_secs(5))
    }
}

/// One node: its stores, its log folder, and chunks it stored while the disk
/// had room, whose listings it records at the line.
struct Node {
    root: PathBuf,
    logs: PathBuf,
    chunks: Arc<ChunkStore>,
    paid: PaidList,
    listings: Arc<ListingService>,
    stored: Vec<[u8; 32]>,
}

impl Node {
    async fn open(dir: &Path, name: &str) -> Self {
        let root = dir.join("nodes").join(name);
        let logs = dir.join("logs").join(name).join("logs");
        std::fs::create_dir_all(&root).unwrap();
        let chunks = Arc::new(
            ChunkStore::new(ChunkStoreConfig {
                root_dir: root.clone(),
                verify_on_read: true,
                disk_reserve: DEFAULT_DISK_RESERVE,
            })
            .await
            .unwrap(),
        );
        let paid = PaidList::new(&root).await.unwrap();
        let listings = ListingService::new(
            &root,
            DEFAULT_DISK_RESERVE,
            Arc::clone(&chunks),
            "http://127.0.0.1:9".to_string(),
            ant_listings::VaultSet::v2_only([0; 20]),
            None,
        )
        .await
        .unwrap();
        let mut stored = Vec::new();
        for n in 0..600u32 {
            let content = format!("{name} chunk {n}").repeat(32).into_bytes();
            let address = *blake3::hash(&content).as_bytes();
            node_put(&chunks, &address, &content).await.unwrap();
            stored.push(address);
        }
        Self {
            root,
            logs,
            chunks,
            paid,
            listings,
            stored,
        }
    }

    /// Floods the node's log files with debug lines, and a warning now and
    /// then, through the node's own pipeline: the level marks, the
    /// non-blocking channel, the writer. Until `stop`.
    fn flood(&self, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
        let files = LogFiles::with_space(
            &self.logs,
            None,
            LineFormat::Text,
            Box::new(StoreSpace(Arc::clone(&self.chunks))),
            Box::new(std::time::SystemTime::now),
        )
        .unwrap();
        std::thread::spawn(move || {
            let (writer, guard) = tracing_appender::non_blocking(files);
            let subscriber = tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(TagLevels(writer)),
            );
            tracing::subscriber::with_default(subscriber, || {
                let padding = "x".repeat(160);
                let started = Instant::now();
                let mut n: u64 = 0;
                while !stop.load(Ordering::SeqCst) {
                    tracing::debug!(n, padding = %padding, "a debug line");
                    if n.is_multiple_of(2_000) {
                        tracing::warn!(n, "a warning");
                    }
                    n += 1;
                    // About 250 bytes a line: hold the rate whatever the
                    // timer's resolution.
                    let due = Duration::from_micros(n * 250 * 1_000_000 / LOG_BYTES_PER_SECOND);
                    if started.elapsed() < due {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            });
            drop(guard);
        })
    }

    /// The node's log files, oldest first, with their sizes.
    fn log_files(&self) -> Vec<(String, u64)> {
        let mut files: Vec<(String, u64)> = std::fs::read_dir(&self.logs)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| {
                (
                    entry.file_name().into_string().unwrap(),
                    entry.metadata().unwrap().len(),
                )
            })
            .collect();
        files.sort();
        files
    }

    fn log_text(&self) -> String {
        self.log_files()
            .iter()
            .filter_map(|(name, _)| std::fs::read(self.logs.join(name)).ok())
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .collect()
    }

    /// Writes a commitment snapshot as the node writes its retention file:
    /// a temporary file, synced, renamed into place.
    fn write_commitment_snapshot(&self, round: u64) -> std::io::Result<()> {
        let path = self.root.join("commitment_retention.bin");
        let tmp = path.with_extension("tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&vec![u8::try_from(round % 251).unwrap(); 64 * 1024])?;
            file.sync_all()?;
        }
        // Windows refuses a rename over a file another process (a virus
        // scanner, say) has open for a moment; retried as the chunk store
        // retries such a sharing violation when it publishes a chunk.
        let mut attempt = 0;
        loop {
            match std::fs::rename(&tmp, &path) {
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied && attempt < 10 => {
                    attempt += 1;
                    std::thread::sleep(Duration::from_millis(20));
                }
                done => return done,
            }
        }
    }
}

async fn node_put(
    chunks: &ChunkStore,
    address: &[u8; 32],
    content: &[u8],
) -> ant_node::Result<bool> {
    chunks.put(address, content).await
}

/// The listing of the chunk at `address`: it shows nothing and stays out of
/// the index.
fn listing(address: [u8; 32]) -> Listing {
    Listing {
        address,
        flags: Flags {
            shown: ShownFields::NONE,
            in_index: false,
            withheld: true,
        },
        metadata: Metadata::default(),
        size: None,
        payment: ListingPayment::default(),
        first_seen: None,
    }
}

fn key(n: u64) -> [u8; 32] {
    *blake3::hash(&n.to_le_bytes()).as_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn two_nodes_at_the_full_line_keep_the_disk_clear_of_the_old_managers_line() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("data");
    std::fs::create_dir_all(&root).unwrap();
    let partition = Arc::new(Partition {
        root: root.clone(),
        seam: dir.path().join("free"),
        others: AtomicU64::new(0),
        lowest: AtomicU64::new(u64::MAX),
        stop: AtomicBool::new(false),
    });
    partition.publish();
    std::env::set_var(FREE_SPACE_FILE, &partition.seam);
    let publisher = {
        let partition = Arc::clone(&partition);
        std::thread::spawn(move || {
            while !partition.stop.load(Ordering::SeqCst) {
                partition.publish();
                std::thread::sleep(PUBLISH_EVERY);
            }
        })
    };

    let nodes = [
        Node::open(&root, "node-1").await,
        Node::open(&root, "node-2").await,
    ];

    // Logs flooded at debug level while the disk has room, until each node
    // has closed two 50 MiB files.
    let stop = Arc::new(AtomicBool::new(false));
    let floods: Vec<_> = nodes
        .iter()
        .map(|node| node.flood(Arc::clone(&stop)))
        .collect();
    let deadline = Instant::now() + Duration::from_secs(300);
    while nodes.iter().any(|node| node.log_files().len() < 3) {
        assert!(
            Instant::now() < deadline,
            "the logs never reached 3 files each"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let oldest_before: Vec<String> = nodes
        .iter()
        .map(|node| node.log_files()[0].0.clone())
        .collect();

    // Other writers take the disk to 100 MiB above the line: the logs reach
    // it, and must give way there while the other writes go on.
    partition.fill_to(DEFAULT_DISK_RESERVE + 100 * MIB);
    let until = Instant::now() + AT_THE_LINE;
    let mut round: u64 = 0;
    while Instant::now() < until {
        for (index, node) in nodes.iter().enumerate() {
            let n = round * 2 + index as u64;
            node.paid.insert(&key(n)).await.unwrap();
            assert!(node.paid.contains(&key(n)).unwrap());
            let address = node.stored[usize::try_from(round).unwrap() % node.stored.len()];
            if round < node.stored.len() as u64 {
                assert!(
                    node.listings.record(listing(address)).await,
                    "round {round}: listing not recorded"
                );
            }
            node.write_commitment_snapshot(round).unwrap();
        }
        round += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let lowest = partition.lowest();
    assert!(
        lowest > OLD_MANAGER_LINE,
        "free space fell to {} MiB",
        lowest / MIB
    );

    // Each node's logs gave way: its oldest file went first.
    for (node, oldest) in nodes.iter().zip(&oldest_before) {
        assert!(
            !node.log_files().iter().any(|(name, _)| name == oldest),
            "{}: the oldest file {oldest} is still there",
            node.logs.display()
        );
    }

    // The upgrade's check lets it through with room for its writes above
    // the old manager's line and the margin: it reaches its download, which
    // fails here, having no release to fetch. Its "binary" is this test's,
    // far larger than a release's, so the disk is given room for that (a
    // release's sizes at the line are in the unit tests and below).
    let test_binary = std::fs::metadata(AutoApplyUpgrader::current_binary_path().unwrap())
        .unwrap()
        .len();
    partition.fill_to(
        OLD_MANAGER_LINE + UPGRADE_SPACE_MARGIN + 2 * ARCHIVE + 2 * test_binary + 64 * MIB,
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    let upgrade = UpgradeInfo {
        version: semver::Version::parse("0.23.0+ds.99").unwrap(),
        download_url: "not a url".to_string(),
        signature_url: "not a url".to_string(),
        release_notes: String::new(),
        published_at: None,
        archive_size: Some(ARCHIVE),
    };
    match AutoApplyUpgrader::new()
        .apply_upgrade(&upgrade)
        .await
        .unwrap()
    {
        UpgradeResult::RolledBack { reason } => {
            assert!(reason.contains("Download failed"), "{reason}");
        }
        other => panic!("at the line the upgrade must reach its download: {other:?}"),
    }
    // Its writes, at a release's sizes, leave the disk clear of the old
    // manager's line.
    let upgrade_dir = nodes[0].root.join("upgrade");
    std::fs::create_dir_all(&upgrade_dir).unwrap();
    for (name, len) in [
        ("archive", ARCHIVE),
        ("archive-cached", ARCHIVE),
        ("ant-node", BINARY),
        ("ant-node.backup", BINARY),
    ] {
        std::fs::write(
            upgrade_dir.join(name),
            vec![0u8; usize::try_from(len).unwrap()],
        )
        .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let lowest = partition.lowest();
    assert!(
        lowest > OLD_MANAGER_LINE,
        "free space fell to {} MiB with the upgrade's writes",
        lowest / MIB
    );
    std::fs::remove_dir_all(&upgrade_dir).unwrap();

    // A disk without room for the upgrade's writes skips the cycle before
    // anything is written.
    partition.fill_to(50 * MIB);
    match AutoApplyUpgrader::new()
        .apply_upgrade(&upgrade)
        .await
        .unwrap()
    {
        UpgradeResult::NotEnoughSpace { needed, available } => {
            assert!(needed > available, "{needed} > {available}");
        }
        other => panic!("a short disk must skip the upgrade: {other:?}"),
    }

    // Four nodes of one machine upgrading at once at the full line take
    // turns, each reading the free space the one before left; each upgrade
    // that goes through keeps its writes, as a cached archive and a backup
    // stay. Those the disk has no more room for are refused, and the disk
    // stays clear of the old manager's line.
    partition.fill_to(DEFAULT_DISK_RESERVE);
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The lowest free space from here on: the disk was taken to 50 MiB on
    // purpose above.
    partition.lowest.store(u64::MAX, Ordering::SeqCst);
    let cache_dir = dir.path().join("upgrade-cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let upgrades: Vec<_> = (0..4)
        .map(|n| {
            let (cache_dir, root) = (cache_dir.clone(), root.clone());
            tokio::spawn(async move {
                let upgrader =
                    AutoApplyUpgrader::new().with_binary_cache(BinaryCache::new(cache_dir));
                match upgrader.room_to_apply(UPGRADE_WRITES, &root).await {
                    Ok(turn) => {
                        let writes = root.join(format!("upgrade-{n}"));
                        std::fs::write(
                            &writes,
                            vec![0u8; usize::try_from(UPGRADE_WRITES).unwrap()],
                        )
                        .unwrap();
                        // The partition sees the writes before the turn passes.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        drop(turn);
                        Some(writes)
                    }
                    Err(UpgradeResult::NotEnoughSpace { .. }) => None,
                    Err(other) => panic!("an upgrade's turn: {other:?}"),
                }
            })
        })
        .collect();
    let mut applied = Vec::new();
    for upgrade in upgrades {
        applied.extend(upgrade.await.unwrap());
    }
    assert!(!applied.is_empty(), "a node at the full line upgrades");
    assert!(applied.len() < 4, "the disk took four upgrades at the line");
    let lowest = partition.lowest();
    assert!(
        lowest > OLD_MANAGER_LINE,
        "free space fell to {} MiB with upgrades at once",
        lowest / MIB
    );
    for writes in applied {
        std::fs::remove_file(writes).unwrap();
    }

    // 600 MiB free has no room for an upgrade above the old manager's line.
    partition.fill_to(600 * MIB);
    tokio::time::sleep(Duration::from_millis(100)).await;
    match AutoApplyUpgrader::new()
        .room_to_apply(UPGRADE_WRITES, &root)
        .await
    {
        Err(UpgradeResult::NotEnoughSpace { available, .. }) => {
            assert!(available <= 600 * MIB, "{available}");
        }
        Err(other) => panic!("600 MiB free must skip the upgrade: {other:?}"),
        Ok(_) => panic!("600 MiB free must skip the upgrade"),
    }

    // Space returns: each writer says what it dropped.
    partition.fill_to(2 * GIB);
    let deadline = Instant::now() + Duration::from_secs(60);
    while nodes.iter().any(|node| {
        !node
            .log_text()
            .contains("Log lines dropped while the disk was at its full line")
    }) {
        assert!(Instant::now() < deadline, "no line says what was dropped");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    stop.store(true, Ordering::SeqCst);
    for flood in floods {
        flood.join().unwrap();
    }
    for node in &nodes {
        let total: u64 = node.log_files().iter().map(|(_, len)| len).sum();
        assert!(
            total <= 500 * MIB,
            "{} holds {total} bytes",
            node.logs.display()
        );
    }

    // Below the line, with the logs quiet, no new chunk is stored.
    partition.fill_to(DEFAULT_DISK_RESERVE - MIB);
    tokio::time::sleep(Duration::from_secs(6)).await;
    for node in &nodes {
        let content = b"a chunk at the line".repeat(64);
        let address = *blake3::hash(&content).as_bytes();
        let refused = node_put(&node.chunks, &address, &content)
            .await
            .unwrap_err();
        assert!(
            refused.to_string().contains("Insufficient disk space"),
            "{refused}"
        );
    }

    partition.stop.store(true, Ordering::SeqCst);
    publisher.join().unwrap();
    std::env::remove_var(FREE_SPACE_FILE);
}

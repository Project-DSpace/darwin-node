//! Full, not faulty: how a full node tells its peers, and how they
//! treat it.
//!
//! A node is full when it is at the full line and its last prune passes, at
//! the falling floor's last step, erased nothing and left nothing selected for
//! erasure and not yet erased (see [`super::retention`], [`after_passes`]).
//! A full node keeps serving and proving what it holds, prices no new data,
//! refuses every new record, and tells its peers with a one-way "full" notice
//! ([`FullNotice`]), renewed while it stays full. Its audit and possession
//! replies keep their format: real digests for the keys it holds, the absent
//! digest for the keys it lacks.
//!
//! Its peers:
//!
//! - **Mark it full** only from its own notice. The mark is held locally and
//!   lapses once the notice is older than [`FULL_NOTICE_FRESHNESS`], unless a
//!   new one renews it ([`FullMarks`]).
//! - **Widen their windows around it** ([`widened_len`]). With f peers marked
//!   full among a key's nearest, hint and fresh-offer targets are the 7 + f
//!   nearest, and the storage admission and retention width is 9 + f, both
//!   capped at [`MAX_WIDENED_WIDTH`] (20), the size of the paid-list group
//!   and of the client's set of nearest nodes (built from the existing
//!   widths). The nodes standing in for the full node are in
//!   range, not erase candidates, and the full node stays responsible for
//!   what it holds.
//! - **Erase near it** only with the proofs of the 7 nearest nodes not marked
//!   full (6 of them): a full node that lacks the key is neither a proof nor
//!   a veto, a full node that holds it still proves it.
//! - **Waive the unheld-key penalty** for it in one bounded case, through one
//!   function, [`FullView::waives_unheld_penalty`]: the peer's notice is
//!   fresh, its answer was that it lacks the key, the check isn't pinned to
//!   its commitment, this node hasn't seen it prove it holds that key (in the
//!   two generations of [`SeenHolders`] it reads), and,
//!   on the possession check after a write, its own quote isn't among the paid
//!   quotes of the proof. Everything else is charged as before. The bound on
//!   how long a node may answer "full" without a penalty is how long one
//!   notice stays fresh: a node that keeps renewing it, prices nothing and
//!   passes audits of what it holds is never penalised for being full.
//!
//! The notice goes only to peers known to read it: a peer whose user agent
//! names [`FULL_NOTICE_FROM_RELEASE`] or a later release, or one that has
//! sent a notice itself. Older nodes drop it, keep charging a full node as
//! before, and don't widen their windows until they update.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use saorsa_core::identity::PeerId;

use crate::ant_protocol::XorName;
use crate::replication::config::{NEIGHBOR_SYNC_INTERVAL_MAX, PAID_LIST_CLOSE_GROUP_SIZE};

pub use crate::replication::protocol::FullNotice;

/// How long one "full" notice stays fresh: two of the longest neighbour-sync
/// intervals.
///
/// This is the bound on how long a node may answer "full" without the
/// unheld-key penalty: a peer's mark lapses this long after its last notice,
/// and the penalty applies again from then.
pub const FULL_NOTICE_FRESHNESS: Duration =
    Duration::from_secs(2 * NEIGHBOR_SYNC_INTERVAL_MAX.as_secs());

/// How many notices a full node sends inside one freshness window: it renews
/// at this fraction of [`FULL_NOTICE_FRESHNESS`], so a mark survives three
/// lost notices in a row.
pub const FULL_NOTICE_RENEWALS_PER_WINDOW: u32 = 4;

/// The first release whose nodes read the "full" notice: its `+ds.<n>` build
/// number.
///
/// A full node sends the notice to a peer whose user agent names this release
/// or a later one, and to a peer that has sent one itself.
pub const FULL_NOTICE_FROM_RELEASE: u64 = 17;

/// The most a widened window grows to: the paid-list group, and the client's
/// set of nearest nodes.
pub const MAX_WIDENED_WIDTH: usize = PAID_LIST_CLOSE_GROUP_SIZE;

/// The most peers whose "full" mark a node holds at once. A node is told only
/// by its neighbours, far fewer than this; past it the stalest mark goes.
const MAX_FULL_MARKS: usize = 1024;

/// The most peers a node remembers as readers of the notice from their own
/// notices.
const MAX_KNOWN_READERS: usize = 4096;

/// How long one "full" notice stays fresh in this process: the
/// `test-utils` seam's value when a test set one, otherwise
/// [`FULL_NOTICE_FRESHNESS`].
#[must_use]
pub fn full_notice_freshness() -> Duration {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(freshness) = crate::test_seams::full_freshness() {
        return freshness;
    }
    FULL_NOTICE_FRESHNESS
}

/// How often a full node renews its notice.
#[must_use]
pub fn full_notice_renewal() -> Duration {
    full_notice_freshness() / FULL_NOTICE_RENEWALS_PER_WINDOW
}

/// The first release whose nodes read the notice in this process: the
/// `test-utils` seam's value when a test set one, otherwise
/// [`FULL_NOTICE_FROM_RELEASE`].
#[must_use]
pub fn full_notice_from_release() -> u64 {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(release) = crate::test_seams::full_notice_from_release() {
        return release;
    }
    FULL_NOTICE_FROM_RELEASE
}

/// Whether `user_agent` is a node's of a release that reads the notice:
/// `node/<version>+ds.<n>[...]` with `n` at least `from_release`. An agent of
/// no release this fork numbers counts as old.
#[must_use]
pub fn agent_reads_full_notice(user_agent: &str, from_release: u64) -> bool {
    user_agent
        .strip_prefix("node/")
        .and_then(|version| semver::Version::parse(version).ok())
        .and_then(|version| {
            version
                .build
                .as_str()
                .strip_prefix("ds.")
                .and_then(|release| release.split('.').next())
                .and_then(|release| release.parse::<u64>().ok())
        })
        .is_some_and(|release| release >= from_release)
}

/// How many of `nearest`, sorted nearest first, a window of `base` holds
/// once widened past the peers marked full.
///
/// The nearest prefix that holds `base` entries not marked full, at most
/// [`MAX_WIDENED_WIDTH`]. With f marked full inside it, that is `base + f`.
#[must_use]
pub fn widened_len<T>(nearest: &[T], base: usize, is_full: impl Fn(&T) -> bool) -> usize {
    let mut not_full = 0usize;
    for (index, entry) in nearest.iter().enumerate() {
        if index >= MAX_WIDENED_WIDTH || not_full >= base {
            return index;
        }
        if !is_full(entry) {
            not_full += 1;
        }
    }
    nearest.len().min(MAX_WIDENED_WIDTH)
}

/// The peers marked full from their own notices, and the peers known to read
/// the notice: they sent one, or their user agent was once seen naming a
/// release that reads it.
#[derive(Debug, Default)]
pub struct FullMarks {
    /// When each peer marked full last sent its notice.
    marks: Mutex<HashMap<PeerId, Instant>>,
    /// Peers known to read the notice, with when that was last seen.
    readers: Mutex<HashMap<PeerId, Instant>>,
}

impl FullMarks {
    /// Note `peer`'s own notice, received at `now`: it is full, or it has
    /// left "full". Only the peer's own notice reaches this.
    pub fn note_notice(&self, peer: PeerId, full: bool, now: Instant) {
        {
            let mut readers = self.readers.lock();
            insert_bounded(&mut readers, peer, now, MAX_KNOWN_READERS);
        }
        let mut marks = self.marks.lock();
        if full {
            insert_bounded(&mut marks, peer, now, MAX_FULL_MARKS);
        } else {
            marks.remove(&peer);
        }
    }

    /// Whether `peer` is marked full at `now`: its last notice said full and
    /// is younger than [`full_notice_freshness`]. A lapsed mark counts as no
    /// mark.
    #[must_use]
    pub fn is_marked(&self, peer: &PeerId, now: Instant) -> bool {
        let freshness = full_notice_freshness();
        self.marks
            .lock()
            .get(peer)
            .is_some_and(|noticed| now.saturating_duration_since(*noticed) < freshness)
    }

    /// The peers marked full at `now`, lapsed marks dropped.
    #[must_use]
    pub fn marked(&self, now: Instant) -> Vec<PeerId> {
        let freshness = full_notice_freshness();
        let mut marks = self.marks.lock();
        marks.retain(|_, noticed| now.saturating_duration_since(*noticed) < freshness);
        marks.keys().copied().collect()
    }

    /// [`Self::marked`], as a set.
    #[must_use]
    pub fn marked_set(&self, now: Instant) -> std::collections::HashSet<PeerId> {
        self.marked(now).into_iter().collect()
    }

    /// Note that `peer` reads the notice, from its user agent seen at `now`:
    /// it stays a reader while disconnected, when its user agent can't be
    /// read, until it leaves the routing table ([`Self::forget`]).
    pub fn note_reader(&self, peer: PeerId, now: Instant) {
        insert_bounded(&mut self.readers.lock(), peer, now, MAX_KNOWN_READERS);
    }

    /// Whether `peer` is known to read the notice: it sent one, or its user
    /// agent was seen naming a release that reads it.
    #[must_use]
    pub fn sent_a_notice(&self, peer: &PeerId) -> bool {
        self.readers.lock().contains_key(peer)
    }

    /// Stop counting `peer` as a reader: its user agent now names a release
    /// that doesn't read the notice (it rolled back). Its mark, if any, stays.
    pub fn forget_reader(&self, peer: &PeerId) {
        self.readers.lock().remove(peer);
    }

    /// Whether to send `peer` the notice, from its user agent read now, if
    /// any. An agent read now decides: one that reads the notice makes the
    /// peer a remembered reader, one that doesn't (a rollback to an older
    /// release among them) removes it from the readers. Only when no agent
    /// can be read (the peer is disconnected) does the remembered set decide.
    pub fn tells(
        &self,
        peer: PeerId,
        agent: Option<&str>,
        from_release: u64,
        now: Instant,
    ) -> bool {
        match agent {
            Some(agent) if agent_reads_full_notice(agent, from_release) => {
                self.note_reader(peer, now);
                true
            }
            Some(_) => {
                self.forget_reader(&peer);
                false
            }
            None => self.sent_a_notice(&peer),
        }
    }

    /// Forget `peer`: it left the routing table.
    pub fn forget(&self, peer: &PeerId) {
        self.marks.lock().remove(peer);
        self.readers.lock().remove(peer);
    }

    /// Mark `peer` full as if its notice had arrived at `at`. Tests only.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn mark_for_test(&self, peer: PeerId, at: Instant) {
        self.marks.lock().insert(peer, at);
    }
}

/// Insert into a map bounded at `cap`, dropping the stalest entry first.
fn insert_bounded(map: &mut HashMap<PeerId, Instant>, peer: PeerId, now: Instant, cap: usize) {
    if map.len() >= cap && !map.contains_key(&peer) {
        if let Some(stalest) = map
            .iter()
            .min_by_key(|(_, noticed)| **noticed)
            .map(|(peer, _)| *peer)
        {
            map.remove(&stalest);
        }
    }
    map.insert(peer, now);
}

/// Bits in the smallest generation of the record of keys peers were seen
/// proving: 2 MiB, the record's size before it was sized by the records held.
const SEEN_MIN_BITS: u64 = 1 << 24;
/// Bits in the largest generation: 32 MiB. A node holding more records than
/// that is sized for turns its generations over sooner instead.
const SEEN_MAX_BITS: u64 = 1 << 28;
/// Hashes per entry.
const SEEN_HASHES: u64 = 3;
/// Bits a generation has for each distinct (peer, key) pair it is sized for.
/// Full, its share of bits set is about 26% and its false "seen" about 1.7%.
const SEEN_BITS_PER_PAIR: u64 = 10;
/// The pairs a generation is sized for per record held: every peer that can
/// prove a key to this node sits within the key's widened window.
const SEEN_PAIRS_PER_RECORD: u64 = MAX_WIDENED_WIDTH as u64;

/// The file, under the node's root, that holds [`SeenHolders`] across
/// restarts.
pub const SEEN_FILE: &str = "seen_holders.bin";
/// The layout of [`SEEN_FILE`]; a file of any other layout starts the record
/// empty.
const SEEN_FORMAT: u8 = 1;

/// Reads a saved [`SeenHolders`] front to back.
struct SavedReader<'a>(&'a [u8]);

impl<'a> SavedReader<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let (taken, rest) = self.0.split_at_checked(len)?;
        self.0 = rest;
        Some(taken)
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    /// Whether a generation was saved next: `None` when the byte is
    /// neither.
    fn present(&mut self) -> Option<bool> {
        match self.take(1)? {
            [0] => Some(false),
            [1] => Some(true),
            _ => None,
        }
    }

    /// One saved generation, or `None` when the bytes aren't one.
    fn generation(&mut self) -> Option<Arc<SeenGeneration>> {
        let bits = self.u64()?;
        let capacity = self.u64()?;
        let distinct = self.u64()?;
        if !(SEEN_MIN_BITS..=SEEN_MAX_BITS).contains(&bits)
            || bits % 64 != 0
            || capacity != bits / SEEN_BITS_PER_PAIR
        {
            return None;
        }
        let words = self
            .take(usize::try_from(bits / 8).ok()?)?
            .as_chunks::<8>()
            .0
            .iter()
            .map(|word| AtomicU64::new(u64::from_le_bytes(*word)))
            .collect();
        Some(Arc::new(SeenGeneration {
            words,
            bits,
            capacity,
            distinct: AtomicU64::new(distinct),
        }))
    }
}

/// Write `bytes` to `path` through a temporary file beside it, synced and then
/// renamed over `path`, so a crash leaves the old file or the new one whole.
fn write_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut temporary = PathBuf::from(path);
    temporary.set_extension("bin.tmp");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary, path)
}

/// One generation of [`SeenHolders`]: a Bloom filter sized when it starts.
struct SeenGeneration {
    words: Box<[AtomicU64]>,
    /// Bits in `words`.
    bits: u64,
    /// The distinct pairs it is sized for; the next generation starts once it
    /// holds this many.
    capacity: u64,
    /// Distinct pairs recorded: records that set at least one bit.
    distinct: AtomicU64,
}

impl SeenGeneration {
    fn sized_for(records: u64) -> Self {
        let bits = records
            .saturating_mul(SEEN_PAIRS_PER_RECORD)
            .saturating_mul(SEEN_BITS_PER_PAIR)
            .clamp(SEEN_MIN_BITS, SEEN_MAX_BITS)
            .next_multiple_of(64);
        let words = usize::try_from(bits / 64).unwrap_or(usize::MAX);
        Self {
            words: (0..words).map(|_| AtomicU64::new(0)).collect(),
            bits,
            capacity: bits / SEEN_BITS_PER_PAIR,
            distinct: AtomicU64::new(0),
        }
    }

    fn word_and_mask(&self, position: u64) -> Option<(&AtomicU64, u64)> {
        let word = self.words.get(usize::try_from(position / 64).ok()?)?;
        Some((word, 1 << (position % 64)))
    }

    /// Set the pair's bits. Counts it as distinct, and returns `true`, when
    /// any bit was unset.
    fn insert(&self, positions: impl Iterator<Item = u64>) -> bool {
        let mut new = false;
        for position in positions {
            if let Some((word, mask)) = self.word_and_mask(position) {
                new |= word.fetch_or(mask, Ordering::Relaxed) & mask == 0;
            }
        }
        if new {
            self.distinct.fetch_add(1, Ordering::Relaxed);
        }
        new
    }

    fn contains(&self, mut positions: impl Iterator<Item = u64>) -> bool {
        positions.all(|position| {
            self.word_and_mask(position)
                .is_some_and(|(word, mask)| word.load(Ordering::Relaxed) & mask != 0)
        })
    }

    /// The share of its bits set.
    #[allow(clippy::cast_precision_loss)] // a ratio for the log
    fn fill(&self) -> f64 {
        let set: u64 = self
            .words
            .iter()
            .map(|word| u64::from(word.load(Ordering::Relaxed).count_ones()))
            .sum();
        set as f64 / self.bits as f64
    }
}

#[derive(Default)]
struct SeenGenerations {
    /// Where pairs are recorded.
    current: Option<Arc<SeenGeneration>>,
    /// The one before, read until the next turns over.
    previous: Option<Arc<SeenGeneration>>,
}

/// How full [`SeenHolders`] is, for the prune-pass line.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SeenFill {
    /// The share of the current generation's bits set.
    pub fill: f64,
    /// The estimated rate of false "seen" answers, both generations read:
    /// from each generation's fill to the power of the hashes per entry.
    pub false_seen: f64,
}

/// The keys this node has seen each peer prove it holds: a digest matched
/// against this node's own copy, or a fetch served with the right bytes.
///
/// A Bloom filter in two generations, sized from the records this node holds
/// ([`Self::size_for_records`]: 10 bits for each of 20 peers per record,
/// between 2 and 32 MiB). Pairs go into the current generation; once it holds
/// as many distinct pairs as it was sized for, a new one starts and the old
/// one is still read until the next turns over, then dropped. So a proof is
/// remembered for at least one generation's worth of other proofs, and a pair
/// proved again is remembered afresh. It can answer "seen" for a pair it never
/// saw, and that charges the penalty, as for any node; the generations keep
/// that rate under about 3.5%, both read, where a filter that only filled
/// would charge more and more often over uptime. It survives restarts: its
/// hash seed and both generations are saved in the file [`SEEN_FILE`] under
/// the node's root after each prune pass that changed it, and loaded at start
/// ([`Self::load`], [`Self::save`]), so only turnover forgets a proof.
pub struct SeenHolders {
    /// The key of the hash that places a pair's bits, saved with the bits so
    /// a reloaded record reads the same positions.
    seed: [u8; 32],
    generations: RwLock<SeenGenerations>,
    /// The records this node held at the last prune pass, for sizing the
    /// next generation.
    records: AtomicU64,
    /// Whether anything changed since the last save.
    changed: AtomicBool,
}

impl Default for SeenHolders {
    fn default() -> Self {
        Self::with_seed(rand::random())
    }
}

impl std::fmt::Debug for SeenHolders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let distinct = self
            .generations
            .read()
            .current
            .as_ref()
            .map(|current| current.distinct.load(Ordering::Relaxed));
        f.debug_struct("SeenHolders")
            .field("distinct", &distinct)
            .finish_non_exhaustive()
    }
}

impl SeenHolders {
    fn with_seed(seed: [u8; 32]) -> Self {
        Self {
            seed,
            generations: RwLock::new(SeenGenerations::default()),
            records: AtomicU64::new(0),
            changed: AtomicBool::new(false),
        }
    }

    fn positions(&self, peer: &PeerId, key: &XorName, bits: u64) -> impl Iterator<Item = u64> {
        let mut hasher = blake3::Hasher::new_keyed(&self.seed);
        hasher.update(peer.as_bytes());
        hasher.update(key);
        let mut first_eight = [0u8; 8];
        first_eight.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
        let hash = u64::from_le_bytes(first_eight);
        let first = hash & 0xFFFF_FFFF;
        let step = (hash >> 32) | 1;
        (0..SEEN_HASHES).map(move |i| first.wrapping_add(i.wrapping_mul(step)) % bits)
    }

    /// Size the generations started from now on for `records` held.
    pub fn size_for_records(&self, records: usize) {
        self.records.store(
            u64::try_from(records).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn current(&self) -> Arc<SeenGeneration> {
        if let Some(current) = self.generations.read().current.as_ref() {
            return Arc::clone(current);
        }
        let mut generations = self.generations.write();
        Arc::clone(generations.current.get_or_insert_with(|| {
            Arc::new(SeenGeneration::sized_for(
                self.records.load(Ordering::Relaxed),
            ))
        }))
    }

    /// Record that `peer` proved it holds `key`.
    pub fn record(&self, peer: &PeerId, key: &XorName) {
        let current = self.current();
        if current.insert(self.positions(peer, key, current.bits)) {
            self.changed.store(true, Ordering::Relaxed);
        }
        if current.distinct.load(Ordering::Relaxed) < current.capacity {
            return;
        }
        // Full: a new generation, the old one read until the next turns over.
        let mut generations = self.generations.write();
        if generations
            .current
            .as_ref()
            .is_some_and(|now| Arc::ptr_eq(now, &current))
        {
            generations.previous = generations.current.take();
            generations.current = Some(Arc::new(SeenGeneration::sized_for(
                self.records.load(Ordering::Relaxed),
            )));
            self.changed.store(true, Ordering::Relaxed);
        }
    }

    /// Whether `peer` may have proved it holds `key` before: `false` only
    /// when it hasn't, to this node's knowledge, within the generations it
    /// still reads.
    #[must_use]
    pub fn may_have_seen(&self, peer: &PeerId, key: &XorName) -> bool {
        let generations = self.generations.read();
        [&generations.current, &generations.previous]
            .iter()
            .filter_map(|generation| generation.as_ref())
            .any(|generation| generation.contains(self.positions(peer, key, generation.bits)))
    }

    /// The record as saved: [`SEEN_FORMAT`], the seed, the records held,
    /// then the current and the previous generation, each a presence byte
    /// and, when present, its bits, capacity and distinct pairs (big-endian)
    /// and its words (little-endian), and last a BLAKE3 hash of all that
    /// came before. `None` when nothing changed since the last call that
    /// returned bytes.
    fn encode_if_changed(&self) -> Option<Vec<u8>> {
        if !self.changed.swap(false, Ordering::Relaxed) {
            return None;
        }
        let generations = self.generations.read();
        let words: usize = [&generations.current, &generations.previous]
            .iter()
            .filter_map(|generation| generation.as_ref())
            .map(|generation| generation.words.len())
            .sum();
        let mut bytes = Vec::with_capacity(1 + 32 + 8 + 2 * (1 + 24) + words * 8 + 32);
        bytes.push(SEEN_FORMAT);
        bytes.extend_from_slice(&self.seed);
        bytes.extend_from_slice(&self.records.load(Ordering::Relaxed).to_be_bytes());
        for generation in [&generations.current, &generations.previous] {
            let Some(generation) = generation else {
                bytes.push(0);
                continue;
            };
            bytes.push(1);
            bytes.extend_from_slice(&generation.bits.to_be_bytes());
            bytes.extend_from_slice(&generation.capacity.to_be_bytes());
            bytes.extend_from_slice(&generation.distinct.load(Ordering::Relaxed).to_be_bytes());
            for word in &generation.words {
                bytes.extend_from_slice(&word.load(Ordering::Relaxed).to_le_bytes());
            }
        }
        drop(generations);
        let check = blake3::hash(&bytes);
        bytes.extend_from_slice(check.as_bytes());
        Some(bytes)
    }

    /// The record saved as `bytes`, or `None` when they aren't a whole record
    /// of this layout.
    fn decode(bytes: &[u8]) -> Option<Self> {
        let (body, check) = bytes.split_at_checked(bytes.len().checked_sub(32)?)?;
        if blake3::hash(body).as_bytes() != check {
            return None;
        }
        let mut reader = SavedReader(body);
        if reader.take(1)? != [SEEN_FORMAT] {
            return None;
        }
        let seed: [u8; 32] = reader.take(32)?.try_into().ok()?;
        let record = Self::with_seed(seed);
        record.records.store(reader.u64()?, Ordering::Relaxed);
        let current = if reader.present()? {
            Some(reader.generation()?)
        } else {
            None
        };
        let previous = if reader.present()? {
            Some(reader.generation()?)
        } else {
            None
        };
        if !reader.0.is_empty() {
            return None;
        }
        *record.generations.write() = SeenGenerations { current, previous };
        Some(record)
    }

    /// The record saved under `root_dir`, or an empty one with a new seed
    /// when there is none or it can't be read (logged).
    #[must_use]
    pub fn load(root_dir: &Path) -> Self {
        let path = root_dir.join(SEEN_FILE);
        match std::fs::read(&path) {
            Ok(bytes) => Self::decode(&bytes).unwrap_or_else(|| {
                crate::logging::warn!(
                    "The record of keys peers were seen proving at {} can't be read; it starts \
                     empty",
                    path.display()
                );
                Self::default()
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                crate::logging::info!(
                    "No saved record of keys peers were seen proving; it starts empty"
                );
                Self::default()
            }
            Err(e) => {
                crate::logging::warn!(
                    "The record of keys peers were seen proving at {} can't be read: {e}; it \
                     starts empty",
                    path.display()
                );
                Self::default()
            }
        }
    }

    /// Save the record under `root_dir` when it changed since the last save:
    /// written to a temporary file, then renamed over the saved one. A write
    /// that fails is logged and tried again after the next pass.
    ///
    /// The temporary file is charged to `storage`'s capacity guard while it
    /// exists, as a replacing write (it takes the old file's place): the
    /// node's own writes and its free-space estimate, which the log writer
    /// reads, count it, and it isn't written when the disk lacks the room
    /// for it, full line or not.
    pub async fn save(&self, root_dir: &Path, storage: &crate::storage::ChunkStore) {
        self.save_with(root_dir, |bytes| storage.reserve_replacement(bytes))
            .await;
    }

    /// [`Self::save`], with `reserve` charging the temporary file's bytes
    /// for as long as what it returns is held. A refused charge is logged,
    /// and the save tried again after the next pass.
    async fn save_with<R>(
        &self,
        root_dir: &Path,
        reserve: impl FnOnce(u64) -> crate::error::Result<R>,
    ) {
        let Some(bytes) = self.encode_if_changed() else {
            return;
        };
        let reservation = match reserve(bytes.len() as u64) {
            Ok(reservation) => reservation,
            Err(e) => {
                self.changed.store(true, Ordering::Relaxed);
                crate::logging::warn!(
                    "Not saving the record of keys peers were seen proving now: {e}"
                );
                return;
            }
        };
        let path = root_dir.join(SEEN_FILE);
        let written = tokio::task::spawn_blocking(move || write_atomically(&path, &bytes)).await;
        drop(reservation);
        let failed = match written {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e.to_string()),
            Err(e) => Some(e.to_string()),
        };
        if let Some(e) = failed {
            self.changed.store(true, Ordering::Relaxed);
            crate::logging::warn!("Failed to save the record of keys peers were seen proving: {e}");
        }
    }

    /// How full the record is, and its estimated rate of false "seen".
    #[must_use]
    pub fn fill(&self) -> SeenFill {
        let generations = self.generations.read();
        let fill = |generation: &Option<Arc<SeenGeneration>>| {
            generation
                .as_ref()
                .map_or(0.0, |generation| generation.fill())
        };
        let current = fill(&generations.current);
        let false_rate = |fill: f64| fill * fill * fill;
        // Either generation answering falsely: a + b - ab.
        let (a, b) = (false_rate(current), false_rate(fill(&generations.previous)));
        SeenFill {
            fill: current,
            false_seen: a.mul_add(-b, a + b),
        }
    }
}

/// The lanes that charge a peer for not holding a key it should, as the lane
/// census lists them (`src/replication/retention/census.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// A fetch a verified source couldn't serve.
    Fetch,
    /// A responsible-chunk audit the peer failed.
    ResponsibleAudit,
    /// A sole-source replica hint its sender then denied.
    ReplicaHint,
    /// The possession check after a write, unanswered.
    PossessionNoAnswer,
    /// The possession check after a write, answered wrongly.
    PossessionFailed,
    /// A prune audit the peer failed.
    PruneAudit,
    /// The pointer possession check after a write, answered with no state of
    /// the pointer at all: a new address, which a full node refuses.
    PointerPossession,
}

impl Lane {
    /// Every lane, in the census's order.
    pub const ALL: [Self; 7] = [
        Self::Fetch,
        Self::ResponsibleAudit,
        Self::ReplicaHint,
        Self::PossessionNoAnswer,
        Self::PossessionFailed,
        Self::PruneAudit,
        Self::PointerPossession,
    ];

    /// The lane's name in logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::ResponsibleAudit => "responsible_audit",
            Self::ReplicaHint => "replica_hint",
            Self::PossessionNoAnswer => "possession_no_answer",
            Self::PossessionFailed => "possession_failed",
            Self::PruneAudit => "prune_audit",
            Self::PointerPossession => "pointer_possession",
        }
    }
}

/// What the peer answered about the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnheldAnswer {
    /// That it doesn't hold the key: the absent digest, "not found", or a
    /// denial in a verification answer.
    Absent,
    /// Anything else: no answer, a wrong digest, an error, a malformed or
    /// refused reply.
    Other,
}

/// Whether the peer's own quote is among the paid quotes of the proof the
/// checker holds, on the possession check after a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnQuote {
    /// Not a check after a write: there is no proof to look in.
    NoProof,
    /// The proof's paid quotes are known, and the peer's isn't among them.
    NotPaid,
    /// The peer's quote is among the paid quotes, or the proof couldn't be
    /// read: the peer is charged.
    PaidOrUnknown,
}

/// One case of a peer not holding a key it should, on one lane.
#[derive(Debug, Clone, Copy)]
pub struct UnheldCase<'a> {
    /// The lane that found it.
    pub lane: Lane,
    /// The peer.
    pub peer: &'a PeerId,
    /// The key.
    pub key: &'a XorName,
    /// What the peer answered.
    pub answer: UnheldAnswer,
    /// Whether the check is pinned to the peer's commitment.
    pub commitment_pinned: bool,
    /// Whether the peer was paid for the key, on a check after a write.
    pub own_quote: OwnQuote,
}

/// What a node knows of its peers' "full" state: the marks, and the keys it
/// has seen each peer prove it holds.
#[derive(Debug, Default)]
pub struct FullView {
    /// The peers marked full.
    pub marks: FullMarks,
    /// The keys peers were seen proving.
    pub seen: SeenHolders,
    /// Keys whose verification found fewer than 4 holders present, since the
    /// last prune pass took the count.
    few_holders: AtomicU64,
    /// What this process told its peers about its own "full" state, and the
    /// order it tells them in.
    pub announcer: FullAnnouncer,
}

/// What this process told its peers about its own "full" state.
///
/// Every notice this node sends goes through [`Self::announce`], which reads
/// the state and sends it under one lock: a renewal that read "full" can't
/// send it after the erasure task has told the peers the node left "full".
///
/// A node at the line at start says so before the first prune pass that
/// decides, and at each renewal until one does (see
/// [`Self::renewal_when_not_full`]). Passes that deferred candidates because
/// bootstrap hasn't drained don't decide: the node may erase those once it
/// has. If the deciding pass then doesn't put the node in "full", or the node
/// leaves the line first, the next renewal takes the notice back, so its
/// peers don't keep widening and waiving for it until the notice goes stale.
#[derive(Debug, Default)]
pub struct FullAnnouncer {
    /// Held across reading the state and sending it.
    send: tokio::sync::Mutex<()>,
    /// Set once a prune pass of the erasure task has decided whether the node
    /// is full ([`Self::note_pass`]).
    first_pass_done: AtomicBool,
    /// Set when the at-start "full" was sent and not yet taken back or
    /// confirmed by a pass that put the node in "full".
    announced_at_start: AtomicBool,
}

impl FullAnnouncer {
    /// Under the lock, read what to send (`None`: nothing) and send it.
    /// Returns what `send` returned, or `None` when nothing was sent.
    pub async fn announce<R, S, F>(&self, read: R, send: S) -> Option<usize>
    where
        R: FnOnce(&Self) -> Option<bool>,
        S: FnOnce(bool) -> F,
        F: std::future::Future<Output = usize>,
    {
        let _send = self.send.lock().await;
        let full = read(self)?;
        Some(send(full).await)
    }

    /// The erasure task's prune passes finished, deferring
    /// `bootstrap_deferred` candidates because bootstrap hasn't drained, and
    /// changing the node's state when `changed` is `Some`.
    ///
    /// Passes that deferred candidates and changed nothing haven't decided
    /// whether the node is full ([`after_passes`] doesn't enter "full" while
    /// any are deferred): the at-start notice is neither skipped nor taken
    /// back for them, and the first pass after bootstrap drains decides.
    pub fn note_pass(&self, bootstrap_deferred: usize, changed: Option<bool>) {
        if bootstrap_deferred == 0 || changed.is_some() {
            self.first_pass_done.store(true, Ordering::Release);
        }
    }

    /// A renewal while the node is in "full": its own notice says so, and
    /// confirms any at-start one.
    pub fn renewal_when_full(&self) {
        self.announced_at_start.store(false, Ordering::Release);
    }

    /// What a renewal sends while the node isn't in "full", if anything:
    /// "full" while the node started at the line and is still at it
    /// (`at_the_line_at_start`) and no pass has decided yet
    /// ([`Self::note_pass`]), so its peers' marks don't lapse while bootstrap
    /// defers the passes; "not full" once, if it sent that and a pass has
    /// since decided without putting it in "full", or it has left the line;
    /// else nothing.
    pub fn renewal_when_not_full(&self, at_the_line_at_start: bool) -> Option<bool> {
        let pass_done = self.first_pass_done.load(Ordering::Acquire);
        if at_the_line_at_start && !pass_done {
            self.announced_at_start.store(true, Ordering::Release);
            return Some(true);
        }
        if self.announced_at_start.swap(false, Ordering::AcqRel) {
            return Some(false);
        }
        None
    }

    /// A prune pass put the node in "full", or took it out: its own notice
    /// says so, and settles any at-start one.
    pub fn state_sent(&self) {
        self.announced_at_start.store(false, Ordering::Release);
    }
}

impl FullView {
    /// A view with no marks, and `seen` as the keys peers were seen proving.
    #[must_use]
    pub fn with_seen(seen: SeenHolders) -> Self {
        Self {
            seen,
            ..Self::default()
        }
    }

    /// Note `count` keys whose verification found fewer than 4 holders.
    pub fn note_few_holders(&self, count: u64) {
        self.few_holders.fetch_add(count, Ordering::Relaxed);
    }

    /// The keys found with fewer than 4 holders since the last call.
    pub fn take_few_holders(&self) -> u64 {
        self.few_holders.swap(0, Ordering::Relaxed)
    }

    /// Whether the unheld-key penalty is waived for `case` at `now`.
    ///
    /// The one place every lane asks. Waived only when all of these hold: the
    /// peer's "full" notice is fresh; it answered that it lacks the key; the
    /// check isn't pinned to its commitment; this node has never seen it
    /// prove it holds that key; and, on the possession check after a write,
    /// its own quote isn't among the paid quotes of the proof. A node paid for
    /// a record it then can't take is charged, and so is everything else.
    #[must_use]
    pub fn waives_unheld_penalty(&self, case: &UnheldCase<'_>, now: Instant) -> bool {
        let waived = case.answer == UnheldAnswer::Absent
            && !case.commitment_pinned
            && case.own_quote != OwnQuote::PaidOrUnknown
            && self.marks.is_marked(case.peer, now)
            && !self.seen.may_have_seen(case.peer, case.key);
        if waived {
            crate::logging::debug!(
                lane = case.lane.as_str(),
                peer = %case.peer,
                key = %hex::encode(case.key),
                "Not charging a full peer for a key it doesn't hold: its full notice is fresh, \
                 it was never seen holding the key, and it was not paid for it"
            );
        }
        waived
    }
}

static VIEWS: std::sync::LazyLock<Mutex<HashMap<[u8; 32], std::sync::Weak<FullView>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Register `view` as what the node `self_id` knows of its peers' "full"
/// state, for the code that reaches it by the node's id (the lanes, the
/// prune passes, and the paths that widen windows).
pub fn register_view(self_id: &PeerId, view: &std::sync::Arc<FullView>) {
    let mut views = VIEWS.lock();
    views.retain(|_, view| view.strong_count() > 0);
    views.insert(*self_id.as_bytes(), std::sync::Arc::downgrade(view));
}

/// What the node `self_id` knows of its peers' "full" state: an empty view,
/// with no marks, for a node with no replication engine running, so it
/// widens nothing and waives nothing.
#[must_use]
pub fn view_for(self_id: &PeerId) -> std::sync::Arc<FullView> {
    VIEWS
        .lock()
        .get(self_id.as_bytes())
        .and_then(std::sync::Weak::upgrade)
        .unwrap_or_default()
}

/// The paid quotes of a proof, as the issuers' peer ids.
///
/// Every quote at the median price, the one the client pays for (more than
/// one when prices tie). `None` when the proof can't be read as a single-node
/// proof.
#[must_use]
pub fn paid_quote_issuers(proof_of_payment: &[u8]) -> Option<Vec<PeerId>> {
    let (proof, _) = crate::payment::deserialize_proof(proof_of_payment).ok()?;
    let mut prices: Vec<_> = proof
        .peer_quotes
        .iter()
        .map(|(_, quote)| quote.price)
        .collect();
    if prices.is_empty() {
        return None;
    }
    prices.sort_unstable();
    let median = *prices.get(prices.len() / 2)?;
    Some(
        proof
            .peer_quotes
            .iter()
            .filter(|(_, quote)| quote.price == median)
            .map(|(peer, _)| PeerId::from_bytes(*peer.as_bytes()))
            .collect(),
    )
}

/// The [`OwnQuote`] of `peer`, for a check after a write whose proof's paid
/// quotes are `paid` (`None`: unreadable).
#[must_use]
pub fn own_quote(peer: &PeerId, paid: Option<&[PeerId]>) -> OwnQuote {
    match paid {
        Some(paid) if !paid.contains(peer) => OwnQuote::NotPaid,
        _ => OwnQuote::PaidOrUnknown,
    }
}

/// The targets of replica hints and fresh offers for `key`: its close group,
/// widened past the peers marked full.
///
/// From `p2p_node`'s local routing table, the node itself included, nearest
/// first, widened by one for each peer in `marked` (marked full) inside it, at
/// most [`MAX_WIDENED_WIDTH`]. With no peer marked, the close group.
pub async fn widened_close_group_with<S: BuildHasher + Sync>(
    p2p_node: &saorsa_core::P2PNode,
    key: &XorName,
    close_group_size: usize,
    marked: &std::collections::HashSet<PeerId, S>,
) -> Vec<PeerId> {
    let dht = p2p_node.dht_manager();
    if marked.is_empty() {
        return dht
            .find_closest_nodes_local_with_self(key, close_group_size)
            .await
            .into_iter()
            .map(|node| node.peer_id)
            .collect();
    }
    let mut nearest: Vec<PeerId> = dht
        .find_closest_nodes_local_with_self(key, MAX_WIDENED_WIDTH)
        .await
        .into_iter()
        .map(|node| node.peer_id)
        .collect();
    let width = widened_len(&nearest, close_group_size, |peer| marked.contains(peer));
    nearest.truncate(width);
    nearest
}

/// [`widened_close_group_with`] the peers `p2p_node` marks full now.
pub async fn widened_close_group(
    p2p_node: &saorsa_core::P2PNode,
    key: &XorName,
    close_group_size: usize,
) -> Vec<PeerId> {
    let marked = view_for(p2p_node.peer_id())
        .marks
        .marked_set(Instant::now());
    widened_close_group_with(p2p_node, key, close_group_size, &marked).await
}

/// How many of its nearest peers a full node tells: two k-buckets, so the
/// nodes that check what it holds hear it, prune auditors a little farther
/// out among them.
pub const FULL_NOTICE_PEERS: usize = 2 * crate::replication::config::K_BUCKET_SIZE;

/// After a node's prune passes of one epoch, enter or leave "full".
///
/// A node enters "full" at the line when the passes ran at the falling
/// floor's last step, erased nothing (`erased`) and left no key selected for
/// erasure and not yet erased (`not_yet_erased`, see
/// [`super::erasure::ErasureGate::not_yet_erased`]): still in its wait,
/// deferred, answered "present" lately or kept. Nothing it may erase now is
/// left. Records still in their out-of-range wait aren't eligible, and one
/// retried after a failed possession check is tried again later, so neither
/// keeps a node from being full. It leaves only with
/// [`LEAVE_FULL_MARGIN`](crate::replication::retention::LEAVE_FULL_MARGIN) of
/// room above the line. Nor does it enter "full" while the passes deferred
/// candidates because bootstrap hasn't drained (`bootstrap_deferred`: the
/// record pass's deferred candidates and the pointer pass's beyond the
/// floor): it may erase those as soon as it has, so the deferral is not
/// "nothing it may erase". Returns the new state when it changed; the caller
/// tells the node's peers.
pub fn after_passes(
    storage: &crate::storage::ChunkStore,
    retention: &crate::replication::retention::DiskRetention,
    erased: usize,
    not_yet_erased: usize,
    bootstrap_deferred: usize,
) -> Option<bool> {
    if storage.is_full() {
        return leave_if_room(storage, retention);
    }
    let trigger = retention.at_the_line()?;
    if retention.step() != Some(crate::replication::retention::LAST_FLOOR_STEP)
        || erased > 0
        || not_yet_erased > 0
        || bootstrap_deferred > 0
    {
        return None;
    }
    let state = storage.enter_full(trigger)?;
    let since = state
        .since
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let free = retention.last_space().map_or(0, |space| space.available);
    // Kept first at the full line (crate::log_files::KEEP_FIRST_TARGETS),
    // where a node enters "full": `nodes status` reads this line.
    crate::logging::info!(
        target: "ant_node::replication::full_state",
        full = 1,
        since,
        trigger = state.trigger.as_str(),
        free_bytes = free,
        line_bytes = retention.line(),
        "Node full: since {since}, at the full line by {} with nothing it may erase now. Full, \
         not faulty: it prices and takes no new data, keeps serving and proving what it holds, \
         and tells its peers. Add disk, or lower NODE_COUNT",
        state.trigger.as_str()
    );
    Some(true)
}

/// Leave "full" if the node has the room to. Returns `Some(false)` when it left.
///
/// The room is [`LEAVE_FULL_MARGIN`] above the line, measured again now, and
/// inodes off the line, with their own margin when inodes put the node in
/// "full" (see [`may_leave_full`]).
///
/// [`LEAVE_FULL_MARGIN`]: crate::replication::retention::LEAVE_FULL_MARGIN
/// [`may_leave_full`]: crate::replication::retention::may_leave_full
pub fn leave_if_room(
    storage: &crate::storage::ChunkStore,
    retention: &crate::replication::retention::DiskRetention,
) -> Option<bool> {
    let trigger = storage.full_state()?.trigger;
    retention.refresh();
    let space = retention.last_space()?;
    if !crate::replication::retention::may_leave_full(space, retention.line(), trigger) {
        return None;
    }
    let state = storage.leave_full()?;
    let full_for = state.since.elapsed().map_or(0, |d| d.as_secs());
    // Kept first at the full line (crate::log_files::KEEP_FIRST_TARGETS).
    crate::logging::info!(
        target: "ant_node::replication::full_state",
        full = 0,
        full_for_secs = full_for,
        free_bytes = space.available,
        line_bytes = retention.line(),
        "Node full: no longer, after {full_for}s; {} bytes free, room above the full line \
         again, so it prices and takes new data",
        space.available
    );
    Some(false)
}

/// At start, once the disk is read, say whether the node is full.
///
/// A node's "full" state isn't kept across a restart. With room above the line, log one
/// "Node full:" line that clears an older "since" line for `nodes status`
/// (the node prices and takes new data again). At the line, log no clear:
/// the node enters "full" again at a prune pass that finds nothing it may
/// erase. Returns whether it is at the line, so the caller tells its peers
/// once before that pass and their marks don't lapse in between.
pub fn at_start(retention: &crate::replication::retention::DiskRetention) -> bool {
    retention.refresh();
    let Some(space) = retention.last_space() else {
        return false;
    };
    if let Some(trigger) = retention.at_the_line() {
        // Kept first at the full line (crate::log_files::KEEP_FIRST_TARGETS).
        crate::logging::info!(
            target: "ant_node::replication::full_state",
            free_bytes = space.available,
            line_bytes = retention.line(),
            "At the full line by {} at start: the node takes no new data, and enters \"full\" \
             again at a prune pass that finds nothing it may erase",
            trigger.as_str()
        );
        return true;
    }
    // Kept first at the full line (crate::log_files::KEEP_FIRST_TARGETS).
    crate::logging::info!(
        target: "ant_node::replication::full_state",
        full = 0,
        free_bytes = space.available,
        line_bytes = retention.line(),
        "Node full: not at start; {} bytes free, above the full line, so it prices and takes \
         new data",
        space.available
    );
    false
}

/// Tell this node's nearest peers that read it whether it is full.
///
/// Its [`FULL_NOTICE_PEERS`] nearest, each only if its user agent names a
/// release that reads the notice or, when no user agent can be read, it is
/// already known to read it (it sent one, or its user agent did so before).
/// A peer found to read it is remembered, so a renewal still reaches it while
/// it is disconnected and its user agent can't be read (sending dials it); a
/// peer whose user agent now names an older release (it rolled back) isn't
/// told, and is no longer remembered ([`FullMarks::tells`]). One-way.
/// Returns how many were told.
pub async fn send_full_notice(p2p_node: &saorsa_core::P2PNode, full: bool) -> usize {
    use crate::replication::config::REPLICATION_PROTOCOL_ID;
    use crate::replication::protocol::{ReplicationMessage, ReplicationMessageBody};

    let self_id = *p2p_node.peer_id();
    let view = view_for(&self_id);
    let from_release = full_notice_from_release();
    let msg = ReplicationMessage {
        request_id: rand::random(),
        body: ReplicationMessageBody::FullNotice(FullNotice { full }),
    };
    let Ok(bytes) = msg.encode() else {
        return 0;
    };
    let peers = p2p_node
        .dht_manager()
        .find_closest_nodes_local(self_id.as_bytes(), FULL_NOTICE_PEERS)
        .await;
    let mut told = 0usize;
    for node in peers {
        let peer = node.peer_id;
        let agent = p2p_node.transport().peer_user_agent(&peer).await;
        if !view
            .marks
            .tells(peer, agent.as_deref(), from_release, Instant::now())
        {
            continue;
        }
        crate::replication::protocol::record_sent(&bytes);
        if p2p_node
            .send_message(&peer, REPLICATION_PROTOCOL_ID, bytes.clone(), &[])
            .await
            .is_ok()
        {
            told += 1;
        }
    }
    crate::logging::debug!(
        "Told {told} peers that this node is {}full",
        if full { "" } else { "no longer " }
    );
    told
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn peer(n: u8) -> PeerId {
        PeerId::from_bytes([n; 32])
    }

    #[test]
    fn the_freshness_is_two_of_the_longest_sync_intervals() {
        assert_eq!(FULL_NOTICE_FRESHNESS, Duration::from_mins(40));
        assert_eq!(full_notice_renewal(), Duration::from_mins(10));
        assert_eq!(MAX_WIDENED_WIDTH, 20);
    }

    #[test]
    fn nodes_of_the_release_with_the_notice_and_later_read_it() {
        for agent in [
            "node/0.23.0+ds.17",
            "node/0.23.0+ds.17.image.abc123",
            "node/0.24.0+ds.18",
            "node/1.0.0+ds.100",
        ] {
            assert!(agent_reads_full_notice(agent, 17), "{agent}");
        }
        for agent in [
            "node/0.23.0+ds.15",
            "node/0.23.0+ds.16.image.abc123",
            "node/0.23.0",
            "client/0.23.0+ds.17",
            "node/0.23.0+ds.",
            "node/garbage",
            "",
        ] {
            assert!(!agent_reads_full_notice(agent, 17), "{agent}");
        }
        assert_eq!(FULL_NOTICE_FROM_RELEASE, 17);
    }

    /// The notice ships in ds.17 with paged sync: the release after ds.16,
    /// whose nodes neither read pages nor the notice. ds.17 has shipped, so the
    /// release stays ds.17 whatever this tree's version: a node of this tree
    /// tells ds.17 nodes and later, and not ds.16 or older. This tree is cut
    /// at ds.17 or a later release.
    #[test]
    fn the_notice_comes_with_paged_sync_in_the_release_after_ds16() {
        assert_eq!(FULL_NOTICE_FROM_RELEASE, 17);
        assert_eq!(
            FULL_NOTICE_FROM_RELEASE,
            crate::replication::config::NEIGHBOR_SYNC_PAGES_FROM_RELEASE
        );
        let own = crate::replication::sync_pages::release_of(env!("CARGO_PKG_VERSION"))
            .expect("a +ds version");
        assert!(
            own >= FULL_NOTICE_FROM_RELEASE,
            "this tree is ds.{own}: the notice comes from ds.{FULL_NOTICE_FROM_RELEASE}"
        );
        let this_tree = format!("node/{}", env!("CARGO_PKG_VERSION"));
        assert!(agent_reads_full_notice(
            &this_tree,
            FULL_NOTICE_FROM_RELEASE
        ));
        assert!(agent_reads_full_notice(
            "node/0.23.0+ds.17",
            FULL_NOTICE_FROM_RELEASE
        ));
        assert!(!agent_reads_full_notice(
            "node/0.23.0+ds.16",
            FULL_NOTICE_FROM_RELEASE
        ));
    }

    #[test]
    fn a_window_widens_by_one_for_each_full_peer_inside_it() {
        let full = |n: &u8| *n % 2 == 1;
        let nearest: Vec<u8> = (0..30).collect();
        // Odd entries full: 7 not full needs 13 entries (0..=12, six odd).
        assert_eq!(widened_len(&nearest, 7, full), 13);
        // Nobody full: the base.
        assert_eq!(widened_len(&nearest, 7, |_| false), 7);
        assert_eq!(widened_len(&nearest, 9, |_| false), 9);
        // f full among the nearest: base + f.
        for f in 0..=4u8 {
            let marked = |n: &u8| *n < f;
            assert_eq!(widened_len(&nearest, 7, marked), 7 + usize::from(f));
            assert_eq!(widened_len(&nearest, 9, marked), 9 + usize::from(f));
        }
        // Capped at 20.
        assert_eq!(widened_len(&nearest, 9, |n| *n < 15), 20);
        assert_eq!(widened_len(&nearest, 7, |_| true), 20);
        // Short lists end where they end.
        assert_eq!(widened_len(&nearest[..5], 7, |_| false), 5);
    }

    #[test]
    fn a_mark_comes_from_the_peers_notice_and_lapses_unless_renewed() {
        let marks = FullMarks::default();
        let now = Instant::now();
        let a = peer(1);
        assert!(!marks.is_marked(&a, now));
        assert!(!marks.sent_a_notice(&a));

        marks.note_notice(a, true, now);
        assert!(marks.is_marked(&a, now));
        assert!(marks.sent_a_notice(&a));
        let freshness = full_notice_freshness();
        assert!(marks.is_marked(&a, now + freshness.saturating_sub(Duration::from_secs(1))));
        assert!(
            !marks.is_marked(&a, now + freshness),
            "a lapsed mark counts as no mark"
        );
        assert_eq!(marks.marked(now + freshness), Vec::<PeerId>::new());

        // Renewed.
        marks.note_notice(a, true, now + freshness / 2);
        assert!(marks.is_marked(&a, now + freshness));

        // Left "full": cleared at once, still a reader.
        marks.note_notice(a, false, now + freshness);
        assert!(!marks.is_marked(&a, now + freshness));
        assert!(marks.sent_a_notice(&a));
    }

    /// A peer whose user agent was once seen to read the notice stays a
    /// reader without sending one, and isn't marked full by it; it stops
    /// being one when it leaves the routing table.
    #[test]
    fn a_reader_seen_by_its_user_agent_is_remembered_until_forgotten() {
        let marks = FullMarks::default();
        let now = Instant::now();
        assert!(!marks.sent_a_notice(&peer(1)));
        marks.note_reader(peer(1), now);
        assert!(marks.sent_a_notice(&peer(1)));
        assert!(
            !marks.is_marked(&peer(1), now),
            "a reader isn't marked full"
        );
        marks.forget(&peer(1));
        assert!(!marks.sent_a_notice(&peer(1)));
    }

    /// A reader whose user agent later names a release that doesn't read the
    /// notice (it rolled back to ds.16) isn't told, and is
    /// no longer a reader; its mark stays. With no user agent to read, the
    /// remembered readers are told; a peer never seen to read it isn't.
    #[test]
    fn a_reader_that_rolled_back_is_not_told() {
        let marks = FullMarks::default();
        let now = Instant::now();
        assert!(marks.tells(peer(1), Some("node/0.23.0+ds.17"), 17, now));
        assert!(marks.sent_a_notice(&peer(1)));
        // Disconnected: remembered.
        assert!(marks.tells(peer(1), None, 17, now));
        marks.mark_for_test(peer(1), now);
        // Rolled back while staying in the routing table.
        assert!(!marks.tells(peer(1), Some("node/0.23.0+ds.16.image.abc123"), 17, now));
        assert!(!marks.sent_a_notice(&peer(1)));
        assert!(marks.is_marked(&peer(1), now), "its mark stays");
        // Then disconnected: no longer remembered as a reader.
        assert!(!marks.tells(peer(1), None, 17, now));
        // Never seen to read it.
        assert!(!marks.tells(peer(2), None, 17, now));
        // A reader by its own notice, then seen with an older agent.
        marks.note_notice(peer(3), false, now);
        assert!(!marks.tells(peer(3), Some("node/0.23.0+ds.16"), 17, now));
        assert!(!marks.sent_a_notice(&peer(3)));
    }

    /// A peer that left the routing table loses its mark and its place among
    /// the notice's readers at once, not when the mark lapses.
    #[test]
    fn a_peer_forgotten_is_neither_marked_nor_a_reader() {
        let marks = FullMarks::default();
        let now = Instant::now();
        marks.note_notice(peer(1), true, now);
        marks.note_notice(peer(2), true, now);
        marks.forget(&peer(1));
        assert!(!marks.is_marked(&peer(1), now));
        assert!(!marks.sent_a_notice(&peer(1)));
        assert!(marks.is_marked(&peer(2), now), "only that peer");
    }

    #[test]
    fn the_marks_are_bounded() {
        let marks = FullMarks::default();
        let start = Instant::now();
        for i in 0..=MAX_FULL_MARKS {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(i as u64).to_be_bytes());
            marks.note_notice(
                PeerId::from_bytes(bytes),
                true,
                start + Duration::from_millis(i as u64),
            );
        }
        assert_eq!(marks.marks.lock().len(), MAX_FULL_MARKS);
        assert!(
            !marks.is_marked(&PeerId::from_bytes([0u8; 32]), start),
            "the stalest mark went first"
        );
    }

    fn numbered(n: u64) -> XorName {
        let mut key = [0u8; 32];
        key[..8].copy_from_slice(&n.to_be_bytes());
        key
    }

    #[test]
    fn a_proof_is_remembered_through_its_generation() {
        let seen = SeenHolders::default();
        let key = [7u8; 32];
        assert!(!seen.may_have_seen(&peer(1), &key));
        assert_eq!(seen.fill(), SeenFill::default(), "nothing allocated yet");
        seen.record(&peer(1), &key);
        assert!(seen.may_have_seen(&peer(1), &key));
        assert!(!seen.may_have_seen(&peer(2), &key), "per peer");
        assert!(!seen.may_have_seen(&peer(1), &[8u8; 32]), "per key");
        for n in 0..10_000 {
            seen.record(&peer(3), &numbered(n));
        }
        assert!(seen.may_have_seen(&peer(1), &key));
    }

    /// The smallest generation, 2^24 bits, holds 2^24 / 10 distinct pairs;
    /// the next starts then, the first still read, and the one after drops
    /// it. Repeat proofs of one pair count once.
    #[test]
    fn generations_turn_over_as_they_fill_and_keep_false_seen_low() {
        let seen = SeenHolders::default();
        let capacity = SEEN_MIN_BITS / SEEN_BITS_PER_PAIR;
        let key = [7u8; 32];
        for _ in 0..1000 {
            seen.record(&peer(1), &key);
        }
        assert_eq!(seen.current().distinct.load(Ordering::Relaxed), 1);

        // Fill the first generation to just short of its capacity.
        let mut n = 0u64;
        let first = seen.current();
        while first.distinct.load(Ordering::Relaxed) < capacity - 1 {
            seen.record(&peer(3), &numbered(n));
            n += 1;
        }
        let fill = seen.fill();
        assert!((0.2..0.3).contains(&fill.fill), "{fill:?}");
        assert!(fill.false_seen < 0.025, "{fill:?}");
        // One more distinct pair: a new generation, the first still read.
        while Arc::ptr_eq(&seen.current(), &first) {
            seen.record(&peer(3), &numbered(n));
            n += 1;
        }
        assert!(
            seen.may_have_seen(&peer(1), &key),
            "the old generation is read"
        );
        let fill = seen.fill();
        assert!(fill.fill < 0.01, "{fill:?}");
        assert!(fill.false_seen < 0.04, "{fill:?}");
        let false_seen = (0..10_000)
            .filter(|m| seen.may_have_seen(&peer(9), &numbered(*m)))
            .count();
        assert!(false_seen < 400, "{false_seen} of 10000");

        // The next turnover drops the first; the pair proved again is back.
        let second = seen.current();
        while Arc::ptr_eq(&seen.current(), &second) {
            seen.record(&peer(3), &numbered(n));
            n += 1;
        }
        seen.record(&peer(1), &key);
        assert!(seen.may_have_seen(&peer(1), &key), "proved afresh");
    }

    #[test]
    fn a_generation_is_sized_from_the_records_held() {
        assert_eq!(SeenGeneration::sized_for(0).bits, SEEN_MIN_BITS);
        // 100,000 records: 20 peers each, 10 bits a pair, 20 million bits.
        let sized = SeenGeneration::sized_for(100_000);
        assert_eq!(sized.bits, 20_000_000);
        assert_eq!(sized.capacity, 2_000_000);
        assert_eq!(SeenGeneration::sized_for(u64::MAX).bits, SEEN_MAX_BITS);
        let seen = SeenHolders::default();
        seen.size_for_records(100_000);
        seen.record(&peer(1), &[7u8; 32]);
        assert_eq!(seen.current().bits, 20_000_000);
    }

    #[test]
    fn the_record_is_saved_and_loaded_whole_and_only_when_changed() {
        let seen = SeenHolders::default();
        let key = [7u8; 32];
        assert!(seen.encode_if_changed().is_none(), "nothing recorded");
        seen.size_for_records(100_000);
        seen.record(&peer(1), &key);
        let bytes = seen.encode_if_changed().unwrap();
        assert!(seen.encode_if_changed().is_none(), "unchanged since");
        seen.record(&peer(1), &key);
        assert!(seen.encode_if_changed().is_none(), "a repeat sets no bit");

        let loaded = SeenHolders::decode(&bytes).unwrap();
        assert!(loaded.may_have_seen(&peer(1), &key), "same seed, same bits");
        assert!(!loaded.may_have_seen(&peer(2), &key));
        assert_eq!(loaded.records.load(Ordering::Relaxed), 100_000);
        assert_eq!(loaded.current().distinct.load(Ordering::Relaxed), 1);
        assert_eq!(loaded.fill(), seen.fill());

        // A damaged, cut or extended file, or another layout, is not read.
        let mut damaged = bytes.clone();
        damaged[100] ^= 1;
        assert!(SeenHolders::decode(&damaged).is_none());
        assert!(SeenHolders::decode(&bytes[..bytes.len() - 1]).is_none());
        assert!(SeenHolders::decode(&[]).is_none());
        let mut other = bytes[..bytes.len() - 32].to_vec();
        other[0] = SEEN_FORMAT + 1;
        let check = blake3::hash(&other);
        other.extend_from_slice(check.as_bytes());
        assert!(SeenHolders::decode(&other).is_none());
    }

    /// Both generations are kept across a save: a proof from before a
    /// turnover is still read after a restart.
    #[test]
    fn both_generations_survive_a_restart() {
        let seen = SeenHolders::default();
        let key = [7u8; 32];
        seen.record(&peer(1), &key);
        let first = seen.current();
        let mut n = 0u64;
        while Arc::ptr_eq(&seen.current(), &first) {
            seen.record(&peer(3), &numbered(n));
            n += 1;
        }
        let loaded = SeenHolders::decode(&seen.encode_if_changed().unwrap()).unwrap();
        assert!(
            loaded.may_have_seen(&peer(1), &key),
            "the previous generation"
        );
        assert!(
            loaded.may_have_seen(&peer(3), &numbered(n - 1)),
            "the current"
        );
        assert!(loaded.generations.read().previous.is_some());
    }

    #[tokio::test]
    async fn a_saved_record_is_loaded_at_start_and_a_bad_file_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let key = [7u8; 32];
        let fresh = SeenHolders::load(dir.path());
        assert!(!fresh.may_have_seen(&peer(1), &key), "no file: empty");
        fresh.record(&peer(1), &key);
        // The disk can't take the temporary file: nothing written, and the
        // save tried again after the next pass.
        let mut charged = None;
        fresh
            .save_with(dir.path(), |bytes| {
                charged = Some(bytes);
                Err::<(), _>(crate::error::Error::Storage("no room".to_string()))
            })
            .await;
        assert!(charged.is_some_and(|bytes| bytes > 0));
        assert!(!dir.path().join(SEEN_FILE).exists());
        assert!(!dir.path().join("seen_holders.bin.tmp").exists());
        let mut charged = None;
        fresh
            .save_with(dir.path(), |bytes| {
                charged = Some(bytes);
                Ok(())
            })
            .await;
        assert!(dir.path().join(SEEN_FILE).exists());
        assert_eq!(
            charged,
            std::fs::metadata(dir.path().join(SEEN_FILE))
                .ok()
                .map(|meta| meta.len()),
            "the charge is the file's size"
        );
        assert!(!dir.path().join("seen_holders.bin.tmp").exists());
        let loaded = SeenHolders::load(dir.path());
        assert!(
            loaded.may_have_seen(&peer(1), &key),
            "kept across a restart"
        );

        std::fs::write(dir.path().join(SEEN_FILE), b"not a record").unwrap();
        let empty = SeenHolders::load(dir.path());
        assert!(!empty.may_have_seen(&peer(1), &key), "a bad file: empty");
    }

    fn case<'a>(peer: &'a PeerId, key: &'a XorName, lane: Lane) -> UnheldCase<'a> {
        UnheldCase {
            lane,
            peer,
            key,
            answer: UnheldAnswer::Absent,
            commitment_pinned: false,
            own_quote: OwnQuote::NoProof,
        }
    }

    #[test]
    fn the_waiver_holds_only_when_every_condition_does() {
        let view = FullView::default();
        let now = Instant::now();
        let a = peer(1);
        let key = [9u8; 32];
        for lane in Lane::ALL {
            let base = case(&a, &key, lane);
            assert!(
                !view.waives_unheld_penalty(&base, now),
                "no notice: charged"
            );
        }
        view.marks.note_notice(a, true, now);
        for lane in Lane::ALL {
            let base = case(&a, &key, lane);
            assert!(view.waives_unheld_penalty(&base, now), "{lane:?}");
            let stale = now + full_notice_freshness();
            assert!(!view.waives_unheld_penalty(&base, stale), "stale notice");
            let pinned = UnheldCase {
                commitment_pinned: true,
                ..base
            };
            assert!(!view.waives_unheld_penalty(&pinned, now), "pinned");
            let other = UnheldCase {
                answer: UnheldAnswer::Other,
                ..base
            };
            assert!(!view.waives_unheld_penalty(&other, now), "not absent");
            let paid = UnheldCase {
                own_quote: OwnQuote::PaidOrUnknown,
                ..base
            };
            assert!(!view.waives_unheld_penalty(&paid, now), "paid");
            let unpaid = UnheldCase {
                own_quote: OwnQuote::NotPaid,
                ..base
            };
            assert!(view.waives_unheld_penalty(&unpaid, now), "not paid");
        }
        view.seen.record(&a, &key);
        assert!(
            !view.waives_unheld_penalty(&case(&a, &key, Lane::Fetch), now),
            "seen holding it"
        );
        assert!(view.waives_unheld_penalty(&case(&a, &[10u8; 32], Lane::Fetch), now));
    }

    #[test]
    fn only_a_paid_quote_or_an_unreadable_proof_charges() {
        let paid = [peer(1), peer(2)];
        assert_eq!(own_quote(&peer(1), Some(&paid)), OwnQuote::PaidOrUnknown);
        assert_eq!(own_quote(&peer(3), Some(&paid)), OwnQuote::NotPaid);
        assert_eq!(own_quote(&peer(3), None), OwnQuote::PaidOrUnknown);
        assert_eq!(paid_quote_issuers(b"not a proof"), None);
    }

    /// A node at the line at start says "full" before its first pass; when
    /// that pass erases and leaves it above the line, not in "full", the next
    /// renewal takes the notice back, once.
    #[test]
    fn an_at_start_full_notice_is_taken_back_when_the_first_pass_does_not_make_the_node_full() {
        let announcer = FullAnnouncer::default();
        assert_eq!(announcer.renewal_when_not_full(true), Some(true));
        // The pass erased and left the node above the line: no state change.
        announcer.note_pass(0, None);
        assert_eq!(announcer.renewal_when_not_full(false), Some(false));
        assert_eq!(announcer.renewal_when_not_full(false), None, "once");

        // A pass that puts the node in "full" settles it: nothing to take back.
        let confirmed = FullAnnouncer::default();
        assert_eq!(confirmed.renewal_when_not_full(true), Some(true));
        confirmed.note_pass(0, Some(true));
        confirmed.state_sent();
        assert_eq!(confirmed.renewal_when_not_full(false), None);

        // As does a renewal while full.
        let renewed = FullAnnouncer::default();
        assert_eq!(renewed.renewal_when_not_full(true), Some(true));
        renewed.renewal_when_full();
        renewed.note_pass(0, None);
        assert_eq!(renewed.renewal_when_not_full(false), None);

        // After the first pass, a node not in "full" never says "full" at start.
        let late = FullAnnouncer::default();
        late.note_pass(0, None);
        assert_eq!(late.renewal_when_not_full(true), None);
    }

    /// A node restarted at the line whose passes defer candidates until
    /// bootstrap drains keeps saying "full": those passes decide nothing. The
    /// first pass after the drain decides.
    #[test]
    fn passes_deferred_for_bootstrap_neither_skip_nor_take_back_the_at_start_notice() {
        // A deferred pass before the first renewal: the renewal still says
        // "full", and so does each one after it while the deferral lasts.
        let early = FullAnnouncer::default();
        early.note_pass(3, None);
        assert_eq!(early.renewal_when_not_full(true), Some(true));
        early.note_pass(3, None);
        assert_eq!(early.renewal_when_not_full(true), Some(true), "renewed");
        // Bootstrap drained, and the pass put the node in "full", which its
        // own notice says: nothing to take back.
        early.note_pass(0, Some(true));
        early.state_sent();
        assert_eq!(early.renewal_when_not_full(true), None);

        // A deferred pass after the at-start notice doesn't take it back.
        let late = FullAnnouncer::default();
        assert_eq!(late.renewal_when_not_full(true), Some(true));
        late.note_pass(2, None);
        assert_eq!(late.renewal_when_not_full(true), Some(true), "kept");
        // The first pass after the drain doesn't put the node in "full": the
        // next renewal takes the notice back, once.
        late.note_pass(0, None);
        assert_eq!(late.renewal_when_not_full(true), Some(false));
        assert_eq!(late.renewal_when_not_full(true), None, "once");

        // A node that left the line while its passes were deferred takes the
        // notice back.
        let left = FullAnnouncer::default();
        assert_eq!(left.renewal_when_not_full(true), Some(true));
        left.note_pass(1, None);
        assert_eq!(left.renewal_when_not_full(false), Some(false));
        assert_eq!(left.renewal_when_not_full(false), None, "once");
    }

    /// A renewal that read "full" and is still sending it holds the lock: the
    /// erasure task's "not full" waits for it, reads the state again and goes
    /// out last, so a stale "full" can't follow it.
    #[tokio::test]
    async fn a_stale_full_renewal_cannot_follow_a_not_full_notice() {
        let announcer = Arc::new(FullAnnouncer::default());
        let state = Arc::new(AtomicBool::new(true));
        let sent = Arc::new(Mutex::new(Vec::<bool>::new()));
        let (reading_done, read) = tokio::sync::oneshot::channel::<()>();
        let (release, released) = tokio::sync::oneshot::channel::<()>();

        let renewal = {
            let (announcer, state, sent) = (
                Arc::clone(&announcer),
                Arc::clone(&state),
                Arc::clone(&sent),
            );
            tokio::spawn(async move {
                announcer
                    .announce(
                        |_| Some(state.load(Ordering::Acquire)),
                        |full| async move {
                            let _ = reading_done.send(());
                            let _ = released.await;
                            sent.lock().push(full);
                            1
                        },
                    )
                    .await
            })
        };
        read.await.expect("the renewal read the state");
        // The erasure task leaves "full" and tells the peers.
        state.store(false, Ordering::Release);
        let leaving = {
            let (announcer, state, sent) = (
                Arc::clone(&announcer),
                Arc::clone(&state),
                Arc::clone(&sent),
            );
            tokio::spawn(async move {
                announcer
                    .announce(
                        |_| Some(state.load(Ordering::Acquire)),
                        |full| async move {
                            sent.lock().push(full);
                            1
                        },
                    )
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert!(sent.lock().is_empty(), "the notice waits for the renewal");
        let _ = release.send(());
        renewal.await.expect("renewal");
        leaving.await.expect("leaving");
        assert_eq!(
            *sent.lock(),
            vec![true, false],
            "\"not full\" goes out last"
        );
    }
}

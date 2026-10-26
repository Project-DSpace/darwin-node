//! Erasure in two phases, and out-of-range clocks that survive restarts.
//!
//! The prune passes run in a task of their own, once each neighbour-sync
//! cycle completes, never inside the sync loop. Erasing a record goes in two
//! phases, so a peer checking a hint from this node never finds the record
//! gone while the hint is in flight:
//!
//! 1. **Selected.** A record a pass finds eligible for erasure enters the
//!    in-memory "being erased" set ([`ErasureGate`]). From then on no hint
//!    this node sends names it: hint lists leave it out both when they are
//!    built and when each is sent.
//! 2. **Erased.** It is erased only in a pass of a later cycle, after a whole
//!    sync cycle passed without it being hinted ([`EPOCHS_BEFORE_ERASURE`]:
//!    the cycle that starts as the pass selects it may have built its first
//!    hints just before, so the cycle after that) and at least a fetch window
//!    ([`FETCH_WINDOW`]) after it was selected, through the usual possession
//!    check and re-check; and never while this node answered "present" for
//!    it within the last fetch window, so a node that said "present" doesn't
//!    say "not found" inside that window. An answer given before the key was
//!    selected is a fetch window old before the key can be erased, however
//!    fast the sync epochs move, so only selected keys' answers are kept.
//!
//! A record that is no longer eligible leaves the set, and is hinted again.
//! So does one the pass that found it ready didn't try, for lack of audit
//! budget or because the pressure ended first
//! ([`ErasureGate::release_untried`]): it is hinted until a later pass
//! selects it again, so no key stays hidden longer than its own wait. So does
//! one whose possession check failed ([`ErasureGate::fail`]): its
//! close group lacks copies, and this node's hint is what lets the group
//! fetch it back. It is then left unselected for [`RETRY_COOLDOWN_EPOCHS`],
//! so it is hinted for at least a whole cycle before it is selected, hidden
//! and tried again.
//!
//! The clock that starts when a record or pointer leaves the node's range is
//! kept, in wall-clock seconds, in a small database of its own
//! ([`OutOfRangeClocks`], `{root}/out_of_range.mdb`, not inside the paid
//! list's), written at the end of each pass with the time of the write, and
//! loaded at start. A restored clock counts only the time the node saw before
//! that write: a restart neither resets the wait nor adds the time the node
//! was down, and a table left from before a rollback to a release that
//! doesn't write it can't make a record eligible early. A record still out of
//! range keeps its seen time, one back in range or no longer held is cleared,
//! a start after the write counts as now, and a missing, unreadable or
//! older-layout table starts every clock now, as before.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions};
use parking_lot::Mutex;
use saorsa_core::identity::PeerId;

use crate::ant_protocol::XorName;
use crate::error::{Error, Result};
use crate::logging::{debug, warn};
use crate::replication::config::{
    FETCH_REQUEST_TIMEOUT, VERIFICATION_REQUEST_TIMEOUT, VERIFICATION_RETRY_BACKOFF_MAX,
};

/// How long after answering "present" for a key a node keeps it from
/// erasure: the verification and fetch timeouts, plus the longest wait before
/// the asking node tries the key again.
pub const FETCH_WINDOW: Duration = Duration::from_secs(
    VERIFICATION_REQUEST_TIMEOUT.as_secs()
        + FETCH_REQUEST_TIMEOUT.as_secs()
        + VERIFICATION_RETRY_BACKOFF_MAX.as_secs(),
);

/// The most keys whose last "present" answer a node remembers one by one.
/// Past it the oldest are folded into one time that stands for all of them.
const MAX_ANSWERED_PRESENT: usize = 1 << 16;

/// How many sync epochs after its selection a key may be erased.
///
/// The pass that selects it runs while the next cycle starts, whose first
/// hints may have been built just before, so a whole cycle after that one
/// must pass.
pub const EPOCHS_BEFORE_ERASURE: u64 = 2;

/// How many sync epochs a key whose possession check failed stays out of the
/// selection, and so is hinted: one whole sync cycle plus
/// [`EPOCHS_BEFORE_ERASURE`].
///
/// The cycle running as the check fails may have built its hints already,
/// without the key (the reason for [`EPOCHS_BEFORE_ERASURE`]); the whole
/// cycle after that hints it to the close group, which may fetch it back; and
/// one more lets the group's own syncs settle before this node selects it,
/// hides it and tries again.
pub const RETRY_COOLDOWN_EPOCHS: u64 = EPOCHS_BEFORE_ERASURE + 1;

/// One key being erased.
#[derive(Debug, Clone, Copy)]
struct Selected {
    /// The sync epoch in which it was selected.
    at: u64,
    /// When it was selected: it is erased no sooner than [`FETCH_WINDOW`]
    /// after, so any "present" answered before its selection has aged out.
    since: Instant,
    /// Whether it was selected again once the cooldown after a failed
    /// possession check ended: tried again later, it doesn't keep a node from
    /// being full.
    retry: bool,
}

/// The keys a node is erasing, records and pointers apart, and the keys it
/// lately answered "present" for.
#[derive(Debug, Default)]
pub struct ErasureGate {
    /// Each record being erased.
    records: Mutex<HashMap<XorName, Selected>>,
    /// Each pointer being erased, likewise.
    pointers: Mutex<HashMap<XorName, Selected>>,
    /// Each record whose possession check failed, with the sync epoch it
    /// failed in: left out of the selection, so hinted, for
    /// [`RETRY_COOLDOWN_EPOCHS`].
    records_cooling: Mutex<HashMap<XorName, u64>>,
    /// Each pointer whose check failed, likewise.
    pointers_cooling: Mutex<HashMap<XorName, u64>>,
    /// Each record found ready and given back untried
    /// ([`Self::release_untried`]): hinted again, but still one the node may
    /// erase, until a later pass selects it again or it is no longer
    /// eligible. With whether it was a retry after a failed check.
    records_given_back: Mutex<HashMap<XorName, bool>>,
    /// Each pointer given back untried, likewise.
    pointers_given_back: Mutex<HashMap<XorName, bool>>,
    /// When this node last answered "present" for each key.
    answered: Mutex<AnsweredPresent>,
}

#[derive(Debug, Default)]
struct AnsweredPresent {
    at: HashMap<XorName, Instant>,
    /// The latest answer forgotten to keep the map bounded: any key may have
    /// been answered then.
    forgotten_until: Option<Instant>,
}

impl ErasureGate {
    fn of(&self, kind: ClockKind) -> &Mutex<HashMap<XorName, Selected>> {
        match kind {
            ClockKind::Records => &self.records,
            ClockKind::Pointers => &self.pointers,
        }
    }

    fn cooling_of(&self, kind: ClockKind) -> &Mutex<HashMap<XorName, u64>> {
        match kind {
            ClockKind::Records => &self.records_cooling,
            ClockKind::Pointers => &self.pointers_cooling,
        }
    }

    fn given_back_of(&self, kind: ClockKind) -> &Mutex<HashMap<XorName, bool>> {
        match kind {
            ClockKind::Records => &self.records_given_back,
            ClockKind::Pointers => &self.pointers_given_back,
        }
    }

    /// Whether `key` is being erased, as a record or a pointer: selected, so
    /// named in no hint.
    #[must_use]
    pub fn is_being_erased(&self, key: &XorName) -> bool {
        self.records.lock().contains_key(key) || self.pointers.lock().contains_key(key)
    }

    /// One pass's two phases for the keys of `kind`, at sync epoch `epoch`.
    /// `eligible` are the keys the pass may erase now, in the order it would
    /// try them.
    ///
    /// Keys of `kind` no longer eligible leave the set. Eligible keys not yet
    /// selected are selected, in order, while the set of `kind` holds fewer
    /// than `max_selected`, except those whose check failed fewer than
    /// [`RETRY_COOLDOWN_EPOCHS`] epochs before ([`Self::fail`]), which stay
    /// hinted meanwhile. Returns the eligible keys selected
    /// [`EPOCHS_BEFORE_ERASURE`] or more epochs before and [`FETCH_WINDOW`]
    /// or longer before `now`, in order: those a whole cycle has passed
    /// without hinting, with no "present" answered before their selection
    /// still in its window, which the pass may now check and erase. The pass
    /// gives back, with [`Self::release_untried`], those it then doesn't try;
    /// selected again, a key waits again from then.
    pub fn select(
        &self,
        kind: ClockKind,
        eligible: &[XorName],
        epoch: u64,
        max_selected: usize,
        now: Instant,
    ) -> Vec<XorName> {
        let keep: HashSet<&XorName> = eligible.iter().collect();
        let mut cooling = self.cooling_of(kind).lock();
        let mut cooled = HashSet::new();
        cooling.retain(|key, failed_at| {
            let eligible = keep.contains(key);
            let cooling = failed_at.saturating_add(RETRY_COOLDOWN_EPOCHS) > epoch;
            if eligible && !cooling {
                cooled.insert(*key);
            }
            eligible && cooling
        });
        let mut given_back = self.given_back_of(kind).lock();
        given_back.retain(|key, _| keep.contains(key));
        let mut selected = self.of(kind).lock();
        selected.retain(|key, _| keep.contains(key));
        let mut ready = Vec::new();
        for key in eligible {
            match selected.get(key) {
                Some(entry) => {
                    if entry.at.saturating_add(EPOCHS_BEFORE_ERASURE) <= epoch
                        && now.saturating_duration_since(entry.since) >= FETCH_WINDOW
                    {
                        ready.push(*key);
                    }
                }
                None => {
                    if selected.len() < max_selected && !cooling.contains_key(key) {
                        let was_retry = given_back.remove(key).unwrap_or(false);
                        selected.insert(
                            *key,
                            Selected {
                                at: epoch,
                                since: now,
                                retry: was_retry || cooled.contains(key),
                            },
                        );
                    }
                }
            }
        }
        ready
    }

    /// Forget `key` of `kind`: erased.
    pub fn release(&self, kind: ClockKind, key: &XorName) {
        self.of(kind).lock().remove(key);
        self.given_back_of(kind).lock().remove(key);
    }

    /// Give back the keys of `kind` in `ready` (what [`Self::select`]
    /// returned) that the pass didn't try, those not in `tried`: deferred for
    /// lack of audit budget, or left when the pressure ended. They leave the
    /// set, so they are hinted again until a later pass selects them, and no
    /// key stays hidden longer than one wait however far the selection runs
    /// ahead of what a pass can audit. They still count as keys the node may
    /// erase ([`Self::not_yet_erased`]) until then. Returns how many were
    /// given back.
    pub fn release_untried(
        &self,
        kind: ClockKind,
        ready: &[XorName],
        tried: &HashSet<XorName>,
    ) -> usize {
        let mut given_back = self.given_back_of(kind).lock();
        let mut selected = self.of(kind).lock();
        let mut count = 0usize;
        for key in ready.iter().filter(|key| !tried.contains(*key)) {
            if let Some(entry) = selected.remove(key) {
                given_back.insert(*key, entry.retry);
                count += 1;
            }
        }
        count
    }

    /// `key` of `kind` failed its possession check at sync epoch `epoch`: its
    /// close group lacks copies. It leaves the set, so it is hinted again and
    /// the group can fetch it from this node, and it is not selected again
    /// before [`RETRY_COOLDOWN_EPOCHS`] have passed.
    pub fn fail(&self, kind: ClockKind, key: &XorName, epoch: u64) {
        self.of(kind).lock().remove(key);
        self.cooling_of(kind).lock().insert(*key, epoch);
    }

    /// How many keys of `kind` are being erased.
    #[must_use]
    pub fn selected_of(&self, kind: ClockKind) -> usize {
        self.of(kind).lock().len()
    }

    /// How many keys of both kinds are being erased.
    #[must_use]
    pub fn selected_count(&self) -> usize {
        self.records.lock().len() + self.pointers.lock().len()
    }

    /// How many keys of both kinds are selected or given back untried and not
    /// yet erased, other than those selected again after a failed possession
    /// check: still in their wait, ready and kept (answered "present" lately,
    /// or tried and kept), or deferred by a pass. A node with any is not
    /// full: it has copies it may still give up. A key retried after a failed
    /// check is tried again later, and doesn't keep a node from being full;
    /// nor does an eligible key the set had no room for, since the set is
    /// then full of keys that do.
    #[must_use]
    pub fn not_yet_erased(&self) -> usize {
        let selected = |map: &Mutex<HashMap<XorName, Selected>>| {
            map.lock().values().filter(|entry| !entry.retry).count()
        };
        let given_back = |map: &Mutex<HashMap<XorName, bool>>| {
            map.lock().values().filter(|retry| !**retry).count()
        };
        selected(&self.records)
            + selected(&self.pointers)
            + given_back(&self.records_given_back)
            + given_back(&self.pointers_given_back)
    }

    /// Note that this node answered "present" for `key` at `now`, if `key` is
    /// being erased ([`Self::is_being_erased`]).
    ///
    /// A key not selected when it is answered can't be erased within the
    /// fetch window: a later pass has to select it first, and
    /// [`Self::select`] finds it ready no sooner than [`FETCH_WINDOW`] after
    /// that (a key given back untried waits again from its new selection),
    /// however fast the sync epochs move. So only selected keys are remembered:
    /// the record stays within the selected set's size, and the answers for
    /// the keys the node keeps anyway (its in-range keys among them) never
    /// fill it, which would fold every key into one time
    /// ([`Self::answered_present_lately`]) and keep the node from erasing
    /// anything, or from entering "full", while answers keep coming.
    pub fn note_answered_present(&self, key: &XorName, now: Instant) {
        if !self.is_being_erased(key) {
            return;
        }
        let mut answered = self.answered.lock();
        if answered.at.len() >= MAX_ANSWERED_PRESENT && !answered.at.contains_key(key) {
            answered
                .at
                .retain(|_, at| now.saturating_duration_since(*at) < FETCH_WINDOW);
            if answered.at.len() >= MAX_ANSWERED_PRESENT {
                let latest = answered.at.values().max().copied();
                answered.at.clear();
                answered.forgotten_until = latest.max(answered.forgotten_until);
            }
        }
        answered.at.insert(*key, now);
    }

    /// Whether this node answered "present" for `key` within
    /// [`FETCH_WINDOW`] before `now`. A key whose answer was forgotten to keep
    /// the record bounded counts as answered when the forgotten ones were.
    #[must_use]
    pub fn answered_present_lately(&self, key: &XorName, now: Instant) -> bool {
        let answered = self.answered.lock();
        let within = |at: Instant| now.saturating_duration_since(at) < FETCH_WINDOW;
        answered.at.get(key).copied().is_some_and(within)
            || answered.forgotten_until.is_some_and(within)
    }
}

static GATES: LazyLock<Mutex<HashMap<[u8; 32], Weak<ErasureGate>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Register `gate` as the erasure gate of the node `self_id`, for the code
/// that reaches it by the node's id (the sync and verification paths).
pub fn register_gate(self_id: &PeerId, gate: &Arc<ErasureGate>) {
    let mut gates = GATES.lock();
    gates.retain(|_, gate| gate.strong_count() > 0);
    gates.insert(*self_id.as_bytes(), Arc::downgrade(gate));
}

/// The erasure gate of the node `self_id`: an empty one, erasing nothing,
/// for a node with no replication engine running.
#[must_use]
pub fn gate_for(self_id: &PeerId) -> Arc<ErasureGate> {
    GATES
        .lock()
        .get(self_id.as_bytes())
        .and_then(Weak::upgrade)
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Out-of-range clocks
// ---------------------------------------------------------------------------

/// The table's name under the node root.
pub const OUT_OF_RANGE_DB: &str = "out_of_range.mdb";

/// LMDB map size for the clocks: 40 bytes an entry, so room for millions.
const MAP_SIZE: usize = 256 * 1024 * 1024;

/// The layout of one saved list: this byte, the write's time in big-endian
/// Unix seconds, then 40-byte entries. Version 1 had no write time; a list of
/// that layout starts every clock now.
const FORMAT_VERSION: u8 = 2;

/// Bytes of the header after the version byte: the write's Unix seconds.
const SAVED_AT_LEN: usize = 8;

/// Bytes of one entry: the key, then big-endian Unix seconds.
const ENTRY_LEN: usize = 32 + 8;

/// Which clocks a saved list holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockKind {
    /// Stored chunks.
    Records,
    /// Pointers.
    Pointers,
}

impl ClockKind {
    const fn db_key(self) -> &'static [u8] {
        match self {
            Self::Records => b"records",
            Self::Pointers => b"pointers",
        }
    }
}

/// One saved list: when it was written, and each clock's start, both in
/// Unix seconds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SavedClocks {
    /// Unix seconds of the write.
    pub saved_at: u64,
    /// Each clock's start, in Unix seconds, by key.
    pub starts: HashMap<XorName, u64>,
}

impl SavedClocks {
    /// Whether no clock was saved.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.starts.is_empty()
    }
}

/// The out-of-range clocks, in a database of their own.
pub struct OutOfRangeClocks {
    env: Env,
    db: Database<Bytes, Bytes>,
}

impl OutOfRangeClocks {
    /// Open or create the table at `{root_dir}/out_of_range.mdb`.
    ///
    /// # Errors
    ///
    /// When the environment can't be opened; the caller runs on with clocks
    /// that start now, as before.
    #[allow(unsafe_code)]
    pub async fn open(root_dir: &Path) -> Result<Self> {
        let env_dir = root_dir.join(OUT_OF_RANGE_DB);
        tokio::task::spawn_blocking(move || -> Result<Self> {
            std::fs::create_dir_all(&env_dir).map_err(|e| {
                Error::Storage(format!("Failed to create the out-of-range table: {e}"))
            })?;
            // SAFETY: as for the paid list: each node has its own root
            // directory, so no two processes open this environment.
            let env = unsafe {
                EnvOpenOptions::new()
                    .map_size(MAP_SIZE)
                    .max_dbs(1)
                    .open(&env_dir)
                    .map_err(|e| {
                        Error::Storage(format!("Failed to open the out-of-range table: {e}"))
                    })?
            };
            let mut wtxn = env
                .write_txn()
                .map_err(|e| Error::Storage(format!("Failed to begin a write: {e}")))?;
            let db: Database<Bytes, Bytes> = env
                .create_database(&mut wtxn, None)
                .map_err(|e| Error::Storage(format!("Failed to create the table: {e}")))?;
            wtxn.commit()
                .map_err(|e| Error::Storage(format!("Failed to create the table: {e}")))?;
            Ok(Self { env, db })
        })
        .await
        .map_err(|e| Error::Storage(format!("Out-of-range table task failed: {e}")))?
    }

    /// The saved clocks of `kind`, with the time they were written. Empty
    /// when none were saved or the list can't be read.
    #[must_use]
    pub fn load(&self, kind: ClockKind) -> SavedClocks {
        let read = || -> Option<Vec<u8>> {
            let rtxn = self.env.read_txn().ok()?;
            self.db
                .get(&rtxn, kind.db_key())
                .ok()
                .flatten()
                .map(<[u8]>::to_vec)
        };
        read().map_or_else(SavedClocks::default, |bytes| decode_clocks(&bytes))
    }

    /// Save the clocks of `kind`, written at `saved_at` Unix seconds, in
    /// place of those saved before.
    ///
    /// # Errors
    ///
    /// When the write fails; the clocks saved before stay, with their own
    /// write time.
    pub async fn save(
        &self,
        kind: ClockKind,
        saved_at: u64,
        clocks: Vec<(XorName, u64)>,
    ) -> Result<()> {
        let env = self.env.clone();
        let db = self.db;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let bytes = encode_clocks(saved_at, &clocks);
            let mut wtxn = env
                .write_txn()
                .map_err(|e| Error::Storage(format!("Failed to begin a write: {e}")))?;
            db.put(&mut wtxn, kind.db_key(), &bytes)
                .map_err(|e| Error::Storage(format!("Failed to save the clocks: {e}")))?;
            wtxn.commit()
                .map_err(|e| Error::Storage(format!("Failed to save the clocks: {e}")))
        })
        .await
        .map_err(|e| Error::Storage(format!("Out-of-range save task failed: {e}")))?
    }
}

/// The bytes one save of `count` clocks writes, which the caller charges to
/// the store's capacity guard for the write.
#[must_use]
pub fn saved_clocks_len(count: usize) -> u64 {
    (1 + SAVED_AT_LEN + count.saturating_mul(ENTRY_LEN)) as u64
}

fn encode_clocks(saved_at: u64, clocks: &[(XorName, u64)]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + SAVED_AT_LEN + clocks.len() * ENTRY_LEN);
    bytes.push(FORMAT_VERSION);
    bytes.extend_from_slice(&saved_at.to_be_bytes());
    for (key, secs) in clocks {
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&secs.to_be_bytes());
    }
    bytes
}

fn decode_clocks(bytes: &[u8]) -> SavedClocks {
    let Some((&FORMAT_VERSION, body)) = bytes.split_first() else {
        debug!("Out-of-range clocks of an unknown layout; every clock starts now");
        return SavedClocks::default();
    };
    let Some((saved_at, entries)) = body.split_first_chunk::<SAVED_AT_LEN>() else {
        warn!("Out-of-range clocks are truncated; every clock starts now");
        return SavedClocks::default();
    };
    let (entries, rest) = entries.as_chunks::<ENTRY_LEN>();
    if !rest.is_empty() {
        warn!("Out-of-range clocks are truncated; every clock starts now");
        return SavedClocks::default();
    }
    let starts = entries
        .iter()
        .filter_map(|entry| {
            let (key, secs) = entry.split_at(32);
            Some((
                key.try_into().ok()?,
                u64::from_be_bytes(secs.try_into().ok()?),
            ))
        })
        .collect();
    SavedClocks {
        saved_at: u64::from_be_bytes(*saved_at),
        starts,
    }
}

/// Unix seconds of the wall-clock time `since` stands for, measured against
/// `now` and `wall_now`.
#[must_use]
pub fn wall_secs(since: Instant, now: Instant, wall_now: SystemTime) -> u64 {
    let elapsed = now.saturating_duration_since(since);
    wall_now
        .checked_sub(elapsed)
        .unwrap_or(UNIX_EPOCH)
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The instant a saved clock that started at `start_secs` stands for, in a
/// list written at `saved_at` (both Unix seconds), as of `now`.
///
/// Only the time the node saw before the write counts: `now - (saved_at -
/// start_secs)`. The time between the write and `now` (the node down, or
/// running a release that doesn't keep the table) doesn't, since nothing
/// checked then that the key stayed out of range. A start after the write (a
/// wall clock that moved back) counts as now. Where the platform's monotonic
/// clock can't reach that far back (a host up for less than the seen time,
/// off Linux), the earliest of the seen time and `cap` that it can reach,
/// else now.
#[must_use]
pub fn restored_instant(start_secs: u64, saved_at: u64, now: Instant, cap: Duration) -> Instant {
    let seen = Duration::from_secs(saved_at.saturating_sub(start_secs));
    now.checked_sub(seen)
        .or_else(|| now.checked_sub(seen.min(cap)))
        .unwrap_or(now)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn key(n: u8) -> XorName {
        [n; 32]
    }

    #[test]
    fn the_fetch_window_is_the_verification_and_fetch_timeouts_and_the_retry_wait() {
        assert_eq!(FETCH_WINDOW, Duration::from_secs(15 + 30 + 300));
    }

    const R: ClockKind = ClockKind::Records;
    const P: ClockKind = ClockKind::Pointers;

    static START: LazyLock<Instant> = LazyLock::new(Instant::now);

    /// When sync epoch `epoch` of these tests starts: a fetch window apart,
    /// so time never holds back a key the epochs let go.
    fn at(epoch: u64) -> Instant {
        *START + FETCH_WINDOW * u32::try_from(epoch).unwrap()
    }

    /// [`ErasureGate::select`] at the start of the sync epoch.
    trait SelectInEpoch {
        fn select_in(
            &self,
            kind: ClockKind,
            eligible: &[XorName],
            epoch: u64,
            max_selected: usize,
        ) -> Vec<XorName>;
    }

    impl SelectInEpoch for ErasureGate {
        fn select_in(
            &self,
            kind: ClockKind,
            eligible: &[XorName],
            epoch: u64,
            max_selected: usize,
        ) -> Vec<XorName> {
            self.select(kind, eligible, epoch, max_selected, at(epoch))
        }
    }

    /// Sync epochs can move on faster than the fetch window (a burst of
    /// topology changes starts rounds at once): a key two epochs past its
    /// selection isn't ready until a fetch window has passed since then, so a
    /// "present" this node answered before selecting it, which isn't kept,
    /// has aged out before the key can be erased.
    #[test]
    fn a_key_is_erased_no_sooner_than_a_fetch_window_after_its_selection() {
        let gate = ErasureGate::default();
        let answered = *START;
        // Answered before it was selected: not remembered.
        gate.note_answered_present(&key(1), answered);
        let selected = answered + Duration::from_secs(10);
        assert_eq!(
            gate.select(R, &[key(1)], 5, 10, selected),
            Vec::<XorName>::new()
        );
        assert!(!gate.answered_present_lately(&key(1), selected));
        // Two epochs on within a minute: not ready.
        let soon = selected + Duration::from_secs(60);
        assert_eq!(
            gate.select(R, &[key(1)], 6, 10, soon),
            Vec::<XorName>::new()
        );
        assert_eq!(
            gate.select(R, &[key(1)], 7, 10, soon),
            Vec::<XorName>::new()
        );
        let almost = selected + FETCH_WINDOW.saturating_sub(Duration::from_secs(1));
        assert_eq!(
            gate.select(R, &[key(1)], 9, 10, almost),
            Vec::<XorName>::new()
        );
        assert!(gate.is_being_erased(&key(1)), "still hidden");
        // A fetch window after its selection: ready, and the answer before it
        // is older than the window.
        let ready_at = selected + FETCH_WINDOW;
        assert_eq!(gate.select(R, &[key(1)], 9, 10, ready_at), vec![key(1)]);
        assert!(ready_at.duration_since(answered) >= FETCH_WINDOW);
        // Given back untried and selected again, it waits again from then.
        gate.release_untried(R, &[key(1)], &HashSet::new());
        let again = ready_at + Duration::from_secs(1);
        assert_eq!(
            gate.select(R, &[key(1)], 10, 10, again),
            Vec::<XorName>::new()
        );
        let later = again + FETCH_WINDOW.saturating_sub(Duration::from_secs(1));
        assert_eq!(
            gate.select(R, &[key(1)], 12, 10, later),
            Vec::<XorName>::new()
        );
        assert_eq!(
            gate.select(R, &[key(1)], 12, 10, again + FETCH_WINDOW),
            vec![key(1)]
        );
    }

    #[test]
    fn a_key_is_erased_only_a_whole_cycle_after_the_one_it_was_selected_in() {
        assert_eq!(EPOCHS_BEFORE_ERASURE, 2);
        let gate = ErasureGate::default();
        let eligible = vec![key(1), key(2), key(3)];
        assert_eq!(gate.select_in(R, &eligible, 5, 10), Vec::<XorName>::new());
        assert!(gate.is_being_erased(&key(1)));
        // The same epoch again: still not ready.
        assert_eq!(gate.select_in(R, &eligible, 5, 10), Vec::<XorName>::new());
        assert_eq!(gate.not_yet_erased(), 3);
        // The next: the cycle that started as it was selected may have hinted it.
        assert_eq!(gate.select_in(R, &eligible, 6, 10), Vec::<XorName>::new());
        // The one after: a whole cycle passed without hinting it.
        assert_eq!(gate.select_in(R, &eligible, 7, 10), eligible);
        assert_eq!(gate.not_yet_erased(), 3, "ready, not yet erased");
        for key in &eligible {
            gate.release(R, key);
        }
        assert_eq!(gate.not_yet_erased(), 0, "erased");
    }

    #[test]
    fn no_hint_names_a_key_being_erased() {
        let gate = ErasureGate::default();
        gate.select_in(R, &[key(2)], 1, 10);
        gate.select_in(P, &[key(4)], 1, 10);
        let hints = [key(1), key(2), key(3), key(4)];
        let sent: Vec<XorName> = hints
            .iter()
            .copied()
            .filter(|key| !gate.is_being_erased(key))
            .collect();
        assert_eq!(sent, vec![key(1), key(3)]);
    }

    #[test]
    fn a_key_no_longer_eligible_is_hinted_again() {
        let gate = ErasureGate::default();
        gate.select_in(R, &[key(1), key(2)], 1, 10);
        assert_eq!(gate.select_in(R, &[key(2)], 3, 10), vec![key(2)]);
        assert!(!gate.is_being_erased(&key(1)));
        gate.release(R, &key(2));
        assert_eq!(gate.selected_count(), 0);
    }

    #[test]
    fn records_and_pointers_are_selected_apart() {
        let gate = ErasureGate::default();
        gate.select_in(R, &[key(1)], 1, 10);
        gate.select_in(P, &[key(2)], 1, 10);
        // A pass of one kind keeps the other's selection.
        assert_eq!(gate.select_in(R, &[key(1)], 3, 10), vec![key(1)]);
        assert!(gate.is_being_erased(&key(2)));
        assert_eq!(gate.select_in(P, &[key(2)], 3, 10), vec![key(2)]);
        assert!(gate.is_being_erased(&key(1)));
        assert_eq!(gate.selected_count(), 2);
    }

    #[test]
    fn the_set_never_holds_more_than_its_cap() {
        let gate = ErasureGate::default();
        let eligible: Vec<XorName> = (0..10).map(key).collect();
        gate.select_in(R, &eligible, 1, 4);
        assert_eq!(gate.selected_count(), 4);
        assert_eq!(gate.select_in(R, &eligible, 2, 4).len(), 0);
        assert_eq!(gate.selected_count(), 4, "no room, none added");
        assert_eq!(gate.select_in(R, &eligible, 3, 4), eligible[..4].to_vec());
        // Two erased, so no longer eligible: room for two more.
        gate.release(R, &key(0));
        gate.release(R, &key(1));
        gate.select_in(R, &eligible[2..], 3, 4);
        assert_eq!(gate.selected_count(), 4);
        assert!(gate.is_being_erased(&key(4)) && gate.is_being_erased(&key(5)));
    }

    /// N eligible keys, a pass able to audit only A < N of them: no more than
    /// the cap is ever hidden, the keys a pass found ready but didn't try are
    /// hinted again at once, and every key is tried in its turn.
    #[test]
    fn keys_a_pass_cannot_audit_are_hinted_again() {
        const N: u8 = 100;
        const A: usize = 7;
        const CAP: usize = 16;
        let gate = ErasureGate::default();
        let mut eligible: Vec<XorName> = (0..N).map(key).collect();
        let mut tried_ever = HashSet::new();
        for epoch in 1..200u64 {
            let ready = gate.select_in(R, &eligible, epoch, CAP);
            assert!(gate.selected_of(R) <= CAP, "at most the cap hidden");
            let hidden = eligible
                .iter()
                .filter(|key| gate.is_being_erased(key))
                .count();
            assert!(hidden <= CAP, "epoch {epoch}: {hidden} hidden");
            // The pass audits the first A ready keys; each is erased.
            let tried: HashSet<XorName> = ready.iter().take(A).copied().collect();
            gate.release_untried(R, &ready, &tried);
            for key in ready.iter().skip(A) {
                assert!(!gate.is_being_erased(key), "deferred, hinted again");
            }
            if ready.len() > A {
                assert!(gate.not_yet_erased() > 0, "deferred keys still owed");
            }
            for key in &tried {
                gate.release(R, key);
                tried_ever.insert(*key);
            }
            eligible.retain(|key| !tried.contains(key));
        }
        assert_eq!(tried_ever.len(), usize::from(N), "every key tried");
        assert_eq!(gate.not_yet_erased(), 0);
    }

    /// A key ready but deferred, or kept after an answer of "present", keeps
    /// the node from being full; one retried after a failed check doesn't;
    /// with nothing left, the count is zero.
    #[test]
    fn what_keeps_a_node_from_being_full() {
        let gate = ErasureGate::default();
        let eligible = vec![key(1), key(2), key(3)];
        gate.select_in(R, &eligible, 1, 10);
        let ready = gate.select_in(R, &eligible, 3, 10);
        assert_eq!(ready, eligible);
        // key(1) tried and kept (answered "present"); key(2) failed; key(3)
        // deferred for lack of budget.
        let tried: HashSet<XorName> = [key(1), key(2)].into_iter().collect();
        assert_eq!(gate.release_untried(R, &ready, &tried), 1);
        gate.fail(R, &key(2), 3);
        assert!(!gate.is_being_erased(&key(3)), "deferred: hinted");
        assert_eq!(gate.not_yet_erased(), 2, "kept and deferred");
        gate.release(R, &key(1));
        assert_eq!(gate.not_yet_erased(), 1, "the deferred one");
        // The deferred one leaves eligibility: nothing is owed.
        gate.select_in(R, &[key(2)], 4, 10);
        assert_eq!(gate.not_yet_erased(), 0, "key(2) cooling");
        // key(2) is selected again after its cooldown: a retry, not owed.
        gate.select_in(R, &[key(2)], 3 + RETRY_COOLDOWN_EPOCHS, 10);
        assert!(gate.is_being_erased(&key(2)));
        assert_eq!(gate.not_yet_erased(), 0, "a retry doesn't block full");
        // Deferred as a retry, it stays a retry.
        let ready = gate.select_in(R, &[key(2)], 5 + RETRY_COOLDOWN_EPOCHS, 10);
        assert_eq!(ready, vec![key(2)]);
        gate.release_untried(R, &ready, &HashSet::new());
        assert_eq!(gate.not_yet_erased(), 0);
        gate.select_in(R, &[key(2)], 6 + RETRY_COOLDOWN_EPOCHS, 10);
        assert!(gate.is_being_erased(&key(2)));
        assert_eq!(gate.not_yet_erased(), 0);
    }

    #[test]
    fn the_retry_cooldown_is_a_whole_cycle_after_the_two_phase_wait() {
        assert_eq!(RETRY_COOLDOWN_EPOCHS, 3);
    }

    /// A key whose possession check failed is hinted again at once, stays
    /// hinted (not selected) for the cooldown, and is then selected again.
    fn a_failed_key_is_hinted_through_its_cooldown_then_selected_again(kind: ClockKind) {
        let gate = ErasureGate::default();
        let eligible = vec![key(1), key(2)];
        // Selected at 10, ready at 12.
        gate.select_in(kind, &eligible, 10, 10);
        assert!(gate.is_being_erased(&key(1)));
        assert_eq!(gate.select_in(kind, &eligible, 12, 10), eligible);
        // Its check fails at 12: no longer hidden.
        gate.fail(kind, &key(1), 12);
        assert!(
            !gate.is_being_erased(&key(1)),
            "hinted again after the failure"
        );
        assert!(gate.is_being_erased(&key(2)));
        // Still eligible, with budget to spare, but cooling: not selected.
        for epoch in 12..12 + RETRY_COOLDOWN_EPOCHS {
            assert_eq!(gate.select_in(kind, &eligible, epoch, 10), vec![key(2)]);
            assert!(
                !gate.is_being_erased(&key(1)),
                "visible through the cooldown, at {epoch}"
            );
        }
        // Then selected, hidden, and tried again two epochs later.
        let again = 12 + RETRY_COOLDOWN_EPOCHS;
        assert_eq!(gate.select_in(kind, &eligible, again, 10), vec![key(2)]);
        assert!(
            gate.is_being_erased(&key(1)),
            "selected again after the cooldown"
        );
        assert_eq!(
            gate.select_in(kind, &eligible, again + EPOCHS_BEFORE_ERASURE, 10),
            eligible
        );
    }

    #[test]
    fn a_record_whose_audit_failed_is_hinted_through_its_cooldown_then_selected_again() {
        a_failed_key_is_hinted_through_its_cooldown_then_selected_again(R);
    }

    #[test]
    fn a_pointer_left_unproven_is_hinted_through_its_cooldown_then_selected_again() {
        a_failed_key_is_hinted_through_its_cooldown_then_selected_again(P);
    }

    #[test]
    fn a_cooling_key_no_longer_eligible_is_forgotten() {
        let gate = ErasureGate::default();
        gate.select_in(R, &[key(1)], 1, 10);
        gate.fail(R, &key(1), 3);
        // Leaves eligibility (back in range, or erased elsewhere), then comes
        // back: a fresh candidate, selected at once.
        gate.select_in(R, &[], 4, 10);
        gate.select_in(R, &[key(1)], 4, 10);
        assert!(gate.is_being_erased(&key(1)));
        assert_eq!(gate.records_cooling.lock().len(), 0);
    }

    #[test]
    fn a_key_answered_present_is_kept_for_the_fetch_window() {
        let gate = ErasureGate::default();
        let now = Instant::now();
        let _ = gate.select_in(ClockKind::Records, &[key(1), key(2)], 0, 8);
        assert!(!gate.answered_present_lately(&key(1), now));
        gate.note_answered_present(&key(1), now);
        assert!(gate.answered_present_lately(
            &key(1),
            now + FETCH_WINDOW.saturating_sub(Duration::from_secs(1))
        ));
        assert!(!gate.answered_present_lately(&key(1), now + FETCH_WINDOW));
        assert!(!gate.answered_present_lately(&key(2), now));
    }

    #[test]
    fn forgotten_answers_still_keep_keys() {
        let gate = ErasureGate::default();
        let now = Instant::now();
        let keys: Vec<XorName> = (0..=MAX_ANSWERED_PRESENT)
            .map(|n| {
                let mut k = [0u8; 32];
                k[..8].copy_from_slice(&(n as u64).to_be_bytes());
                k
            })
            .collect();
        let _ = gate.select_in(ClockKind::Records, &keys, 0, usize::MAX);
        for k in &keys {
            gate.note_answered_present(k, now);
        }
        // Some key answered at `now` was forgotten: every key is kept.
        assert!(gate.answered_present_lately(&key(0xEE), now));
        assert!(!gate.answered_present_lately(&key(0xEE), now + FETCH_WINDOW));
    }

    /// Answers for keys not being erased, however many, aren't remembered:
    /// they can't fill the record and fold every key into one time, so a
    /// selected key not answered stays free to erase.
    #[test]
    fn answers_for_keys_not_being_erased_are_not_remembered() {
        let gate = ErasureGate::default();
        let now = Instant::now();
        let _ = gate.select_in(ClockKind::Records, &[key(0xEE)], 0, 8);
        let _ = gate.select_in(ClockKind::Pointers, &[key(0xEF)], 0, 8);
        for n in 0..=MAX_ANSWERED_PRESENT {
            let mut k = [0u8; 32];
            k[..8].copy_from_slice(&(n as u64).to_be_bytes());
            k[31] = 1;
            gate.note_answered_present(&k, now);
        }
        assert!(gate.answered.lock().at.is_empty());
        assert!(!gate.answered_present_lately(&key(0xEE), now));
        // A selected record or pointer answered is remembered.
        gate.note_answered_present(&key(0xEE), now);
        gate.note_answered_present(&key(0xEF), now);
        assert!(gate.answered_present_lately(&key(0xEE), now));
        assert!(gate.answered_present_lately(&key(0xEF), now));
    }

    #[test]
    fn each_node_has_its_own_gate() {
        let a = PeerId::from_bytes([1; 32]);
        let b = PeerId::from_bytes([2; 32]);
        let gate = Arc::new(ErasureGate::default());
        register_gate(&a, &gate);
        gate.select_in(R, &[key(1)], 1, 1);
        assert!(gate_for(&a).is_being_erased(&key(1)));
        assert!(!gate_for(&b).is_being_erased(&key(1)));
        drop(gate);
        assert!(!gate_for(&a).is_being_erased(&key(1)));
    }

    #[test]
    fn the_clocks_round_trip_and_a_bad_layout_starts_them_now() {
        let clocks = vec![(key(1), 1_700_000_000), (key(2), 42)];
        let decoded = decode_clocks(&encode_clocks(1_700_000_100, &clocks));
        assert_eq!(decoded.saved_at, 1_700_000_100);
        assert_eq!(decoded.starts.len(), 2);
        assert_eq!(decoded.starts.get(&key(1)), Some(&1_700_000_000));
        assert!(decode_clocks(&[]).is_empty());
        assert!(decode_clocks(&[9, 1, 2]).is_empty());
        assert!(decode_clocks(&[FORMAT_VERSION, 1, 2]).is_empty());
        let mut truncated = encode_clocks(1, &clocks);
        truncated.pop();
        assert!(decode_clocks(&truncated).is_empty());
        // A list of the first layout, with no write time, starts every clock
        // now.
        let mut v1 = vec![1u8];
        v1.extend_from_slice(&key(1));
        v1.extend_from_slice(&1_700_000_000u64.to_be_bytes());
        assert!(decode_clocks(&v1).is_empty());
    }

    #[test]
    fn a_saved_time_maps_back_to_the_same_elapsed_wait() {
        let now = Instant::now();
        // Unix second 1_800_000_000.
        let wall_now = UNIX_EPOCH + Duration::from_hours(500_000);
        let saved_at = wall_secs(now, now, wall_now);
        assert_eq!(saved_at, 1_800_000_000);
        let since = now.checked_sub(Duration::from_hours(1)).unwrap();
        let secs = wall_secs(since, now, wall_now);
        assert_eq!(secs, 1_800_000_000 - 3600);
        let back = restored_instant(secs, saved_at, now, Duration::from_hours(24));
        assert_eq!(now.duration_since(back), Duration::from_hours(1));
        // A start after the write (a wall clock that moved back) counts as now.
        let future = restored_instant(1_900_000_000, saved_at, now, Duration::from_hours(24));
        assert_eq!(future, now);
    }

    #[test]
    fn time_after_the_write_does_not_count_toward_the_wait() {
        // Saved 4 days ago, with a clock that started an hour before that
        // write: restored, it has waited an hour, not 4 days and an hour.
        let now = Instant::now();
        let wall_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let saved_at = wall_now - 4 * 24 * 3600;
        let start = saved_at - 3600;
        let back = restored_instant(start, saved_at, now, Duration::from_hours(72));
        assert_eq!(now.duration_since(back), Duration::from_hours(1));
    }

    #[tokio::test]
    async fn the_clocks_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let clocks = OutOfRangeClocks::open(dir.path()).await.unwrap();
        assert!(clocks.load(ClockKind::Records).is_empty());
        clocks
            .save(ClockKind::Records, 250, vec![(key(1), 100), (key(2), 200)])
            .await
            .unwrap();
        clocks
            .save(ClockKind::Pointers, 350, vec![(key(3), 300)])
            .await
            .unwrap();
        drop(clocks);
        let clocks = OutOfRangeClocks::open(dir.path()).await.unwrap();
        let records = clocks.load(ClockKind::Records);
        assert_eq!(records.saved_at, 250);
        assert_eq!(records.starts.get(&key(2)), Some(&200));
        let pointers = clocks.load(ClockKind::Pointers);
        assert_eq!(pointers.saved_at, 350);
        assert_eq!(pointers.starts.get(&key(3)), Some(&300));
        // A later save replaces the list and its write time: a cleared clock
        // is gone.
        clocks
            .save(ClockKind::Records, 400, vec![(key(2), 200)])
            .await
            .unwrap();
        let records = clocks.load(ClockKind::Records);
        assert_eq!(records.starts.len(), 1);
        assert_eq!(records.saved_at, 400);
        assert!(dir.path().join(OUT_OF_RANGE_DB).is_dir());
        assert!(!dir.path().join("paid_list.mdb").exists());
    }
}

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
//!    hints just before, so the cycle after that), through the usual
//!    possession check and re-check; and never while this node answered
//!    "present" for it within the last fetch window ([`FETCH_WINDOW`]), so a
//!    node that said "present" doesn't say "not found" inside that window.
//!
//! A record that is no longer eligible leaves the set, and is hinted again.
//! So does one whose possession check failed ([`ErasureGate::fail`]): its
//! close group lacks copies, and this node's hint is what lets the group
//! fetch it back. It is then left unselected for [`RETRY_COOLDOWN_EPOCHS`],
//! so it is hinted for at least a whole cycle before it is selected, hidden
//! and tried again.
//!
//! The clock that starts when a record or pointer leaves the node's range is
//! kept, in wall-clock seconds, in a small database of its own
//! ([`OutOfRangeClocks`], `{root}/out_of_range.mdb`, not inside the paid
//! list's), written at the end of each pass and loaded at start. A record
//! still out of range keeps its time, one back in range or no longer held is
//! cleared, a time in the future counts as now, and a missing or unreadable
//! table starts every clock now, as before.

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

/// The keys a node is erasing, records and pointers apart, and the keys it
/// lately answered "present" for.
#[derive(Debug, Default)]
pub struct ErasureGate {
    /// Each record being erased, with the sync epoch in which it was selected.
    records: Mutex<HashMap<XorName, u64>>,
    /// Each pointer being erased, likewise.
    pointers: Mutex<HashMap<XorName, u64>>,
    /// Each record whose possession check failed, with the sync epoch it
    /// failed in: left out of the selection, so hinted, for
    /// [`RETRY_COOLDOWN_EPOCHS`].
    records_cooling: Mutex<HashMap<XorName, u64>>,
    /// Each pointer whose check failed, likewise.
    pointers_cooling: Mutex<HashMap<XorName, u64>>,
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
    fn of(&self, kind: ClockKind) -> &Mutex<HashMap<XorName, u64>> {
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
    /// selected are selected, at most `max_new` of them, in order, except
    /// those whose check failed fewer than [`RETRY_COOLDOWN_EPOCHS`] epochs
    /// before ([`Self::fail`]), which stay hinted meanwhile. Returns the
    /// eligible keys selected [`EPOCHS_BEFORE_ERASURE`] or more epochs before,
    /// in order: those a whole cycle has passed without hinting, which the
    /// pass may now check and erase.
    pub fn select(
        &self,
        kind: ClockKind,
        eligible: &[XorName],
        epoch: u64,
        max_new: usize,
    ) -> Vec<XorName> {
        let keep: HashSet<&XorName> = eligible.iter().collect();
        let mut cooling = self.cooling_of(kind).lock();
        cooling.retain(|key, failed_at| {
            keep.contains(key) && failed_at.saturating_add(RETRY_COOLDOWN_EPOCHS) > epoch
        });
        let mut selected = self.of(kind).lock();
        selected.retain(|key, _| keep.contains(key));
        let mut ready = Vec::new();
        let mut added = 0usize;
        for key in eligible {
            match selected.get(key) {
                Some(at) => {
                    if at.saturating_add(EPOCHS_BEFORE_ERASURE) <= epoch {
                        ready.push(*key);
                    }
                }
                None => {
                    if added < max_new && !cooling.contains_key(key) {
                        selected.insert(*key, epoch);
                        added += 1;
                    }
                }
            }
        }
        ready
    }

    /// Forget `key` of `kind`: erased, or kept after all.
    pub fn release(&self, kind: ClockKind, key: &XorName) {
        self.of(kind).lock().remove(key);
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

    /// How many of the keys being erased, of both kinds, are still waiting at
    /// `epoch` for their first erasure: selected fewer than
    /// [`EPOCHS_BEFORE_ERASURE`] epochs before, so no pass has tried them yet.
    /// A node with any is not full: it has copies it may still give up.
    #[must_use]
    pub fn waiting_at(&self, epoch: u64) -> usize {
        let count = |map: &Mutex<HashMap<XorName, u64>>| {
            map.lock()
                .values()
                .filter(|at| at.saturating_add(EPOCHS_BEFORE_ERASURE) > epoch)
                .count()
        };
        count(&self.records) + count(&self.pointers)
    }

    /// Note that this node answered "present" for `key` at `now`.
    pub fn note_answered_present(&self, key: &XorName, now: Instant) {
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

/// The layout of one saved list: this byte, then 40-byte entries.
const FORMAT_VERSION: u8 = 1;

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

    /// The saved clocks of `kind`, as Unix seconds by key. Empty when none
    /// were saved or the list can't be read.
    #[must_use]
    pub fn load(&self, kind: ClockKind) -> HashMap<XorName, u64> {
        let read = || -> Option<Vec<u8>> {
            let rtxn = self.env.read_txn().ok()?;
            self.db
                .get(&rtxn, kind.db_key())
                .ok()
                .flatten()
                .map(<[u8]>::to_vec)
        };
        read().map_or_else(HashMap::new, |bytes| decode_clocks(&bytes))
    }

    /// Save the clocks of `kind`, in place of those saved before.
    ///
    /// # Errors
    ///
    /// When the write fails; the clocks saved before stay.
    pub async fn save(&self, kind: ClockKind, clocks: Vec<(XorName, u64)>) -> Result<()> {
        let env = self.env.clone();
        let db = self.db;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let bytes = encode_clocks(&clocks);
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

fn encode_clocks(clocks: &[(XorName, u64)]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + clocks.len() * ENTRY_LEN);
    bytes.push(FORMAT_VERSION);
    for (key, secs) in clocks {
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&secs.to_be_bytes());
    }
    bytes
}

fn decode_clocks(bytes: &[u8]) -> HashMap<XorName, u64> {
    let Some((&FORMAT_VERSION, entries)) = bytes.split_first() else {
        debug!("Out-of-range clocks of an unknown layout; every clock starts now");
        return HashMap::new();
    };
    let (entries, rest) = entries.as_chunks::<ENTRY_LEN>();
    if !rest.is_empty() {
        warn!("Out-of-range clocks are truncated; every clock starts now");
        return HashMap::new();
    }
    entries
        .iter()
        .filter_map(|entry| {
            let (key, secs) = entry.split_at(32);
            Some((
                key.try_into().ok()?,
                u64::from_be_bytes(secs.try_into().ok()?),
            ))
        })
        .collect()
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

/// The instant a saved clock of `saved_secs` Unix seconds stands for,
/// measured against `now` and `wall_now`.
///
/// A time in the future counts as now. Where the platform's monotonic clock
/// can't reach that far back (a host up for less than the time elapsed, off
/// Linux), the earliest of the elapsed time and `cap` that it can reach, else
/// now.
#[must_use]
pub fn restored_instant(
    saved_secs: u64,
    now: Instant,
    wall_now: SystemTime,
    cap: Duration,
) -> Instant {
    let saved = UNIX_EPOCH + Duration::from_secs(saved_secs);
    let elapsed = wall_now.duration_since(saved).unwrap_or(Duration::ZERO);
    now.checked_sub(elapsed)
        .or_else(|| now.checked_sub(elapsed.min(cap)))
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

    #[test]
    fn a_key_is_erased_only_a_whole_cycle_after_the_one_it_was_selected_in() {
        assert_eq!(EPOCHS_BEFORE_ERASURE, 2);
        let gate = ErasureGate::default();
        let eligible = vec![key(1), key(2), key(3)];
        assert_eq!(gate.select(R, &eligible, 5, 10), Vec::<XorName>::new());
        assert!(gate.is_being_erased(&key(1)));
        // The same epoch again: still not ready.
        assert_eq!(gate.select(R, &eligible, 5, 10), Vec::<XorName>::new());
        assert_eq!(gate.waiting_at(5), 3);
        // The next: the cycle that started as it was selected may have hinted it.
        assert_eq!(gate.select(R, &eligible, 6, 10), Vec::<XorName>::new());
        assert_eq!(gate.waiting_at(6), 3);
        // The one after: a whole cycle passed without hinting it.
        assert_eq!(gate.select(R, &eligible, 7, 10), eligible);
        assert_eq!(gate.waiting_at(7), 0, "each is tried at 7");
    }

    #[test]
    fn no_hint_names_a_key_being_erased() {
        let gate = ErasureGate::default();
        gate.select(R, &[key(2)], 1, 10);
        gate.select(P, &[key(4)], 1, 10);
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
        gate.select(R, &[key(1), key(2)], 1, 10);
        assert_eq!(gate.select(R, &[key(2)], 3, 10), vec![key(2)]);
        assert!(!gate.is_being_erased(&key(1)));
        gate.release(R, &key(2));
        assert_eq!(gate.selected_count(), 0);
    }

    #[test]
    fn records_and_pointers_are_selected_apart() {
        let gate = ErasureGate::default();
        gate.select(R, &[key(1)], 1, 10);
        gate.select(P, &[key(2)], 1, 10);
        // A pass of one kind keeps the other's selection.
        assert_eq!(gate.select(R, &[key(1)], 3, 10), vec![key(1)]);
        assert!(gate.is_being_erased(&key(2)));
        assert_eq!(gate.select(P, &[key(2)], 3, 10), vec![key(2)]);
        assert!(gate.is_being_erased(&key(1)));
        assert_eq!(gate.selected_count(), 2);
    }

    #[test]
    fn at_most_the_budget_is_selected_each_pass() {
        let gate = ErasureGate::default();
        let eligible: Vec<XorName> = (0..10).map(key).collect();
        gate.select(R, &eligible, 1, 4);
        assert_eq!(gate.selected_count(), 4);
        assert_eq!(gate.select(R, &eligible, 2, 4).len(), 0);
        assert_eq!(gate.selected_count(), 8);
        assert_eq!(gate.select(R, &eligible, 3, 4).len(), 4);
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
        gate.select(kind, &eligible, 10, 10);
        assert!(gate.is_being_erased(&key(1)));
        assert_eq!(gate.select(kind, &eligible, 12, 10), eligible);
        // Its check fails at 12: no longer hidden.
        gate.fail(kind, &key(1), 12);
        assert!(
            !gate.is_being_erased(&key(1)),
            "hinted again after the failure"
        );
        assert!(gate.is_being_erased(&key(2)));
        // Still eligible, with budget to spare, but cooling: not selected.
        for epoch in 12..12 + RETRY_COOLDOWN_EPOCHS {
            assert_eq!(gate.select(kind, &eligible, epoch, 10), vec![key(2)]);
            assert!(
                !gate.is_being_erased(&key(1)),
                "visible through the cooldown, at {epoch}"
            );
        }
        // Then selected, hidden, and tried again two epochs later.
        let again = 12 + RETRY_COOLDOWN_EPOCHS;
        assert_eq!(gate.select(kind, &eligible, again, 10), vec![key(2)]);
        assert!(
            gate.is_being_erased(&key(1)),
            "selected again after the cooldown"
        );
        assert_eq!(
            gate.select(kind, &eligible, again + EPOCHS_BEFORE_ERASURE, 10),
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
        gate.select(R, &[key(1)], 1, 10);
        gate.fail(R, &key(1), 3);
        // Leaves eligibility (back in range, or erased elsewhere), then comes
        // back: a fresh candidate, selected at once.
        gate.select(R, &[], 4, 10);
        gate.select(R, &[key(1)], 4, 10);
        assert!(gate.is_being_erased(&key(1)));
        assert_eq!(gate.records_cooling.lock().len(), 0);
    }

    #[test]
    fn a_key_answered_present_is_kept_for_the_fetch_window() {
        let gate = ErasureGate::default();
        let now = Instant::now();
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
        for n in 0..=MAX_ANSWERED_PRESENT {
            let mut k = [0u8; 32];
            k[..8].copy_from_slice(&(n as u64).to_be_bytes());
            gate.note_answered_present(&k, now);
        }
        // Some key answered at `now` was forgotten: every key is kept.
        assert!(gate.answered_present_lately(&key(0xEE), now));
        assert!(!gate.answered_present_lately(&key(0xEE), now + FETCH_WINDOW));
    }

    #[test]
    fn each_node_has_its_own_gate() {
        let a = PeerId::from_bytes([1; 32]);
        let b = PeerId::from_bytes([2; 32]);
        let gate = Arc::new(ErasureGate::default());
        register_gate(&a, &gate);
        gate.select(R, &[key(1)], 1, 1);
        assert!(gate_for(&a).is_being_erased(&key(1)));
        assert!(!gate_for(&b).is_being_erased(&key(1)));
        drop(gate);
        assert!(!gate_for(&a).is_being_erased(&key(1)));
    }

    #[test]
    fn the_clocks_round_trip_and_a_bad_layout_starts_them_now() {
        let clocks = vec![(key(1), 1_700_000_000), (key(2), 42)];
        let decoded = decode_clocks(&encode_clocks(&clocks));
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.get(&key(1)), Some(&1_700_000_000));
        assert!(decode_clocks(&[]).is_empty());
        assert!(decode_clocks(&[9, 1, 2]).is_empty());
        let mut truncated = encode_clocks(&clocks);
        truncated.pop();
        assert!(decode_clocks(&truncated).is_empty());
    }

    #[test]
    fn a_saved_time_maps_back_to_the_same_elapsed_wait() {
        let now = Instant::now();
        // Unix second 1_800_000_000.
        let wall_now = UNIX_EPOCH + Duration::from_hours(500_000);
        let since = now.checked_sub(Duration::from_hours(1)).unwrap();
        let secs = wall_secs(since, now, wall_now);
        assert_eq!(secs, 1_800_000_000 - 3600);
        let back = restored_instant(secs, now, wall_now, Duration::from_hours(24));
        assert_eq!(now.duration_since(back), Duration::from_hours(1));
        // A time in the future counts as now.
        let future = restored_instant(1_900_000_000, now, wall_now, Duration::from_hours(24));
        assert_eq!(future, now);
    }

    #[tokio::test]
    async fn the_clocks_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let clocks = OutOfRangeClocks::open(dir.path()).await.unwrap();
        assert!(clocks.load(ClockKind::Records).is_empty());
        clocks
            .save(ClockKind::Records, vec![(key(1), 100), (key(2), 200)])
            .await
            .unwrap();
        clocks
            .save(ClockKind::Pointers, vec![(key(3), 300)])
            .await
            .unwrap();
        drop(clocks);
        let clocks = OutOfRangeClocks::open(dir.path()).await.unwrap();
        let records = clocks.load(ClockKind::Records);
        assert_eq!(records.get(&key(2)), Some(&200));
        assert_eq!(clocks.load(ClockKind::Pointers).get(&key(3)), Some(&300));
        // A later save replaces the list: a cleared clock is gone.
        clocks
            .save(ClockKind::Records, vec![(key(2), 200)])
            .await
            .unwrap();
        assert_eq!(clocks.load(ClockKind::Records).len(), 1);
        assert!(dir.path().join(OUT_OF_RANGE_DB).is_dir());
        assert!(!dir.path().join("paid_list.mdb").exists());
    }
}

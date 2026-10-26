//! What a node keeps, and when it may erase.
//!
//! A node keeps every record and pointer it holds while its disk has room,
//! including those it is no longer responsible for: they stay stored, are
//! served, and are offered to their close group by neighbour sync. The prune
//! passes ([`super::pruning`] for chunks, [`super::pointer`] for pointers)
//! erase only under disk pressure, and then only records far from the node:
//!
//! - **Free space** is the smaller of two shares of the partition holding the
//!   record store: its free bytes, measured as the chunk store's capacity
//!   guard measures them, and its free inodes, where the filesystem counts
//!   them (a filesystem that reports none, or a query that fails, leaves bytes
//!   alone). A byte measurement that fails counts as no pressure.
//! - **Disk pressure** starts when free space falls to the first step of the
//!   falling floor below, and lasts until free space is back to at least
//!   [`DISK_PRESSURE_END_PERCENT`] (15%) of bytes and inodes, and to the
//!   clamp's level plus [`LEAVE_FULL_MARGIN`]. The state lives in memory, is
//!   shared by both passes and the storage migration's shed, and starts
//!   without pressure when the node starts.
//! - **The falling floor.** Only records farther from the node than the
//!   floor may be erased. The floor falls as free space does, through
//!   [`FLOOR_STEPS`]: 10 times the XOR distance from the node to its own
//!   `close_group_size`-th nearest peer (the 7th at production parameters,
//!   from the local routing table, the node itself not counted) below 10%
//!   free, 5 times below 7.5%, 2 times below 5%, and the edge of the node's
//!   own range below 2.5%. At the edge no distance is kept: every record the
//!   prune pass counts as a candidate is eligible, which is a record for which
//!   the node has not been among the retention width's nearest (9, more with
//!   full peers among them) for the whole out-of-range wait; no record within
//!   the width ever is. With fewer peers known than the close group, nothing
//!   may be erased. "Farther" is strict, and multiples are computed without
//!   overflow: past the end of the key space nothing is farther.
//! - **Small partitions.** Each step also fires once free bytes are at or
//!   below the full line plus one chunk ([`STEP_CLAMP_ABOVE_LINE`]), so on a
//!   partition too small for a step's share to clear the line, the steps
//!   collapse in order onto the line and the last one always runs before the
//!   node is "full". A store with no line (`disk_reserve_mb` 0) has no clamp.
//! - **Order.** Eligible records go farthest from the node first, chunks and
//!   pointers each in their own pass. A record whose possession check failed
//!   earlier in the same spell of pressure is tried after those not tried yet,
//!   so the per-pass budgets reach every eligible record in turn. Each is
//!   erased only after the nodes now nearest to it prove they hold it and the
//!   pass's re-check just before deleting passes. Free space is measured again
//!   before each record, and erasing stops once the pressure is over.
//! - **At the line.** Free bytes at or below the clamp's level, or free inodes
//!   below the last step's 2.5% share, are "at the line": a node there whose
//!   last pass at the edge erased nothing is full (see [`super::full`]),
//!   until free space is back above the line by [`LEAVE_FULL_MARGIN`].
//!
//! A failed proof of possession for a key beyond ten times the distance to
//! the node's 7th-nearest peer (the floor's first step, whatever step it is
//! at) is not penalised: the node's routing table may not know which nodes
//! are nearest a key that far away.
//!
//! Everything else about pruning stays as it was: the out-of-range wait, the
//! retained-commitment veto, the bootstrap deferral and the audit budgets.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
#[cfg(any(test, feature = "test-utils"))]
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use saorsa_core::identity::PeerId;
use saorsa_core::P2PNode;

use crate::ant_protocol::{XorName, MAX_CHUNK_SIZE};
use crate::client::xor_distance;
use crate::logging::{debug, info, warn};
use crate::storage::chunk_store::CHUNKS_DIR_NAME;
use crate::storage::{ChunkStore, FullTrigger, Inodes, INODES_AT_THE_LINE_PER_MILLE, MIB};

#[cfg(test)]
mod census;

/// Disk pressure starts when free space falls below this share of the
/// partition holding the record store, in percent: the falling floor's first
/// step.
pub const DISK_PRESSURE_START_PERCENT: u64 = 10;

/// Disk pressure ends once free space is back to at least this share of the
/// partition, in percent.
pub const DISK_PRESSURE_END_PERCENT: u64 = 15;

/// The multiple of the distance to the node's `close_group_size`-th nearest
/// peer at the falling floor's first step, and the reach within which a
/// failed proof of possession is penalised.
pub const ERASE_FLOOR_MULTIPLE: u8 = 10;

/// How far a step of the falling floor lets erasure reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// Records farther than this many times the distance to the node's
    /// `close_group_size`-th nearest peer.
    Multiple(u8),
    /// The edge of the node's own range: every record the prune pass counts
    /// as a candidate, with no distance kept.
    Edge,
}

/// One step of the falling floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorStep {
    /// The step fires once free space is below this share of the partition,
    /// in tenths of a percent.
    pub below_per_mille: u64,
    /// How far erasure reaches from it on.
    pub reach: Reach,
}

/// The falling floor's steps, first to last.
pub const FLOOR_STEPS: [FloorStep; 4] = [
    FloorStep {
        below_per_mille: DISK_PRESSURE_START_PERCENT * 10,
        reach: Reach::Multiple(ERASE_FLOOR_MULTIPLE),
    },
    FloorStep {
        below_per_mille: 75,
        reach: Reach::Multiple(5),
    },
    FloorStep {
        below_per_mille: 50,
        reach: Reach::Multiple(2),
    },
    FloorStep {
        below_per_mille: INODES_AT_THE_LINE_PER_MILLE,
        reach: Reach::Edge,
    },
];

/// The index of the falling floor's last step, the edge of the node's range.
pub const LAST_FLOOR_STEP: usize = FLOOR_STEPS.len() - 1;

/// Each step also fires once free bytes are at or below the full line plus
/// this: one chunk, the most a single record takes.
pub const STEP_CLAMP_ABOVE_LINE: u64 = MAX_CHUNK_SIZE as u64;

/// How far above the full line free bytes must be for a full node to leave
/// "full", and for disk pressure that the clamp started to end.
///
/// 64 chunks, a whole wave of an upload, so a node that leaves "full" can
/// take at least one before it is back at the line.
pub const LEAVE_FULL_MARGIN: u64 = 64 * STEP_CLAMP_ABOVE_LINE;

/// How many inodes above the last step's share a full node needs to leave
/// "full" when inodes put it there.
///
/// The margin in records of the smallest allocation, 4 KiB, one inode each. Bounded by the filesystem's own size
/// (see [`leave_full_inode_margin`]), so a filesystem with few inodes can
/// always reach it.
pub const LEAVE_FULL_INODE_MARGIN: u64 = LEAVE_FULL_MARGIN / (4 * 1024);

/// The inode margin for leaving "full" on a filesystem of `total` inodes.
///
/// [`LEAVE_FULL_INODE_MARGIN`], or a fortieth of the inodes (2.5%, the line's
/// own share) where that is smaller. The line's share plus this is at most
/// 5% of the inodes, so a node whose inodes are freed can always leave.
#[must_use]
pub fn leave_full_inode_margin(total: u64) -> u64 {
    LEAVE_FULL_INODE_MARGIN.min(total / 40)
}

const _: () = assert!(
    LEAVE_FULL_MARGIN == 256 * MIB,
    "the margin for leaving full is 256 MiB"
);
const _: () = assert!(
    FLOOR_STEPS[0].below_per_mille > FLOOR_STEPS[1].below_per_mille
        && FLOOR_STEPS[1].below_per_mille > FLOOR_STEPS[2].below_per_mille
        && FLOOR_STEPS[2].below_per_mille > FLOOR_STEPS[3].below_per_mille,
    "the floor falls step by step"
);

/// Free and total bytes of the partition holding the record store, and its
/// inodes where the filesystem counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskSpace {
    /// Bytes available to the node on the partition.
    pub available: u64,
    /// The partition's size in bytes.
    pub total: u64,
    /// Free and total inodes; `None` where they aren't counted, and bytes
    /// alone count.
    pub inodes: Option<Inodes>,
}

impl DiskSpace {
    /// A measurement of bytes alone.
    #[must_use]
    pub const fn bytes(available: u64, total: u64) -> Self {
        Self {
            available,
            total,
            inodes: None,
        }
    }

    /// Whether free bytes are below `per_mille` tenths of a percent of the
    /// partition.
    fn bytes_below_per_mille(self, per_mille: u64) -> bool {
        u128::from(self.available) * 1000 < u128::from(self.total) * u128::from(per_mille)
    }

    /// Whether free space, the smaller of the byte and inode shares, is below
    /// `per_mille` tenths of a percent of the partition.
    fn below_per_mille(self, per_mille: u64) -> bool {
        self.bytes_below_per_mille(per_mille)
            || self
                .inodes
                .is_some_and(|inodes| inodes.below_per_mille(per_mille))
    }

    /// Free space as a percentage of the partition, for logs.
    #[allow(clippy::cast_precision_loss)]
    #[cfg_attr(not(feature = "logging"), allow(dead_code))]
    fn free_percent(self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.available as f64 * 100.0 / self.total as f64
        }
    }
}

/// The level at or below which every step of the falling floor fires: the
/// full line plus [`STEP_CLAMP_ABOVE_LINE`]. Zero, no clamp, for a store
/// with no line.
#[must_use]
pub const fn step_clamp(line: u64) -> u64 {
    if line == 0 {
        0
    } else {
        line.saturating_add(STEP_CLAMP_ABOVE_LINE)
    }
}

/// Whether the step `index` of [`FLOOR_STEPS`] fires at `space`, for a store
/// whose full line is `line`.
///
/// It fires at free space below the step's share, or free bytes at or below
/// [`step_clamp`]. A partition reported as having no size reaches no step.
#[must_use]
pub fn step_fires(index: usize, space: DiskSpace, line: u64) -> bool {
    let Some(step) = FLOOR_STEPS.get(index) else {
        return false;
    };
    if space.total == 0 {
        return false;
    }
    let clamp = step_clamp(line);
    space.below_per_mille(step.below_per_mille) || (clamp > 0 && space.available <= clamp)
}

/// The deepest step of the falling floor that fires at `space`, if any.
#[must_use]
pub fn deepest_step(space: DiskSpace, line: u64) -> Option<usize> {
    (0..FLOOR_STEPS.len())
        .rev()
        .find(|index| step_fires(*index, space, line))
}

/// What puts a node with `space` at the full line `line`, if it is there:
/// free bytes at or below [`step_clamp`] (the line itself for a store with
/// none), or free inodes below the last step's share.
#[must_use]
pub fn at_the_line(space: DiskSpace, line: u64) -> Option<FullTrigger> {
    if space.available <= step_clamp(line) {
        Some(FullTrigger::Bytes)
    } else if space.inodes.is_some_and(Inodes::at_the_line) {
        Some(FullTrigger::Inodes)
    } else {
        None
    }
}

/// Whether a full node with `space`, put at the line by `trigger`, has the
/// room to leave "full".
///
/// That is free bytes at least [`LEAVE_FULL_MARGIN`] above the line, and,
/// where inodes are counted, inodes not at the line. When inodes put the node
/// in "full", they also need [`leave_full_inode_margin`] above the last
/// step's share, so the state doesn't flap; when bytes did, the inodes need
/// no margin of their own.
#[must_use]
pub fn may_leave_full(space: DiskSpace, line: u64, trigger: FullTrigger) -> bool {
    let bytes = space.available >= line.saturating_add(LEAVE_FULL_MARGIN);
    let inodes = space.inodes.is_none_or(|inodes| match trigger {
        FullTrigger::Bytes => !inodes.at_the_line(),
        FullTrigger::Inodes => {
            let at_the_line =
                u128::from(inodes.total) * u128::from(INODES_AT_THE_LINE_PER_MILLE) / 1000;
            u128::from(inodes.free)
                >= at_the_line + u128::from(leave_full_inode_margin(inodes.total))
        }
    });
    bytes && inodes
}

/// Whether the node is under disk pressure after measuring `space`, given
/// whether it was before the measurement, for a store with no full line.
///
/// See [`next_disk_pressure_at`].
#[must_use]
pub fn next_disk_pressure(was_under_pressure: bool, space: Option<DiskSpace>) -> bool {
    next_disk_pressure_at(was_under_pressure, space, 0)
}

/// Whether the node is under disk pressure after measuring `space`, given
/// whether it was before the measurement, for a store whose full line is
/// `line`.
///
/// Pressure starts at the falling floor's first step (below
/// [`DISK_PRESSURE_START_PERCENT`] free, or at the clamp), holds while free
/// space stays below [`DISK_PRESSURE_END_PERCENT`] or the clamp's level plus
/// [`LEAVE_FULL_MARGIN`], and ends once both are reached. A failed
/// measurement (`None`), or a partition reported as having no size, is no
/// pressure.
#[must_use]
pub fn next_disk_pressure_at(
    was_under_pressure: bool,
    space: Option<DiskSpace>,
    line: u64,
) -> bool {
    let Some(space) = space.filter(|space| space.total > 0) else {
        return false;
    };
    if was_under_pressure {
        let clamp = step_clamp(line);
        let below_clamp_margin =
            clamp > 0 && space.available < clamp.saturating_add(LEAVE_FULL_MARGIN);
        space.below_per_mille(DISK_PRESSURE_END_PERCENT * 10) || below_clamp_margin
    } else {
        step_fires(0, space, line)
    }
}

/// The erase floor of a node whose routing table holds `peers` at the
/// falling floor's first step: [`ERASE_FLOOR_MULTIPLE`] times the XOR
/// distance from the node to its `close_group_size`-th nearest peer.
///
/// `None` when fewer peers than that are known, so nothing may be erased. A
/// multiple past the end of the key space saturates to the largest distance,
/// which no record is farther than.
#[must_use]
pub fn erase_floor(
    self_id: &XorName,
    peers: &[XorName],
    close_group_size: usize,
) -> Option<XorName> {
    erase_floor_at(
        self_id,
        peers,
        close_group_size,
        Reach::Multiple(ERASE_FLOOR_MULTIPLE),
    )
}

/// The erase floor of a node whose routing table holds `peers`, at a step
/// reaching `reach`.
///
/// That many times the distance to its `close_group_size`-th nearest peer,
/// or, at the edge of its range, zero, so that every key but the node's own
/// address is beyond it.
///
/// `None` when fewer peers than that are known, at any step.
#[must_use]
pub fn erase_floor_at(
    self_id: &XorName,
    peers: &[XorName],
    close_group_size: usize,
    reach: Reach,
) -> Option<XorName> {
    let rank = close_group_size.checked_sub(1)?;
    let mut distances: Vec<XorName> = peers
        .iter()
        .filter(|peer| *peer != self_id)
        .map(|peer| xor_distance(self_id, peer))
        .collect();
    distances.sort_unstable();
    distances.dedup();
    let distance = distances.get(rank)?;
    Some(match reach {
        Reach::Multiple(factor) => saturating_multiply(distance, factor),
        Reach::Edge => [0u8; 32],
    })
}

/// `distance * factor` as a 256-bit big-endian number, or the largest
/// distance when the product does not fit.
fn saturating_multiply(distance: &XorName, factor: u8) -> XorName {
    let mut product = [0u8; 32];
    let mut carry = 0u16;
    for (out, byte) in product.iter_mut().zip(distance.iter()).rev() {
        let value = u16::from(*byte) * u16::from(factor) + carry;
        let [high, low] = value.to_be_bytes();
        *out = low;
        carry = u16::from(high);
    }
    if carry == 0 {
        product
    } else {
        [u8::MAX; 32]
    }
}

/// Whether `key` lies strictly farther from the node than `floor`. Nothing
/// does when there is no floor.
#[must_use]
pub fn is_beyond_floor(self_id: &XorName, key: &XorName, floor: Option<&XorName>) -> bool {
    floor.is_some_and(|floor| xor_distance(self_id, key) > *floor)
}

/// The floors one pass works with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Floors {
    /// The floor at the step the node is at: records beyond it may be erased.
    pub erase: Option<XorName>,
    /// The floor's first step, [`ERASE_FLOOR_MULTIPLE`] times the distance: a
    /// failed proof for a key beyond it is not penalised, whatever the step.
    pub penalty: Option<XorName>,
}

/// The records of one kind whose possession check failed during the current
/// spell of disk pressure, and when.
///
/// Process-local. [`erase_order`] keeps only the records that are still
/// eligible, so it forgets them all once the pressure is over.
#[derive(Debug, Default)]
pub struct FailedChecks {
    failed_at: Mutex<HashMap<XorName, Instant>>,
}

impl FailedChecks {
    /// Note that the possession check for `key` failed just now.
    pub fn record(&self, key: &XorName) {
        self.failed_at.lock().insert(*key, Instant::now());
    }

    /// Forget `key`: it was erased, or is no longer a candidate.
    pub fn forget(&self, key: &XorName) {
        self.failed_at.lock().remove(key);
    }

    /// When the possession check for `key` last failed, if it has during
    /// this spell of pressure.
    #[must_use]
    pub fn last_failed(&self, key: &XorName) -> Option<Instant> {
        self.failed_at.lock().get(key).copied()
    }
}

/// The candidates a pass may try to erase, in the order it tries them.
///
/// None without disk pressure, and only those strictly beyond the floor,
/// farthest from the node first. Those whose possession check already failed
/// during this spell of pressure come after the ones not tried yet, the one
/// that failed longest ago first. `failures` forgets every record that is not
/// eligible now.
pub fn erase_order<T>(
    under_disk_pressure: bool,
    self_id: &XorName,
    floor: Option<&XorName>,
    failures: &FailedChecks,
    candidates: Vec<T>,
    key_of: impl Fn(&T) -> XorName,
) -> Vec<T> {
    let Some(floor) = floor.filter(|_| under_disk_pressure) else {
        failures.failed_at.lock().clear();
        return Vec::new();
    };
    let eligible: Vec<(XorName, XorName, T)> = candidates
        .into_iter()
        .filter_map(|candidate| {
            let key = key_of(&candidate);
            let distance = xor_distance(self_id, &key);
            (distance > *floor).then_some((key, distance, candidate))
        })
        .collect();

    let mut failed_at = failures.failed_at.lock();
    let eligible_keys: HashSet<XorName> = eligible.iter().map(|(key, _, _)| *key).collect();
    failed_at.retain(|key, _| eligible_keys.contains(key));
    let mut ordered: Vec<(Option<Instant>, Reverse<XorName>, T)> = eligible
        .into_iter()
        .map(|(key, distance, candidate)| {
            (failed_at.get(&key).copied(), Reverse(distance), candidate)
        })
        .collect();
    drop(failed_at);

    // Not tried yet (`None`) before failed, then farthest first.
    ordered.sort_by_key(|(failed, distance, _)| (*failed, *distance));
    ordered
        .into_iter()
        .map(|(_, _, candidate)| candidate)
        .collect()
}

/// Run `step` on each candidate in turn while the node stays under disk
/// pressure, and return what each step returned.
///
/// Free space is measured again before each candidate, so erasing stops as
/// soon as the pressure is over or free space can no longer be measured. The
/// remaining candidates are left for a later pass.
pub(crate) async fn erase_while_under_pressure<T, O, F, Fut>(
    retention: &DiskRetention,
    candidates: Vec<T>,
    mut step: F,
) -> Vec<O>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = O>,
{
    let mut outcomes = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if !retention.refresh() {
            break;
        }
        outcomes.push(step(candidate).await);
    }
    outcomes
}

/// What the last measurement found.
#[derive(Debug, Clone, Copy, Default)]
struct Measured {
    /// Whether the node is under disk pressure.
    under_pressure: bool,
    /// The deepest step of the falling floor reached, while under pressure:
    /// at least the first.
    step: Option<usize>,
    /// What puts the node at the full line, if it is there.
    at_the_line: Option<FullTrigger>,
    /// The measurement itself, if it succeeded.
    space: Option<DiskSpace>,
}

/// The disk-pressure state of one node, and the measurements behind it.
///
/// Shared by the chunk and pointer prune passes and the storage migration, so
/// they never disagree about whether the node is erasing, or how far. It also
/// holds the records whose possession check failed during the current spell
/// of pressure.
pub struct DiskRetention {
    /// A directory on the partition holding the record store: the chunk
    /// store's own, which its capacity guard measures.
    dir: PathBuf,
    /// The full line: the free bytes the chunk store keeps unused.
    line: u64,
    /// What the last measurement found.
    measured: Mutex<Measured>,
    /// Chunks whose possession check failed during this spell of pressure.
    failed_chunk_checks: FailedChecks,
    /// Pointers whose possession check failed during this spell of pressure.
    failed_pointer_checks: FailedChecks,
    /// Inputs a test fixed instead of reading the disk and the routing table.
    #[cfg(any(test, feature = "test-utils"))]
    fixed: Mutex<FixedInputs>,
}

/// A measurement a test supplies in place of the disk's.
#[cfg(any(test, feature = "test-utils"))]
type SpaceSource = Arc<dyn Fn() -> Option<DiskSpace> + Send + Sync>;

/// Inputs a test fixed.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
struct FixedInputs {
    /// Measured instead of the disk, at every measurement.
    space: Option<SpaceSource>,
    floor: Fixed<Option<XorName>>,
}

/// One input a test may fix.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Default, Clone, Copy)]
enum Fixed<T> {
    /// Read the real source.
    #[default]
    Real,
    /// Use this value instead.
    To(T),
}

impl DiskRetention {
    /// The state for a node whose chunk store is `storage`: free space is
    /// measured on the partition holding the store's chunk directory, against
    /// the store's full line.
    #[must_use]
    pub fn for_store(storage: &ChunkStore) -> Self {
        Self::new(storage.root_dir().join(CHUNKS_DIR_NAME)).with_line(storage.full_line())
    }

    /// The state for a record store on the partition holding `dir`, with no
    /// full line. Starts without disk pressure.
    #[must_use]
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            line: 0,
            measured: Mutex::new(Measured::default()),
            failed_chunk_checks: FailedChecks::default(),
            failed_pointer_checks: FailedChecks::default(),
            #[cfg(any(test, feature = "test-utils"))]
            fixed: Mutex::new(FixedInputs::default()),
        }
    }

    /// The same state, measured against the full line `line`.
    #[must_use]
    pub fn with_line(mut self, line: u64) -> Self {
        self.line = line;
        self
    }

    /// The full line this state measures against.
    #[must_use]
    pub fn line(&self) -> u64 {
        self.line
    }

    /// Whether the node was under disk pressure at the last measurement.
    #[must_use]
    pub fn under_pressure(&self) -> bool {
        self.measured.lock().under_pressure
    }

    /// The deepest step of the falling floor the last measurement reached,
    /// as an index into [`FLOOR_STEPS`]: `None` without disk pressure.
    #[must_use]
    pub fn step(&self) -> Option<usize> {
        self.measured.lock().step
    }

    /// What put the node at the full line at the last measurement, if it was
    /// there.
    #[must_use]
    pub fn at_the_line(&self) -> Option<FullTrigger> {
        self.measured.lock().at_the_line
    }

    /// The last successful measurement, if the last one succeeded.
    #[must_use]
    pub fn last_space(&self) -> Option<DiskSpace> {
        self.measured.lock().space
    }

    /// The chunks whose possession check failed during this spell of
    /// pressure.
    #[must_use]
    pub fn failed_chunk_checks(&self) -> &FailedChecks {
        &self.failed_chunk_checks
    }

    /// The pointers whose possession check failed during this spell of
    /// pressure.
    #[must_use]
    pub fn failed_pointer_checks(&self) -> &FailedChecks {
        &self.failed_pointer_checks
    }

    /// Measure free space again and update the pressure state, the step and
    /// whether the node is at the line. Returns whether the node is now under
    /// disk pressure.
    pub fn refresh(&self) -> bool {
        let space = self.measure().filter(|space| space.total > 0);
        let mut measured = self.measured.lock();
        let before = *measured;
        let under_pressure = next_disk_pressure_at(before.under_pressure, space, self.line);
        let step = match space {
            Some(space) if under_pressure => Some(deepest_step(space, self.line).unwrap_or(0)),
            _ => None,
        };
        *measured = Measured {
            under_pressure,
            step,
            at_the_line: space.and_then(|space| at_the_line(space, self.line)),
            space,
        };
        drop(measured);
        log_pressure_change(before, under_pressure, step, space);
        under_pressure
    }

    /// Free and total bytes and inodes of the partition, or `None` if the
    /// bytes could not be read.
    fn measure(&self) -> Option<DiskSpace> {
        #[cfg(any(test, feature = "test-utils"))]
        {
            let source = self.fixed.lock().space.clone();
            if let Some(source) = source {
                return source();
            }
            if let Some(space) = crate::test_seams::free_space() {
                return match space {
                    Ok(space) => Some(DiskSpace {
                        available: space.free,
                        total: match space.total {
                            Some(total) => total,
                            None => fs2::total_space(&self.dir).ok()?,
                        },
                        inodes: space
                            .inodes
                            .map(|(free, total)| Inodes { free, total })
                            .filter(|inodes| inodes.total > 0),
                    }),
                    Err(e) => {
                        debug!("The free-space seam could not be read: {e}");
                        None
                    }
                };
            }
        }
        match fs2::statvfs(&self.dir) {
            Ok(stats) => Some(DiskSpace {
                available: stats.available_space(),
                total: stats.total_space(),
                inodes: crate::storage::free_inodes(&self.dir),
            }),
            Err(e) => {
                debug!(
                    "Could not measure free space on the partition holding {}: {e}",
                    self.dir.display()
                );
                None
            }
        }
    }

    /// The floors from the node's local routing table at the step the last
    /// measurement reached (see [`erase_floor_at`]): `erase` is `None`, and
    /// nothing may be erased, without disk pressure or with too few peers
    /// known.
    pub async fn floors_from_routing_table(
        &self,
        p2p_node: &P2PNode,
        self_id: &PeerId,
        close_group_size: usize,
    ) -> Floors {
        #[cfg(any(test, feature = "test-utils"))]
        {
            let fixed = self.fixed.lock().floor;
            if let Fixed::To(floor) = fixed {
                return Floors {
                    erase: floor,
                    penalty: floor,
                };
            }
        }
        let peers: Vec<XorName> = p2p_node
            .dht_manager()
            .find_closest_nodes_local(self_id.as_bytes(), close_group_size)
            .await
            .into_iter()
            .map(|node| *node.peer_id.as_bytes())
            .collect();
        self.floors_from_peers(self_id.as_bytes(), &peers, close_group_size)
    }

    /// The floors for a node with `peers` at the step the last measurement
    /// reached (see [`Self::floors_from_routing_table`]).
    #[must_use]
    pub fn floors_from_peers(
        &self,
        self_id: &XorName,
        peers: &[XorName],
        close_group_size: usize,
    ) -> Floors {
        let reach = self
            .step()
            .and_then(|step| FLOOR_STEPS.get(step))
            .map(|step| step.reach);
        Floors {
            erase: reach.and_then(|reach| erase_floor_at(self_id, peers, close_group_size, reach)),
            penalty: erase_floor(self_id, peers, close_group_size),
        }
    }
}

/// Log a change in the pressure state or the floor's step.
#[cfg_attr(not(feature = "logging"), allow(unused_variables))]
fn log_pressure_change(
    before: Measured,
    under_pressure: bool,
    step: Option<usize>,
    space: Option<DiskSpace>,
) {
    let inodes_note = |space: DiskSpace| {
        space.inodes.map_or_else(String::new, |inodes| {
            format!(", {} of {} inodes free", inodes.free, inodes.total)
        })
    };
    match (before.under_pressure, under_pressure, space) {
        (false, true, Some(space)) => info!(
            "Disk pressure: {:.1}% of the record store's partition is free{}; records far from \
             this node may now be erased, farthest first, until {DISK_PRESSURE_END_PERCENT}% is \
             free (floor step {}: {})",
            space.free_percent(),
            inodes_note(space),
            step.unwrap_or(0) + 1,
            step_reach_text(step),
        ),
        (true, true, Some(space)) if before.step != step => info!(
            "Disk pressure: the erase floor moves to step {} ({}); {:.1}% of the record store's \
             partition is free{}",
            step.unwrap_or(0) + 1,
            step_reach_text(step),
            space.free_percent(),
            inodes_note(space),
        ),
        (true, false, Some(space)) => info!(
            "Disk pressure over: {:.1}% of the record store's partition is free{}; keeping every \
             record again",
            space.free_percent(),
            inodes_note(space),
        ),
        (true, false, None) => warn!(
            "Free space on the record store's partition could not be measured; \
             erasing stops until it can"
        ),
        _ => {}
    }
}

/// How far a step reaches, for logs.
#[cfg_attr(not(feature = "logging"), allow(dead_code))]
fn step_reach_text(step: Option<usize>) -> String {
    match step
        .and_then(|step| FLOOR_STEPS.get(step))
        .map(|step| step.reach)
    {
        Some(Reach::Multiple(factor)) => {
            format!("{factor} times the distance to the node's 7th-nearest peer")
        }
        Some(Reach::Edge) => "the edge of the node's own range".to_string(),
        None => "none".to_string(),
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl DiskRetention {
    /// From now on, measure `space` instead of the disk (`None`: the
    /// measurement fails). Tests only.
    pub fn set_space_for_test(&self, space: Option<DiskSpace>) {
        self.set_space_source_for_test(move || space);
    }

    /// From now on, call `source` at every measurement instead of reading the
    /// disk, so a test can model the space each erasure returns. Tests only.
    pub fn set_space_source_for_test(
        &self,
        source: impl Fn() -> Option<DiskSpace> + Send + Sync + 'static,
    ) {
        self.fixed.lock().space = Some(Arc::new(source));
    }

    /// From now on, use `floor` instead of the routing table's, for erasure
    /// and for penalties alike (`None`: nothing may be erased; all zeroes:
    /// every other key is beyond it). Tests only.
    pub fn set_floor_for_test(&self, floor: Option<XorName>) {
        self.fixed.lock().floor = Fixed::To(floor);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// A partition of 1000 bytes with `available` free, as a measurement.
    #[allow(clippy::unnecessary_wraps)]
    fn space(available: u64) -> Option<DiskSpace> {
        Some(DiskSpace::bytes(available, 1000))
    }

    /// A key at XOR distance `distance` from the all-zero node id.
    fn at_distance(distance: XorName) -> XorName {
        distance
    }

    /// A distance whose last byte is `low` and second-last byte is `high`.
    fn distance(high: u8, low: u8) -> XorName {
        let mut d = [0u8; 32];
        d[30] = high;
        d[31] = low;
        d
    }

    const NODE: XorName = [0u8; 32];

    /// [`erase_order`] with no failed checks to account for.
    fn order<T>(
        under_disk_pressure: bool,
        floor: Option<&XorName>,
        candidates: Vec<T>,
        key_of: impl Fn(&T) -> XorName,
    ) -> Vec<T> {
        erase_order(
            under_disk_pressure,
            &NODE,
            floor,
            &FailedChecks::default(),
            candidates,
            key_of,
        )
    }

    // -- Disk pressure -------------------------------------------------------

    #[test]
    fn pressure_starts_only_below_ten_percent_free() {
        assert!(!next_disk_pressure(false, space(1000)));
        assert!(!next_disk_pressure(false, space(120)));
        assert!(
            !next_disk_pressure(false, space(100)),
            "exactly 10% free is not below 10%"
        );
        assert!(next_disk_pressure(false, space(99)));
        assert!(next_disk_pressure(false, space(0)));
    }

    #[test]
    fn pressure_holds_between_ten_and_fifteen_percent() {
        for available in [99, 100, 120, 149] {
            assert!(
                next_disk_pressure(true, space(available)),
                "{available} of 1000 free must keep the pressure on"
            );
            assert!(
                !next_disk_pressure(false, space(available.max(100))),
                "{available} of 1000 free must not start it"
            );
        }
    }

    #[test]
    fn pressure_ends_at_fifteen_percent_or_more() {
        assert!(!next_disk_pressure(true, space(150)));
        assert!(!next_disk_pressure(true, space(151)));
        assert!(!next_disk_pressure(true, space(1000)));
    }

    #[test]
    fn a_failed_measurement_is_no_pressure() {
        assert!(!next_disk_pressure(false, None));
        assert!(
            !next_disk_pressure(true, None),
            "a failed measurement ends the pressure"
        );
        let no_size = Some(DiskSpace::bytes(0, 0));
        assert!(!next_disk_pressure(false, no_size));
        assert!(!next_disk_pressure(true, no_size));
    }

    #[test]
    fn pressure_arithmetic_does_not_overflow_on_huge_partitions() {
        let huge = Some(DiskSpace::bytes(u64::MAX / 20, u64::MAX));
        assert!(next_disk_pressure(false, huge), "5% free starts pressure");
        let roomy = Some(DiskSpace::bytes(u64::MAX / 4, u64::MAX));
        assert!(!next_disk_pressure(true, roomy), "25% free ends it");
    }

    #[test]
    fn the_shared_state_latches_between_measurements() {
        let retention = DiskRetention::new(PathBuf::from("."));
        assert!(
            !retention.under_pressure(),
            "a node starts without pressure"
        );

        retention.set_space_for_test(space(120));
        assert!(!retention.refresh(), "12% free does not start pressure");
        retention.set_space_for_test(space(90));
        assert!(retention.refresh(), "9% free starts it");
        retention.set_space_for_test(space(120));
        assert!(retention.refresh(), "12% free keeps it");
        assert!(retention.under_pressure());
        retention.set_space_for_test(space(150));
        assert!(!retention.refresh(), "15% free ends it");
        retention.set_space_for_test(space(120));
        assert!(!retention.refresh(), "and 12% does not restart it");
        retention.set_space_for_test(space(90));
        assert!(retention.refresh());
        retention.set_space_for_test(None);
        assert!(!retention.refresh(), "a failed measurement ends it");
    }

    #[test]
    fn a_measurement_source_is_asked_at_every_measurement() {
        let retention = DiskRetention::new(PathBuf::from("."));
        let available = Arc::new(Mutex::new(90u64));
        let source = Arc::clone(&available);
        retention.set_space_source_for_test(move || space(*source.lock()));

        assert!(retention.refresh());
        *available.lock() = 150;
        assert!(!retention.refresh(), "the second measurement reads 15%");
    }

    #[test]
    fn the_real_partition_can_be_measured() {
        let dir = tempfile::tempdir().expect("tempdir");
        let retention = DiskRetention::new(dir.path().to_path_buf());
        let measured = retention.measure().expect("statvfs on a temp dir");
        assert!(measured.total > 0);
        assert!(measured.available <= measured.total);
    }

    /// `statvfs` fails on a path that does not exist on Unix (Windows answers
    /// for the volume instead).
    #[cfg(unix)]
    #[test]
    fn a_partition_that_cannot_be_measured_is_no_pressure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let gone = DiskRetention::new(dir.path().join("does-not-exist"));
        assert_eq!(gone.measure(), None);
        assert!(!gone.refresh());
    }

    // -- The erase floor -------------------------------------------------------

    #[test]
    fn the_floor_is_ten_times_the_distance_to_the_seventh_nearest_peer() {
        // Peers at distances 1..=9, listed out of order.
        let peers: Vec<XorName> = [9u8, 3, 7, 1, 8, 2, 6, 4, 5]
            .iter()
            .map(|d| at_distance(distance(0, *d)))
            .collect();
        assert_eq!(erase_floor(&NODE, &peers, 7), Some(distance(0, 70)));
        assert_eq!(erase_floor(&NODE, &peers, 2), Some(distance(0, 20)));
    }

    #[test]
    fn the_node_itself_and_duplicates_are_not_counted_as_peers() {
        let mut peers: Vec<XorName> = (1..=6u8).map(|d| distance(0, d)).collect();
        peers.push(NODE);
        peers.push(distance(0, 6));
        assert_eq!(
            erase_floor(&NODE, &peers, 7),
            None,
            "six distinct peers besides the node are fewer than seven"
        );
        peers.push(distance(0, 7));
        assert_eq!(erase_floor(&NODE, &peers, 7), Some(distance(0, 70)));
    }

    #[test]
    fn fewer_than_seven_known_peers_means_nothing_is_eligible() {
        let peers: Vec<XorName> = (1..=6u8).map(|d| distance(0, d)).collect();
        let floor = erase_floor(&NODE, &peers, 7);
        assert_eq!(floor, None);
        let far = [u8::MAX; 32];
        assert!(!is_beyond_floor(&NODE, &far, floor.as_ref()));
        assert_eq!(
            order(true, floor.as_ref(), vec![far], |k| *k),
            Vec::<XorName>::new()
        );
        assert_eq!(erase_floor(&NODE, &peers, 0), None);
    }

    #[test]
    fn ten_times_the_distance_carries_across_bytes() {
        let peers = vec![distance(0x01, 0x9A)];
        // 0x019A * 10 = 0x1004.
        assert_eq!(erase_floor(&NODE, &peers, 1), Some(distance(0x10, 0x04)));
    }

    #[test]
    fn ten_times_the_distance_saturates_and_then_nothing_is_farther() {
        // A seventh-nearest peer a tenth of the key space away or more puts
        // ten times its distance past the end of the key space.
        let mut tenth = [0u8; 32];
        tenth[0] = 0x1A;
        let peers: Vec<XorName> = (0..7u8)
            .map(|i| {
                let mut p = tenth;
                p[31] = i;
                p
            })
            .collect();
        let floor = erase_floor(&NODE, &peers, 7);
        assert_eq!(floor, Some([u8::MAX; 32]));
        assert!(!is_beyond_floor(&NODE, &[u8::MAX; 32], floor.as_ref()));
        assert_eq!(
            order(true, floor.as_ref(), vec![[u8::MAX; 32]], |k| *k),
            Vec::<XorName>::new()
        );

        // Just inside the key space: 0x19.. * 10 still fits.
        let mut fits = [0u8; 32];
        fits[0] = 0x19;
        let floor = erase_floor(&NODE, &[fits], 1).expect("floor");
        assert_eq!(floor[0], 0xFA);
    }

    #[test]
    fn beyond_the_floor_is_strict() {
        let floor = Some(distance(0, 70));
        assert!(!is_beyond_floor(&NODE, &distance(0, 69), floor.as_ref()));
        assert!(
            !is_beyond_floor(&NODE, &distance(0, 70), floor.as_ref()),
            "a record exactly at the floor is kept"
        );
        assert!(is_beyond_floor(&NODE, &distance(0, 71), floor.as_ref()));
        assert!(!is_beyond_floor(&NODE, &distance(0, 71), None));
    }

    #[test]
    fn distance_is_measured_from_the_node_not_from_zero() {
        let node = [0xF0u8; 32];
        let mut near = node;
        near[31] ^= 0x01;
        let far = [0x0Fu8; 32];
        let floor = Some(distance(0, 0xFF));
        assert!(!is_beyond_floor(&node, &near, floor.as_ref()));
        assert!(is_beyond_floor(&node, &far, floor.as_ref()));
        assert!(
            !is_beyond_floor(&node, &[0xF0; 32], floor.as_ref()),
            "the node's own address is at distance zero"
        );
    }

    // -- Order ----------------------------------------------------------------

    #[test]
    fn eligible_records_go_farthest_first() {
        let floor = Some(distance(0, 10));
        let candidates = vec![
            distance(0, 50),
            distance(0, 11),
            distance(3, 0),
            distance(0, 200),
            distance(0, 10),
            distance(0, 2),
        ];
        let ordered = order(true, floor.as_ref(), candidates, |k| *k);
        assert_eq!(
            ordered,
            vec![
                distance(3, 0),
                distance(0, 200),
                distance(0, 50),
                distance(0, 11)
            ],
            "farthest first, and only strictly beyond the floor"
        );
    }

    #[test]
    fn nothing_is_eligible_without_disk_pressure() {
        let floor = Some([0u8; 32]);
        let candidates = vec![[u8::MAX; 32], distance(0, 1)];
        assert_eq!(
            order(false, floor.as_ref(), candidates, |k| *k),
            Vec::<XorName>::new()
        );
    }

    #[test]
    fn records_whose_check_failed_are_tried_after_those_not_tried_yet() {
        let floor = Some(distance(0, 10));
        let failures = FailedChecks::default();
        let candidates = || {
            vec![
                distance(0, 50),
                distance(0, 200),
                distance(3, 0),
                distance(0, 11),
            ]
        };
        let ordered = |candidates: Vec<XorName>| {
            erase_order(true, &NODE, floor.as_ref(), &failures, candidates, |k| *k)
        };

        assert_eq!(
            ordered(candidates()),
            vec![
                distance(3, 0),
                distance(0, 200),
                distance(0, 50),
                distance(0, 11)
            ]
        );

        // The two farthest failed, the farther one first.
        failures.record(&distance(3, 0));
        failures.record(&distance(0, 200));
        assert_eq!(
            ordered(candidates()),
            vec![
                distance(0, 50),
                distance(0, 11),
                distance(3, 0),
                distance(0, 200)
            ],
            "the ones not tried yet first, farthest first, then the one that failed longest ago"
        );

        // A record that is no longer eligible is forgotten.
        assert_eq!(
            ordered(vec![distance(0, 200), distance(0, 50)]),
            vec![distance(0, 50), distance(0, 200)]
        );
        assert_eq!(failures.last_failed(&distance(3, 0)), None);
        assert!(failures.last_failed(&distance(0, 200)).is_some());

        // Erased, or the pressure is over: forgotten too.
        failures.forget(&distance(0, 200));
        assert_eq!(failures.last_failed(&distance(0, 200)), None);
        failures.record(&distance(0, 50));
        assert_eq!(
            erase_order(false, &NODE, floor.as_ref(), &failures, candidates(), |k| {
                *k
            }),
            Vec::<XorName>::new()
        );
        assert_eq!(
            failures.last_failed(&distance(0, 50)),
            None,
            "a spell of pressure that ends forgets every failure"
        );
    }

    // -- The erase loop --------------------------------------------------------

    #[tokio::test]
    async fn erasing_stops_once_fifteen_percent_is_free() {
        let retention = DiskRetention::new(PathBuf::from("."));
        retention.set_space_for_test(space(90));
        let freed_per_erase = 20;
        let available = Mutex::new(90u64);

        let erased = erase_while_under_pressure(&retention, (0..10u32).collect(), |candidate| {
            let mut available = available.lock();
            *available += freed_per_erase;
            retention.set_space_for_test(space(*available));
            async move { candidate }
        })
        .await;

        // 9% -> 11% -> 13% -> 15%: three erasures, then the pressure is over.
        assert_eq!(erased, vec![0, 1, 2]);
        assert!(!retention.under_pressure());
    }

    #[tokio::test]
    async fn nothing_is_erased_without_disk_pressure() {
        let retention = DiskRetention::new(PathBuf::from("."));
        retention.set_space_for_test(space(500));
        let erased =
            erase_while_under_pressure(&retention, vec![1, 2, 3], |c| async move { c }).await;
        assert_eq!(erased, Vec::<i32>::new());

        retention.set_space_for_test(None);
        let erased =
            erase_while_under_pressure(&retention, vec![1, 2, 3], |c| async move { c }).await;
        assert_eq!(
            erased,
            Vec::<i32>::new(),
            "nor when free space cannot be measured"
        );
    }

    #[tokio::test]
    async fn a_failed_measurement_mid_way_stops_erasing() {
        let retention = DiskRetention::new(PathBuf::from("."));
        retention.set_space_for_test(space(50));
        let erased = erase_while_under_pressure(&retention, vec![1, 2, 3], |c| {
            retention.set_space_for_test(None);
            async move { c }
        })
        .await;
        assert_eq!(erased, vec![1]);
    }

    // -- The falling floor -------------------------------------------------------

    use crate::storage::{DEFAULT_DISK_RESERVE, GIB};

    /// Bytes in a decimal gigabyte, as partitions are sold.
    const GB: u64 = 1_000_000_000;

    /// The partitions the small-disk tests simulate, the operator guide's
    /// 20 GB minimum among them.
    const PARTITIONS: [u64; 4] = [15 * GB, 20 * GB, 40 * GB, 80 * GB];

    fn at(available: u64, total: u64) -> DiskSpace {
        DiskSpace::bytes(available, total)
    }

    #[test]
    fn the_steps_are_the_falling_floors() {
        let shares: Vec<u64> = FLOOR_STEPS.iter().map(|s| s.below_per_mille).collect();
        assert_eq!(shares, vec![100, 75, 50, 25]);
        let reaches: Vec<Reach> = FLOOR_STEPS.iter().map(|s| s.reach).collect();
        assert_eq!(
            reaches,
            vec![
                Reach::Multiple(10),
                Reach::Multiple(5),
                Reach::Multiple(2),
                Reach::Edge
            ]
        );
        assert_eq!(STEP_CLAMP_ABOVE_LINE, 4 * MIB);
        assert_eq!(LEAVE_FULL_MARGIN, 256 * MIB);
    }

    #[test]
    fn on_a_large_partition_each_step_fires_at_its_own_share() {
        let total = 10_000 * GB;
        let line = DEFAULT_DISK_RESERVE;
        assert_eq!(deepest_step(at(total / 10, total), line), None);
        assert_eq!(deepest_step(at(total / 10 - 1, total), line), Some(0));
        assert_eq!(
            deepest_step(at(total * 75 / 1000 - 1, total), line),
            Some(1)
        );
        assert_eq!(deepest_step(at(total / 20 - 1, total), line), Some(2));
        assert_eq!(deepest_step(at(total / 40 - 1, total), line), Some(3));
        assert_eq!(
            deepest_step(at(total / 40, total), line),
            Some(2),
            "exactly 2.5% free is not below it"
        );
    }

    /// Each step fires at the larger of its share and the line plus one
    /// chunk, so on the small partitions the steps collapse, in order, onto
    /// the line, and the last one runs before the node is at the line.
    #[test]
    fn on_small_partitions_the_steps_collapse_onto_the_line_in_order() {
        let line = DEFAULT_DISK_RESERVE;
        let clamp = line + 4 * MIB;
        assert_eq!(step_clamp(line), clamp);
        for total in PARTITIONS {
            // Walk free space down from 12% to nothing, a MiB at a time near the
            // line: the step never goes back, and every step is reached before
            // the line is.
            let mut last = None;
            let mut first_at: [Option<u64>; 4] = [None; 4];
            let mut available = total * 12 / 100;
            loop {
                let space = at(available, total);
                let step = deepest_step(space, line);
                assert!(step >= last, "{total}: the floor rose at {available}");
                if let Some(step) = step {
                    for reached in first_at.iter_mut().take(step + 1) {
                        reached.get_or_insert(available);
                    }
                }
                last = step;
                if available == 0 {
                    break;
                }
                available = available.saturating_sub(if available > clamp + GIB {
                    64 * MIB
                } else {
                    MIB
                });
            }
            let first_at = first_at.map(|level| level.expect("every step is reached"));
            // Each step's level is the larger of its share and the clamp.
            for (index, step) in FLOOR_STEPS.iter().enumerate() {
                let share =
                    u64::try_from(u128::from(total) * u128::from(step.below_per_mille) / 1000)
                        .unwrap();
                let expected = share.max(clamp);
                assert!(
                    first_at[index] <= expected && first_at[index] + 64 * MIB >= expected,
                    "{total}: step {index} first fired at {}, its level is {expected}",
                    first_at[index]
                );
            }
            // The last step fires no later than the clamp, which is above the line.
            assert!(first_at[LAST_FLOOR_STEP] >= clamp - MIB);
            assert_eq!(deepest_step(at(clamp, total), line), Some(LAST_FLOOR_STEP));
            assert_eq!(
                at_the_line(at(clamp, total), line),
                Some(FullTrigger::Bytes)
            );
            assert_eq!(at_the_line(at(clamp + 1, total), line), None);
        }
    }

    /// With the 1 GiB line the clamp decides the 2.5% step below about
    /// 40 GiB, and the 5% step below about 20 GiB.
    #[test]
    fn the_clamp_decides_the_last_steps_on_partitions_under_40_gib() {
        let line = DEFAULT_DISK_RESERVE;
        let clamp = step_clamp(line);
        let clamped = |total: u64, index: usize| {
            let share = u64::try_from(
                u128::from(total) * u128::from(FLOOR_STEPS[index].below_per_mille) / 1000,
            )
            .unwrap();
            share < clamp
        };
        assert!(clamped(15 * GB, LAST_FLOOR_STEP));
        assert!(clamped(20 * GB, LAST_FLOOR_STEP));
        assert!(clamped(40 * GB, LAST_FLOOR_STEP));
        assert!(!clamped(80 * GB, LAST_FLOOR_STEP));
        assert!(clamped(15 * GB, 2));
        assert!(clamped(20 * GB, 2));
        assert!(!clamped(40 * GB, 2));
        assert!(!clamped(15 * GB, 0));
    }

    #[test]
    fn a_store_with_no_line_has_no_clamp() {
        assert_eq!(step_clamp(0), 0);
        assert_eq!(deepest_step(at(1, 1000), 0), Some(LAST_FLOOR_STEP));
        assert_eq!(deepest_step(at(100, 1000), 0), None);
        assert_eq!(at_the_line(at(1, 1000), 0), None);
        assert_eq!(at_the_line(at(0, 1000), 0), Some(FullTrigger::Bytes));
    }

    #[test]
    fn pressure_started_by_the_clamp_ends_a_margin_above_it() {
        let line = DEFAULT_DISK_RESERVE;
        let clamp = step_clamp(line);
        // 5 GB: 10% (500 MB) is below the clamp, so the clamp starts it.
        let total = 5 * GB;
        assert!(!next_disk_pressure_at(
            false,
            Some(at(clamp + 1, total)),
            line
        ));
        assert!(next_disk_pressure_at(false, Some(at(clamp, total)), line));
        assert!(next_disk_pressure_at(
            true,
            Some(at(clamp + LEAVE_FULL_MARGIN - 1, total)),
            line
        ));
        assert!(!next_disk_pressure_at(
            true,
            Some(at(clamp + LEAVE_FULL_MARGIN, total)),
            line
        ));
        // On a large partition 15% is the higher of the two.
        let total = 1000 * GB;
        assert!(next_disk_pressure_at(
            true,
            Some(at(total * 15 / 100 - 1, total)),
            line
        ));
        assert!(!next_disk_pressure_at(
            true,
            Some(at(total * 15 / 100, total)),
            line
        ));
    }

    #[test]
    fn the_floor_follows_the_step() {
        let peers: Vec<XorName> = (1..=9u8).map(|d| distance(0, d)).collect();
        assert_eq!(
            erase_floor_at(&NODE, &peers, 7, Reach::Multiple(10)),
            Some(distance(0, 70))
        );
        assert_eq!(
            erase_floor_at(&NODE, &peers, 7, Reach::Multiple(5)),
            Some(distance(0, 35))
        );
        assert_eq!(
            erase_floor_at(&NODE, &peers, 7, Reach::Multiple(2)),
            Some(distance(0, 14))
        );
        assert_eq!(
            erase_floor_at(&NODE, &peers, 7, Reach::Edge),
            Some([0u8; 32]),
            "at the edge every key but the node's own is beyond the floor"
        );
        assert_eq!(
            erase_floor_at(&NODE, &peers[..6], 7, Reach::Edge),
            None,
            "with fewer than seven peers known nothing may be erased, at any step"
        );
        assert!(is_beyond_floor(&NODE, &distance(0, 1), Some(&[0u8; 32])));
        assert!(!is_beyond_floor(&NODE, &NODE, Some(&[0u8; 32])));
    }

    #[test]
    fn the_shared_state_reports_its_step_and_keeps_the_penalty_at_ten_times() {
        let line = DEFAULT_DISK_RESERVE;
        let total = 1000 * GB;
        let retention = DiskRetention::new(PathBuf::from(".")).with_line(line);
        let peers: Vec<XorName> = (1..=9u8).map(|d| distance(0, d)).collect();
        let floors = |retention: &DiskRetention| retention.floors_from_peers(&NODE, &peers, 7);

        retention.set_space_for_test(Some(at(total / 5, total)));
        assert!(!retention.refresh());
        assert_eq!(retention.step(), None);
        assert_eq!(floors(&retention).erase, None, "no floor without pressure");
        assert_eq!(floors(&retention).penalty, Some(distance(0, 70)));

        for (available, step, erase) in [
            (total * 9 / 100, 0, Some(distance(0, 70))),
            (total * 7 / 100, 1, Some(distance(0, 35))),
            (total * 4 / 100, 2, Some(distance(0, 14))),
            (total * 2 / 100, 3, Some([0u8; 32])),
        ] {
            retention.set_space_for_test(Some(at(available, total)));
            assert!(retention.refresh());
            assert_eq!(retention.step(), Some(step), "{available}");
            assert_eq!(floors(&retention).erase, erase, "{available}");
            assert_eq!(
                floors(&retention).penalty,
                Some(distance(0, 70)),
                "a failed proof is penalised within ten times the distance, at every step"
            );
        }
        // Back up between 10% and 15%: still pressure, at the first step.
        retention.set_space_for_test(Some(at(total * 12 / 100, total)));
        assert!(retention.refresh());
        assert_eq!(retention.step(), Some(0));
    }

    // -- Inodes ------------------------------------------------------------------

    fn with_inodes(available: u64, total: u64, free: u64, inodes: u64) -> DiskSpace {
        DiskSpace {
            available,
            total,
            inodes: Some(Inodes {
                free,
                total: inodes,
            }),
        }
    }

    #[test]
    fn free_space_is_the_smaller_of_the_byte_and_inode_shares() {
        let total = 1000 * GB;
        let line = DEFAULT_DISK_RESERVE;
        // Bytes plentiful, inodes at 6%: the third step.
        let space = with_inodes(total / 2, total, 60, 1000);
        assert_eq!(deepest_step(space, line), Some(1));
        let space = with_inodes(total / 2, total, 40, 1000);
        assert_eq!(deepest_step(space, line), Some(2));
        let space = with_inodes(total / 2, total, 24, 1000);
        assert_eq!(deepest_step(space, line), Some(LAST_FLOOR_STEP));
        assert!(next_disk_pressure_at(false, Some(space), line));
    }

    #[test]
    fn inodes_below_the_last_steps_share_are_at_the_line() {
        let total = 1000 * GB;
        let line = DEFAULT_DISK_RESERVE;
        assert_eq!(
            at_the_line(with_inodes(total / 2, total, 24, 1000), line),
            Some(FullTrigger::Inodes)
        );
        assert_eq!(
            at_the_line(with_inodes(total / 2, total, 25, 1000), line),
            None
        );
        // Bytes first when both are short.
        assert_eq!(
            at_the_line(with_inodes(line, total, 0, 1000), line),
            Some(FullTrigger::Bytes)
        );
    }

    #[test]
    fn a_filesystem_that_counts_no_inodes_is_never_at_the_line_by_inodes() {
        let total = 1000 * GB;
        let line = DEFAULT_DISK_RESERVE;
        let no_count = DiskSpace::bytes(total / 2, total);
        assert_eq!(at_the_line(no_count, line), None);
        assert_eq!(deepest_step(no_count, line), None);
        assert!(may_leave_full(no_count, line, FullTrigger::Bytes));
        assert!(may_leave_full(no_count, line, FullTrigger::Inodes));
        assert!(!crate::storage::Inodes { free: 0, total: 0 }.at_the_line());
    }

    #[test]
    fn leaving_full_needs_the_margin_above_the_line() {
        let total = 20 * GB;
        let line = DEFAULT_DISK_RESERVE;
        for trigger in [FullTrigger::Bytes, FullTrigger::Inodes] {
            assert!(!may_leave_full(at(line, total), line, trigger));
            assert!(!may_leave_full(
                at(step_clamp(line) + 1, total),
                line,
                trigger
            ));
            assert!(!may_leave_full(
                at(line + LEAVE_FULL_MARGIN - 1, total),
                line,
                trigger
            ));
            assert!(may_leave_full(
                at(line + LEAVE_FULL_MARGIN, total),
                line,
                trigger
            ));
        }
        // Inodes that put the node in "full" need their own margin.
        let inodes = 10_000_000;
        let share = inodes * 25 / 1000;
        let roomy = line + 10 * GIB;
        assert_eq!(leave_full_inode_margin(inodes), LEAVE_FULL_INODE_MARGIN);
        assert!(!may_leave_full(
            with_inodes(roomy, total, share + LEAVE_FULL_INODE_MARGIN - 1, inodes),
            line,
            FullTrigger::Inodes
        ));
        assert!(may_leave_full(
            with_inodes(roomy, total, share + LEAVE_FULL_INODE_MARGIN, inodes),
            line,
            FullTrigger::Inodes
        ));
    }

    /// Bytes put the node in "full": inodes need only be off the line, with no
    /// margin, however few the filesystem has.
    #[test]
    fn a_node_full_by_bytes_leaves_on_a_filesystem_with_few_inodes() {
        let total = 20 * GB;
        let line = DEFAULT_DISK_RESERVE;
        let roomy = line + LEAVE_FULL_MARGIN;
        // ext4 made with -T largefile4 on a 20 GB partition: about 5,000 inodes.
        let inodes = 5_000;
        let share = inodes * 25 / 1000;
        assert!(may_leave_full(
            with_inodes(roomy, total, inodes / 2, inodes),
            line,
            FullTrigger::Bytes
        ));
        assert!(may_leave_full(
            with_inodes(roomy, total, share, inodes),
            line,
            FullTrigger::Bytes
        ));
        // Not straight back to the line by inodes.
        assert!(!may_leave_full(
            with_inodes(roomy, total, share - 1, inodes),
            line,
            FullTrigger::Bytes
        ));
        // A larger filesystem whose free inodes sit between the share and the
        // share plus the fixed margin: inodes never put it at the line.
        let more = 10_000_000;
        assert!(may_leave_full(
            with_inodes(roomy, total, more * 25 / 1000 + 1, more),
            line,
            FullTrigger::Bytes
        ));
    }

    /// Inodes put a node on a tiny filesystem in "full": it leaves once free
    /// inodes reach the share plus a fortieth of the inodes.
    #[test]
    fn a_node_full_by_inodes_on_a_tiny_filesystem_can_leave() {
        let total = 20 * GB;
        let line = DEFAULT_DISK_RESERVE;
        let roomy = line + LEAVE_FULL_MARGIN;
        let inodes = 20_000;
        let share = inodes * 25 / 1000;
        let margin = leave_full_inode_margin(inodes);
        assert_eq!(margin, inodes / 40);
        assert!(!may_leave_full(
            with_inodes(roomy, total, share + margin - 1, inodes),
            line,
            FullTrigger::Inodes
        ));
        assert!(may_leave_full(
            with_inodes(roomy, total, share + margin, inodes),
            line,
            FullTrigger::Inodes
        ));
    }
}

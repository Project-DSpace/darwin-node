//! What a node keeps, and when it may erase.
//!
//! A node keeps every record and pointer it holds while its disk has room,
//! including those it is no longer responsible for: they stay stored, are
//! served, and are offered to their close group by neighbour sync. The prune
//! passes ([`super::pruning`] for chunks, [`super::pointer`] for pointers)
//! erase only under disk pressure, and then only records far from the node:
//!
//! - **Disk pressure** starts when free space on the partition holding the
//!   record store falls below [`DISK_PRESSURE_START_PERCENT`] (10%) of that
//!   partition's size, and lasts until free space is back to at least
//!   [`DISK_PRESSURE_END_PERCENT`] (15%). Free space is measured as the chunk
//!   store's capacity guard measures it: the space available on the partition
//!   holding the chunk directory. A measurement that fails counts as no
//!   pressure. The state lives in memory, is shared by both passes and the
//!   storage migration's shed, and starts without pressure when the node
//!   starts.
//! - **The erase floor.** Only records farther from the node than
//!   [`ERASE_FLOOR_MULTIPLE`] (10) times the XOR distance from the node to its
//!   own `close_group_size`-th nearest peer (the 7th at production parameters,
//!   from the local routing table, the node itself not counted) may be erased.
//!   With fewer peers known, nothing may. "Farther" is strict, and the multiple
//!   is computed without overflow: past the end of the key space nothing is
//!   farther.
//! - **Order.** Eligible records go farthest from the node first, chunks and
//!   pointers each in their own pass. A record whose possession check failed
//!   earlier in the same spell of pressure is tried after those not tried yet,
//!   so the per-pass budgets reach every eligible record in turn. Each is
//!   erased only after the nodes now nearest to it prove they hold it and the
//!   pass's re-check just before deleting passes. Free space is measured again
//!   before each record, and erasing stops once it is back to 15%.
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

use crate::ant_protocol::XorName;
use crate::client::xor_distance;
use crate::logging::{debug, info, warn};
use crate::storage::chunk_store::CHUNKS_DIR_NAME;
use crate::storage::ChunkStore;

#[cfg(test)]
mod census;

/// Disk pressure starts when free space falls below this share of the
/// partition holding the record store, in percent.
pub const DISK_PRESSURE_START_PERCENT: u64 = 10;

/// Disk pressure ends once free space is back to at least this share of the
/// partition, in percent.
pub const DISK_PRESSURE_END_PERCENT: u64 = 15;

/// Under disk pressure, records farther from the node than this many times
/// the distance to its `close_group_size`-th nearest peer may be erased.
pub const ERASE_FLOOR_MULTIPLE: u8 = 10;

/// Free and total bytes of the partition holding the record store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskSpace {
    /// Bytes available to the node on the partition.
    pub available: u64,
    /// The partition's size in bytes.
    pub total: u64,
}

impl DiskSpace {
    /// Whether free space is below `percent` of the partition.
    fn below_percent(self, percent: u64) -> bool {
        u128::from(self.available) * 100 < u128::from(self.total) * u128::from(percent)
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

/// Whether the node is under disk pressure after measuring `space`, given
/// whether it was before the measurement.
///
/// Pressure starts below [`DISK_PRESSURE_START_PERCENT`] free, holds while
/// free space stays below [`DISK_PRESSURE_END_PERCENT`], and ends at that or
/// more. A failed measurement (`None`), or a partition reported as having no
/// size, is no pressure.
#[must_use]
pub fn next_disk_pressure(was_under_pressure: bool, space: Option<DiskSpace>) -> bool {
    let Some(space) = space.filter(|space| space.total > 0) else {
        return false;
    };
    if was_under_pressure {
        space.below_percent(DISK_PRESSURE_END_PERCENT)
    } else {
        space.below_percent(DISK_PRESSURE_START_PERCENT)
    }
}

/// The erase floor of a node whose routing table holds `peers`:
/// [`ERASE_FLOOR_MULTIPLE`] times the XOR distance from the node to its
/// `close_group_size`-th nearest peer.
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
    let rank = close_group_size.checked_sub(1)?;
    let mut distances: Vec<XorName> = peers
        .iter()
        .filter(|peer| *peer != self_id)
        .map(|peer| xor_distance(self_id, peer))
        .collect();
    distances.sort_unstable();
    distances.dedup();
    distances
        .get(rank)
        .map(|distance| saturating_multiply(distance, ERASE_FLOOR_MULTIPLE))
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

/// The records of one kind whose possession check failed during the current
/// spell of disk pressure, and when.
///
/// Process-local, like the out-of-range clocks. [`erase_order`] keeps only the
/// records that are still eligible, so it forgets them all once the pressure
/// is over.
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
/// soon as it is back to [`DISK_PRESSURE_END_PERCENT`] or can no longer be
/// measured. The remaining candidates are left for a later pass.
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

/// The disk-pressure state of one node, and the measurements behind it.
///
/// Shared by the chunk and pointer prune passes and the storage migration, so
/// they never disagree about whether the node is erasing. It also holds the
/// records whose possession check failed during the current spell of
/// pressure.
pub struct DiskRetention {
    /// A directory on the partition holding the record store: the chunk
    /// store's own, which its capacity guard measures.
    dir: PathBuf,
    /// Whether the node is under disk pressure.
    under_pressure: Mutex<bool>,
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
    /// measured on the partition holding the store's chunk directory.
    #[must_use]
    pub fn for_store(storage: &ChunkStore) -> Self {
        Self::new(storage.root_dir().join(CHUNKS_DIR_NAME))
    }

    /// The state for a record store on the partition holding `dir`. Starts
    /// without disk pressure.
    #[must_use]
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            under_pressure: Mutex::new(false),
            failed_chunk_checks: FailedChecks::default(),
            failed_pointer_checks: FailedChecks::default(),
            #[cfg(any(test, feature = "test-utils"))]
            fixed: Mutex::new(FixedInputs::default()),
        }
    }

    /// Whether the node was under disk pressure at the last measurement.
    #[must_use]
    pub fn under_pressure(&self) -> bool {
        *self.under_pressure.lock()
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

    /// Measure free space again and update the pressure state. Returns
    /// whether the node is now under disk pressure.
    pub fn refresh(&self) -> bool {
        let space = self.measure();
        let mut under_pressure = self.under_pressure.lock();
        let was = *under_pressure;
        let now = next_disk_pressure(was, space);
        *under_pressure = now;
        drop(under_pressure);
        match (was, now, space.filter(|space| space.total > 0)) {
            (false, true, Some(space)) => info!(
                "Disk pressure: {:.1}% of the record store's partition is free, below \
                 {DISK_PRESSURE_START_PERCENT}%; records far from this node may now be erased, \
                 farthest first, until {DISK_PRESSURE_END_PERCENT}% is free",
                space.free_percent()
            ),
            (true, false, Some(space)) => info!(
                "Disk pressure over: {:.1}% of the record store's partition is free; \
                 keeping every record again",
                space.free_percent()
            ),
            (true, false, None) => warn!(
                "Free space on the record store's partition could not be measured; \
                 erasing stops until it can"
            ),
            _ => {}
        }
        now
    }

    /// Free and total bytes of the partition, or `None` if they could not be
    /// read.
    fn measure(&self) -> Option<DiskSpace> {
        #[cfg(any(test, feature = "test-utils"))]
        {
            let source = self.fixed.lock().space.clone();
            if let Some(source) = source {
                return source();
            }
        }
        match fs2::statvfs(&self.dir) {
            Ok(stats) => Some(DiskSpace {
                available: stats.available_space(),
                total: stats.total_space(),
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

    /// The erase floor from the node's local routing table (see
    /// [`erase_floor`]). `None`: nothing may be erased.
    pub async fn floor_from_routing_table(
        &self,
        p2p_node: &P2PNode,
        self_id: &PeerId,
        close_group_size: usize,
    ) -> Option<XorName> {
        #[cfg(any(test, feature = "test-utils"))]
        {
            let fixed = self.fixed.lock().floor;
            if let Fixed::To(floor) = fixed {
                return floor;
            }
        }
        let peers: Vec<XorName> = p2p_node
            .dht_manager()
            .find_closest_nodes_local(self_id.as_bytes(), close_group_size)
            .await
            .into_iter()
            .map(|node| *node.peer_id.as_bytes())
            .collect();
        erase_floor(self_id.as_bytes(), &peers, close_group_size)
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

    /// From now on, use `floor` instead of the routing table's (`None`:
    /// nothing may be erased; all zeroes: every other key is beyond it).
    /// Tests only.
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
        Some(DiskSpace {
            available,
            total: 1000,
        })
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
        let no_size = Some(DiskSpace {
            available: 0,
            total: 0,
        });
        assert!(!next_disk_pressure(false, no_size));
        assert!(!next_disk_pressure(true, no_size));
    }

    #[test]
    fn pressure_arithmetic_does_not_overflow_on_huge_partitions() {
        let huge = Some(DiskSpace {
            available: u64::MAX / 20,
            total: u64::MAX,
        });
        assert!(next_disk_pressure(false, huge), "5% free starts pressure");
        let roomy = Some(DiskSpace {
            available: u64::MAX / 4,
            total: u64::MAX,
        });
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
}

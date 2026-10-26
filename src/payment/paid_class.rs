//! The size class a payment covers, as a payment notice records it in the
//! paid list.
//!
//! A notice names a key and carries its payment proof, but no bytes, so it is
//! admitted at the smallest class's amount. The class its settled amount
//! covers is kept as the paid-list entry's value, one byte, so a fetch that
//! this entry alone authorises can be checked against the length of the bytes
//! that arrive. A node answers a paid-list query with that byte too, and a
//! node that learns a key from a paid-list quorum records the class the
//! quorum's entries record.

use ant_protocol::chunk::DATA_TYPE_CHUNK;
use ant_protocol::payment::pricing::{
    record_weight, size_class_top, weighted_amount, RecordWeight, FULL_RECORD_BYTES,
    SMALLEST_CLASS_BYTES,
};
use evmlib::common::Amount;

/// A chunk size class, named by the top of its sizes and held as that top's
/// base-2 logarithm: 14 for the 16 KiB class up to 22 for the 4 MiB class.
/// Classes order by size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PaidClass(u8);

impl PaidClass {
    /// The smallest class, whose top is
    /// [`SMALLEST_CLASS_BYTES`](ant_protocol::payment::pricing::SMALLEST_CLASS_BYTES).
    pub const SMALLEST: Self = Self(log2(SMALLEST_CLASS_BYTES));

    /// The full class, a full chunk's, whose top is
    /// [`FULL_RECORD_BYTES`](ant_protocol::payment::pricing::FULL_RECORD_BYTES).
    pub const FULL: Self = Self(log2(FULL_RECORD_BYTES));

    /// The class of a chunk of `len` bytes.
    #[must_use]
    pub fn of_len(len: usize) -> Self {
        Self(log2(size_class_top(u64::try_from(len).unwrap_or(u64::MAX))))
    }

    /// The top of the class's sizes, in bytes.
    #[must_use]
    pub const fn top(self) -> u64 {
        1u64 << self.0
    }

    /// The weight a payment for this class covers.
    #[must_use]
    pub fn weight(self) -> RecordWeight {
        record_weight(
            DATA_TYPE_CHUNK,
            usize::try_from(self.top()).unwrap_or(usize::MAX),
        )
    }

    /// The smallest class whose weight is at least `weight`, or the full
    /// class if none is.
    #[must_use]
    pub fn of_weight(weight: RecordWeight) -> Self {
        Self::all()
            .find(|class| class.weight() >= weight)
            .unwrap_or(Self::FULL)
    }

    /// The largest class `amount` pays for when the paid quote's price is
    /// `median_price`: the largest whose weighted amount is at most `amount`.
    /// `None` if it pays for none.
    #[must_use]
    pub fn covered_by(median_price: Amount, amount: Amount) -> Option<Self> {
        Self::all().rev().find(|class| {
            weighted_amount(median_price, class.weight()).is_some_and(|needed| needed <= amount)
        })
    }

    /// Whether a payment for this class pays for a chunk of `len` bytes.
    #[must_use]
    pub fn covers(self, len: usize) -> bool {
        self >= Self::of_len(len)
    }

    /// The class as a paid-list value: one byte.
    #[must_use]
    pub const fn to_value(self) -> [u8; 1] {
        [self.0]
    }

    /// The class a paid-list value records. An empty value reads as the full
    /// class: every key written before notices recorded a class, every key
    /// admitted with its bytes or learned by a presence quorum, and every key
    /// a paid-list quorum reports at the full class, holds one. A value that
    /// isn't one byte naming a class reads as the full class too.
    #[must_use]
    pub fn from_value(value: &[u8]) -> Self {
        match value {
            [byte] if (Self::SMALLEST.0..=Self::FULL.0).contains(byte) => Self(*byte),
            _ => Self::FULL,
        }
    }

    /// Every class, smallest first.
    fn all() -> impl DoubleEndedIterator<Item = Self> {
        (Self::SMALLEST.0..=Self::FULL.0).map(Self)
    }
}

/// The base-2 logarithm of a class top, a power of two.
#[allow(clippy::cast_possible_truncation)] // at most 63, so it fits a byte
const fn log2(top: u64) -> u8 {
    top.trailing_zeros() as u8
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const KIB: usize = 1024;

    #[test]
    fn the_classes_run_from_16_kib_to_4_mib_as_one_byte_each() {
        assert_eq!(PaidClass::SMALLEST.top(), 16 * 1024);
        assert_eq!(PaidClass::FULL.top(), 4 * 1024 * 1024);
        assert_eq!(PaidClass::SMALLEST.to_value(), [14]);
        assert_eq!(PaidClass::FULL.to_value(), [22]);
        assert_eq!(PaidClass::all().count(), 9);
    }

    #[test]
    fn a_length_is_in_the_class_of_its_size_class_top() {
        assert_eq!(PaidClass::of_len(0), PaidClass::SMALLEST);
        assert_eq!(PaidClass::of_len(16 * KIB), PaidClass::SMALLEST);
        assert_eq!(PaidClass::of_len(16 * KIB + 1).top(), 32 * 1024);
        assert_eq!(PaidClass::of_len(4 * KIB * KIB), PaidClass::FULL);
        assert_eq!(PaidClass::of_len(usize::MAX), PaidClass::FULL);
    }

    #[test]
    fn a_class_covers_its_own_lengths_and_smaller_ones_only() {
        let class = PaidClass::of_len(100 * KIB);
        assert_eq!(class.top(), 128 * 1024);
        assert!(class.covers(0));
        assert!(class.covers(128 * KIB));
        assert!(!class.covers(128 * KIB + 1));
        assert!(PaidClass::FULL.covers(4 * KIB * KIB));
    }

    #[test]
    fn an_empty_or_unknown_value_reads_as_the_full_class() {
        assert_eq!(PaidClass::from_value(&[]), PaidClass::FULL);
        assert_eq!(PaidClass::from_value(&[13]), PaidClass::FULL);
        assert_eq!(PaidClass::from_value(&[23]), PaidClass::FULL);
        assert_eq!(PaidClass::from_value(&[14, 14]), PaidClass::FULL);
        for class in PaidClass::all() {
            assert_eq!(PaidClass::from_value(&class.to_value()), class);
        }
    }

    #[test]
    fn an_amount_covers_the_largest_class_it_pays_for() {
        let median = Amount::from(1_000_000_007u64);
        for class in PaidClass::all() {
            let exact = weighted_amount(median, class.weight()).unwrap();
            assert_eq!(PaidClass::covered_by(median, exact), Some(class));
            let short = exact - Amount::from(1u64);
            let below = PaidClass::all().rev().find(|smaller| *smaller < class);
            assert_eq!(PaidClass::covered_by(median, short), below);
        }
        let full = median * Amount::from(3u64);
        assert_eq!(PaidClass::covered_by(median, full), Some(PaidClass::FULL));
        assert_eq!(
            PaidClass::covered_by(median, full * Amount::from(2u64)),
            Some(PaidClass::FULL)
        );
    }

    #[test]
    fn a_weight_is_in_the_smallest_class_that_weighs_as_much() {
        let notice = record_weight(DATA_TYPE_CHUNK, 0);
        assert_eq!(PaidClass::of_weight(notice), PaidClass::SMALLEST);
        assert_eq!(PaidClass::of_weight(RecordWeight::FULL), PaidClass::FULL);
        for class in PaidClass::all() {
            assert_eq!(PaidClass::of_weight(class.weight()), class);
        }
    }
}

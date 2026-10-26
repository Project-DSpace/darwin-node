//! Pricing by record size: the functions in the vendored ant-protocol
//! (`vendor/ant-protocol/src/payment/pricing.rs`) that weigh a record by its
//! size class, the amount a client pays for it, and a node's weight total.
//! Every store check requires `weighted_amount` for the class of the bytes it
//! holds (`src/payment/verifier.rs`, `Held`); nodes and clients must compute
//! the same values, so these tests fail if a function or one of its
//! parameters changes.
//!
//! A record's weight is `φ + (1 − φ) × (class top ÷ 4 MiB)`, with φ = 1/4 and
//! nine size classes whose tops are the powers of two from 16 KiB to 4 MiB.
//! The pointer's 5,303 bytes are pinned in `tests/pointer_convergence.rs` too.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use ant_protocol::chunk::{DATA_TYPE_CHUNK, MAX_CHUNK_SIZE};
use ant_protocol::evm::Amount;
use ant_protocol::payment::calculate_price;
use ant_protocol::payment::pricing::{
    record_weight, size_class_top, weight_total, weighted_amount, RecordWeight,
    FIXED_SHARE_DENOMINATOR, FIXED_SHARE_NUMERATOR, FULL_RECORD_BYTES, SMALLEST_CLASS_BYTES,
    WEIGHT_DENOMINATOR,
};
use ant_protocol::pointer::{DATA_TYPE_POINTER, POINTER_WIRE_LEN};

const KIB: usize = 1024;

/// The tops of the nine size classes, smallest first.
const CLASS_TOPS: [usize; 9] = [
    16 * KIB,
    32 * KIB,
    64 * KIB,
    128 * KIB,
    256 * KIB,
    512 * KIB,
    1024 * KIB,
    2048 * KIB,
    4096 * KIB,
];

/// Each class's weight, in 1/1,024 of a full chunk, at φ = 1/4.
const CLASS_WEIGHTS_IN_1024THS: [u64; 9] = [259, 262, 268, 280, 304, 352, 448, 640, 1024];

/// `weight` in 1/1,024 of a full chunk, which every class weight is exactly.
fn in_1024ths(weight: RecordWeight) -> u64 {
    let scaled = weight.numerator() * 1024;
    assert_eq!(
        scaled % WEIGHT_DENOMINATOR,
        0,
        "{weight:?} is not a whole number of 1/1,024"
    );
    scaled / WEIGHT_DENOMINATOR
}

fn chunk(len: usize) -> RecordWeight {
    record_weight(DATA_TYPE_CHUNK, len)
}

/// 3 × `median` × `numerator / WEIGHT_DENOMINATOR`, rounded up, computed here
/// apart from the function under test.
fn three_times_rounded_up(median: Amount, numerator: u64) -> Amount {
    let exact = median * Amount::from(3u64) * Amount::from(numerator);
    let denominator = Amount::from(WEIGHT_DENOMINATOR);
    let whole = exact / denominator;
    if whole * denominator == exact {
        whole
    } else {
        whole + Amount::from(1u64)
    }
}

#[test]
fn the_fixed_share_is_a_quarter_and_a_weight_is_measured_against_a_full_chunk() {
    assert_eq!((FIXED_SHARE_NUMERATOR, FIXED_SHARE_DENOMINATOR), (1, 4));
    assert_eq!(FULL_RECORD_BYTES, MAX_CHUNK_SIZE as u64);
    assert_eq!(SMALLEST_CLASS_BYTES, 16 * 1024);
    assert_eq!(WEIGHT_DENOMINATOR, 4 * FULL_RECORD_BYTES);
}

#[test]
fn the_nine_classes_weigh_259_to_1024_thousand_twenty_fourths() {
    for (top, expected) in CLASS_TOPS.iter().zip(CLASS_WEIGHTS_IN_1024THS) {
        assert_eq!(in_1024ths(chunk(*top)), expected, "class top {top}");
    }
}

#[test]
fn a_record_pays_its_class_top_from_one_byte_past_the_class_below() {
    let mut bottom = 0;
    for top in CLASS_TOPS {
        assert_eq!(size_class_top(top as u64), top as u64);
        for len in [bottom, bottom + 1, top - 1, top] {
            assert_eq!(
                chunk(len),
                chunk(top),
                "{len} bytes is in the class of {top}"
            );
        }
        bottom = top + 1;
    }
    // Just past a class top is the next class: the weight steps up there.
    for pair in CLASS_TOPS.windows(2) {
        assert!(chunk(pair[0] + 1) > chunk(pair[0]), "past {}", pair[0]);
        assert_eq!(chunk(pair[0] + 1), chunk(pair[1]));
    }
    // An empty record is in the smallest class; nothing weighs above a full chunk.
    assert_eq!(in_1024ths(chunk(0)), 259);
    assert_eq!(chunk(MAX_CHUNK_SIZE + 1), RecordWeight::FULL);
    assert_eq!(chunk(usize::MAX), RecordWeight::FULL);
}

#[test]
fn a_full_chunk_weighs_exactly_one_and_pays_three_times_the_median() {
    assert_eq!(chunk(MAX_CHUNK_SIZE), RecordWeight::FULL);
    assert_eq!(RecordWeight::FULL.numerator(), WEIGHT_DENOMINATOR);
    for records in [0, 1, 6_000, 60_000] {
        let median = calculate_price(records);
        assert_eq!(
            weighted_amount(median, RecordWeight::FULL),
            Some(median * Amount::from(3u64)),
            "at {records} records"
        );
    }
}

#[test]
fn the_weighted_amount_is_three_times_the_median_times_the_weight_rounded_up() {
    for records in [0, 1, 777, 6_000, 60_000] {
        let median = calculate_price(records);
        for top in CLASS_TOPS {
            let weight = chunk(top);
            let amount = weighted_amount(median, weight).unwrap();
            assert_eq!(amount, three_times_rounded_up(median, weight.numerator()));
            // Never below the exact amount, and at most one wei above it.
            let exact = median * Amount::from(3u64) * Amount::from(weight.numerator());
            let denominator = Amount::from(WEIGHT_DENOMINATOR);
            assert!(amount * denominator >= exact);
            assert!((amount - Amount::from(1u64)) * denominator < exact);
            // A smaller class never pays more than a bigger one.
            assert!(amount <= median * Amount::from(3u64));
        }
    }
    // An odd median whose weighted amount isn't a whole number of wei.
    let odd = Amount::from(1_000_003u64);
    assert_eq!(
        weighted_amount(odd, chunk(0)),
        Some(three_times_rounded_up(odd, chunk(0).numerator()))
    );
    assert_eq!(weighted_amount(Amount::ZERO, chunk(0)), Some(Amount::ZERO));
    assert_eq!(weighted_amount(Amount::MAX, chunk(0)), None, "overflow");
}

#[test]
fn a_pointer_weighs_as_its_5303_bytes_whatever_size_is_declared() {
    assert_eq!(POINTER_WIRE_LEN, 5303);
    let pointer = record_weight(DATA_TYPE_POINTER, POINTER_WIRE_LEN);
    assert_eq!(pointer, chunk(5303));
    assert_eq!(in_1024ths(pointer), 259, "the 16 KiB class");
    for declared in [0, 5303, 16 * KIB, MAX_CHUNK_SIZE, usize::MAX] {
        assert_eq!(
            record_weight(DATA_TYPE_POINTER, declared),
            pointer,
            "{declared}"
        );
    }
}

#[test]
fn a_pointer_write_pays_three_times_the_median_times_259_1024ths() {
    for records in [0, 6_000] {
        let median = calculate_price(records);
        let amount = weighted_amount(median, record_weight(DATA_TYPE_POINTER, 0)).unwrap();
        let exact_in_1024ths = median * Amount::from(3u64 * 259);
        let whole = exact_in_1024ths / Amount::from(1024u64);
        let expected = if whole * Amount::from(1024u64) == exact_in_1024ths {
            whole
        } else {
            whole + Amount::from(1u64)
        };
        assert_eq!(amount, expected, "at {records} records");
    }
}

#[test]
fn a_data_type_the_table_does_not_know_weighs_as_a_full_chunk() {
    for data_type in [2, 7, u32::MAX] {
        assert_eq!(
            record_weight(data_type, 0),
            RecordWeight::FULL,
            "{data_type}"
        );
    }
}

#[test]
fn the_weight_total_is_never_above_the_key_count_and_equals_it_for_full_chunks() {
    // m = K for full chunks, and for records whose length isn't known, which
    // count as full chunks.
    for k in [1u64, 2, 7, 1_000] {
        let full = (0..k).map(|_| RecordWeight::FULL);
        assert_eq!(weight_total(full), k);
    }
    // m ≤ K for every mix, and m ≥ 1 whenever K ≥ 1.
    let sizes = [
        0,
        1,
        5303,
        16 * KIB + 1,
        300 * KIB,
        MAX_CHUNK_SIZE - 1,
        MAX_CHUNK_SIZE,
    ];
    for k in 1..=sizes.len() * 3 {
        let weights: Vec<RecordWeight> = sizes
            .iter()
            .cycle()
            .take(k)
            .map(|&len| chunk(len))
            .collect();
        let m = weight_total(weights.iter().copied());
        assert!(m >= 1 && m <= k as u64, "m = {m} for K = {k}");
        // Exactly the sum of the weights, rounded up.
        let sum: u128 = weights.iter().map(|w| u128::from(w.numerator())).sum();
        assert_eq!(u128::from(m), sum.div_ceil(u128::from(WEIGHT_DENOMINATOR)));
    }
    // Four of the smallest records make 259 × 4 / 1,024 of a chunk: m = 2.
    assert_eq!(weight_total([chunk(0); 4]), 2);
    // One tiny record still counts as 1.
    assert_eq!(weight_total([chunk(1)]), 1);
    assert_eq!(weight_total(std::iter::empty()), 0);
}

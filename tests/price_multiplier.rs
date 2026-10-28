//! Prices on this network: the current curve and the legacy curve beside it.
//!
//! Dan, 2026-10-28 decision: 100 DARWIN per GB, divisor 2,000. The current
//! curve, `calculate_price`, has a baseline of 0.1396736 DARWIN, a coefficient
//! of 9 × the baseline and a divisor of 2,000 records: a GB of full chunks
//! costs exactly 100 DARWIN on empty nodes, 1,000 at 2,000 records and 8,200
//! at 6,000. The legacy curve, `calculate_price_legacy`, is upstream's curve
//! × 32 (the multiplier set 2026-10-06), still accepted beside it.
//!
//! Both are set in the vendored ant-protocol,
//! `vendor/ant-protocol/src/payment/pricing.rs`, which `[patch.crates-io]` in
//! `Cargo.toml` puts in place of the crates.io release. Nodes and clients must
//! carry the same curves, so these tests fail if the patch stops applying or
//! a curve changes.
//!
//! The pins of pricing by record size are beside this file, in
//! `tests/record_weight.rs`: `record_weight`; `weighted_amount`; a node's
//! weight total, which is never above its committed key count and equals it
//! when every record is a full chunk; and the weighted amount for a pointer,
//! at its 5,303 bytes, which `tests/pointer_convergence.rs` pins too.

use ant_protocol::evm::Amount;
use ant_protocol::payment::pricing::{calculate_price, calculate_price_legacy};

/// 1 DARWIN in wei.
const WEI_PER_DARWIN: u128 = 1_000_000_000_000_000_000;

/// The bytes of a full chunk the per-GB price is set by, as Dan's decision
/// states it. The largest record, `FULL_RECORD_BYTES`, is 4,194,304 bytes and
/// pays the same per chunk.
const FULL_CHUNK_BYTES: u64 = 4_190_208;

/// A GB, 10⁹ bytes.
const GB: u64 = 1_000_000_000;

/// Upstream's price for a node storing nothing, in wei (0.00390625 ANT).
const UPSTREAM_BASELINE_WEI: u128 = 3_906_250_000_000_000;

/// Upstream's quadratic coefficient, in wei (0.03515625 ANT).
const UPSTREAM_COEFFICIENT_WEI: u128 = 35_156_250_000_000_000;

/// The legacy curve's multiplier over upstream's prices.
const LEGACY_PRICE_MULTIPLIER: u128 = 32;

/// Whether a GB of full chunks, each paying 3 × the quote `price`, costs
/// exactly `darwin_per_gb`.
fn a_gb_costs(price: Amount, darwin_per_gb: u128) -> bool {
    Amount::from(GB) * Amount::from(3u64) * price
        == Amount::from(darwin_per_gb * WEI_PER_DARWIN) * Amount::from(FULL_CHUNK_BYTES)
}

#[test]
fn a_gb_costs_100_darwin_on_empty_nodes() {
    assert_eq!(calculate_price(0), Amount::from(139_673_600_000_000_000u128));
    assert!(a_gb_costs(calculate_price(0), 100));
}

#[test]
fn a_gb_costs_1_000_darwin_at_the_divisor_of_2_000_records() {
    assert_eq!(
        calculate_price(2_000),
        calculate_price(0) * Amount::from(10u64)
    );
    assert!(a_gb_costs(calculate_price(2_000), 1_000));
}

#[test]
fn a_gb_costs_8_200_darwin_at_6_000_records() {
    assert_eq!(
        calculate_price(6_000),
        calculate_price(0) * Amount::from(82u64)
    );
    assert!(a_gb_costs(calculate_price(6_000), 8_200));
}

#[test]
fn the_legacy_curve_is_upstreams_times_32() {
    assert_eq!(
        calculate_price_legacy(0),
        Amount::from(UPSTREAM_BASELINE_WEI * LEGACY_PRICE_MULTIPLIER)
    );
    assert_eq!(
        calculate_price_legacy(6_000),
        Amount::from((UPSTREAM_BASELINE_WEI + UPSTREAM_COEFFICIENT_WEI) * LEGACY_PRICE_MULTIPLIER)
    );
}

//! Prices on this network carry its multiplier over Autonomi's.
//!
//! `PRICE_MULTIPLIER` (32) is set in the vendored ant-protocol,
//! `vendor/ant-protocol/src/payment/pricing.rs`, which `[patch.crates-io]` in
//! `Cargo.toml` puts in place of the crates.io release. Nodes and clients must
//! carry the same multiplier, so these tests fail if the patch stops applying
//! or the multiplier changes.
//!
//! The pins of pricing by record size are beside this file, in
//! `tests/record_weight.rs`: `record_weight`; `weighted_amount`; a node's
//! weight total, which is never above its committed key count and equals it
//! when every record is a full chunk; and the weighted amount for a pointer,
//! at its 5,303 bytes, which `tests/pointer_convergence.rs` pins too. Every
//! store check requires the weighted amount; no quote uses the weights yet.

use ant_protocol::evm::Amount;
use ant_protocol::payment::calculate_price;

/// Upstream's price for a node storing nothing, in wei (0.00390625 ANT).
const UPSTREAM_BASELINE_WEI: u128 = 3_906_250_000_000_000;

/// Upstream's quadratic coefficient, in wei (0.03515625 ANT).
const UPSTREAM_COEFFICIENT_WEI: u128 = 35_156_250_000_000_000;

/// Records stored at the curve's lower stable boundary, where the quadratic
/// term equals the coefficient.
const PRICING_DIVISOR: usize = 6_000;

/// This network's multiplier over upstream's prices.
const PRICE_MULTIPLIER: u128 = 32;

#[test]
fn an_empty_node_charges_upstreams_baseline_times_the_multiplier() {
    assert_eq!(
        calculate_price(0),
        Amount::from(UPSTREAM_BASELINE_WEI * PRICE_MULTIPLIER)
    );
}

#[test]
fn the_quadratic_term_carries_the_multiplier_too() {
    assert_eq!(
        calculate_price(PRICING_DIVISOR),
        Amount::from((UPSTREAM_BASELINE_WEI + UPSTREAM_COEFFICIENT_WEI) * PRICE_MULTIPLIER)
    );
}

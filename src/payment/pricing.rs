//! Quadratic pricing with a baseline floor.
//!
//! ADR-0004: the pricing formula is now the **single source of truth** in
//! `ant-protocol` (`ant_protocol::payment::pricing`), so the node (when pricing
//! a quote) and the client (when verifying the forced price before paying)
//! compute byte-for-byte identical prices and can never drift. This module
//! re-exports it so every existing `crate::payment::pricing::…` caller keeps
//! working unchanged.
//!
//! See `ant-protocol/src/payment/pricing.rs` for the formula, constants, and
//! the full unit-test suite.
//!
//! The node alone decides which curve it quotes: [`quote_price_at`].

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use evmlib::common::Amount;

pub use ant_protocol::payment::pricing::{
    band_bottom_count, calculate_price, calculate_price_legacy, derive_records_stored_from_price,
    price_band, price_band_legacy, price_in_accepted_band, price_in_band, price_in_band_legacy,
};

/// Dan, 2026-10-28 decision: 100 DARWIN per GB, divisor 2,000. Nodes quote
/// the legacy curve before 2026-10-29T00:00:00Z and the current curve from
/// it, by the node's wall clock when the quote is made. Unix seconds.
pub const PRICE_SWITCH_AT: u64 = 1_793_232_000;

/// Whether a quote made at `at` is priced on the legacy curve
/// ([`calculate_price_legacy`]): true before [`PRICE_SWITCH_AT`].
#[must_use]
pub fn quotes_on_legacy_curve(at: SystemTime) -> bool {
    at < UNIX_EPOCH + Duration::from_secs(PRICE_SWITCH_AT)
}

/// The price a node quotes for `count` at `at`: the legacy curve before
/// [`PRICE_SWITCH_AT`], the current curve ([`calculate_price`]) from it.
#[must_use]
pub fn quote_price_at(count: usize, at: SystemTime) -> Amount {
    if quotes_on_legacy_curve(at) {
        calculate_price_legacy(count)
    } else {
        calculate_price(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn the_switch_is_2026_10_29_midnight_utc() {
        assert_eq!(PRICE_SWITCH_AT, 1_793_232_000);
    }

    #[test]
    fn nodes_quote_the_legacy_curve_only_before_the_switch() {
        assert!(quotes_on_legacy_curve(UNIX_EPOCH));
        assert!(quotes_on_legacy_curve(at(PRICE_SWITCH_AT - 1)));
        assert!(!quotes_on_legacy_curve(at(PRICE_SWITCH_AT)));
        assert!(!quotes_on_legacy_curve(at(PRICE_SWITCH_AT + 1)));
    }

    #[test]
    fn quote_price_at_picks_the_curve_by_time() {
        assert_eq!(
            quote_price_at(1_000, at(PRICE_SWITCH_AT - 1)),
            calculate_price_legacy(1_000)
        );
        assert_eq!(
            quote_price_at(1_000, at(PRICE_SWITCH_AT)),
            calculate_price(1_000)
        );
        assert_eq!(
            quote_price_at(1_000, at(PRICE_SWITCH_AT + 86_400)),
            calculate_price(1_000)
        );
        assert_ne!(calculate_price_legacy(1_000), calculate_price(1_000));
    }
}

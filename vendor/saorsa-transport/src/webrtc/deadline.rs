// Copyright 2026 Saorsa Labs Limited
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Monotonic frame deadlines shared by native and browser WebRTC I/O.

use std::time::Duration;
use web_time::Instant;

/// A bounded frame deadline measured independently of the adjustable wall clock.
#[derive(Debug, Clone, Copy)]
pub struct TransferDeadline {
    started: Instant,
    budget: Duration,
}

impl TransferDeadline {
    /// Start a deadline with an explicit processing or transfer budget.
    #[must_use]
    pub fn new(budget: Duration) -> Self {
        Self {
            started: Instant::now(),
            budget,
        }
    }

    /// Start the shared transfer budget for a complete frame.
    #[must_use]
    pub fn for_frame(bytes: usize) -> Self {
        Self::new(super::transfer_timeout(bytes))
    }

    /// Extend from the original frame start after its declared size is known.
    /// Receiving more fragments never restarts the clock.
    pub fn extend_for_frame(&mut self, bytes: usize) {
        self.budget = self.budget.max(super::transfer_timeout(bytes));
    }

    /// Remaining budget, clamped to zero after expiration.
    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.budget.saturating_sub(self.started.elapsed())
    }

    /// Remaining browser timer milliseconds, rounded up and safe for JS timers.
    #[must_use]
    pub fn remaining_ms(&self) -> u32 {
        let millis = self.remaining().as_nanos().div_ceil(1_000_000);
        millis.min(i32::MAX as u128) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragments_extend_from_the_original_start() {
        let mut deadline = TransferDeadline::for_frame(4);
        let started = deadline.started;
        deadline.extend_for_frame(4 * 1024 * 1024);
        assert_eq!(deadline.started, started);
        assert_eq!(
            deadline.budget,
            super::super::transfer_timeout(4 * 1024 * 1024)
        );
        deadline.extend_for_frame(4);
        assert_eq!(
            deadline.budget,
            super::super::transfer_timeout(4 * 1024 * 1024)
        );
    }

    #[test]
    fn expiration_and_timer_conversion_are_bounded() {
        assert_eq!(TransferDeadline::new(Duration::ZERO).remaining_ms(), 0);
        assert_eq!(
            TransferDeadline::new(Duration::MAX).remaining_ms(),
            i32::MAX as u32
        );
    }
}

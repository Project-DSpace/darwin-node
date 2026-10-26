//! Counters for fresh replication's notices, holds, offers and possession
//! checks, logged with the replication traffic summary.
//!
//! Process-global relaxed atomics, monotonic since the process started, as
//! the per-message tables in [`crate::replication::protocol`]: rates are
//! deltas read at query time, so a dropped line corrupts nothing. Two are
//! gauges, the possession checks waiting for the byte budget and the bytes
//! the checks hold, and read as they are.
//!
//! The field names are pinned ([`SUMMARY_FIELDS`], in `tests/fork_pins.rs`),
//! so a monitor that reads them can't break silently.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::replication::heard_from::Served;

/// One counter of the summary line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum FreshCounter {
    /// Holding notices sent after a store, one per recipient.
    HoldingNoticesSent,
    /// Holding notices sent from an "already stored" answer, one per recipient.
    HeldAnswerNoticesSent,
    /// Notices with a proof sent, one per recipient.
    ProofNoticesSent,
    /// Holding notices received.
    HoldingNoticesReceived,
    /// Notices with a proof received.
    ProofNoticesReceived,
    /// Offers sent, one per recipient (each attempt).
    OffersSent,
    /// Offers skipped: the peer was heard from.
    OffersSkippedHeardFrom,
    /// Offers skipped: the peer is marked full.
    OffersSkippedMarkedFull,
    /// Holds that ended early: every close-group peer heard from or full.
    HoldsEndedEarly,
    /// Holds that ended at the hold time.
    HoldsEndedAtHold,
    /// Holds ended early because the held proofs reached their cap.
    HoldsEndedAtCap,
    /// Close-group peers neither skipped nor offered when a hold ended.
    PeersLeftUnserved,
    /// Stored writes with no proof to send: holding notices only.
    HoldsWithoutProof,
    /// Heard-from entries not kept because the table was full.
    HeardFromNotKept,
    /// Proofs the slim builder declined, so not forwarded.
    SlimProofsDeclined,
    /// Received notices whose proof was refused for its form.
    ProofsRefusedFormNotice,
    /// Received offers whose proof was refused for its form.
    ProofsRefusedFormOffer,
    /// Received pointer offers whose proof was refused for its form.
    ProofsRefusedFormPointerOffer,
    /// Possession checks scheduled for peers heard from.
    PossessionChecksHeardFrom,
    /// Possession checks scheduled for peers marked full.
    PossessionChecksMarkedFull,
    /// Possession checks scheduled for peers offered the record.
    PossessionChecksOffered,
    /// Gauge: possession checks waiting for the byte budget.
    PossessionChecksWaitingBudget,
    /// Gauge: bytes the running possession checks hold.
    PossessionBudgetBytesHeld,
    /// Confirmed possession failures on peers whose offer was skipped.
    PossessionFailuresSkipped,
    /// "PoP verification error" on a received offer or notice.
    PopVerificationErrors,
    /// "Paid notify rejected".
    PaidNotifyRejected,
    /// "Paid notify dropped at admission".
    PaidNotifyDroppedAtAdmission,
    /// Received pointer offers refused for their payment.
    PointerOffersRefused,
    /// Answers to one-way offers, not sent.
    OfferAnswersNotSent,
}

const N_COUNTERS: usize = 29;

/// The summary line's field names, in [`FreshCounter`] order.
pub const SUMMARY_FIELDS: [&str; N_COUNTERS] = [
    "holding_notices_sent",
    "held_answer_notices_sent",
    "proof_notices_sent",
    "holding_notices_received",
    "proof_notices_received",
    "offers_sent",
    "offers_skipped_heard_from",
    "offers_skipped_marked_full",
    "holds_ended_early",
    "holds_ended_at_hold",
    "holds_ended_at_cap",
    "peers_left_unserved",
    "holds_without_proof",
    "heard_from_not_kept",
    "slim_proofs_declined",
    "proofs_refused_form_notice",
    "proofs_refused_form_offer",
    "proofs_refused_form_pointer_offer",
    "possession_checks_heard_from",
    "possession_checks_marked_full",
    "possession_checks_offered",
    "possession_checks_waiting_budget",
    "possession_budget_bytes_held",
    "possession_failures_skipped",
    "pop_verification_errors",
    "paid_notify_rejected",
    "paid_notify_dropped_at_admission",
    "pointer_offers_refused",
    "offer_answers_not_sent",
];

static COUNTERS: [AtomicU64; N_COUNTERS] = [const { AtomicU64::new(0) }; N_COUNTERS];

/// Add `n` to a counter.
pub(crate) fn add(counter: FreshCounter, n: u64) {
    COUNTERS[counter as usize].fetch_add(n, Ordering::Relaxed);
}

/// Add one to a counter.
pub(crate) fn bump(counter: FreshCounter) {
    add(counter, 1);
}

/// Take `n` from a gauge.
pub(crate) fn sub(counter: FreshCounter, n: u64) {
    COUNTERS[counter as usize].fetch_sub(n, Ordering::Relaxed);
}

/// A counter's value.
#[must_use]
pub fn read(counter: FreshCounter) -> u64 {
    COUNTERS[counter as usize].load(Ordering::Relaxed)
}

/// Count `n` possession checks scheduled for peers served as `served`.
pub(crate) fn checks_scheduled(served: Served, n: u64) {
    let counter = match served {
        Served::HeardFrom => FreshCounter::PossessionChecksHeardFrom,
        Served::MarkedFull => FreshCounter::PossessionChecksMarkedFull,
        Served::Offered => FreshCounter::PossessionChecksOffered,
    };
    add(counter, n);
}

/// Where a received proof was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefusedIn {
    Notice,
    Offer,
    PointerOffer,
}

/// Count a received proof the payment check refused, when it was refused for
/// its form: its bytes don't decode as a single-node proof, or its quotes fail
/// the structure check. Refusals for anything else (the chain read, the price
/// owed) are not counted here.
pub(crate) fn note_refused_proof(context: RefusedIn, proof: &[u8]) {
    if !crate::payment::slim::refused_for_form(proof) {
        return;
    }
    bump(match context {
        RefusedIn::Notice => FreshCounter::ProofsRefusedFormNotice,
        RefusedIn::Offer => FreshCounter::ProofsRefusedFormOffer,
        RefusedIn::PointerOffer => FreshCounter::ProofsRefusedFormPointerOffer,
    });
}

/// Emit the counters as one INFO line, on the traffic summary's target and
/// cadence. The field names are [`SUMMARY_FIELDS`].
pub(crate) fn log_fresh_summary() {
    let c = |counter: FreshCounter| read(counter);
    crate::logging::info!(
        target: "ant_node::replication::traffic",
        group = 7,
        holding_notices_sent = c(FreshCounter::HoldingNoticesSent),
        held_answer_notices_sent = c(FreshCounter::HeldAnswerNoticesSent),
        proof_notices_sent = c(FreshCounter::ProofNoticesSent),
        holding_notices_received = c(FreshCounter::HoldingNoticesReceived),
        proof_notices_received = c(FreshCounter::ProofNoticesReceived),
        offers_sent = c(FreshCounter::OffersSent),
        offers_skipped_heard_from = c(FreshCounter::OffersSkippedHeardFrom),
        offers_skipped_marked_full = c(FreshCounter::OffersSkippedMarkedFull),
        holds_ended_early = c(FreshCounter::HoldsEndedEarly),
        holds_ended_at_hold = c(FreshCounter::HoldsEndedAtHold),
        holds_ended_at_cap = c(FreshCounter::HoldsEndedAtCap),
        peers_left_unserved = c(FreshCounter::PeersLeftUnserved),
        holds_without_proof = c(FreshCounter::HoldsWithoutProof),
        heard_from_not_kept = c(FreshCounter::HeardFromNotKept),
        slim_proofs_declined = c(FreshCounter::SlimProofsDeclined),
        proofs_refused_form_notice = c(FreshCounter::ProofsRefusedFormNotice),
        proofs_refused_form_offer = c(FreshCounter::ProofsRefusedFormOffer),
        proofs_refused_form_pointer_offer = c(FreshCounter::ProofsRefusedFormPointerOffer),
        possession_checks_heard_from = c(FreshCounter::PossessionChecksHeardFrom),
        possession_checks_marked_full = c(FreshCounter::PossessionChecksMarkedFull),
        possession_checks_offered = c(FreshCounter::PossessionChecksOffered),
        possession_checks_waiting_budget = c(FreshCounter::PossessionChecksWaitingBudget),
        possession_budget_bytes_held = c(FreshCounter::PossessionBudgetBytesHeld),
        possession_failures_skipped = c(FreshCounter::PossessionFailuresSkipped),
        pop_verification_errors = c(FreshCounter::PopVerificationErrors),
        paid_notify_rejected = c(FreshCounter::PaidNotifyRejected),
        paid_notify_dropped_at_admission = c(FreshCounter::PaidNotifyDroppedAtAdmission),
        pointer_offers_refused = c(FreshCounter::PointerOffersRefused),
        offer_answers_not_sent = c(FreshCounter::OfferAnswersNotSent),
        "fresh replication summary (cumulative)"
    );
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// The last counter fills the table, and every field name is logged by
    /// the summary line under that name, in counter order.
    #[test]
    fn every_counter_is_logged_under_its_pinned_name() {
        assert_eq!(FreshCounter::OfferAnswersNotSent as usize + 1, N_COUNTERS);
        let source = include_str!("fresh_metrics.rs");
        let body_start = source
            .find("fn log_fresh_summary")
            .expect("the summary function");
        let body = &source[body_start..];
        let mut from = 0;
        for name in SUMMARY_FIELDS {
            let field = format!("        {name} = c(");
            let at = body[from..]
                .find(&field)
                .unwrap_or_else(|| panic!("{name} is not logged, or out of order"));
            from += at + field.len();
        }
    }
}

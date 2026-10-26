//! Cumulative EVM RPC call accounting (V2-834 Part D.3).
//!
//! Each call is one HTTP request to an RPC, whatever it carries: a batch of
//! `completedPayments` calls counts once, as a batch. The merkle read goes
//! through `evmlib` → `alloy`, which serialises JSON-RPC inside its own
//! transport, so request and response body sizes are not recorded here; this
//! table records calls and outcomes only. EVM RPC is HTTPS over TCP and sits
//! outside the UDP reconciliation invariant; call counts are what is needed to
//! bound it.

use std::sync::atomic::{AtomicU64, Ordering};

/// Which on-chain read was made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvmRpcCall {
    /// `IPaymentVault::completedPayments(quote_hash)` (single-node path), one
    /// call on its own.
    CompletedPayments,
    /// `getCompletedMerklePayment(pool_hash)` (merkle batch path).
    CompletedMerklePayment,
    /// `eth_getTransactionReceipt` of a transaction a payment proof names.
    TransactionReceipt,
    /// A JSON-RPC batch of `completedPayments` calls, sent as one request.
    CompletedPaymentsBatch,
    /// `eth_getBlockByNumber`, for the time of the block an envelope was paid
    /// in.
    BlockTimestamp,
    /// A root vault's `records(key)`: whether a wave of a root payment was
    /// recorded.
    RootRecord,
}

impl EvmRpcCall {
    const N: usize = 6;

    const fn index(self) -> usize {
        match self {
            Self::CompletedPayments => 0,
            Self::CompletedMerklePayment => 1,
            Self::TransactionReceipt => 2,
            Self::CompletedPaymentsBatch => 3,
            Self::BlockTimestamp => 4,
            Self::RootRecord => 5,
        }
    }
}

static OK_COUNT: [AtomicU64; EvmRpcCall::N] = [const { AtomicU64::new(0) }; EvmRpcCall::N];
static ERR_COUNT: [AtomicU64; EvmRpcCall::N] = [const { AtomicU64::new(0) }; EvmRpcCall::N];

/// Record the outcome of one on-chain read.
pub fn record(call: EvmRpcCall, ok: bool) {
    let table = if ok { &OK_COUNT } else { &ERR_COUNT };
    table[call.index()].fetch_add(1, Ordering::Relaxed);
}

/// A payment proof settled on chain, by its tag. A merkle proof (0x02) is
/// refused before anything is read, so it never settles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettledProof {
    /// A single-node proof (0x01), settled in the per-quote vault's record.
    SingleNode,
    /// A root proof (0x03), settled in the root vault's record of its wave.
    Root,
}

impl SettledProof {
    const N: usize = 2;

    const fn index(self) -> usize {
        match self {
            Self::SingleNode => 0,
            Self::Root => 1,
        }
    }
}

static SETTLED_COUNT: [AtomicU64; SettledProof::N] = [const { AtomicU64::new(0) }; SettledProof::N];

/// Record one payment proof the payment check accepted on chain, by its tag:
/// a proof the cache already held isn't counted again.
pub fn record_settlement(proof: SettledProof) {
    SETTLED_COUNT[proof.index()].fetch_add(1, Ordering::Relaxed);
}

/// The proofs of `proof`'s tag settled so far in this process.
#[cfg(test)]
pub fn settlements(proof: SettledProof) -> u64 {
    SETTLED_COUNT[proof.index()].load(Ordering::Relaxed)
}

/// The calls of `call`'s kind recorded so far in this process, as
/// `(ok, err)`.
#[cfg(test)]
pub fn counts(call: EvmRpcCall) -> (u64, u64) {
    (
        OK_COUNT[call.index()].load(Ordering::Relaxed),
        ERR_COUNT[call.index()].load(Ordering::Relaxed),
    )
}

/// Emit the cumulative EVM RPC call figures as one INFO line, target
/// `ant_node::payment::traffic`.
pub fn log_evm_rpc_summary() {
    use EvmRpcCall as C;

    let ok = |c: C| OK_COUNT[c.index()].load(Ordering::Relaxed);
    let err = |c: C| ERR_COUNT[c.index()].load(Ordering::Relaxed);
    let settled = |p: SettledProof| SETTLED_COUNT[p.index()].load(Ordering::Relaxed);

    crate::logging::info!(
        target: "ant_node::payment::traffic",
        completed_payments_ok_count = ok(C::CompletedPayments),
        completed_payments_err_count = err(C::CompletedPayments),
        completed_payments_batch_ok_count = ok(C::CompletedPaymentsBatch),
        completed_payments_batch_err_count = err(C::CompletedPaymentsBatch),
        receipt_ok_count = ok(C::TransactionReceipt),
        receipt_err_count = err(C::TransactionReceipt),
        block_ok_count = ok(C::BlockTimestamp),
        block_err_count = err(C::BlockTimestamp),
        root_record_ok_count = ok(C::RootRecord),
        root_record_err_count = err(C::RootRecord),
        merkle_payment_ok_count = ok(C::CompletedMerklePayment),
        merkle_payment_err_count = err(C::CompletedMerklePayment),
        settled_single_node_count = settled(SettledProof::SingleNode),
        settled_root_count = settled(SettledProof::Root),
        "evm rpc summary (cumulative)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every kind of call has its own pair of counts, and each new kind is
    /// counted apart from the others.
    #[test]
    fn each_kind_of_call_is_counted_apart() {
        let kinds = [
            EvmRpcCall::CompletedPayments,
            EvmRpcCall::CompletedMerklePayment,
            EvmRpcCall::TransactionReceipt,
            EvmRpcCall::CompletedPaymentsBatch,
            EvmRpcCall::BlockTimestamp,
            EvmRpcCall::RootRecord,
        ];
        let mut indexes: Vec<usize> = kinds.iter().map(|kind| kind.index()).collect();
        indexes.sort_unstable();
        indexes.dedup();
        assert_eq!(indexes.len(), EvmRpcCall::N);
        assert!(indexes.iter().all(|index| *index < EvmRpcCall::N));
    }

    /// Each proof tag that settles has its own count.
    #[test]
    fn each_settled_proof_tag_is_counted_apart() {
        let mut indexes: Vec<usize> = [SettledProof::SingleNode, SettledProof::Root]
            .iter()
            .map(|proof| proof.index())
            .collect();
        indexes.sort_unstable();
        indexes.dedup();
        assert_eq!(indexes.len(), SettledProof::N);
        assert!(indexes.iter().all(|index| *index < SettledProof::N));
        let before = settlements(SettledProof::Root);
        record_settlement(SettledProof::Root);
        assert!(settlements(SettledProof::Root) > before);
    }
}

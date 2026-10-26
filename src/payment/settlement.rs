//! Reading payments from the payment vault, for the payment check.
//!
//! For each quote it checks, the payment check asks the vault what was paid
//! for the quote's hash (`completedPayments`): an amount, and the leading 16
//! bytes of the address it was paid to. That per-quote read is the authority,
//! and every refusal comes from it. In front of it sits a cache of payment
//! facts, by quote hash, which can only ever spare a read, never refuse one:
//!
//! - It holds only positive, settled facts: a non-zero amount, and the payee it
//!   was paid to. Never an empty record, a missing receipt, an error or a
//!   timeout.
//! - Its facts are the vault's own: the answers to per-quote reads, and the
//!   `DataPaymentMade` logs in the receipts of the transactions a payment
//!   proof names, one read for each distinct transaction, at most
//!   [`MAX_PROOF_TXS`] of them. A log counts only from a receipt whose
//!   transaction succeeded and only if the vault emitted it
//!   ([`crate::listings::vault_payments`]), and its payee and amount go through
//!   the same check as a read's answer.
//! - The vault writes each quote's record once and logs it only on that
//!   write, so a fact never goes stale.
//!
//! A quote with no fact, or with a fact that doesn't pay it, goes to the
//! per-quote read, so a proof whose transactions don't carry its quote's
//! payment (paid in another transaction, paid earlier, or taken first by
//! someone else's payment) is checked exactly as before.
//!
//! Every read goes through one pool of connections to the node's RPCs: the
//! node's own, then, where it is one of Robinhood Chain's public RPCs or one
//! retired from them, the others ([`crate::listings::listings_rpc_urls`]). A
//! read starts with the RPC that answered last. An RPC that answered busy, or
//! didn't answer in time, is asked after the others for [`HOLD_OFF`]; it is
//! still asked when they fail, so being held off changes only the order. A
//! failed call moves on to the next RPC at once, and a read of payment records
//! that fails on every RPC is made again in a second round,
//! [`PAYMENT_READ_BACKOFF`] later.
//!
//! While a read of payment records is in flight, further reads wait for it and
//! then go together, as one JSON-RPC batch; an RPC that refuses batches, or
//! leaves calls out of its answer, is read one call at a time. Two reads of
//! one record at once share one call, but only while it is in flight: a caller
//! that joined a read that came back empty, failed or was dropped makes a read
//! of its own, and never takes on another's failure.
//!
//! A client waiting on a store gives each node a fixed time to answer
//! ([`CLIENT_STORE_WAIT`]). Reads on a store's path ([`Lane::Put`]) give each
//! call [`PUT_CALL_TIMEOUT`], and ask the next RPC as well when the first hasn't
//! answered after [`PUT_SECOND_RPC_AFTER`], taking the first good answer. The
//! store's whole payment check ends at [`PUT_PAYMENT_DEADLINE`]. Reads run on
//! their own, so one still in flight at the deadline finishes and fills the
//! cache for the client's next try. Reads nobody waits on ([`Lane::Background`])
//! give each call [`BACKGROUND_CALL_TIMEOUT`].
//!
//! A payment record that reads empty is read once more, after
//! [`EMPTY_PAYMENT_RECHECK_DELAY`], by a read of its own that skips the cache,
//! on a fresh connection, at the RPC after the one that answered empty
//! ([`SettlementReads::settlement_again`]). An envelope's receipt that an RPC
//! didn't know is read once more the same way
//! ([`SettlementReads::receipt_again`]).

use crate::listings::{parse_receipt, parse_u64, quantity, vault_payments, MAX_PROOF_TXS};
use crate::logging::debug;
use crate::payment::traffic::{self, EvmRpcCall};
use ant_listings::{PaymentReceipt, DATA_PAYMENT_MADE_TOPIC};
use evmlib::common::Amount;
use futures::stream::{FuturesUnordered, StreamExt};
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

/// How long a released client waits for a node's answer to a store.
///
/// The client's `STORE_RESPONSE_TIMEOUT`: past it, the client gives up on
/// that node. Clients already released keep this wait, so a node answers
/// inside it.
pub const CLIENT_STORE_WAIT: Duration = Duration::from_secs(10);

/// The deadline of a store's whole payment check, from when it starts.
///
/// It covers the payment reads, the read again of a record that read empty,
/// and an envelope's payment lookup. At the deadline the node answers with
/// its payment error, before the client stops waiting, and the reads still
/// in flight go on to fill the cache, so the client's next try passes.
pub const PUT_PAYMENT_DEADLINE: Duration = Duration::from_secs(7);

const _: () = assert!(
    PUT_PAYMENT_DEADLINE.as_millis() < CLIENT_STORE_WAIT.as_millis(),
    "a store's payment check must end before the client stops waiting"
);

/// How long one call on a store's path may take.
pub const PUT_CALL_TIMEOUT: Duration = Duration::from_millis(2_500);

/// How long a call on a store's path may go unanswered before the same call
/// goes to the next RPC as well. The first good answer counts.
pub const PUT_SECOND_RPC_AFTER: Duration = Duration::from_secs(2);

/// On a store's path, how long the receipts of a proof's transactions are
/// read alone before the per-quote read of a quote they haven't settled
/// starts beside them. Claude's proposal: a receipt from an RPC that answers
/// in a round trip settles its quotes within it, saving their reads, and
/// with every RPC slow (about 2.3 s a call) the per-quote read, read empty,
/// the pause and the read again (about 6.1 s) still end inside
/// [`PUT_PAYMENT_DEADLINE`].
pub const PUT_RECEIPTS_HEAD_START: Duration = Duration::from_millis(500);

/// How long one call nobody is waiting on may take: reads for fresh
/// replication offers, paid-list notices and listings.
pub const BACKGROUND_CALL_TIMEOUT: Duration = Duration::from_secs(15);

/// How long an RPC that answered busy, or didn't answer in time, is asked
/// after the others. It is still asked when they fail.
pub const HOLD_OFF: Duration = Duration::from_secs(30);

/// Rounds of a read of payment records before it fails. Each round asks the
/// RPCs in turn, so a provider that answers `-32005 the network is busy` or
/// drops a call doesn't make the node refuse a chunk that was paid for.
pub const PAYMENT_READ_ROUNDS: u32 = 2;

/// The pause before each later round of a read of payment records.
pub const PAYMENT_READ_BACKOFF: Duration = Duration::from_millis(500);

/// The pause before payment records that read empty are read once more.
///
/// A client pays and sends the chunk at once. The RPC this node reads through
/// can be a block or two behind the one the client paid through, and then
/// answers with the record as it was before the payment: no amount and no
/// rewards address, which the check refuses as a redirected payment. Read
/// again a moment later, the payment is there. Long enough for an RPC a few of
/// Robinhood Chain's blocks behind to catch up; short, because a store nobody
/// paid for waits it out too before it is refused, once. An envelope's
/// receipt that the RPC didn't know yet is read once more after the same
/// pause.
pub const EMPTY_PAYMENT_RECHECK_DELAY: Duration = Duration::from_millis(1500);

/// Payment facts kept, by quote hash.
const FACTS_CAPACITY: usize = 16_384;

/// Receipts kept, by transaction hash, for the envelope lookup that follows
/// a store's payment check. Only the vault's payment logs are kept of each.
const RECEIPTS_CAPACITY: usize = 64;

/// Block times kept, by block number.
const BLOCKS_CAPACITY: usize = 1_024;

/// The most `completedPayments` calls sent in one JSON-RPC batch. Public
/// RPCs cap batches, some well below a hundred calls, and answer a larger
/// one as refused.
const MAX_BATCH: usize = 32;

/// The reads of a batch no RPC answered in time are made one at a time, at
/// most this many at once. Claude's proposal: an RPC that stalled is sent a
/// few calls, not a whole batch's worth at once; a batch of `MAX_BATCH`
/// answered in about a second a call is read in about 8 seconds.
const SINGLES_AFTER_TIMED_OUT_BATCH: usize = 4;

/// `completedPayments(bytes32)`'s selector: the first four bytes of the
/// Keccak-256 hash of that signature.
const COMPLETED_PAYMENTS_SELECTOR: [u8; 4] = [0xfe, 0x3c, 0x80, 0x6e];

/// The JSON-RPC error code of a provider that is busy or limiting its rate.
const BUSY_CODE: i64 = -32_005;

/// What the vault recorded for a quote hash (`completedPayments`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settlement {
    /// The amount paid; zero for an empty record.
    pub amount: Amount,
    /// The leading 16 bytes of the address paid, as the vault packs it.
    pub payee_prefix: [u8; 16],
}

impl Settlement {
    /// A record with nothing in it: no payment, or none the RPC has seen.
    pub fn is_empty(&self) -> bool {
        self.amount.is_zero() && self.payee_prefix == [0; 16]
    }

    /// A payment the vault recorded. Only these are kept as facts.
    fn is_settled(&self) -> bool {
        !self.amount.is_zero()
    }
}

/// An answer, and the RPC that gave it, by its place in the node's list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answered<T> {
    pub value: T,
    pub rpc: usize,
}

/// A record read: what the vault holds for a quote hash, or why it couldn't
/// be read.
pub type RecordRead = Result<Answered<Settlement>, String>;

/// A receipt read: the receipt, `None` if the RPC didn't know the transaction,
/// or why it couldn't be read.
pub type ReceiptRead = Result<Answered<Option<Arc<PaymentReceipt>>>, String>;

/// Who is waiting on a read, which sets how long its calls may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lane {
    /// A client's store waits on it: calls are short, and a slow RPC's call
    /// goes to the next RPC as well.
    Put,
    /// Nobody waits on it: fresh replication offers, paid-list notices and
    /// listings.
    Background,
}

impl Lane {
    const COUNT: usize = 2;

    const fn index(self) -> usize {
        match self {
            Self::Put => 0,
            Self::Background => 1,
        }
    }

    const fn call_timeout(self) -> Duration {
        match self {
            Self::Put => PUT_CALL_TIMEOUT,
            Self::Background => BACKGROUND_CALL_TIMEOUT,
        }
    }

    const fn second_rpc_after(self) -> Option<Duration> {
        match self {
            Self::Put => Some(PUT_SECOND_RPC_AFTER),
            Self::Background => None,
        }
    }
}

/// What a call's answer must hold to count as one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// A `completedPayments` record: 64 bytes, the payee and the amount.
    Record,
    /// A receipt, or `null` for a transaction the RPC doesn't know.
    Receipt,
    /// A block, or `null` for a block the RPC doesn't know.
    Block,
    /// A JSON-RPC batch's answer: a list.
    Batch,
}

/// Why a call to one RPC failed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CallError {
    /// It answered busy: HTTP 429 or 503, or JSON-RPC `-32005`.
    Busy(String),
    /// It didn't answer in time.
    TimedOut,
    /// It answered a batch with anything but a list.
    BatchRefused(String),
    /// Anything else: a dropped connection, an HTTP error, an error answer,
    /// or an answer that doesn't hold what was asked for.
    Failed(String),
}

impl CallError {
    /// Whether the RPC is asked after the others for a while.
    const fn holds_off(&self) -> bool {
        matches!(self, Self::Busy(_) | Self::TimedOut)
    }
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy(why) => write!(f, "busy: {why}"),
            Self::TimedOut => write!(f, "no answer in time"),
            Self::BatchRefused(why) => write!(f, "batch refused: {why}"),
            Self::Failed(why) => write!(f, "{why}"),
        }
    }
}

/// One of the node's RPCs.
struct Rpc {
    url: reqwest::Url,
    /// Until when it is asked after the others.
    held_off_until: Mutex<Option<Instant>>,
    /// Until when reads for it go as single calls, after it refused a batch.
    batches_refused_until: Mutex<Option<Instant>>,
}

impl Rpc {
    fn new(url: reqwest::Url) -> Self {
        Self {
            url,
            held_off_until: Mutex::new(None),
            batches_refused_until: Mutex::new(None),
        }
    }

    fn held_off(&self, now: Instant) -> bool {
        self.held_off_until.lock().is_some_and(|until| until > now)
    }

    fn hold_off(&self) {
        *self.held_off_until.lock() = Some(Instant::now() + HOLD_OFF);
    }

    fn answered(&self) {
        *self.held_off_until.lock() = None;
    }

    fn refuses_batches(&self, now: Instant) -> bool {
        self.batches_refused_until
            .lock()
            .is_some_and(|until| until > now)
    }

    fn refused_a_batch(&self) {
        *self.batches_refused_until.lock() = Some(Instant::now() + HOLD_OFF);
    }
}

/// One JSON-RPC request, and how to send it.
struct Call<'a> {
    body: &'a Value,
    kind: EvmRpcCall,
    expect: Expect,
    lane: Lane,
    rounds: u32,
    /// For a read made again: the RPC that answered before. The read starts
    /// at the one after it, on a fresh connection.
    again_after: Option<usize>,
}

/// The node's RPCs, and the connections to them.
struct Pool {
    rpcs: RwLock<Arc<Vec<Rpc>>>,
    /// The RPC that answered last, by its place in the list.
    answered: AtomicUsize,
    /// Keeps connections open between calls.
    pooled: Option<reqwest::Client>,
    /// Opens a connection for each call: for reads made again, so a load
    /// balancer can't hand them to the backend that answered before.
    fresh: Option<reqwest::Client>,
}

impl Pool {
    fn new(urls: Vec<reqwest::Url>) -> Self {
        let pooled = reqwest::Client::builder()
            .build()
            .map_err(|e| crate::logging::error!("Payment reads have no HTTP client: {e}"))
            .ok();
        let fresh = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| crate::logging::error!("Payment reads have no HTTP client: {e}"))
            .ok();
        Self {
            rpcs: RwLock::new(Arc::new(urls.into_iter().map(Rpc::new).collect())),
            answered: AtomicUsize::new(0),
            pooled,
            fresh,
        }
    }

    fn rpcs(&self) -> Arc<Vec<Rpc>> {
        Arc::clone(&self.rpcs.read())
    }

    /// The RPCs in the order a read asks them: from the one that answered
    /// last, through the list, with those held off moved behind the rest.
    fn order(&self, rpcs: &[Rpc]) -> Vec<usize> {
        let now = Instant::now();
        let (ready, held): (Vec<usize>, Vec<usize>) =
            starting_at(rpcs.len(), self.answered.load(Ordering::Relaxed))
                .into_iter()
                .partition(|index| !rpcs.get(*index).is_some_and(|rpc| rpc.held_off(now)));
        ready.into_iter().chain(held).collect()
    }

    /// Whether the RPC a read would ask first refuses batches for now.
    fn first_refuses_batches(&self) -> bool {
        let rpcs = self.rpcs();
        let now = Instant::now();
        self.order(&rpcs)
            .first()
            .and_then(|index| rpcs.get(*index))
            .is_some_and(|rpc| rpc.refuses_batches(now))
    }

    /// Make `call`, in up to `call.rounds` rounds [`PAYMENT_READ_BACKOFF`]
    /// apart, until an RPC answers.
    async fn call(&self, call: &Call<'_>) -> Result<Answered<Value>, CallError> {
        let rpcs = self.rpcs();
        if rpcs.is_empty() {
            return Err(CallError::Failed(
                "no RPC to read the payment through".into(),
            ));
        }
        let client = if call.again_after.is_some() {
            self.fresh.as_ref()
        } else {
            self.pooled.as_ref()
        };
        let Some(client) = client else {
            return Err(CallError::Failed("no HTTP client".into()));
        };
        let mut last = CallError::Failed("no RPC answered".into());
        for round in 0..call.rounds.max(1) {
            if round > 0 {
                tokio::time::sleep(PAYMENT_READ_BACKOFF).await;
            }
            let order = call.again_after.map_or_else(
                || self.order(&rpcs),
                |after| starting_at(rpcs.len(), after.wrapping_add(1)),
            );
            match self.round(client, &rpcs, &order, call).await {
                Ok(answered) => return Ok(answered),
                Err(refused @ CallError::BatchRefused(_)) => return Err(refused),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// One round of `call`: each RPC in `order` in turn, the next at once when
    /// one fails, and, on a store's path, the next as well when one is slow.
    /// The first good answer counts.
    async fn round(
        &self,
        client: &reqwest::Client,
        rpcs: &[Rpc],
        order: &[usize],
        call: &Call<'_>,
    ) -> Result<Answered<Value>, CallError> {
        let start = |index: usize| async move {
            let outcome = match rpcs.get(index) {
                Some(rpc) => Self::attempt(client, rpc, call).await,
                None => Err(CallError::Failed("no such RPC".into())),
            };
            (index, outcome)
        };
        let mut pending = FuturesUnordered::new();
        // The calls in flight: each RPC's index, and when its call started.
        let mut in_flight: Vec<(usize, Instant)> = Vec::new();
        let mut next = 0;
        let mut second_at = None;
        let mut last = CallError::Failed("no RPC answered".into());
        loop {
            if pending.is_empty() {
                let Some(&index) = order.get(next) else {
                    return Err(last);
                };
                next += 1;
                pending.push(start(index));
                in_flight.push((index, Instant::now()));
                second_at = call
                    .lane
                    .second_rpc_after()
                    .map(|after| Instant::now() + after);
            }
            let more = next < order.len();
            let second = async {
                match second_at {
                    Some(at) if more => tokio::time::sleep_until(at).await,
                    _ => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                Some((index, outcome)) = pending.next() => {
                    in_flight.retain(|(flying, _)| *flying != index);
                    match outcome {
                        Ok(value) => {
                            self.answered.store(index, Ordering::Relaxed);
                            if let Some(rpc) = rpcs.get(index) {
                                rpc.answered();
                            }
                            Self::abandon(rpcs, &in_flight, call);
                            return Ok(Answered { value, rpc: index });
                        }
                        Err(CallError::BatchRefused(why)) => {
                            if let Some(rpc) = rpcs.get(index) {
                                rpc.refused_a_batch();
                            }
                            Self::abandon(rpcs, &in_flight, call);
                            return Err(CallError::BatchRefused(why));
                        }
                        Err(error) => {
                            if error.holds_off() {
                                if let Some(rpc) = rpcs.get(index) {
                                    rpc.hold_off();
                                }
                            }
                            debug!(
                                "{:?} call failed on RPC {index} of {}: {error}",
                                call.kind,
                                rpcs.len()
                            );
                            last = error;
                            // A failed call moves on to the next RPC at once.
                            if let Some(&index) = order.get(next) {
                                next += 1;
                                pending.push(start(index));
                                in_flight.push((index, Instant::now()));
                                second_at =
                                    call.lane.second_rpc_after().map(|after| Instant::now() + after);
                            }
                        }
                    }
                },
                () = second => {
                    if let Some(&index) = order.get(next) {
                        debug!(
                            "{:?} call unanswered after {:?}; asking RPC {index} as well",
                            call.kind,
                            call.lane.second_rpc_after().unwrap_or_default()
                        );
                        next += 1;
                        pending.push(start(index));
                        in_flight.push((index, Instant::now()));
                        second_at = call.lane.second_rpc_after().map(|after| Instant::now() + after);
                    }
                }
            }
        }
    }

    /// The calls a round leaves unanswered when another RPC's answer ends
    /// it, `in_flight`: those that had gone at least the lane's time before
    /// the next RPC is asked as well are counted as failed calls in the RPC
    /// traffic figures, and their RPCs held off, as a call that timed out
    /// would be. One started more recently only lost a race, and is neither.
    fn abandon(rpcs: &[Rpc], in_flight: &[(usize, Instant)], call: &Call<'_>) {
        let Some(slow) = call.lane.second_rpc_after() else {
            return;
        };
        let now = Instant::now();
        for (index, started) in in_flight {
            if now.saturating_duration_since(*started) < slow {
                continue;
            }
            if let Some(rpc) = rpcs.get(*index) {
                rpc.hold_off();
            }
            traffic::record(call.kind, false);
            debug!(
                "{:?} call left unanswered on RPC {index} of {} after {:?}",
                call.kind,
                rpcs.len(),
                now.saturating_duration_since(*started)
            );
        }
    }

    /// One call to one RPC, within the lane's time for a call. Counted in
    /// the RPC traffic figures as `call.kind`.
    async fn attempt(
        client: &reqwest::Client,
        rpc: &Rpc,
        call: &Call<'_>,
    ) -> Result<Value, CallError> {
        let sent = async {
            let response = client
                .post(rpc.url.clone())
                .json(call.body)
                .send()
                .await
                .map_err(|e| CallError::Failed(e.without_url().to_string()))?;
            let status = response.status();
            // Some RPCs answer a batch over their cap with HTTP 429: a
            // batch it refuses, whose calls go one at a time.
            if call.expect == Expect::Batch && status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(CallError::BatchRefused(format!("HTTP {status}")));
            }
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            {
                return Err(CallError::Busy(format!("HTTP {status}")));
            }
            if !status.is_success() {
                let why = format!("HTTP {status}");
                return Err(
                    if call.expect == Expect::Batch && status.is_client_error() {
                        CallError::BatchRefused(why)
                    } else {
                        CallError::Failed(why)
                    },
                );
            }
            response.json::<Value>().await.map_err(|e| {
                let undecodable = e.is_decode();
                let why = e.without_url().to_string();
                // A batch answered with anything but JSON is a batch the
                // RPC doesn't take: its calls go one at a time.
                if call.expect == Expect::Batch && undecodable {
                    CallError::BatchRefused(why)
                } else {
                    CallError::Failed(why)
                }
            })
        };
        let outcome = match tokio::time::timeout(call.lane.call_timeout(), sent).await {
            Ok(Ok(response)) => answer(response, call.expect),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(CallError::TimedOut),
        };
        // V2-834: EVM RPC is counted per call.
        traffic::record(call.kind, outcome.is_ok());
        outcome
    }
}

/// The RPCs of a list of `count`, from the one at `first` through the list.
fn starting_at(count: usize, first: usize) -> Vec<usize> {
    if count == 0 {
        return Vec::new();
    }
    (0..count).map(|offset| (first + offset) % count).collect()
}

/// What a JSON-RPC `response` answers, if it holds what was asked for.
fn answer(response: Value, expect: Expect) -> Result<Value, CallError> {
    if expect == Expect::Batch {
        return match response {
            // Every call answered busy is a busy RPC: the batch goes to the
            // next. Any other answer is taken call by call.
            Value::Array(answers)
                if !answers.is_empty()
                    && answers
                        .iter()
                        .all(|answer| answer["error"]["code"].as_i64() == Some(BUSY_CODE)) =>
            {
                Err(CallError::Busy(
                    "every call of the batch answered busy".into(),
                ))
            }
            Value::Array(_) => Ok(response),
            other => Err(match rpc_result(other) {
                Err(busy @ CallError::Busy(_)) => busy,
                Err(error) => CallError::BatchRefused(error.to_string()),
                Ok(_) => CallError::BatchRefused("a result, not a list".into()),
            }),
        };
    }
    let result = rpc_result(response)?;
    let held = match expect {
        Expect::Record => decode_record(&result).map(|_| ()),
        Expect::Receipt => {
            if result.is_null() || result["blockNumber"].is_string() {
                Ok(())
            } else {
                Err("the receipt is malformed".to_string())
            }
        }
        Expect::Block => {
            if result.is_null() || result["timestamp"].is_string() {
                Ok(())
            } else {
                Err("the block is malformed".to_string())
            }
        }
        Expect::Batch => Ok(()),
    };
    held.map(|()| result).map_err(CallError::Failed)
}

/// The result one JSON-RPC answer carries, or its error.
fn rpc_result(mut response: Value) -> Result<Value, CallError> {
    if let Some(error) = response.get("error") {
        let message = error["message"].as_str().unwrap_or("an error").to_string();
        return Err(if error["code"].as_i64() == Some(BUSY_CODE) {
            CallError::Busy(message)
        } else {
            CallError::Failed(message)
        });
    }
    response
        .get_mut("result")
        .map(Value::take)
        .ok_or_else(|| CallError::Failed("the answer has no result".into()))
}

/// A `completedPayments` record: `(bytes16 rewardsAddress, uint128 amount)`,
/// each in a 32-byte word. Anything else is no record: an answer without the
/// payee never passes for one that names the quote's.
fn decode_record(result: &Value) -> Result<Settlement, String> {
    let hex_words = result
        .as_str()
        .ok_or("the record is not a hex string")?
        .trim_start_matches("0x");
    let bytes = hex::decode(hex_words).map_err(|e| format!("the record is not hex: {e}"))?;
    let words: [u8; 64] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        format!(
            "the record is {} bytes, not the payee and the amount in 64",
            bytes.len()
        )
    })?;
    let (payee_word, amount_word) = words.split_at(32);
    let (payee, payee_padding) = payee_word.split_at(16);
    let (amount_padding, amount) = amount_word.split_at(16);
    if payee_padding
        .iter()
        .chain(amount_padding)
        .any(|byte| *byte != 0)
    {
        return Err("the record's words are not padded with zeros".into());
    }
    let mut payee_prefix = [0u8; 16];
    payee_prefix.copy_from_slice(payee);
    let mut amount_bytes = [0u8; 16];
    amount_bytes.copy_from_slice(amount);
    Ok(Settlement {
        amount: Amount::from(u128::from_be_bytes(amount_bytes)),
        payee_prefix,
    })
}

/// A JSON-RPC request object.
fn request(id: usize, method: &str, params: &Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

/// The `completedPayments(quote_hash)` call to `vault`.
fn record_request(id: usize, vault: &[u8; 20], quote_hash: &[u8; 32]) -> Value {
    request(
        id,
        "eth_call",
        &json!([
            {
                "to": format!("0x{}", hex::encode(vault)),
                "input": format!(
                    "0x{}{}",
                    hex::encode(COMPLETED_PAYMENTS_SELECTOR),
                    hex::encode(quote_hash)
                ),
            },
            "latest"
        ]),
    )
}

/// Reads in flight, by the hash they read, which later callers join while
/// they last.
struct Flights<T>(Mutex<HashMap<[u8; 32], watch::Receiver<Option<T>>>>);

/// Joining a read in flight, or starting one.
enum Joined<T> {
    /// A read of the hash was in flight: wait for it.
    Follower(watch::Receiver<Option<T>>),
    /// None was: the caller starts it, and ends it with the [`Landing`].
    Leader(Landing<T>, watch::Receiver<Option<T>>),
}

impl<T: Clone> Flights<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(HashMap::new())))
    }

    fn join(self: &Arc<Self>, key: [u8; 32]) -> Joined<T> {
        let (sender, receiver) = watch::channel(None);
        {
            let mut flights = self.0.lock();
            if let Some(joined) = flights.get(&key) {
                return Joined::Follower(joined.clone());
            }
            flights.insert(key, receiver.clone());
        }
        Joined::Leader(
            Landing {
                flights: Arc::clone(self),
                key,
                sender,
                receiver: receiver.clone(),
            },
            receiver,
        )
    }
}

/// Ends a read in flight: takes it off the list, so no later caller joins
/// it, then hands its result to those that did. Dropped without a result (its
/// task was cancelled), it still takes the read off the list, and those that
/// joined it read for themselves.
struct Landing<T> {
    flights: Arc<Flights<T>>,
    key: [u8; 32],
    sender: watch::Sender<Option<T>>,
    receiver: watch::Receiver<Option<T>>,
}

impl<T> Landing<T> {
    fn land(&self, result: T) {
        self.leave();
        let _ = self.sender.send(Some(result));
    }

    fn leave(&self) {
        let mut flights = self.flights.0.lock();
        if flights
            .get(&self.key)
            .is_some_and(|receiver| receiver.same_channel(&self.receiver))
        {
            flights.remove(&self.key);
        }
    }
}

impl<T> Drop for Landing<T> {
    fn drop(&mut self) {
        self.leave();
    }
}

/// The result of the read `receiver` waits on, or `None` if it was dropped
/// before it came back.
async fn shared<T: Clone + Send + Sync>(mut receiver: watch::Receiver<Option<T>>) -> Option<T> {
    receiver
        .wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|result| result.clone())
}

/// Record reads waiting to be sent, and whether a request of them is in
/// flight.
#[derive(Default)]
struct Queue {
    in_flight: bool,
    waiting: Vec<QueuedRead>,
}

/// A lane's queue being sent. Dropped before the sending is done, as when
/// the task sending it panics or is cancelled, it frees the lane and drops
/// the reads waiting in it: each of their callers gets "the read was
/// dropped" at once, and the next read queued is sent.
struct Sending {
    reads: Arc<SettlementReads>,
    lane: Lane,
    done: bool,
}

impl Drop for Sending {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let waiting = {
            let mut queue = self.reads.queues[self.lane.index()].lock();
            queue.in_flight = false;
            std::mem::take(&mut queue.waiting)
        };
        drop(waiting);
    }
}

/// One record read waiting to be sent.
struct QueuedRead {
    quote_hash: [u8; 32],
    reply: oneshot::Sender<RecordRead>,
}

/// The node's reads of the payment vault, for the payment check and for
/// envelopes' payments: the cache of payment facts, the receipts read for it,
/// and the pool of connections to the node's RPCs. One per node, shared by
/// the payment check and the listings (see the module's description).
pub struct SettlementReads {
    vault: [u8; 20],
    pool: Pool,
    /// Payment facts, by quote hash: only positive, settled ones.
    facts: Mutex<LruCache<[u8; 32], Settlement>>,
    /// Receipts read, by transaction hash, with only the vault's payment logs.
    receipts: Mutex<LruCache<[u8; 32], Arc<PaymentReceipt>>>,
    /// Block times read, by block number.
    blocks: Mutex<LruCache<u64, u64>>,
    /// Record reads in flight, by lane.
    record_flights: [Arc<Flights<RecordRead>>; Lane::COUNT],
    /// Receipt reads in flight, by lane.
    receipt_flights: [Arc<Flights<ReceiptRead>>; Lane::COUNT],
    /// Record reads waiting to go out, by lane.
    queues: [Mutex<Queue>; Lane::COUNT],
    /// Tests only: make the next send of queued reads panic.
    #[cfg(test)]
    panic_next_send: std::sync::atomic::AtomicBool,
}

impl SettlementReads {
    /// Reads of the vault at `vault` through `rpc_urls`, in that order.
    pub fn new(vault: [u8; 20], rpc_urls: Vec<reqwest::Url>) -> Arc<Self> {
        let capacity = |capacity: usize| NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
        Arc::new(Self {
            vault,
            pool: Pool::new(rpc_urls),
            facts: Mutex::new(LruCache::new(capacity(FACTS_CAPACITY))),
            receipts: Mutex::new(LruCache::new(capacity(RECEIPTS_CAPACITY))),
            blocks: Mutex::new(LruCache::new(capacity(BLOCKS_CAPACITY))),
            record_flights: [Flights::new(), Flights::new()],
            receipt_flights: [Flights::new(), Flights::new()],
            queues: [Mutex::new(Queue::default()), Mutex::new(Queue::default())],
            #[cfg(test)]
            panic_next_send: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Whether these reads read the same vault through the same RPCs, in the
    /// same order, as `other`.
    pub fn reads_like(&self, other: &Self) -> bool {
        let urls = |reads: &Self| -> Vec<reqwest::Url> {
            reads
                .pool
                .rpcs()
                .iter()
                .map(|rpc| rpc.url.clone())
                .collect()
        };
        self.vault == other.vault && urls(self) == urls(other)
    }

    /// Tests only: read through `urls` in place of the RPCs given at start.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_rpcs_for_tests(&self, urls: Vec<reqwest::Url>) {
        *self.pool.rpcs.write() = Arc::new(urls.into_iter().map(Rpc::new).collect());
        self.pool.answered.store(0, Ordering::Relaxed);
    }

    /// The payment fact kept for `quote_hash`, if any: a payment the vault
    /// recorded. A fact only ever passes a check; one that doesn't pay what a
    /// quote needs says nothing, and the per-quote read decides.
    pub fn fact(&self, quote_hash: &[u8; 32]) -> Option<Settlement> {
        self.facts.lock().get(quote_hash).copied()
    }

    /// Keep `settlement` as the fact for `quote_hash`, if it is a payment.
    fn remember(&self, quote_hash: [u8; 32], settlement: Settlement) {
        if settlement.is_settled() {
            self.facts.lock().put(quote_hash, settlement);
        }
    }

    /// Keep the payments the vault recorded in `receipt` as facts.
    fn remember_payments(&self, receipt: &PaymentReceipt) {
        let payments = vault_payments(receipt, &self.vault);
        if payments.is_empty() {
            return;
        }
        let mut facts = self.facts.lock();
        for payment in payments {
            facts.put(
                payment.quote_hash,
                Settlement {
                    amount: Amount::from(payment.amount),
                    payee_prefix: payment.payee_prefix(),
                },
            );
        }
    }

    /// Fill the facts from the receipts of `tx_hashes`, a payment proof's
    /// transactions: one read for each distinct one, at most
    /// [`MAX_PROOF_TXS`], all at once. A receipt that isn't there, or can't
    /// be read, adds nothing: the per-quote read decides.
    pub async fn fill_from_receipts(self: &Arc<Self>, tx_hashes: &[[u8; 32]], lane: Lane) {
        let mut seen = HashSet::new();
        let reads: Vec<_> = tx_hashes
            .iter()
            .filter(|tx_hash| seen.insert(**tx_hash))
            .take(MAX_PROOF_TXS)
            .map(|tx_hash| self.receipt(*tx_hash, lane, false))
            .collect();
        for read in futures::future::join_all(reads).await {
            if let Err(e) = read {
                debug!("A receipt read for the payment facts failed: {e}");
            }
        }
    }

    /// What the vault holds for `quote_hash`: the per-quote read, which the
    /// payment check's refusals come from.
    ///
    /// A read of the hash already in flight in `lane` is joined. If it comes
    /// back empty or failed, or is dropped before it comes back, this makes a
    /// read of its own, so it never takes on another caller's failure. Reads
    /// run on their own: one whose caller goes away still finishes, and keeps
    /// a payment it finds as a fact.
    pub async fn settlement(self: &Arc<Self>, quote_hash: [u8; 32], lane: Lane) -> RecordRead {
        let flights = &self.record_flights[lane.index()];
        let (receiver, leader) = match flights.join(quote_hash) {
            Joined::Follower(receiver) => (receiver, false),
            Joined::Leader(landing, receiver) => {
                let reads = Arc::clone(self);
                tokio::spawn(async move {
                    let read = reads.queued_record(quote_hash, lane).await;
                    landing.land(read);
                });
                (receiver, true)
            }
        };
        match (leader, shared(receiver).await) {
            (true, Some(read)) => read,
            (false, Some(Ok(answered))) if !answered.value.is_empty() => Ok(answered),
            // Joined a read that came back empty or failed, or one dropped
            // before it came back: read for ourselves.
            _ => self.queued_record(quote_hash, lane).await,
        }
    }

    /// The record of `quote_hash` read once more, after one read empty: a
    /// read of its own, skipping the cache and any read in flight, on a fresh
    /// connection, starting at the RPC after `after`, the one that answered
    /// empty.
    pub async fn settlement_again(
        &self,
        quote_hash: [u8; 32],
        lane: Lane,
        after: usize,
    ) -> RecordRead {
        let body = record_request(1, &self.vault, &quote_hash);
        let answered = self
            .pool
            .call(&Call {
                body: &body,
                kind: EvmRpcCall::CompletedPayments,
                expect: Expect::Record,
                lane,
                rounds: PAYMENT_READ_ROUNDS,
                again_after: Some(after),
            })
            .await
            .map_err(|e| e.to_string())?;
        self.settled(quote_hash, &answered)
    }

    /// The records of `quotes` read once more, after `pause`, each as
    /// [`Self::settlement_again`] reads it (a quote hash and the RPC that
    /// answered it empty), in a task of their own. The pause and the reads
    /// go on if the caller goes away, as when a PUT's deadline falls
    /// meanwhile, so a payment they find is still kept as a fact for the
    /// client's next try. The answers come back in the order of `quotes`; a
    /// `None` in `quotes` is answered `None`, with no read.
    pub fn settlements_again_later(
        self: &Arc<Self>,
        quotes: Vec<Option<([u8; 32], usize)>>,
        lane: Lane,
        pause: Duration,
    ) -> mpsc::Receiver<Option<RecordRead>> {
        let (sender, receiver) = mpsc::channel(quotes.len().max(1));
        let reads = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(pause).await;
            for quote in quotes {
                let read = match quote {
                    Some((quote_hash, after)) => {
                        Some(reads.settlement_again(quote_hash, lane, after).await)
                    }
                    None => None,
                };
                // The caller may have gone; the read is kept as a fact all
                // the same.
                let _ = sender.send(read).await;
            }
        });
        receiver
    }

    /// The record of `quote_hash` from an answer the pool took, kept as a
    /// fact if it is a payment.
    fn settled(&self, quote_hash: [u8; 32], answered: &Answered<Value>) -> RecordRead {
        let settlement = decode_record(&answered.value)?;
        self.remember(quote_hash, settlement);
        Ok(Answered {
            value: settlement,
            rpc: answered.rpc,
        })
    }

    /// Queue a read of `quote_hash`'s record in `lane`, and wait for it. A
    /// read queued while none is in flight goes at once; the others wait for
    /// the one in flight, then go together.
    async fn queued_record(self: &Arc<Self>, quote_hash: [u8; 32], lane: Lane) -> RecordRead {
        let (reply, answer) = oneshot::channel();
        let send_now = {
            let mut queue = self.queues[lane.index()].lock();
            queue.waiting.push(QueuedRead { quote_hash, reply });
            if queue.in_flight {
                None
            } else {
                queue.in_flight = true;
                Some(std::mem::take(&mut queue.waiting))
            }
        };
        if let Some(reads) = send_now {
            tokio::spawn(Arc::clone(self).send_queued(lane, reads));
        }
        answer
            .await
            .unwrap_or_else(|_| Err("the read was dropped".into()))
    }

    /// Send `reads`, then whatever queued meanwhile, until the queue is
    /// empty. If this ends any other way (a panic, or the runtime shutting
    /// down), [`Sending`] frees the lane, so later reads are still sent.
    async fn send_queued(self: Arc<Self>, lane: Lane, mut reads: Vec<QueuedRead>) {
        let mut sending = Sending {
            reads: Arc::clone(&self),
            lane,
            done: false,
        };
        loop {
            #[cfg(test)]
            assert!(
                !self.panic_next_send.swap(false, Ordering::SeqCst),
                "a send of queued reads panicked, as a test asked"
            );
            let mut batches = Vec::new();
            while !reads.is_empty() {
                let rest = reads.split_off(reads.len().min(MAX_BATCH));
                batches.push(std::mem::replace(&mut reads, rest));
            }
            futures::future::join_all(
                batches
                    .into_iter()
                    .map(|batch| self.send_reads(lane, batch)),
            )
            .await;
            let mut queue = self.queues[lane.index()].lock();
            if queue.waiting.is_empty() {
                queue.in_flight = false;
                sending.done = true;
                return;
            }
            reads = std::mem::take(&mut queue.waiting);
        }
    }

    /// Send one batch of record reads, or a single read on its own. Calls a
    /// batch's answer leaves out, or answers with anything but a record, are
    /// read again on their own, and so is every call of a batch the RPC
    /// refuses or no RPC answers in time.
    async fn send_reads(self: &Arc<Self>, lane: Lane, reads: Vec<QueuedRead>) {
        if reads.len() == 1 || self.pool.first_refuses_batches() {
            futures::future::join_all(reads.into_iter().map(|read| async move {
                let answer = self.read_alone(read.quote_hash, lane).await;
                let _ = read.reply.send(answer);
            }))
            .await;
            return;
        }
        let body = Value::Array(
            reads
                .iter()
                .enumerate()
                .map(|(id, read)| record_request(id, &self.vault, &read.quote_hash))
                .collect(),
        );
        let batch = self
            .pool
            .call(&Call {
                body: &body,
                kind: EvmRpcCall::CompletedPaymentsBatch,
                expect: Expect::Batch,
                lane,
                rounds: PAYMENT_READ_ROUNDS,
                again_after: None,
            })
            .await;
        match batch {
            Ok(answered) => {
                let mut by_id: HashMap<u64, Value> = HashMap::new();
                if let Value::Array(answers) = answered.value {
                    for answer in answers {
                        if let Some(id) = answer["id"].as_u64() {
                            by_id.insert(id, answer);
                        }
                    }
                }
                for (id, read) in reads.into_iter().enumerate() {
                    let record = u64::try_from(id)
                        .ok()
                        .and_then(|id| by_id.remove(&id))
                        .ok_or_else(|| "left out of the batch's answer".to_string())
                        .and_then(|answer| {
                            rpc_result(answer)
                                .map_err(|e| e.to_string())
                                .and_then(|result| decode_record(&result))
                        });
                    match record {
                        Ok(settlement) => {
                            self.remember(read.quote_hash, settlement);
                            let _ = read.reply.send(Ok(Answered {
                                value: settlement,
                                rpc: answered.rpc,
                            }));
                        }
                        Err(why) => {
                            debug!(
                                "A batched payment read came back without a record ({why}); \
                                 reading it on its own"
                            );
                            self.read_alone_later(read, lane);
                        }
                    }
                }
            }
            // Refused, or failed otherwise than busy or slow: its calls may
            // still be answered one at a time.
            Err(error @ (CallError::BatchRefused(_) | CallError::Failed(_))) => {
                debug!("A batch of payment reads came back with no answer ({error}); reading them one by one");
                for read in reads {
                    self.read_alone_later(read, lane);
                }
            }
            // No RPC answered the batch in time: a batch of up to
            // `MAX_BATCH` calls can take longer than one call, so each read
            // goes on its own, a few at a time, in a task of its own.
            Err(CallError::TimedOut) => {
                debug!(
                    "A batch of {} payment reads went unanswered in time; reading them one by one",
                    reads.len()
                );
                self.read_alone_later_few_at_a_time(reads, lane);
            }
            // Every RPC busy: each is held off, and one call at a time would
            // only add to its load.
            Err(error) => {
                let why = error.to_string();
                for read in reads {
                    let _ = read.reply.send(Err(why.clone()));
                }
            }
        }
    }

    /// Read each of `reads` on its own, at most
    /// [`SINGLES_AFTER_TIMED_OUT_BATCH`] at once, in a task of their own, so
    /// the queue goes on and an RPC that stalled isn't sent every call of
    /// the batch at once.
    fn read_alone_later_few_at_a_time(self: &Arc<Self>, reads: Vec<QueuedRead>, lane: Lane) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            futures::stream::iter(reads)
                .for_each_concurrent(SINGLES_AFTER_TIMED_OUT_BATCH, |read| {
                    let this = Arc::clone(&this);
                    async move {
                        let answer = this.read_alone(read.quote_hash, lane).await;
                        let _ = read.reply.send(answer);
                    }
                })
                .await;
        });
    }

    /// Read `read` on its own, in a task of its own, so the queue goes on.
    fn read_alone_later(self: &Arc<Self>, read: QueuedRead, lane: Lane) {
        let reads = Arc::clone(self);
        tokio::spawn(async move {
            let answer = reads.read_alone(read.quote_hash, lane).await;
            let _ = read.reply.send(answer);
        });
    }

    /// One `completedPayments` call on its own, through the pool.
    async fn read_alone(&self, quote_hash: [u8; 32], lane: Lane) -> RecordRead {
        let body = record_request(1, &self.vault, &quote_hash);
        let answered = self
            .pool
            .call(&Call {
                body: &body,
                kind: EvmRpcCall::CompletedPayments,
                expect: Expect::Record,
                lane,
                rounds: PAYMENT_READ_ROUNDS,
                again_after: None,
            })
            .await
            .map_err(|e| e.to_string())?;
        self.settled(quote_hash, &answered)
    }

    /// The receipt of `tx_hash`, from those kept or read: `None` if the RPC
    /// that answered didn't know the transaction. Its vault payments are kept
    /// as facts. A read of it already in flight in `lane` is joined; if that
    /// one fails or is dropped and `own_on_failure` is set, this makes a read
    /// of its own.
    pub async fn receipt(
        self: &Arc<Self>,
        tx_hash: [u8; 32],
        lane: Lane,
        own_on_failure: bool,
    ) -> ReceiptRead {
        let kept = self.receipts.lock().get(&tx_hash).cloned();
        if let Some(receipt) = kept {
            self.remember_payments(&receipt);
            return Ok(Answered {
                value: Some(receipt),
                rpc: self.pool.answered.load(Ordering::Relaxed),
            });
        }
        let flights = &self.receipt_flights[lane.index()];
        let (receiver, leader) = match flights.join(tx_hash) {
            Joined::Follower(receiver) => (receiver, false),
            Joined::Leader(landing, receiver) => {
                let reads = Arc::clone(self);
                tokio::spawn(async move {
                    let read = reads.read_receipt(tx_hash, lane, None).await;
                    landing.land(read);
                });
                (receiver, true)
            }
        };
        match shared(receiver).await {
            Some(Ok(answered)) => Ok(answered),
            Some(Err(why)) if leader || !own_on_failure => Err(why),
            None if !own_on_failure => Err("the read was dropped".into()),
            _ => self.read_receipt(tx_hash, lane, None).await,
        }
    }

    /// The receipt of `tx_hash` read once more, after an RPC didn't know the
    /// transaction: a read of its own on a fresh connection, starting at the
    /// RPC after `after`, the one that didn't know it.
    pub async fn receipt_again(&self, tx_hash: [u8; 32], lane: Lane, after: usize) -> ReceiptRead {
        self.read_receipt(tx_hash, lane, Some(after)).await
    }

    async fn read_receipt(
        &self,
        tx_hash: [u8; 32],
        lane: Lane,
        again_after: Option<usize>,
    ) -> ReceiptRead {
        let body = request(
            1,
            "eth_getTransactionReceipt",
            &json!([format!("0x{}", hex::encode(tx_hash))]),
        );
        let answered = self
            .pool
            .call(&Call {
                body: &body,
                kind: EvmRpcCall::TransactionReceipt,
                expect: Expect::Receipt,
                lane,
                rounds: 1,
                again_after,
            })
            .await
            .map_err(|e| e.to_string())?;
        if answered.value.is_null() {
            return Ok(Answered {
                value: None,
                rpc: answered.rpc,
            });
        }
        let mut receipt = parse_receipt(tx_hash, &answered.value).map_err(|e| e.to_string())?;
        // Only the vault's payment logs matter here, so only they are kept.
        receipt.logs.retain(|log| {
            log.address == self.vault && log.topics.first() == Some(&DATA_PAYMENT_MADE_TOPIC)
        });
        let receipt = Arc::new(receipt);
        self.remember_payments(&receipt);
        self.receipts.lock().put(tx_hash, Arc::clone(&receipt));
        Ok(Answered {
            value: Some(receipt),
            rpc: answered.rpc,
        })
    }

    /// The timestamp of block `number`, kept or read: `None` if the RPC
    /// doesn't know the block.
    pub async fn block_timestamp(&self, number: u64, lane: Lane) -> Result<Option<u64>, String> {
        let kept = self.blocks.lock().get(&number).copied();
        if let Some(timestamp) = kept {
            return Ok(Some(timestamp));
        }
        let body = request(1, "eth_getBlockByNumber", &json!([quantity(number), false]));
        let answered = self
            .pool
            .call(&Call {
                body: &body,
                kind: EvmRpcCall::BlockTimestamp,
                expect: Expect::Block,
                lane,
                rounds: 1,
                again_after: None,
            })
            .await
            .map_err(|e| e.to_string())?;
        if answered.value.is_null() {
            return Ok(None);
        }
        let timestamp = answered.value["timestamp"]
            .as_str()
            .ok_or("the block has no timestamp")
            .map_err(str::to_string)
            .and_then(|hex| parse_u64(hex).map_err(|e| e.to_string()))?;
        self.blocks.lock().put(number, timestamp);
        Ok(Some(timestamp))
    }
}

#[cfg(test)]
pub mod mock;
#[cfg(test)]
mod tests;

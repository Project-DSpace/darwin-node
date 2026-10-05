//! Reading payments from the payment chain over JSON-RPC.
//!
//! A client PUT names its payment transactions, so the node reads their
//! receipts ([`Chain::payment`]). Checking a listing from another node is
//! different: it shows only the fields its uploader chose, so the node finds
//! the payment from whatever it shows ([`Chain::fetch`] for an
//! [`ant_listings::Lookup`]): the receipt of a tx hash, the vault's payment
//! logs for a quote hash or in one block, or just a block's timestamp.

use crate::error::{Error, Result};
use ant_listings::{ListingPayment, Lookup, PaymentLog, PaymentReceipt, DATA_PAYMENT_MADE_TOPIC};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;
use tokio::sync::Semaphore;

/// How long one RPC call may take.
const RPC_TIMEOUT: Duration = Duration::from_secs(15);

/// The fewest blocks one `eth_getLogs` call is narrowed to when the chain
/// refuses a wider range. Below this a refusal is an error.
const MIN_LOG_RANGE: u64 = 64;

/// How many blocks one `eth_getLogs` call covers when a search for a quote's
/// payment looks back from the quote's time, unless the chain has already
/// refused a range that wide. The forwards search's own range can be a
/// single block, when the quote is dated after the latest block.
const BACK_LOG_RANGE: u64 = 10_000;

/// The most `eth_getLogs` calls one search for a quote's payment makes. A
/// search that runs out reports nothing found, and the caller tries again
/// later.
const MAX_LOG_CALLS: usize = 64;

/// The most `eth_getLogs` calls a brief search for a quote's payment makes
/// ([`Search::Brief`]).
const BRIEF_LOG_CALLS: usize = 8;

/// The most transactions read for one lookup. A quote can be paid again, by
/// anyone and any number of times; only the earliest payment counts.
const MAX_RECEIPTS: usize = 4;

/// The most block timestamps kept to narrow later searches for the block at
/// a time. When full, every other one is dropped.
const MAX_KNOWN_BLOCKS: usize = 4_096;

/// The most searches for a quote's payment by time that run at once. Each
/// can take dozens of calls, to the endpoint that also verifies every chunk
/// PUT's payment, and a node taking over a range repairs many envelopes
/// together.
const MAX_TIME_SEARCHES: usize = 2;

/// What one lookup found: each receipt with its block's timestamp, as
/// [`ant_listings::verify_listing_receipts`] takes them.
pub type Fetched = Vec<(Option<PaymentReceipt>, Option<u64>)>;

/// How far a search for a quote's payment by time goes. The quote's time is
/// whatever its signer put there, so the search can start anywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Search {
    /// Up to [`MAX_LOG_CALLS`] log queries, the first over every block
    /// searched: for a copy a quorum of the close group serves.
    Full,
    /// Up to [`BRIEF_LOG_CALLS`] log queries over at most [`BACK_LOG_RANGE`]
    /// blocks each: for a copy fewer serve, which one peer can send with a
    /// quote of its own dated in 1970. An uploader pays a quote soon after it
    /// is signed, so an honest copy's payment is still found.
    Brief,
}

impl Search {
    /// The most log queries the search makes.
    pub const fn log_calls(self) -> usize {
        match self {
            Self::Full => MAX_LOG_CALLS,
            Self::Brief => BRIEF_LOG_CALLS,
        }
    }

    /// The most blocks one log query covers.
    const fn widest(self) -> u64 {
        match self {
            Self::Full => u64::MAX,
            Self::Brief => BACK_LOG_RANGE,
        }
    }
}

/// A JSON-RPC client for the payment chain.
pub struct Chain {
    http: reqwest::Client,
    rpc_url: String,
    /// The timestamps of blocks read so far, by block number, used only to
    /// narrow searches for the block at a time.
    known_blocks: Mutex<BTreeMap<u64, u64>>,
    /// Bounds the searches for a quote's payment by time running at once.
    time_searches: Semaphore,
    /// Tests only: a mock chain answering every call in place of `rpc_url`.
    #[cfg(test)]
    mock: Mutex<Option<std::sync::Arc<Mutex<tests::MockChain>>>>,
}

/// A vault `DataPaymentMade` log, located.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VaultLog {
    tx_hash: [u8; 32],
    block_number: u64,
    log_index: u64,
}

impl Chain {
    /// A client for the chain at `rpc_url`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client can't be built.
    pub fn new(rpc_url: String) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(RPC_TIMEOUT)
            .build()
            .map_err(|e| Error::Network(format!("listings RPC client: {e}")))?;
        Ok(Self {
            http,
            rpc_url,
            known_blocks: Mutex::new(BTreeMap::new()),
            time_searches: Semaphore::new(MAX_TIME_SEARCHES),
            #[cfg(test)]
            mock: Mutex::new(None),
        })
    }

    /// Tests only: answer every call from `mock`, in this process rather
    /// than over HTTP, so that a test can run on a paused clock.
    #[cfg(test)]
    pub fn answer_from(&self, mock: std::sync::Arc<Mutex<tests::MockChain>>) {
        *self.mock.lock() = Some(mock);
    }

    /// The receipt of `tx_hash` and its block's timestamp, or `None` if the
    /// chain doesn't know the transaction (yet).
    ///
    /// # Errors
    ///
    /// Returns an error if a call fails or the answer is malformed.
    pub async fn payment(&self, tx_hash: &[u8; 32]) -> Result<Option<(PaymentReceipt, u64)>> {
        let Some(receipt) = self.receipt(tx_hash).await? else {
            return Ok(None);
        };
        let timestamp = self
            .block_timestamp(receipt.block_number)
            .await?
            .ok_or_else(|| malformed("the receipt's block is missing"))?;
        Ok(Some((receipt, timestamp)))
    }

    /// Everything the chain has for `lookup`, made for a listing showing
    /// `shown`: the receipts it locates (of the earliest [`MAX_RECEIPTS`]
    /// transactions), each with its block's timestamp, or for
    /// [`Lookup::Block`] only the block's timestamp. Empty when nothing was
    /// found.
    ///
    /// For [`Lookup::Quote`], only payments of the quote that could be the
    /// one the listing describes are read: those at its log index and in a
    /// block of its timestamp, where it shows them. Anyone can pay a quote,
    /// so others may come first. Without a block number the search starts at
    /// the block of `search_from` and runs forwards, up to the block of
    /// `not_after` (or the latest), stopping at the first range with such a
    /// payment, since the earliest payment is the one that counts; if that
    /// finds nothing, it runs backwards from `search_from`. A shown timestamp
    /// narrows the search to the blocks of that second instead. Either way it
    /// makes at most the log queries `search` allows, and at most
    /// [`MAX_TIME_SEARCHES`] such searches run at once.
    ///
    /// # Errors
    ///
    /// Returns an error if a call fails or an answer is malformed.
    pub async fn fetch(
        &self,
        lookup: &Lookup,
        shown: &ListingPayment,
        vault: &[u8; 20],
        search: Search,
    ) -> Result<Fetched> {
        match *lookup {
            Lookup::Transaction { tx_hash } => Ok(self
                .payment(&tx_hash)
                .await?
                .map(|(receipt, timestamp)| vec![(Some(receipt), Some(timestamp))])
                .unwrap_or_default()),
            Lookup::Quote {
                quote_hash,
                block_number,
                search_from,
                not_after,
            } => {
                let topics = [Some(DATA_PAYMENT_MADE_TOPIC), None, None, Some(quote_hash)];
                let logs = match block_number {
                    // One block: one timestamp, so only the log index can
                    // tell its payments apart.
                    Some(block) => at_log_index(
                        self.vault_logs(vault, &topics, block, block).await?,
                        shown.log_index,
                    ),
                    None => {
                        self.search_quote_logs(
                            vault,
                            &topics,
                            search_from,
                            not_after,
                            shown,
                            search,
                        )
                        .await?
                    }
                };
                self.receipts_for(&logs).await
            }
            Lookup::BlockLog {
                block_number,
                log_index,
            } => {
                let topics = [Some(DATA_PAYMENT_MADE_TOPIC), None, None, None];
                let logs = at_log_index(
                    self.vault_logs(vault, &topics, block_number, block_number)
                        .await?,
                    Some(log_index),
                );
                self.receipts_for(&logs).await
            }
            Lookup::Block { block_number } => {
                Ok(vec![(None, self.block_timestamp(block_number).await?)])
            }
            Lookup::Nothing => Ok(Vec::new()),
        }
    }

    /// The receipt of `tx_hash`, or `None` if the chain doesn't know it.
    async fn receipt(&self, tx_hash: &[u8; 32]) -> Result<Option<PaymentReceipt>> {
        let hash = format!("0x{}", hex::encode(tx_hash));
        let receipt = self
            .call("eth_getTransactionReceipt", json!([hash]))
            .await?;
        if receipt.is_null() {
            return Ok(None);
        }
        parse_receipt(*tx_hash, &receipt).map(Some)
    }

    /// The timestamp of block `number`, or `None` if there is no such block.
    async fn block_timestamp(&self, number: u64) -> Result<Option<u64>> {
        let block = self
            .call("eth_getBlockByNumber", json!([quantity(number), false]))
            .await?;
        if block.is_null() {
            return Ok(None);
        }
        let timestamp = block["timestamp"]
            .as_str()
            .ok_or_else(|| malformed("block has no timestamp"))
            .and_then(parse_u64)?;
        self.remember_block(number, timestamp);
        Ok(Some(timestamp))
    }

    /// Keep the timestamp of block `number` for later searches by time.
    fn remember_block(&self, number: u64, timestamp: u64) {
        let mut known = self.known_blocks.lock();
        if known.len() >= MAX_KNOWN_BLOCKS && !known.contains_key(&number) {
            // Every other one goes: the rest still spread over every time
            // searched for, a little less finely.
            let mut keep = false;
            known.retain(|_, _| {
                keep = !keep;
                keep
            });
        }
        known.insert(number, timestamp);
    }

    /// The timestamp of block `number`, known or read; an error if there is
    /// no such block.
    async fn timestamp_of(&self, number: u64) -> Result<u64> {
        let known = self.known_blocks.lock().get(&number).copied();
        match known {
            Some(timestamp) => Ok(timestamp),
            None => self
                .block_timestamp(number)
                .await?
                .ok_or_else(|| malformed("a block before the latest is missing")),
        }
    }

    /// The latest block's number.
    async fn latest_block(&self) -> Result<u64> {
        let number = self.call("eth_blockNumber", json!([])).await?;
        number
            .as_str()
            .ok_or_else(|| malformed("no block number"))
            .and_then(parse_u64)
    }

    /// The last block up to `latest` whose timestamp is at most `time`, or
    /// block 0 if there is none. The search starts between the nearest
    /// blocks already read on either side of `time`, and guesses from their
    /// timestamps where in between it is, as if blocks came at an even pace;
    /// after a guess that doesn't halve the range, the next step halves it.
    /// One call per step: a few where the chain's pace is steady or a nearby
    /// time was searched for before, at most about twice as many as halving
    /// alone where it isn't.
    async fn block_at(&self, time: u64, latest: u64) -> Result<u64> {
        let latest_time = self.timestamp_of(latest).await?;
        if latest_time <= time {
            return Ok(latest);
        }
        // The answer is in `low..high`; `high`'s timestamp is after `time`,
        // `low`'s at most `time` once known.
        let (mut low, mut low_time) = (0, None);
        let (mut high, mut high_time) = (latest, latest_time);
        for (&number, &timestamp) in self.known_blocks.lock().range(..latest) {
            if timestamp > time {
                (high, high_time) = (number, timestamp);
                break;
            }
            (low, low_time) = (number, Some(timestamp));
        }
        let mut low_time = if let Some(timestamp) = low_time {
            timestamp
        } else {
            let timestamp = self.timestamp_of(low).await?;
            if timestamp > time {
                return Ok(low);
            }
            timestamp
        };
        let mut halve = false;
        while low + 1 < high {
            let width = high - low;
            let middle = if halve {
                low + width / 2
            } else {
                guess_block(time, (low, low_time), (high, high_time))
            };
            let timestamp = self.timestamp_of(middle).await?;
            if timestamp <= time {
                (low, low_time) = (middle, timestamp);
            } else {
                (high, high_time) = (middle, timestamp);
            }
            halve = !halve && (high - low) > width / 2;
        }
        Ok(low)
    }

    /// The first and last blocks up to `latest` timed exactly `time`, or
    /// `None` if no block is.
    async fn blocks_at(&self, time: u64, latest: u64) -> Result<Option<(u64, u64)>> {
        let last = self.block_at(time, latest).await?;
        if self.timestamp_of(last).await? != time {
            return Ok(None);
        }
        let Some(before) = time.checked_sub(1) else {
            return Ok(Some((0, last)));
        };
        // Before `last`, whose timestamp is after `before`; block 0 if no
        // block is timed before `time`.
        let previous = self.block_at(before, last).await?;
        if self.timestamp_of(previous).await? > before {
            return Ok(Some((previous, last)));
        }
        Ok(Some((previous + 1, last)))
    }

    /// The payments of a quote that could be the one a listing showing
    /// `shown` describes, searched for from the block at `search_from`, or in
    /// the blocks of the shown timestamp (see [`Self::fetch`]), as far as
    /// `search` goes.
    async fn search_quote_logs(
        &self,
        vault: &[u8; 20],
        topics: &[Option<[u8; 32]>; 4],
        search_from: u64,
        not_after: Option<u64>,
        shown: &ListingPayment,
        search: Search,
    ) -> Result<Vec<VaultLog>> {
        let _searching = self
            .time_searches
            .acquire()
            .await
            .map_err(|e| Error::Network(format!("payment chain search: {e}")))?;
        let latest = self.latest_block().await?;
        let (first, last) = if let Some(time) = shown.block_timestamp {
            let Some(blocks) = self.blocks_at(time, latest).await? else {
                return Ok(Vec::new());
            };
            blocks
        } else {
            let last = match not_after {
                Some(time) => self.block_at(time, latest).await?,
                None => latest,
            };
            (self.block_at(search_from, last).await?, last)
        };
        let mut budget = search.log_calls();
        let mut range = (last - first + 1).min(search.widest());
        let mut narrowed = false;
        // Forwards from the quote's time.
        let mut from = first;
        while from <= last && budget > 0 {
            let to = from.saturating_add(range - 1).min(last);
            budget -= 1;
            match self.vault_logs(vault, topics, from, to).await {
                Ok(logs) => {
                    let logs = at_log_index(logs, shown.log_index);
                    if !logs.is_empty() {
                        return Ok(logs);
                    }
                    match to.checked_add(1) {
                        Some(next) => from = next,
                        None => break,
                    }
                }
                Err(e) if range > MIN_LOG_RANGE => {
                    crate::logging::debug!("Narrowing a log search after: {e}");
                    range = (range / 2).max(MIN_LOG_RANGE);
                    narrowed = true;
                }
                Err(e) => return Err(e),
            }
        }
        if shown.block_timestamp.is_some() {
            // The payment is in one of those blocks, or not on chain.
            return Ok(Vec::new());
        }
        // The quote's time is its signer's word: look back from it too, as
        // widely as the chain allows.
        if !narrowed {
            range = range.max(BACK_LOG_RANGE);
        }
        let mut to = first;
        while to > 0 && budget > 0 {
            let to_block = to - 1;
            let from_block = to_block.saturating_sub(range - 1);
            budget -= 1;
            match self.vault_logs(vault, topics, from_block, to_block).await {
                Ok(logs) => {
                    let logs = at_log_index(logs, shown.log_index);
                    if !logs.is_empty() {
                        return Ok(logs);
                    }
                    to = from_block;
                }
                Err(e) if range > MIN_LOG_RANGE => {
                    crate::logging::debug!("Narrowing a log search after: {e}");
                    range = (range / 2).max(MIN_LOG_RANGE);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Vec::new())
    }

    /// The vault's logs matching `topics` (`None` = any) in blocks
    /// `from..=to`.
    async fn vault_logs(
        &self,
        vault: &[u8; 20],
        topics: &[Option<[u8; 32]>; 4],
        from: u64,
        to: u64,
    ) -> Result<Vec<VaultLog>> {
        let topics: Vec<Value> = topics
            .iter()
            .map(|topic| {
                topic.as_ref().map_or(Value::Null, |topic| {
                    json!(format!("0x{}", hex::encode(topic)))
                })
            })
            .collect();
        let filter = json!({
            "address": format!("0x{}", hex::encode(vault)),
            "fromBlock": quantity(from),
            "toBlock": quantity(to),
            "topics": topics,
        });
        let logs = self.call("eth_getLogs", json!([filter])).await?;
        let logs = logs
            .as_array()
            .ok_or_else(|| malformed("logs are not a list"))?;
        logs.iter()
            .map(|log| parse_vault_log(log).ok_or_else(|| malformed("a log is incomplete")))
            .collect()
    }

    /// The receipt of each of the first [`MAX_RECEIPTS`] transactions among
    /// `logs`, in chain order, once, with its block's timestamp. Transactions
    /// the chain no longer knows are left out.
    async fn receipts_for(&self, logs: &[VaultLog]) -> Result<Fetched> {
        let mut logs = logs.to_vec();
        logs.sort_by_key(|log| (log.block_number, log.log_index));
        let mut seen = HashSet::new();
        let tx_hashes: Vec<[u8; 32]> = logs
            .iter()
            .map(|log| log.tx_hash)
            .filter(|tx_hash| seen.insert(*tx_hash))
            .take(MAX_RECEIPTS)
            .collect();
        let mut fetched = Vec::with_capacity(tx_hashes.len());
        for tx_hash in tx_hashes {
            if let Some((receipt, timestamp)) = self.payment(&tx_hash).await? {
                fetched.push((Some(receipt), Some(timestamp)));
            }
        }
        Ok(fetched)
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        #[cfg(test)]
        {
            let mock = self.mock.lock().clone();
            if let Some(mock) = mock {
                return rpc_result(method, &mock.lock().answer(method, &params));
            }
        }
        let response: Value = self
            .http
            .post(&self.rpc_url)
            .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| Error::Network(format!("payment chain {method}: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Network(format!("payment chain {method}: {e}")))?;
        rpc_result(method, &response)
    }
}

/// The result a JSON-RPC `response` to `method` carries, or its error.
fn rpc_result(method: &str, response: &Value) -> Result<Value> {
    if let Some(error) = response.get("error") {
        return Err(Error::Network(format!("payment chain {method}: {error}")));
    }
    Ok(response.get("result").cloned().unwrap_or(Value::Null))
}

/// The logs at `log_index`, or all of them if it is hidden. A listing that
/// shows its log index describes no payment elsewhere.
fn at_log_index(logs: Vec<VaultLog>, log_index: Option<u64>) -> Vec<VaultLog> {
    match log_index {
        Some(index) => logs
            .into_iter()
            .filter(|log| log.log_index == index)
            .collect(),
        None => logs,
    }
}

/// Where between blocks `low` and `high` the last block timed at most `time`
/// should be, from their timestamps, if blocks came at an even pace: a block
/// strictly between them. `low`'s timestamp is at most `time`, `high`'s after
/// it, and at least one block lies between them.
fn guess_block(time: u64, (low, low_time): (u64, u64), (high, high_time): (u64, u64)) -> u64 {
    let blocks = u128::from(high - low);
    let elapsed = u128::from(time.saturating_sub(low_time));
    let span = u128::from(high_time.saturating_sub(low_time)).max(1);
    let offset = u64::try_from(elapsed * blocks / span).unwrap_or(u64::MAX);
    low.saturating_add(offset).clamp(low + 1, high - 1)
}

/// A JSON-RPC receipt for `tx_hash`.
fn parse_receipt(tx_hash: [u8; 32], receipt: &Value) -> Result<PaymentReceipt> {
    let block_number = receipt["blockNumber"]
        .as_str()
        .ok_or_else(|| malformed("receipt has no block number"))
        .and_then(parse_u64)?;
    Ok(PaymentReceipt {
        tx_hash,
        success: receipt["status"].as_str() == Some("0x1"),
        block_number,
        logs: receipt["logs"]
            .as_array()
            .map(|logs| logs.iter().filter_map(parse_log).collect())
            .unwrap_or_default(),
    })
}

/// One receipt log, or `None` if it is malformed.
fn parse_log(log: &Value) -> Option<PaymentLog> {
    Some(PaymentLog {
        address: parse_bytes(log["address"].as_str()?)?,
        topics: log["topics"]
            .as_array()?
            .iter()
            .map(|topic| topic.as_str().and_then(parse_bytes))
            .collect::<Option<Vec<[u8; 32]>>>()?,
        log_index: parse_u64(log["logIndex"].as_str()?).ok()?,
    })
}

/// Where an `eth_getLogs` log is, or `None` if it is incomplete (a pending
/// log has no block).
fn parse_vault_log(log: &Value) -> Option<VaultLog> {
    Some(VaultLog {
        tx_hash: parse_bytes(log["transactionHash"].as_str()?)?,
        block_number: parse_u64(log["blockNumber"].as_str()?).ok()?,
        log_index: parse_u64(log["logIndex"].as_str()?).ok()?,
    })
}

fn quantity(number: u64) -> String {
    format!("0x{number:x}")
}

fn parse_u64(hex: &str) -> Result<u64> {
    u64::from_str_radix(hex.trim_start_matches("0x"), 16)
        .map_err(|e| malformed(&format!("bad quantity {hex}: {e}")))
}

fn parse_bytes<const N: usize>(hex: &str) -> Option<[u8; N]> {
    hex::decode(hex.trim_start_matches("0x"))
        .ok()?
        .try_into()
        .ok()
}

fn malformed(what: &str) -> Error {
    Error::Network(format!("payment chain answer malformed: {what}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    pub const VAULT: [u8; 20] = [0x69; 20];

    /// Which calls a mock chain fails, by method and parameters.
    pub type FailingCalls = Box<dyn Fn(&str, &Value) -> bool + Send>;

    /// A log of the mock chain's vault.
    #[derive(Debug, Clone)]
    pub struct MockLog {
        pub tx_hash: [u8; 32],
        pub block_number: u64,
        pub log: PaymentLog,
    }

    /// A chain to test against: blocks with timestamps, receipts and vault
    /// logs, served over JSON-RPC on a local port, or answering a [`Chain`]
    /// in process ([`Chain::answer_from`]).
    #[derive(Default)]
    pub struct MockChain {
        /// Block number to timestamp; the last is the latest block.
        pub blocks: BTreeMap<u64, u64>,
        pub receipts: Vec<PaymentReceipt>,
        pub logs: Vec<MockLog>,
        /// `eth_getLogs` refuses ranges wider than this.
        pub max_log_range: Option<u64>,
        /// The calls this says fail, by method and parameters, with an
        /// error as an overloaded provider's.
        pub fails: Option<FailingCalls>,
        /// Every call made, by method.
        pub calls: Vec<String>,
        /// The first and last block of every `eth_getLogs` call made.
        pub log_ranges: Vec<(u64, u64)>,
    }

    impl MockChain {
        /// A receipt, with its logs indexed as vault logs too.
        pub fn add_receipt(&mut self, receipt: PaymentReceipt) {
            for log in &receipt.logs {
                self.logs.push(MockLog {
                    tx_hash: receipt.tx_hash,
                    block_number: receipt.block_number,
                    log: log.clone(),
                });
            }
            self.receipts.push(receipt);
        }

        /// The JSON-RPC response to a call of `method` with `params`.
        pub fn answer(&mut self, method: &str, params: &Value) -> Value {
            self.calls.push(method.to_string());
            if self
                .fails
                .as_ref()
                .is_some_and(|fails| fails(method, params))
            {
                return json!({ "jsonrpc": "2.0", "id": 1,
                    "error": { "code": -32005, "message": "rate limited" } });
            }
            let result = match method {
                "eth_blockNumber" => json!(quantity(*self.blocks.keys().last().unwrap())),
                "eth_getBlockByNumber" => {
                    let number = parse_u64(params[0].as_str().unwrap()).unwrap();
                    self.blocks
                        .get(&number)
                        .map_or(Value::Null, |time| json!({ "timestamp": quantity(*time) }))
                }
                "eth_getTransactionReceipt" => {
                    let hash: [u8; 32] = parse_bytes(params[0].as_str().unwrap()).unwrap();
                    self.receipts
                        .iter()
                        .find(|r| r.tx_hash == hash)
                        .map_or(Value::Null, receipt_json)
                }
                "eth_getLogs" => {
                    let filter = &params[0];
                    let from = parse_u64(filter["fromBlock"].as_str().unwrap()).unwrap();
                    let to = parse_u64(filter["toBlock"].as_str().unwrap()).unwrap();
                    self.log_ranges.push((from, to));
                    if self.max_log_range.is_some_and(|max| to - from + 1 > max) {
                        return json!({ "jsonrpc": "2.0", "id": 1,
                            "error": { "code": -32005, "message": "range too large" } });
                    }
                    let address: [u8; 20] =
                        parse_bytes(filter["address"].as_str().unwrap()).unwrap();
                    let topics: Vec<Option<[u8; 32]>> = filter["topics"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|t| t.as_str().and_then(parse_bytes))
                        .collect();
                    let matching: Vec<Value> = self
                        .logs
                        .iter()
                        .filter(|l| {
                            l.log.address == address
                                && (from..=to).contains(&l.block_number)
                                && topics.iter().enumerate().all(|(i, topic)| {
                                    topic.is_none_or(|t| l.log.topics.get(i) == Some(&t))
                                })
                        })
                        .map(|l| {
                            json!({
                                "transactionHash": format!("0x{}", hex::encode(l.tx_hash)),
                                "blockNumber": quantity(l.block_number),
                                "logIndex": quantity(l.log.log_index),
                            })
                        })
                        .collect();
                    json!(matching)
                }
                _ => {
                    return json!({ "jsonrpc": "2.0", "id": 1,
                        "error": { "code": -32601, "message": "no such method" } })
                }
            };
            json!({ "jsonrpc": "2.0", "id": 1, "result": result })
        }

        /// Serve this chain; returns its URL.
        pub async fn serve(chain: Arc<Mutex<Self>>) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move {
                while let Ok((mut socket, _)) = listener.accept().await {
                    let chain = Arc::clone(&chain);
                    tokio::spawn(async move {
                        while let Some(body) = read_request(&mut socket).await {
                            let request: Value = serde_json::from_slice(&body).unwrap();
                            let response = chain
                                .lock()
                                .answer(request["method"].as_str().unwrap(), &request["params"]);
                            let body = response.to_string();
                            let reply = format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                                 content-length: {}\r\n\r\n{body}",
                                body.len()
                            );
                            if socket.write_all(reply.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                    });
                }
            });
            url
        }
    }

    /// One HTTP request's body, or `None` when the connection closes.
    async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
            let read = socket.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            buffer.extend_from_slice(&chunk[..read]);
        };
        let headers = String::from_utf8_lossy(&buffer[..header_end]).to_lowercase();
        let length: usize = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .map_or(0, |value| value.trim().parse().unwrap());
        while buffer.len() < header_end + length {
            let read = socket.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            buffer.extend_from_slice(&chunk[..read]);
        }
        Some(buffer[header_end..header_end + length].to_vec())
    }

    fn receipt_json(receipt: &PaymentReceipt) -> Value {
        json!({
            "status": if receipt.success { "0x1" } else { "0x0" },
            "blockNumber": quantity(receipt.block_number),
            "logs": receipt.logs.iter().map(|log| json!({
                "address": format!("0x{}", hex::encode(log.address)),
                "topics": log.topics.iter().map(|t| format!("0x{}", hex::encode(t))).collect::<Vec<_>>(),
                "logIndex": quantity(log.log_index),
            })).collect::<Vec<_>>(),
        })
    }

    /// A vault payment log for `quote_hash` at `log_index`.
    pub fn vault_log(quote_hash: [u8; 32], log_index: u64) -> PaymentLog {
        let mut rewards = [0u8; 32];
        rewards[12..].copy_from_slice(&[0xab; 20]);
        let mut amount = [0u8; 32];
        amount[31] = 87;
        PaymentLog {
            address: VAULT,
            topics: vec![DATA_PAYMENT_MADE_TOPIC, rewards, amount, quote_hash],
            log_index,
        }
    }

    /// A chain of `blocks` blocks, block `n` at time `1_790_000_000 + 2n`,
    /// with a payment of `[q; 32]` in tx `[q; 32]` for each `(block, q)`.
    pub fn chain_with(blocks: u64, payments: &[(u64, u8)]) -> MockChain {
        let mut chain = MockChain {
            blocks: (0..blocks).map(|n| (n, 1_790_000_000 + 2 * n)).collect(),
            ..MockChain::default()
        };
        for &(block, q) in payments {
            chain.add_receipt(PaymentReceipt {
                tx_hash: [q; 32],
                success: true,
                block_number: block,
                logs: vec![vault_log([q; 32], 4)],
            });
        }
        chain
    }

    async fn client(chain: MockChain) -> (Chain, Arc<Mutex<MockChain>>) {
        let chain = Arc::new(Mutex::new(chain));
        let url = MockChain::serve(Arc::clone(&chain)).await;
        (Chain::new(url).unwrap(), chain)
    }

    #[test]
    fn parses_a_vault_log() {
        let log = json!({
            "address": "0x6969696969696969696969696969696969696969",
            "topics": [
                "0xf998960b1c6f0e0e89b7bbe6b6fbf3e03e6f08eee5b8430877d8adb8e149d580",
                "0x000000000000000000000000abababababababababababababababababababab",
                "0x0000000000000000000000000000000000000000000000000000000000000057",
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ],
            "logIndex": "0x1f"
        });
        let parsed = parse_log(&log).unwrap();
        assert_eq!(parsed.log_index, 31);
        assert_eq!(parsed.topics.len(), 4);
        assert_eq!(parsed.topics[0], ant_listings::DATA_PAYMENT_MADE_TOPIC);
        assert_eq!(parsed.address[0], 0x69);
    }

    #[test]
    fn skips_malformed_logs() {
        assert!(
            parse_log(&json!({ "address": "0x12", "topics": [], "logIndex": "0x0" })).is_none()
        );
        assert!(parse_log(&json!({ "topics": [] })).is_none());
        assert!(parse_vault_log(&json!({ "transactionHash": "0x12" })).is_none());
    }

    #[tokio::test]
    async fn fetches_by_transaction() {
        let (chain, _) = client(chain_with(10, &[(4, 1)])).await;
        let found = chain
            .fetch(
                &Lookup::Transaction { tx_hash: [1; 32] },
                &ListingPayment::default(),
                &VAULT,
                Search::Full,
            )
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        let (receipt, timestamp) = &found[0];
        assert_eq!(receipt.as_ref().unwrap().block_number, 4);
        assert_eq!(*timestamp, Some(1_790_000_008));
        let missing = chain
            .fetch(
                &Lookup::Transaction { tx_hash: [2; 32] },
                &ListingPayment::default(),
                &VAULT,
                Search::Full,
            )
            .await
            .unwrap();
        assert_eq!(missing, Fetched::new());
    }

    #[tokio::test]
    async fn fetches_a_block_log() {
        let (chain, _) = client(chain_with(10, &[(4, 1), (4, 2)])).await;
        // Both pay at log index 4 of block 4 in this mock; pick by index.
        let found = chain
            .fetch(
                &Lookup::BlockLog {
                    block_number: 4,
                    log_index: 4,
                },
                &ListingPayment::default(),
                &VAULT,
                Search::Full,
            )
            .await
            .unwrap();
        assert_eq!(found.len(), 2);
        let none = chain
            .fetch(
                &Lookup::BlockLog {
                    block_number: 4,
                    log_index: 5,
                },
                &ListingPayment::default(),
                &VAULT,
                Search::Full,
            )
            .await
            .unwrap();
        assert_eq!(none, Fetched::new());
    }

    #[tokio::test]
    async fn fetches_a_block_timestamp() {
        let (chain, _) = client(chain_with(10, &[])).await;
        assert_eq!(
            chain
                .fetch(
                    &Lookup::Block { block_number: 3 },
                    &ListingPayment::default(),
                    &VAULT,
                    Search::Full
                )
                .await
                .unwrap(),
            vec![(None, Some(1_790_000_006))]
        );
        assert_eq!(
            chain
                .fetch(
                    &Lookup::Block { block_number: 99 },
                    &ListingPayment::default(),
                    &VAULT,
                    Search::Full
                )
                .await
                .unwrap(),
            vec![(None, None)]
        );
        assert_eq!(
            chain
                .fetch(
                    &Lookup::Nothing,
                    &ListingPayment::default(),
                    &VAULT,
                    Search::Full
                )
                .await
                .unwrap(),
            Fetched::new()
        );
    }

    #[tokio::test]
    async fn finds_a_quote_payment_in_its_block_or_by_time() {
        let (chain, mock) = client(chain_with(1000, &[(700, 5)])).await;
        let by_block = chain
            .fetch(
                &Lookup::Quote {
                    quote_hash: [5; 32],
                    block_number: Some(700),
                    search_from: 0,
                    not_after: None,
                },
                &ListingPayment::default(),
                &VAULT,
                Search::Full,
            )
            .await
            .unwrap();
        assert_eq!(by_block.len(), 1);
        let wrong_block = chain
            .fetch(
                &Lookup::Quote {
                    quote_hash: [5; 32],
                    block_number: Some(701),
                    search_from: 0,
                    not_after: None,
                },
                &ListingPayment::default(),
                &VAULT,
                Search::Full,
            )
            .await
            .unwrap();
        assert_eq!(wrong_block, Fetched::new());

        // By time, with a provider that caps log ranges at 100 blocks.
        mock.lock().max_log_range = Some(100);
        let by_time = chain
            .fetch(
                &Lookup::Quote {
                    quote_hash: [5; 32],
                    block_number: None,
                    search_from: 1_790_000_000 + 2 * 650,
                    not_after: None,
                },
                &ListingPayment::default(),
                &VAULT,
                Search::Full,
            )
            .await
            .unwrap();
        assert_eq!(by_time.len(), 1);
        assert_eq!(by_time[0].0.as_ref().unwrap().block_number, 700);

        // A quote dated after its payment is still found, looking back.
        let backwards = chain
            .fetch(
                &Lookup::Quote {
                    quote_hash: [5; 32],
                    block_number: None,
                    search_from: 1_790_000_000 + 2 * 900,
                    not_after: None,
                },
                &ListingPayment::default(),
                &VAULT,
                Search::Full,
            )
            .await
            .unwrap();
        assert_eq!(backwards.len(), 1);

        // Not paid at all: nothing, within the call budget.
        mock.lock().calls.clear();
        let unpaid = chain
            .fetch(
                &Lookup::Quote {
                    quote_hash: [6; 32],
                    block_number: None,
                    search_from: 1_790_000_000,
                    not_after: Some(1_790_000_000 + 2 * 999),
                },
                &ListingPayment::default(),
                &VAULT,
                Search::Full,
            )
            .await
            .unwrap();
        assert_eq!(unpaid, Fetched::new());
        assert!(log_calls(&mock) <= MAX_LOG_CALLS);
    }

    #[tokio::test]
    async fn finds_a_quote_dated_after_the_latest_block() {
        // Paid a few hundred blocks before the latest, with a provider that
        // caps log ranges at 100 blocks. Forwards from the quote's time is a
        // single block; looking back reaches the payment within the budget.
        let (chain, mock) = client(chain_with(1000, &[(700, 5)])).await;
        mock.lock().max_log_range = Some(100);
        let lookup = Lookup::Quote {
            quote_hash: [5; 32],
            block_number: None,
            search_from: 1_790_000_000 + 2 * 5000,
            not_after: None,
        };
        let found = chain
            .fetch(&lookup, &ListingPayment::default(), &VAULT, Search::Full)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0.as_ref().unwrap().block_number, 700);
        assert!(log_calls(&mock) <= MAX_LOG_CALLS);

        // Where the chain takes wide ranges, in one call back.
        mock.lock().max_log_range = None;
        mock.lock().calls.clear();
        let found = chain
            .fetch(&lookup, &ListingPayment::default(), &VAULT, Search::Full)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(log_calls(&mock), 2);
    }

    /// How many `eth_getLogs` calls the mock chain has answered.
    fn log_calls(mock: &Mutex<MockChain>) -> usize {
        mock.lock()
            .calls
            .iter()
            .filter(|c| *c == "eth_getLogs")
            .count()
    }

    /// The most blocks an `eth_getLogs` call has asked the mock chain for
    /// since this was last asked.
    fn widest_log_range(mock: &Mutex<MockChain>) -> Option<u64> {
        let ranges = std::mem::take(&mut mock.lock().log_ranges);
        ranges.iter().map(|(from, to)| to - from + 1).max()
    }

    #[tokio::test]
    async fn a_brief_search_reads_a_few_blocks_at_a_time() {
        // A quote dated 1970, so the search runs from block 0, paid in block
        // 29,000 of 30,000; a provider that takes any range.
        let (chain, mock) = client(chain_with(30_000, &[(29_000, 5)])).await;
        let from_1970 = Lookup::Quote {
            quote_hash: [5; 32],
            block_number: None,
            search_from: 0,
            not_after: None,
        };
        let shown = ListingPayment::default();

        // In full: every block in one query.
        let found = chain.fetch(&from_1970, &shown, &VAULT, Search::Full);
        assert_eq!(found.await.unwrap().len(), 1);
        assert_eq!(widest_log_range(&mock), Some(30_000));
        // Briefly: no more than BACK_LOG_RANGE blocks a query.
        mock.lock().calls.clear();
        let found = chain.fetch(&from_1970, &shown, &VAULT, Search::Brief);
        assert_eq!(found.await.unwrap().len(), 1);
        assert_eq!(log_calls(&mock), 3);
        assert_eq!(widest_log_range(&mock), Some(BACK_LOG_RANGE));

        // Paid further on than its few queries reach: not found briefly,
        // only in full.
        let (chain, mock) = client(chain_with(100_000, &[(99_000, 5)])).await;
        let found = chain.fetch(&from_1970, &shown, &VAULT, Search::Brief);
        assert_eq!(found.await.unwrap(), Fetched::new());
        assert_eq!(log_calls(&mock), BRIEF_LOG_CALLS);
        assert_eq!(widest_log_range(&mock), Some(BACK_LOG_RANGE));
        let found = chain.fetch(&from_1970, &shown, &VAULT, Search::Full);
        assert_eq!(found.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn reads_only_the_earliest_payments_of_a_quote() {
        // Paid ten times, in later blocks by smaller tx hashes.
        let mut mock = chain_with(1000, &[]);
        for i in 0..10u8 {
            mock.add_receipt(PaymentReceipt {
                tx_hash: [100 - i; 32],
                success: true,
                block_number: 500 + u64::from(i),
                logs: vec![vault_log([5; 32], 4)],
            });
        }
        let (chain, mock) = client(mock).await;
        let found = chain
            .fetch(
                &Lookup::Quote {
                    quote_hash: [5; 32],
                    block_number: None,
                    search_from: 1_790_000_000 + 2 * 450,
                    not_after: None,
                },
                &ListingPayment::default(),
                &VAULT,
                Search::Full,
            )
            .await
            .unwrap();
        let blocks: Vec<u64> = found
            .iter()
            .map(|(receipt, _)| receipt.as_ref().unwrap().block_number)
            .collect();
        assert_eq!(blocks, vec![500, 501, 502, 503]);
        let receipts = mock
            .lock()
            .calls
            .iter()
            .filter(|c| *c == "eth_getTransactionReceipt")
            .count();
        assert_eq!(receipts, MAX_RECEIPTS);
    }

    #[tokio::test]
    async fn reads_only_the_payments_of_a_quote_a_listing_can_describe() {
        // Paid four times at another log index, then at log index 4 in
        // block 600, as by a quoting node paying its own quote first.
        let mut mock = chain_with(1000, &[]);
        for i in 0..4u8 {
            mock.add_receipt(PaymentReceipt {
                tx_hash: [100 + i; 32],
                success: true,
                block_number: 500 + u64::from(i),
                logs: vec![vault_log([5; 32], 9)],
            });
        }
        mock.add_receipt(PaymentReceipt {
            tx_hash: [5; 32],
            success: true,
            block_number: 600,
            logs: vec![vault_log([5; 32], 4)],
        });
        let (chain, mock) = client(mock).await;
        let lookup = Lookup::Quote {
            quote_hash: [5; 32],
            block_number: None,
            search_from: 1_790_000_000 + 2 * 450,
            not_after: None,
        };
        let blocks = |found: Fetched| -> Vec<u64> {
            found
                .iter()
                .map(|(receipt, _)| receipt.as_ref().unwrap().block_number)
                .collect()
        };

        // Nothing shown to tell them apart: the earliest.
        let hidden = ListingPayment::default();
        let found = chain
            .fetch(&lookup, &hidden, &VAULT, Search::Full)
            .await
            .unwrap();
        assert_eq!(blocks(found), vec![500, 501, 502, 503]);

        // The log index shown: only the payment there, also where the
        // provider caps log ranges and the first range holds only others.
        let at_index = ListingPayment {
            log_index: Some(4),
            ..ListingPayment::default()
        };
        let found = chain
            .fetch(&lookup, &at_index, &VAULT, Search::Full)
            .await
            .unwrap();
        assert_eq!(blocks(found), vec![600]);
        mock.lock().max_log_range = Some(64);
        let found = chain
            .fetch(&lookup, &at_index, &VAULT, Search::Full)
            .await
            .unwrap();
        assert_eq!(blocks(found), vec![600]);
        mock.lock().max_log_range = None;

        // The timestamp shown: only the blocks of that second, in one call.
        mock.lock().calls.clear();
        let at_time = ListingPayment {
            block_timestamp: Some(1_790_000_000 + 2 * 600),
            ..ListingPayment::default()
        };
        let found = chain
            .fetch(&lookup, &at_time, &VAULT, Search::Full)
            .await
            .unwrap();
        assert_eq!(blocks(found), vec![600]);
        assert_eq!(log_calls(&mock), 1);
        // A second no block has: nothing, and no log searched.
        mock.lock().calls.clear();
        let between = ListingPayment {
            block_timestamp: Some(1_790_000_000 + 2 * 600 + 1),
            ..ListingPayment::default()
        };
        assert_eq!(
            chain
                .fetch(&lookup, &between, &VAULT, Search::Full)
                .await
                .unwrap(),
            Fetched::new()
        );
        assert_eq!(log_calls(&mock), 0);

        // In a block given, the log index still tells them apart.
        let in_block = Lookup::Quote {
            quote_hash: [5; 32],
            block_number: Some(500),
            search_from: 0,
            not_after: None,
        };
        assert_eq!(
            chain
                .fetch(&in_block, &at_index, &VAULT, Search::Full)
                .await
                .unwrap(),
            Fetched::new()
        );
        let at_nine = ListingPayment {
            log_index: Some(9),
            ..ListingPayment::default()
        };
        let found = chain
            .fetch(&in_block, &at_nine, &VAULT, Search::Full)
            .await
            .unwrap();
        assert_eq!(blocks(found), vec![500]);
    }

    #[tokio::test]
    async fn finds_the_blocks_of_a_second() {
        // Several blocks share most seconds.
        let (chain, mock) = client(uneven_chain(2_000, 500)).await;
        let blocks = mock.lock().blocks.clone();
        let by_hand = |time: u64, latest: u64| {
            let numbers: Vec<u64> = blocks
                .range(..=latest)
                .filter(|(_, timestamp)| **timestamp == time)
                .map(|(number, _)| *number)
                .collect();
            Some((*numbers.first()?, *numbers.last()?))
        };
        let mut times: Vec<u64> = blocks.values().copied().step_by(37).collect();
        times.extend([0, blocks[&0], blocks[&1_234], blocks[&1_999]]);
        for latest in [1_999, 1_234] {
            for &time in &times {
                for time in [time.saturating_sub(1), time, time + 1] {
                    assert_eq!(
                        chain.blocks_at(time, latest).await.unwrap(),
                        by_hand(time, latest),
                        "time {time}, latest {latest}"
                    );
                }
            }
        }
        assert!(blocks.values().collect::<HashSet<_>>().len() < blocks.len());
    }

    #[tokio::test]
    async fn finds_the_block_at_a_time() {
        let (chain, _) = client(chain_with(1000, &[])).await;
        assert_eq!(
            chain.block_at(1_790_000_000 + 2 * 321, 999).await.unwrap(),
            321
        );
        assert_eq!(
            chain
                .block_at(1_790_000_000 + 2 * 321 + 1, 999)
                .await
                .unwrap(),
            321
        );
        assert_eq!(chain.block_at(0, 999).await.unwrap(), 0);
        assert_eq!(chain.block_at(u64::MAX, 999).await.unwrap(), 999);
    }

    /// A chain of `blocks` blocks whose times advance unevenly, as blocks
    /// made on demand do: 0 to 3 seconds apart, so several share a second,
    /// with a quiet hour or more about once in `pause_one_in` blocks.
    fn uneven_chain(blocks: u64, pause_one_in: u64) -> MockChain {
        let mut seed = 0x2545_f491_4f6c_dd1d_u64;
        let mut time = 1_790_000_000;
        let blocks = (0..blocks)
            .map(|number| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                time += if seed.is_multiple_of(pause_one_in) {
                    3_600 + seed % 7_200
                } else {
                    seed % 4
                };
                (number, time)
            })
            .collect();
        MockChain {
            blocks,
            ..MockChain::default()
        }
    }

    /// The last block up to `latest` timed at most `time`, or block 0.
    fn block_at_by_hand(blocks: &BTreeMap<u64, u64>, time: u64, latest: u64) -> u64 {
        blocks
            .range(..=latest)
            .rev()
            .find(|(_, timestamp)| **timestamp <= time)
            .map_or(0, |(number, _)| *number)
    }

    /// How many blocks the mock chain has been asked for.
    fn block_calls(mock: &Mutex<MockChain>) -> usize {
        mock.lock()
            .calls
            .iter()
            .filter(|c| *c == "eth_getBlockByNumber")
            .count()
    }

    #[tokio::test]
    async fn finds_the_block_at_any_time_on_an_uneven_chain() {
        // Quiet hours every few hundred blocks: guesses from the pace of
        // the chain are poor, and the search mostly halves the range.
        let (chain, mock) = client(uneven_chain(20_000, 500)).await;
        let blocks = mock.lock().blocks.clone();
        let (first, last) = (blocks[&0], blocks[&19_999]);
        let mut times: Vec<u64> = (0..=40).map(|i| first + (last - first) * i / 40).collect();
        // Before the first block, after the last, and on blocks that share
        // their second with others.
        times.extend([0, first - 1, first, last, last + 1, u64::MAX]);
        times.extend([7, 1_234, 5_000, 12_345, 19_998].map(|n| blocks[&n]));
        for latest in [19_999, 12_345] {
            for &time in &times {
                let calls = block_calls(&mock);
                assert_eq!(
                    chain.block_at(time, latest).await.unwrap(),
                    block_at_by_hand(&blocks, time, latest),
                    "time {time}, latest {latest}"
                );
                // Never much worse than halving the range at every step.
                assert!(block_calls(&mock) - calls <= 2 * 15 + 2);
            }
        }
    }

    #[tokio::test]
    async fn a_search_by_time_starts_from_the_blocks_read_before() {
        let (chain, mock) = client(uneven_chain(200_000, 50_000)).await;
        let latest = 199_999;
        let time = mock.lock().blocks[&120_000] + 1;
        let answer = block_at_by_hand(&mock.lock().blocks, time, latest);
        assert_eq!(chain.block_at(time, latest).await.unwrap(), answer);
        let cold = block_calls(&mock);

        // The same time again: no calls at all.
        assert_eq!(chain.block_at(time, latest).await.unwrap(), answer);
        assert_eq!(block_calls(&mock), cold);

        // An hour later, as for another upload of the same hour: a few.
        let later = time + 3_600;
        let answer = block_at_by_hand(&mock.lock().blocks, later, latest);
        assert_eq!(chain.block_at(later, latest).await.unwrap(), answer);
        let warm = block_calls(&mock) - cold;
        assert!(warm <= 6 && warm < cold, "{warm} calls warm, {cold} cold");
    }

    #[test]
    fn known_blocks_are_capped() {
        let chain = Chain::new("http://127.0.0.1:0".into()).unwrap();
        for number in 0..10_000u64 {
            chain.remember_block(number, 1_790_000_000 + 2 * number);
        }
        let known = chain.known_blocks.lock().clone();
        assert!(known.len() <= MAX_KNOWN_BLOCKS);
        // Still spread from the first to the last.
        assert!(known.contains_key(&0) && known.contains_key(&9_999));
        assert!(known.len() > MAX_KNOWN_BLOCKS / 2);
    }

    #[tokio::test]
    async fn searches_by_time_take_turns() {
        let (chain, mock) = client(chain_with(1000, &[(700, 5)])).await;
        let by_time = Lookup::Quote {
            quote_hash: [5; 32],
            block_number: None,
            search_from: 1_790_000_000 + 2 * 650,
            not_after: None,
        };
        let slots = u32::try_from(MAX_TIME_SEARCHES).unwrap();
        let running = chain.time_searches.acquire_many(slots).await.unwrap();
        // Every slot taken: a search by time waits, without a call.
        let waited = tokio::time::timeout(
            Duration::from_millis(200),
            chain.fetch(&by_time, &ListingPayment::default(), &VAULT, Search::Full),
        )
        .await;
        assert!(waited.is_err());
        assert_eq!(mock.lock().calls, Vec::<String>::new());
        // A lookup in one block doesn't search, so doesn't wait.
        let in_block = Lookup::Quote {
            quote_hash: [5; 32],
            block_number: Some(700),
            search_from: 0,
            not_after: None,
        };
        assert_eq!(
            chain
                .fetch(&in_block, &ListingPayment::default(), &VAULT, Search::Full)
                .await
                .unwrap()
                .len(),
            1
        );

        drop(running);
        assert_eq!(
            chain
                .fetch(&by_time, &ListingPayment::default(), &VAULT, Search::Full)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

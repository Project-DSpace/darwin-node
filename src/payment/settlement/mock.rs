//! A payment chain to test the payment reads against, served over JSON-RPC
//! on 127.0.0.1, and quotes to pay on it.
//!
//! The chain holds the vault's payments as the vault writes them: a quote's
//! record is written once, by the first non-zero payment of it, and the vault
//! logs `DataPaymentMade` only for that write, so a later payment of the same
//! quote moves nothing and logs nothing. Each provider serving the chain can
//! be behind it, answer busy, drop calls, take calls and never answer, refuse
//! or cut short JSON-RPC batches, or answer records without their payee.
//! A root vault beside it holds the waves recorded on the chain, by key.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::payment::{EvmVerifierConfig, PaymentVerifier, PaymentVerifierConfig, PriceFloorConfig};
use ant_listings::root::{RootPayment, Wave};
use ant_listings::PaymentLog;
use evmlib::{EncodedPeerId, PaymentQuote, ProofOfPayment, RewardsAddress};
use saorsa_core::identity::node_identity::peer_id_from_public_key_bytes;
use saorsa_core::MlDsa65;
use saorsa_pqc::pqc::types::MlDsaSecretKey;
use saorsa_pqc::pqc::MlDsaOperations;
use std::sync::OnceLock;
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The vault the chain's payments go through.
pub const VAULT: [u8; 20] = [0x69; 20];

/// The account that pays the waves [`Chain::record_wave`] records.
pub const PAYER: [u8; 20] = [0xc1; 20];

/// The chain's root vault, which records each wave of payments under one
/// key.
pub const ROOT_VAULT: [u8; 20] = [0x6a; 20];

/// The token the vault moves, whose transfer logs fill receipts too.
const TOKEN: [u8; 20] = [0x70; 20];

/// `keccak256("Transfer(address,address,uint256)")`.
const TRANSFER_TOPIC: [u8; 32] = [
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
];

/// The timestamp of block `number`.
pub const fn block_time(number: u64) -> u64 {
    1_790_000_000 + 2 * number
}

/// One `payForQuotes` entry: the quote hash, the payee, the amount.
pub type Entry = ([u8; 32], [u8; 20], u128);

/// A transaction on the chain.
#[derive(Debug, Clone)]
pub struct Tx {
    pub hash: [u8; 32],
    pub block: u64,
    pub success: bool,
    /// Its `payForQuotes` entries, in order.
    pub entries: Vec<Entry>,
    /// Logs its receipt shows beyond the vault's and the token's: another
    /// contract's, or one a provider shows for a reverted transaction.
    pub extra_logs: Vec<PaymentLog>,
    /// When a provider that isn't behind starts seeing it.
    pub seen_from: Instant,
}

/// A wave the root vault recorded.
#[derive(Debug, Clone)]
pub struct RecordedKey {
    pub key: [u8; 32],
    pub leaf_count: u32,
    /// The account that paid the wave.
    pub payer: [u8; 20],
    /// When a provider that isn't behind starts seeing it.
    pub seen_from: Instant,
}

/// The chain, its transactions in chain order, and the waves its root vault
/// recorded.
#[derive(Debug, Default)]
pub struct Chain {
    pub txs: Vec<Tx>,
    pub waves: Vec<RecordedKey>,
}

impl Chain {
    /// A new chain behind a lock, as providers share it.
    pub fn shared() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::default()))
    }

    /// Add a transaction that succeeded, in the next block, seen from now.
    pub fn pay(&mut self, hash: [u8; 32], entries: Vec<Entry>) -> &mut Tx {
        let block = self.txs.last().map_or(100, |tx| tx.block + 1);
        self.txs.push(Tx {
            hash,
            block,
            success: true,
            entries,
            extra_logs: Vec::new(),
            seen_from: Instant::now(),
        });
        self.txs.last_mut().unwrap()
    }

    /// Record `wave` at the root vault, seen from now.
    pub fn record_wave(&mut self, wave: &Wave) {
        self.record_wave_by(wave, PAYER, Duration::ZERO);
    }

    /// Record `wave` at the root vault, paid by `payer`, seen `after` from
    /// now.
    pub fn record_wave_by(&mut self, wave: &Wave, payer: [u8; 20], after: Duration) {
        self.waves.push(RecordedKey {
            key: wave.key(&ROOT_VAULT),
            leaf_count: u32::try_from(wave.leaf_count()).unwrap(),
            payer,
            seen_from: Instant::now() + after,
        });
    }

    /// The root vault's record under `key`, as a provider `behind` sees it:
    /// the time, the leaf count and the payer, each in a word, or zeros.
    fn wave_record(&self, key: &[u8; 32], behind: Duration) -> [u8; 96] {
        let mut words = [0u8; 96];
        let now = Instant::now();
        if let Some(wave) = self
            .waves
            .iter()
            .find(|wave| wave.key == *key && wave.seen_from + behind <= now)
        {
            words[24..32].copy_from_slice(&block_time(100).to_be_bytes());
            words[60..64].copy_from_slice(&wave.leaf_count.to_be_bytes());
            words[76..].copy_from_slice(&wave.payer);
        }
        words
    }

    /// The transactions a provider `behind` sees now, with their places.
    fn seen(&self, behind: Duration) -> impl Iterator<Item = (usize, &Tx)> {
        let now = Instant::now();
        self.txs
            .iter()
            .enumerate()
            .filter(move |(_, tx)| tx.seen_from + behind <= now)
    }

    /// The entries of transaction `index` the vault wrote: those paying a
    /// quote no earlier payment recorded.
    fn written(&self, index: usize) -> Vec<Entry> {
        let Some(tx) = self.txs.get(index) else {
            return Vec::new();
        };
        if !tx.success {
            return Vec::new();
        }
        let mut recorded: HashSet<[u8; 32]> = self.txs[..index]
            .iter()
            .filter(|earlier| earlier.success)
            .flat_map(|earlier| earlier.entries.iter())
            .filter(|(_, _, amount)| *amount != 0)
            .map(|(quote_hash, _, _)| *quote_hash)
            .collect();
        tx.entries
            .iter()
            .filter(|(quote_hash, _, amount)| *amount != 0 && recorded.insert(*quote_hash))
            .copied()
            .collect()
    }

    /// The vault's record for `quote_hash`, as a provider `behind` sees it:
    /// the payee's leading 16 bytes and the amount, or zeros.
    pub fn record(&self, quote_hash: &[u8; 32], behind: Duration) -> ([u8; 16], u128) {
        for (_, tx) in self.seen(behind) {
            if !tx.success {
                continue;
            }
            for (paid, payee, amount) in &tx.entries {
                if paid == quote_hash && *amount != 0 {
                    let mut prefix = [0u8; 16];
                    prefix.copy_from_slice(&payee[..16]);
                    return (prefix, *amount);
                }
            }
        }
        ([0u8; 16], 0)
    }

    /// The receipt of `tx_hash` as JSON, as a provider `behind` sees it, or
    /// `null`.
    fn receipt(&self, tx_hash: &[u8; 32], behind: Duration) -> Value {
        let Some((index, tx)) = self.seen(behind).find(|(_, tx)| tx.hash == *tx_hash) else {
            return Value::Null;
        };
        let mut logs = Vec::new();
        for (quote_hash, payee, amount) in self.written(index) {
            let mut from = [0u8; 32];
            from[12..].copy_from_slice(&[0xc1; 20]);
            logs.push(log_json(
                &TOKEN,
                &[TRANSFER_TOPIC, from, address_topic(&payee)],
                logs.len(),
            ));
            logs.push(log_json(
                &VAULT,
                &[
                    DATA_PAYMENT_MADE_TOPIC,
                    address_topic(&payee),
                    amount_topic(amount),
                    quote_hash,
                ],
                logs.len(),
            ));
        }
        for log in &tx.extra_logs {
            logs.push(log_json(&log.address, &log.topics, logs.len()));
        }
        json!({
            "transactionHash": format!("0x{}", hex::encode(tx.hash)),
            "status": if tx.success { "0x1" } else { "0x0" },
            "blockNumber": quantity(tx.block),
            "logs": logs,
        })
    }

    /// The latest block.
    fn latest(&self) -> u64 {
        self.txs.last().map_or(100, |tx| tx.block) + 10
    }
}

/// An address as an indexed log topic.
pub fn address_topic(address: &[u8; 20]) -> [u8; 32] {
    let mut topic = [0u8; 32];
    topic[12..].copy_from_slice(address);
    topic
}

/// An amount as an indexed log topic.
pub fn amount_topic(amount: u128) -> [u8; 32] {
    let mut topic = [0u8; 32];
    topic[16..].copy_from_slice(&amount.to_be_bytes());
    topic
}

/// A `DataPaymentMade` log of `quote_hash`, paid to `payee`, emitted by
/// `address`.
pub fn payment_log(
    address: [u8; 20],
    quote_hash: [u8; 32],
    payee: [u8; 20],
    amount: u128,
) -> PaymentLog {
    PaymentLog {
        address,
        topics: vec![
            DATA_PAYMENT_MADE_TOPIC,
            address_topic(&payee),
            amount_topic(amount),
            quote_hash,
        ],
        data: Vec::new(),
        log_index: 0,
    }
}

fn log_json(address: &[u8; 20], topics: &[[u8; 32]], index: usize) -> Value {
    json!({
        "address": format!("0x{}", hex::encode(address)),
        "topics": topics.iter().map(|t| format!("0x{}", hex::encode(t))).collect::<Vec<_>>(),
        "data": "0x",
        "logIndex": quantity(index as u64),
    })
}

/// What a provider does with one HTTP request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Act {
    /// Answer it from the chain.
    Answer,
    /// Answer `-32005 the network is busy`.
    Busy,
    /// Close the connection without an answer.
    Drop,
    /// Take it and never answer.
    Stall,
}

/// How a provider answers a JSON-RPC batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Batches {
    /// Every call in it.
    Answer,
    /// With one error, not a list, as a provider without batches does.
    Refuse,
    /// Only the first so many calls.
    Cap(usize),
    /// Every call, up to so many; a larger batch with HTTP 429, as Robinhood
    /// Chain's own RPC answers one over its cap.
    TooManyOver(usize),
    /// Every call answered busy, each with its own error, as a provider
    /// limiting its rate answers.
    EachBusy,
    /// Taken and never answered, while calls on their own are answered.
    Stall,
}

/// How a provider behaves.
pub struct Spec {
    /// How far behind the chain it is.
    pub behind: Duration,
    pub batches: Batches,
    /// Answer `completedPayments` with the amount alone, without the payee.
    pub short_records: bool,
    /// A pause before each answer.
    pub delay: Duration,
    /// What to do with its `n`th HTTP request, counted from 1.
    pub act: Box<dyn Fn(usize) -> Act + Send + Sync>,
}

impl Default for Spec {
    fn default() -> Self {
        Self {
            behind: Duration::ZERO,
            batches: Batches::Answer,
            short_records: false,
            delay: Duration::ZERO,
            act: Box::new(|_| Act::Answer),
        }
    }
}

impl Spec {
    /// Every request taken and never answered.
    pub fn stalled() -> Self {
        Self {
            act: Box::new(|_| Act::Stall),
            ..Self::default()
        }
    }

    /// `act` for each request.
    pub fn acting(act: impl Fn(usize) -> Act + Send + Sync + 'static) -> Self {
        Self {
            act: Box::new(act),
            ..Self::default()
        }
    }
}

/// A call a provider got.
#[derive(Debug, Clone)]
pub struct Received {
    pub method: String,
    /// The quote hash a `completedPayments` call read, the transaction hash
    /// of a receipt, or the block of a block read, in hex.
    pub subject: String,
    /// The HTTP request it came in, counted from 1.
    pub request: usize,
    /// How many calls that request carried: 1 for one on its own.
    pub batch: usize,
    /// The connection it came on, counted from 1.
    pub connection: usize,
    pub at: Instant,
}

/// A provider serving a chain on 127.0.0.1.
pub struct Provider {
    pub url: reqwest::Url,
    received: Arc<Mutex<Vec<Received>>>,
}

impl Provider {
    /// Every call it got, in order.
    pub fn received(&self) -> Vec<Received> {
        self.received.lock().clone()
    }

    /// The calls of `method` it got.
    pub fn calls(&self, method: &str) -> Vec<Received> {
        self.received()
            .into_iter()
            .filter(|call| call.method == method)
            .collect()
    }

    /// The `completedPayments` calls it got.
    pub fn record_reads(&self) -> Vec<Received> {
        self.calls("eth_call")
    }

    /// The receipt reads it got.
    pub fn receipt_reads(&self) -> Vec<Received> {
        self.calls("eth_getTransactionReceipt")
    }

    /// How many HTTP requests it got.
    pub fn requests(&self) -> usize {
        self.received()
            .iter()
            .map(|call| call.request)
            .max()
            .unwrap_or(0)
    }
}

/// Serve `chain` as `spec` says.
#[allow(clippy::too_many_lines)]
pub async fn provider(chain: &Arc<Mutex<Chain>>, spec: Spec) -> Provider {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let received: Arc<Mutex<Vec<Received>>> = Arc::default();
    let requests = Arc::new(AtomicUsize::new(0));
    let (chain, spec, log) = (Arc::clone(chain), Arc::new(spec), Arc::clone(&received));
    tokio::spawn(async move {
        let mut connections = 0;
        while let Ok((mut socket, _)) = listener.accept().await {
            connections += 1;
            let connection = connections;
            let (chain, spec, log, requests) = (
                Arc::clone(&chain),
                Arc::clone(&spec),
                Arc::clone(&log),
                Arc::clone(&requests),
            );
            tokio::spawn(async move {
                while let Some(body) = read_http_body(&mut socket).await {
                    let request: Value = serde_json::from_slice(&body).unwrap();
                    let number = requests.fetch_add(1, Ordering::SeqCst) + 1;
                    let calls = match &request {
                        Value::Array(calls) => calls.clone(),
                        call => vec![call.clone()],
                    };
                    for call in &calls {
                        log.lock().push(Received {
                            method: call["method"].as_str().unwrap_or_default().to_string(),
                            subject: subject(call),
                            request: number,
                            batch: calls.len(),
                            connection,
                            at: Instant::now(),
                        });
                    }
                    let mut status = "200 OK";
                    let response = match (spec.act)(number) {
                        Act::Answer => {
                            if request.is_array() {
                                match spec.batches {
                                    Batches::Stall => {
                                        std::future::pending::<()>().await;
                                        return;
                                    }
                                    Batches::Answer => Value::Array(
                                        calls
                                            .iter()
                                            .map(|c| answer_call(&chain, &spec, c))
                                            .collect(),
                                    ),
                                    Batches::Cap(most) => Value::Array(
                                        calls
                                            .iter()
                                            .take(most)
                                            .map(|c| answer_call(&chain, &spec, c))
                                            .collect(),
                                    ),
                                    Batches::Refuse => json!({ "jsonrpc": "2.0", "id": null,
                                        "error": { "code": -32600,
                                            "message": "batch requests are not supported" } }),
                                    Batches::TooManyOver(most) if calls.len() > most => {
                                        status = "429 Too Many Requests";
                                        json!({ "code": 429, "message": "Too Many Requests" })
                                    }
                                    Batches::TooManyOver(_) => Value::Array(
                                        calls
                                            .iter()
                                            .map(|c| answer_call(&chain, &spec, c))
                                            .collect(),
                                    ),
                                    Batches::EachBusy => Value::Array(
                                        calls
                                            .iter()
                                            .map(|c| {
                                                json!({ "jsonrpc": "2.0", "id": c["id"],
                                                    "error": { "code": -32005,
                                                        "message": "the network is busy" } })
                                            })
                                            .collect(),
                                    ),
                                }
                            } else {
                                answer_call(&chain, &spec, &request)
                            }
                        }
                        Act::Busy => json!({ "jsonrpc": "2.0", "id": request["id"],
                            "error": { "code": -32005, "message": "the network is busy" } }),
                        Act::Drop => return,
                        Act::Stall => {
                            std::future::pending::<()>().await;
                            return;
                        }
                    };
                    if !spec.delay.is_zero() {
                        tokio::time::sleep(spec.delay).await;
                    }
                    let body = response.to_string();
                    let reply = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n\
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
    Provider { url, received }
}

/// What a call asks about, in hex.
fn subject(call: &Value) -> String {
    let params = &call["params"];
    match call["method"].as_str().unwrap_or_default() {
        "eth_call" => {
            let input = params[0]["input"]
                .as_str()
                .or_else(|| params[0]["data"].as_str())
                .unwrap_or_default();
            input
                .get(input.len().saturating_sub(64)..)
                .unwrap_or_default()
                .to_lowercase()
        }
        _ => params[0]
            .as_str()
            .unwrap_or_default()
            .trim_start_matches("0x")
            .to_lowercase(),
    }
}

/// The answer to one call, from the chain as the provider sees it.
fn answer_call(chain: &Mutex<Chain>, spec: &Spec, call: &Value) -> Value {
    let id = call["id"].clone();
    let params = &call["params"];
    let chain = chain.lock();
    let result = match call["method"].as_str().unwrap_or_default() {
        "eth_call" => {
            let input = params[0]["input"]
                .as_str()
                .or_else(|| params[0]["data"].as_str())
                .unwrap_or_default()
                .trim_start_matches("0x");
            let to = params[0]["to"].as_str().unwrap_or_default();
            if to.eq_ignore_ascii_case(&format!("0x{}", hex::encode(ROOT_VAULT))) {
                assert_eq!(
                    &input[..8],
                    hex::encode(super::ROOT_RECORDS_SELECTOR),
                    "only records is read at the root vault"
                );
                let key: [u8; 32] = hex::decode(&input[8..]).unwrap().try_into().unwrap();
                let words = chain.wave_record(&key, spec.behind);
                return json!({ "jsonrpc": "2.0", "id": id,
                    "result": format!("0x{}", hex::encode(words)) });
            }
            let quote_hash: [u8; 32] = hex::decode(&input[8..]).unwrap().try_into().unwrap();
            assert_eq!(
                &input[..8],
                hex::encode(COMPLETED_PAYMENTS_SELECTOR),
                "only completedPayments is read"
            );
            let (prefix, amount) = chain.record(&quote_hash, spec.behind);
            let mut words = [0u8; 64];
            words[..16].copy_from_slice(&prefix);
            words[48..].copy_from_slice(&amount.to_be_bytes());
            let words = if spec.short_records {
                &words[32..]
            } else {
                &words[..]
            };
            json!(format!("0x{}", hex::encode(words)))
        }
        "eth_getTransactionReceipt" => {
            let tx_hash: [u8; 32] =
                hex::decode(params[0].as_str().unwrap().trim_start_matches("0x"))
                    .unwrap()
                    .try_into()
                    .unwrap();
            chain.receipt(&tx_hash, spec.behind)
        }
        "eth_getBlockByNumber" => {
            let number = parse_u64(params[0].as_str().unwrap()).unwrap();
            if number <= chain.latest() {
                json!({ "number": quantity(number), "timestamp": quantity(block_time(number)) })
            } else {
                Value::Null
            }
        }
        other => {
            return json!({ "jsonrpc": "2.0", "id": id,
                "error": { "code": -32601, "message": format!("no method {other}") } })
        }
    };
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// One HTTP request's body, or `None` once the connection closes.
async fn read_http_body(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
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
        .map_or(0, |value| value.trim().parse().unwrap_or(0));
    while buffer.len() < header_end + length {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    Some(buffer[header_end..header_end + length].to_vec())
}

/// Seven quote issuers, the close group of every address in these tests:
/// each one's public key and secret key bytes.
fn issuers() -> &'static Vec<(Vec<u8>, Vec<u8>)> {
    static ISSUERS: OnceLock<Vec<(Vec<u8>, Vec<u8>)>> = OnceLock::new();
    ISSUERS.get_or_init(|| {
        (0..crate::ant_protocol::CLOSE_GROUP_SIZE)
            .map(|_| {
                let (public, secret) = MlDsa65::new().generate_keypair().unwrap();
                (public.as_bytes().to_vec(), secret.as_bytes().to_vec())
            })
            .collect()
    })
}

/// The issuers' peer ids.
pub fn issuer_peers() -> Vec<[u8; 32]> {
    issuers()
        .iter()
        .map(|(public, _)| *peer_id_from_public_key_bytes(public).unwrap().as_bytes())
        .collect()
}

/// A proof's quotes for one address, as a client gathers them: seven, from
/// nodes holding 0 to 6 committed keys, priced as those nodes price them.
/// Only the median's is signed: the check reads no other's signature.
pub struct Bundle {
    pub address: [u8; 32],
    pub quotes: Vec<(EncodedPeerId, PaymentQuote)>,
    pub median: PaymentQuote,
}

impl Bundle {
    /// Quotes for `address`.
    pub fn new(address: [u8; 32]) -> Self {
        let quotes: Vec<(EncodedPeerId, PaymentQuote)> = issuers()
            .iter()
            .enumerate()
            .map(|(records, (public, secret))| {
                let count = u32::try_from(records).unwrap();
                let mut quote = PaymentQuote {
                    content: xor_name::XorName(address),
                    timestamp: SystemTime::now(),
                    price: crate::payment::calculate_price(records),
                    rewards_address: RewardsAddress::new(
                        [u8::try_from(records + 0x31).unwrap(); 20],
                    ),
                    committed_key_count: count,
                    commitment_pin: (count > 0).then_some([0xA5; 32]),
                    pub_key: public.clone(),
                    signature: Vec::new(),
                };
                if records == crate::ant_protocol::CLOSE_GROUP_SIZE / 2 {
                    let secret = MlDsaSecretKey::from_bytes(secret).unwrap();
                    quote.signature = MlDsa65::new()
                        .sign(&secret, &quote.bytes_for_sig())
                        .unwrap()
                        .as_bytes()
                        .to_vec();
                }
                let peer = *peer_id_from_public_key_bytes(public).unwrap().as_bytes();
                (EncodedPeerId::new(peer), quote)
            })
            .collect();
        let median = quotes[crate::ant_protocol::CLOSE_GROUP_SIZE / 2].1.clone();
        Self {
            address,
            quotes,
            median,
        }
    }

    /// The quote hash a client pays.
    pub fn quote_hash(&self) -> [u8; 32] {
        self.median.hash().0
    }

    /// The median quote's payee.
    pub fn payee(&self) -> [u8; 20] {
        self.median.rewards_address.0 .0
    }

    /// The amount a client pays: three times the median price.
    pub fn amount(&self) -> u128 {
        u128::try_from(self.median.price * Amount::from(3u64)).unwrap()
    }

    /// The `payForQuotes` entry a client sends for these quotes.
    pub fn entry(&self) -> Entry {
        (self.quote_hash(), self.payee(), self.amount())
    }

    /// The payment a client makes for these quotes in a wave: the median
    /// quote's, to its payee, of three times its price.
    pub fn root_payment(&self) -> RootPayment {
        RootPayment {
            quote_hash: self.quote_hash(),
            payee: self.payee(),
            amount: Amount::from(self.amount()),
        }
    }

    /// The root proof a client sends for its payment in `wave`, naming
    /// `tx_hashes`.
    pub fn root_proof_in(&self, wave: &Wave, tx_hashes: &[[u8; 32]]) -> Vec<u8> {
        let index = wave
            .payments()
            .iter()
            .position(|payment| payment.quote_hash == self.quote_hash())
            .expect("the wave pays this quote");
        let payment = wave.payments()[index];
        self.root_proof(
            payment.amount,
            index as u64,
            wave.leaf_count(),
            &wave.path(index).unwrap(),
            tx_hashes,
        )
    }

    /// A root proof for the median quote, as it is given: the amount, the
    /// leaf's place, the leaf count and the path, naming `tx_hashes`.
    pub fn root_proof(
        &self,
        amount: Amount,
        index: u64,
        leaf_count: u64,
        path: &[[u8; 32]],
        tx_hashes: &[[u8; 32]],
    ) -> Vec<u8> {
        crate::payment::serialize_root_proof(&crate::payment::RootPaymentProof {
            payment: crate::payment::PaymentProof {
                proof_of_payment: ProofOfPayment {
                    peer_quotes: self.quotes.clone(),
                },
                tx_hashes: tx_hashes
                    .iter()
                    .map(|tx_hash| evmlib::common::TxHash::from(*tx_hash))
                    .collect(),
                commitment_sidecars: Vec::new(),
            },
            quote_hash: self.quote_hash().into(),
            amount,
            index,
            leaf_count,
            path: path.iter().map(|hash| (*hash).into()).collect(),
        })
        .unwrap()
    }

    /// The proof a client sends, naming `tx_hashes`.
    pub fn proof(&self, tx_hashes: &[[u8; 32]]) -> Vec<u8> {
        crate::payment::serialize_single_node_proof(&crate::payment::PaymentProof {
            proof_of_payment: ProofOfPayment {
                peer_quotes: self.quotes.clone(),
            },
            tx_hashes: tx_hashes
                .iter()
                .map(|tx_hash| evmlib::common::TxHash::from(*tx_hash))
                .collect(),
            commitment_sidecars: Vec::new(),
        })
        .unwrap()
    }
}

/// A node's payment check, reading the chain's vault through `providers` in
/// that order, the issuers being among its closest peers for every address.
pub fn verifier(providers: &[&Provider]) -> PaymentVerifier {
    let verifier = PaymentVerifier::new(PaymentVerifierConfig {
        evm: EvmVerifierConfig {
            network: evmlib::Network::new_custom(
                providers[0].url.as_str(),
                &format!("0x{}", hex::encode(TOKEN)),
                &format!("0x{}", hex::encode(VAULT)),
            ),
            root_vault: None,
        },
        cache_capacity: 1_000,
        close_group_size: crate::ant_protocol::CLOSE_GROUP_SIZE,
        local_rewards_address: RewardsAddress::new([0xD1; 20]),
        price_floor: PriceFloorConfig::default(),
    });
    verifier.set_payment_rpc_urls_for_tests(providers.iter().map(|p| p.url.clone()).collect());
    verifier.set_paid_quote_k_closest_for_tests(issuer_peers());
    verifier
}

/// [`verifier`], taking root proofs paid at the chain's root vault too.
pub fn root_verifier(providers: &[&Provider]) -> PaymentVerifier {
    let verifier = PaymentVerifier::new(PaymentVerifierConfig {
        evm: EvmVerifierConfig {
            network: evmlib::Network::new_custom(
                providers[0].url.as_str(),
                &format!("0x{}", hex::encode(TOKEN)),
                &format!("0x{}", hex::encode(VAULT)),
            ),
            root_vault: Some(RootVault {
                address: ROOT_VAULT,
                token: TOKEN,
            }),
        },
        cache_capacity: 1_000,
        close_group_size: crate::ant_protocol::CLOSE_GROUP_SIZE,
        local_rewards_address: RewardsAddress::new([0xD1; 20]),
        price_floor: PriceFloorConfig::default(),
    });
    verifier.set_payment_rpc_urls_for_tests(providers.iter().map(|p| p.url.clone()).collect());
    verifier.set_paid_quote_k_closest_for_tests(issuer_peers());
    verifier
}

/// The wave paying each of `bundles`' median quotes, in quote-hash order, as
/// a root vault takes it.
pub fn wave_of(bundles: &[&Bundle]) -> Wave {
    let mut payments: Vec<RootPayment> = bundles.iter().map(|b| b.root_payment()).collect();
    payments.sort_by_key(|payment| payment.quote_hash);
    Wave::new(payments).unwrap()
}

/// Reads of the chain's vault through `providers`, in that order.
pub fn reads(providers: &[&Provider]) -> Arc<SettlementReads> {
    SettlementReads::new(VAULT, providers.iter().map(|p| p.url.clone()).collect())
}

/// A transaction hash made of `byte`.
pub const fn tx(byte: u8) -> [u8; 32] {
    [byte; 32]
}

//! darwin-harness: the client side of the mixed-version harness
//! (`scripts/test_mixed_versions.py`).
//!
//! `darwin-harness upload` joins a devnet from its manifest as a client and
//! stores records of random bytes as a client does: for each record it asks
//! the nodes nearest the record's address for quotes, pays the median quote 3
//! times its price on the devnet's chain (at `--settlement-version 2`, 3 times
//! its price times the record's weight), and sends the record with its proof
//! to every node that quoted. A node that answers `StorerUpdateRequired` is
//! passed over for the next nearest, as a client passes it over. The nodes
//! then pass it on to each other: fresh offers, paid-list notices, neighbour
//! sync, audits. `darwin-harness check` later reads each record back from the
//! nodes nearest it. Both write what they saw as JSON, for the harness to
//! compare across runs.
//!
//! `darwin-harness browser-node` and `browser-load` measure what browser
//! sessions cost a node (`scripts/measure_browser_sessions.py`).
//!
//! Built only with the `test-utils` feature: a test tool, never released.

#![allow(clippy::too_many_lines)]

#[cfg(feature = "webrtc-direct")]
mod browser;

use ant_node::ant_protocol::{
    ChunkGetRequest, ChunkGetResponse, ChunkMessage, ChunkMessageBody, ChunkPutRequest,
    ChunkPutResponse, ChunkQuoteRequestV2, ChunkQuoteResponse, CLOSE_GROUP_MAJORITY,
    CLOSE_GROUP_SIZE, MAX_WIRE_MESSAGE_SIZE,
};
use ant_node::client::{compute_address, send_and_await_chunk_response, xor_distance};
use ant_node::core::{CoreNodeConfig, NodeMode, P2PNode, PeerId};
use ant_node::devnet::DevnetManifest;
use ant_node::payment::{serialize_single_node_proof, PaymentProof};
use ant_protocol::chunk::DATA_TYPE_CHUNK;
use ant_protocol::payment::pricing::{record_weight, weighted_amount};
use clap::{Parser, Subcommand};
use color_eyre::eyre::{eyre, Result, WrapErr};
use evmlib::common::Amount;
use evmlib::wallet::Wallet;
use evmlib::{EncodedPeerId, PaymentQuote, ProofOfPayment};
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long a released client waits for a PUT's answer (darwin-client's
/// `STORE_RESPONSE_TIMEOUT`): a node that answers later has failed the
/// client, so the harness waits no longer either.
const PUT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait for a quote or a read.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// How long to keep trying to find the nodes nearest an address.
const LOOKUP_DEADLINE: Duration = Duration::from_secs(120);

/// The multiple of the median quote's price a client pays.
const PAYMENT_MULTIPLIER: u64 = 3;

/// The settlement version the harness declares on its quotes unless told
/// otherwise: 1, as the released client does, which pays every record as a
/// full chunk. Every node release quotes it, so a mixed network stores each
/// record as it does today. Not this crate's `CURRENT_SETTLEMENT_VERSION`: a
/// node older than this tree refuses a version-2 quote with
/// `StorerUpdateRequired`.
const SETTLEMENT_VERSION: u32 = 1;

/// What a quote refusal from a node that settles under older rules than the
/// client's contains (`ProtocolError::StorerUpdateRequired`, as debug text).
const STORER_UPDATE_REQUIRED: &str = "StorerUpdateRequired";

/// The client side of the mixed-version harness.
#[derive(Parser, Debug)]
#[command(name = "darwin-harness", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Store records of random bytes, each paid as a client pays.
    Upload {
        /// The devnet's manifest (`ant-devnet --manifest`): its bootstrap
        /// nodes and its chain, with a funded wallet.
        #[arg(long)]
        manifest: PathBuf,
        /// The size of each record in bytes, one record per size.
        #[arg(long, value_delimiter = ',', required = true)]
        sizes: Vec<usize>,
        /// The settlement version declared on the quotes: 1 pays every
        /// record as a full chunk, 2 by its size class (3 times the median
        /// quote's price times the record's weight).
        #[arg(long, default_value_t = SETTLEMENT_VERSION,
              value_parser = clap::value_parser!(u32).range(1..=2))]
        settlement_version: u32,
        /// Where to write what happened, as JSON.
        #[arg(long)]
        out: PathBuf,
    },
    /// Read records back from the nodes nearest them.
    Check {
        /// The devnet's manifest.
        #[arg(long)]
        manifest: PathBuf,
        /// What `upload` wrote.
        #[arg(long)]
        uploads: PathBuf,
        /// Where to write what each node answered, as JSON.
        #[arg(long)]
        out: PathBuf,
    },
    /// Run a two-node devnet whose first node serves browsers, with records
    /// stored on it, until standard input closes.
    #[cfg(feature = "webrtc-direct")]
    BrowserNode {
        /// A node config file: its `[webrtc_direct]` limits apply to the
        /// listeners (the defaults without it).
        #[arg(long)]
        config: Option<PathBuf>,
        /// The records to store on the node, as `SIZExCOUNT,...`.
        #[arg(long, default_value = "4194304x16,65536x16,4096x16")]
        records: String,
        /// Written once the node is ready: its process, endpoint and records.
        #[arg(long)]
        ready: PathBuf,
        /// The listener's counters, one line of JSON a second.
        #[arg(long)]
        snapshots: Option<PathBuf>,
    },
    /// Open browser sessions to a `browser-node`, read records over them,
    /// and close them, each step on a line of standard input.
    #[cfg(feature = "webrtc-direct")]
    BrowserLoad {
        /// What `browser-node` wrote.
        #[arg(long)]
        ready: PathBuf,
        /// How many sessions.
        #[arg(long)]
        sessions: usize,
        /// How long to read for.
        #[arg(long, default_value_t = 30)]
        get_seconds: u64,
        /// Reads a second across every session.
        #[arg(long, default_value_t = 200)]
        max_rate: u32,
        /// Where to write the summary, as JSON.
        #[arg(long)]
        out: PathBuf,
    },
}

/// One node's answer to one request.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Answer {
    /// The node, in hex.
    peer: String,
    /// What it answered: `stored`, `already_stored`, `quoted`, `held`,
    /// `not_found`, or what went wrong.
    outcome: String,
}

/// What happened to one record's upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Upload {
    /// The record's address, in hex.
    address: String,
    /// Its size in bytes.
    size: usize,
    /// The nodes nearest its address, nearest first.
    close_group: Vec<String>,
    /// Each node's answer to the quote request.
    quotes: Vec<Answer>,
    /// The node whose quote was paid, and the amount, in wei.
    paid_peer: Option<String>,
    paid_amount: Option<String>,
    /// The payment's transactions.
    tx_hashes: Vec<String>,
    /// Each quoting node's answer to the PUT.
    puts: Vec<Answer>,
    /// Whether at least a majority of the close group stored it.
    stored: bool,
    /// Why not, if it wasn't.
    error: Option<String>,
}

/// What `upload` writes.
#[derive(Debug, Serialize, Deserialize)]
struct Uploads {
    /// The client's own peer id, so its traffic can be told from the nodes'.
    client: String,
    /// The settlement version its quotes declared.
    #[serde(default = "settlement_version_one")]
    settlement_version: u32,
    records: Vec<Upload>,
}

const fn settlement_version_one() -> u32 {
    SETTLEMENT_VERSION
}

/// What the nodes that stored a record, and the nodes nearest it now,
/// answered when asked for it.
#[derive(Debug, Serialize, Deserialize)]
struct Check {
    address: String,
    /// The nodes that answered its PUT that they stored it.
    storers: Vec<String>,
    /// The nodes nearest it now, nearest first.
    close_group: Vec<String>,
    /// Each of those nodes' answer, once each.
    answers: Vec<Answer>,
    /// How many of the storers returned the record's bytes.
    held_by_storers: usize,
    /// How many of the nodes nearest it now did.
    held_by_close_group: usize,
}

/// What `check` writes.
#[derive(Debug, Serialize, Deserialize)]
struct Checks {
    client: String,
    records: Vec<Check>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    match cli.command {
        Command::Upload {
            manifest,
            sizes,
            settlement_version,
            out,
        } => {
            let manifest = read_manifest(&manifest)?;
            let client = join(&manifest).await?;
            let result = upload(&client, &manifest, &sizes, settlement_version).await;
            client.shutdown().await.ok();
            write_json(&out, &result?)
        }
        Command::Check {
            manifest,
            uploads,
            out,
        } => {
            let manifest = read_manifest(&manifest)?;
            let uploads: Uploads = serde_json::from_slice(
                &std::fs::read(&uploads)
                    .wrap_err_with(|| format!("reading {}", uploads.display()))?,
            )?;
            let client = join(&manifest).await?;
            let result = check(&client, &uploads).await;
            client.shutdown().await.ok();
            write_json(&out, &result?)
        }
        #[cfg(feature = "webrtc-direct")]
        Command::BrowserNode {
            config,
            records,
            ready,
            snapshots,
        } => browser::node(config, &records, &ready, snapshots).await,
        #[cfg(feature = "webrtc-direct")]
        Command::BrowserLoad {
            ready,
            sessions,
            get_seconds,
            max_rate,
            out,
        } => browser::load(&ready, sessions, get_seconds, max_rate, &out).await,
    }
}

fn read_manifest(path: &Path) -> Result<DevnetManifest> {
    let text =
        std::fs::read_to_string(path).wrap_err_with(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).wrap_err_with(|| format!("parsing {}", path.display()))
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(value)?)
        .wrap_err_with(|| format!("writing {}", path.display()))
}

/// A client node joined to the devnet, on loopback only. It takes messages as
/// large as a node's (`MAX_WIRE_MESSAGE_SIZE`), so a read of a full-size
/// chunk arrives.
async fn join(manifest: &DevnetManifest) -> Result<P2PNode> {
    let mut config = CoreNodeConfig::builder()
        .port(0)
        .ipv6(false)
        .local(true)
        .mode(NodeMode::Client)
        .max_message_size(MAX_WIRE_MESSAGE_SIZE)
        .build()?;
    config.bootstrap_peers.clone_from(&manifest.bootstrap);
    let client = P2PNode::new(config).await?;
    client.start().await?;
    let deadline = Instant::now() + LOOKUP_DEADLINE;
    while client.connected_peers().await.is_empty() {
        if Instant::now() >= deadline {
            return Err(eyre!("the client connected to no devnet node"));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    println!("client {} joined", client.peer_id().to_hex());
    Ok(client)
}

/// The `CLOSE_GROUP_SIZE` nodes nearest `address`, nearest first, from a
/// network lookup, retried until it finds that many. The client itself,
/// which a lookup can return, is left out: it stores nothing.
async fn close_group(client: &P2PNode, address: &[u8; 32]) -> Result<Vec<PeerId>> {
    nearest(client, address, CLOSE_GROUP_SIZE).await
}

/// Up to `count` nodes nearest `address`, nearest first, and at least
/// `CLOSE_GROUP_SIZE`, from a network lookup retried until it finds that many.
async fn nearest(client: &P2PNode, address: &[u8; 32], count: usize) -> Result<Vec<PeerId>> {
    let deadline = Instant::now() + LOOKUP_DEADLINE;
    loop {
        let mut found: Vec<PeerId> = client
            .dht()
            .find_closest_nodes(address, CLOSE_GROUP_SIZE * 2)
            .await
            .map(|nodes| nodes.into_iter().map(|node| node.peer_id).collect())
            .unwrap_or_default();
        found.retain(|peer| peer != client.peer_id());
        found.sort_by_key(|peer| xor_distance(address, peer.as_bytes()));
        found.dedup();
        found.truncate(count);
        if found.len() >= CLOSE_GROUP_SIZE {
            return Ok(found);
        }
        if Instant::now() >= deadline {
            return Err(eyre!(
                "found only {} nodes near {}",
                found.len(),
                hex::encode(address)
            ));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// A chunk-protocol request to `peer`, answered by `pick` within `timeout`.
async fn ask<T>(
    client: &P2PNode,
    peer: &PeerId,
    body: ChunkMessageBody,
    timeout: Duration,
    pick: impl Fn(ChunkMessageBody) -> Option<std::result::Result<T, String>>,
) -> std::result::Result<T, String> {
    let request_id: u64 = rand::thread_rng().gen();
    let message = ChunkMessage { request_id, body }
        .encode()
        .map_err(|e| format!("encode: {e}"))?;
    send_and_await_chunk_response(
        client,
        peer,
        message,
        request_id,
        timeout,
        &[],
        pick,
        |e| format!("send: {e}"),
        || format!("no answer within {}s", timeout.as_secs()),
    )
    .await
}

struct Quoted {
    peer: PeerId,
    quote: PaymentQuote,
    commitment: Option<Vec<u8>>,
}

async fn quote(
    client: &P2PNode,
    peer: PeerId,
    address: [u8; 32],
    size: usize,
    settlement_version: u32,
) -> (PeerId, std::result::Result<Quoted, String>) {
    let mut request = ChunkQuoteRequestV2::new(address, u64::try_from(size).unwrap_or(u64::MAX));
    request.settlement_version = settlement_version;
    let answer = ask(
        client,
        &peer,
        ChunkMessageBody::QuoteRequestV2(request),
        REQUEST_TIMEOUT,
        |body| match body {
            ChunkMessageBody::QuoteResponse(ChunkQuoteResponse::Success {
                quote,
                already_stored,
                commitment,
            }) => Some(if already_stored {
                Err("already stored".to_string())
            } else {
                rmp_serde::from_slice::<PaymentQuote>(&quote)
                    .map(|quote| (quote, commitment))
                    .map_err(|e| format!("quote does not decode: {e}"))
            }),
            ChunkMessageBody::QuoteResponse(other) => Some(Err(format!("{other:?}"))),
            _ => None,
        },
    )
    .await;
    let quoted = answer.map(|(quote, commitment)| Quoted {
        peer,
        quote,
        commitment,
    });
    (peer, quoted)
}

async fn put(
    client: &P2PNode,
    peer: PeerId,
    address: [u8; 32],
    content: &[u8],
    proof: Vec<u8>,
) -> Answer {
    let request = ChunkPutRequest::with_payment(address, content.to_vec().into(), proof);
    let outcome = ask(
        client,
        &peer,
        ChunkMessageBody::PutRequest(request),
        PUT_TIMEOUT,
        |body| match body {
            ChunkMessageBody::PutResponse(ChunkPutResponse::Success { .. }) => {
                Some(Ok("stored".to_string()))
            }
            ChunkMessageBody::PutResponse(ChunkPutResponse::AlreadyExists { .. }) => {
                Some(Ok("already_stored".to_string()))
            }
            ChunkMessageBody::PutResponse(other) => Some(Err(format!("refused: {other:?}"))),
            _ => None,
        },
    )
    .await;
    Answer {
        peer: peer.to_hex(),
        outcome: outcome.unwrap_or_else(|e| e),
    }
}

async fn upload(
    client: &P2PNode,
    manifest: &DevnetManifest,
    sizes: &[usize],
    settlement_version: u32,
) -> Result<Uploads> {
    let evm = manifest
        .evm
        .as_ref()
        .ok_or_else(|| eyre!("the manifest has no chain: start the devnet with --enable-evm"))?;
    let network = evmlib::Network::new_custom(
        &evm.rpc_url,
        &evm.payment_token_address,
        &evm.payment_vault_address,
    );
    let wallet = Wallet::new_from_private_key(network, &evm.wallet_private_key)
        .map_err(|e| eyre!("the manifest's wallet key: {e}"))?;
    let mut records = Vec::with_capacity(sizes.len());
    for (index, size) in sizes.iter().copied().enumerate() {
        let mut content = vec![0u8; size];
        rand::thread_rng().fill_bytes(&mut content);
        let address = compute_address(&content);
        let mut record = Upload {
            address: hex::encode(address),
            size,
            close_group: Vec::new(),
            quotes: Vec::new(),
            paid_peer: None,
            paid_amount: None,
            tx_hashes: Vec::new(),
            puts: Vec::new(),
            stored: false,
            error: None,
        };
        if let Err(e) = store(
            client,
            &wallet,
            address,
            &content,
            settlement_version,
            &mut record,
        )
        .await
        {
            record.error = Some(format!("{e:#}"));
        }
        println!(
            "record {index} ({size} bytes) at {}: {}",
            record.address,
            record.error.as_deref().unwrap_or(if record.stored {
                "stored"
            } else {
                "not stored"
            })
        );
        records.push(record);
    }
    Ok(Uploads {
        client: client.peer_id().to_hex(),
        settlement_version,
        records,
    })
}

/// Quotes, pays and stores one record, noting each step in `record`.
async fn store(
    client: &P2PNode,
    wallet: &Wallet,
    address: [u8; 32],
    content: &[u8],
    settlement_version: u32,
    record: &mut Upload,
) -> Result<()> {
    let candidates = nearest(client, &address, CLOSE_GROUP_SIZE * 2).await?;
    record.close_group = candidates
        .iter()
        .take(CLOSE_GROUP_SIZE)
        .map(PeerId::to_hex)
        .collect();

    // The close group first; each node that settles under older rules than
    // the quote declares is passed over for the next nearest, as a client
    // passes it over. At version 1 every node quotes, so only the close group
    // is asked.
    let mut quoted = Vec::new();
    let mut asked = 0;
    let mut wave = CLOSE_GROUP_SIZE;
    while wave > 0 && asked < candidates.len() {
        let end = (asked + wave).min(candidates.len());
        let answers = futures::future::join_all(
            candidates[asked..end]
                .iter()
                .map(|peer| quote(client, *peer, address, content.len(), settlement_version)),
        )
        .await;
        asked = end;
        wave = 0;
        for (peer, answer) in answers {
            record.quotes.push(Answer {
                peer: peer.to_hex(),
                outcome: answer
                    .as_ref()
                    .map_or_else(Clone::clone, |_| "quoted".into()),
            });
            match answer {
                Ok(answer) => quoted.push(answer),
                Err(reason) if reason.contains(STORER_UPDATE_REQUIRED) => wave += 1,
                Err(_) => {}
            }
        }
    }
    if quoted.len() < CLOSE_GROUP_MAJORITY {
        return Err(eyre!("only {} of the nodes asked quoted", quoted.len()));
    }

    // Paid as a client pays: the median quote, 3 times its price, and at
    // version 2 times the record's weight too.
    quoted.sort_by_key(|quoted| quoted.quote.price);
    let median = &quoted[quoted.len() / 2];
    let amount = if settlement_version >= 2 {
        weighted_amount(
            median.quote.price,
            record_weight(DATA_TYPE_CHUNK, content.len()),
        )
    } else {
        median
            .quote
            .price
            .checked_mul(Amount::from(PAYMENT_MULTIPLIER))
    }
    .ok_or_else(|| eyre!("the payment overflows"))?;
    let (paid, _gas) = wallet
        .pay_for_quotes(vec![(
            median.quote.hash(),
            median.quote.rewards_address,
            amount,
        )])
        .await
        .map_err(|e| eyre!("paying: {e:?}"))?;
    record.paid_peer = Some(median.peer.to_hex());
    record.paid_amount = Some(amount.to_string());
    let tx_hashes: Vec<_> = paid.values().copied().collect();
    record.tx_hashes = tx_hashes.iter().map(|tx| format!("{tx:#x}")).collect();

    let mut sidecars: Vec<Vec<u8>> = quoted
        .iter()
        .filter_map(|quoted| quoted.commitment.clone())
        .collect();
    sidecars.sort();
    sidecars.dedup();
    let proof = serialize_single_node_proof(&PaymentProof {
        proof_of_payment: ProofOfPayment {
            peer_quotes: quoted
                .iter()
                .map(|quoted| {
                    (
                        EncodedPeerId::new(*quoted.peer.as_bytes()),
                        quoted.quote.clone(),
                    )
                })
                .collect(),
        },
        tx_hashes,
        commitment_sidecars: sidecars,
    })
    .map_err(|e| eyre!("serializing the proof: {e}"))?;

    record.puts = futures::future::join_all(
        quoted
            .iter()
            .map(|quoted| put(client, quoted.peer, address, content, proof.clone())),
    )
    .await;
    let stored = record
        .puts
        .iter()
        .filter(|answer| answer.outcome == "stored" || answer.outcome == "already_stored")
        .count();
    record.stored = stored >= CLOSE_GROUP_MAJORITY;
    if !record.stored {
        return Err(eyre!("{stored} of {} nodes stored it", record.puts.len()));
    }
    Ok(())
}

/// 32 bytes from hex: an address or a peer id.
fn bytes32(hex_text: &str) -> Result<[u8; 32]> {
    hex::decode(hex_text)?
        .try_into()
        .map_err(|_| eyre!("{hex_text} is not 32 bytes"))
}

/// Asks the nodes that stored each record, and the nodes nearest it now,
/// for it. Nodes that joined after it was stored get it only by repair,
/// which waits for a record to age, so the storers say whether it was kept.
async fn check(client: &P2PNode, uploads: &Uploads) -> Result<Checks> {
    let mut records = Vec::with_capacity(uploads.records.len());
    for upload in uploads.records.iter().filter(|upload| upload.stored) {
        let address = bytes32(&upload.address)?;
        let storers = upload
            .puts
            .iter()
            .filter(|answer| answer.outcome == "stored" || answer.outcome == "already_stored")
            .map(|answer| bytes32(&answer.peer).map(PeerId::from_bytes))
            .collect::<Result<Vec<_>>>()?;
        let group = close_group(client, &address).await?;
        let mut asked = storers.clone();
        asked.extend(group.iter().filter(|peer| !storers.contains(peer)));
        let answers = futures::future::join_all(asked.iter().map(|peer| async move {
            let outcome = ask(
                client,
                peer,
                ChunkMessageBody::GetRequest(ChunkGetRequest::new(address)),
                REQUEST_TIMEOUT,
                |body| match body {
                    ChunkMessageBody::GetResponse(ChunkGetResponse::Success {
                        content, ..
                    }) => Some(if compute_address(&content) == address {
                        Ok("held".to_string())
                    } else {
                        Err("returned other bytes".to_string())
                    }),
                    ChunkMessageBody::GetResponse(ChunkGetResponse::NotFound { .. }) => {
                        Some(Ok("not_found".to_string()))
                    }
                    ChunkMessageBody::GetResponse(other) => Some(Err(format!("{other:?}"))),
                    _ => None,
                },
            )
            .await;
            Answer {
                peer: peer.to_hex(),
                outcome: outcome.unwrap_or_else(|e| e),
            }
        }))
        .await;
        let held = |peers: &[PeerId]| {
            asked
                .iter()
                .zip(&answers)
                .filter(|(peer, answer)| peers.contains(peer) && answer.outcome == "held")
                .count()
        };
        let (held_by_storers, held_by_close_group) = (held(&storers), held(&group));
        println!(
            "record {}: held by {held_by_storers} of the {} that stored it, and by \
             {held_by_close_group} of the {} nearest it now",
            upload.address,
            storers.len(),
            group.len()
        );
        records.push(Check {
            address: upload.address.clone(),
            storers: storers.iter().map(PeerId::to_hex).collect(),
            close_group: group.iter().map(PeerId::to_hex).collect(),
            answers,
            held_by_storers,
            held_by_close_group,
        });
    }
    Ok(Checks {
        client: client.peer_id().to_hex(),
        records,
    })
}

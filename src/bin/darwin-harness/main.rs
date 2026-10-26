//! darwin-harness: the client side of the mixed-version harness
//! (`scripts/test_mixed_versions.py`).
//!
//! `darwin-harness upload` joins a devnet from its manifest as a client and
//! stores records of random bytes as a client does: for each record it asks
//! the nodes nearest the record's address for quotes, pays the median quote 3
//! times its price on the devnet's chain, and sends the record with its proof
//! to every node that quoted. The nodes then pass it on to each other: fresh
//! offers, paid-list notices, neighbour sync, audits. Some records go to one
//! storer only, and pointers too, so the rest of the close group gets them by
//! that storer's offers, with the slim proof. `darwin-harness faults` then
//! runs the fault cases: repeated PUTs, and, through the faults seam of this
//! tree's nodes, holding notices lost on the way to one storer, a failed
//! paid-list write, and retries answered from the payment cache with a proof
//! the slim builder declines. `darwin-harness check` later reads each record
//! back from the nodes nearest it, and asks the paid group whether each
//! member lists it. Each writes what it saw as JSON, for the harness to
//! compare across runs.
//!
//! `darwin-harness browser-node` and `browser-load` measure what browser
//! sessions cost a node (`scripts/measure_browser_sessions.py`).
//!
//! Built only with the `test-utils` feature: a test tool, never released.

#![allow(clippy::too_many_lines)]

#[cfg(feature = "webrtc-direct")]
mod browser;

use ant_node::ant_protocol::chunk::{
    PointerGetRequest, PointerGetResponse, PointerPutRequest, PointerPutResponse,
};
use ant_node::ant_protocol::{
    ChunkGetRequest, ChunkGetResponse, ChunkMessage, ChunkMessageBody, ChunkPutRequest,
    ChunkPutResponse, ChunkQuoteRequestV2, ChunkQuoteResponse, CLOSE_GROUP_MAJORITY,
    CLOSE_GROUP_SIZE, DATA_TYPE_CHUNK, MAX_WIRE_MESSAGE_SIZE, PROOF_TAG_MERKLE,
    PROOF_TAG_SINGLE_NODE,
};
use ant_node::client::{compute_address, send_and_await_chunk_response, xor_distance};
use ant_node::core::{CoreNodeConfig, NodeMode, P2PNode, PeerId};
use ant_node::devnet::DevnetManifest;
use ant_node::payment::{serialize_single_node_proof, PaymentProof};
use ant_node::pointer::{
    Pointer, PointerTarget, PointerTargetKind, DATA_TYPE_POINTER, POINTER_WIRE_LEN,
};
use ant_node::replication::config::{PAID_LIST_CLOSE_GROUP_SIZE, REPLICATION_PROTOCOL_ID};
use ant_node::replication::protocol::{
    ReplicationMessage, ReplicationMessageBody, VerificationRequest,
};
use clap::{Parser, Subcommand};
use color_eyre::eyre::{eyre, Result, WrapErr};
use evmlib::common::Amount;
use evmlib::wallet::Wallet;
use evmlib::{EncodedPeerId, PaymentQuote, ProofOfPayment};
use rand::{Rng, RngCore};
use saorsa_pqc::api::sig::ml_dsa_65;
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
        /// How many of the records, the first ones, go to one storer only:
        /// the nearest node that quoted.
        #[arg(long, default_value_t = 0)]
        one_storer: usize,
        /// How many pointers to store after the records, each paid at its
        /// state and sent to one storer only.
        #[arg(long, default_value_t = 0)]
        pointers: usize,
        /// Where to write what happened, as JSON.
        #[arg(long)]
        out: PathBuf,
    },
    /// Run the fault cases after the uploads.
    Faults {
        /// The devnet's manifest.
        #[arg(long)]
        manifest: PathBuf,
        /// What `upload` wrote: its stored records are PUT again.
        #[arg(long)]
        uploads: PathBuf,
        /// The file this tree's nodes read their faults from
        /// (`ANT_TEST_SEAM_FAULTS_FILE`).
        #[arg(long)]
        faults_file: PathBuf,
        /// The peer ids of this tree's nodes, in hex: the only nodes the
        /// seam reaches. None skips the seam's cases.
        #[arg(long, value_delimiter = ',')]
        new_peers: Vec<String>,
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
    /// `chunk` or `pointer`.
    kind: String,
    /// Whether its PUT went to one storer only.
    one_storer: bool,
    /// A pointer's state, in hex: what it was paid at.
    state_id: Option<String>,
    /// The nodes nearest its address, nearest first.
    close_group: Vec<String>,
    /// Its paid group at upload: up to `PAID_LIST_CLOSE_GROUP_SIZE` nodes
    /// nearest its address.
    paid_group: Vec<String>,
    /// Each node's answer to the quote request.
    quotes: Vec<Answer>,
    /// The node whose quote was paid, and the amount, in wei.
    paid_peer: Option<String>,
    paid_amount: Option<String>,
    /// The payment's transactions.
    tx_hashes: Vec<String>,
    /// Each quoting node's answer to the PUT.
    puts: Vec<Answer>,
    /// Whether at least a majority of the close group stored it, or, for a
    /// record PUT to one storer, that storer.
    stored: bool,
    /// Why not, if it wasn't.
    error: Option<String>,
}

/// What `upload` writes.
#[derive(Debug, Serialize, Deserialize)]
struct Uploads {
    /// The client's own peer id, so its traffic can be told from the nodes'.
    client: String,
    records: Vec<Upload>,
}

/// What the nodes that stored a record, and the nodes nearest it now,
/// answered when asked for it.
#[derive(Debug, Serialize, Deserialize)]
struct Check {
    address: String,
    /// `chunk` or `pointer`.
    kind: String,
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
    /// For a chunk, each member of its paid group at upload: whether it
    /// lists the key in its paid list (`listed`, `not_listed`, or what went
    /// wrong).
    paid_listed: Vec<Answer>,
}

/// One fault case: what was done, at which node, and what the nodes
/// answered.
#[derive(Debug, Serialize, Deserialize)]
struct FaultCase {
    case: String,
    /// The record it was run with, in hex.
    address: String,
    /// The node the fault was aimed at, if one.
    peer: Option<String>,
    /// The PUTs' answers.
    answers: Vec<Answer>,
    /// What came of it, in a few words.
    note: String,
}

/// What `faults` writes.
#[derive(Debug, Serialize, Deserialize)]
struct Faults {
    client: String,
    cases: Vec<FaultCase>,
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
            one_storer,
            pointers,
            out,
        } => {
            let manifest = read_manifest(&manifest)?;
            let client = join(&manifest).await?;
            let result = upload(&client, &manifest, &sizes, one_storer, pointers).await;
            client.shutdown().await.ok();
            write_json(&out, &result?)
        }
        Command::Faults {
            manifest,
            uploads,
            faults_file,
            new_peers,
            out,
        } => {
            let manifest = read_manifest(&manifest)?;
            let uploads: Uploads = serde_json::from_slice(
                &std::fs::read(&uploads)
                    .wrap_err_with(|| format!("reading {}", uploads.display()))?,
            )?;
            let new_peers = new_peers
                .iter()
                .filter(|peer| !peer.is_empty())
                .map(|peer| bytes32(peer).map(PeerId::from_bytes))
                .collect::<Result<Vec<_>>>()?;
            let client = join(&manifest).await?;
            let result = faults(&client, &manifest, &uploads, &faults_file, &new_peers).await;
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
        found.truncate(CLOSE_GROUP_SIZE);
        if found.len() == CLOSE_GROUP_SIZE {
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

/// Up to `PAID_LIST_CLOSE_GROUP_SIZE` nodes nearest `address`, nearest
/// first, from one network lookup: the paid group, as far as the client
/// sees it. The client itself is left out.
async fn paid_group(client: &P2PNode, address: &[u8; 32]) -> Vec<PeerId> {
    let mut found: Vec<PeerId> = client
        .dht()
        .find_closest_nodes(address, PAID_LIST_CLOSE_GROUP_SIZE + 1)
        .await
        .map(|nodes| nodes.into_iter().map(|node| node.peer_id).collect())
        .unwrap_or_default();
    found.retain(|peer| peer != client.peer_id());
    found.sort_by_key(|peer| xor_distance(address, peer.as_bytes()));
    found.dedup();
    found.truncate(PAID_LIST_CLOSE_GROUP_SIZE);
    found
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
    content: [u8; 32],
    size: usize,
    data_type: u32,
) -> (PeerId, std::result::Result<Quoted, String>) {
    let mut request = ChunkQuoteRequestV2::new(content, u64::try_from(size).unwrap_or(u64::MAX));
    request.data_type = data_type;
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

async fn put_pointer(client: &P2PNode, peer: PeerId, record: &Pointer, proof: Vec<u8>) -> Answer {
    let request = PointerPutRequest::with_payment(record.to_bytes().into(), proof);
    let outcome = ask(
        client,
        &peer,
        ChunkMessageBody::PointerPutRequest(request),
        PUT_TIMEOUT,
        |body| match body {
            ChunkMessageBody::PointerPutResponse(PointerPutResponse::Success { .. }) => {
                Some(Ok("stored".to_string()))
            }
            ChunkMessageBody::PointerPutResponse(PointerPutResponse::Unchanged { .. }) => {
                Some(Ok("already_stored".to_string()))
            }
            ChunkMessageBody::PointerPutResponse(other) => Some(Err(format!("refused: {other:?}"))),
            _ => None,
        },
    )
    .await;
    Answer {
        peer: peer.to_hex(),
        outcome: outcome.unwrap_or_else(|e| e),
    }
}

fn new_record(address: [u8; 32], size: usize, kind: &str, one_storer: bool) -> Upload {
    Upload {
        address: hex::encode(address),
        size,
        kind: kind.to_string(),
        one_storer,
        state_id: None,
        close_group: Vec::new(),
        paid_group: Vec::new(),
        quotes: Vec::new(),
        paid_peer: None,
        paid_amount: None,
        tx_hashes: Vec::new(),
        puts: Vec::new(),
        stored: false,
        error: None,
    }
}

fn wallet_of(manifest: &DevnetManifest) -> Result<Wallet> {
    let evm = manifest
        .evm
        .as_ref()
        .ok_or_else(|| eyre!("the manifest has no chain: start the devnet with --enable-evm"))?;
    let network = evmlib::Network::new_custom(
        &evm.rpc_url,
        &evm.payment_token_address,
        &evm.payment_vault_address,
    );
    Wallet::new_from_private_key(network, &evm.wallet_private_key)
        .map_err(|e| eyre!("the manifest's wallet key: {e}"))
}

fn print_record(index: usize, record: &Upload) {
    println!(
        "{} {index} ({} bytes{}) at {}: {}",
        record.kind,
        record.size,
        if record.one_storer {
            ", one storer"
        } else {
            ""
        },
        record.address,
        record.error.as_deref().unwrap_or(if record.stored {
            "stored"
        } else {
            "not stored"
        })
    );
}

async fn upload(
    client: &P2PNode,
    manifest: &DevnetManifest,
    sizes: &[usize],
    one_storer: usize,
    pointers: usize,
) -> Result<Uploads> {
    let wallet = wallet_of(manifest)?;
    let mut records = Vec::with_capacity(sizes.len() + pointers);
    for (index, size) in sizes.iter().copied().enumerate() {
        let mut content = vec![0u8; size];
        rand::thread_rng().fill_bytes(&mut content);
        let address = compute_address(&content);
        let mut record = new_record(address, size, "chunk", index < one_storer);
        let to = if record.one_storer {
            Storers::Nearest
        } else {
            Storers::AllQuoted
        };
        if let Err(e) = store(client, &wallet, address, &content, to, &mut record).await {
            record.error = Some(format!("{e:#}"));
        }
        print_record(index, &record);
        records.push(record);
    }
    for index in 0..pointers {
        let mut record = new_record([0; 32], POINTER_WIRE_LEN, "pointer", true);
        if let Err(e) = store_pointer(client, &wallet, &mut record).await {
            record.error = Some(format!("{e:#}"));
        }
        print_record(index, &record);
        records.push(record);
    }
    Ok(Uploads {
        client: client.peer_id().to_hex(),
        records,
    })
}

/// Which nodes a record's PUT goes to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Storers {
    /// Every node that quoted, as a client sends it.
    AllQuoted,
    /// The nearest node that quoted only: the rest of the close group gets
    /// the record by that storer's offers, with the slim proof.
    Nearest,
}

/// Quotes from `group` for `content`, and pays the median one as a client
/// pays, 3 times its price, noting each step in `record`. Returns the nodes
/// that quoted, nearest first, and the proof, every quote in it.
async fn pay(
    client: &P2PNode,
    wallet: &Wallet,
    group: &[PeerId],
    content: [u8; 32],
    size: usize,
    data_type: u32,
    record: &mut Upload,
) -> Result<(Vec<PeerId>, Vec<u8>)> {
    let answers = futures::future::join_all(
        group
            .iter()
            .map(|peer| quote(client, *peer, content, size, data_type)),
    )
    .await;
    let mut quoted = Vec::new();
    for (peer, answer) in answers {
        record.quotes.push(Answer {
            peer: peer.to_hex(),
            outcome: answer
                .as_ref()
                .map_or_else(Clone::clone, |_| "quoted".into()),
        });
        if let Ok(answer) = answer {
            quoted.push(answer);
        }
    }
    if quoted.len() < CLOSE_GROUP_MAJORITY {
        return Err(eyre!("only {} of the close group quoted", quoted.len()));
    }
    let nearest_first: Vec<PeerId> = quoted.iter().map(|quoted| quoted.peer).collect();

    // Paid as a client pays: the median quote, 3 times its price.
    quoted.sort_by_key(|quoted| quoted.quote.price);
    let median = &quoted[quoted.len() / 2];
    let amount = median
        .quote
        .price
        .checked_mul(Amount::from(PAYMENT_MULTIPLIER))
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
    Ok((nearest_first, proof))
}

/// Notes the close group and the paid group of `address` in `record`, and
/// returns the close group.
async fn groups(client: &P2PNode, address: &[u8; 32], record: &mut Upload) -> Result<Vec<PeerId>> {
    let group = close_group(client, address).await?;
    record.close_group = group.iter().map(PeerId::to_hex).collect();
    record.paid_group = paid_group(client, address)
        .await
        .iter()
        .map(PeerId::to_hex)
        .collect();
    Ok(group)
}

/// Quotes, pays and stores one record, noting each step in `record`.
async fn store(
    client: &P2PNode,
    wallet: &Wallet,
    address: [u8; 32],
    content: &[u8],
    to: Storers,
    record: &mut Upload,
) -> Result<()> {
    let group = groups(client, &address, record).await?;
    let (quoted, proof) = pay(
        client,
        wallet,
        &group,
        address,
        content.len(),
        DATA_TYPE_CHUNK,
        record,
    )
    .await?;
    let targets: &[PeerId] = match to {
        Storers::AllQuoted => &quoted,
        Storers::Nearest => &quoted[..1],
    };
    record.puts = futures::future::join_all(
        targets
            .iter()
            .map(|peer| put(client, *peer, address, content, proof.clone())),
    )
    .await;
    judge_puts(record)
}

/// Whether the record's PUTs stored it: by a majority of the close group, or
/// by its one storer.
fn judge_puts(record: &mut Upload) -> Result<()> {
    let stored = record
        .puts
        .iter()
        .filter(|answer| answer.outcome == "stored" || answer.outcome == "already_stored")
        .count();
    let needed = if record.one_storer {
        1
    } else {
        CLOSE_GROUP_MAJORITY
    };
    record.stored = stored >= needed;
    if !record.stored {
        return Err(eyre!("{stored} of {} nodes stored it", record.puts.len()));
    }
    Ok(())
}

/// Signs, pays for and stores a new pointer at one storer, noting each step
/// in `record`: quoted and paid at its state, as a client pays for one.
async fn store_pointer(client: &P2PNode, wallet: &Wallet, record: &mut Upload) -> Result<()> {
    let (pk, sk) = ml_dsa_65()
        .generate_keypair()
        .map_err(|e| eyre!("a pointer key: {e:?}"))?;
    let mut target = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut target);
    let pointer = Pointer::sign(
        &sk,
        &pk,
        0,
        PointerTarget::new(PointerTargetKind::Chunk, target),
    )
    .map_err(|e| eyre!("signing the pointer: {e:?}"))?;
    let address = pointer.address();
    record.address = hex::encode(address);
    record.state_id = Some(hex::encode(pointer.state_id()));
    let group = groups(client, &address, record).await?;
    let (quoted, proof) = pay(
        client,
        wallet,
        &group,
        pointer.state_id(),
        POINTER_WIRE_LEN,
        DATA_TYPE_POINTER,
        record,
    )
    .await?;
    record.puts = vec![put_pointer(client, quoted[0], &pointer, proof).await];
    judge_puts(record)
}

// --------------------------------------------------------------------------
// Faults
// --------------------------------------------------------------------------

/// The proofs a retry answered from the payment cache carries: none, too
/// short, merkle-tagged, and garbled.
fn cache_retry_proofs() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("empty", Vec::new()),
        ("16 bytes", vec![PROOF_TAG_SINGLE_NODE; 16]),
        ("merkle-tagged", {
            let mut proof = vec![0x5Au8; 64];
            proof[0] = PROOF_TAG_MERKLE;
            proof
        }),
        ("garbled", vec![PROOF_TAG_SINGLE_NODE; 64]),
    ]
}

/// Adds a line to the faults file every node of this tree reads.
fn add_fault(faults_file: &Path, word: &str, value: &[u8; 32]) -> Result<()> {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(faults_file)
        .wrap_err_with(|| format!("opening {}", faults_file.display()))?;
    writeln!(file, "{word} {}", hex::encode(value))?;
    Ok(())
}

/// The fault cases, after the uploads: repeated PUTs of every stored chunk,
/// and, at nodes of this tree, holding notices lost on the way to one
/// storer, a failed paid-list write, and retries answered from the payment
/// cache with a proof the slim builder declines.
async fn faults(
    client: &P2PNode,
    manifest: &DevnetManifest,
    uploads: &Uploads,
    faults_file: &Path,
    new_peers: &[PeerId],
) -> Result<Faults> {
    let wallet = wallet_of(manifest)?;
    let mut cases = Vec::new();

    // Repeated PUTs, answered "already stored": holding notices at most.
    for upload in uploads
        .records
        .iter()
        .filter(|upload| upload.stored && upload.kind == "chunk" && !upload.one_storer)
    {
        let address = bytes32(&upload.address)?;
        let content = get_chunk(client, &address, &upload.puts).await;
        let Some(content) = content else {
            cases.push(case(
                "repeated PUT",
                &upload.address,
                None,
                Vec::new(),
                "no node returned the record",
            ));
            continue;
        };
        let storers: Vec<PeerId> = upload
            .puts
            .iter()
            .filter(|answer| answer.outcome == "stored" || answer.outcome == "already_stored")
            .map(|answer| bytes32(&answer.peer).map(PeerId::from_bytes))
            .collect::<Result<_>>()?;
        let answers = futures::future::join_all(storers.iter().map(|peer| {
            put(
                client,
                *peer,
                address,
                &content,
                vec![PROOF_TAG_SINGLE_NODE; 64],
            )
        }))
        .await;
        let note = if answers.iter().all(|a| a.outcome == "already_stored") {
            "every storer answered already stored"
        } else {
            "a storer answered other than already stored"
        };
        cases.push(case("repeated PUT", &upload.address, None, answers, note));
    }

    if new_peers.is_empty() {
        println!("no node of this tree: the seam's fault cases are skipped");
        return Ok(Faults {
            client: client.peer_id().to_hex(),
            cases,
        });
    }

    // Holding notices lost on the way to one storer of this tree: it hears
    // from no other node of this tree, and offers to every peer when its hold
    // ends, older nodes included.
    {
        let (address, content, mut record) = fresh_chunk(4096);
        let group = groups(client, &address, &mut record).await?;
        if let Some(deaf) = group.iter().find(|peer| new_peers.contains(peer)) {
            add_fault(faults_file, "drop-holding-to", deaf.as_bytes())?;
            let (quoted, proof) = pay(
                client,
                &wallet,
                &group,
                address,
                content.len(),
                DATA_TYPE_CHUNK,
                &mut record,
            )
            .await?;
            let answers = futures::future::join_all(
                quoted
                    .iter()
                    .map(|peer| put(client, *peer, address, &content, proof.clone())),
            )
            .await;
            cases.push(case(
                "holding notices lost",
                &record.address,
                Some(deaf.to_hex()),
                answers,
                "every holding notice to the named storer was dropped",
            ));
        }
    }

    // A failed paid-list write at every storer of this tree: each sends the
    // whole paid group the slim proof when it announces the write.
    {
        let (address, content, mut record) = fresh_chunk(65536);
        let group = groups(client, &address, &mut record).await?;
        add_fault(faults_file, "fail-paid-list", &address)?;
        let (quoted, proof) = pay(
            client,
            &wallet,
            &group,
            address,
            content.len(),
            DATA_TYPE_CHUNK,
            &mut record,
        )
        .await?;
        let answers = futures::future::join_all(
            quoted
                .iter()
                .map(|peer| put(client, *peer, address, &content, proof.clone())),
        )
        .await;
        cases.push(case(
            "failed paid-list write",
            &record.address,
            None,
            answers,
            "the paid-list write failed at every storer of this tree",
        ));
    }

    // Retries answered from the payment cache: a storer of this tree passes
    // the payment check and then fails the store; the retry carries a proof
    // the slim builder declines, so the storer sends holding notices only.
    // The rest of the close group gets the paid PUT after, as a client sends
    // it to every node that quoted.
    for (name, bad) in cache_retry_proofs() {
        let (address, content, mut record) = fresh_chunk(4096);
        let group = groups(client, &address, &mut record).await?;
        let Some(storer) = group.iter().find(|peer| new_peers.contains(peer)).copied() else {
            continue;
        };
        add_fault(faults_file, "fail-store", &address)?;
        let (quoted, proof) = pay(
            client,
            &wallet,
            &group,
            address,
            content.len(),
            DATA_TYPE_CHUNK,
            &mut record,
        )
        .await?;
        let first = put(client, storer, address, &content, proof.clone()).await;
        let retry = put(client, storer, address, &content, bad).await;
        let others = futures::future::join_all(
            quoted
                .iter()
                .filter(|peer| **peer != storer)
                .map(|peer| put(client, *peer, address, &content, proof.clone())),
        )
        .await;
        let note = if retry.outcome == "stored" {
            "the retry was answered from the payment cache"
        } else {
            "the retry was not stored"
        };
        cases.push(case(
            &format!("cache retry, {name} proof"),
            &record.address,
            Some(storer.to_hex()),
            [vec![first, retry], others].concat(),
            note,
        ));
    }

    for case in &cases {
        println!("{} at {}: {}", case.case, case.address, case.note);
    }
    Ok(Faults {
        client: client.peer_id().to_hex(),
        cases,
    })
}

fn case(
    name: &str,
    address: &str,
    peer: Option<String>,
    answers: Vec<Answer>,
    note: &str,
) -> FaultCase {
    FaultCase {
        case: name.to_string(),
        address: address.to_string(),
        peer,
        answers,
        note: note.to_string(),
    }
}

/// A record of random bytes, and its upload's notes.
fn fresh_chunk(size: usize) -> ([u8; 32], Vec<u8>, Upload) {
    let mut content = vec![0u8; size];
    rand::thread_rng().fill_bytes(&mut content);
    let address = compute_address(&content);
    (address, content, new_record(address, size, "chunk", false))
}

/// A stored chunk's bytes, from the first of its storers that returns them.
async fn get_chunk(client: &P2PNode, address: &[u8; 32], puts: &[Answer]) -> Option<Vec<u8>> {
    for answer in puts {
        let Ok(peer) = bytes32(&answer.peer).map(PeerId::from_bytes) else {
            continue;
        };
        let got = ask(
            client,
            &peer,
            ChunkMessageBody::GetRequest(ChunkGetRequest::new(*address)),
            REQUEST_TIMEOUT,
            |body| match body {
                ChunkMessageBody::GetResponse(ChunkGetResponse::Success { content, .. }) => {
                    Some(Ok(content.to_vec()))
                }
                ChunkMessageBody::GetResponse(other) => Some(Err(format!("{other:?}"))),
                _ => None,
            },
        )
        .await;
        if let Ok(content) = got {
            if compute_address(&content) == *address {
                return Some(content);
            }
        }
    }
    None
}

/// 32 bytes from hex: an address or a peer id.
fn bytes32(hex_text: &str) -> Result<[u8; 32]> {
    hex::decode(hex_text)?
        .try_into()
        .map_err(|_| eyre!("{hex_text} is not 32 bytes"))
}

/// Whether `peer` lists `key` in its paid list, asked by the verification
/// request nodes send each other: `listed`, `not_listed`, or what went wrong.
async fn paid_listing(client: &P2PNode, peer: &PeerId, key: [u8; 32]) -> Answer {
    let request = ReplicationMessage {
        request_id: rand::thread_rng().gen(),
        body: ReplicationMessageBody::VerificationRequest(VerificationRequest {
            keys: vec![key],
            paid_list_check_indices: vec![0],
        }),
    };
    let outcome = match request.encode() {
        Err(e) => format!("encode: {e}"),
        Ok(bytes) => match client
            .send_request(peer, REPLICATION_PROTOCOL_ID, bytes, REQUEST_TIMEOUT)
            .await
        {
            Err(e) => format!("no answer: {e}"),
            Ok(response) => match ReplicationMessage::decode(&response.data).map(|m| m.body) {
                Ok(ReplicationMessageBody::VerificationResponse(answer)) => {
                    match answer.results.iter().find(|result| result.key == key) {
                        Some(result) if result.paid == Some(true) => "listed".to_string(),
                        Some(result) if result.paid == Some(false) => "not_listed".to_string(),
                        Some(_) => "paid list unreadable".to_string(),
                        None => "the key was not answered".to_string(),
                    }
                }
                Ok(other) => format!("answered {other:?}"),
                Err(e) => format!("answer does not decode: {e}"),
            },
        },
    };
    Answer {
        peer: peer.to_hex(),
        outcome,
    }
}

/// `peer`'s answer for `record`: `held`, `not_found`, or what went wrong. A
/// pointer is held when the node returns the state it was stored at.
async fn read_back(client: &P2PNode, peer: &PeerId, upload: &Upload) -> Answer {
    let outcome = match bytes32(&upload.address) {
        Err(e) => Err(format!("{e}")),
        Ok(address) if upload.kind == "pointer" => {
            let state_id = upload.state_id.clone().unwrap_or_default();
            ask(
                client,
                peer,
                ChunkMessageBody::PointerGetRequest(PointerGetRequest::new(address)),
                REQUEST_TIMEOUT,
                move |body| match body {
                    ChunkMessageBody::PointerGetResponse(PointerGetResponse::Success {
                        record,
                    }) => Some(match Pointer::from_bytes(&record) {
                        Ok(pointer) if hex::encode(pointer.state_id()) == state_id => {
                            Ok("held".to_string())
                        }
                        Ok(_) => Err("returned another state".to_string()),
                        Err(e) => Err(format!("returned a record that does not parse: {e:?}")),
                    }),
                    ChunkMessageBody::PointerGetResponse(PointerGetResponse::NotFound {
                        ..
                    }) => Some(Ok("not_found".to_string())),
                    ChunkMessageBody::PointerGetResponse(other) => Some(Err(format!("{other:?}"))),
                    _ => None,
                },
            )
            .await
        }
        Ok(address) => {
            ask(
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
            .await
        }
    };
    Answer {
        peer: peer.to_hex(),
        outcome: outcome.unwrap_or_else(|e| e),
    }
}

/// Asks the nodes that stored each record, and the nodes nearest it now,
/// for it. Nodes that joined after it was stored get it only by repair,
/// which waits for a record to age, so the storers say whether it was kept.
/// A record PUT to one storer counts its close group at upload as its
/// storers: they get it by the storer's offers. For a chunk, it also asks
/// the paid group at upload whether each member lists it in its paid list.
async fn check(client: &P2PNode, uploads: &Uploads) -> Result<Checks> {
    let mut records = Vec::with_capacity(uploads.records.len());
    for upload in uploads.records.iter().filter(|upload| upload.stored) {
        let address = bytes32(&upload.address)?;
        let storers = if upload.one_storer {
            upload
                .close_group
                .iter()
                .map(|peer| bytes32(peer).map(PeerId::from_bytes))
                .collect::<Result<Vec<_>>>()?
        } else {
            upload
                .puts
                .iter()
                .filter(|answer| answer.outcome == "stored" || answer.outcome == "already_stored")
                .map(|answer| bytes32(&answer.peer).map(PeerId::from_bytes))
                .collect::<Result<Vec<_>>>()?
        };
        let group = close_group(client, &address).await?;
        let mut asked = storers.clone();
        asked.extend(group.iter().filter(|peer| !storers.contains(peer)));
        let answers =
            futures::future::join_all(asked.iter().map(|peer| read_back(client, peer, upload)))
                .await;
        let held = |peers: &[PeerId]| {
            asked
                .iter()
                .zip(&answers)
                .filter(|(peer, answer)| peers.contains(peer) && answer.outcome == "held")
                .count()
        };
        let (held_by_storers, held_by_close_group) = (held(&storers), held(&group));
        let paid_listed = if upload.kind == "chunk" {
            let members = upload
                .paid_group
                .iter()
                .map(|peer| bytes32(peer).map(PeerId::from_bytes))
                .collect::<Result<Vec<_>>>()?;
            futures::future::join_all(
                members
                    .iter()
                    .map(|peer| paid_listing(client, peer, address)),
            )
            .await
        } else {
            Vec::new()
        };
        println!(
            "{} {}: held by {held_by_storers} of the {} that stored it, and by \
             {held_by_close_group} of the {} nearest it now; listed by {} of the {} in its \
             paid group at upload",
            upload.kind,
            upload.address,
            storers.len(),
            group.len(),
            paid_listed.iter().filter(|a| a.outcome == "listed").count(),
            paid_listed.len(),
        );
        records.push(Check {
            address: upload.address.clone(),
            kind: upload.kind.clone(),
            storers: storers.iter().map(PeerId::to_hex).collect(),
            close_group: group.iter().map(PeerId::to_hex).collect(),
            answers,
            held_by_storers,
            held_by_close_group,
            paid_listed,
        });
    }
    Ok(Checks {
        client: client.peer_id().to_hex(),
        records,
    })
}

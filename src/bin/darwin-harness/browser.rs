//! What browser sessions cost a node (`scripts/measure_browser_sessions.py`).
//!
//! `browser-node` runs a two-node devnet on loopback whose first node serves
//! browsers under the `[webrtc_direct]` limits of a node config file, with
//! records of given sizes stored on it, until its standard input closes.
//! `browser-load` opens sessions to that node as browsers do (ICE, DTLS,
//! SCTP, the post-quantum handshake and HELLO), holds them idle, then has
//! each read records over its data channel, one at a time, and closes them.
//! The two run as separate processes, so the node's memory is measured
//! without the sessions' client side.

use ant_node::ant_protocol::{ChunkGetRequest, ChunkGetResponse, ChunkMessage, ChunkMessageBody};
use ant_node::config::NodeConfig;
use ant_node::devnet::{Devnet, DevnetConfig};
use ant_node::BrowserEndpoint;
use color_eyre::eyre::{eyre, Result, WrapErr};
use rand::{Rng, RngCore};
use saorsa_transport::transport::{WebRtcCertificateHash, WebRtcDirectAddr};
use saorsa_transport::webrtc::{
    decode_pq_frame, encode_pq_frame, parse_response_frame, pq_frame_length, PqClientHandshake,
    PqSession, BROWSER_PROTOCOL_VERSION, MAX_BROWSER_FRAME_BYTES, PQ_ENCRYPTED_OVERHEAD_BYTES,
    PQ_SERVER_ACCEPT_BYTES, WEBRTC_DIRECT_DATA_CHANNEL, WEBRTC_WRITE_CHUNK_BYTES,
};
use saorsa_transport::webrtc_direct::{WebRtcDataChannel, WebRtcDirectClient};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Semaphore;

/// Sessions dialled at once. The listener keeps at most 4 unproven
/// connectivity checks from one address at a time, so more would only be
/// dropped and retried.
const DIAL_CONCURRENCY: usize = 4;

/// How long one session may take to open, and one read to finish.
const DIAL_TIMEOUT: Duration = Duration::from_secs(60);
const READ_TIMEOUT: Duration = Duration::from_secs(120);

/// The chain ID the devnet's stand-in RPC answers with (Anvil's).
const CHAIN_ID: &str = "0x7a69";

/// What `browser-node` writes once its listener is up and its records stored.
#[derive(Debug, Serialize, Deserialize)]
pub struct Ready {
    /// The process to measure.
    pub pid: u32,
    /// The listener's endpoint.
    pub endpoint: String,
    /// The records stored on the node.
    pub records: Vec<Record>,
}

/// One stored record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub address: String,
    pub size: usize,
}

/// `SIZExCOUNT,...`: how many records of each size.
fn parse_records(spec: &str) -> Result<Vec<(usize, usize)>> {
    spec.split(',')
        .filter(|part| !part.trim().is_empty())
        .map(|part| {
            let (size, count) = part
                .trim()
                .split_once('x')
                .ok_or_else(|| eyre!("{part} is not SIZExCOUNT"))?;
            Ok((size.parse()?, count.parse()?))
        })
        .collect()
}

/// Answers every JSON-RPC call with the chain ID: all a browser listener asks
/// its chain at start. Reads of records never reach the chain.
async fn stand_in_rpc() -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/", listener.local_addr()?);
    let task = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = Vec::new();
                let body = loop {
                    let mut buffer = [0u8; 4096];
                    match socket.read(&mut buffer).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buffer[..read]),
                    }
                    let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..end]).to_lowercase();
                    let length = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if request.len() >= end + 4 + length {
                        break request[end + 4..end + 4 + length].to_vec();
                    }
                };
                let id = serde_json::from_slice::<Value>(&body)
                    .ok()
                    .and_then(|call| call.get("id").cloned())
                    .unwrap_or(json!(1));
                let answer = json!({"jsonrpc": "2.0", "id": id, "result": CHAIN_ID}).to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{answer}",
                    answer.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    Ok((url, task))
}

/// Runs the devnet until standard input closes.
pub async fn node(
    config: Option<PathBuf>,
    records: &str,
    ready: &Path,
    snapshots: Option<PathBuf>,
) -> Result<()> {
    let limits = match &config {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .wrap_err_with(|| format!("reading {}", path.display()))?;
            toml::from_str::<NodeConfig>(&text)
                .wrap_err_with(|| format!("parsing {}", path.display()))?
                .webrtc_direct
        }
        None => NodeConfig::default().webrtc_direct,
    };
    let (rpc_url, rpc) = stand_in_rpc().await?;
    let data = tempfile::tempdir()?;
    let mut devnet_config = DevnetConfig::minimal();
    devnet_config.node_count = 2;
    devnet_config.bootstrap_count = 1;
    devnet_config.base_port = 0;
    devnet_config.webrtc_direct = true;
    devnet_config.webrtc_direct_limits = Some(limits);
    devnet_config.data_dir = data.path().join("devnet");
    devnet_config.spawn_delay = Duration::from_millis(20);
    devnet_config.evm_network = Some(evmlib::Network::new_custom(
        &rpc_url,
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
    ));
    let mut devnet = Devnet::new(devnet_config).await?;
    devnet.start().await?;
    let result = serve(&devnet, records, ready, snapshots).await;
    devnet.shutdown().await.ok();
    rpc.abort();
    result
}

async fn serve(
    devnet: &Devnet,
    records: &str,
    ready: &Path,
    snapshots: Option<PathBuf>,
) -> Result<()> {
    let (protocol, diagnostics) = devnet
        .test_browser_node(0)
        .ok_or_else(|| eyre!("the devnet has no browser node"))?;
    let endpoint = devnet
        .browser_endpoints()
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("the browser node has no endpoint"))?
        .endpoint;
    let snapshot = diagnostics.snapshot();
    let mut stored = Vec::new();
    for (size, count) in parse_records(records)? {
        for _ in 0..count {
            let mut content = vec![0u8; size];
            rand::thread_rng().fill_bytes(&mut content);
            let address = ant_node::client::compute_address(&content);
            protocol.storage().put(&address, &content).await?;
            stored.push(Record {
                address: hex::encode(address),
                size,
            });
        }
    }
    let info = Ready {
        pid: std::process::id(),
        endpoint: endpoint.multiaddr,
        records: stored,
    };
    let partial = ready.with_extension("partial");
    std::fs::write(&partial, serde_json::to_vec_pretty(&info)?)?;
    std::fs::rename(&partial, ready)?;
    eprintln!(
        "browser node ready: {} records, max_connections {}, max_channels {}, \
         max_concurrent_requests {}, max_in_flight_bytes {}",
        info.records.len(),
        snapshot.max_connections,
        snapshot.max_channels,
        snapshot.max_concurrent_requests,
        snapshot.max_in_flight_bytes
    );

    let sampler = snapshots.map(|path| {
        let diagnostics = diagnostics.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let Ok(mut file) = tokio::fs::File::create(&path).await else {
                return;
            };
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                let s = diagnostics.snapshot();
                let line = json!({
                    "t": started.elapsed().as_secs_f64(),
                    "sessions": s.active_connections,
                    "transport_connected": s.transport.active_connections,
                    "transport_connecting": s.transport.connecting_connections,
                    "transport_refused": s.transport.rejected_connections,
                    "channels": s.active_channels,
                    "requests": s.active_requests,
                    "in_flight_bytes": s.in_flight_bytes,
                    "connection_rejections": s.connection_rejections,
                    "channel_rejections": s.channel_rejections,
                    "request_rejections": s.request_rejections,
                    "rate_rejections": s.rate_rejections,
                    "byte_rejections": s.byte_rejections,
                    "connection_errors": s.connection_errors,
                    "channel_errors": s.channel_errors,
                    "bytes_sent": s.transport.bytes_sent,
                });
                if file
                    .write_all(format!("{line}\n").as_bytes())
                    .await
                    .is_err()
                {
                    return;
                }
            }
        })
    });

    // Until standard input closes.
    let mut stdin = tokio::io::stdin();
    let mut sink = Vec::new();
    stdin.read_to_end(&mut sink).await.ok();
    if let Some(sampler) = sampler {
        sampler.abort();
    }
    Ok(())
}

/// One browser session: its association and post-quantum channel.
struct Session {
    client: WebRtcDirectClient,
    pq: PqSession,
    next_id: u64,
}

impl Session {
    async fn open(endpoint: &BrowserEndpoint) -> Result<Self> {
        let parsed = endpoint.parse().map_err(|e| eyre!("{e}"))?;
        let address = WebRtcDirectAddr::new(
            parsed.socket_addr().map_err(|e| eyre!("{e}"))?,
            WebRtcCertificateHash::new(parsed.certificate_hash),
        )
        .map_err(|e| eyre!("{e}"))?;
        let client = WebRtcDirectClient::dial(&address, WEBRTC_DIRECT_DATA_CHANNEL)
            .await
            .map_err(|e| eyre!("dial: {e}"))?;
        let expected = parsed.peer_id_bytes().map_err(|e| eyre!("{e}"))?;
        let (handshake, hello) = PqClientHandshake::start().map_err(|e| eyre!("{e}"))?;
        send(client.data_channel(), &hello).await?;
        let accept = receive(client.data_channel(), PQ_SERVER_ACCEPT_BYTES).await?;
        let pq = handshake
            .finish(&accept, &expected)
            .map_err(|e| eyre!("handshake: {e}"))?;
        let mut session = Self {
            client,
            pq,
            next_id: 1,
        };
        let (header, _) = session
            .rpc(
                json!({"version": BROWSER_PROTOCOL_VERSION, "type": "hello"}),
                &[],
            )
            .await?;
        if header["status"] != "ok" {
            return Err(eyre!("HELLO answered {header}"));
        }
        Ok(session)
    }

    async fn rpc(&mut self, mut request: Value, content: &[u8]) -> Result<(Value, Vec<u8>)> {
        let id = self.next_id;
        self.next_id += 1;
        request["request_id"] = json!(id);
        request["content_length"] = json!(content.len());
        let mut frame = serde_json::to_vec(&request)?;
        frame.extend_from_slice(content);
        let sealed = self.pq.seal(&frame).map_err(|e| eyre!("seal: {e}"))?;
        send(self.client.data_channel(), &sealed).await?;
        let sealed = receive(
            self.client.data_channel(),
            MAX_BROWSER_FRAME_BYTES + PQ_ENCRYPTED_OVERHEAD_BYTES,
        )
        .await?;
        let plain = self.pq.open(&sealed).map_err(|e| eyre!("open: {e}"))?;
        let parsed = parse_response_frame(&plain).map_err(|e| eyre!("frame: {e}"))?;
        let header = serde_json::to_value(&parsed.header)?;
        if header["status"] == "error" {
            // An answer to request 0 is the node closing the channel.
            let closing = if header["request_id"] == json!(0) {
                "channel closed, "
            } else {
                ""
            };
            return Err(eyre!(
                "{closing}{}: {}",
                header["code"].as_str().unwrap_or("error"),
                header["message"].as_str().unwrap_or("")
            ));
        }
        if header["request_id"] != json!(id) {
            return Err(eyre!("answer to request {} for {id}", header["request_id"]));
        }
        Ok((header, parsed.content.to_vec()))
    }

    /// Read a record as a browser does: a chunk-protocol GET.
    async fn get(&mut self, address: [u8; 32]) -> Result<usize> {
        let message = ChunkMessage {
            request_id: self.next_id,
            body: ChunkMessageBody::GetRequest(ChunkGetRequest::new(address)),
        }
        .encode()?;
        let (header, content) = self
            .rpc(
                json!({"version": BROWSER_PROTOCOL_VERSION, "type": "chunk_protocol"}),
                &message,
            )
            .await?;
        if header["status"] != "ok" {
            return Err(eyre!("{}", header["status"]));
        }
        match ChunkMessage::decode(&content)?.body {
            ChunkMessageBody::GetResponse(ChunkGetResponse::Success { content, .. }) => {
                Ok(content.len())
            }
            ChunkMessageBody::GetResponse(other) => Err(eyre!("{other:?}")),
            _ => Err(eyre!("not a GET response")),
        }
    }
}

async fn send(channel: &WebRtcDataChannel, payload: &[u8]) -> Result<()> {
    let frame = encode_pq_frame(payload).map_err(|e| eyre!("{e}"))?;
    for chunk in frame.chunks(WEBRTC_WRITE_CHUNK_BYTES) {
        channel.send(chunk).await.map_err(|e| eyre!("send: {e}"))?;
    }
    Ok(())
}

async fn receive(channel: &WebRtcDataChannel, max: usize) -> Result<Vec<u8>> {
    let mut frame = Vec::new();
    loop {
        let message = channel.receive().await.map_err(|e| eyre!("receive: {e}"))?;
        if message.is_empty() {
            return Err(eyre!("channel closed"));
        }
        frame.extend_from_slice(&message);
        if let Some(expected) = pq_frame_length(&frame, max).map_err(|e| eyre!("{e}"))? {
            if frame.len() > expected {
                return Err(eyre!("frame has trailing bytes"));
            }
            if frame.len() == expected {
                return decode_pq_frame(&frame, max).map_err(|e| eyre!("{e}"));
            }
        }
    }
}

/// One finished read.
struct Read {
    size: usize,
    elapsed: Duration,
}

#[derive(Default)]
struct Reads {
    ok: Vec<Read>,
    failed: BTreeMap<String, u64>,
}

fn emit(event: &Value) {
    println!("{event}");
}

fn percentile(values: &mut [f64], share: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let at = ((values.len() - 1) as f64 * share).round() as usize;
    Some(values[at])
}

/// Opens `sessions` sessions, then on the first line of standard input reads
/// for `get_seconds`, then on the next closes them. Each step is reported on
/// standard output as one line of JSON; the summary goes to `out`.
#[allow(clippy::too_many_lines)]
pub async fn load(
    ready: &Path,
    sessions: usize,
    get_seconds: u64,
    max_rate: u32,
    out: &Path,
) -> Result<()> {
    let info: Ready = serde_json::from_slice(
        &std::fs::read(ready).wrap_err_with(|| format!("reading {}", ready.display()))?,
    )?;
    let endpoint = BrowserEndpoint {
        multiaddr: info.endpoint.clone(),
    };
    let records: Vec<([u8; 32], usize)> = info
        .records
        .iter()
        .map(|record| {
            let address: [u8; 32] = hex::decode(&record.address)?
                .try_into()
                .map_err(|_| eyre!("bad address"))?;
            Ok((address, record.size))
        })
        .collect::<Result<_>>()?;
    if records.is_empty() {
        return Err(eyre!("the node holds no records"));
    }

    // Open.
    let started = Instant::now();
    let gate = Arc::new(Semaphore::new(DIAL_CONCURRENCY));
    let dials = futures::future::join_all((0..sessions).map(|_| {
        let gate = Arc::clone(&gate);
        let endpoint = endpoint.clone();
        async move {
            let _permit = gate.acquire_owned().await;
            let begun = Instant::now();
            let session = tokio::time::timeout(DIAL_TIMEOUT, Session::open(&endpoint))
                .await
                .map_err(|_| eyre!("timed out"))
                .and_then(|opened| opened);
            (session, begun.elapsed())
        }
    }))
    .await;
    let open_seconds = started.elapsed().as_secs_f64();
    let mut open = Vec::new();
    let mut dial_ms = Vec::new();
    let mut open_failures: BTreeMap<String, u64> = BTreeMap::new();
    for (session, took) in dials {
        match session {
            Ok(session) => {
                open.push(session);
                dial_ms.push(took.as_secs_f64() * 1000.0);
            }
            Err(error) => *open_failures.entry(error.to_string()).or_default() += 1,
        }
    }
    let opened = open.len();
    emit(&json!({
        "event": "opened", "opened": opened, "failed": sessions - opened,
        "failures": open_failures, "seconds": open_seconds,
        "dial_ms_median": percentile(&mut dial_ms.clone(), 0.5),
        "dial_ms_p95": percentile(&mut dial_ms.clone(), 0.95),
    }));

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    lines.next_line().await?;

    // Read. A shared pacer hands out max_rate reads a second across every
    // session; each session reads one record at a time.
    let pacer = Arc::new(Semaphore::new(0));
    let pacing = {
        let pacer = Arc::clone(&pacer);
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(Duration::from_secs_f64(1.0 / f64::from(max_rate.max(1))));
            loop {
                tick.tick().await;
                if pacer.available_permits() < max_rate as usize {
                    pacer.add_permits(1);
                }
            }
        })
    };
    let deadline = Instant::now() + Duration::from_secs(get_seconds);
    let reading = Instant::now();
    let records = Arc::new(records);
    let workers = open.into_iter().map(|mut session| {
        let pacer = Arc::clone(&pacer);
        let records = Arc::clone(&records);
        async move {
            let mut reads = Reads::default();
            let mut alive = true;
            while alive && Instant::now() < deadline {
                let Ok(permit) = pacer.acquire().await else {
                    break;
                };
                permit.forget();
                if Instant::now() >= deadline {
                    break;
                }
                let (address, size) = records[rand::thread_rng().gen_range(0..records.len())];
                let begun = Instant::now();
                match tokio::time::timeout(READ_TIMEOUT, session.get(address)).await {
                    Ok(Ok(read)) if read == size => reads.ok.push(Read {
                        size,
                        elapsed: begun.elapsed(),
                    }),
                    Ok(Ok(read)) => {
                        *reads
                            .failed
                            .entry(format!("{read} bytes, not {size}"))
                            .or_default() += 1;
                    }
                    Ok(Err(error)) => {
                        let error = error.to_string();
                        // A closed channel ends the session's reads.
                        alive = !(error.starts_with("receive")
                            || error.starts_with("send")
                            || error.starts_with("channel closed"));
                        *reads.failed.entry(error).or_default() += 1;
                    }
                    Err(_) => {
                        alive = false;
                        *reads.failed.entry("timed out".to_string()).or_default() += 1;
                    }
                }
            }
            (session, reads, alive)
        }
    });
    let finished = futures::future::join_all(workers).await;
    pacing.abort();
    let read_seconds = reading.elapsed().as_secs_f64();
    let mut all = Reads::default();
    let mut still_open = Vec::new();
    let mut lost = 0usize;
    for (session, reads, alive) in finished {
        all.ok.extend(reads.ok);
        for (error, count) in reads.failed {
            *all.failed.entry(error).or_default() += count;
        }
        if !alive {
            lost += 1;
        }
        still_open.push(session);
    }
    let reads_ok = all.ok.len();
    let reads_failed: u64 = all.failed.values().sum();
    emit(&json!({
        "event": "read", "reads_ok": reads_ok, "reads_failed": reads_failed,
        "sessions_lost": lost, "seconds": read_seconds,
    }));

    lines.next_line().await?;
    // Close.
    for session in &still_open {
        session.client.close().await.ok();
    }
    drop(still_open);
    emit(&json!({"event": "closed"}));

    // Summary.
    let bytes: usize = all.ok.iter().map(|read| read.size).sum();
    let mut per_size = BTreeMap::new();
    let full = ant_node::ant_protocol::MAX_CHUNK_SIZE;
    let mut full_rate = Vec::new();
    let mut full_ms = Vec::new();
    let sizes: std::collections::BTreeSet<usize> = all.ok.iter().map(|read| read.size).collect();
    for size in sizes {
        let mut ms: Vec<f64> = all
            .ok
            .iter()
            .filter(|read| read.size == size)
            .map(|read| read.elapsed.as_secs_f64() * 1000.0)
            .collect();
        #[allow(clippy::cast_precision_loss)]
        let mut rate: Vec<f64> = ms
            .iter()
            .map(|ms| size as f64 / 1e6 / (ms / 1000.0))
            .collect();
        if size == full {
            full_rate.clone_from(&rate);
            full_ms.clone_from(&ms);
        }
        per_size.insert(
            size.to_string(),
            json!({
                "reads": ms.len(),
                "ms_median": percentile(&mut ms.clone(), 0.5),
                "ms_p95": percentile(&mut ms, 0.95),
                "mb_per_s_median": percentile(&mut rate, 0.5),
            }),
        );
    }
    #[allow(clippy::cast_precision_loss)]
    let summary = json!({
        "sessions": sessions,
        "opened": opened,
        "open_failures": open_failures,
        "open_seconds": open_seconds,
        "dial_ms_median": percentile(&mut dial_ms.clone(), 0.5),
        "dial_ms_p95": percentile(&mut dial_ms, 0.95),
        "get_seconds": get_seconds,
        "read_seconds": read_seconds,
        "max_rate": max_rate,
        "reads_ok": reads_ok,
        "reads_failed": reads_failed,
        "read_failures": all.failed,
        "sessions_lost": lost,
        "node_mb_per_s": bytes as f64 / 1e6 / read_seconds.max(f64::EPSILON),
        "reads_per_s": reads_ok as f64 / read_seconds.max(f64::EPSILON),
        "full_read_mb_per_s_median": percentile(&mut full_rate, 0.5).unwrap_or(0.0),
        "full_read_ms_median": percentile(&mut full_ms.clone(), 0.5),
        "full_read_ms_p95": percentile(&mut full_ms, 0.95),
        "per_size": per_size,
    });
    std::fs::write(
        out,
        serde_json::to_vec_pretty(&json!({ "summary": summary }))?,
    )
    .wrap_err_with(|| format!("writing {}", out.display()))?;
    Ok(())
}

//! Node implementation - thin wrapper around saorsa-core's `P2PNode`.

use crate::ant_protocol::CHUNK_PROTOCOL_ID;
use crate::config::{
    default_nodes_dir, default_root_dir, NetworkMode, NodeConfig, RootVaultSetting,
    VaultDeploymentBlock, NODE_IDENTITY_FILENAME,
};
use crate::error::{Error, Result};
use crate::event::{create_event_channel, NodeEvent, NodeEventsChannel, NodeEventsSender};
use crate::logging::{debug, error, info, warn};
use crate::payment::metrics::QuotingMetricsTracker;
use crate::payment::wallet::parse_rewards_address;
use crate::payment::{
    EvmVerifierConfig, PaymentVerifier, PaymentVerifierConfig, PriceFloorConfig, QuoteGenerator,
};
use crate::replication::config::ReplicationConfig;
use crate::replication::fresh::FreshWriteEvent;
use crate::replication::ReplicationEngine;
use crate::storage::legacy_artifacts;
use crate::storage::traffic as storage_traffic;
use crate::storage::MIB;
use crate::storage::{AntProtocol, ChunkRequestContext, ChunkStore, ChunkStoreConfig};
use crate::upgrade::{
    upgrade_cache_dir, AutoApplyUpgrader, BinaryCache, ReleaseCache, UpgradeMonitor, UpgradeResult,
};
use rand::Rng;
use saorsa_core::identity::NodeIdentity;
use saorsa_core::{
    IPDiversityConfig as CoreDiversityConfig, MultiAddr, NodeConfig as CoreNodeConfig, P2PEvent,
    P2PNode,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

/// How long shutdown waits for in-flight request handlers to finish.
///
/// Short, because these are single request/response exchanges and the peer will retry.
/// The point is to stop new storage work starting, not to see every last request through.
const PROTOCOL_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// The user agent this node announces itself with.
///
/// `saorsa-core` admits a peer to the DHT only when its agent starts with `node/`, so a node
/// that lost the prefix would stop being routed to. The version is this build's rather than
/// the transport's, because it is the one a peer needs in order to tell which release it is
/// talking to.
///
/// The image marker, when the node has one ([`image_marker`]), follows the release in the
/// version's build metadata: `node/0.23.0+ds.16.image.<commit>`. The node census reads a node's
/// release as the semver after `node/` and the `ds.<n>` its build metadata starts with, so it
/// still reads the release, and can count the nodes whose image gave no marker.
fn user_agent(image: Option<&str>) -> String {
    let version = env!("CARGO_PKG_VERSION");
    image.map_or_else(
        || format!("node/{version}"),
        |marker| {
            let separator = if version.contains('+') { '.' } else { '+' };
            format!("node/{version}{separator}image.{marker}")
        },
    )
}

/// The image marker this node reports: what `ANT_IMAGE_VERSION` gives (`--image-version`), if
/// it is what the storage-node image passes, the 40 lowercase hex digits of the darwin-node
/// commit the image was built from. Images before the one that ships darwin-nodes, whose node
/// manager deletes a node's data on a nearly full disk, pass none.
pub(crate) fn image_marker(given: Option<&str>) -> Option<&str> {
    given.filter(|marker| {
        marker.len() == 40
            && marker
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// Builder for constructing an Ant node.
pub struct NodeBuilder {
    config: NodeConfig,
}

impl NodeBuilder {
    /// Create a new node builder with the given configuration.
    #[must_use]
    pub fn new(config: NodeConfig) -> Self {
        Self { config }
    }

    /// Reject startup in production mode without a usable rewards address.
    ///
    /// A node that cannot receive payment must not silently run on the
    /// production network. The placeholder address shipped in the example
    /// config and an empty string both count as "unconfigured".
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if `network_mode` is `Production` and
    /// `payment.rewards_address` is unset, empty, or the example placeholder.
    fn validate_production_rewards_address(config: &NodeConfig) -> Result<()> {
        if config.network_mode != NetworkMode::Production {
            return Ok(());
        }
        let configured = config
            .payment
            .rewards_address
            .as_deref()
            .is_some_and(|addr| !addr.is_empty() && addr != "0xYOUR_ARBITRUM_ADDRESS_HERE");
        if configured {
            Ok(())
        } else {
            Err(Error::Config(
                "CRITICAL: Rewards address is not configured. \
                 Set payment.rewards_address in config to your Arbitrum wallet address."
                    .to_string(),
            ))
        }
    }

    /// Build and start the node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to start.
    #[allow(clippy::too_many_lines)]
    pub async fn build(mut self) -> Result<RunningNode> {
        info!("Building ant-node with config: {:?}", self.config);

        Self::validate_production_rewards_address(&self.config)?;

        #[cfg(not(feature = "webrtc-direct"))]
        if self.config.webrtc_direct.enabled {
            return Err(Error::Config(
                "webrtc_direct is enabled but this binary was not built with the \
                 'webrtc-direct' feature"
                    .to_string(),
            ));
        }

        // Resolve identity and root_dir (may update self.config.root_dir)
        let identity = Arc::new(Self::resolve_identity(&mut self.config).await?);
        let peer_id = identity.peer_id().to_hex();

        info!(peer_id = %peer_id, root_dir = %self.config.root_dir.display(), "Node identity resolved");

        // One line at start: the image marker, or that there is none.
        if let Some(marker) = image_marker(self.config.image_version.as_deref()) {
            info!(image_version = marker, "Image marker");
        } else {
            warn!(
                "No image marker (ANT_IMAGE_VERSION): if this node runs in a storage-node \
                 container, that container's node manager may delete a node's data when the \
                 disk is nearly full; pull the new image (docker compose pull, then docker \
                 compose up -d)"
            );
        }

        // Ensure root directory exists
        std::fs::create_dir_all(&self.config.root_dir)?;

        // Create shutdown token
        let shutdown = CancellationToken::new();

        // Create event channel
        let (events_tx, events_rx) = create_event_channel();

        // Convert our config to saorsa-core's config
        let mut core_config = Self::build_core_config(&self.config)?;
        // Inject the ML-DSA identity so the P2PNode's transport peer ID
        // matches the pub_key embedded in payment quotes.
        core_config.node_identity = Some(Arc::clone(&identity));
        debug!("Core config: {:?}", core_config);

        // Initialize saorsa-core's P2PNode
        let p2p_node = P2PNode::new(core_config)
            .await
            .map_err(|e| Error::Startup(format!("Failed to create P2P node: {e}")))?;
        if !self.config.relay_serving {
            Self::stop_relay_serving(&p2p_node);
        }

        // Create upgrade monitor
        let upgrade_monitor = {
            let node_id_seed = p2p_node.peer_id().as_bytes();
            Some(Self::build_upgrade_monitor(&self.config, node_id_seed))
        };

        let repl_config = ReplicationConfig::default();

        // Initialize ANT protocol handler for chunk storage and
        // wire the fresh-write channel so PUTs trigger replication.
        let (ant_protocol, fresh_write_rx) = if self.config.storage.enabled {
            let (fresh_write_tx, fresh_write_rx) = tokio::sync::mpsc::unbounded_channel();
            let mut protocol =
                Self::build_ant_protocol(&self.config, &identity, repl_config.close_group_size)
                    .await?;
            protocol.set_fresh_write_sender(fresh_write_tx);
            (Some(Arc::new(protocol)), Some(fresh_write_rx))
        } else {
            info!("Chunk storage disabled");
            (None, None)
        };

        // Only now, and only if a store was actually opened. Clearing up after the storage
        // migration deletes directories the previous release had retired, and retirement
        // counted on every chunk the node kept being in the file store — which is only true
        // if the file store is there. Doing this earlier put the deletion in front of a
        // constructor that can still fail on an unreadable layout, a directory it cannot
        // create, or a lock another process has not let go of, and a node that lost both
        // stores that way had nothing to go back to.
        //
        // A node with storage switched off never builds one at all, so it never establishes
        // that anything was copied anywhere, and it does not delete. It also has no use for
        // the disk it would recover. Leaving the directory costs space on a node that is not
        // storing anything anyway.
        if ant_protocol.is_some() {
            legacy_artifacts::clean_up(&self.config.root_dir);
        } else {
            info!(
                "Chunk storage is disabled, so anything the storage migration left behind is \
                 being left where it is: nothing here can establish that its chunks were \
                 copied anywhere."
            );
        }

        let p2p_arc = Arc::new(p2p_node);

        // Wire the P2PNode handle into AntProtocol so payment proofs can query
        // live-DHT closeness.
        if let Some(ref protocol) = ant_protocol {
            protocol.attach_p2p_node(Arc::clone(&p2p_arc));
        }

        let replication_engine = match (&ant_protocol, fresh_write_rx) {
            (Some(protocol), Some(fresh_rx)) => {
                Self::build_replication_engine(
                    protocol,
                    repl_config,
                    &p2p_arc,
                    &identity,
                    &self.config.root_dir,
                    fresh_rx,
                    &shutdown,
                )
                .await?
            }
            _ => None,
        };

        let node = RunningNode {
            config: self.config,
            p2p_node: p2p_arc,
            shutdown,
            events_tx,
            events_rx: Some(events_rx),
            upgrade_monitor,
            ant_protocol,
            replication_engine,
            protocol_task: None,
            protocol_children: TaskTracker::new(),
            #[cfg(feature = "webrtc-direct")]
            webrtc_direct_task: None,
            #[cfg(feature = "webrtc-direct")]
            webrtc_diagnostics: tokio::sync::watch::channel(None).0,
            upgrade_exit_code: Arc::new(AtomicI32::new(-1)),
        };

        Ok(node)
    }

    /// Start the replication engine.
    ///
    /// # Errors
    ///
    /// Never, currently: an engine that fails to start is logged and the node runs without
    /// one, as it always has. The signature keeps its `Result` because the caller's does.
    async fn build_replication_engine(
        protocol: &Arc<AntProtocol>,
        repl_config: ReplicationConfig,
        p2p: &Arc<P2PNode>,
        identity: &Arc<NodeIdentity>,
        root_dir: &Path,
        fresh_rx: UnboundedReceiver<FreshWriteEvent>,
        shutdown: &CancellationToken,
    ) -> Result<Option<ReplicationEngine>> {
        let mut engine = match ReplicationEngine::new(
            repl_config,
            Arc::clone(p2p),
            protocol.storage(),
            protocol.payment_verifier_arc(),
            Arc::clone(identity),
            root_dir,
            fresh_rx,
            shutdown.clone(),
        )
        .await
        {
            Ok(engine) => engine,
            Err(e) => {
                warn!("Failed to initialize replication engine: {e}");
                return Ok(None);
            }
        };

        // ADR-0016: pointers replicate through the same engine. The PUT handler
        // hands each newly stored paid state to it, and asks it before taking
        // a final state (ADR-0018).
        if let Some(service) = protocol.pointer_service() {
            engine.with_pointer_service(service);
        }

        // Fresh offers carry the upload's payment proof: listings are recorded
        // from them as from client PUTs.
        if let Some(listings) = protocol.listing_service() {
            engine.with_listings(Arc::clone(listings));
        }

        // ADR-0004: wire the engine's commitment state as the quote generator's
        // commitment source so quotes force their price from the live storage
        // commitment. Done here because the engine owns the commitment state and is
        // built after the protocol.
        let concrete = Arc::clone(engine.commitment_state());
        let source: Arc<dyn crate::payment::quote::CommitmentSource> = concrete;
        protocol.attach_commitment_source(source);
        // ADR-0004: share the engine's gossip commitment cache with the verifier so the
        // cross-check can resolve quote pins against neighbours' commitments.
        protocol
            .payment_verifier_arc()
            .attach_commitment_cache(Arc::clone(engine.last_commitment_by_peer()));
        // ADR-0004: give the verifier the monetized-pin sender so commitments that back
        // a payment get a deterministic first audit from the engine's drainer.
        protocol
            .payment_verifier_arc()
            .attach_monetized_pin_sender(engine.monetized_pin_sender());

        Ok(Some(engine))
    }

    /// Refuse MASQUE relay sessions from now on (`relay_serving = false`).
    ///
    /// saorsa-core gives every node that is not a client a relay server, on which any
    /// peer can open a session and have this node send UDP for it to any address, and
    /// it has a switch for that server that nothing in saorsa-core calls. Flipped here,
    /// before `start()`, so the node refuses sessions before it joins the network: a
    /// refused peer is told 503, as by a relay that is full, and tries its next
    /// candidate. The transport accepts connections from inside `P2PNode::new`, so a
    /// session opened in the moment before this call would still be served.
    fn stop_relay_serving(p2p_node: &P2PNode) {
        p2p_node.transport().set_relay_serving_enabled(false);
        info!(
            "Relay serving is off (relay_serving = false): this node refuses MASQUE relay sessions"
        );
    }

    /// Build the saorsa-core `NodeConfig` from our config.
    fn build_core_config(config: &NodeConfig) -> Result<CoreNodeConfig> {
        let local = matches!(config.network_mode, NetworkMode::Development);

        let mut core_config = CoreNodeConfig::builder()
            .port(config.port)
            .ipv6(!config.ipv4_only)
            .local(local)
            .max_message_size(config.max_message_size)
            .build()
            .map_err(|e| Error::Config(format!("Failed to create core config: {e}")))?;

        // Add bootstrap peers.
        core_config.bootstrap_peers = config
            .bootstrap
            .iter()
            .map(|addr| MultiAddr::quic(*addr))
            .collect();

        // Propagate network-mode tuning into saorsa-core where supported.
        match config.network_mode {
            NetworkMode::Production => {
                core_config.diversity_config = Some(CoreDiversityConfig::default());
            }
            NetworkMode::Testnet => {
                // Testnet allows loopback so nodes can be co-located on one machine.
                core_config.allow_loopback = true;
                core_config.diversity_config = Some(CoreDiversityConfig {
                    max_per_ip: config.testnet.max_per_ip,
                    max_per_subnet: config.testnet.max_per_subnet,
                });
            }
            NetworkMode::Development => {
                core_config.diversity_config = Some(CoreDiversityConfig::permissive());
            }
        }

        core_config.custom_user_agent =
            Some(user_agent(image_marker(config.image_version.as_deref())));

        // Persist close group peers + trust scores across restarts.
        // Default to root_dir (alongside node_identity.key) when not explicitly set.
        core_config.close_group_cache_dir = Some(
            config
                .close_group_cache_dir
                .clone()
                .unwrap_or_else(|| config.root_dir.clone()),
        );

        Ok(core_config)
    }

    /// Resolve the node identity from disk or generate a new one.
    ///
    /// **When `root_dir` differs from the platform default** (set via `--root-dir`
    /// or loaded from `config.toml`):
    ///   - Use `root_dir` directly: load existing identity or generate a new one.
    ///
    /// **When `root_dir` is the platform default** (first run, no config file):
    ///   1. Scan `{default_root_dir}/nodes/` for subdirectories containing
    ///      `node_identity.key`.
    ///   2. **None found** — first run: generate identity, create
    ///      `nodes/{full_peer_id}/`, save identity there, update `config.root_dir`.
    ///   3. **Exactly one found** — load it and update `config.root_dir`.
    ///   4. **Multiple found** — return an error asking for `--root-dir`.
    async fn resolve_identity(config: &mut NodeConfig) -> Result<NodeIdentity> {
        if config.root_dir != default_root_dir() {
            return Self::load_or_generate_identity(&config.root_dir).await;
        }

        let nodes_dir = default_nodes_dir();
        let identity_dirs = Self::scan_identity_dirs(&nodes_dir)?;

        match identity_dirs.len() {
            0 => {
                // First run: generate new identity and create a peer-id-scoped subdirectory
                let identity = NodeIdentity::generate().map_err(|e| {
                    Error::Startup(format!("Failed to generate node identity: {e}"))
                })?;
                let peer_id = identity.peer_id().to_hex();
                let peer_dir = nodes_dir.join(&peer_id);
                std::fs::create_dir_all(&peer_dir)?;
                identity
                    .save_to_file(&peer_dir.join(NODE_IDENTITY_FILENAME))
                    .await
                    .map_err(|e| Error::Startup(format!("Failed to save node identity: {e}")))?;
                config.root_dir = peer_dir;
                Ok(identity)
            }
            1 => {
                let dir = identity_dirs
                    .first()
                    .ok_or_else(|| Error::Config("No identity dirs found".to_string()))?;
                let identity = NodeIdentity::load_from_file(&dir.join(NODE_IDENTITY_FILENAME))
                    .await
                    .map_err(|e| Error::Startup(format!("Failed to load node identity: {e}")))?;
                config.root_dir.clone_from(dir);
                Ok(identity)
            }
            _ => {
                let dirs: Vec<String> = identity_dirs
                    .iter()
                    .filter_map(|d| d.file_name().map(|n| n.to_string_lossy().into_owned()))
                    .collect();
                Err(Error::Config(format!(
                    "Multiple node identities found at {}: [{}]. Specify --root-dir to select one.",
                    nodes_dir.display(),
                    dirs.join(", ")
                )))
            }
        }
    }

    /// Load an existing identity from `dir/node_identity.key`, or generate and save a new one.
    async fn load_or_generate_identity(dir: &std::path::Path) -> Result<NodeIdentity> {
        let key_path = dir.join(NODE_IDENTITY_FILENAME);
        if key_path.exists() {
            NodeIdentity::load_from_file(&key_path)
                .await
                .map_err(|e| Error::Startup(format!("Failed to load node identity: {e}")))
        } else {
            let identity = NodeIdentity::generate()
                .map_err(|e| Error::Startup(format!("Failed to generate node identity: {e}")))?;
            std::fs::create_dir_all(dir)?;
            identity
                .save_to_file(&key_path)
                .await
                .map_err(|e| Error::Startup(format!("Failed to save node identity: {e}")))?;
            Ok(identity)
        }
    }

    /// Scan `base_dir` for immediate subdirectories that contain `node_identity.key`.
    fn scan_identity_dirs(base_dir: &std::path::Path) -> Result<Vec<PathBuf>> {
        let mut dirs = Vec::new();
        let read_dir = match std::fs::read_dir(base_dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(dirs),
            Err(e) => return Err(e.into()),
        };
        for entry in read_dir {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() && path.join(NODE_IDENTITY_FILENAME).exists() {
                dirs.push(path);
            }
        }
        Ok(dirs)
    }

    fn build_upgrade_monitor(config: &NodeConfig, node_id_seed: &[u8]) -> UpgradeMonitor {
        let mut monitor = UpgradeMonitor::new(
            config.upgrade.github_repo.clone(),
            config.upgrade.channel,
            config.upgrade.check_interval_hours,
        );

        if let Ok(cache_dir) = upgrade_cache_dir() {
            monitor = monitor.with_release_cache(ReleaseCache::new(
                cache_dir,
                std::time::Duration::from_hours(1),
            ));
        }

        if config.upgrade.staged_rollout_hours > 0 {
            monitor =
                monitor.with_staged_rollout(node_id_seed, config.upgrade.staged_rollout_hours);
        }

        monitor
    }
    /// Build the ANT protocol handler from config.
    ///
    /// Initializes the chunk store, payment verifier, and quote generator.
    /// Wires ML-DSA-65 signing from the node's identity into the quote generator.
    async fn build_ant_protocol(
        config: &NodeConfig,
        identity: &NodeIdentity,
        close_group_size: usize,
    ) -> Result<AntProtocol> {
        let storage_config = ChunkStoreConfig {
            root_dir: config.root_dir.clone(),
            verify_on_read: config.storage.verify_on_read,
            disk_reserve: config.storage.disk_reserve_mb.saturating_mul(MIB),
        };
        // The listing store leaves the same part of the disk free.
        let disk_reserve = storage_config.disk_reserve;
        let storage = ChunkStore::new(storage_config)
            .await
            .map_err(|e| Error::Startup(format!("Failed to create the chunk store: {e}")))?;

        // Parse rewards address (required — node must know where to receive payments)
        let rewards_address = match config.payment.rewards_address {
            Some(ref addr) => parse_rewards_address(addr)?,
            None => {
                return Err(Error::Startup(
                    "No rewards address configured. Set --rewards-address or payment.rewards_address in config.".to_string(),
                ));
            }
        };

        // Create payment verifier
        let evm_network = config.payment.evm_network.clone().into_evm_network();
        info!("{}", payment_settings(&evm_network));
        // Listings check payments on the same chain, through the same vault.
        let listings_rpc_url = evm_network.rpc_url().to_string();
        let mut listings_vault = [0u8; 20];
        listings_vault.copy_from_slice(evm_network.payment_vault_address().as_slice());
        // The root vault root proofs are paid in, which listings read beside
        // the per-quote vault: the network's setting, else on the production
        // network the one built in. With none, root proofs are refused.
        let root_vault_setting = config
            .payment
            .evm_network
            .root_vault()
            .map_err(Error::Startup)?;
        info!("{}", root_vault_line(root_vault_setting));
        if let Some(line) = root_vault_override_line(
            config.payment.evm_network.is_production(),
            root_vault_setting,
        ) {
            warn!("{line}");
        }
        let root_vault = root_vault_setting.vault();
        let payment_config = PaymentVerifierConfig {
            evm: EvmVerifierConfig {
                network: evm_network,
                root_vault,
            },
            cache_capacity: config.payment.cache_capacity,
            close_group_size,
            local_rewards_address: rewards_address,
            // Shadow mode by default; enforcement is a per-node canary opt-in
            // via ANT_PRICE_FLOOR_ENFORCE (see PriceFloorConfig).
            price_floor: PriceFloorConfig::from_env(),
        };
        if payment_config.price_floor.enforce {
            info!(
                "Price floor ENFORCEMENT enabled (tolerance {}%)",
                payment_config.price_floor.tolerance_percent
            );
        }
        let payment_verifier = PaymentVerifier::new(payment_config);
        let metrics_tracker = QuotingMetricsTracker::new(0);
        let mut quote_generator = QuoteGenerator::new(rewards_address, metrics_tracker);

        // Wire ML-DSA-65 signing from node identity.
        // This same signer is used for both regular quotes and merkle candidate quotes.
        crate::payment::wire_ml_dsa_signer(&mut quote_generator, identity)?;

        let storage = Arc::new(storage);
        let payment_verifier = Arc::new(payment_verifier);

        // Pointers live beside the chunks, under the same root. Opening the
        // store here is what makes pointer PUT/GET answerable at all: without
        // it every pointer request is refused, which is the right answer for a
        // node that keeps none but the wrong one for a node that should.
        let pointer_store = crate::pointer::PointerStore::new(&config.root_dir).await?;
        let pointers = crate::pointer::PointerService::new(pointer_store)
            // Refuse a pointer whose address a chunk already occupies, rather
            // than letting one kind silently overwrite the other.
            .with_chunk_store(Arc::clone(&storage))
            // Payment is verified against each record's state, so every update
            // is paid for rather than riding the first one.
            .with_payments(Arc::clone(&payment_verifier));

        // Listings for envelope chunks, kept beside the chunks under the same root.
        let vault_deployment_block = config.payment.evm_network.vault_deployment_block();
        if let Some(line) = vault_deployment_line(vault_deployment_block) {
            info!("{line}");
        }
        let listings = crate::listings::ListingService::new(
            &config.root_dir,
            disk_reserve,
            Arc::clone(&storage),
            listings_rpc_url,
            ant_listings::VaultSet {
                v2: listings_vault,
                root: root_vault,
            },
            vault_deployment_block.block(),
        )
        .await?;

        let protocol = AntProtocol::new(storage, payment_verifier, Arc::new(quote_generator))
            .with_pointer_service(pointers)
            .with_listing_service(listings);

        info!(
            "ANT protocol handler initialized with ML-DSA-65 signing (protocol={CHUNK_PROTOCOL_ID})"
        );

        Ok(protocol)
    }
}

/// The line a node logs at start about where it is paid: the payment token,
/// the vault and the RPC's host. Only the host: an RPC URL can carry an
/// operator's API key, in its user info, path or query, and is never logged.
/// The image's smoke test checks that every node names DARWIN's token and
/// vault.
fn payment_settings(network: &evmlib::Network) -> String {
    format!(
        "Payments: token {}, vault {}, RPC host {}",
        network.payment_token_address(),
        network.payment_vault_address(),
        network.rpc_url().host_str().unwrap_or("(none)")
    )
}

/// The line a node logs once at start about the block its payment vault was
/// deployed in, when no setting gives it: that it takes the production
/// network's, built in, or that it knows none, so listings showing any block
/// are checked on chain alone. `None` for a block a setting gives, which the
/// listing service names as it starts.
fn vault_deployment_line(block: VaultDeploymentBlock) -> Option<String> {
    match block {
        VaultDeploymentBlock::Given(_) => None,
        VaultDeploymentBlock::BuiltIn(block) => Some(format!(
            "No setting gives the block the payment vault was deployed in; taking {block}, \
             the production network's"
        )),
        VaultDeploymentBlock::Unknown => Some(
            "No block the payment vault was deployed in is known for this network: listings \
             showing any block are checked on chain alone"
                .to_string(),
        ),
    }
}

/// What a node logs once at start about the root vault root proofs are
/// paid in: its address, and where it is from, or that root proofs are
/// refused.
fn root_vault_line(setting: RootVaultSetting) -> String {
    let address = |vault: ant_listings::RootVault| {
        format!(
            "{}",
            evmlib::common::Address::from_slice(vault.address.as_slice())
        )
    };
    match setting {
        RootVaultSetting::Given(vault) => format!("Root vault {}", address(vault)),
        RootVaultSetting::BuiltIn(vault) => format!(
            "Root vault {}, the production network's: no setting gives one",
            address(vault)
        ),
        RootVaultSetting::None => {
            "No root vault is known for this network: root proofs are refused".to_string()
        }
    }
}

/// What a node on the production network warns once at start when its root
/// vault setting gives a vault other than the one built in: the setting wins,
/// so the node reads that vault, finds no record of any root payment made
/// through the built-in one, and refuses those proofs. `None` for a setting
/// that gives the built-in vault, for no setting, and off the production
/// network.
fn root_vault_override_line(production: bool, setting: RootVaultSetting) -> Option<String> {
    let RootVaultSetting::Given(vault) = setting else {
        return None;
    };
    let given = format!("0x{}", hex::encode(vault.address));
    if !production || given.eq_ignore_ascii_case(crate::config::PRODUCTION_ROOT_VAULT) {
        return None;
    }
    Some(format!(
        "The root vault setting gives {given}, not the production network's {}: this node \
         reads only the vault given, and refuses root proofs paid through the built-in one",
        crate::config::PRODUCTION_ROOT_VAULT
    ))
}

/// A running Ant node.
pub struct RunningNode {
    config: NodeConfig,
    p2p_node: Arc<P2PNode>,
    shutdown: CancellationToken,
    events_tx: NodeEventsSender,
    events_rx: Option<NodeEventsChannel>,
    upgrade_monitor: Option<UpgradeMonitor>,
    /// ANT protocol handler for chunk storage.
    ant_protocol: Option<Arc<AntProtocol>>,
    /// Replication engine (manages neighbor sync, verification, audits).
    replication_engine: Option<ReplicationEngine>,
    /// Protocol message routing background task.
    protocol_task: Option<JoinHandle<()>>,
    /// The per-message handler tasks the protocol loop spawns.
    ///
    /// Tracked rather than detached so shutdown can stop accepting work and then wait for
    /// what is already in flight. Aborting only the loop leaves its children running, and
    /// a chunk read that outlives the loop keeps working against a store the shutdown is
    /// about to tear down.
    protocol_children: TaskTracker,
    /// ADR-0015 browser listener task.
    #[cfg(feature = "webrtc-direct")]
    webrtc_direct_task: Option<JoinHandle<()>>,
    #[cfg(feature = "webrtc-direct")]
    webrtc_diagnostics: tokio::sync::watch::Sender<Option<crate::web_rtc::WebRtcServerDiagnostics>>,
    /// Exit code requested by a successful upgrade (-1 = no upgrade exit pending).
    upgrade_exit_code: Arc<AtomicI32>,
}

impl RunningNode {
    /// Subscribe before `run()` to receive the browser diagnostics handle after startup.
    /// The handle can be sampled at any time and retains counters after shutdown.
    #[cfg(feature = "webrtc-direct")]
    #[must_use]
    pub fn subscribe_webrtc_diagnostics(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<crate::web_rtc::WebRtcServerDiagnostics>> {
        self.webrtc_diagnostics.subscribe()
    }

    /// Get the node's root directory.
    #[must_use]
    pub fn root_dir(&self) -> &PathBuf {
        &self.config.root_dir
    }

    /// Get a receiver for node events.
    ///
    /// Note: Can only be called once. Subsequent calls return None.
    pub fn events(&mut self) -> Option<NodeEventsChannel> {
        self.events_rx.take()
    }

    /// Subscribe to node events.
    #[must_use]
    pub fn subscribe_events(&self) -> NodeEventsChannel {
        self.events_tx.subscribe()
    }

    /// Run the node until shutdown is requested.
    ///
    /// # Errors
    ///
    /// Returns an error if the node encounters a fatal error.
    pub async fn run(&mut self) -> Result<()> {
        let result = self.run_until_shutdown().await;
        self.cleanup().await;
        // If an upgrade triggered the shutdown, exit with the requested code.
        // This happens *after* all cleanup (P2P shutdown, log flush, etc.) so
        // that destructors and async resources are properly torn down.
        let exit_code = self.upgrade_exit_code.load(Ordering::SeqCst);
        if result.is_ok() && exit_code >= 0 {
            info!("Exiting with code {} for upgrade restart", exit_code);
            std::process::exit(exit_code);
        }

        result
    }

    /// Start serving and wait for shutdown; the caller owns cleanup on every exit.
    #[allow(clippy::too_many_lines)]
    async fn run_until_shutdown(&mut self) -> Result<()> {
        info!("Node runtime loop starting");

        // Subscribe to DHT events BEFORE starting the P2P node so the
        // bootstrap-sync task does not miss the BootstrapComplete event
        // emitted during P2PNode::start().
        let dht_events_for_bootstrap = self
            .replication_engine
            .as_ref()
            .map(|_| self.p2p_node.dht_manager().subscribe_events());

        // Start the P2P node
        self.p2p_node
            .start()
            .await
            .map_err(|e| Error::Startup(format!("Failed to start P2P node: {e}")))?;

        let listen_addrs = self.p2p_node.listen_addrs().await;
        info!(listen_addrs = ?listen_addrs, "P2P node started");

        // Extract the actual bound port (config port may be 0 = auto-select)
        let actual_port = listen_addrs
            .first()
            .and_then(MultiAddr::port)
            .unwrap_or(self.config.port);
        info!(
            port = actual_port,
            "Node is running on port: {}", actual_port
        );

        #[cfg(feature = "webrtc-direct")]
        if self.config.webrtc_direct.enabled {
            let evm_network = self.config.payment.evm_network.clone().into_evm_network();
            match crate::web_rtc::spawn(
                &self.config.webrtc_direct,
                &self.config.root_dir,
                Arc::clone(&self.p2p_node),
                self.ant_protocol.clone(),
                &evm_network,
                self.shutdown.clone(),
                None,
            )
            .await
            {
                Ok(server) => {
                    let _ = self
                        .webrtc_diagnostics
                        .send_replace(Some(server.diagnostics));
                    self.webrtc_direct_task = Some(server.task);
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }

        // Emit started event
        if let Err(e) = self.events_tx.send(NodeEvent::Started) {
            warn!("Failed to send Started event: {e}");
        }

        // Start protocol message routing (P2P → AntProtocol → P2P response)
        self.start_protocol_routing();

        // Start replication engine background tasks
        if let Some(ref mut engine) = self.replication_engine {
            // Safety: dht_events_for_bootstrap is Some when replication_engine
            // is Some (both arms use the same condition).
            if let Some(dht_events) = dht_events_for_bootstrap {
                engine.start(dht_events);
            }
            info!("Replication engine started");
        }

        // Start upgrade monitor if enabled
        if let Some(monitor) = self.upgrade_monitor.take() {
            let events_tx = self.events_tx.clone();
            let shutdown = self.shutdown.clone();
            let stop_on_upgrade = self.config.upgrade.stop_on_upgrade;
            let upgrade_exit_code = Arc::clone(&self.upgrade_exit_code);

            tokio::spawn(async move {
                let mut monitor = monitor;
                let mut upgrader = AutoApplyUpgrader::new().with_stop_on_upgrade(stop_on_upgrade);
                if let Ok(cache_dir) = upgrade_cache_dir() {
                    upgrader = upgrader.with_binary_cache(BinaryCache::new(cache_dir));
                }

                // The first check comes a few minutes after start, at a random point, so a
                // node that restarted while waiting for a release takes it when it is due
                // rather than an hour later, and nodes started together don't all check at
                // once.
                {
                    let first_check = first_upgrade_check_delay();
                    let first_check_time = chrono::Utc::now()
                        + chrono::Duration::from_std(first_check).unwrap_or_else(|e| {
                            warn!("chrono::Duration::from_std failed for jitter ({e}), defaulting to 1 minute");
                            chrono::Duration::minutes(1)
                        });
                    info!(
                        "First upgrade check scheduled for {} (in {}s)",
                        first_check_time.to_rfc3339(),
                        first_check.as_secs()
                    );
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(first_check) => {}
                    }
                }

                // The node has started cleanly: the shared cache keeps the archives of the
                // running and previous versions, and of any newer one, and no others.
                prune_upgrade_cache(upgrader.current_version().clone()).await;

                loop {
                    tokio::select! {
                        () = shutdown.cancelled() => {
                            break;
                        }
                        result = monitor.check_for_ready_upgrade() => {
                            // An upgrade the disk had no room for: the version, and the
                            // bytes needed and free.
                            let mut skipped_for_space = None;
                            match result {
                                Ok(Some(upgrade_info)) => {
                                    info!(
                                        current_version = %upgrader.current_version(),
                                        new_version = %upgrade_info.version,
                                        "Upgrade available"
                                    );

                                    // Send notification event
                                    if let Err(e) = events_tx.send(NodeEvent::UpgradeAvailable {
                                        version: upgrade_info.version.to_string(),
                                    }) {
                                        warn!("Failed to send UpgradeAvailable event: {e}");
                                    }

                                    // Auto-apply the upgrade
                                    info!("Starting auto-apply upgrade...");
                                    match upgrader.apply_upgrade(&upgrade_info).await {
                                        Ok(UpgradeResult::Success { version, exit_code }) => {
                                            info!("Upgrade to {} successful, initiating graceful shutdown", version);
                                            upgrade_exit_code.store(exit_code, Ordering::SeqCst);
                                            shutdown.cancel();
                                            break;
                                        }
                                        Ok(UpgradeResult::RolledBack { reason }) => {
                                            warn!("Error during upgrade process: {}", reason);
                                        }
                                        Ok(UpgradeResult::NoUpgrade) => {
                                            info!("Already running latest version");
                                        }
                                        Ok(UpgradeResult::NotEnoughSpace { needed, available }) => {
                                            skipped_for_space =
                                                Some((upgrade_info.version.clone(), needed, available));
                                        }
                                        Err(e) => {
                                            error!("Error during upgrade process: {}", e);
                                        }
                                    }
                                }
                                Ok(None) => {
                                    if let Some(remaining) = monitor.time_until_upgrade() {
                                        info!(
                                            "Upgrade pending, rollout delay remaining: {}m {}s",
                                            remaining.as_secs() / 60,
                                            remaining.as_secs() % 60
                                        );
                                    } else {
                                        info!("No upgrade available");
                                    }
                                }
                                Err(e) => {
                                    warn!("Error during upgrade process: {}", e);
                                }
                            }
                            // An upgrade skipped for space says so in one line, with the
                            // next check, a check interval later.
                            if let Some((version, needed, available)) = skipped_for_space {
                                let next = jittered_interval(monitor.check_interval());
                                let next_check = chrono::Utc::now()
                                    + chrono::Duration::from_std(next)
                                        .unwrap_or_else(|_| chrono::Duration::hours(1));
                                warn!(
                                    "Upgrade to {version} skipped this cycle: {} MiB free on the \
                                     disk the node's binary is on, {} MiB needed (twice the archive \
                                     and twice the binary); next check at {}",
                                    available / crate::storage::MIB,
                                    needed.div_ceil(crate::storage::MIB),
                                    next_check.to_rfc3339()
                                );
                                tokio::select! {
                                    () = shutdown.cancelled() => {
                                        break;
                                    }
                                    () = tokio::time::sleep(next) => {}
                                }
                                continue;
                            }
                            // If an upgrade is pending, sleep for exactly the remaining
                            // rollout delay so the node restarts at its scheduled time
                            // rather than waiting for the next check interval tick.
                            let sleep_duration = monitor.time_until_upgrade().map_or_else(
                                || {
                                    // No pending upgrade - schedule next check with jitter
                                    let jittered_duration =
                                        jittered_interval(monitor.check_interval());
                                    let next_check = chrono::Utc::now()
                                        + chrono::Duration::from_std(jittered_duration).unwrap_or_else(|e| {
                                            warn!("chrono::Duration::from_std failed for interval ({e}), defaulting to 1 hour");
                                            chrono::Duration::hours(1)
                                        });
                                    info!("Next upgrade check scheduled for {}", next_check.to_rfc3339());
                                    jittered_duration
                                },
                                |remaining| {
                                    // If the rollout delay has fully elapsed but the upgrade was
                                    // not successfully applied, avoid a tight loop by backing off
                                    // at least one check interval before retrying.
                                    if remaining.is_zero() {
                                        let backoff = jittered_interval(monitor.check_interval());
                                        let next_check = chrono::Utc::now()
                                            + chrono::Duration::from_std(backoff).unwrap_or_else(|e| {
                                                warn!("chrono::Duration::from_std failed for backoff ({e}), defaulting to 1 hour");
                                                chrono::Duration::hours(1)
                                            });
                                        info!(
                                            "Upgrade rollout delay elapsed but previous apply did not succeed; \
                                             backing off, next check scheduled for {}",
                                            next_check.to_rfc3339()
                                        );
                                        backoff
                                    } else {
                                        let wake_time = chrono::Utc::now()
                                            + chrono::Duration::from_std(remaining).unwrap_or_else(|e| {
                                                warn!("chrono::Duration::from_std failed for rollout delay ({e}), defaulting to 1 minute");
                                                chrono::Duration::minutes(1)
                                            });
                                        info!("Will apply upgrade at {}", wake_time.to_rfc3339());
                                        remaining
                                    }
                                },
                            );
                            // Use select! so shutdown can interrupt long sleeps
                            // (e.g. during a full rollout window delay).
                            tokio::select! {
                                () = shutdown.cancelled() => {
                                    break;
                                }
                                () = tokio::time::sleep(sleep_duration) => {}
                            }
                        }
                    }
                }
            });
        }

        info!("Node running, waiting for shutdown signal");

        // The main event loop, with signal handling. Everything above this starts
        // something; this is where the node waits.
        self.run_event_loop().await
    }

    /// Drain dependent work before shutting down native networking.
    async fn cleanup(&mut self) {
        self.shutdown.cancel();
        // The shared token closes the WebRtcDirect accept loop and active
        // browser sessions before storage and native P2P are torn down.
        #[cfg(feature = "webrtc-direct")]
        if let Some(task) = self.webrtc_direct_task.take() {
            if let Err(error) = task.await {
                warn!("WebRtcDirect task shutdown failed: {error}");
            }
        }

        // Protocol routing stops first, loop and children both. The routing loop waits on
        // `events.recv()` and has no cancellation branch of its own, and it holds an `Arc`
        // on the P2P node that keeps the sender it is waiting on alive, so nothing else
        // here will ever wake it. Left running it holds the chunk store and its
        // single-process lock open after the node has returned. Aborting the accept loop
        // alone is not enough either: the requests already in flight run in their own
        // tasks, which is what the drain below is for.
        if let Some(handle) = self.protocol_task.take() {
            handle.abort();
            // Awaited, not just asked to stop. `abort` schedules cancellation; it does not
            // establish that the task is gone, and what matters here is that it has
            // dropped its `Arc` on the protocol and with it the store's single-process
            // lock before this function returns. The join resolves as cancelled.
            let _ = handle.await;
        }
        // Cancelled first, so anything still queued behind the concurrency permits gives
        // up rather than starting fresh storage work, then given a moment to finish what
        // is genuinely in flight.
        self.shutdown.cancel();
        self.protocol_children.close();
        if tokio::time::timeout(PROTOCOL_DRAIN_GRACE, self.protocol_children.wait())
            .await
            .is_err()
        {
            warn!(
                "{} request handler(s) had not finished after {}s; continuing shutdown \
                 without them.",
                self.protocol_children.len(),
                PROTOCOL_DRAIN_GRACE.as_secs()
            );
        }

        // Shutdown replication engine before P2P so background tasks don't
        // use a dead P2P layer, and Arc<ChunkStore> references are released.
        if let Some(ref mut engine) = self.replication_engine {
            engine.shutdown().await;
        }

        // Shutdown P2P node
        info!("Shutting down P2P node...");
        if let Err(e) = self.p2p_node.shutdown().await {
            warn!("Error during P2P node shutdown: {e}");
        }

        if let Err(e) = self.events_tx.send(NodeEvent::ShuttingDown) {
            warn!("Failed to send ShuttingDown event: {e}");
        }
        info!("Node shutdown complete");
    }

    /// Run the main event loop, handling shutdown and signals.
    #[cfg(unix)]
    async fn run_event_loop(&self) -> Result<()> {
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sighup = signal(SignalKind::hangup())?;

        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => {
                    info!("Shutdown signal received");
                    break;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Received SIGINT (Ctrl-C), initiating shutdown");
                    self.shutdown();
                    break;
                }
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, initiating shutdown");
                    self.shutdown();
                    break;
                }
                _ = sighup.recv() => {
                    info!("Received SIGHUP (config reload not yet supported)");
                }
            }
        }
        Ok(())
    }

    /// Run the main event loop, handling shutdown signals (non-Unix version).
    #[cfg(not(unix))]
    async fn run_event_loop(&self) -> Result<()> {
        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => {
                    info!("Shutdown signal received");
                    break;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Received Ctrl-C, initiating shutdown");
                    self.shutdown();
                    break;
                }
            }
        }
        Ok(())
    }

    /// Handle one inbound protocol message and send whatever it produced.
    async fn answer_one_request(
        protocol: &Arc<AntProtocol>,
        p2p: &Arc<P2PNode>,
        source: &saorsa_core::identity::PeerId,
        data: &[u8],
        data_type: &str,
        response_topic: &str,
        received_at: Instant,
    ) {
        if data_type != "chunk" {
            return;
        }
        let queue_wait = received_at.elapsed();
        let handled = protocol
            .try_handle_request_with_context(
                data,
                Some(ChunkRequestContext::new(
                    source.to_string(),
                    received_at,
                    queue_wait,
                )),
            )
            .await;
        let telemetry = handled.get_telemetry;
        let traffic_key = handled.traffic_key;
        match handled.response {
            Ok(Some(response)) => {
                let response_len = response.len();
                let send_started = Instant::now();
                // Handed over as the `Bytes` it already is: a chunk GET
                // response is up to a whole chunk, and `to_vec` copied it.
                let send_result = p2p
                    .send_message(source, response_topic, response, &[])
                    .await;
                if let Some(telemetry) = telemetry {
                    telemetry.finish_send(send_started.elapsed(), send_result.is_ok());
                }
                // V2-834: attribute response bytes only once the send is
                // confirmed; failed sends are itemised separately.
                match (&send_result, traffic_key) {
                    (Ok(()), Some(key)) => storage_traffic::record_tx(key, response_len),
                    (Ok(()), None) => {
                        storage_traffic::record_tx(
                            storage_traffic::ChunkResponseKey::Other,
                            response_len,
                        );
                    }
                    (Err(_), _) => storage_traffic::record_send_failed(response_len),
                }
                if let Err(e) = send_result {
                    warn!("Failed to send {data_type} protocol response to {source}: {e}");
                }
            }
            Ok(None) => {
                if let Some(telemetry) = telemetry {
                    telemetry.finish_without_send("no_response");
                }
            }
            Err(e) => {
                if let Some(telemetry) = telemetry {
                    telemetry.finish_without_send("encode_error");
                }
                warn!("{data_type} protocol handler error: {e}");
            }
        }
    }

    /// Start the protocol message routing background task.
    ///
    /// Subscribes to P2P events and routes incoming chunk protocol messages
    /// to the `AntProtocol` handler, sending responses back to the sender.
    fn start_protocol_routing(&mut self) {
        let protocol = match self.ant_protocol {
            Some(ref p) => Arc::clone(p),
            None => return,
        };

        let mut events = self.p2p_node.subscribe_events();
        let p2p = Arc::clone(&self.p2p_node);
        let semaphore = Arc::new(Semaphore::new(64));
        let children = self.protocol_children.clone();
        let stopping = self.shutdown.clone();
        let listings = protocol.listing_service().cloned();

        self.protocol_task = Some(tokio::spawn(async move {
            while let Some(event) = next_routing_event(&mut events).await {
                if let P2PEvent::Message {
                    topic,
                    source: Some(source),
                    data,
                    ..
                } = event
                {
                    if topic == ant_listings::TOPIC {
                        if let Some(listings) = listings.clone() {
                            Self::answer_listings_request(
                                listings,
                                Arc::clone(&p2p),
                                source,
                                data,
                                stopping.clone(),
                                &children,
                            );
                        }
                        continue;
                    }

                    let handler_info: Option<(&str, &str)> = if topic == CHUNK_PROTOCOL_ID {
                        Some(("chunk", CHUNK_PROTOCOL_ID))
                    } else {
                        None
                    };

                    if let Some((data_type, response_topic)) = handler_info {
                        debug!("Received {data_type} protocol message from {source}");
                        let received_at = Instant::now();
                        let protocol = Arc::clone(&protocol);
                        let p2p = Arc::clone(&p2p);
                        let sem = semaphore.clone();
                        let stopping = stopping.clone();
                        children.spawn(async move {
                            // A queued handler must not start work once shutdown has
                            // begun. With 64 permits and a busy node the queue behind them
                            // can be long, and every one of those would otherwise start
                            // fresh storage reads while the store beneath is being torn
                            // down.
                            let _permit = {
                                let acquired = tokio::select! {
                                    biased;
                                    () = stopping.cancelled() => return,
                                    p = sem.acquire() => p,
                                };
                                match acquired {
                                    Ok(permit) => permit,
                                    Err(_) => return,
                                }
                            };
                            // Checked again: the wait for a permit may have been long.
                            if stopping.is_cancelled() {
                                return;
                            }
                            Self::answer_one_request(
                                &protocol,
                                &p2p,
                                &source,
                                &data,
                                data_type,
                                response_topic,
                                received_at,
                            )
                            .await;
                        });
                    }
                }
            }
        }));
        info!("Protocol message routing started");
    }

    /// Answer one message on the listings topic, under the same shutdown
    /// rule as chunk requests.
    fn answer_listings_request(
        listings: Arc<crate::listings::ListingService>,
        p2p: Arc<P2PNode>,
        source: saorsa_core::identity::PeerId,
        data: Vec<u8>,
        stopping: CancellationToken,
        children: &TaskTracker,
    ) {
        children.spawn(async move {
            let (p2p, to) = (&p2p, &source);
            Self::answer_listings(
                &listings,
                &source,
                &data,
                &stopping,
                move |response| async move {
                    if let Err(e) = p2p
                        .send_message(to, ant_listings::TOPIC, response, &[])
                        .await
                    {
                        warn!("Failed to send listings response to {to}: {e}");
                    }
                },
            )
            .await;
        });
    }

    /// Answer one message `source` sent on the listings topic, if it needs
    /// one, by handing the answer to `send`. The permit the answer was built
    /// under ([`Self::listings_answer`]) is held until `send` has finished,
    /// so an answer waiting to go out to a peer that reads slowly counts
    /// against the listing service's permits as one being built does: no
    /// more answers than there are permits are held at once.
    async fn answer_listings<F, Fut>(
        listings: &crate::listings::ListingService,
        source: &saorsa_core::identity::PeerId,
        data: &[u8],
        stopping: &CancellationToken,
        send: F,
    ) where
        F: FnOnce(Vec<u8>) -> Fut + Send,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let Some((permit, answer)) = Self::listings_answer(listings, source, data, stopping).await
        else {
            return;
        };
        send(answer).await;
        drop(permit);
    }

    /// The answer to one message `source` sent on the listings topic, if it
    /// needs one, with the permit it was built under. An answer to one of
    /// this node's own requests is handed to it at once. A request waits for
    /// one of the listing service's own permits
    /// ([`crate::listings::ListingService::request_permits`]), never for one
    /// of those chunk requests take, so neither kind of request holds up the
    /// other; it is dropped if the node starts stopping meanwhile. The permit
    /// is to be held until the answer has been sent.
    async fn listings_answer<'a>(
        listings: &'a crate::listings::ListingService,
        source: &saorsa_core::identity::PeerId,
        data: &[u8],
        stopping: &CancellationToken,
    ) -> Option<(SemaphorePermit<'a>, Vec<u8>)> {
        if listings.handle_response(source, data) {
            return None;
        }
        let permit = tokio::select! {
            biased;
            () = stopping.cancelled() => return None,
            permit = listings.request_permits().acquire() => permit.ok()?,
        };
        if stopping.is_cancelled() {
            return None;
        }
        let answer = listings.handle_message(source, data).await?;
        Some((permit, answer))
    }

    /// Request the node to shut down.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// The next event for the protocol routing loop, or `None` once the event stream has
/// closed.
///
/// The loop reads saorsa-core's event broadcast, which keeps a fixed number of events
/// (`DEFAULT_EVENT_CHANNEL_CAPACITY`, 1,000) for its subscribers. One that falls further
/// behind than that gets `RecvError::Lagged`: the oldest events are gone, but the receiver
/// still works and its next `recv` returns the oldest event still held. Ending the loop
/// there, as `while let Ok(..)` did, left the node connected and looking healthy while it
/// answered no request again until it restarted. So it warns and carries on, and the
/// requests among the skipped events go unanswered, as lost messages do. Only `Closed`,
/// after which `recv` never yields again, ends it. The replication engine treats its own
/// subscription the same way.
async fn next_routing_event<T: Clone>(events: &mut broadcast::Receiver<T>) -> Option<T> {
    loop {
        match events.recv().await {
            Ok(event) => return Some(event),
            Err(RecvError::Lagged(missed)) => warn!(
                "Protocol message routing fell {missed} P2P events behind and skipped them; \
                 requests among them go unanswered"
            ),
            Err(RecvError::Closed) => {
                warn!("P2P event stream closed; protocol message routing stopped");
                return None;
            }
        }
    }
}

/// The earliest the first upgrade check comes after start.
const FIRST_UPGRADE_CHECK_EARLIEST: std::time::Duration = std::time::Duration::from_secs(60);

/// The latest the first upgrade check comes after start.
const FIRST_UPGRADE_CHECK_LATEST: std::time::Duration = std::time::Duration::from_secs(300);

/// When the first upgrade check comes after start: at a random point between
/// [`FIRST_UPGRADE_CHECK_EARLIEST`] and [`FIRST_UPGRADE_CHECK_LATEST`].
fn first_upgrade_check_delay() -> std::time::Duration {
    rand::thread_rng().gen_range(FIRST_UPGRADE_CHECK_EARLIEST..=FIRST_UPGRADE_CHECK_LATEST)
}

/// Prunes the shared upgrade cache to the archives of the running version,
/// the one before it and any newer one ([`BinaryCache::prune`]), logging what
/// it deleted.
async fn prune_upgrade_cache(running: semver::Version) {
    let Ok(cache_dir) = upgrade_cache_dir() else {
        return;
    };
    let cache = BinaryCache::new(cache_dir);
    match tokio::task::spawn_blocking(move || cache.prune(&running)).await {
        Ok(Ok(deleted)) if !deleted.is_empty() => {
            let deleted: Vec<String> = deleted.iter().map(ToString::to_string).collect();
            info!(
                "Upgrade cache: deleted the archives of {}, older than the running and previous versions",
                deleted.join(", ")
            );
        }
        Ok(Ok(_)) => {}
        Ok(Err(e)) => warn!("Upgrade cache: could not prune it: {e}"),
        Err(e) => warn!("Upgrade cache: the prune task failed: {e}"),
    }
}

/// Apply ±5% jitter to a base interval to prevent thundering-herd behaviour
/// when multiple nodes check for upgrades on the same schedule.
fn jittered_interval(base: std::time::Duration) -> std::time::Duration {
    let secs = base.as_secs();
    let variance = secs / 20; // 5%
    if variance == 0 {
        return base;
    }
    let jitter = rand::thread_rng().gen_range(0..=variance * 2);
    std::time::Duration::from_secs(secs.saturating_sub(variance) + jitter)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use rand::Rng;
    use tempfile::TempDir;

    use crate::storage::legacy_artifacts::LEGACY_ENV_DIR;

    /// The e2e port range, so a test bind never lands on a production or dev instance.
    const TEST_PORT_RANGE: std::ops::Range<u16> = 20000..60000;

    /// How many times a bind is retried before the failure is treated as real.
    const BIND_ATTEMPTS: u32 = 5;

    /// A well-formed address that receives nothing; no chain is contacted in these tests.
    const TEST_REWARDS_ADDRESS: &str = "0x0000000000000000000000000000000000000001";

    /// A node config that builds without touching a chain or a real network.
    fn local_node_config(root: &std::path::Path, port: u16) -> NodeConfig {
        NodeConfig {
            root_dir: root.to_path_buf(),
            port,
            ipv4_only: true,
            network_mode: NetworkMode::Development,
            payment: crate::config::PaymentConfig {
                rewards_address: Some(TEST_REWARDS_ADDRESS.to_string()),
                ..crate::config::PaymentConfig::default()
            },
            ..NodeConfig::default()
        }
    }

    /// A browser listener that cannot bind fails startup, and `run` still drains what it
    /// had started before returning.
    #[cfg(feature = "webrtc-direct")]
    #[tokio::test]
    async fn browser_startup_failure_drains_before_returning() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("node");
        std::fs::create_dir_all(&root).expect("mkdir");
        let occupied = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let mut config = local_node_config(&root, 0);
        config.webrtc_direct.enabled = true;
        config.webrtc_direct.bind = occupied.local_addr().expect("address");
        let mut node = NodeBuilder::new(config).build().await.expect("build");
        let cancelled = node.shutdown.clone();
        let result = node.run().await;
        assert!(result.is_err(), "occupied browser port must fail startup");
        assert!(cancelled.is_cancelled());
        assert!(node.protocol_children.is_empty());
        // The test runtime remains alive: cleanup must not rely on process exit.
        tokio::task::yield_now().await;
    }

    /// A node builds on a root with nothing left over from the old store.
    #[tokio::test]
    async fn a_node_builds_on_a_clean_root() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("node");
        std::fs::create_dir_all(&root).expect("mkdir");

        let mut built = None;
        let mut last_err = String::new();
        for _ in 0..BIND_ATTEMPTS {
            let port = rand::thread_rng().gen_range(TEST_PORT_RANGE);
            match NodeBuilder::new(local_node_config(&root, port))
                .build()
                .await
            {
                Ok(node) => {
                    built = Some(node);
                    break;
                }
                Err(e) => {
                    last_err = e.to_string();
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
            }
        }
        let Some(node) = built else {
            panic!("could not build a node after {BIND_ATTEMPTS} attempts: {last_err}");
        };

        // What it tells every peer, in the user agent saorsa-core sends with each signed
        // message: this build's version under the `node/` prefix, and nothing after it
        // when it has no image marker.
        let agent = node.p2p_node.config().user_agent();
        assert_eq!(agent, format!("node/{}", env!("CARGO_PKG_VERSION")));
        assert_eq!(agent, user_agent(None));
        node.shutdown.cancel();
    }

    /// The image marker the image passes reaches the user agent saorsa-core sends with every
    /// signed message; anything else given in its place is left out.
    #[test]
    fn the_image_marker_reaches_the_user_agent() {
        let dir = TempDir::new().expect("temp dir");
        let mut config = local_node_config(dir.path(), 0);
        config.image_version = Some(MARKER.to_string());
        let core = NodeBuilder::build_core_config(&config).expect("core config");
        assert_eq!(core.user_agent(), user_agent(Some(MARKER)));
        assert!(core.user_agent().ends_with(&format!("image.{MARKER}")));

        config.image_version = Some("not a commit".to_string());
        let core = NodeBuilder::build_core_config(&config).expect("core config");
        assert_eq!(core.user_agent(), user_agent(None));
    }

    /// The commit an image passes: 40 lowercase hex digits.
    const MARKER: &str = "9757a72afbf6b56617d2be28573cb8d4b3fb2f65";

    #[test]
    fn only_a_commit_is_an_image_marker() {
        assert_eq!(image_marker(Some(MARKER)), Some(MARKER));
        for given in [
            "",
            "9757a72",
            "9757A72AFBF6B56617D2BE28573CB8D4B3FB2F65",
            "9757a72afbf6b56617d2be28573cb8d4b3fb2f650",
            "9757a72afbf6b56617d2be28573cb8d4b3fb2f6 ",
            "9757a72afbf6b56617d2be28573cb8d4b3fb2f6.",
            "g757a72afbf6b56617d2be28573cb8d4b3fb2f65",
        ] {
            assert_eq!(image_marker(Some(given)), None, "{given:?}");
        }
        assert_eq!(image_marker(None), None);
    }

    /// The release a node's user agent gives, read as darwin-client's node census reads it
    /// (`listings_census::supports_feed`): the semver after `node/`, and the number after the
    /// `ds.` its build metadata starts with.
    fn census_release(agent: &str) -> Option<u64> {
        let version = semver::Version::parse(agent.strip_prefix("node/")?).ok()?;
        version
            .build
            .as_str()
            .strip_prefix("ds.")?
            .split('.')
            .next()?
            .parse()
            .ok()
    }

    /// A marked user agent is still `node/` and a semver, with the release where the node
    /// census reads it, so the census reads a marked node's release as an unmarked one's.
    #[test]
    fn the_node_census_reads_the_release_of_a_marked_user_agent() {
        let plain = user_agent(None);
        let marked = user_agent(Some(MARKER));
        assert!(marked.starts_with("node/"), "{marked}");
        let release = census_release(&plain);
        assert!(
            release.is_some(),
            "this build's version gives no ds release: {plain}"
        );
        assert_eq!(census_release(&marked), release, "{marked}");
        let version = semver::Version::parse(marked.strip_prefix("node/").unwrap()).unwrap();
        assert_eq!(
            version.build.as_str().rsplit('.').next(),
            Some(MARKER),
            "the marker ends the build metadata"
        );
    }

    /// A node with chunks in a store this build cannot read STARTS, however it is
    /// configured, and does not lose them.
    ///
    /// This asserted the opposite until the release that shipped it was reconsidered twice.
    /// First it refused to start, which serves nothing at all and cannot be recovered by
    /// hand, because nothing can hold a node on an older build. Then it deleted whatever it
    /// found, which is data loss for exactly the node that most needs the data: the upgrade
    /// monitor picks the newest eligible release rather than the next one, so a node that was
    /// offline through the previous release arrives here with everything it owns in that
    /// directory and nothing in the file store.
    ///
    /// Both configurations, because they are different code paths: a node with
    /// `storage.enabled = false` never builds a store, and the old environment is on its disk
    /// just the same.
    #[tokio::test]
    async fn a_node_with_an_unmigrated_store_starts_and_keeps_it() {
        for storage_enabled in [true, false] {
            let dir = TempDir::new().expect("temp dir");
            let root = dir.path().join("node");
            let env = root.join(LEGACY_ENV_DIR);
            std::fs::create_dir_all(&env).expect("mkdir");
            std::fs::write(env.join("data.mdb"), b"chunks that were never copied out")
                .expect("seed");

            let port = rand::thread_rng().gen_range(TEST_PORT_RANGE);
            let mut config = local_node_config(&root, port);
            config.storage.enabled = storage_enabled;

            let node = NodeBuilder::new(config).build().await.unwrap_or_else(|e| {
                panic!(
                    "a node with an unmigrated store must start (storage.enabled = \
                     {storage_enabled}): {e}"
                )
            });
            settle();
            assert!(
                env.join("data.mdb").exists(),
                "chunks that were never copied out were deleted (storage.enabled = \
                 {storage_enabled})"
            );
            node.shutdown.cancel();
        }
    }

    /// A node with storage switched off deletes nothing, even a leftover marked finished.
    ///
    /// It never builds a store, so nothing about this node establishes that those chunks were
    /// copied anywhere: the mark is a claim made by a previous release about a file store
    /// this process has not opened and will not open. It also has no use for the disk. So the
    /// directory stays and stays recoverable.
    #[tokio::test]
    async fn a_node_with_storage_disabled_deletes_nothing() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("node");
        let env = root.join(LEGACY_ENV_DIR);
        std::fs::create_dir_all(&env).expect("mkdir");
        std::fs::write(
            env.join("data.mdb"),
            b"bytes retirement had already cleared",
        )
        .expect("seed");
        std::fs::write(env.join("RETIRED"), b"retired").expect("mark");

        let port = rand::thread_rng().gen_range(TEST_PORT_RANGE);
        let mut config = local_node_config(&root, port);
        config.storage.enabled = false;
        let node = NodeBuilder::new(config).build().await.expect("must start");
        settle();
        assert!(
            env.join("data.mdb").exists(),
            "a node that never opened a store deleted one on the strength of a mark it \
             could not check"
        );
        node.shutdown.cancel();
    }

    /// A store the migration finished with is removed, once the file store has opened.
    #[tokio::test]
    async fn a_finished_store_is_removed_once_the_replacement_opens() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("node");
        let env = root.join(LEGACY_ENV_DIR);
        std::fs::create_dir_all(&env).expect("mkdir");
        std::fs::write(
            env.join("data.mdb"),
            b"bytes retirement had already cleared",
        )
        .expect("seed");
        std::fs::write(env.join("RETIRED"), b"retired").expect("mark");

        let port = rand::thread_rng().gen_range(TEST_PORT_RANGE);
        let node = NodeBuilder::new(local_node_config(&root, port))
            .build()
            .await
            .expect("a node with a finished leftover must start");
        assert!(wait_gone(&env));
        node.shutdown.cancel();
    }

    /// Nothing is deleted until the store that replaced it has actually opened.
    ///
    /// Retirement counted on every chunk the node kept being in the file store, which is only
    /// true if the file store opens. An earlier version deleted first and let the constructor fail behind it,
    /// on an unreadable layout or a directory it could not create, and a node that lost both
    /// stores that way had nothing left to go back to.
    #[tokio::test]
    async fn a_finished_store_survives_a_file_store_that_will_not_open() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("node");
        let env = root.join(LEGACY_ENV_DIR);
        std::fs::create_dir_all(&env).expect("mkdir");
        std::fs::write(
            env.join("data.mdb"),
            b"bytes retirement had already cleared",
        )
        .expect("seed");
        std::fs::write(env.join("RETIRED"), b"retired").expect("mark");

        // A file where the chunk directory has to be, so the store cannot create it.
        std::fs::write(root.join("chunks"), b"not a directory").expect("block the store");

        let port = rand::thread_rng().gen_range(TEST_PORT_RANGE);
        let built = NodeBuilder::new(local_node_config(&root, port))
            .build()
            .await;
        assert!(
            built.is_err(),
            "the file store was supposed to fail to open"
        );
        settle();
        assert!(
            env.join("data.mdb").exists(),
            "the old store was deleted before the one replacing it could open"
        );
    }

    /// The removal runs on its own thread.
    fn settle() {
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    /// The removal runs on its own thread, so give it a moment.
    fn wait_gone(path: &std::path::Path) -> bool {
        for _ in 0..200 {
            if !path.exists() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        false
    }

    /// Build a node on a random port of the test range, retrying a bind failure.
    async fn build_on_a_free_port(root: &std::path::Path) -> RunningNode {
        build_on_a_free_port_paid_on(root, crate::config::EvmNetworkConfig::default()).await
    }

    /// [`build_on_a_free_port`], the node paid on `network`.
    async fn build_on_a_free_port_paid_on(
        root: &std::path::Path,
        network: crate::config::EvmNetworkConfig,
    ) -> RunningNode {
        let mut last_err = String::new();
        for _ in 0..BIND_ATTEMPTS {
            let port = rand::thread_rng().gen_range(TEST_PORT_RANGE);
            let mut config = local_node_config(root, port);
            config.payment.evm_network = network.clone();
            match NodeBuilder::new(config).build().await {
                Ok(node) => return node,
                Err(e) => {
                    last_err = e.to_string();
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
            }
        }
        panic!("could not build a node after {BIND_ATTEMPTS} attempts: {last_err}");
    }

    /// A built node records listings in one service: the chunk handler records
    /// them from PUTs, the replication engine from fresh offers, and the service
    /// watches the node's chunk store for envelopes stored and chunks removed.
    /// A node assembled by hand can leave any of these out with every other
    /// test still passing.
    #[tokio::test]
    async fn a_built_node_keeps_listings() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("node");
        std::fs::create_dir_all(&root).expect("mkdir");
        let node = build_on_a_free_port(&root).await;

        let protocol = node
            .ant_protocol
            .as_ref()
            .expect("a node with storage has a chunk handler");
        let recorded = protocol
            .listing_service()
            .expect("the chunk handler records listings from PUTs");
        let engine = node
            .replication_engine
            .as_ref()
            .expect("a built node has a replication engine");
        let offered = engine
            .listings()
            .expect("the replication engine records listings from fresh offers");
        assert!(
            Arc::ptr_eq(recorded, offered),
            "the chunk handler and the replication engine must share one listing service"
        );
        assert_eq!(
            protocol.storage().observers_set(),
            (true, true),
            "the listing service must watch the node's chunk store for stored and removed chunks"
        );
        node.shutdown.cancel();
    }

    /// A built node on the production network takes the block its payment
    /// vault was deployed in from its setting, or, with none, as after an
    /// upgrade of its binary alone, the one built in. A node on another
    /// network has none.
    #[tokio::test]
    async fn a_built_node_knows_the_block_its_vault_was_deployed_in() {
        use crate::config::{
            EvmNetworkConfig, PRODUCTION_PAYMENT_TOKEN, PRODUCTION_PAYMENT_VAULT,
            PRODUCTION_VAULT_DEPLOYMENT_BLOCK,
        };
        let production = |block| EvmNetworkConfig::Custom {
            rpc_url: "http://127.0.0.1:0".to_string(),
            payment_token_address: PRODUCTION_PAYMENT_TOKEN.to_string(),
            payment_vault_address: PRODUCTION_PAYMENT_VAULT.to_string(),
            payment_vault_deployment_block: block,
            root_vault_address: None,
        };
        for (network, expected) in [
            (production(None), Some(PRODUCTION_VAULT_DEPLOYMENT_BLOCK)),
            (production(Some(81_000_000)), Some(81_000_000)),
            (EvmNetworkConfig::ArbitrumOne, None),
        ] {
            let dir = TempDir::new().expect("temp dir");
            let root = dir.path().join("node");
            std::fs::create_dir_all(&root).expect("mkdir");
            let node = build_on_a_free_port_paid_on(&root, network.clone()).await;
            let listings = node
                .ant_protocol
                .as_ref()
                .and_then(|protocol| protocol.listing_service())
                .expect("a built node keeps listings");
            assert_eq!(listings.vault_deployment_block(), expected, "{network:?}");
            node.shutdown.cancel();
        }
    }

    /// A built node on the production network takes the root vault built in,
    /// with no setting, as after an upgrade of its binary alone, and its
    /// payment check and its listings read the same one. A node on another
    /// network has none, and refuses root proofs.
    #[tokio::test]
    async fn a_built_node_knows_its_root_vault_only_on_the_production_network() {
        use crate::config::{
            EvmNetworkConfig, PRODUCTION_PAYMENT_TOKEN, PRODUCTION_PAYMENT_VAULT,
            PRODUCTION_ROOT_VAULT,
        };
        let production = EvmNetworkConfig::Custom {
            rpc_url: "http://127.0.0.1:0".to_string(),
            payment_token_address: PRODUCTION_PAYMENT_TOKEN.to_string(),
            payment_vault_address: PRODUCTION_PAYMENT_VAULT.to_string(),
            payment_vault_deployment_block: None,
            root_vault_address: None,
        };
        let built_in = production.root_vault().expect("parses").vault();
        assert_eq!(
            built_in.map(|vault| format!("0x{}", hex::encode(vault.address))),
            Some(PRODUCTION_ROOT_VAULT.to_lowercase())
        );
        for (network, expected) in [
            (production, built_in),
            (EvmNetworkConfig::ArbitrumOne, None),
        ] {
            let dir = TempDir::new().expect("temp dir");
            let root = dir.path().join("node");
            std::fs::create_dir_all(&root).expect("mkdir");
            let node = build_on_a_free_port_paid_on(&root, network.clone()).await;
            let protocol = node
                .ant_protocol
                .as_ref()
                .expect("a built node has a protocol");
            assert_eq!(
                protocol.payment_verifier().root_vault(),
                expected,
                "{network:?}"
            );
            let listings = protocol
                .listing_service()
                .expect("a built node keeps listings");
            assert_eq!(listings.vaults().root, expected, "{network:?}");
            node.shutdown.cancel();
        }
    }

    /// What a node logs at start about its root vault.
    #[test]
    fn the_root_vault_line_says_where_the_vault_is_from() {
        let vault = ant_listings::RootVault {
            address: [0xf6; 20],
            token: [0x51; 20],
        };
        let shown = format!(
            "{}",
            evmlib::common::Address::from_slice(vault.address.as_slice())
        );
        assert_eq!(
            root_vault_line(RootVaultSetting::Given(vault)),
            format!("Root vault {shown}")
        );
        assert_eq!(
            root_vault_line(RootVaultSetting::BuiltIn(vault)),
            format!("Root vault {shown}, the production network's: no setting gives one")
        );
        assert_eq!(
            root_vault_line(RootVaultSetting::None),
            "No root vault is known for this network: root proofs are refused"
        );
    }

    /// A production-network node whose setting gives a root vault other than
    /// the built-in one is warned at start; the built-in vault given, no
    /// setting, and another network are not.
    #[test]
    fn a_production_node_is_warned_when_its_setting_overrides_the_built_in_root_vault() {
        let other = ant_listings::RootVault {
            address: [0x9f; 20],
            token: [0x51; 20],
        };
        let line = root_vault_override_line(true, RootVaultSetting::Given(other))
            .expect("an override on the production network is warned");
        assert!(
            line.contains(&format!("0x{}", hex::encode(other.address))),
            "{line}"
        );
        assert!(
            line.contains(crate::config::PRODUCTION_ROOT_VAULT),
            "{line}"
        );

        let mut built_in = other;
        built_in.address.copy_from_slice(
            &hex::decode(crate::config::PRODUCTION_ROOT_VAULT.trim_start_matches("0x"))
                .expect("hex"),
        );
        assert_eq!(
            root_vault_override_line(true, RootVaultSetting::Given(built_in)),
            None
        );
        assert_eq!(
            root_vault_override_line(true, RootVaultSetting::BuiltIn(built_in)),
            None
        );
        assert_eq!(root_vault_override_line(true, RootVaultSetting::None), None);
        assert_eq!(
            root_vault_override_line(false, RootVaultSetting::Given(other)),
            None
        );
    }

    /// What a node logs once at start about the block its payment vault was
    /// deployed in: nothing more for a block a setting gives, which the
    /// listing service names; the production network's, built in, when none
    /// does; and on another network, that it knows none.
    #[test]
    fn the_vault_deployment_line_says_where_the_block_is_from() {
        assert_eq!(
            vault_deployment_line(VaultDeploymentBlock::Given(81_897_121)),
            None
        );
        assert_eq!(
            vault_deployment_line(VaultDeploymentBlock::BuiltIn(81_897_121)).as_deref(),
            Some(
                "No setting gives the block the payment vault was deployed in; taking \
                 81897121, the production network's"
            )
        );
        assert_eq!(
            vault_deployment_line(VaultDeploymentBlock::Unknown).as_deref(),
            Some(
                "No block the payment vault was deployed in is known for this network: \
                 listings showing any block are checked on chain alone"
            )
        );
    }

    use super::*;
    use crate::config::NODES_SUBDIR;

    /// Listing requests take permits of their own, 16, never those chunk
    /// requests take: with all of them held, a request waits until one frees,
    /// or is dropped once the node starts stopping. A request's permit is
    /// held until its answer has been sent. An answer to one of the node's
    /// own requests takes none.
    #[tokio::test]
    async fn listing_requests_take_permits_of_their_own() {
        use ant_listings::{ListingKey, ListingsBody, ListingsMessage};

        let dir = TempDir::new().expect("temp dir");
        let chunks = Arc::new(
            ChunkStore::new(ChunkStoreConfig {
                root_dir: dir.path().to_path_buf(),
                ..ChunkStoreConfig::test_default()
            })
            .await
            .expect("chunk store"),
        );
        let listings = crate::listings::ListingService::new(
            dir.path(),
            0,
            chunks,
            "http://127.0.0.1:0".into(),
            ant_listings::VaultSet::v2_only([0x69; 20]),
            None,
        )
        .await
        .expect("listing service");
        let permits = listings.request_permits();
        assert_eq!(permits.available_permits(), 16);
        let source = saorsa_core::identity::PeerId::from_bytes([1; 32]);
        let message = |request_id, body| {
            ListingsMessage { request_id, body }
                .encode()
                .expect("encodes")
        };
        let request = message(
            7,
            ListingsBody::ListRequest {
                from: ListingKey::MIN,
                limit: 1,
            },
        );
        let stopping = CancellationToken::new();

        // Every one held: a request waits, and is answered once one frees.
        let held = permits.acquire_many(16).await.expect("permits");
        let answering = RunningNode::listings_answer(&listings, &source, &request, &stopping);
        tokio::pin!(answering);
        let waited = tokio::time::timeout(std::time::Duration::from_millis(200), &mut answering);
        assert!(waited.await.is_err(), "answered without a permit");
        drop(held);
        let (permit, answer) = answering.await.expect("an answer");
        let answer = ListingsMessage::decode(&answer).expect("decodes");
        assert_eq!(answer.request_id, 7);
        assert!(matches!(answer.body, ListingsBody::ListResponse { .. }));
        // The answer comes with its permit.
        assert_eq!(permits.available_permits(), 15);
        drop(permit);
        assert_eq!(permits.available_permits(), 16);

        // The permit is held until the answer has been sent: one waiting to
        // go out counts against the 16 as one being built does.
        let send_done = tokio::sync::Notify::new();
        let finish = &send_done;
        let sending = RunningNode::answer_listings(
            &listings,
            &source,
            &request,
            &stopping,
            move |answer| async move {
                let answer = ListingsMessage::decode(&answer).expect("decodes");
                assert_eq!(answer.request_id, 7);
                assert_eq!(permits.available_permits(), 15, "sent without its permit");
                finish.notified().await;
            },
        );
        tokio::pin!(sending);
        let waited = tokio::time::timeout(std::time::Duration::from_millis(200), &mut sending);
        assert!(waited.await.is_err(), "the send was not waited for");
        assert_eq!(
            permits.available_permits(),
            15,
            "the permit was let go before the answer was sent"
        );
        send_done.notify_one();
        sending.await;
        assert_eq!(permits.available_permits(), 16);

        // An answer to a request of the node's takes none.
        let held = permits.acquire_many(16).await.expect("permits");
        let response = message(8, ListingsBody::GetResponse { listing: None });
        let taken = RunningNode::listings_answer(&listings, &source, &response, &stopping).await;
        assert!(taken.is_none());
        // A request waiting when the node starts stopping is dropped.
        let answering = RunningNode::listings_answer(&listings, &source, &request, &stopping);
        stopping.cancel();
        assert!(answering.await.is_none());
        drop(held);
    }

    /// What a node logs at start about where it is paid: the token, the vault
    /// and the RPC's host, never the rest of the URL.
    #[test]
    fn the_payment_line_names_the_token_vault_and_rpc_host_alone() {
        let darwin = crate::config::EvmNetworkConfig::Custom {
            rpc_url: "https://rpc.mainnet.chain.robinhood.com".to_string(),
            payment_token_address: "0x511f76a3de41f000ec11ea4fd212fb3587c6cef3".to_string(),
            payment_vault_address: "0xE0f627986c0438De0E314B644f3C8aA21D3f4f6B".to_string(),
            payment_vault_deployment_block: Some(81_897_121),
            root_vault_address: None,
        }
        .into_evm_network();
        assert_eq!(
            payment_settings(&darwin),
            "Payments: token 0x511f76A3De41F000EC11EA4fD212fb3587c6cef3, \
             vault 0xE0f627986c0438De0E314B644f3C8aA21D3f4f6B, \
             RPC host rpc.mainnet.chain.robinhood.com"
        );

        let keyed = evmlib::Network::new_custom(
            "https://key-user:key-password@rpc.example.org:8545/v2/key-in-path?apikey=key-in-query",
            "0x511f76a3de41f000ec11ea4fd212fb3587c6cef3",
            "0xE0f627986c0438De0E314B644f3C8aA21D3f4f6B",
        );
        let line = payment_settings(&keyed);
        assert!(line.ends_with(", RPC host rpc.example.org"), "{line}");
        for secret in [
            "key-user",
            "key-password",
            "8545",
            "key-in-path",
            "key-in-query",
        ] {
            assert!(!line.contains(secret), "the line shows {secret:?}: {line}");
        }
    }

    #[test]
    fn test_build_upgrade_monitor_staged_rollout_enabled() {
        let config = NodeConfig {
            upgrade: crate::config::UpgradeConfig {
                staged_rollout_hours: 24,
                ..Default::default()
            },
            ..Default::default()
        };
        let seed = b"node-seed";

        let monitor = NodeBuilder::build_upgrade_monitor(&config, seed);
        assert!(monitor.has_staged_rollout());
    }

    #[test]
    fn test_build_upgrade_monitor_staged_rollout_disabled() {
        let config = NodeConfig {
            upgrade: crate::config::UpgradeConfig {
                staged_rollout_hours: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let seed = b"node-seed";

        let monitor = NodeBuilder::build_upgrade_monitor(&config, seed);
        assert!(!monitor.has_staged_rollout());
    }

    #[test]
    fn test_build_core_config_sets_production_mode() {
        let config = NodeConfig {
            network_mode: NetworkMode::Production,
            ..Default::default()
        };
        let core = NodeBuilder::build_core_config(&config).expect("core config");
        assert!(core.diversity_config.is_some());
    }

    #[test]
    fn test_build_core_config_ipv4_only() {
        let config = NodeConfig {
            ipv4_only: true,
            ..Default::default()
        };
        let core = NodeBuilder::build_core_config(&config).expect("core config");
        assert!(!core.ipv6, "ipv4_only should disable IPv6");
    }

    #[test]
    fn test_build_core_config_dual_stack_by_default() {
        let config = NodeConfig::default();
        let core = NodeBuilder::build_core_config(&config).expect("core config");
        assert!(core.ipv6, "dual-stack should be the default");
    }

    #[test]
    fn test_build_core_config_sets_development_mode_permissive() {
        let config = NodeConfig {
            network_mode: NetworkMode::Development,
            ..Default::default()
        };
        let core = NodeBuilder::build_core_config(&config).expect("core config");
        let diversity = core.diversity_config.expect("diversity");
        assert_eq!(diversity.max_per_ip, Some(usize::MAX));
        assert_eq!(diversity.max_per_subnet, Some(usize::MAX));
    }

    /// What `stop_relay_serving` relies on, checked against saorsa-transport's relay
    /// server itself: it serves by default, and once switched off it refuses a session,
    /// whether the peer asks to send to one address or to any, with a 503 and before
    /// binding a socket for it. Nothing upstream calls or tests the switch, so a
    /// dependency update that changes what it does fails here.
    #[tokio::test]
    async fn a_relay_server_switched_off_refuses_every_session() {
        use saorsa_transport::masque::{ConnectUdpRequest, MasqueRelayConfig, MasqueRelayServer};

        let relay: std::net::SocketAddr = "203.0.113.1:10000".parse().unwrap();
        let peer: std::net::SocketAddr = "198.51.100.7:40000".parse().unwrap();
        let target: std::net::SocketAddr = "192.0.2.9:53".parse().unwrap();

        let server = MasqueRelayServer::new(MasqueRelayConfig::default(), relay);
        assert!(
            server.is_relay_serving_enabled(),
            "saorsa-transport serves relays unless told not to"
        );

        server.set_relay_serving_enabled(false);
        for request in [
            ConnectUdpRequest::bind_any(),
            ConnectUdpRequest::target(target),
        ] {
            let response = server
                .handle_connect_request(&request, peer, None)
                .await
                .unwrap();
            assert_eq!(response.status, 503, "{request:?} must be refused");
            assert!(response.proxy_public_address.is_none());
        }
        assert_eq!(server.session_count().await, 0);
    }

    /// The routing loop's receive step: a receiver that fell behind the broadcast
    /// carries on with the oldest events still held, and only a closed stream ends it.
    #[tokio::test]
    async fn routing_carries_on_after_falling_behind_and_ends_when_the_stream_closes() {
        let (events, mut rx) = broadcast::channel::<u32>(4);
        for event in 0..10 {
            events.send(event).unwrap();
        }
        // 6 of the 10 were overwritten before the first read, which reports the lag.
        for expected in 6..10 {
            assert_eq!(next_routing_event(&mut rx).await, Some(expected));
        }
        events.send(10).unwrap();
        assert_eq!(next_routing_event(&mut rx).await, Some(10));

        // Falling behind again later is no different.
        for event in 11..20 {
            events.send(event).unwrap();
        }
        assert_eq!(next_routing_event(&mut rx).await, Some(16));

        drop(events);
        for expected in 17..20 {
            assert_eq!(next_routing_event(&mut rx).await, Some(expected));
        }
        assert_eq!(next_routing_event(&mut rx).await, None);
    }

    #[test]
    fn test_scan_identity_dirs_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dirs = NodeBuilder::scan_identity_dirs(tmp.path()).unwrap();
        assert!(dirs.is_empty(), "expected no identity dirs, got {dirs:?}");
    }

    #[test]
    fn test_scan_identity_dirs_nonexistent_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nonexistent_identity_dir");
        let dirs = NodeBuilder::scan_identity_dirs(&path).unwrap();
        assert!(dirs.is_empty(), "expected no identity dirs, got {dirs:?}");
    }

    #[test]
    fn test_scan_identity_dirs_finds_one() {
        let tmp = tempfile::tempdir().unwrap();
        let node_dir = tmp.path().join("abc123");
        std::fs::create_dir_all(&node_dir).unwrap();
        std::fs::write(node_dir.join(NODE_IDENTITY_FILENAME), "{}").unwrap();

        let dirs = NodeBuilder::scan_identity_dirs(tmp.path()).unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0], node_dir);
    }

    #[test]
    fn test_scan_identity_dirs_finds_multiple() {
        let tmp = tempfile::tempdir().unwrap();
        for name in &["node_a", "node_b"] {
            let dir = tmp.path().join(name);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(NODE_IDENTITY_FILENAME), "{}").unwrap();
        }
        // A directory without a key file should be ignored
        std::fs::create_dir_all(tmp.path().join("no_key")).unwrap();

        let dirs = NodeBuilder::scan_identity_dirs(tmp.path()).unwrap();
        assert_eq!(dirs.len(), 2);
    }

    #[tokio::test]
    async fn test_resolve_identity_first_run_creates_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = NodeConfig {
            root_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };

        let identity = NodeBuilder::resolve_identity(&mut config).await.unwrap();
        // Key file should exist
        assert!(tmp.path().join(NODE_IDENTITY_FILENAME).exists());
        // peer_id should be derivable from the identity
        let peer_id = identity.peer_id().to_hex();
        assert_eq!(peer_id.len(), 64); // 32 bytes hex-encoded
    }

    #[tokio::test]
    async fn test_resolve_identity_loads_existing() {
        let tmp = tempfile::tempdir().unwrap();

        // Generate and save an identity
        let original = NodeIdentity::generate().unwrap();
        original
            .save_to_file(&tmp.path().join(NODE_IDENTITY_FILENAME))
            .await
            .unwrap();

        let mut config = NodeConfig {
            root_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };

        let loaded = NodeBuilder::resolve_identity(&mut config).await.unwrap();
        assert_eq!(loaded.peer_id(), original.peer_id());
    }

    #[test]
    fn test_peer_id_hex_length() {
        let id = saorsa_core::identity::PeerId::from_bytes([0x42; 32]);
        let hex = id.to_hex();
        assert_eq!(hex.len(), 64); // 32 bytes = 64 hex chars
    }

    /// Simulates a node restart: first run creates identity in a scoped subdir
    /// under `nodes/`, second run discovers and reloads it — `peer_id` must be
    /// identical and the directory name is the full 64-char hex peer ID.
    #[tokio::test]
    async fn test_identity_persisted_across_restarts() {
        let base_dir = tempfile::tempdir().unwrap();
        let nodes_dir = base_dir.path().join(NODES_SUBDIR);

        // First "boot": generate identity, save it in nodes/{peer_id}/
        let identity1 = NodeIdentity::generate().unwrap();
        let peer_id1 = identity1.peer_id().to_hex();
        let peer_dir = nodes_dir.join(&peer_id1);
        std::fs::create_dir_all(&peer_dir).unwrap();
        identity1
            .save_to_file(&peer_dir.join(NODE_IDENTITY_FILENAME))
            .await
            .unwrap();

        // Verify directory name is the full 64-char hex peer ID
        assert_eq!(peer_id1.len(), 64);
        assert_eq!(peer_dir.file_name().unwrap().to_string_lossy(), peer_id1);

        // Second "boot": scan should find and reload the same identity
        let identity_dirs = NodeBuilder::scan_identity_dirs(&nodes_dir).unwrap();
        assert_eq!(identity_dirs.len(), 1);
        let loaded = NodeIdentity::load_from_file(&identity_dirs[0].join(NODE_IDENTITY_FILENAME))
            .await
            .unwrap();
        let peer_id2 = loaded.peer_id().to_hex();

        assert_eq!(peer_id1, peer_id2, "peer_id must survive restart");
        assert_eq!(
            identity_dirs[0], peer_dir,
            "root_dir must be the same directory"
        );
    }

    /// When two identity subdirs exist under `nodes/`, the scan finds multiple
    /// and the resolve path would error asking for `--root-dir`.
    #[tokio::test]
    async fn test_multiple_identities_errors() {
        let base_dir = tempfile::tempdir().unwrap();
        let nodes_dir = base_dir.path().join(NODES_SUBDIR);

        // Create two identity subdirectories under nodes/
        for name in &["aaaa", "bbbb"] {
            let dir = nodes_dir.join(name);
            std::fs::create_dir_all(&dir).unwrap();
            let identity = NodeIdentity::generate().unwrap();
            identity
                .save_to_file(&dir.join(NODE_IDENTITY_FILENAME))
                .await
                .unwrap();
        }

        let identity_dirs = NodeBuilder::scan_identity_dirs(&nodes_dir).unwrap();
        assert_eq!(identity_dirs.len(), 2, "should find both identity dirs");
    }

    /// With a non-default `root_dir` (explicit path), the identity is created on
    /// first run and reloaded on subsequent runs from the same directory.
    #[tokio::test]
    async fn test_explicit_root_dir_persists_across_restarts() {
        let tmp = tempfile::tempdir().unwrap();

        // First boot — non-default root_dir triggers explicit path
        let mut config1 = NodeConfig {
            root_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let identity1 = NodeBuilder::resolve_identity(&mut config1).await.unwrap();

        // Second boot — same dir
        let mut config2 = NodeConfig {
            root_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let identity2 = NodeBuilder::resolve_identity(&mut config2).await.unwrap();

        assert_eq!(
            identity1.peer_id(),
            identity2.peer_id(),
            "explicit --root-dir must yield stable identity"
        );
    }

    /// The prefix is what keeps this node in its peers' routing tables, and the version is
    /// this build's, with nothing after it but the image marker.
    #[test]
    fn the_user_agent_is_this_builds_version_under_the_node_prefix() {
        assert_eq!(
            super::user_agent(None),
            format!("node/{}", env!("CARGO_PKG_VERSION"))
        );
        assert!(super::user_agent(None).starts_with("node/"));
        let marker = "9757a72afbf6b56617d2be28573cb8d4b3fb2f65";
        assert_eq!(
            super::user_agent(Some(marker)),
            format!("node/{}.image.{marker}", env!("CARGO_PKG_VERSION"))
        );
    }

    /// The first upgrade check comes a few minutes after start, at a random point, rather
    /// than an hour later.
    #[test]
    fn the_first_upgrade_check_comes_one_to_five_minutes_after_start() {
        let delays: Vec<u64> = (0..1000)
            .map(|_| super::first_upgrade_check_delay().as_secs())
            .collect();
        assert!(
            delays.iter().all(|secs| (60..=300).contains(secs)),
            "{delays:?}"
        );
        assert!(
            delays.iter().any(|secs| *secs < 120) && delays.iter().any(|secs| *secs > 240),
            "spread over the window"
        );
    }
}

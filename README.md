# darwin-node

**Quantum-proof storage nodes for the Darwin network.**

darwin-node is the node software for Darwin, our storage network. It is a fork
of [Autonomi's ant-node](https://github.com/WithAutonomi/ant-node)
(MIT/Apache-2.0) and keeps upstream's crate and binary name, `ant-node`. Most
of this README is upstream's. Where it talks about the "legacy" network, it
means Autonomi's earlier network, which Darwin never ran.

## What differs in this fork

- **Payments are in DARWIN on Robinhood Chain** (chain ID 4663): token
  `0x511f76A3De41F000EC11EA4fD212fb3587c6cef3` through the payment vault
  `0xE0f627986c0438De0E314B644f3C8aA21D3f4f6B`, at prices set by
  `PRICE_MULTIPLIER` in `vendor/ant-protocol` (32). The storage-node Docker
  image sets this with `ANT_EVM_RPC_URL`, `ANT_EVM_PAYMENT_TOKEN` and
  `ANT_EVM_PAYMENT_VAULT`. The bare binary still defaults to Arbitrum One
  (Autonomi's contracts): run it on Darwin only with those three set (see
  [Join the Network](#join-the-network)).
- **Batch payments are refused** (from ds.9). Nodes take per-chunk
  (single-node) payments only. A merkle candidate quote request, versioned
  or not, is answered `QuoteFailed("merkle (batch) payments are not
  accepted; pay per chunk")` before anything is signed: a batch is paid
  on-chain before any PUT, so refusing the quote is what keeps a client from
  paying for storage it won't get. A merkle proof on a PUT or a fresh
  replication offer is refused with the same message. Neighbour-sync repair
  carries no proof, so chunks already stored keep being repaired. Upstream's
  merkle code stays compiled, unused, so merges stay cheap.
- **Payment checks fall back across RPCs** (from ds.8). A node reads a
  storage payment (`completedPayments`) through its own RPC and, when that is one of
  Robinhood Chain's public RPCs, through the others, in up to 2 rounds half a
  second apart. Only a failed call moves on: any answer, even that nothing
  was paid, settles the check. Upstream reads once, through one RPC, so a
  provider answering `-32005 the network is busy` made nodes refuse paid
  chunks. Reads of listings' payments have fallen back the same way since
  ds.4.
- **Relay serving can be turned off** (from ds.10). saorsa-core gives every
  node a MASQUE relay server, on which any peer can open a session and have
  the node send UDP for it to any address. `ANT_RELAY_SERVING=false` (or
  `--relay-serving false`, or `relay_serving = false` in a config file)
  flips saorsa-core's switch for it, which nothing upstream calls, as the
  node starts: relay session requests are then answered 503, as by a full
  relay. The node can still use other peers' relays. Unset, nodes serve
  relays, as upstream's do. The Docker image passes the variable through to
  every node.
- **Request routing survives falling behind** (from ds.10). The loop that
  hands chunk and listings requests to their handlers reads saorsa-core's
  event broadcast, which keeps 1,000 events. Upstream's loop stopped for good
  when it fell further behind than that, and the node then answered no
  request while it stayed connected and looked healthy. This one logs a
  warning, skips the events it missed and carries on, as the replication
  engine already did; only a closed stream ends it.
- **Nodes keep data while the disk has room.** A node keeps the records and
  pointers it holds after it stops being responsible for them, and goes on
  serving them and offering them to their close group. It erases only when
  free space on the disk holding its data falls below 10%, and then only
  records far from it (more than 10 times the distance to its 7th-nearest
  peer), farthest first, each after the nodes now nearest to the record prove
  they hold it, until 15% is free again. Upstream deletes such records once
  they have been out of range for three days, whether or not the disk needs
  the room. A test (`src/replication/retention/census.rs`) lists every
  call in the code that can remove stored data, with the reason it is there,
  so a change that adds one fails the test suite until it is reviewed.
- **No bootstrap peers ship with the binary.** `config/bootstrap_peers.toml`
  is empty on purpose, so an unconfigured node can't join Autonomi's network.
  The image passes Darwin's bootstrap servers itself.
- **Run nodes with the Docker image**: [docker/README.md](docker/README.md).
  The network runs in production mode, in which each node's routing table
  takes at most 2 nodes from one IP address, and 5 from one /24 subnet, in
  each bucket and among the 20 nodes nearest that node. More nodes on one IP
  are turned away from some of those places and add load for less storage,
  so run 2 nodes per IP.
- **Releases are Linux x86_64 only**, built and signed by
  `.github/workflows/release.yml`. Auto-upgrade reads this repository's
  releases without credentials. The storage-node image's `:latest` moves
  only when a release is published (from ds.10): release.yml then points it
  at the image the Build workflow made from the release's commit. A push to
  main publishes the image as `:<commit>` only.
- **Release builds link self_encryption** (GPL-3.0 with MaidSafe's linking
  exception), which upstream's don't: `vendor/ant-listings` reads the data
  maps in file envelopes with it. The exception lets the binary be
  distributed under its own licence; self_encryption stays under GPL-3.0.
  Release archives include `THIRD-PARTY-NOTICES.txt`, generated by
  `scripts/third_party_notices/` with the licences and notices of every
  crate in the build (self_encryption's in full), and the Rust standard
  library's `RUST-STD-COPYRIGHT.html`. The Docker image has them in
  `/usr/share/doc/storage-node/`, with a `NOTICE` for the `ant` node manager
  it bundles.
- **Rate limits**: only browser (WebRTC Direct) sessions and requests are
  limited. New connections over saorsa-core's rate per IP are only logged,
  and unpaid storage and quote requests aren't limited; see [Rate
  Limiting](#rate-limiting).

---

## Table of Contents

1. [The Quantum Threat](#the-quantum-threat)
2. [Key Advantages Over Legacy ant-node](#key-advantages-over-legacy-ant-node)
3. [Post-Quantum Cryptography](#post-quantum-cryptography)
4. [NAT Traversal](#nat-traversal)
5. [Network Hardening](#network-hardening)
6. [Dual IPv4/IPv6 DHT](#dual-ipv4ipv6-dht)
7. [Migration from Legacy ant-node](#migration-from-legacy-ant-node)
8. [Development Status](#development-status)
9. [Auto-Upgrade System](#auto-upgrade-system)
10. [Running as a Service](#running-as-a-service)
11. [Architecture](#architecture)
12. [Quick Start](#quick-start)
13. [CLI Reference](#cli-reference)
14. [Configuration](#configuration)
15. [Security Considerations](#security-considerations)
16. [Related Projects](#related-projects)

---

## The Quantum Threat

### Harvest Now, Decrypt Later (HNDL)

Nation-state actors and sophisticated adversaries are already collecting encrypted network traffic today. When cryptographically relevant quantum computers (CRQCs) become available, they will decrypt this harvested data. This is known as the **Harvest Now, Decrypt Later (HNDL)** attack.

**Timeline:**
- NIST estimates CRQCs capable of breaking RSA-2048 and ECC in **10-15 years**
- Some researchers suggest it could be sooner
- Data stored today on decentralized networks must remain secure for **decades**

### Why Classical Cryptography Fails

| Algorithm | Type | Quantum Attack | Time to Break |
|-----------|------|----------------|---------------|
| RSA-2048 | Asymmetric | Shor's Algorithm | Hours |
| ECDSA/Ed25519 | Signatures | Shor's Algorithm | Hours |
| X25519/ECDH | Key Exchange | Shor's Algorithm | Hours |
| AES-256 | Symmetric | Grover's Algorithm | Still secure* |

*Symmetric algorithms remain secure with sufficient key sizes (256-bit), but key exchange and signatures are completely broken.

### The Legacy Network Risk

Legacy ant-node uses:
- **BLS signatures** - Quantum vulnerable (elliptic curve based)
- **Ed25519 signatures** - Quantum vulnerable (elliptic curve based)
- **X25519 key exchange** - Quantum vulnerable (elliptic curve based)

Even though self-encrypted data on the legacy network is secure, the legacy network has metadata (signatures, encrypted keys) that will be compromised by quantum computers. **The time to act is now.**

---

## Key Advantages Over Legacy ant-node

| Feature | Legacy ant-node | ant-node |
|---------|----------|-------------|
| **Digital Signatures** | BLS/Ed25519 (quantum-vulnerable) | ML-DSA-65 (FIPS 204, quantum-proof) |
| **Key Exchange** | X25519 (quantum-vulnerable) | ML-KEM-768 (FIPS 203, quantum-proof) |
| **NAT Traversal** | None (requires port forwarding) | Native QUIC traversal (100% success, no STUN/ICE) |
| **DHT Architecture** | IPv4-only | Dual IPv4/IPv6 with separate close groups |
| **Sybil Resistance** | Basic per-IP limits | Multi-layer: subnet, ASN, geographic, node age |
| **Node Limiting** | None | Prevents large actors spinning up millions of nodes |
| **Trust System** | Manual metrics | EigenTrust++ with data integrity & replica health |
| **Geographic Routing** | None | 7-region distribution (no entity holds multiple copies) |
| **Eclipse Protection** | Limited | Diversity scoring, ASN limits, detection algorithms |
| **Auto-Upgrade** | Manual | Staged rollout of signed releases (this fork: Linux x86_64 only) |
| **Rate Limiting** | Basic | Browser sessions and requests; new connections per IP are only logged (see [Rate Limiting](#rate-limiting)) |
| **Node Identity** | Ed25519 pubkey (unused, never signs data) | ML-DSA-65 pubkey (actively signs all operations) |
| **Legacy Data** | N/A | Full support for classical encrypted data |

The Sybil resistance, node limiting, geographic routing and eclipse
protection rows are upstream's design. What this code applies is in
[Routing-Table Admission](#routing-table-admission).

---

## Post-Quantum Cryptography

ant-node implements **pure post-quantum cryptography** with no hybrid fallbacks. All algorithms are NIST FIPS-approved standards.

### ML-DSA-65 (FIPS 204) - Digital Signatures

- **Security Level**: 128-bit quantum security (NIST Level 2)
- **Public Key Size**: 1,952 bytes
- **Signature Size**: 3,309 bytes
- **Use Cases**: Node identity, message authentication, upgrade verification

ML-DSA (Module-Lattice Digital Signature Algorithm), formerly known as CRYSTALS-Dilithium, is based on the hardness of lattice problems that remain intractable for quantum computers.

### ML-KEM-768 (FIPS 203) - Key Encapsulation

- **Security Level**: 128-bit quantum security (NIST Level 2)
- **Public Key Size**: 1,184 bytes
- **Ciphertext Size**: 1,088 bytes
- **Shared Secret**: 32 bytes
- **Use Cases**: Secure key exchange, encrypted communications

ML-KEM (Module-Lattice Key Encapsulation Mechanism), formerly CRYSTALS-Kyber, provides secure key exchange resistant to both classical and quantum attacks.

### ChaCha20-Poly1305 - Symmetric Encryption

- **Security Level**: 256-bit (quantum-resistant with Grover consideration)
- **Use Cases**: Data encryption, authenticated encryption

Symmetric algorithms remain quantum-resistant at sufficient key sizes. ChaCha20-Poly1305 provides high-performance authenticated encryption.

### No Hybrid Mode

Unlike some transitional implementations, ant-node uses **pure post-quantum cryptography**:

- No Ed25519 fallback
- No X25519 hybrid key exchange
- No classical signature chains

This ensures maximum security and eliminates the complexity of dual-algorithm systems.

### Backward Compatibility

saorsa-core fully supports **legacy classical encrypted data**:

- **Existing Data**: AES-256-GCM-SIV encrypted content from legacy ant-node remains readable
- **New Signatures**: All new signatures use ML-DSA-65 (quantum-proof)
- **New Key Exchange**: All new key exchanges use ML-KEM-768 (quantum-proof)
- **Gradual Migration**: Network can operate with mixed classical/quantum-proof data

This allows seamless migration without data loss while ensuring all new data is quantum-secure.

---

## NAT Traversal

### Everyone Can Run a Node

One of the most significant advantages of ant-node is **full native QUIC NAT traversal**. Unlike legacy ant-node, which requires manual port forwarding, saorsa-core implements NAT traversal directly within the QUIC protocol itself - **no ICE, no STUN, no external protocols**.

> **100% Success Rate**: In our testing, we have successfully traversed 100% of network connections using native QUIC NAT traversal.

### The Legacy ant-node Limitation

Legacy ant-node has **no native NAT traversal**:
- Requires manual router configuration or UPnP
- Excludes users behind carrier-grade NAT (CGNAT)
- Excludes most mobile and residential users
- Limits network participation to technically savvy operators

### Native QUIC NAT Traversal

Based on **draft-seemann-quic-nat-traversal-02** (Marten Seemann & Eric Kinnear, Apple Inc.), saorsa-core implements NAT traversal as a pure QUIC protocol extension.

**Why Not ICE/STUN?**

| Aspect | ICE/STUN | Native QUIC (Darwin) |
|--------|----------|----------------------|
| **External Dependencies** | Requires STUN/TURN servers | None - 100% within QUIC |
| **Protocol Complexity** | Separate signaling (SDP) | QUIC frames only |
| **Path Validation** | ICE connectivity checks | Native QUIC PATH_CHALLENGE |
| **Connection Migration** | Separate step after ICE | Automatic QUIC feature |
| **Amplification Protection** | STUN fingerprints | QUIC rate limits |

**Three Custom QUIC Frames:**

1. **ADD_ADDRESS** - Server advertises candidate addresses to peer
2. **PUNCH_ME_NOW** - Coordinates simultaneous hole punching
3. **REMOVE_ADDRESS** - Removes stale address candidates

### How It Works

```
1. Node starts -> enumerates local interfaces and addresses
2. Peers exchange addresses via ADD_ADDRESS frames
3. Client sends PUNCH_ME_NOW to coordinate timing
4. Both peers simultaneously send QUIC PATH_CHALLENGE packets
5. NAT bindings created -> PATH_RESPONSE packets received
6. QUIC connection migrates to direct path automatically
7. Application data flows directly (no relay needed)
```

**Key Innovation**: Uses QUIC's existing path validation mechanism (RFC 9000 Section 8.2) as the actual NAT traversal technique, eliminating external protocol dependencies entirely.

### Benefits

- **Universal Participation**: Anyone can run a node, regardless of network configuration
- **No Port Forwarding**: Works automatically on any network
- **No External Servers**: No STUN/TURN infrastructure required
- **CGNAT Support**: Works on mobile networks and carrier-grade NAT
- **Zero Configuration**: Just start the node - NAT traversal is automatic
- **Increased Decentralization**: More participants = more resilient network

This ensures that **everyone can contribute to the network**, democratizing participation beyond those with favorable network configurations or technical expertise.

---

## Network Hardening

> **Upstream's design, mostly not in this code.** Of what follows,
> saorsa-core applies only IP limits in each node's routing table and
> trust-based replacement, as described in [Routing-Table
> Admission](#routing-table-admission). Node IDs are not bound to IP
> addresses, and there are no ASN, VPN, node-age, resource-proof or
> geographic checks.

### Multi-Layer Sybil Resistance

Sybil attacks involve creating many fake identities to gain disproportionate influence. ant-node implements defense-in-depth:

#### Layer 1: IPv6 Node Identity Binding

```
NodeID = SHA256(IPv6_Address || PublicKey || Salt)
```

Each node's identity is cryptographically bound to its IPv6 address, making identity spoofing detectable.

#### Layer 2: Subnet Limits

Prevents concentration of nodes in single networks:

| Scope | Maximum Nodes |
|-------|---------------|
| Per /64 subnet | 1 |
| Per /48 subnet | 3 |
| Per /32 subnet | 10 |
| Per ASN | 20 |

VPN and hosting providers have **halved limits** to prevent abuse.

#### Layer 3: Node Age & Preference

The network **actively prefers older, more proven nodes**. This creates a natural barrier against attackers who would need to maintain nodes for extended periods before gaining significant influence.

| Status | Age Requirement | Trust Weight | Capabilities |
|--------|-----------------|--------------|--------------|
| **New** | 0-24 hours | 0.25x | Limited routing, no close group membership |
| **Young** | 1-7 days | 0.5x | Standard routing, limited replication |
| **Established** | 7-30 days | 1.0x | Full participation, all operations |
| **Veteran** | 30+ days | 1.5x | Trusted bootstrap, priority in close groups |

**Why Node Age Matters:**
- **Attack Cost**: Attackers must invest months to gain meaningful network influence
- **Proven Reliability**: Older nodes have demonstrated consistent uptime
- **Historical Trust**: Long-running nodes have accumulated positive EigenTrust scores
- **Priority Routing**: Veteran nodes receive routing preference, improving reliability

#### Layer 4: Node Limiting

Prevents **large actors from spinning up millions of nodes at once**:

- **Rate-limited Registration**: New nodes must wait before full participation
- **Resource Proof**: Nodes must demonstrate actual storage/bandwidth capacity
- **Progressive Trust**: Influence grows slowly with proven good behavior
- **Suspicious Pattern Detection**: Rapid node deployment triggers investigation

This makes **hostile takeover economically infeasible** - an attacker would need to:
1. Acquire millions of unique IP addresses across diverse subnets and ASNs
2. Maintain nodes for months to accumulate trust
3. Provide actual storage and bandwidth resources
4. Avoid triggering pattern detection algorithms

#### Layer 5: Geographic Distribution

Ensures close groups aren't dominated by nodes in single datacenters or regions.

### EigenTrust++ Reputation System

Automated reputation scoring based on observable behavior. This is a **foundational system** that we continue to build upon with additional integrity checks.

**Core Scoring Factors:**
- **Response Rate**: Percentage of valid responses to requests
- **Uptime**: Time node has been continuously available
- **Storage Reliability**: Successful data retrievals
- **Bandwidth Contribution**: Network throughput provided

**Advanced Integrity Checks (Building on EigenTrust):**
- **Data Integrity Verification**: Periodic challenges to verify nodes actually store the data they claim
- **Replica Health Monitoring**: Continuous verification that replicas are valid and accessible
- **Proof of Storage**: Cryptographic proofs that data exists without retrieving entire content
- **Cross-Node Validation**: Nodes verify each other's claims through random sampling

**Key Features:**
- **Time Decay**: Trust decays at 0.99x per hour (recent behavior matters more)
- **Pre-trusted Bootstrap**: Initial nodes have verified trust scores
- **Automatic Removal**: Nodes below threshold are automatically excluded
- **Convergence**: Iterative algorithm converges to stable trust values
- **Extensible Framework**: New verification methods can be added as the network evolves

**Trust Score Impact:**
| Trust Level | Multiplier | Effect |
|-------------|------------|--------|
| Very Low | 0.1x | Excluded from routing, data migrated away |
| Low | 0.5x | Reduced routing priority, monitored |
| Normal | 1.0x | Standard participation |
| High | 1.5x | Preferred for routing and storage |
| Very High | 2.0x | Priority close group membership, bootstrap eligible |

The EigenTrust++ system provides the foundation for **continuous network health monitoring**, ensuring that bad actors are detected and isolated before they can cause harm.

### Geographic Routing

7-region latency-aware distribution ensures **no single entity can hold more than one copy of any piece of data**:

1. **North America**
2. **South America**
3. **Europe**
4. **Africa**
5. **Asia Pacific**
6. **Middle East**
7. **Oceania**

**Data Distribution Rules:**
- Close groups are constructed with **geographic diversity requirements**
- Each replica of data is stored in a **different geographic region** where possible
- Nodes from the same ASN/organization cannot hold multiple replicas of the same data
- XorName-based addressing combined with geographic constraints ensures distribution

**Why This Matters:**
- **No Single Point of Failure**: Data survives regional outages or censorship
- **No Entity Concentration**: Even large operators cannot hold multiple copies
- **Regulatory Resilience**: Data exists across multiple legal jurisdictions
- **Attack Resistance**: Compromising data requires attacking nodes worldwide

**Benefits:**
- Prevents datacenter concentration in close groups
- Optimizes latency for data retrieval (nearest geographic replica)
- Ensures regulatory diversity (no single jurisdiction dominance)
- Makes coordinated data destruction practically impossible

---

## Dual IPv4/IPv6 DHT

ant-node implements a novel dual-stack DHT architecture that maximizes network connectivity.

### Separate Close Groups

Each XorName has **two** close groups:
- IPv4 close group (K=20 closest IPv4 nodes)
- IPv6 close group (K=20 closest IPv6 nodes)

### Cross-Replication

Data is replicated to **both** close groups:
- IPv4-only nodes can always retrieve IPv4-stored data
- IPv6-only nodes can always retrieve IPv6-stored data
- Dual-stack nodes can retrieve from either

### Happy Eyeballs (RFC 8305)

For connections, ant-node implements Happy Eyeballs:
1. Attempt IPv6 connection first
2. Start IPv4 connection after short delay
3. Use whichever completes first
4. Cache successful connection type for peer

### Benefits

- **Maximum Connectivity**: Works on IPv4-only, IPv6-only, or dual-stack networks
- **Future-Proof**: Ready for IPv6-only internet segments
- **Resilient**: Network partition in one IP version doesn't affect the other

---

## Migration from Legacy ant-node

> **Not in this code.** This section is upstream's design for moving data
> from Autonomi's legacy network. Neither upstream v0.21.0 nor this fork has
> the migration tool: there is no `--migrate-ant-data` flag, no `[migration]`
> config section and no legacy-network payment check. It does not apply to
> Darwin, which has no legacy network.

### Payment Model

Every new record is paid for on chain. In Darwin that is DARWIN on Robinhood
Chain (see [What differs in this fork](#what-differs-in-this-fork)).

Verification:
1. **LRU Cache**: Fast lookup of recently verified XorNames
2. **EVM Verification**: Verify on-chain payment for new data

### CRDT Data: The Identity Challenge

Mutable data types (Scratchpad, Pointer, GraphEntry) present a **fundamental challenge** for migration because they are owner-indexed and require valid signatures.

#### Why CRDT Data Cannot Be Directly Migrated

**ant-nodes are pure post-quantum** - they only understand ML-DSA-65 signatures:

```
User: "Here's my Scratchpad update, signed with Ed25519"
ant-node: "Invalid signature - rejected"
```

This is by design. Adding classical crypto verification would:
- Increase attack surface
- Defeat the purpose of quantum-proof security
- Create complexity that could introduce vulnerabilities

#### The Two-Network Reality

```
+---------------------------------------------------------------------+
|                     TWO SEPARATE NETWORKS                            |
+--------------------------------+------------------------------------+
|      LEGACY NETWORK            |         DARWIN NETWORK             |
|      (Classical Crypto)        |         (Quantum Crypto)           |
+--------------------------------+------------------------------------+
|  Nodes: legacy ant-node        |  Nodes: ant-node                   |
|  Signatures: Ed25519/BLS       |  Signatures: ML-DSA-65 ONLY        |
|  Key Exchange: X25519          |  Key Exchange: ML-KEM-768          |
|                                |                                    |
|  Scratchpad address:           |  Scratchpad address:               |
|  hash("scratchpad:" ||         |  hash("scratchpad:" ||             |
|       Ed25519_pubkey)          |       ML-DSA_65_pubkey)            |
|           |                    |           |                        |
|    0x7a3f...                   |    0xb2c8...                       |
|                                |                                    |
|  DIFFERENT ADDRESSES - SAME USER, DIFFERENT IDENTITY                |
+--------------------------------+------------------------------------+
```

#### What This Means for Users

| Data Type | Legacy -> Darwin | Notes |
|-----------|-------------------|-------|
| **Chunk** | Migratable | Content-addressed, no signature needed |
| **Scratchpad** | New identity | Different address on PQ network |
| **Pointer** | New identity | Different address on PQ network |
| **GraphEntry** | New identity | Different address on PQ network |

**Users don't "migrate" their CRDT data - they establish a new quantum-safe identity on the Darwin network.**

#### The HybridClient Architecture

The HybridClient connects to **both networks simultaneously**:

```rust
// HybridClient talks to TWO different networks
let hybrid = HybridClient::new(config)?;

// Read from the legacy network (via legacy ant-node peers)
let legacy_data = hybrid.legacy().get_scratchpad(&ed25519_owner).await?;

// Write to the Darwin network (via ant-node peers)
let new_scratchpad = hybrid.quantum().put_scratchpad(
    &mldsa_keypair,
    content_type,
    legacy_data.payload().to_vec(),
).await?;

// Note: These have DIFFERENT addresses!
// legacy_data.address() != new_scratchpad.address()
```

#### Migration Strategies for CRDT Data

**Strategy 1: Data Snapshot + New Identity**
```
1. Read final state from the legacy network (LegacyClient)
2. Generate new ML-DSA-65 keypair (new identity)
3. Create equivalent Scratchpad on PQ network (QuantumClient)
4. Update applications to use new address
5. Old legacy data becomes a read-only archive
```

**Strategy 2: Identity Registry**
```
1. Create a public "migration registry" on PQ network
2. Users register: Ed25519_pubkey_hash -> ML-DSA_pubkey_hash
3. Applications can resolve old addresses to new ones
4. Provides continuity for social/reputation systems
```

**Strategy 3: Parallel Operation (Transition Period)**
```
1. The legacy network continues running for existing CRDT data
2. Users gradually create new identities on PQ network
3. Applications support both networks during transition
4. Eventually deprecate legacy access
```

#### Chunk Migration: The Easy Case

Chunks are content-addressed and immutable - they can be directly migrated:

```rust
// Chunks have the SAME address on both networks
let chunk = hybrid.legacy().get_chunk(&xorname).await?;
hybrid.quantum().store_chunk(chunk.data()).await?;

// Address is hash(content) - identical on both networks
```

#### Security Implications

| Aspect | Legacy network | Darwin |
|--------|-------------------|--------------|
| **Signatures** | Ed25519 | ML-DSA-65 |
| **Quantum Safe** | Vulnerable | Protected |
| **CRDT Updates** | Via legacy network only | Via Darwin only |
| **Identity** | Ed25519 pubkey | ML-DSA-65 pubkey |

**Critical**: Users cannot update their legacy CRDT data through PQ ant-nodes. To update legacy data, they must connect to the legacy network directly (which continues to run legacy ant-nodes).

#### Long-Term Vision

```
Phase 1 (Now):     Both networks operate, HybridClient bridges them
Phase 2 (Transition): Users migrate to PQ identities, legacy read-only
Phase 3 (Future):  Legacy deprecated, PQ network is the primary network
```

The goal is not to maintain classical crypto forever, but to provide a **clean migration path** where users can:
1. Access their existing data (read-only eventually)
2. Establish new quantum-safe identities
3. Transition at their own pace

---

## Development Status

### Current Implementation Status

| Component | Status | Description |
|-----------|--------|-------------|
| **Core Library** | Complete | Full node implementation with client APIs |
| **Data Types** | Complete | Chunk, Scratchpad, Pointer, GraphEntry |
| **Payment Verification** | Complete | EVM verification + LRU cache |
| **Auto-Upgrade** | Complete | ML-DSA-65 signature verification; this fork releases Linux x86_64 only |
| **E2E Test Infrastructure** | Complete | Real P2P testnet with 25+ nodes |

### Test Coverage

Run `cargo test --features test-utils` (the EVM tests need Foundry's
`anvil`). The library alone has about 1,300 tests. CI runs the suite weekly
and by hand, not on every push (`.github/workflows/test.yml`).

### Data Types Supported

| Type | Size Limit | Addressing | Mutability | Use Cases |
|------|------------|------------|------------|-----------|
| **Chunk** | 4 MB | Content-addressed (SHA256) | Immutable | Files, documents, media |
| **Scratchpad** | 4 MB | Owner public key | Mutable (CRDT counter) | User profiles, settings |
| **Pointer** | 32 bytes | Owner public key | Mutable (counter) | Mutable references, DNS-like |
| **GraphEntry** | 100 KB | Content + owner + parents | Immutable | Version control, social feeds |

### Migration Capability

None in this code: there is no `src/migration`, and `src/client/` only
re-exports the shared protocol types from `ant_protocol` (the client is
[darwin-client](https://github.com/Project-DSpace/darwin-client)). Payment
verification (`src/payment/`) checks the LRU cache, then the payment on chain.

### E2E Test Infrastructure

The test infrastructure spawns real P2P networks for integration testing:

```rust
// Spawn a 25-node testnet
let harness = TestHarness::setup().await?;

// Store and retrieve data across nodes
let chunk_addr = harness.node(5).store_chunk(&data).await?;
let retrieved = harness.node(20).get_chunk(&chunk_addr).await?;

// With EVM payment verification
let harness = TestHarness::setup_with_evm().await?;
assert!(harness.anvil().is_healthy().await);
```

For the direct-browser testnet, where every node exposes WebRTC Direct and a
default immutable file is published at startup, see
[Browser-enabled local testnet](docs/WEBRTC_DIRECT_TESTNET.md).

### Roadmap

| Phase | Target | Status |
|-------|--------|--------|
| **Phase 1** | Core implementation | Complete |
| **Phase 2** | E2E test infrastructure | Complete |
| **Phase 3** | Darwin production network | Running since 2026-10-06 |

---

## Auto-Upgrade System

ant-node automatically stays up-to-date with secure, verified upgrades. Upstream releases for Windows, macOS and Linux; this fork releases **Linux x86_64 only** (`ant-node-cli-linux-x64`), so nodes on other platforms don't upgrade themselves. Nodes read the releases of `Project-DSpace/darwin-node` without credentials.

### Cross-Platform Support (upstream)

| Platform | Architecture | Binary |
|----------|--------------|--------|
| **Linux** | x86_64, aarch64 | `ant-node-linux-*` |
| **macOS** | x86_64, aarch64 (Apple Silicon) | `ant-node-darwin-*` |
| **Windows** | x86_64 | `ant-node-windows-*.exe` |

The auto-upgrade system automatically detects the current platform and downloads the correct binary.

### How It Works

1. **Monitor**: Periodically check GitHub releases for new versions
2. **Download**: Fetch the correct binary for the current platform
3. **Verify**: Validate ML-DSA-65 signature against embedded public key
4. **Stage**: Download completes before any changes are made
5. **Apply**: Replace current binary and restart
6. **Rollback**: Automatic rollback if new version fails health checks

### Network-Safe Staged Rollout

To prevent **network collapse** from simultaneous upgrades, ant-node implements **randomized staged rollout**:

**How Staged Rollout Works:**
- Each node adds a **random delay** (0-24 hours) before applying upgrades
- Delay is deterministically derived from node ID (consistent but distributed)
- Critical security updates can override with shorter delay windows
- Network maintains minimum online node threshold during rollout

**Why This Matters:**
- **No Mass Restart**: Nodes don't all restart simultaneously
- **Network Continuity**: DHT maintains routing capability throughout upgrade
- **Data Availability**: Close groups retain quorum during transition
- **Rollback Safety**: Problems detected early before full network deployment

```
Upgrade Timeline (example):
Hour 0:  Release published
Hour 1:  ~4% of nodes upgrade (random delay 0-1h)
Hour 6:  ~25% of nodes upgraded
Hour 12: ~50% of nodes upgraded
Hour 24: ~100% of nodes upgraded
```

### Security Guarantees

**Embedded Public Key:**
The ML-DSA-65 verification key is compiled into the binary at build time. It cannot be changed by:
- Configuration files
- Environment variables
- Network messages
- Remote commands

The only way to change the signing key is a manual upgrade with a new binary.

**Signature Verification:**
- Every release is signed with the project's ML-DSA-65 private key
- Signature size: 3,309 bytes
- Failed verification = upgrade rejected
- Tampered binaries cannot be installed

### Unattended Operation

ant-node is designed for **true unattended operation**:

- **Zero Intervention**: Nodes upgrade themselves without human action
- **Self-Healing**: Failed upgrades automatically rollback
- **Health Monitoring**: Post-upgrade health checks verify functionality
- **Notification**: Optional alerts for upgrade events (success/failure)

This enables node operators to deploy and forget, knowing their nodes will stay current and secure.

### Release Channels

| Channel | Description | Rollout Speed |
|---------|-------------|---------------|
| **stable** | Production releases, thoroughly tested | 24-hour staged |
| **beta** | Pre-release versions for early testing | 6-hour staged |

```bash
# Use stable channel (default)
ant-node --upgrade-channel stable

# Use beta channel for testing
ant-node --upgrade-channel beta
```

### Configuration

```toml
[upgrade]
channel = "stable"
check_interval_hours = 1
github_repo = "Project-DSpace/darwin-node"
stop_on_upgrade = false  # Set true when running under a service manager
# staged_rollout_hours = 24  # For staged rollout
```

---

## Running as a Service

For production deployments, run ant-node under a service manager so it restarts automatically after upgrades.

In Darwin, use the Docker image ([docker/README.md](docker/README.md)) instead. If you run the binary under a service manager anyway, add the settings from [Join the Network](#join-the-network) to the command line, since the examples below would start a node with no peers that checks payments on Arbitrum One. This fork releases Linux x86_64 only, so the macOS and Windows examples apply to builds from source.

### systemd (Linux)

Create a service file at `/etc/systemd/system/ant-node.service`:

```ini
[Unit]
Description=Darwin Node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/ant-node --stop-on-upgrade
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ant-node
```

Key settings:
- `--stop-on-upgrade` tells the node to exit cleanly after applying an upgrade
- `Restart=always` ensures systemd restarts the node after the upgrade exit

### launchd (macOS)

Create a plist at `~/Library/LaunchAgents/com.autonomi.node.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.autonomi.node</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/ant-node</string>
        <string>--stop-on-upgrade</string>
    </array>
    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.autonomi.node.plist
```

### Windows Service

Use [WinSW](https://github.com/winsw/winsw) or [NSSM](https://nssm.cc/) to wrap ant-node as a Windows service.

With WinSW, create `ant-node-service.xml`:

```xml
<service>
  <id>ant-node</id>
  <name>Darwin Node</name>
  <executable>C:\ant-node\ant-node.exe</executable>
  <arguments>--stop-on-upgrade</arguments>
  <onfailure action="restart" delay="5 sec"/>
</service>
```

The node exits with code 100 on upgrade, which WinSW treats as a failure and restarts.

### Standalone Mode (No Service Manager)

Without `--stop-on-upgrade`, the node spawns the upgraded binary as a new process before exiting. This is the default behavior and requires no service manager.

---

## Architecture

ant-node follows a **thin wrapper** design philosophy, adding minimal code on top of saorsa-core.

### Component Responsibilities

| Component | Provider | Description |
|-----------|----------|-------------|
| **P2P Networking** | saorsa-core | QUIC transport, connection management |
| **DHT Routing** | saorsa-core | Trust-weighted Kademlia |
| **Reputation** | saorsa-core | EigenTrust++ engine |
| **Security** | saorsa-core | Rate limiting, blacklisting, diversity scoring |
| **Content Storage** | saorsa-core | Local chunk storage |
| **Replication** | saorsa-core | Data redundancy management |
| **Auto-Upgrade** | ant-node | Binary update system |
| **Migration** | ant-node | Legacy ant-node data import |
| **CLI** | ant-node | User interface |

### Code Size

ant-node adds approximately **1,000 lines** of new code:
- Easy to audit
- Minimal attack surface
- Clear separation of concerns

### Dependency Chain

```
ant-node
    +-- saorsa-core
            +-- saorsa-pqc (ML-DSA, ML-KEM)
```

---

## Quick Start

### Prerequisites

- Rust 1.95+ (for building from source)
- Linux, macOS, or Windows

### Build from Source

```bash
# Clone the repository
git clone https://github.com/Project-DSpace/darwin-node
cd darwin-node

# Build release binary
cargo build --release

# Binary location
./target/release/ant-node --version
```

### Run with Defaults

```bash
# Start node with default settings
./target/release/ant-node
```

### Join the Network

On first startup, the node needs bootstrap peers to join the network.
Upstream ships Autonomi's peers in `bootstrap_peers.toml` next to the binary;
in this fork that file is empty, so pass Darwin's peers yourself, along with
the Robinhood Chain payment settings (the binary otherwise checks payments on
Arbitrum One). Set a fixed `--port` as well: the default, 0, picks a random
port, which a node behind a router can't have forwarded (UDP):

```bash
./target/release/ant-node \
    --rewards-address 0xYOUR_WALLET_ADDRESS \
    --port 10000 \
    --ipv4-only \
    --bootstrap "142.93.32.90:10000" \
    --bootstrap "165.22.193.90:10000" \
    --bootstrap "142.93.96.232:10000" \
    --evm-rpc-url https://rpc.mainnet.chain.robinhood.com \
    --evm-payment-token 0x511f76A3De41F000EC11EA4fD212fb3587c6cef3 \
    --evm-payment-vault 0xE0f627986c0438De0E314B644f3C8aA21D3f4f6B
```

The Docker image does all of this for you ([docker/README.md](docker/README.md)).

### Full Configuration

```bash
./target/release/ant-node \
    --root-dir ~/.ant-node \
    --port 12000 \
    --upgrade-channel stable \
    --enable-logging \
    --log-level info
```

---

## CLI Reference

```
ant-node [OPTIONS]

Options:
    --root-dir <PATH>
        Node data directory
        [default: ~/.ant-node]

    --port <PORT>
        Listening port (0 for automatic selection)
        [default: 0]

    --ipv4-only
        Force IPv4-only mode (disable dual-stack).
        Use on hosts without working IPv6.

    --relay-serving <BOOL>
        Serve MASQUE relay sessions for other peers: true or false
        [default: true]

    --bootstrap <ADDR>
        Bootstrap peer socket addresses (can be specified multiple times)
        Example: 1.2.3.4:10000

    --upgrade-channel <CHANNEL>
        Release channel: stable, beta
        [default: stable]

    --network-mode <MODE>
        production, testnet or development
        [default: production]

    --rewards-address <ADDRESS>
        Wallet that receives storage fees

    --evm-rpc-url <URL>, --evm-payment-token <ADDRESS>, --evm-payment-vault <ADDRESS>
        Custom EVM network; all three together replace --evm-network
        (default: arbitrum-one). Darwin: Robinhood Chain, DARWIN token and vault

    --enable-logging
        Turn on log output; the other --log-* options need it

    --log-level <LEVEL>
        Log verbosity: trace, debug, info, warn, error
        [default: info]

    -h, --help
        Print help information

    -V, --version
        Print version information
```

Run `ant-node --help` for the full list.

---

## Configuration

Configuration sources (highest to lowest priority):

1. **Command-line arguments** (e.g., `--bootstrap`)
2. **Environment variables** (`ANT_*`)
3. **Configuration file** (`--config path/to/config.toml`)
4. **Bootstrap peers file** (auto-discovered `bootstrap_peers.toml`)

### Bootstrap Peers File

The `bootstrap_peers.toml` file provides initial peers for joining the network.
It is shipped alongside the binary in release archives and auto-discovered on startup
when no `--bootstrap` CLI argument is provided. In this fork the shipped file
is empty (`peers = []`); pass peers with `--bootstrap` or `ANT_BOOTSTRAP`.

**Format:**
```toml
peers = [
    "10.0.0.1:10000",
    "10.0.0.2:10000",
]
```

**Search order** (first match wins):

| Priority | Source | Path |
|----------|--------|------|
| 1 | `$ANT_BOOTSTRAP_PEERS_PATH` env var | Explicit path to file |
| 2 | Executable directory | `<exe_dir>/bootstrap_peers.toml` |
| 3 | Platform config dir | Linux: `~/.config/ant/bootstrap_peers.toml` |
|   |                      | macOS: `~/Library/Application Support/ant/bootstrap_peers.toml` |
|   |                      | Windows: `%APPDATA%\ant\bootstrap_peers.toml` |
| 4 | System config (Unix) | `/etc/ant/bootstrap_peers.toml` |

**Bootstrap peer precedence** (highest wins):
1. `--bootstrap` CLI argument or `ANT_BOOTSTRAP` env var
2. `bootstrap = [...]` in a `--config` file
3. Auto-discovered `bootstrap_peers.toml`
4. Empty (node cannot join an existing network)

### Environment Variables

```bash
export ANT_ROOT_DIR=~/.ant-node
export ANT_PORT=12000
export ANT_UPGRADE_CHANNEL=stable
export ANT_NETWORK_MODE=production
export ANT_IPV4_ONLY=true
export ANT_RELAY_SERVING=true   # the default; false refuses relay sessions
export ANT_REWARDS_ADDRESS=0xYOUR_WALLET_ADDRESS
export ANT_EVM_RPC_URL=https://rpc.mainnet.chain.robinhood.com
export ANT_EVM_PAYMENT_TOKEN=0x511f76A3De41F000EC11EA4fD212fb3587c6cef3
export ANT_EVM_PAYMENT_VAULT=0xE0f627986c0438De0E314B644f3C8aA21D3f4f6B
export RUST_LOG=info   # log level, with ANT_ENABLE_LOGGING=true
```

Every option has an `ANT_*` variable; `ant-node --help` lists them.
Upgrades are always on: there is no switch to turn them off.

### Configuration File

`~/.ant-node/config.toml`:

```toml
root_dir = "~/.ant-node"
port = 0  # Auto-select
relay_serving = true  # false: refuse MASQUE relay sessions
bootstrap = [
    "10.0.0.1:10000",
    "10.0.0.2:10000",
]

[upgrade]
# Upgrades are always enabled; configure behavior here
channel = "stable"
check_interval_hours = 1
github_repo = "Project-DSpace/darwin-node"
stop_on_upgrade = false
```

The file can't set payments: the command line (or its `ANT_*` variables)
always sets the rewards address, the EVM network and the cache, and replaces
any `[payment]` section. The same goes for `network_mode` and the upgrade
channel. There is no `[migration]` section.

---

## Security Considerations

### Rate Limiting

What is limited:

| Limit | Where | Value |
|-------|-------|-------|
| Browser (WebRTC Direct) sessions and requests | `WebRtcDirectConfig` in `src/config.rs` | per IP, per session and in total |
| Storage and listings requests handled at once | `start_protocol_routing` in `src/node.rs` | 64 per node, from all peers together |

Logged only: saorsa-core's transport counts new connections, per IP and in
total, against 1,000 per 60 s with bursts of 100. A connection over the
limit gets a `Rate-limited incoming connection` warning in the log, but
stays open, and its messages are handled like any other's.

Not limited (a known gap): unpaid chunk PUTs and quote requests over the
storage protocol. A peer holding quotes can send unpaid PUTs, and each one
costs the node chain RPC calls. Requests beyond the 64 being handled wait
their turn, with no limit on how many wait or on any one peer's share.

### Routing-Table Admission

A node's ID is the BLAKE3 hash of its ML-DSA-65 public key, so anyone can
make as many as they like. When saorsa-core adds a peer to a node's routing
table, it applies:

- **IP diversity**, in production mode: at most 2 peers with one IP
  address, and 5 from one /24 (IPv4) or /48 (IPv6) subnet, in each bucket
  and among the 20 peers nearest the node. A closer peer over a limit takes
  the place of the farthest one on that address or subnet, unless that one
  has a trust score of 0.7 or more and was seen recently. Testnet and
  development mode lift both limits. The limits apply within each node's
  own routing table: they don't cap how many nodes from one IP are among
  the 7 that store a chunk.
- **Trust**: when a bucket is full, the peer with the lowest trust score
  below saorsa-core's swap threshold gives way to a newcomer above it.

Nothing else is checked: there is no blacklist, no ASN or geographic check,
no eclipse detection and no node-age or resource proof. [Network
Hardening](#network-hardening) above describes upstream's design, not this
code.

### Relay Serving

Unless relay serving is off, a node accepts MASQUE relay sessions from any
peer and sends UDP for them, from its own address, to any address. Replies
reach it on a port it binds at random for each session, so where a firewall
drops traffic to those ports, only the sending half works.
`ANT_RELAY_SERVING=false` turns relay serving off (see [What differs in this
fork](#what-differs-in-this-fork)).

### Logs

Logging is off unless `--enable-logging` (or `ANT_ENABLE_LOGGING=true`) is
set. `--log-level` and `RUST_LOG` take one level for every module:
`error`, `warn`, `info`, `debug` or `trace`. A directive such as
`ant_node=debug` is refused, and the node exits at start. For debug output:

```bash
RUST_LOG=debug ./ant-node --enable-logging
```

At `info`, the log shows rate-limited connections (as warnings), each change
to the 20 peers nearest the node (`K-closest peer swap completed`) and each
relay session the node serves. It also shows one line per prune pass, about
every neighbour-sync cycle: `Prune pass complete: …` for chunks and `Pointer
prune pass complete: …` for pointers, with `disk_pressure=1` while the node
is erasing and `pruned=` the records erased in that pass. `Disk pressure:`
and `Disk pressure over:` mark when erasing starts and stops.

---

## Related Projects

| Project | Description | Repository |
|---------|-------------|------------|
| **saorsa-core** | Core networking and security library | [github.com/WithAutonomi/saorsa-core](https://github.com/WithAutonomi/saorsa-core) |
| **saorsa-pqc** | Post-quantum cryptography primitives | [github.com/WithAutonomi/saorsa-pqc](https://github.com/WithAutonomi/saorsa-pqc) |
| **darwin** | Client CLI for file and chunk operations with EVM payments | [github.com/Project-DSpace/darwin-client](https://github.com/Project-DSpace/darwin-client) |

---

## License

This project is dual-licensed under MIT and Apache-2.0.

---

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch
3. Run tests and clippy before submitting
4. Submit a pull request

### Development

```bash
# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -- --enable-logging

# Check for issues
cargo clippy -- -D warnings

# Format code
cargo fmt
```

---

**darwin-node**: Securing the future of decentralized data, one quantum-proof node at a time.

### Shared browser application protocol

WebRTC Direct HELLO advertises `chunk_protocol`. A request with this type carries
an encoded `ant_protocol::ChunkMessage` in its binary body; the response uses the
same type and native encoding. Requests use the existing ANT storage handler,
including quote signing, payment proofs, commitment validation and chunk checks.
The authenticated session and per-source resource limits apply as usual. Encoded
messages may be at most 5 MiB; stored records still have the native 4 MiB limit.

`cargo test --test webrtc_direct_devnet --features test-utils seeded_public_file_downloads_and_paid_uploads_over_direct_node_endpoints -- --ignored`
exercises this path over real local WebRTC endpoints and an Anvil payment chain.

Browser transport decisions and startup policy: [ADR-0015](docs/adr/ADR-0015-direct-browser-clients-over-webrtc-direct.md).

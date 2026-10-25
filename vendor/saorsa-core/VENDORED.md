# Vendored from crates.io (saorsa-core 0.29.1, sha256 96eb0b46...; upstream commit 29bb069, tag v0.29.1;
# MIT OR Apache-2.0, https://github.com/WithAutonomi/saorsa-core).
# The copy leaves out the crate's docs/, .github/, editor and agent settings and saved test output,
# none of which the build or the crate's tests read.
# The only change from upstream is the order a peer's addresses are dialled in: a verified Direct
# address before the relay. AddressType::priority (src/peer_record.rs) is Direct, Relay, Unverified,
# Lan; select_dial_candidates_with_context (src/dht_network_manager.rs) dials LAN first for a peer on
# our own WAN IP, then Direct, then the relay (still skipped on our own WAN IP), then Unverified, then
# LAN when nothing else is known; NodeInfo::merge_typed_address (src/dht/core_engine.rs) stores
# addresses in the same order. Weighing one view of a peer against another keeps upstream's order
# (AddressType::tag_rank: Relay, Direct, Unverified, Lan): a lookup hint can't re-tag a relay address
# as Direct, and the lookup's tie-break between reports (best_tier_priority, src/client_routing.rs)
# is unchanged.
# Upstream's tests that pinned relay first now pin direct first; the direct_first_* tests, the tag
# rank and re-tag tests in src/dht/core_engine.rs and the tie-break test in src/client_routing.rs
# are new. Drop this copy once an upstream release dials a verified direct address before a relay.

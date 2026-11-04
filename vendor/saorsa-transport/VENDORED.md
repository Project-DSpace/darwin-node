# Vendored from crates.io (saorsa-transport 0.38.2, sha256 cd334b3d...; upstream commit 5438201, tag v0.38.2;
# MIT OR Apache-2.0, https://github.com/WithAutonomi/saorsa-transport).
# The copy keeps the crate's manifest, licences and src/, and of examples/ and tests/ the targets
# Cargo.toml lists and the files they read. It leaves out the crate's docs/, editor, CI and agent
# settings and notes, packaging and lint settings, Go and protobuf files, shell scripts, README.md,
# CHANGELOG.md, Cargo.lock, Cargo.toml.orig, the disabled tests and examples no target compiles, and
# saved test output, none of which the build or the crate's tests read.
# Cargo warns that the copy's own [profile.release] is ignored. That is expected (cargo ignored it
# when the crate came from crates.io too), and the copy's Cargo.toml is left unpatched on purpose.
# The only change from upstream is to the bookkeeping of accepted connections, in
# src/nat_traversal_api.rs. Upstream's relay endpoint accept loop keyed a connection by the address
# its socket reports, [::ffff:a.b.c.d]:p for an IPv4 peer, in the connection map and the set of
# announced addresses (emitted_established_events), while spawn_connection_handler announced it under
# a.b.c.d:p. remove_connection cleared only the forms still in the map, so a.b.c.d:p stayed announced
# once the connection was gone, and every later connection from the peer's socket was taken for a
# duplicate: accepted, and never handed on. Now NatTraversalEndpoint::take_accepted_connection does
# the bookkeeping of both accept loops, the main endpoint's and a relay endpoint's, under
# normalize_socket_addr(remote_address); remove_connection takes every form of the address that keys
# no connection out of the set, whether or not it removed one; and a connection from an address
# announced while no live connection is held under any of its forms is announced in place of the one
# that is gone, with an INFO line ("accept: <address> was announced for a connection that is gone;
# announcing the new one"). A second connection while one is live is still not announced and gets
# only a relay request handler. The tests at the end of the file's tests module are new:
# accepted_connections_are_keyed_by_the_normalized_address,
# a_reconnect_after_a_relayed_connection_closed_is_announced,
# an_announcement_without_a_live_connection_is_renewed and
# a_second_connection_while_one_is_live_is_not_announced. Drop this copy once an upstream release
# fixes it.

The copy's git objects. The Build workflow compares them with these on every
run, so a change to the copy is a change to this file too. The patched file
is recorded on its own; the rest of the copy (every other file but this one)
is recorded as the sha256 of its `git ls-tree -r` lines, the same as at
commit e43153e, which added the crates.io release unchanged:

- src/nat_traversal_api.rs blob: 83e57d713c1b3a50bba36ab33276917f08dfe423
- rest sha256: b240d2e08308cb63b0abeedb461630343d9e7f0436be9112d056d6d958a15fe9

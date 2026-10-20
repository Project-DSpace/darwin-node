# Vendored from Project-DSpace/darwin-listings 0.3.1, tagged v0.3.1 at commit 84787fea433c43d20786c912ca5ab2e0010ff345, unchanged; only .github/ is left out.
# darwin-listings is public again; whether to return to the git dependency is open.

The copy's git objects, the same as at that commit. The Build workflow
compares them with these on every run:

- src tree: 794969bdca3d3f44bf9ffa386a37ad753f3a9caa
- Cargo.toml blob: 426c4c98274262c387356763d8981229a62499e7

The crate is a member of this repository's workspace, so its own tests run
under this repository's Cargo.lock (`cargo test --locked -p ant-listings`, in
the Test workflow). Its own Cargo.lock is kept as it was released and is not
used here.

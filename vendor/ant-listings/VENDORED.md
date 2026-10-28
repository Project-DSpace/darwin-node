# Vendored from Project-DSpace/darwin-listings 0.4.0, branch root-readers at commit a58de649e66584c0ed7d6e3868d79f37f3aeac40 (not yet tagged), unchanged; only .github/ is left out.
# darwin-listings is public again; whether to return to the git dependency is open.

The copy's git objects, the same as at that commit. The Build workflow
compares them with these on every run:

- src tree: a7f8449e6d9a1c810793d27e05814c98a5188928
- Cargo.toml blob: 034b2354619768a25517638666144bf8a93653d7

The crate is a member of this repository's workspace, so its own tests run
under this repository's Cargo.lock (`cargo test --locked -p ant-listings`, in
the Test workflow). Its own Cargo.lock is kept as it was released and is not
used here.

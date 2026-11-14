# Vendored from Project-DSpace/darwin-listings 0.4.0 with ant-protocol 6.0.0, main at commit a4d5e10bcdc23d6c0693c7d1422f25e1628a8305 (not yet released; 2b1cb84 moved the manifest, lock and changelog to ant-protocol 6.0.0 and saorsa-core 0.30.0, and a4d5e10 changed four loops in the tests of src/feed_sync.rs and src/walk.rs, as the current stable clippy asks), unchanged; only .github/ is left out.
# darwin-listings is public again; whether to return to the git dependency is open.

The copy's git objects, the same as at that commit. The Build workflow
compares them with these on every run:

- src tree: 1898f77ec3710125b61682085f09c9c2d656b029
- Cargo.toml blob: d6309456d053102e6a59eb7dc534ea79881c9c6d

The crate is a member of this repository's workspace, so its own tests run
under this repository's Cargo.lock (`cargo test --locked -p ant-listings`, in
the Test workflow). Its own Cargo.lock is kept as it was released and is not
used here.

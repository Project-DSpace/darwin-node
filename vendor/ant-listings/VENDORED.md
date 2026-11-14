# Vendored from Project-DSpace/darwin-listings 0.4.0 with ant-protocol 6.0.0, branch upstream-0.24 at commit 2b1cb84923f2dbd8b9092bc9b85eb6c6a0d0be86 (not yet released; main's a58de649 with only the manifest, lock and changelog moved to ant-protocol 6.0.0 and saorsa-core 0.30.0), unchanged; only .github/ is left out.
# darwin-listings is public again; whether to return to the git dependency is open.

The copy's git objects, the same as at that commit. The Build workflow
compares them with these on every run:

- src tree: a7f8449e6d9a1c810793d27e05814c98a5188928
- Cargo.toml blob: d6309456d053102e6a59eb7dc534ea79881c9c6d

The crate is a member of this repository's workspace, so its own tests run
under this repository's Cargo.lock (`cargo test --locked -p ant-listings`, in
the Test workflow). Its own Cargo.lock is kept as it was released and is not
used here.

# Vendored from Project-DSpace/darwin-listings 0.4.0, branch root-readers at commit 7e709e0f825d3650560f494f85ac6ebcbf8188ae (not yet tagged), unchanged; only .github/ is left out.
# darwin-listings is public again; whether to return to the git dependency is open.

The copy's git objects, the same as at that commit. The Build workflow
compares them with these on every run:

- src tree: 0dcd4152d34250435413865bf072360f72eb1ecf
- Cargo.toml blob: 034b2354619768a25517638666144bf8a93653d7

The crate is a member of this repository's workspace, so its own tests run
under this repository's Cargo.lock (`cargo test --locked -p ant-listings`, in
the Test workflow). Its own Cargo.lock is kept as it was released and is not
used here.

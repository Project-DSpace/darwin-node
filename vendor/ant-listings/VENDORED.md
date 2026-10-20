# Vendored from Project-DSpace/darwin-listings 0.3.0, tagged v0.3.0 at commit 88abb624b286e25cb6d7e6ceb194d255976c602b, unchanged; only .github/ is left out.
# darwin-listings is public again; whether to return to the git dependency is open.

The copy's git objects, the same as at that commit. The Build workflow
compares them with these on every run:

- src tree: f4bd32552aae9305b62398533df3c8ee58a74004
- Cargo.toml blob: 8e9d428e13f52977524c115c113d0e9bdc2d322c

The crate is a member of this repository's workspace, so its own tests run
under this repository's Cargo.lock (`cargo test --locked -p ant-listings`, in
the Test workflow). Its own Cargo.lock is kept as it was released and is not
used here.

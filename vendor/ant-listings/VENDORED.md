# Vendored from Project-DSpace/darwin-listings v0.2.1 (commit 381d7c180f7c241bdd76619797d7aba40126c762), unchanged; only .github/ is left out.
# darwin-listings is public again; whether to return to the git dependency is open.

The copy's git objects, the same as at that commit. The Build workflow
compares them with these on every run:

- src tree: 33716b32f244dd5554735278a14da5ef79ed4417
- Cargo.toml blob: bf6d0191c4f0aa04482adb1d8da68774cf883ee3

The crate is a member of this repository's workspace, so its own tests run
under this repository's Cargo.lock (`cargo test --locked -p ant-listings`, in
the Test workflow). Its own Cargo.lock is kept as it was released and is not
used here.

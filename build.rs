//! Build script for ant-node.

use std::path::{Path, PathBuf};
use std::process::Command;

/// `git rev-parse` with the given arguments, trimmed, or `None` when git is
/// missing or fails (a copy with no `.git`).
fn git(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Where git keeps `name` for this checkout: `.git/<name>` in a plain clone,
/// the worktree's or the common folder in a linked worktree.
fn git_path(name: &str) -> Option<PathBuf> {
    git(&["rev-parse", "--path-format=absolute", "--git-path", name]).map(PathBuf::from)
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // The commit, where no `.git` says it: CI's checkout and darwin-builder's
    // shim both give it.
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-env-changed=BUILDER_COMMIT");

    // Rerun when HEAD moves: HEAD itself (a branch switch or detached HEAD),
    // the current branch's ref (a new commit) and packed-refs (`git gc`).
    // Only paths that exist are named. Cargo treats a missing
    // rerun-if-changed path as changed, so naming one (packed-refs in a
    // shallow clone, every `.git/...` in a linked worktree, where `.git` is a
    // file, or in a copy with no `.git`) rebuilt this crate, and every test
    // target with it, on every cargo call.
    let mut watched: Vec<PathBuf> = Vec::new();
    watched.extend(git_path("HEAD"));
    if let Some(branch) = git(&["rev-parse", "--symbolic-full-name", "HEAD"]) {
        if branch.starts_with("refs/") {
            watched.extend(git_path(&branch));
        }
    }
    watched.extend(git_path("packed-refs"));
    for path in watched.iter().filter(|p| Path::new(p).exists()) {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let commit = git(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=ANT_GIT_COMMIT={commit}");
}

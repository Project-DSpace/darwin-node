//! The storage-node image's node manager, and what the image's scripts
//! delete.
//!
//! The image (docker/Dockerfile) runs darwin-nodes, darwin-client's build of
//! the node manager, from the darwin-client commit docker/manager-revision
//! pins; the Build workflow builds it from that commit with its own lockfile
//! (scripts/check-workflows.sh holds build.yml to that). darwin-client's own
//! tests pin what darwin-nodes leaves out: the disk-pressure eviction, reset,
//! downloads and log forwarding. These tests read the repository: the image
//! installs that manager and downloads nothing, its scripts call it and never
//! Autonomi's `ant`, the registry stays where the image before kept it, the
//! image marker reaches the nodes only from the manager's environment, and
//! every deletion the image's scripts make is reviewed. docker/smoke-test.sh
//! runs the image itself.
//!
//! Upstream has no such file, so a merge can't drop it while resolving a
//! conflict.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

/// The repository the image's node manager is built from.
const MANAGER_REPOSITORY: &str = "https://github.com/Project-DSpace/darwin-client.git";

/// The scripts the image runs: the entrypoint, and the `nodes` helper
/// operators run with `docker exec`.
const IMAGE_SCRIPTS: [&str; 2] = ["docker/entrypoint.sh", "docker/nodes"];

/// A file of the repository, with its line endings as git stores them, so a
/// checkout that writes CRLF reads the same.
fn repo_text(path: &str) -> String {
    let full = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    std::fs::read_to_string(&full)
        .unwrap_or_else(|e| panic!("read {path}: {e}"))
        .replace("\r\n", "\n")
}

/// The darwin-client commit docker/manager-revision pins: its repository on
/// the first line, the commit on the second, as darwin-client's
/// `browser-tests/node-revision` pins a darwin-node revision.
fn manager_revision() -> String {
    let text = repo_text("docker/manager-revision");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "docker/manager-revision holds the repository, then the commit: {text:?}"
    );
    assert_eq!(lines[0], MANAGER_REPOSITORY);
    let commit = lines[1];
    assert!(
        commit.len() == 40
            && commit
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "docker/manager-revision names {commit:?}, not a full commit"
    );
    commit.to_string()
}

/// The Dockerfile's instructions, one per entry, with continued lines joined
/// and comment lines left out.
fn dockerfile_instructions() -> Vec<String> {
    let mut instructions = Vec::new();
    let mut current = String::new();
    for line in repo_text("docker/Dockerfile").lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || (trimmed.is_empty() && current.is_empty()) {
            continue;
        }
        let (text, continued) = trimmed
            .strip_suffix('\\')
            .map_or((trimmed, false), |text| (text, true));
        current.push_str(text.trim_end());
        current.push(' ');
        if !continued {
            instructions.push(current.trim().to_string());
            current.clear();
        }
    }
    instructions
}

/// A shell script's commands as bash reads them: one entry per line, with
/// lines continued by a backslash, and quoted text spanning lines, joined;
/// comments left out; whitespace runs made one space.
fn shell_commands(source: &str) -> Vec<String> {
    let mut commands = Vec::new();
    let mut current = String::new();
    let mut chars = source.chars().peekable();
    let (mut single, mut double) = (false, false);
    let finish = |current: &mut String, commands: &mut Vec<String>| {
        let command = current.split_whitespace().collect::<Vec<_>>().join(" ");
        if !command.is_empty() {
            commands.push(command);
        }
        current.clear();
    };
    while let Some(c) = chars.next() {
        if single {
            current.push(c);
            single = c != '\'';
            continue;
        }
        if double {
            current.push(c);
            if c == '\\' {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            } else if c == '"' {
                double = false;
            }
            continue;
        }
        match c {
            '\\' => match chars.next() {
                Some('\n') => current.push(' '),
                Some(next) => {
                    current.push(c);
                    current.push(next);
                }
                None => current.push(c),
            },
            '\'' => {
                single = true;
                current.push(c);
            }
            '"' => {
                double = true;
                current.push(c);
            }
            '#' if current
                .chars()
                .next_back()
                .is_none_or(|last| last.is_whitespace() || ";&|()".contains(last)) =>
            {
                while chars.peek().is_some_and(|&next| next != '\n') {
                    chars.next();
                }
            }
            '\n' => finish(&mut current, &mut commands),
            _ => current.push(c),
        }
    }
    finish(&mut current, &mut commands);
    commands
}

/// The words of a command, split at whitespace and the shell's operators,
/// with quotes taken out. Quoted text is split too: a command in quotes
/// (`sh -c '...'`) is still a command.
fn words(command: &str) -> Vec<String> {
    command
        .split(|c: char| c.is_whitespace() || ";&|(){}<>`".contains(c))
        .map(|word| word.replace(['"', '\''], ""))
        .filter(|word| !word.is_empty())
        .collect()
}

#[test]
fn the_image_installs_darwin_nodes_from_the_pinned_commit_and_downloads_nothing() {
    let commit = manager_revision();
    let instructions = dockerfile_instructions();
    assert!(
        instructions
            .iter()
            .any(|i| i == "COPY dist/darwin-nodes/darwin-nodes /usr/local/bin/darwin-nodes"),
        "the Dockerfile doesn't install the build's darwin-nodes: {instructions:#?}"
    );
    for instruction in &instructions {
        // Autonomi's `ant`, which the image downloaded before, is gone, and
        // nothing is fetched while the image is built but Ubuntu's packages.
        for name in ["WithAutonomi", "ant-cli-v", "/usr/local/bin/ant "] {
            assert!(
                !instruction.contains(name),
                "the Dockerfile names {name}: {instruction}"
            );
        }
        if instruction.starts_with("RUN ") {
            assert!(
                !["://", "curl -", "wget "]
                    .iter()
                    .any(|fetch| instruction.contains(fetch)),
                "a RUN instruction fetches something: {instruction}"
            );
        }
    }
    // The licences and notices darwin-nodes is packaged with go beside it.
    assert!(
        instructions.iter().any(|i| i.starts_with("COPY ")
            && i.ends_with(" /usr/share/doc/storage-node/darwin-nodes/")
            && [
                "dist/darwin-nodes/LICENSE-MIT",
                "dist/darwin-nodes/LICENSE-APACHE",
                "dist/darwin-nodes/THIRD-PARTY-NOTICES.txt",
                "dist/darwin-nodes/RUST-STD-COPYRIGHT.html",
            ]
            .iter()
            .all(|file| i.contains(file))),
        "the Dockerfile doesn't install darwin-nodes' licences and notices: {instructions:#?}"
    );
    let notice = repo_text("docker/NOTICE");
    assert!(
        notice.contains("darwin-nodes (/usr/local/bin/darwin-nodes)")
            && notice.contains(&format!("at commit {commit}")),
        "docker/NOTICE must say what darwin-nodes is and name commit {commit}:\n{notice}"
    );
    assert!(
        !notice.contains("/usr/local/bin/ant)"),
        "docker/NOTICE still names Autonomi's ant:\n{notice}"
    );
}

#[test]
fn the_image_scripts_run_darwin_nodes_and_never_ant() {
    for path in IMAGE_SCRIPTS.iter().chain(&["docker/smoke-test.sh"]) {
        for command in shell_commands(&repo_text(path)) {
            let words = words(&command);
            assert!(
                !words
                    .windows(2)
                    .any(|pair| pair[0] == "ant" && pair[1] == "node"),
                "{path} runs Autonomi's `ant node`: {command}"
            );
        }
    }
    let entrypoint = shell_commands(&repo_text("docker/entrypoint.sh"));
    assert!(
        entrypoint
            .iter()
            .any(|c| c == r#"manager() { as_user darwin-nodes "$@"; }"#),
        "the entrypoint's manager() doesn't run darwin-nodes"
    );
    assert!(
        entrypoint
            .iter()
            .any(|c| c.contains("--clear-groups darwin-nodes daemon run ")),
        "the entrypoint doesn't run darwin-nodes' daemon"
    );
    let nodes = shell_commands(&repo_text("docker/nodes"));
    assert!(
        nodes
            .iter()
            .any(|c| c.starts_with("exec setpriv ") && c.ends_with(r#" darwin-nodes "$@""#)),
        "docker/nodes doesn't run darwin-nodes: {nodes:#?}"
    );
}

/// darwin-nodes keeps its registry in `$XDG_DATA_HOME/ant`, as `ant` 0.3.9
/// did (darwin-client's tests pin that), so with the image's `XDG_DATA_HOME`
/// the registry the image before wrote is read where it is.
#[test]
fn the_registry_is_read_where_the_image_before_kept_it() {
    let env: Vec<String> = dockerfile_instructions()
        .into_iter()
        .filter(|i| i.starts_with("ENV "))
        .collect();
    assert!(
        env.iter().any(|i| i
            .split_whitespace()
            .any(|a| a == "XDG_DATA_HOME=/data/manager")),
        "the image's XDG_DATA_HOME is not /data/manager: {env:?}"
    );
    let entrypoint = shell_commands(&repo_text("docker/entrypoint.sh"));
    for line in [
        "STATE=/data/manager/ant",
        "REGISTRY=$STATE/node_registry.json",
    ] {
        assert!(
            entrypoint.iter().any(|c| c == line),
            "the entrypoint no longer has {line}"
        );
    }
}

/// The image marker, the commit the image is built from, is written into the
/// image at build and reaches every node in `ANT_IMAGE_VERSION` from the
/// manager's environment. The registry never keeps it: an image before this
/// one, started on the same /data, would pass a kept marker on to its nodes.
#[test]
fn the_image_marker_reaches_the_nodes_only_from_the_managers_environment() {
    let instructions = dockerfile_instructions();
    assert!(
        instructions.iter().any(|i| i == "ARG IMAGE_VERSION"),
        "the Dockerfile takes no IMAGE_VERSION"
    );
    assert!(
        instructions.iter().any(|i| i.starts_with("RUN ")
            && i.contains("grep -qxE '[0-9a-f]{40}'")
            && i.ends_with(r#"echo "$IMAGE_VERSION" > /opt/ant/image-version"#)),
        "the Dockerfile doesn't write a checked IMAGE_VERSION to /opt/ant/image-version"
    );
    let entrypoint = shell_commands(&repo_text("docker/entrypoint.sh"));
    assert!(
        entrypoint
            .iter()
            .any(|c| c == "image_marker=$(cat /opt/ant/image-version)"),
        "the entrypoint doesn't read the image marker"
    );
    assert!(
        entrypoint.iter().any(|c| c.starts_with("export ")
            && words(c)
                .iter()
                .any(|w| w == "ANT_IMAGE_VERSION=$image_marker")),
        "the entrypoint doesn't export the image marker to the manager and its nodes"
    );
    // The registry's env_variables lose any ANT_IMAGE_VERSION at every start.
    assert!(
        entrypoint.iter().any(|c| c.contains(
            r#"IN("ANT_WEBRTC_DIRECT_PORT", "ANT_WEBRTC_DIRECT_ADVERTISED_ADDR", "ANT_EVM_RPC_URL", "ANT_IMAGE_VERSION") | not"#
        )),
        "the entrypoint doesn't keep the image marker out of the registry"
    );
    // And never puts it there: a node's registry settings come from node_env.
    let node_env = entrypoint
        .iter()
        .skip_while(|c| !c.starts_with("node_env() {"))
        .take_while(|c| *c != "}")
        .cloned()
        .collect::<Vec<_>>();
    assert!(!node_env.is_empty(), "the entrypoint has no node_env()");
    assert!(
        node_env.iter().all(|c| !c.contains("ANT_IMAGE_VERSION")),
        "node_env() puts the image marker in the registry: {node_env:#?}"
    );
}

/// Words that delete files or data, or take a node out of the registry.
/// Moves (`mv`), plain writes and redirections are not counted: they move a
/// folder aside or write a file whole, as the erasure census of the node's
/// own code (src/replication/retention/census.rs) leaves out renames and
/// plain writes.
const DELETING_WORDS: &[&str] = &[
    "rm", "rmdir", "unlink", "shred", "truncate", "-delete", "dismiss",
];

/// A reviewed command of the image's scripts that deletes something, and why
/// that is safe.
struct Deletion {
    /// The script, relative to the repository's root.
    file: &'static str,
    /// The command as [`shell_commands`] gives it.
    command: &'static str,
    /// How many times the script has it.
    count: usize,
    /// What it deletes, and why that is safe.
    reason: &'static str,
}

/// Every command of the image's scripts that deletes something. Deleting a
/// node's data is not among them: that is kept while the node is registered,
/// for 3 days once retired, and for good once set aside as unregistered.
const DELETIONS: &[Deletion] = &[
    Deletion {
        file: "docker/entrypoint.sh",
        command: r#"rm -f "$STATE/daemon.pid" "$STATE/daemon.port" "$NODES"/node-*/node.pid"#,
        count: 1,
        reason: "The manager's and the nodes' process files from the container's last run: \
                 process IDs start again in a new container, so a stale one could name an \
                 unrelated process. No node data.",
    },
    Deletion {
        file: "docker/entrypoint.sh",
        command: r#"find "$RETIRED" -mindepth 1 -maxdepth 1 ! -name unregistered -mtime +3 -exec rm -rf {} +"#,
        count: 1,
        reason: "The sweep of retired folders: the data and logs of nodes retired when the \
                 operator lowered NODE_COUNT, and the logs of nodes an earlier manager evicted, \
                 each dated when retired and deleted at a start more than 3 days later, so a \
                 lowered NODE_COUNT can be undone within 3 days. It never reaches \
                 retired/unregistered, where folders no node is registered for are kept for good.",
    },
    Deletion {
        file: "docker/entrypoint.sh",
        command: r#"manager dismiss "$id" >/dev/null"#,
        count: 2,
        reason: "Takes a node out of the registry and leaves its folders: a node an earlier \
                 manager evicted, whose data that manager deleted, dismissed once at this image's \
                 first start; and a node above a lowered NODE_COUNT, whose folders are then \
                 retired for 3 days.",
    },
    Deletion {
        file: "docker/entrypoint.sh",
        command: r#"rm -f "$tmp""#,
        count: 1,
        reason: "The temporary copy of the registry the settings were written to, once it is \
                 installed in the registry's place.",
    },
];

#[test]
fn every_deletion_the_image_scripts_make_is_reviewed() {
    let mut found: BTreeMap<(String, String), usize> = BTreeMap::new();
    for file in IMAGE_SCRIPTS {
        for command in shell_commands(&repo_text(file)) {
            if words(&command)
                .iter()
                .any(|word| DELETING_WORDS.contains(&word.as_str()))
            {
                *found.entry((file.to_string(), command)).or_default() += 1;
            }
        }
    }
    let reviewed: BTreeMap<(String, String), usize> = DELETIONS
        .iter()
        .map(|d| ((d.file.to_string(), d.command.to_string()), d.count))
        .collect();
    assert_eq!(
        reviewed.len(),
        DELETIONS.len(),
        "a deletion is listed twice"
    );
    for deletion in DELETIONS {
        assert!(
            !deletion.reason.is_empty(),
            "{} has no reason",
            deletion.command
        );
    }

    let mut differences = String::new();
    for ((file, command), count) in &found {
        match reviewed.get(&(file.clone(), command.clone())) {
            Some(listed) if listed == count => {}
            Some(listed) => write!(
                differences,
                "\n{file}: `{command}` is there {count} time(s), listed {listed}"
            )
            .unwrap(),
            None => write!(
                differences,
                "\n{file}: `{command}` deletes something and is not listed"
            )
            .unwrap(),
        }
    }
    for (file, command) in reviewed.keys() {
        if !found.contains_key(&(file.clone(), command.clone())) {
            write!(
                differences,
                "\n{file}: `{command}` is listed but no longer there"
            )
            .unwrap();
        }
    }
    assert!(
        differences.is_empty(),
        "the image's scripts delete outside the reviewed list (DELETIONS); review each, \
         and list it with its reason:{differences}"
    );
}

#[test]
fn shell_commands_are_read_as_bash_reads_them() {
    let source = "a=1 # a comment\n\
                  # a whole line\n\
                  echo \"# not a comment\" '# nor this'\n\
                  n=${#list[@]}\n\
                  find x \\\n    -exec rm {} +\n\
                  jq '\n  .a\n  | .b' file\n";
    assert_eq!(
        shell_commands(source),
        [
            "a=1",
            "echo \"# not a comment\" '# nor this'",
            "n=${#list[@]}",
            "find x -exec rm {} +",
            "jq ' .a | .b' file",
        ]
    );
    assert_eq!(
        words(r#"sh -c 'rm -rf "$dir"'; x=$(unlink y)"#),
        ["sh", "-c", "rm", "-rf", "$dir", "x=$", "unlink", "y"]
    );
}

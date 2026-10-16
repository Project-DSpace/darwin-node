//! Census of the calls in this crate that can remove stored data.
//!
//! A node keeps every record and pointer it holds while its disk has room, and
//! the prune passes erase one only under the rule in the parent module. This
//! test reads the crate's own `src/` tree and fails when a call that can remove
//! stored data appears anywhere not listed in [`ERASE_SITES`], or when a listed
//! site holds more or fewer such calls than listed. A change that adds an erase
//! path, a merge from upstream among them, then fails CI until someone has
//! looked at it and listed it with its reason.
//!
//! The source is read as tokens, so layout, comments and string literals make
//! no difference. Test-only code is left out: items under a `cfg` that is false
//! in a production build (`test`, `feature = "test-utils"`), `#[test]`
//! functions, and module files declared only that way.
//!
//! Counted, in production code:
//!
//! - every mention of a primitive that removes stored data ([`PRIMITIVES`]:
//!   file and directory removal, the system calls behind them, store and
//!   table deletes), except its own definition, so that an import under
//!   another name is counted too;
//! - every call of a function of this crate that removes stored records
//!   under another name ([`ERASERS`]);
//! - `clear` called with an argument: a table cleared inside a transaction
//!   (clearing an in-memory collection takes none);
//! - `truncate(true)`: a file opened so that its old contents are dropped.
//!
//! Not counted: renames, plain writes and `File::create`, which publish a file
//! written under a temporary name or move one aside, and a temporary directory
//! dropped at the end of its scope. Nor can a count tell which receiver a
//! listed call is on, so a listed call turned into another of the same name
//! at the same site passes; the review of the change has to catch that.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// Identifiers that remove stored data, counted wherever they are mentioned.
const PRIMITIVES: &[&str] = &[
    "delete",
    "delete_range",
    "del_current",
    "ftruncate",
    "remove_batch",
    "remove_dir",
    "remove_dir_all",
    "remove_file",
    "rmdir",
    "set_len",
    "unlink",
    "unlinkat",
];

/// Functions of this crate that remove stored records under another name,
/// counted where they are called.
const ERASERS: &[&str] = &[
    "delete_blocking",
    "delete_held",
    "delete_in_txn",
    "delete_retired_directory",
    "delete_unobserved",
    "delete_with_slack",
    "erase_stored_record",
    "quarantine_corrupt",
    "remove_legacy_dir",
    "remove_marked_directory",
    "retire_legacy",
    "try_delete",
];

/// Calls that build a node's disk-pressure state. The prune passes and the
/// storage migration must share one, so that the pressure lasts until 15% is
/// free for all of them: one built afresh would end it at 10%.
const DISK_PRESSURE_STATE_BUILDERS: &[(Option<&str>, &str)] =
    &[(None, "for_store"), (Some("DiskRetention"), "new")];

/// A reviewed site where stored data may be removed, and why that is safe.
struct EraseSite {
    /// File, relative to the crate root.
    file: &'static str,
    /// `Type::function`, `function`, or `<module>` outside any function.
    site: &'static str,
    /// The primitive or eraser called there.
    call: &'static str,
    /// How many times it is called there.
    count: usize,
    /// Why it is there and what governs it.
    reason: &'static str,
}

/// Shorthand for one [`ERASE_SITES`] entry.
const fn site(
    file: &'static str,
    site: &'static str,
    call: &'static str,
    count: usize,
    reason: &'static str,
) -> EraseSite {
    EraseSite {
        file,
        site,
        call,
        count,
        reason,
    }
}

const TEMP_FILE: &str = "Removes the temporary file of a write that did not complete; \
     the stored record, if any, is untouched.";
const LEGACY_DELETE: &str = "The legacy store's record removal, reached only through \
     ChunkStore::delete_unobserved and ChunkStore::copy_batch.";
const LEGACY_RETIREMENT: &str = "Part of removing a retired legacy store directory, which \
     only ChunkStore::retire_legacy and the restart sweep of a directory carrying the \
     retirement mark do.";
const UPGRADE_CACHE: &str = "Upgrade cache files; no record data.";
const LISTING_METADATA: &str = "Listing metadata, not record data.";

/// Every reviewed site. See the module documentation for what is counted.
const ERASE_SITES: &[EraseSite] = &[
    site(
        "src/devnet.rs",
        "Devnet::shutdown",
        "remove_dir_all",
        1,
        "The local devnet tool removes its own temporary data directory at shutdown; \
         never a running node's store.",
    ),
    site(
        "src/listings/store.rs",
        "ListingStore::clear_pending",
        "delete",
        1,
        LISTING_METADATA,
    ),
    site(
        "src/listings/store.rs",
        "ListingStore::drop_key_reference",
        "delete",
        1,
        LISTING_METADATA,
    ),
    site(
        "src/listings/store.rs",
        "ListingStore::migrate_v1",
        "clear",
        1,
        "Empties the version 1 listing table once its records have moved to the \
         current tables; listing metadata.",
    ),
    site(
        "src/listings/store.rs",
        "ListingStore::migrate_v1",
        "delete",
        1,
        "Moves version 1 listing records into the current tables; listing metadata.",
    ),
    site(
        "src/listings/store.rs",
        "ListingStore::remove_in",
        "delete",
        5,
        "Drops the listing of an envelope chunk the node no longer holds; it follows \
         the chunk's removal and never removes chunk bytes.",
    ),
    site(
        "src/listings/store.rs",
        "ListingStore::settle",
        "delete",
        1,
        LISTING_METADATA,
    ),
    site(
        "src/pointer/store.rs",
        "Inner::commit_blocking",
        "remove_file",
        2,
        TEMP_FILE,
    ),
    site(
        "src/pointer/store.rs",
        "Inner::delete_blocking",
        "remove_file",
        1,
        "The pointer store's record removal, reached only through PointerStore::delete.",
    ),
    site(
        "src/pointer/store.rs",
        "PointerStore::delete",
        "delete_blocking",
        1,
        "Removes a pointer record whatever state it holds; no production code calls it \
         (the pruner uses PointerStore::delete_held).",
    ),
    site(
        "src/pointer/store.rs",
        "PointerStore::delete_held",
        "delete_blocking",
        1,
        "Removes a pointer record only while it still holds the state its close group \
         proved; its only production caller is PointerReplication::delete.",
    ),
    site(
        "src/pointer/store.rs",
        "adopt_flat_record",
        "remove_file",
        1,
        "Sweeps a temporary file of an interrupted pointer write found at startup.",
    ),
    site(
        "src/pointer/store.rs",
        "scan_shard",
        "remove_file",
        1,
        "Sweeps a temporary file of an interrupted pointer write found at startup.",
    ),
    site("src/pointer/store.rs", "stage", "remove_file", 1, TEMP_FILE),
    site(
        "src/replication/paid_list.rs",
        "PaidList::remove",
        "delete",
        1,
        "Removes a paid-list entry: payment bookkeeping, not record data.",
    ),
    site(
        "src/replication/paid_list.rs",
        "PaidList::remove_batch",
        "delete",
        1,
        "Removes paid-list entries: payment bookkeeping, not record data.",
    ),
    site(
        "src/replication/pointer.rs",
        "PointerReplication::delete",
        "delete_held",
        1,
        "Erases a pointer record PointerReplication::erase_if_proven has cleared, only \
         while the node still holds the state its close group proved.",
    ),
    site(
        "src/replication/pointer.rs",
        "PointerReplication::erase_if_proven",
        "delete",
        1,
        "Erases a pointer only under disk pressure, beyond the erase floor, farthest \
         first, after all but one of its current close group prove they hold it and the \
         re-check just before deleting passes.",
    ),
    site(
        "src/replication/pruning.rs",
        "delete_paid_entries",
        "remove_batch",
        1,
        "Removes paid-list entries three quarters of the key's current paid close group \
         confirm they track: payment bookkeeping, not record data.",
    ),
    site(
        "src/replication/pruning.rs",
        "erase_if_still_eligible",
        "erase_stored_record",
        1,
        "Erases a chunk only under disk pressure, beyond the erase floor, farthest first, \
         after all but one of its current close group prove possession and the re-check \
         just before deleting passes.",
    ),
    site(
        "src/replication/pruning.rs",
        "erase_stored_record",
        "delete",
        1,
        "The prune pass's only chunk removal, reached only from erase_if_still_eligible.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "ChunkStore::copy_batch",
        "delete",
        2,
        "Drops legacy records the file store cannot hold or serve: larger than a chunk \
         may be, or bytes that do not hash to their name.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "ChunkStore::delete",
        "delete_unobserved",
        1,
        "Removes a chunk from both backings; the only production caller of \
         ChunkStore::delete is the prune pass's erase_stored_record.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "ChunkStore::delete_unobserved",
        "delete",
        2,
        "The chunk store's removal from the legacy store and the file store, reached \
         only through ChunkStore::delete.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "ChunkStore::remove_legacy_dir",
        "delete_retired_directory",
        1,
        LEGACY_RETIREMENT,
    ),
    site(
        "src/storage/chunk_store.rs",
        "ChunkStore::retire_legacy",
        "remove_legacy_dir",
        1,
        LEGACY_RETIREMENT,
    ),
    site(
        "src/storage/chunk_store.rs",
        "delete_retired_directory",
        "remove_marked_directory",
        1,
        LEGACY_RETIREMENT,
    ),
    site(
        "src/storage/chunk_store.rs",
        "mark_directory_retired",
        "remove_file",
        1,
        "Removes a partly written retirement mark; no record data.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "remove_marked_directory",
        "remove_dir",
        1,
        LEGACY_RETIREMENT,
    ),
    site(
        "src/storage/chunk_store.rs",
        "remove_marked_directory",
        "remove_dir_all",
        1,
        LEGACY_RETIREMENT,
    ),
    site(
        "src/storage/chunk_store.rs",
        "remove_marked_directory",
        "remove_file",
        2,
        LEGACY_RETIREMENT,
    ),
    site(
        "src/storage/chunk_store.rs",
        "restore_unmarked_environment",
        "remove_dir",
        1,
        "Removes an empty tombstone directory; no record data.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "sweep_retired_legacy",
        "delete_retired_directory",
        1,
        LEGACY_RETIREMENT,
    ),
    site(
        "src/storage/file_store.rs",
        "FileStore::delete",
        "remove_file",
        1,
        "The file store's chunk removal, reached only through ChunkStore::delete.",
    ),
    site(
        "src/storage/file_store.rs",
        "FileStore::get",
        "quarantine_corrupt",
        1,
        "Removes a chunk file whose bytes, read again, do not hash to its name: never a \
         valid record.",
    ),
    site(
        "src/storage/file_store.rs",
        "FileStore::quarantine_corrupt",
        "remove_file",
        1,
        "Removes a chunk file proven not to hold the record its name says.",
    ),
    site(
        "src/storage/file_store.rs",
        "publish_in_place",
        "remove_file",
        2,
        "Removes a chunk file this same write created and could not complete.",
    ),
    site(
        "src/storage/file_store.rs",
        "publish_via_rename",
        "remove_file",
        1,
        TEMP_FILE,
    ),
    site(
        "src/storage/file_store.rs",
        "sweep_marker_temps",
        "remove_file",
        1,
        "Sweeps temporary marker files of an interrupted write at startup.",
    ),
    site(
        "src/storage/file_store.rs",
        "sweep_temp",
        "remove_file",
        1,
        "Sweeps temporary chunk files of an interrupted write at startup.",
    ),
    site(
        "src/storage/file_store.rs",
        "write_and_replace",
        "remove_file",
        1,
        TEMP_FILE,
    ),
    site(
        "src/storage/file_store.rs",
        "write_and_replace",
        "truncate",
        1,
        "Overwrites, with verified bytes, a chunk file proven not to hash to its name.",
    ),
    site(
        "src/storage/file_store.rs",
        "write_file_atomic",
        "remove_file",
        1,
        TEMP_FILE,
    ),
    site(
        "src/storage/file_store.rs",
        "write_temp",
        "remove_file",
        2,
        TEMP_FILE,
    ),
    site(
        "src/storage/lmdb.rs",
        "LmdbStorage::delete",
        "delete_with_slack",
        1,
        LEGACY_DELETE,
    ),
    site(
        "src/storage/lmdb.rs",
        "LmdbStorage::delete",
        "try_delete",
        1,
        LEGACY_DELETE,
    ),
    site(
        "src/storage/lmdb.rs",
        "LmdbStorage::delete_with_slack",
        "delete_in_txn",
        1,
        LEGACY_DELETE,
    ),
    site(
        "src/storage/lmdb.rs",
        "LmdbStorage::try_delete",
        "delete_in_txn",
        1,
        LEGACY_DELETE,
    ),
    site(
        "src/storage/lmdb.rs",
        "delete_in_txn",
        "delete",
        1,
        LEGACY_DELETE,
    ),
    site(
        "src/storage/migration.rs",
        "retire_tick",
        "retire_legacy",
        1,
        "Retires the legacy store once every record copied to files is verified there and \
         every record given up (shed) is proven held by all but one of its current close \
         group. Shedding needs the copier to have run out of room; it keeps its own gates \
         rather than the prune passes' (ADR-0014, ADR-0005 amendment of 2026-10-16).",
    ),
    site(
        "src/upgrade/binary_cache.rs",
        "BinaryCache::atomic_copy",
        "remove_file",
        1,
        UPGRADE_CACHE,
    ),
    site(
        "src/upgrade/binary_cache.rs",
        "BinaryCache::get_verified_archive",
        "remove_file",
        2,
        UPGRADE_CACHE,
    ),
    site(
        "src/upgrade/binary_cache.rs",
        "BinaryCache::store_archive",
        "remove_file",
        1,
        UPGRADE_CACHE,
    ),
    site(
        "src/upgrade/release_cache.rs",
        "ReleaseCache::write_inner",
        "remove_file",
        1,
        "Release cache file; no record data.",
    ),
    site(
        "src/web_rtc.rs",
        "spawn",
        "remove_file",
        1,
        "Removes the browser address file a previous run advertised; no record data.",
    ),
];

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

/// One token of Rust source. Comments and whitespace are dropped; literals
/// keep their text so that `cfg` predicates can be read.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Literal(String),
    Lifetime,
    Punct(char),
    /// `::`, `->` or `=>`.
    Joint(&'static str),
}

#[derive(Debug, Clone)]
struct Token {
    tok: Tok,
    line: usize,
}

fn is_ident_start(c: char) -> bool {
    c == '_' || c.is_alphabetic()
}

fn is_ident_continue(c: char) -> bool {
    c == '_' || c.is_alphanumeric()
}

/// Split Rust source into tokens.
#[allow(clippy::too_many_lines)]
fn tokenize(source: &str) -> Vec<Token> {
    let chars: Vec<char> = source.chars().collect();
    let at = |i: usize| chars.get(i).copied();
    let mut tokens = Vec::new();
    let mut line = 1usize;
    let mut i = 0usize;
    while let Some(c) = at(i) {
        let start_line = line;
        let mut push = |tok: Tok| {
            tokens.push(Token {
                tok,
                line: start_line,
            });
        };
        match c {
            '\n' => {
                line += 1;
                i += 1;
            }
            c if c.is_whitespace() => i += 1,
            '/' if at(i + 1) == Some('/') => {
                while at(i).is_some_and(|c| c != '\n') {
                    i += 1;
                }
            }
            '/' if at(i + 1) == Some('*') => {
                let mut depth = 0usize;
                while let Some(c) = at(i) {
                    if c == '/' && at(i + 1) == Some('*') {
                        depth += 1;
                        i += 2;
                    } else if c == '*' && at(i + 1) == Some('/') {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        if c == '\n' {
                            line += 1;
                        }
                        i += 1;
                    }
                }
            }
            '"' => {
                let (text, end) = read_string(&chars, i + 1, &mut line);
                push(Tok::Literal(text));
                i = end;
            }
            '\'' => {
                if at(i + 1) == Some('\\') {
                    // An escaped character: skip the escape, then find the quote.
                    let mut j = i + 3;
                    while at(j).is_some_and(|c| c != '\'') {
                        j += 1;
                    }
                    push(Tok::Literal(String::new()));
                    i = j + 1;
                } else if at(i + 2) == Some('\'') {
                    push(Tok::Literal(String::new()));
                    i += 3;
                } else {
                    i += 1;
                    while at(i).is_some_and(is_ident_continue) {
                        i += 1;
                    }
                    push(Tok::Lifetime);
                }
            }
            c if is_ident_start(c) => {
                if let Some((text, end)) = read_prefixed_literal(&chars, i, &mut line) {
                    push(Tok::Literal(text));
                    i = end;
                    continue;
                }
                // `r#name` is the identifier `name`.
                if c == 'r' && at(i + 1) == Some('#') && at(i + 2).is_some_and(is_ident_start) {
                    i += 2;
                }
                let begin = i;
                while at(i).is_some_and(is_ident_continue) {
                    i += 1;
                }
                push(Tok::Ident(chars[begin..i].iter().collect()));
            }
            c if c.is_ascii_digit() => {
                while let Some(c) = at(i) {
                    let fraction = c == '.' && at(i + 1).is_some_and(|d| d.is_ascii_digit());
                    if is_ident_continue(c) || fraction {
                        i += 1;
                    } else {
                        break;
                    }
                }
                push(Tok::Literal(String::new()));
            }
            ':' if at(i + 1) == Some(':') => {
                push(Tok::Joint("::"));
                i += 2;
            }
            '-' if at(i + 1) == Some('>') => {
                push(Tok::Joint("->"));
                i += 2;
            }
            '=' if at(i + 1) == Some('>') => {
                push(Tok::Joint("=>"));
                i += 2;
            }
            c => {
                push(Tok::Punct(c));
                i += 1;
            }
        }
    }
    tokens
}

/// Read a quoted string whose body starts at `start`, honouring escapes.
/// Returns its text and the index just past the closing quote.
fn read_string(chars: &[char], start: usize, line: &mut usize) -> (String, usize) {
    let mut text = String::new();
    let mut i = start;
    while let Some(&c) = chars.get(i) {
        match c {
            '"' => return (text, i + 1),
            '\\' => {
                if let Some(&escaped) = chars.get(i + 1) {
                    if escaped == '\n' {
                        *line += 1;
                    }
                    text.push(escaped);
                }
                i += 2;
            }
            c => {
                if c == '\n' {
                    *line += 1;
                }
                text.push(c);
                i += 1;
            }
        }
    }
    (text, i)
}

/// A string, byte string, C string or byte literal starting with a prefix
/// at `i` (`b"…"`, `r#"…"#`, `br"…"`, `cr"…"`, `b'…'`), if there is one.
fn read_prefixed_literal(chars: &[char], i: usize, line: &mut usize) -> Option<(String, usize)> {
    let at = |j: usize| chars.get(j).copied();
    let mut j = i;
    if matches!(at(j), Some('b' | 'c')) {
        j += 1;
    }
    if at(j) == Some('r') {
        let mut hashes = 0usize;
        let mut k = j + 1;
        while at(k) == Some('#') {
            hashes += 1;
            k += 1;
        }
        if at(k) != Some('"') {
            return None;
        }
        let body = k + 1;
        let mut end = body;
        while end < chars.len() {
            if chars[end] == '"' && (1..=hashes).all(|h| at(end + h) == Some('#')) {
                let text: String = chars[body..end].iter().collect();
                *line += text.matches('\n').count();
                return Some((text, end + 1 + hashes));
            }
            end += 1;
        }
        let text: String = chars[body..].iter().collect();
        *line += text.matches('\n').count();
        return Some((text, chars.len()));
    }
    if j == i {
        return None;
    }
    match at(j) {
        Some('"') => Some(read_string(chars, j + 1, line)),
        Some('\'') if at(i) == Some('b') => {
            let mut k = j + 1;
            if at(k) == Some('\\') {
                k += 2;
            }
            while at(k).is_some_and(|c| c != '\'') {
                k += 1;
            }
            Some((String::new(), k + 1))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Attributes
// ---------------------------------------------------------------------------

/// A `cfg` predicate's value in a production build: `test` and the
/// `test-utils` feature are off, anything else may be either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cfg {
    On,
    Off,
    Either,
}

/// Evaluate the `cfg` predicate starting at `*pos`, leaving `*pos` past it.
fn eval_cfg(tokens: &[Token], pos: &mut usize) -> Cfg {
    let Some(Tok::Ident(name)) = tokens.get(*pos).map(|t| &t.tok) else {
        // Not a predicate: skip to the end of this argument.
        while let Some(token) = tokens.get(*pos) {
            if matches!(token.tok, Tok::Punct(',' | ')')) {
                break;
            }
            *pos += 1;
        }
        return Cfg::Either;
    };
    *pos += 1;
    match tokens.get(*pos).map(|t| &t.tok) {
        Some(Tok::Punct('(')) if matches!(name.as_str(), "all" | "any" | "not") => {
            *pos += 1;
            let mut values = Vec::new();
            while let Some(token) = tokens.get(*pos) {
                match token.tok {
                    Tok::Punct(')') => {
                        *pos += 1;
                        break;
                    }
                    Tok::Punct(',') => *pos += 1,
                    _ => {
                        let before = *pos;
                        values.push(eval_cfg(tokens, pos));
                        if *pos == before {
                            *pos += 1;
                        }
                    }
                }
            }
            match name.as_str() {
                "not" => match values.first() {
                    Some(Cfg::On) => Cfg::Off,
                    Some(Cfg::Off) => Cfg::On,
                    _ => Cfg::Either,
                },
                "all" if values.contains(&Cfg::Off) => Cfg::Off,
                "all" if values.iter().all(|v| *v == Cfg::On) => Cfg::On,
                "any" if values.contains(&Cfg::On) => Cfg::On,
                "any" if values.iter().all(|v| *v == Cfg::Off) => Cfg::Off,
                _ => Cfg::Either,
            }
        }
        Some(Tok::Punct('=')) => {
            *pos += 1;
            let value = match tokens.get(*pos).map(|t| &t.tok) {
                Some(Tok::Literal(value)) => value.clone(),
                _ => String::new(),
            };
            *pos += 1;
            if name == "feature" && value == "test-utils" {
                Cfg::Off
            } else {
                Cfg::Either
            }
        }
        _ if name == "test" => Cfg::Off,
        _ => Cfg::Either,
    }
}

/// Whether an attribute (its tokens between the brackets) makes what it is
/// on test-only: a `cfg` that is off in a production build, or a test
/// attribute such as `test` or `tokio::test`.
fn is_test_only_attribute(attr: &[Token]) -> bool {
    let mut path = Vec::new();
    let mut pos = 0;
    while let Some(Tok::Ident(segment)) = attr.get(pos).map(|t| &t.tok) {
        path.push(segment.as_str());
        pos += 1;
        if matches!(attr.get(pos).map(|t| &t.tok), Some(Tok::Joint("::"))) {
            pos += 1;
        } else {
            break;
        }
    }
    match path.as_slice() {
        ["cfg"] if matches!(attr.get(pos).map(|t| &t.tok), Some(Tok::Punct('('))) => {
            pos += 1;
            eval_cfg(attr, &mut pos) == Cfg::Off
        }
        [.., "test"] => true,
        _ => false,
    }
}

/// Index of the bracket closing the one at `open`, or the end of the tokens.
fn matching_close(tokens: &[Token], open: usize) -> usize {
    let mut depth = 0usize;
    for (offset, token) in tokens.iter().enumerate().skip(open) {
        match token.tok {
            Tok::Punct('(' | '[' | '{') => depth += 1,
            Tok::Punct(')' | ']' | '}') => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return offset;
                }
            }
            _ => {}
        }
    }
    tokens.len()
}

// ---------------------------------------------------------------------------
// Walking a file
// ---------------------------------------------------------------------------

/// What a bracket opened.
#[derive(Debug, Clone)]
enum Frame {
    /// A function body.
    Fn(String),
    /// An `impl` or `trait` block, by the type or trait it names.
    Impl(String),
    /// An inline module.
    Mod(String),
    /// Anything else.
    Other,
}

/// A `mod name;` declaration.
#[derive(Debug, Clone)]
struct ModDecl {
    /// Inline modules it sits in, outermost first.
    within: Vec<String>,
    name: String,
    test_only: bool,
}

/// What one file holds.
#[derive(Debug, Default)]
struct FileScan {
    /// Lines of each counted call, by `(site, call)`.
    calls: BTreeMap<(String, String), Vec<usize>>,
    decls: Vec<ModDecl>,
    /// The whole file is test-only (`#![cfg(test)]` at its top).
    test_only: bool,
}

/// Keywords that start an item or a `let` statement; within one, a comma at
/// the top level does not end it (generics, where clauses).
const ITEM_KEYWORDS: &[&str] = &[
    "const",
    "enum",
    "extern",
    "fn",
    "impl",
    "let",
    "macro_rules",
    "mod",
    "static",
    "struct",
    "trait",
    "type",
    "union",
    "use",
];

/// What a walk counts.
#[derive(Debug, Clone, Copy)]
enum Counting {
    /// Calls that can remove stored data (see the module documentation).
    Erasures,
    /// Calls of these functions, by name and, where given, the path segment
    /// before it (`Type::name`).
    Calls(&'static [(Option<&'static str>, &'static str)]),
}

struct Walker<'a> {
    tokens: &'a [Token],
    counting: Counting,
    stack: Vec<Frame>,
    /// A label for the next `{` opened at this stack depth.
    pending: Option<(Frame, usize)>,
    scan: FileScan,
}

impl<'a> Walker<'a> {
    fn new(tokens: &'a [Token], counting: Counting) -> Self {
        Self {
            tokens,
            counting,
            stack: Vec::new(),
            pending: None,
            scan: FileScan::default(),
        }
    }

    fn tok(&self, pos: usize) -> Option<&Tok> {
        self.tokens.get(pos).map(|t| &t.tok)
    }

    fn ident(&self, pos: usize) -> Option<&str> {
        match self.tok(pos) {
            Some(Tok::Ident(name)) => Some(name.as_str()),
            _ => None,
        }
    }

    fn inline_modules(&self) -> Vec<String> {
        self.stack
            .iter()
            .filter_map(|frame| match frame {
                Frame::Mod(name) => Some(name.clone()),
                _ => None,
            })
            .collect()
    }

    /// The site a call at the current depth belongs to.
    fn site(&self) -> String {
        let Some(fn_at) = self
            .stack
            .iter()
            .rposition(|frame| matches!(frame, Frame::Fn(_)))
        else {
            return "<module>".to_string();
        };
        let Some(Frame::Fn(name)) = self.stack.get(fn_at) else {
            return "<module>".to_string();
        };
        let owner = self.stack[..fn_at]
            .iter()
            .rev()
            .find_map(|frame| match frame {
                Frame::Impl(owner) => Some(owner.clone()),
                _ => None,
            });
        owner.map_or_else(|| name.clone(), |owner| format!("{owner}::{name}"))
    }

    fn count(&mut self, call: &str, line: usize) {
        let site = self.site();
        self.scan
            .calls
            .entry((site, call.to_string()))
            .or_default()
            .push(line);
    }

    fn walk(mut self) -> FileScan {
        let mut pos = 0;
        while pos < self.tokens.len() {
            pos = self.step(pos);
        }
        self.scan
    }

    /// Handle the token at `pos`; returns where to continue.
    fn step(&mut self, pos: usize) -> usize {
        let Some(token) = self.tokens.get(pos) else {
            return pos + 1;
        };
        let line = token.line;
        match &token.tok {
            Tok::Punct('#') => return self.attribute(pos),
            Tok::Punct('{') => {
                let depth = self.stack.len();
                let frame = if self.pending.as_ref().is_some_and(|(_, at)| *at == depth) {
                    self.pending.take().map_or(Frame::Other, |(frame, _)| frame)
                } else {
                    Frame::Other
                };
                self.stack.push(frame);
            }
            Tok::Punct('(' | '[') => self.stack.push(Frame::Other),
            Tok::Punct('}' | ')' | ']') => {
                self.stack.pop();
                if self
                    .pending
                    .as_ref()
                    .is_some_and(|(_, at)| *at > self.stack.len())
                {
                    self.pending = None;
                }
            }
            Tok::Punct(';') => {
                if let Some((frame, at)) = self.pending.take() {
                    if at == self.stack.len() {
                        if let Frame::Mod(name) = frame {
                            self.scan.decls.push(ModDecl {
                                within: self.inline_modules(),
                                name,
                                test_only: false,
                            });
                        }
                    } else {
                        self.pending = Some((frame, at));
                    }
                }
            }
            Tok::Ident(name) => {
                let name = name.clone();
                return self.ident_at(pos, &name, line);
            }
            _ => {}
        }
        pos + 1
    }

    fn ident_at(&mut self, pos: usize, name: &str, line: usize) -> usize {
        let depth = self.stack.len();
        let previous = pos.checked_sub(1).and_then(|p| self.tok(p)).cloned();
        let next = self.tok(pos + 1).cloned();
        match name {
            "fn" => {
                if let Some(fn_name) = self.ident(pos + 1) {
                    self.pending = Some((Frame::Fn(fn_name.to_string()), depth));
                    return pos + 2;
                }
            }
            "trait" => {
                if let Some(trait_name) = self.ident(pos + 1) {
                    self.pending = Some((Frame::Impl(trait_name.to_string()), depth));
                    return pos + 2;
                }
            }
            "mod" => {
                if let Some(mod_name) = self.ident(pos + 1) {
                    self.pending = Some((Frame::Mod(mod_name.to_string()), depth));
                    return pos + 2;
                }
            }
            "impl" => {
                let item_position = match &previous {
                    None | Some(Tok::Punct(';' | '}' | '{' | ']')) => true,
                    Some(Tok::Ident(word)) => word == "unsafe" || word == "default",
                    _ => false,
                };
                if item_position {
                    let owner = self.impl_owner(pos + 1);
                    self.pending = Some((Frame::Impl(owner), depth));
                }
                return pos + 1;
            }
            _ => {}
        }
        if matches!(previous, Some(Tok::Ident(ref word)) if word == "fn") {
            return pos + 1;
        }
        let called = matches!(next, Some(Tok::Punct('(')))
            || (matches!(next, Some(Tok::Joint("::")))
                && matches!(self.tok(pos + 2), Some(Tok::Punct('<'))));
        match self.counting {
            Counting::Erasures => self.count_erasure(pos, name, called, line),
            Counting::Calls(functions) if called => {
                let owner = if matches!(previous, Some(Tok::Joint("::"))) {
                    pos.checked_sub(2)
                        .and_then(|at| self.ident(at))
                        .map(str::to_string)
                } else {
                    None
                };
                let label = functions
                    .iter()
                    .find(|(wanted, function)| {
                        *function == name
                            && wanted.is_none_or(|wanted| owner.as_deref() == Some(wanted))
                    })
                    .map(|(wanted, function)| {
                        wanted
                            .map_or_else(|| (*function).to_string(), |w| format!("{w}::{function}"))
                    });
                if let Some(label) = label {
                    self.count(&label, line);
                }
            }
            Counting::Calls(_) => {}
        }
        pos + 1
    }

    /// Count `name` at `pos` if it is a call that can remove stored data.
    fn count_erasure(&mut self, pos: usize, name: &str, called: bool, line: usize) {
        if PRIMITIVES.contains(&name) || (ERASERS.contains(&name) && called) {
            self.count(name, line);
        } else if name == "clear" && called && !matches!(self.tok(pos + 2), Some(Tok::Punct(')'))) {
            self.count("clear", line);
        } else if name == "truncate"
            && called
            && matches!(self.tok(pos + 2), Some(Tok::Ident(word)) if word == "true")
            && matches!(self.tok(pos + 3), Some(Tok::Punct(')')))
        {
            self.count("truncate", line);
        }
    }

    /// The type an `impl` header starting at `pos` names: after `for` if there
    /// is one, else the type itself, by the last path segment before its
    /// generic arguments.
    fn impl_owner(&self, mut pos: usize) -> String {
        let mut angle = 0usize;
        let mut segment: Option<String> = None;
        let mut owner: Option<String> = None;
        let mut closed_generics = false;
        while let Some(tok) = self.tok(pos) {
            match tok {
                Tok::Punct('{' | ';') => break,
                Tok::Ident(word) if angle == 0 && word == "where" => break,
                Tok::Ident(word) if angle == 0 && word == "for" => {
                    segment = None;
                    owner = None;
                    closed_generics = false;
                }
                Tok::Punct('<') => {
                    if angle == 0 && owner.is_none() {
                        owner = segment.clone();
                    }
                    angle += 1;
                }
                Tok::Punct('>') => {
                    angle = angle.saturating_sub(1);
                    if angle == 0 {
                        closed_generics = true;
                    }
                }
                Tok::Ident(word)
                    if angle == 0
                        && !matches!(word.as_str(), "dyn" | "mut" | "unsafe" | "const")
                        && (!closed_generics || owner.is_none()) =>
                {
                    segment = Some(word.clone());
                }
                _ => {}
            }
            pos += 1;
        }
        owner.or(segment).unwrap_or_else(|| "<impl>".to_string())
    }

    /// Handle an attribute starting with `#` at `pos`; skips what a test-only
    /// one is on. Returns where to continue.
    fn attribute(&mut self, pos: usize) -> usize {
        let inner = matches!(self.tok(pos + 1), Some(Tok::Punct('!')));
        let open = if inner { pos + 2 } else { pos + 1 };
        if !matches!(self.tok(open), Some(Tok::Punct('['))) {
            return pos + 1;
        }
        let close = matching_close(self.tokens, open);
        let attr = self.tokens.get(open + 1..close).unwrap_or(&[]);
        let test_only = is_test_only_attribute(attr);
        let after = close + 1;
        if !test_only {
            return after;
        }
        if inner {
            if self.stack.is_empty() {
                self.scan.test_only = true;
            }
            return self.end_of_frame(after);
        }
        self.skip_item(after)
    }

    /// The position of the bracket closing the current frame, from `pos`.
    fn end_of_frame(&self, mut pos: usize) -> usize {
        let mut depth = 0usize;
        while let Some(tok) = self.tok(pos) {
            match tok {
                Tok::Punct('(' | '[' | '{') => depth += 1,
                Tok::Punct(')' | ']' | '}') => {
                    if depth == 0 {
                        return pos;
                    }
                    depth -= 1;
                }
                _ => {}
            }
            pos += 1;
        }
        pos
    }

    /// Skip the item, statement, field or arm an outer test-only attribute is
    /// on, starting at `pos`. Errs towards skipping too little, never too much.
    fn skip_item(&mut self, mut pos: usize) -> usize {
        // Further attributes on the same item.
        while matches!(self.tok(pos), Some(Tok::Punct('#')))
            && matches!(self.tok(pos + 1), Some(Tok::Punct('[')))
        {
            pos = matching_close(self.tokens, pos + 1) + 1;
        }
        self.note_test_only_mod(pos);
        let mut depth = 0usize;
        let mut item_keyword = false;
        let mut brace_at_top = false;
        while let Some(tok) = self.tok(pos) {
            match tok {
                Tok::Ident(word) if depth == 0 && ITEM_KEYWORDS.contains(&word.as_str()) => {
                    item_keyword = true;
                }
                Tok::Punct(c @ ('(' | '[' | '{')) => {
                    if depth == 0 && *c == '{' {
                        brace_at_top = true;
                    }
                    depth += 1;
                }
                Tok::Punct(c @ (')' | ']' | '}')) => {
                    if depth == 0 {
                        return pos;
                    }
                    depth -= 1;
                    if depth == 0 && *c == '}' && brace_at_top {
                        return pos + 1;
                    }
                }
                Tok::Punct(';') if depth == 0 => return pos + 1,
                Tok::Punct(',') if depth == 0 && !item_keyword => return pos + 1,
                _ => {}
            }
            pos += 1;
        }
        pos
    }

    /// Record `[pub[(…)]] mod name;` at `pos` as a test-only declaration.
    fn note_test_only_mod(&mut self, mut pos: usize) {
        if self.ident(pos) == Some("pub") {
            pos += 1;
            if matches!(self.tok(pos), Some(Tok::Punct('('))) {
                pos = matching_close(self.tokens, pos) + 1;
            }
        }
        if self.ident(pos) != Some("mod") {
            return;
        }
        if let (Some(name), Some(Tok::Punct(';'))) = (self.ident(pos + 1), self.tok(pos + 2)) {
            let decl = ModDecl {
                within: self.inline_modules(),
                name: name.to_string(),
                test_only: true,
            };
            self.scan.decls.push(decl);
        }
    }
}

/// Scan one file's source for calls that can remove stored data.
fn scan_source(source: &str) -> FileScan {
    scan_counting(source, Counting::Erasures)
}

/// Scan one file's source for what `counting` counts.
fn scan_counting(source: &str, counting: Counting) -> FileScan {
    let tokens = tokenize(source);
    Walker::new(&tokens, counting).walk()
}

// ---------------------------------------------------------------------------
// The crate
// ---------------------------------------------------------------------------

/// Every `.rs` file under `dir`, by path relative to `root` with `/`.
fn rust_files(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            rust_files(root, &path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let relative = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            out.push((relative, path));
        }
    }
}

/// The file a `mod` declaration in `file` refers to, if it is one of `files`.
fn declared_file(file: &str, decl: &ModDecl, files: &BTreeSet<String>) -> Option<String> {
    let (dir, name) = file.rsplit_once('/')?;
    let mut base = if matches!(name, "mod.rs" | "lib.rs" | "main.rs") {
        dir.to_string()
    } else {
        format!("{dir}/{}", name.trim_end_matches(".rs"))
    };
    for module in &decl.within {
        base = format!("{base}/{module}");
    }
    [
        format!("{base}/{}.rs", decl.name),
        format!("{base}/{}/mod.rs", decl.name),
    ]
    .into_iter()
    .find(|candidate| files.contains(candidate))
}

/// Counted calls in the production code of the crate at `root`, by
/// `(file, site, call)`, with the lines they are on.
fn census(root: &Path) -> BTreeMap<(String, String, String), Vec<usize>> {
    census_counting(root, Counting::Erasures)
}

/// What `counting` counts in the production code of the crate at `root`, by
/// `(file, site, call)`, with the lines they are on.
fn census_counting(
    root: &Path,
    counting: Counting,
) -> BTreeMap<(String, String, String), Vec<usize>> {
    let mut files = Vec::new();
    rust_files(root, &root.join("src"), &mut files);
    let scans: BTreeMap<String, FileScan> = files
        .iter()
        .map(|(relative, path)| {
            let source = std::fs::read_to_string(path).unwrap_or_default();
            (relative.clone(), scan_counting(&source, counting))
        })
        .collect();
    let names: BTreeSet<String> = scans.keys().cloned().collect();

    // A file is test-only when every declaration of it is, or sits in a
    // test-only file.
    let mut declared_by: BTreeMap<String, Vec<(String, bool)>> = BTreeMap::new();
    for (file, scan) in &scans {
        for decl in &scan.decls {
            if let Some(target) = declared_file(file, decl, &names) {
                declared_by
                    .entry(target)
                    .or_default()
                    .push((file.clone(), decl.test_only));
            }
        }
    }
    let mut test_only: BTreeSet<String> = scans
        .iter()
        .filter(|(_, scan)| scan.test_only)
        .map(|(file, _)| file.clone())
        .collect();
    loop {
        let newly: Vec<String> = declared_by
            .iter()
            .filter(|(target, _)| !test_only.contains(*target))
            .filter(|(_, decls)| {
                decls.iter().all(|(declarer, decl_test_only)| {
                    *decl_test_only || test_only.contains(declarer)
                })
            })
            .map(|(target, _)| target.clone())
            .collect();
        if newly.is_empty() {
            break;
        }
        test_only.extend(newly);
    }

    let mut found = BTreeMap::new();
    for (file, scan) in scans {
        if test_only.contains(&file) {
            continue;
        }
        for ((site, call), lines) in scan.calls {
            found.insert((file.clone(), site, call), lines);
        }
    }
    found
}

/// The census of this crate against [`ERASE_SITES`]: an empty string when
/// they agree, else a report of the differences.
fn differences(found: &BTreeMap<(String, String, String), Vec<usize>>) -> String {
    let mut listed: BTreeMap<(String, String, String), usize> = BTreeMap::new();
    let mut report = String::new();
    for site in ERASE_SITES {
        let key = (
            site.file.to_string(),
            site.site.to_string(),
            site.call.to_string(),
        );
        if listed.insert(key, site.count).is_some() {
            let _ = writeln!(
                report,
                "  listed twice: {} {} {}",
                site.file, site.site, site.call
            );
        }
        if site.reason.trim().is_empty() {
            let _ = writeln!(
                report,
                "  listed without a reason: {} {} {}",
                site.file, site.site, site.call
            );
        }
    }

    let mut new_calls = String::new();
    let mut gone_calls = String::new();
    for (key, lines) in found {
        let allowed = listed.get(key).copied().unwrap_or(0);
        let (file, site, call) = key;
        if lines.len() > allowed {
            let at: Vec<String> = lines.iter().map(|line| format!("{file}:{line}")).collect();
            let _ = writeln!(
                new_calls,
                "  {file}  {site}  {call}: found {}, listed {allowed}  ({})",
                lines.len(),
                at.join(", ")
            );
        } else if lines.len() < allowed {
            let _ = writeln!(
                gone_calls,
                "  {file}  {site}  {call}: found {}, listed {allowed}",
                lines.len()
            );
        }
    }
    for (key, allowed) in &listed {
        if !found.contains_key(key) {
            let (file, site, call) = key;
            let _ = writeln!(
                gone_calls,
                "  {file}  {site}  {call}: found 0, listed {allowed}"
            );
        }
    }

    if !new_calls.is_empty() {
        let _ = write!(
            report,
            "New calls that can remove stored data:\n{new_calls}"
        );
    }
    if !gone_calls.is_empty() {
        let _ = write!(report, "Listed calls no longer found:\n{gone_calls}");
    }
    report
}

/// The census of this crate's `src/` tree must match [`ERASE_SITES`].
#[test]
fn every_call_that_can_remove_stored_data_is_reviewed() {
    let found = census(Path::new(env!("CARGO_MANIFEST_DIR")));
    let report = differences(&found);
    assert!(
        report.is_empty(),
        "\nThe calls in src/ that can remove stored data no longer match the reviewed list.\n\n\
         {report}\n\
         A node keeps every record and pointer it holds while its disk has room. The prune \
         passes erase one only under disk pressure, beyond the erase floor, farthest first, \
         and only after the nodes now nearest to it prove they hold it \
         (src/replication/retention.rs). Every other call that can remove stored data is \
         listed, with the reason it is there, in ERASE_SITES in \
         src/replication/retention/census.rs.\n\n\
         To review a new call: decide whether it can remove the only local copy of a valid \
         record or pointer. If it can, route it through the prune passes' rule instead, or \
         at least through their disk-pressure state, as the storage migration's shed does. \
         If it cannot (temporary files, metadata, data proven wrong), add an entry to \
         ERASE_SITES with the file, the site and call exactly as shown above, the count, and \
         a one-line reason, and have it reviewed. For a listed call that has gone, remove or \
         lower its entry.\n"
    );
}

/// The prune passes and the storage migration share one disk-pressure state
/// per node, built in one place in production code. One built anywhere else
/// would start without the pressure the others are under, and end it at 10%
/// free instead of 15%.
#[test]
fn the_disk_pressure_state_is_built_once_per_node() {
    let found = census_counting(
        Path::new(env!("CARGO_MANIFEST_DIR")),
        Counting::Calls(DISK_PRESSURE_STATE_BUILDERS),
    );
    let sites: Vec<(String, String, String, usize)> = found
        .into_iter()
        .map(|((file, site, call), lines)| (file, site, call, lines.len()))
        .collect();
    assert_eq!(
        sites,
        vec![(
            "src/replication/mod.rs".to_string(),
            "ReplicationEngine::new".to_string(),
            "for_store".to_string(),
            1
        )],
        "\nA node's disk-pressure state (DiskRetention) is built once, in \
         ReplicationEngine::new, and shared. Pass the engine's \
         (ReplicationEngine::disk_retention) instead of building another.\n"
    );
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn calls(source: &str) -> Vec<(String, String, usize)> {
        scan_source(source)
            .calls
            .into_iter()
            .map(|((site, call), lines)| (site, call, lines.len()))
            .collect()
    }

    fn one(site: &str, call: &str) -> (String, String, usize) {
        (site.to_string(), call.to_string(), 1)
    }

    #[test]
    fn comments_and_string_literals_are_not_calls() {
        let source = r##"
            // std::fs::remove_file(path);
            /* store.delete(key); /* nested */ remove_dir_all(dir) */
            /// Doc: remove_file
            fn f() {
                let a = "remove_file(path)";
                let b = r#"store.delete("x")"#;
                let c = b"delete";
                let d = '"';
                let e = 'x';
                println!("{}", "remove_dir(p)");
            }
        "##;
        assert!(calls(source).is_empty(), "{:?}", calls(source));
    }

    #[test]
    fn a_call_split_across_lines_or_spaced_out_is_still_counted() {
        let source = "fn prune() {\n    storage\n        .delete(\n            key,\n        )\n        .await;\n    std :: fs :: remove_file ( path ) ;\n}\n";
        assert_eq!(
            calls(source),
            vec![one("prune", "delete"), one("prune", "remove_file")]
        );
    }

    #[test]
    fn test_only_code_is_left_out_and_production_code_is_not() {
        let source = r#"
            #[cfg(test)]
            mod tests { fn g() { std::fs::remove_file(p); } }
            #[cfg(any(test, feature = "test-utils"))]
            pub fn h(store: &Store) { store.delete(k); }
            #[test]
            fn t() { std::fs::remove_dir(p); }
            #[tokio::test(flavor = "multi_thread")]
            async fn u() { std::fs::remove_dir(p); }
            #[cfg(all(test, unix))]
            fn v() { std::fs::remove_dir(p); }
            #[cfg(test)]
            #[allow(clippy::unwrap_used)]
            fn w<A, B>(a: A, b: B) -> Result<(), ()> { std::fs::remove_dir(p); Ok(()) }
            #[cfg(not(test))]
            fn production() { std::fs::remove_file(p); }
            #[cfg(unix)]
            fn on_unix() { std::fs::remove_file(p); }
            #[cfg(feature = "logging")]
            fn logged() { std::fs::remove_file(p); }
            #[cfg(any(test, unix))]
            fn maybe() { std::fs::remove_file(p); }
        "#;
        assert_eq!(
            calls(source),
            vec![
                one("logged", "remove_file"),
                one("maybe", "remove_file"),
                one("on_unix", "remove_file"),
                one("production", "remove_file"),
            ]
        );
    }

    #[test]
    fn a_test_only_statement_hides_only_itself() {
        let source = r"
            fn mixed() {
                #[cfg(test)]
                let x = vec![1, 2];
                std::fs::remove_file(p);
                match y {
                    #[cfg(test)]
                    A => std::fs::remove_dir(p),
                    B => std::fs::remove_dir_all(p),
                }
            }
        ";
        assert_eq!(
            calls(source),
            vec![one("mixed", "remove_dir_all"), one("mixed", "remove_file")]
        );
    }

    #[test]
    fn sites_are_named_by_type_and_function() {
        let source = r"
            impl<T: Clone> Store<T> where T: Send {
                pub async fn erase(&self, key: &[u8; 32]) -> Result<bool> {
                    self.inner.delete(key)
                }
            }
            impl Drop for Guard<'_> {
                fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
            }
            impl<'a> std::fmt::Display for Wrapper<'a> {
                fn fmt(&self) -> impl Iterator<Item = u8> { store.delete(k) }
            }
            fn free(cb: fn(u8) -> u8) { remove_marked_directory(dir); }
            trait Eraser { fn wipe(&self) { std::fs::remove_file(p); } fn plain(&self); }
        ";
        assert_eq!(
            calls(source),
            vec![
                one("Eraser::wipe", "remove_file"),
                one("Guard::drop", "remove_dir_all"),
                one("Store::erase", "delete"),
                one("Wrapper::fmt", "delete"),
                one("free", "remove_marked_directory"),
            ]
        );
    }

    #[test]
    fn an_import_under_another_name_is_counted_where_it_is_made() {
        let source = "use std::fs::remove_file as zap;\nfn f() { zap(p); }\n";
        assert_eq!(calls(source), vec![one("<module>", "remove_file")]);
    }

    #[test]
    fn definitions_and_mentions_of_erasers_without_a_call_are_not_counted() {
        let source = r"
            fn delete(&self) {}
            async fn erase_stored_record(k: &K) -> bool { true }
            fn f(config: &Config) { if config.retire_legacy { } }
        ";
        assert!(calls(source).is_empty(), "{:?}", calls(source));
    }

    #[test]
    fn clear_and_truncate_count_only_in_their_erasing_forms() {
        let source = r"
            fn f() {
                items.clear();
                table.clear(&mut wtxn)?;
                std::fs::OpenOptions::new().write(true).truncate(true).open(p);
                items.truncate(3);
                file.set_len(0);
            }
        ";
        assert_eq!(
            calls(source),
            vec![one("f", "clear"), one("f", "set_len"), one("f", "truncate")]
        );
    }

    #[test]
    fn named_calls_are_counted_with_their_owner_and_definitions_are_not() {
        let source = r"
            impl DiskRetention {
                pub fn for_store(s: &Store) -> Self { Self::new(s.dir()) }
                pub fn new(dir: PathBuf) -> Self { Self { dir } }
            }
            fn engine(s: &Store) {
                let a = retention::DiskRetention::for_store(&s);
                let b = DiskRetention :: new ( dir );
                let c = Other::new(dir);
                let d = Vec::new();
            }
        ";
        let found: Vec<(String, String, usize)> =
            scan_counting(source, Counting::Calls(DISK_PRESSURE_STATE_BUILDERS))
                .calls
                .into_iter()
                .map(|((site, call), lines)| (site, call, lines.len()))
                .collect();
        assert_eq!(
            found,
            vec![
                one("engine", "DiskRetention::new"),
                one("engine", "for_store")
            ]
        );
    }

    #[test]
    fn the_system_calls_behind_removal_are_counted() {
        let source = r"
            fn f(p: &CStr, fd: i32) {
                unsafe {
                    libc::unlink(p.as_ptr());
                    libc::unlinkat(fd, p.as_ptr(), 0);
                    libc::rmdir(p.as_ptr());
                    libc::ftruncate(fd, 0);
                }
            }
        ";
        assert_eq!(
            calls(source),
            vec![
                one("f", "ftruncate"),
                one("f", "rmdir"),
                one("f", "unlink"),
                one("f", "unlinkat"),
            ]
        );
    }

    #[test]
    fn module_files_declared_only_for_tests_are_test_only() {
        let lib = scan_source(
            "#[cfg(test)]\nmod census;\npub mod store;\n#[cfg(any(test, feature = \"test-utils\"))]\npub mod files;\n#[cfg(not(any(test, feature = \"test-utils\")))]\npub(crate) mod files;\n",
        );
        let declared: Vec<(String, bool)> = lib
            .decls
            .iter()
            .map(|d| (d.name.clone(), d.test_only))
            .collect();
        assert_eq!(
            declared,
            vec![
                ("census".to_string(), true),
                ("store".to_string(), false),
                ("files".to_string(), true),
                ("files".to_string(), false),
            ]
        );
        let files: BTreeSet<String> = ["src/a/census.rs", "src/a/b/mod.rs"]
            .iter()
            .map(ToString::to_string)
            .collect();
        let decl = |name: &str| ModDecl {
            within: Vec::new(),
            name: name.to_string(),
            test_only: false,
        };
        assert_eq!(
            declared_file("src/a.rs", &decl("census"), &files),
            Some("src/a/census.rs".to_string())
        );
        assert_eq!(
            declared_file("src/a/mod.rs", &decl("b"), &files),
            Some("src/a/b/mod.rs".to_string())
        );
        assert!(scan_source("#![cfg(test)]\nfn f() { std::fs::remove_file(p); }\n").test_only);
    }
}

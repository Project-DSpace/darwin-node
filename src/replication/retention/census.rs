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
//!   under another name ([`ERASERS`]). Verifying reads are counted there
//!   too: `get_or_quarantine` removes a chunk file whose bytes don't hash to
//!   its name, so each read that can remove one is listed. The plain
//!   verifying read, `get`, never removes a file and is not counted;
//! - `clear` called with an argument: a table cleared inside a transaction
//!   (clearing an in-memory collection takes none);
//! - `truncate(true)`: a file opened so that its old contents are dropped.
//!
//! Not counted: renames, plain writes and `File::create`, which publish a file
//! written under a temporary name or move one aside, and a temporary directory
//! dropped at the end of its scope. Nor can a count tell which receiver a
//! listed call is on, so a listed call turned into another of the same name
//! at the same site passes; the review of the change has to catch that.
//!
//! The same reading of the source pins other calls to reviewed sites, each in
//! a test of its own: the disk-pressure state's builders, the routing loop's
//! event reader, the lanes that penalise an unheld close-group chunk, upstream's
//! merkle payment code (compiled, never reached), and the payment vault reads
//! and RPC addresses behind the payment check's fallback. A last test reads
//! the vendored protocol's wire messages and payment proof types, and the
//! vendored listings protocol's messages, against a reviewed list of what a
//! node does with each.
//!
//! The storage-node image's scripts (docker/entrypoint.sh, docker/nodes) can
//! remove a node's folders too, outside this crate: tests/darwin_image.rs
//! lists every deletion they make (`DELETIONS`), the 3-day sweep of retired
//! folders among them, and every move (`MOVES`), each move into the swept
//! folder a deletion put off.

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
    "clean_up",
    "delete_blocking",
    "delete_held",
    "delete_mark_last",
    "delete_unobserved",
    "empty_and_unmark",
    "erase_stored_record",
    "get_or_quarantine",
    "quarantine_corrupt",
    "remove_all",
];

/// Calls that build a node's disk-pressure state. The chunk and pointer prune
/// passes must share one, so that the pressure lasts until 15% is free for
/// both: one built afresh would end it at 10%.
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
const LEGACY_LEFTOVER: &str = "Part of removing, once the chunk store has opened, a \
     chunks.mdb directory left by an earlier release's storage migration that carries its \
     RETIRED mark or is empty (upstream v0.23.0, ADR-0022): retirement marked it only after \
     every chunk the node kept was in the file store and every chunk it gave up was proven \
     held by its close group. Anything else there is kept.";
const UPGRADE_CACHE: &str = "Upgrade cache files; no record data.";
const LOG_FILES: &str = "The node's own log files, never record data.";
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
        "src/listings/service.rs",
        "ListingService::ensure_listing",
        "get_or_quarantine",
        1,
        "Reads an envelope chunk to check its listing with its close group; a copy whose \
         bytes, read again under the write lane, don't hash to its name is removed, as in \
         v0.21.0+ds.11.",
    ),
    site(
        "src/listings/service.rs",
        "ListingService::held",
        "get_or_quarantine",
        1,
        "Reads an envelope chunk the store has stopped claiming, to tell a listing whose \
         chunk is gone from one whose chunk can't be read; a copy whose bytes, read again \
         under the write lane, don't hash to its name is removed, as in v0.21.0+ds.11.",
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
        "ListingStore::number_feed",
        "clear",
        2,
        "Empties the listing change feed to number the listings the node serves afresh \
         under a new epoch, when a start finds the feed new, its numbering cut short, or \
         the store written to by another version; listing metadata, rebuilt from the \
         listings themselves, which stay.",
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
        "ListingStore::renew_record",
        "delete",
        1,
        "Drops an address's old row in the listing change feed as its new record is \
         written, in the same transaction: the feed keeps one row per address. Listing \
         metadata.",
    ),
    site(
        "src/listings/store.rs",
        "ListingStore::settle",
        "delete",
        1,
        LISTING_METADATA,
    ),
    site(
        "src/log_files.rs",
        "LogFiles::delete_oldest_closed",
        "remove_file",
        1,
        LOG_FILES,
    ),
    site(
        "src/log_files.rs",
        "LogFiles::enforce_limits",
        "remove_file",
        1,
        LOG_FILES,
    ),
    site(
        "src/node.rs",
        "NodeBuilder::build",
        "clean_up",
        1,
        "Removes what an earlier release's storage migration left in the node root, only \
         once the chunk store has opened and only for a node that stores chunks: a \
         chunks.mdb directory, or a numbered tombstone of one, that carries its RETIRED \
         mark or is empty (upstream v0.23.0, ADR-0022). An unmarked one with data in it, a \
         link, or one whose state can't be read is kept and named in a warning.",
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
        "src/replication/audit.rs",
        "compare_with_local_copies",
        "get_or_quarantine",
        1,
        "Reads this node's own copy, the reference a digest audit judges a peer against; \
         a copy whose bytes, read again under the write lane, don't hash to its name is \
         removed, as upstream v0.23.0 does, and the key is skipped, so no peer is judged \
         against it and replication restores the chunk from other nodes.",
    ),
    site(
        "src/replication/mod.rs",
        "ReplicationEngine::start_fresh_offer_dispatcher",
        "get_or_quarantine",
        1,
        "Reads a newly stored chunk back to offer it to its close group; a copy whose \
         bytes, read again under the write lane, don't hash to its name is removed, as in \
         v0.21.0+ds.11.",
    ),
    site(
        "src/replication/mod.rs",
        "handle_fetch_request",
        "get_or_quarantine",
        1,
        "Hands a chunk to a peer that fetches it for replication; a copy whose bytes, read \
         again under the write lane, don't hash to its name is removed, as in \
         v0.21.0+ds.11.",
    ),
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
        "src/replication/possession.rs",
        "run_possession_check",
        "get_or_quarantine",
        1,
        "Reads this node's own copy, the reference the possession check after a fresh \
         write judges peers against; a copy whose bytes, read again under the write lane, \
         don't hash to its name is removed, as upstream v0.23.0 does, and the check is \
         skipped, so no peer is judged against it and replication restores the chunk from \
         other nodes.",
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
        "src/replication/pruning.rs",
        "local_record_bytes",
        "get_or_quarantine",
        1,
        "Reads this node's own copy, the reference that holders are judged against, through \
         local_record_digest, by a prune audit; a copy whose bytes, read again under the \
         write lane, don't hash to its name is removed, as upstream v0.23.0 does for prune \
         audits, and the key is not audited, so no holder is judged against it and \
         replication restores the chunk from other nodes.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "ChunkStore::delete",
        "delete_unobserved",
        1,
        "Removes a chunk; the only production caller of ChunkStore::delete is the prune \
         pass's erase_stored_record.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "ChunkStore::delete_unobserved",
        "remove_file",
        1,
        "The chunk store's chunk removal, reached only through ChunkStore::delete.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "ChunkStore::get_or_quarantine",
        "quarantine_corrupt",
        1,
        "The chunk store's read that can remove a chunk file whose bytes, read again under \
         the write lane, do not hash to its name: never a valid record. Its callers are each \
         listed here.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "ChunkStore::quarantine_corrupt",
        "remove_file",
        1,
        "Removes a chunk file proven not to hold the record its name says.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "ChunkStore::recheck_corrupt",
        "get_or_quarantine",
        1,
        "Rechecks, one at a time on a single worker, a committed chunk this node's answer \
         to a storage audit read raw and found not to hash to its name; a copy whose bytes, \
         read again under the write lane, still don't is removed, as upstream v0.23.0 does, \
         so the next commitment leaves it out and replication restores it from other nodes.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "publish_in_place",
        "remove_file",
        2,
        "Removes a chunk file this same write created and could not complete.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "publish_via_rename",
        "remove_file",
        1,
        TEMP_FILE,
    ),
    site(
        "src/storage/chunk_store.rs",
        "sweep_temp",
        "remove_file",
        1,
        "Sweeps temporary chunk files of an interrupted write at startup.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "write_and_replace",
        "remove_file",
        1,
        TEMP_FILE,
    ),
    site(
        "src/storage/chunk_store.rs",
        "write_and_replace",
        "truncate",
        1,
        "Overwrites, with verified bytes, a chunk file proven not to hash to its name.",
    ),
    site(
        "src/storage/chunk_store.rs",
        "write_file_atomic",
        "remove_file",
        1,
        TEMP_FILE,
    ),
    site(
        "src/storage/chunk_store.rs",
        "write_temp",
        "remove_file",
        2,
        TEMP_FILE,
    ),
    site(
        "src/storage/handler.rs",
        "AntProtocol::get_local",
        "get_or_quarantine",
        1,
        "Hands a locally stored chunk to the library's caller; a copy whose bytes, read \
         again under the write lane, don't hash to its name is removed, as in \
         v0.21.0+ds.11.",
    ),
    site(
        "src/storage/handler.rs",
        "AntProtocol::handle_get_inner",
        "get_or_quarantine",
        1,
        "Hands a chunk to a client's GET; a copy whose bytes, read again under the write \
         lane, don't hash to its name is removed, as in v0.21.0+ds.11.",
    ),
    site(
        "src/storage/legacy_artifacts.rs",
        "clean_up",
        "remove_all",
        1,
        LEGACY_LEFTOVER,
    ),
    site(
        "src/storage/legacy_artifacts.rs",
        "delete_mark_last",
        "empty_and_unmark",
        1,
        LEGACY_LEFTOVER,
    ),
    site(
        "src/storage/legacy_artifacts.rs",
        "delete_mark_last",
        "remove_dir",
        1,
        LEGACY_LEFTOVER,
    ),
    site(
        "src/storage/legacy_artifacts.rs",
        "empty_and_unmark",
        "remove_file",
        2,
        "Part of removing a finished-with migration leftover (see clean_up), off Unix: its \
         files, then its mark.",
    ),
    site(
        "src/storage/legacy_artifacts.rs",
        "empty_and_unmark",
        "unlinkat",
        2,
        "Part of removing a finished-with migration leftover (see clean_up), on Unix: its \
         files, then its mark, each against one directory handle opened without following \
         links, refusing any subdirectory.",
    ),
    site(
        "src/storage/legacy_artifacts.rs",
        "remove_all",
        "delete_mark_last",
        1,
        LEGACY_LEFTOVER,
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
        "BinaryCache::prune",
        "remove_file",
        1,
        "Archives of versions older than the running and previous ones, after a clean \
         start; upgrade cache files, no record data.",
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
        "process_get_chunk",
        "get_or_quarantine",
        1,
        "Hands a chunk to a browser's GET; a copy whose bytes, read again under the write \
         lane, don't hash to its name is removed, as in v0.21.0+ds.11.",
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
    /// before it (`Type::name`). A name ending in `*` stands for every name
    /// that starts with what comes before it.
    Calls(&'static [(Option<&'static str>, &'static str)]),
    /// Every mention of these names, called or not (an import, a type, a
    /// path), as for [`Counting::Calls`]. Definitions are not mentions.
    Mentions(&'static [(Option<&'static str>, &'static str)]),
}

/// The label a mention of `name`, after `owner::` if it has an owner, is
/// counted under, if `wanted` lists it: the name, after the owner its entry
/// asks for. An entry's name ending in `*` matches every name starting with
/// what comes before it.
fn wanted_label(
    wanted: &[(Option<&str>, &str)],
    owner: Option<&str>,
    name: &str,
) -> Option<String> {
    wanted
        .iter()
        .find(|(wanted_owner, pattern)| {
            let named = pattern
                .strip_suffix('*')
                .map_or(*pattern == name, |prefix| name.starts_with(prefix));
            named && wanted_owner.is_none_or(|wanted_owner| owner == Some(wanted_owner))
        })
        .map(|(wanted_owner, _)| {
            wanted_owner.map_or_else(
                || name.to_string(),
                |wanted_owner| format!("{wanted_owner}::{name}"),
            )
        })
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
        let wanted = match self.counting {
            Counting::Erasures => {
                self.count_erasure(pos, name, called, line);
                return pos + 1;
            }
            Counting::Calls(wanted) if called => wanted,
            Counting::Calls(_) => return pos + 1,
            Counting::Mentions(wanted) => wanted,
        };
        let owner = if matches!(previous, Some(Tok::Joint("::"))) {
            pos.checked_sub(2)
                .and_then(|at| self.ident(at))
                .map(str::to_string)
        } else {
            None
        };
        if let Some(label) = wanted_label(wanted, owner.as_deref(), name) {
            self.count(&label, line);
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
         at least through their disk-pressure state. If it cannot (temporary files, metadata, data proven wrong), add an entry to \
         ERASE_SITES with the file, the site and call exactly as shown above, the count, and \
         a one-line reason, and have it reviewed. For a listed call that has gone, remove or \
         lower its entry.\n"
    );
}

/// The chunk and pointer prune passes share one disk-pressure state per
/// node, built in one place in production code. One built anywhere else
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
         ReplicationEngine::new, and shared. Pass the engine's instead of building \
         another.\n"
    );
}

// ---------------------------------------------------------------------------
// Other reviewed calls
// ---------------------------------------------------------------------------

/// A reviewed site of a call that one of the tests below pins: the file, site
/// and call as the census names them, and how many times it is made there.
type Reviewed = (&'static str, &'static str, &'static str, usize);

/// What `counting` finds in this crate's production code, against
/// `reviewed`: an empty string when they agree, else each difference, with
/// the lines of the calls found.
fn unreviewed(counting: Counting, reviewed: &[Reviewed]) -> String {
    let found = census_counting(Path::new(env!("CARGO_MANIFEST_DIR")), counting);
    let listed: BTreeMap<(String, String, String), usize> = reviewed
        .iter()
        .map(|(file, site, call, count)| {
            (
                (
                    (*file).to_string(),
                    (*site).to_string(),
                    (*call).to_string(),
                ),
                *count,
            )
        })
        .collect();
    let mut report = String::new();
    for (key, lines) in &found {
        let allowed = listed.get(key).copied().unwrap_or(0);
        if lines.len() != allowed {
            let (file, site, call) = key;
            let at: Vec<String> = lines.iter().map(|line| format!("{file}:{line}")).collect();
            let _ = writeln!(
                report,
                "  {file}  {site}  {call}: found {}, reviewed {allowed}  ({})",
                lines.len(),
                at.join(", ")
            );
        }
    }
    for (key, allowed) in &listed {
        if !found.contains_key(key) {
            let (file, site, call) = key;
            let _ = writeln!(
                report,
                "  {file}  {site}  {call}: found 0, reviewed {allowed}"
            );
        }
    }
    report
}

/// The protocol routing loop reads its events through `next_routing_event`,
/// which carries on when the loop falls behind the event broadcast. Upstream's
/// loop ended there, and the node then answered no request while it stayed
/// connected.
#[test]
fn the_routing_loop_reads_its_events_through_the_lag_tolerant_reader() {
    const READER: &[(Option<&str>, &str)] = &[(None, "next_routing_event")];
    const REVIEWED: &[Reviewed] = &[(
        "src/node.rs",
        "RunningNode::start_protocol_routing",
        "next_routing_event",
        1,
    )];
    let report = unreviewed(Counting::Calls(READER), REVIEWED);
    assert!(
        report.is_empty(),
        "\nThe protocol routing loop no longer reads its events only through \
         next_routing_event.\n\n{report}\n\
         RunningNode::start_protocol_routing (src/node.rs) must read saorsa-core's event \
         broadcast through next_routing_event, which warns and carries on past \
         RecvError::Lagged. A loop that ends there leaves the node connected and answering \
         no request until it restarts.\n"
    );
}

/// A peer that doesn't hold a close-group chunk it should is penalised on
/// every lane that finds it out, as in upstream v0.23.0: the responsible-chunk
/// audit, the possession check after a fresh write, the prune audit, a
/// sole-source replica hint its sender then denies, and a fetch a verified
/// source can't serve. Nothing can withhold it: the release switch, its
/// environment override and the helper the lanes went through while nodes
/// moved off the LMDB chunk store are gone, and stay gone. Each lane charges
/// the peer's trust itself, and the functions holding the lanes keep the
/// reviewed number of trust charges.
#[test]
fn the_unheld_chunk_penalty_is_charged_on_every_lane() {
    const SWITCH: &[(Option<&str>, &str)] = &[
        (None, "penalise_unheld_close_group_chunk"),
        (None, "close_group_storage_penalty_suspended"),
        (None, "set_close_group_storage_penalty_suspended"),
        (None, "apply_close_group_storage_penalty_policy"),
        (None, "RELEASE_SUSPEND_CLOSE_GROUP_STORAGE_PENALTY"),
        (None, "SUSPEND_CLOSE_GROUP_STORAGE_PENALTY_ENV"),
        (None, "CLOSE_GROUP_STORAGE_PENALTY_SUSPENDED"),
    ];
    let switch = unreviewed(Counting::Mentions(SWITCH), &[]);
    assert!(
        switch.is_empty(),
        "\nThe switch that withheld the penalty for an unheld close-group chunk is back.\n\n\
         {switch}\n\
         It existed only while nodes moved off the LMDB chunk store, which is gone. A peer \
         that doesn't hold a chunk it should is penalised on every lane, as in upstream \
         v0.23.0.\n"
    );

    const CHARGE: &[(Option<&str>, &str)] = &[(None, "report_trust_event")];
    const LANES: &[Reviewed] = &[
        // A fetch a verified source answered NotFound or Error, among the fetch
        // path's other charges (6 in all).
        (
            "src/replication/mod.rs",
            "execute_single_fetch",
            "report_trust_event",
            6,
        ),
        // A responsible-chunk audit the peer failed, whatever the reason, timeouts
        // included; with the audit's pass and its bootstrap-claim abuse (3 in all).
        (
            "src/replication/mod.rs",
            "handle_audit_result",
            "report_trust_event",
            3,
        ),
        // A sole-source replica hint the close group rejected, or its sender denied.
        (
            "src/replication/mod.rs",
            "run_verification_cycle",
            "report_trust_event",
            1,
        ),
        // The possession check after a fresh write: no answer, or a wrong one.
        (
            "src/replication/possession.rs",
            "report_possession_audit_failure",
            "report_trust_event",
            1,
        ),
        // The possession check after a fresh write: a confirmed failure.
        (
            "src/replication/possession.rs",
            "report_possession_confirmed_failure",
            "report_trust_event",
            1,
        ),
        // A prune audit the peer failed, for a key within the erase floor.
        (
            "src/replication/pruning.rs",
            "report_prune_audit_failure_once",
            "report_trust_event",
            1,
        ),
    ];
    let found = census_counting(
        Path::new(env!("CARGO_MANIFEST_DIR")),
        Counting::Calls(CHARGE),
    );
    let mut lanes = String::new();
    for (file, site, call, count) in LANES {
        let key = (
            (*file).to_string(),
            (*site).to_string(),
            (*call).to_string(),
        );
        let lines = found.get(&key).map_or(&[][..], Vec::as_slice);
        if lines.len() != *count {
            let at: Vec<String> = lines.iter().map(|line| format!("{file}:{line}")).collect();
            let _ = writeln!(
                lanes,
                "  {file}  {site}  {call}: found {}, reviewed {count}  ({})",
                lines.len(),
                at.join(", ")
            );
        }
    }
    assert!(
        lanes.is_empty(),
        "\nThe lanes that penalise a peer for an unheld close-group chunk no longer charge \
         its trust as reviewed.\n\n{lanes}\n\
         Each of these functions charges the peer itself with report_trust_event. If a \
         charge moved, follow it and list where it went; if one is gone, find out why \
         before changing the count.\n"
    );
}

/// Upstream's merkle (batch) payment code stays compiled, so that merges stay
/// cheap, but no production code reaches it: a merkle quote request is refused
/// before anything is signed, and a merkle proof before it is verified.
#[test]
fn the_merkle_payment_code_is_not_reached() {
    const MERKLE: &[(Option<&str>, &str)] = &[
        (None, "verify_merkle_payment"),
        (None, "sign_merkle_candidate_quote"),
        (None, "create_merkle_candidate_quote"),
        (None, "get_completed_merkle_payment"),
    ];
    const REVIEWED: &[Reviewed] = &[
        // Inside upstream's merkle proof verification, which nothing calls.
        (
            "src/payment/verifier.rs",
            "PaymentVerifier::verify_merkle_payment",
            "get_completed_merkle_payment",
            1,
        ),
        // Inside upstream's merkle quote signing, which nothing calls.
        (
            "src/storage/handler.rs",
            "AntProtocol::sign_merkle_candidate_quote",
            "create_merkle_candidate_quote",
            1,
        ),
    ];
    let report = unreviewed(Counting::Calls(MERKLE), REVIEWED);
    assert!(
        report.is_empty(),
        "\nProduction code calls upstream's merkle payment code.\n\n{report}\n\
         This node refuses merkle (batch) payments: a merkle candidate quote request is \
         answered with BATCH_PAYMENTS_REFUSED before anything is signed \
         (AntProtocol::handle_merkle_candidate_quote), and a merkle proof is refused before \
         it is verified, for chunks and pointers alike (PaymentVerifier::verify_payment_inner). \
         A batch is paid on chain before any node sees a PUT, so a node that took the \
         quote and then refused the PUT would let the client pay for nothing.\n"
    );
}

/// The payment vault is read only by the payment check's two reads: its
/// records, through the pool of the payment reads (`SettlementReads`), and
/// the merkle record, through `read_payment`. The root vault's records of
/// waves go through the same pool. Each tries the node's RPC and then the
/// chain's other public RPCs.
#[test]
fn the_payment_vault_is_read_only_through_the_rpc_fallback() {
    const VAULT: &[(Option<&str>, &str)] = &[
        (None, "IPaymentVault"),
        (None, "PaymentVaultHandler"),
        (Some("payment_vault"), "get_*"),
        (None, "COMPLETED_PAYMENTS_SELECTOR"),
        (None, "ROOT_RECORDS_SELECTOR"),
    ];
    const REVIEWED: &[Reviewed] = &[
        // completedPayments' selector, defined once, and used in the one
        // request every record read sends through the payment reads' pool.
        (
            "src/payment/settlement.rs",
            "<module>",
            "COMPLETED_PAYMENTS_SELECTOR",
            1,
        ),
        (
            "src/payment/settlement.rs",
            "record_request",
            "COMPLETED_PAYMENTS_SELECTOR",
            1,
        ),
        // The root vault's records, by a wave's key: the selector, defined
        // once, and used in the one request every wave record read sends
        // through the same pool.
        (
            "src/payment/settlement.rs",
            "<module>",
            "ROOT_RECORDS_SELECTOR",
            1,
        ),
        (
            "src/payment/settlement.rs",
            "wave_record_request",
            "ROOT_RECORDS_SELECTOR",
            1,
        ),
        // completedMerklePayments, through read_payment; nothing calls it.
        (
            "src/payment/verifier.rs",
            "PaymentVerifier::verify_merkle_payment",
            "PaymentVaultHandler",
            1,
        ),
    ];
    let report = unreviewed(Counting::Mentions(VAULT), REVIEWED);
    assert!(
        report.is_empty(),
        "\nThe payment vault is read somewhere new.\n\n{report}\n\
         A payment check reads the vault's records through the payment reads' pool \
         (src/payment/settlement.rs), and the merkle record through \
         PaymentVerifier::read_payment; each tries the node's RPC and then the chain's \
         other public RPCs, so that one busy provider does not make the node refuse a \
         paid chunk. Read the vault through them, and list the read here.\n"
    );
}

/// The per-quote cache of payment facts is filled from one vault's logs
/// alone, the per-quote vault's, on purpose: single-node proofs (tag 0x01)
/// settle there, and a root vault's `DataPaymentMade` log in the same
/// receipt never stands in for a per-quote record. Root proofs (tag 0x03)
/// settle in the root vault's record of their wave, read by its key.
#[test]
fn the_per_quote_cache_takes_the_per_quote_vaults_logs_alone() {
    const VAULT_PAYMENTS: &[(Option<&str>, &str)] = &[(None, "vault_payments")];
    const REVIEWED: &[Reviewed] = &[
        // Re-exports of the function: no read of their own.
        ("src/listings/mod.rs", "<module>", "vault_payments", 1),
        ("src/payment/settlement.rs", "<module>", "vault_payments", 1),
        // The per-quote cache, from the per-quote vault's address alone.
        (
            "src/payment/settlement.rs",
            "SettlementReads::remember_payments",
            "vault_payments",
            1,
        ),
    ];
    let report = unreviewed(Counting::Mentions(VAULT_PAYMENTS), REVIEWED);
    assert!(
        report.is_empty(),
        "\nA receipt's vault payments are taken somewhere new.\n\n{report}\n\
         The per-quote cache takes the per-quote vault's logs alone \
         (SettlementReads::remember_payments), so a root vault's log never settles a \
         single-node proof. Listings take every vault's payments through the vault set. \
         Say which vault a new place takes, and list it here.\n"
    );
}

/// Every place that decides by a payment proof's type is a reviewed one
/// ([`PROOF_TYPE_SITES`]).
#[test]
fn every_place_that_decides_by_proof_type_is_reviewed() {
    let report = unreviewed(Counting::Mentions(PROOF_TYPE_DECIDERS), PROOF_TYPE_SITES);
    assert!(
        report.is_empty(),
        "\nProduction code decides by a payment proof's type somewhere new.\n\n{report}\n\
         Each place must handle every type: single-node and root proofs taken, each \
         settled in its own vault, and merkle proofs refused. Decide with a match that \
         names every ProofType, with no catch-all arm, and list the place in \
         PROOF_TYPE_SITES (src/replication/retention/census.rs).\n"
    );
}

/// The node's configured RPC address is taken only where a list of RPCs to
/// fall back across is built from it, or by the local devnet tool.
#[test]
fn the_rpc_address_is_taken_only_where_a_fallback_list_is_built() {
    const RPC: &[(Option<&str>, &str)] = &[(None, "rpc_url")];
    const REVIEWED: &[Reviewed] = &[
        // The devnet tool writes the test network's RPC into its manifest.
        (
            "src/bin/ant-devnet/main.rs",
            "resolve_evm_info",
            "rpc_url",
            1,
        ),
        // The chain ID for browser clients, read through payment_chain_id_retrying,
        // which falls back on the chain's other public RPCs.
        ("src/browser.rs", "browser_payment_network", "rpc_url", 1),
        // The local devnet's listing service, which builds its list from it.
        ("src/devnet.rs", "Devnet::create_ant_protocol", "rpc_url", 1),
        // The listing service, which builds its list from it (listings_rpc_urls).
        (
            "src/node.rs",
            "NodeBuilder::build_ant_protocol",
            "rpc_url",
            1,
        ),
        // The line a node logs at start, which names the RPC's host alone and
        // reads nothing through it.
        ("src/node.rs", "payment_settings", "rpc_url", 1),
        // The payment check's list: the node's RPC, then the chain's other public ones.
        (
            "src/payment/verifier.rs",
            "PaymentVerifier::network_rpc_urls",
            "rpc_url",
            1,
        ),
    ];
    let report = unreviewed(Counting::Calls(RPC), REVIEWED);
    assert!(
        report.is_empty(),
        "\nThe node's RPC address is taken somewhere new.\n\n{report}\n\
         Payments are read through PaymentVerifier::network_rpc_urls, the node's RPC and \
         then the chain's other public RPCs, and listings' payments through \
         listings_rpc_urls. A read through the configured RPC alone fails whenever that \
         provider is busy. Build the list from it instead, and list the call here.\n"
    );
}

// ---------------------------------------------------------------------------
// Wire messages
// ---------------------------------------------------------------------------

/// Every message of the vendored protocol's `ChunkMessageBody`, with what a
/// node does when one arrives (`AntProtocol::handle_message_with_context`,
/// src/storage/handler.rs).
const CHUNK_MESSAGES: &[(&str, &str)] = &[
    (
        "PutRequest",
        "Stores a chunk once paid per chunk; a merkle proof is refused.",
    ),
    (
        "PointerPutRequest",
        "Stores a pointer state once paid at its state, before any finality look; a merkle \
         proof is refused.",
    ),
    ("QuoteRequest", "A single-node quote."),
    (
        "QuoteRequestV2",
        "A single-node quote, after the settlement version check.",
    ),
    (
        "MerkleCandidateQuoteRequest",
        "Refused before anything is signed.",
    ),
    (
        "MerkleCandidateQuoteRequestV2",
        "Refused before anything is signed.",
    ),
    ("GetRequest", "A read: nothing is stored or paid."),
    ("PointerGetRequest", "A read: nothing is stored or paid."),
    ("PutResponse", "A response: dropped, never answered."),
    ("GetResponse", "A response: dropped, never answered."),
    ("QuoteResponse", "A response: dropped, never answered."),
    (
        "MerkleCandidateQuoteResponse",
        "A response: dropped, never answered.",
    ),
    ("PointerPutResponse", "A response: dropped, never answered."),
    ("PointerGetResponse", "A response: dropped, never answered."),
];

/// Every message of the vendored listings protocol's `ListingsBody`, with
/// what a node does when one arrives (`ListingService::handle_response` and
/// `ListingService::handle_message`, src/listings/service.rs). None stores a
/// record or takes a payment.
const LISTINGS_MESSAGES: &[(&str, &str)] = &[
    (
        "ListRequest",
        "A read of the listings index: nothing is stored or paid.",
    ),
    ("ListResponse", "A response: dropped, never answered."),
    (
        "GetRequest",
        "A read of one listing: nothing is stored or paid.",
    ),
    (
        "GetResponse",
        "Handed to the node's own request waiting for it, if any; never answered.",
    ),
    ("Error", "A response: dropped, never answered."),
    (
        "ChangesRequest",
        "A read of the listing change feed: nothing is stored or paid. It may set a watch, \
         kept in memory only and at most 4 at once, that pushes the feed's new records in \
         its range to the reader for at most 180 seconds.",
    ),
    (
        "ChangesResponse",
        "A response or a push: dropped, never answered.",
    ),
    (
        "RangeListRequest",
        "A read of the change feed's entries: nothing is stored or paid. A listing whose \
         chunk has gone is dropped, as a list request drops one.",
    ),
    ("RangeListResponse", "A response: dropped, never answered."),
];

/// Every kind of payment proof the vendored protocol tells apart
/// (`ProofType`), with what the payment check does with it
/// (`PaymentVerifier::verify_payment_inner`).
const PROOF_TYPES: &[(&str, &str)] = &[
    (
        "SingleNode",
        "Verified: the median quote, its issuer's closeness and its settlement on chain.",
    ),
    (
        "Merkle",
        "Refused in every context, for chunks and pointers alike.",
    ),
    (
        "Root",
        "Verified: the median quote, its issuer's closeness, and its leaf's place in a wave \
         the root vault recorded, read by the wave's key; refused where the node knows no \
         root vault.",
    ),
];

/// The functions that decide by a payment proof's type: `detect_proof_type`,
/// and each tag's decoder, which refuses every other tag. A place that calls
/// a decoder directly takes that one type alone, as the listing service once
/// took single-node proofs alone, so it is counted as a place that decides.
const PROOF_TYPE_DECIDERS: &[(Option<&str>, &str)] = &[
    (None, "detect_proof_type"),
    (None, "deserialize_single_node_proof"),
    (None, "deserialize_merkle_proof"),
    (None, "deserialize_root_proof"),
    (None, "deserialize_proof"),
];

/// Every place in production code that decides by a payment proof's type,
/// found by its mentions of [`PROOF_TYPE_DECIDERS`], with what each type does
/// there. Each decides with a `match` that names every type, so a new type
/// doesn't build until each place handles it; this list catches a new place.
const PROOF_TYPE_SITES: &[Reviewed] = &[
    // Re-exports of the vendored protocol's functions: no decision.
    ("src/payment/mod.rs", "<module>", "detect_proof_type", 1),
    (
        "src/payment/mod.rs",
        "<module>",
        "deserialize_merkle_proof",
        1,
    ),
    ("src/payment/mod.rs", "<module>", "deserialize_proof", 1),
    (
        "src/payment/mod.rs",
        "<module>",
        "deserialize_root_proof",
        1,
    ),
    ("src/payment/proof.rs", "<module>", "detect_proof_type", 1),
    (
        "src/payment/proof.rs",
        "<module>",
        "deserialize_merkle_proof",
        1,
    ),
    ("src/payment/proof.rs", "<module>", "deserialize_proof", 1),
    (
        "src/payment/proof.rs",
        "<module>",
        "deserialize_root_proof",
        1,
    ),
    (
        "src/payment/proof.rs",
        "<module>",
        "deserialize_single_node_proof",
        1,
    ),
    // The dispatch: single-node and root proofs are verified, each against
    // its own vault; merkle proofs are refused first; an unknown tag is
    // refused.
    (
        "src/payment/verifier.rs",
        "PaymentVerifier::verify_payment_inner",
        "detect_proof_type",
        1,
    ),
    (
        "src/payment/verifier.rs",
        "PaymentVerifier::verify_payment_inner",
        "deserialize_single_node_proof",
        1,
    ),
    (
        "src/payment/verifier.rs",
        "PaymentVerifier::verify_payment_inner",
        "deserialize_root_proof",
        1,
    ),
    // Upstream's merkle check, which the dispatch never reaches: merkle
    // proofs are refused first. Kept for cheap merges, and unit-tested.
    (
        "src/payment/verifier.rs",
        "PaymentVerifier::verify_merkle_payment",
        "deserialize_merkle_proof",
        1,
    ),
    // The metrics label: "single_node", "merkle", "root", else "unknown".
    (
        "src/payment/verifier.rs",
        "payment_proof_type_label",
        "detect_proof_type",
        1,
    ),
    (
        "src/payment/verifier.rs",
        "<module>",
        "detect_proof_type",
        1,
    ),
    (
        "src/payment/verifier.rs",
        "<module>",
        "deserialize_merkle_proof",
        1,
    ),
    (
        "src/payment/verifier.rs",
        "<module>",
        "deserialize_root_proof",
        1,
    ),
    (
        "src/payment/verifier.rs",
        "<module>",
        "deserialize_single_node_proof",
        1,
    ),
    // The sidecar strip before a stored chunk is offered to its close
    // group: single-node, merkle and root proofs alike lose their sidecars;
    // an unknown tag is forwarded unchanged.
    (
        "src/storage/handler.rs",
        "AntProtocol::strip_commitment_sidecars",
        "detect_proof_type",
        2,
    ),
    (
        "src/storage/handler.rs",
        "AntProtocol::strip_commitment_sidecars",
        "deserialize_single_node_proof",
        2,
    ),
    (
        "src/storage/handler.rs",
        "AntProtocol::strip_commitment_sidecars",
        "deserialize_merkle_proof",
        2,
    ),
    (
        "src/storage/handler.rs",
        "AntProtocol::strip_commitment_sidecars",
        "deserialize_root_proof",
        2,
    ),
    // Whether a fresh offer is root-paid, so a peer that refuses it is left
    // out of the key's possession check: root proofs only; single-node,
    // merkle and unknown tags are checked as before.
    (
        "src/replication/fresh.rs",
        "is_root_paid",
        "detect_proof_type",
        1,
    ),
    (
        "src/replication/fresh.rs",
        "<module>",
        "detect_proof_type",
        1,
    ),
    // An envelope's listing: found from a single-node or a root proof's
    // transactions; refused for a merkle proof or an unknown tag.
    (
        "src/listings/service.rs",
        "ListingService::find_payment",
        "detect_proof_type",
        1,
    ),
    (
        "src/listings/service.rs",
        "ListingService::find_payment",
        "deserialize_single_node_proof",
        1,
    ),
    (
        "src/listings/service.rs",
        "ListingService::find_payment",
        "deserialize_root_proof",
        1,
    ),
    (
        "src/listings/service.rs",
        "<module>",
        "detect_proof_type",
        1,
    ),
    (
        "src/listings/service.rs",
        "<module>",
        "deserialize_single_node_proof",
        1,
    ),
    (
        "src/listings/service.rs",
        "<module>",
        "deserialize_root_proof",
        1,
    ),
];

/// The variants of the enum called `name` in `source`, in the order they are
/// declared; empty if there is no such enum.
fn enum_variants(source: &str, name: &str) -> Vec<String> {
    let tokens = tokenize(source);
    let tok = |pos: usize| tokens.get(pos).map(|token| &token.tok);
    let declared = (0..tokens.len()).find(|&pos| {
        matches!(tok(pos), Some(Tok::Ident(word)) if word == "enum")
            && matches!(tok(pos + 1), Some(Tok::Ident(word)) if word == name)
    });
    let Some(open) =
        declared.and_then(|at| (at..tokens.len()).find(|&pos| tok(pos) == Some(&Tok::Punct('{'))))
    else {
        return Vec::new();
    };
    let close = matching_close(&tokens, open);
    let mut variants = Vec::new();
    let mut at_variant = true;
    let mut pos = open + 1;
    while pos < close {
        match tok(pos) {
            // An attribute on a variant.
            Some(Tok::Punct('#')) if tok(pos + 1) == Some(&Tok::Punct('[')) => {
                pos = matching_close(&tokens, pos + 1);
            }
            // A variant's fields.
            Some(Tok::Punct('(' | '[' | '{')) => pos = matching_close(&tokens, pos),
            Some(Tok::Punct(',')) => at_variant = true,
            Some(Tok::Ident(variant)) if at_variant => {
                variants.push(variant.clone());
                at_variant = false;
            }
            _ => {}
        }
        pos += 1;
    }
    variants
}

/// Every message the chunk protocol carries, and every kind of payment proof,
/// is one whose handling has been reviewed. The chunk messages are
/// `#[non_exhaustive]` in the vendored protocol, so the compiler says nothing
/// when a new version of it adds a variant, or when a merge adds the handling
/// of one; this does. The proof types are not, so every `match` on them names
/// each one; this keeps their list reviewed all the same.
#[test]
fn the_wire_messages_are_the_reviewed_ones() {
    let vendor = Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor");
    for (file, name, reviewed) in [
        (
            "ant-protocol/src/chunk.rs",
            "ChunkMessageBody",
            CHUNK_MESSAGES,
        ),
        (
            "ant-protocol/src/payment/proof.rs",
            "ProofType",
            PROOF_TYPES,
        ),
        (
            "ant-listings/src/message.rs",
            "ListingsBody",
            LISTINGS_MESSAGES,
        ),
    ] {
        let source = std::fs::read_to_string(vendor.join(file)).unwrap_or_default();
        let declared = enum_variants(&source, name);
        let declared_set: BTreeSet<&str> = declared.iter().map(String::as_str).collect();
        let listed: BTreeSet<&str> = reviewed.iter().map(|(variant, _)| *variant).collect();
        let not_reviewed: Vec<&str> = declared_set.difference(&listed).copied().collect();
        let gone: Vec<&str> = listed.difference(&declared_set).copied().collect();
        let without_reason: Vec<&str> = reviewed
            .iter()
            .filter(|(_, reason)| reason.trim().is_empty())
            .map(|(variant, _)| *variant)
            .collect();
        assert!(
            !declared.is_empty()
                && not_reviewed.is_empty()
                && gone.is_empty()
                && without_reason.is_empty(),
            "\n{name} in vendor/{file} no longer matches the reviewed list \
             in src/replication/retention/census.rs.\n\n  declared: {declared:?}\n  \
             not reviewed: {not_reviewed:?}\n  listed but gone: {gone:?}\n  \
             listed without a reason: {without_reason:?}\n\n\
             Nodes take single-node and root payments, and refuse merkle (batch) payments at \
             quote time and on any proof. Before listing a new message or proof type, make \
             sure the node's handler (src/storage/handler.rs) and payment check \
             (src/payment/verifier.rs) take it through those rules, with tests that pin it; \
             until then, refuse it. A listings message is answered, or dropped, by \
             src/listings/service.rs: list what it does there.\n"
        );
    }
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
            fn free(cb: fn(u8) -> u8) { delete_mark_last(dir); }
            trait Eraser { fn wipe(&self) { std::fs::remove_file(p); } fn plain(&self); }
        ";
        assert_eq!(
            calls(source),
            vec![
                one("Eraser::wipe", "remove_file"),
                one("Guard::drop", "remove_dir_all"),
                one("Store::erase", "delete"),
                one("Wrapper::fmt", "delete"),
                one("free", "delete_mark_last"),
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
            fn f(config: &Config) { if config.delete_mark_last { } }
        ";
        assert!(calls(source).is_empty(), "{:?}", calls(source));
    }

    /// A verifying read that can remove a chunk file is counted where it is called,
    /// like any other eraser; the plain verifying read, which never removes one, is not.
    #[test]
    fn a_read_that_can_remove_a_file_is_counted_and_a_plain_read_is_not() {
        let source = r"
            impl ChunkStore {
                pub async fn get(&self, key: &K) -> R { self.files.get(key).await }
                pub async fn get_or_quarantine(&self, key: &K) -> R {
                    self.files.get_or_quarantine(key).await
                }
            }
            async fn serve(store: &ChunkStore, key: &K) -> R {
                store.get_or_quarantine(key).await
            }
            async fn look(store: &ChunkStore, key: &K) -> R { store.get(key).await }
        ";
        assert_eq!(
            calls(source),
            vec![
                one("ChunkStore::get_or_quarantine", "get_or_quarantine"),
                one("serve", "get_or_quarantine"),
            ]
        );
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

    /// A mention is counted whether or not it is called: an import, a type, a
    /// path. A name ending in `*` stands for every name it starts, after the
    /// owner its entry asks for; a definition is no mention.
    #[test]
    fn mentions_are_counted_called_or_not_and_a_star_matches_a_prefix() {
        const WANTED: &[(Option<&str>, &str)] =
            &[(None, "IPaymentVault"), (Some("payment_vault"), "get_*")];
        let source = r"
            use evmlib::contract::payment_vault::interface::IPaymentVault;
            fn read(p: P) {
                IPaymentVault::new(vault, p).completedPayments(h);
                payment_vault::get_completed_merkle_payment(p, h);
                payment_vault::get_market_price(p);
                other::get_completed_merkle_payment(p, h);
                payment_vault::handler(p);
            }
            fn IPaymentVault() {}
            fn get_everything() {}
        ";
        let found: Vec<(String, String, usize)> = scan_counting(source, Counting::Mentions(WANTED))
            .calls
            .into_iter()
            .map(|((site, call), lines)| (site, call, lines.len()))
            .collect();
        assert_eq!(
            found,
            vec![
                one("<module>", "IPaymentVault"),
                one("read", "IPaymentVault"),
                one("read", "payment_vault::get_completed_merkle_payment"),
                one("read", "payment_vault::get_market_price"),
            ]
        );

        // As calls, an import is not counted, and the prefix still applies.
        let called: Vec<(String, String, usize)> = scan_counting(source, Counting::Calls(WANTED))
            .calls
            .into_iter()
            .map(|((site, call), lines)| (site, call, lines.len()))
            .collect();
        assert_eq!(
            called,
            vec![
                one("read", "payment_vault::get_completed_merkle_payment"),
                one("read", "payment_vault::get_market_price"),
            ]
        );
    }

    #[test]
    fn the_variants_of_an_enum_are_read_in_order() {
        let source = r#"
            /// Not this one, though it comes first.
            enum Other { Wrong, AlsoWrong(u8) }
            #[derive(Debug)]
            #[non_exhaustive]
            pub enum Body<T> {
                /// A doc comment, then a tuple variant.
                Put(Request<T>),
                #[serde(rename = "fetch")]
                Get { address: [u8; 32], data: Vec<(u8, u8)> },
                // A comment: Ignored,
                Unit,
                Numbered = 7,
            }
        "#;
        assert_eq!(
            enum_variants(source, "Body"),
            vec!["Put", "Get", "Unit", "Numbered"]
        );
        assert_eq!(enum_variants(source, "Other"), vec!["Wrong", "AlsoWrong"]);
        assert!(enum_variants(source, "Missing").is_empty());
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

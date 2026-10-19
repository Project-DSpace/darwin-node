//! Persistent listings, backed by LMDB.
//!
//! ```text
//! {root}/listings.mdb/   -- LMDB environment directory
//! ```
//!
//! Nine databases:
//!
//! - `index`: listings in the index, by [`ListingKey::to_bytes`] (time, block
//!   number, log index, address, big-endian), so LMDB's byte order is the
//!   index order. List requests read only this one.
//! - `index_addresses`: the address of each listing in `index`, to its key.
//! - `unindexed`: listings outside the index, by address.
//! - `quote_keys`: the public keys of the quotes listings carry, each stored
//!   once under its BLAKE3 hash with a count of the records using it. A record
//!   holds the hash in place of the key, and the full quote is rebuilt, byte for
//!   byte, when the listing is read.
//! - `provisional`: the addresses whose listing was taken by repair without a
//!   quorum of the close group behind it. Such a listing is kept but not
//!   served until it is settled.
//! - `pending`: the envelopes whose listing is still being checked with the
//!   close group, so the checks go on after a restart. Once checks have gone
//!   on past their schedule as re-checks, the mark holds when those began
//!   (Unix seconds, 8 bytes, big-endian), so a restart keeps their end.
//! - `changes`: the change feed's records by number (big-endian), each the
//!   address it records.
//! - `change_of`: each address's record in the change feed: its number, and
//!   the digest entry of the listing served there or the address's removal.
//! - `change_meta`: the change feed's epoch and the number its next record
//!   takes, and the id of the last write transaction this version made.
//!
//! The change feed numbers the changes to the listings the node serves in the
//! index, the ones list requests return (in the index and not provisional),
//! in the same transaction as each change. It keeps one record per address,
//! the latest change to it, so a removal needs no expiry. Records are numbered
//! from 1 within an epoch, a random number other than 0 that is new each time
//! the served listings are numbered afresh: when the store is first opened
//! with the feed, when that numbering was cut short, and when another version
//! has written to the store since this one last did, as after a downgrade and
//! an upgrade back. Such a version keeps no feed. LMDB's last transaction id
//! tells: each write transaction of this version records its own. A start
//! that can't number them, as on a full disk, leaves them to the next start:
//! until then the feed can't be read, and no write records its id. The count
//! and set hash of the feed's entries are kept in memory per address prefix
//! of 16 bits, rebuilt at each start, so a range's totals are read without
//! reading its entries. Each write that adds records is handed, as it
//! commits, to the feed's watcher ([`ListingStore::watch_feed`]), which
//! pushes them to the readers watching their range.
//!
//! The map is sized from the disk, as the chunk store's is, leaving the
//! operator's disk reserve free, and grows when it fills. A store written by
//! version 1 (`by_key` and `by_address`) is moved into these databases when it
//! is opened, a batch at a time; what can't be moved then, as on a full disk,
//! is moved at a later start.

use super::map_size::compute_map_size;
use crate::ant_protocol::XorName;
use crate::error::{Error, Result};
use crate::logging::{debug, info, warn};
use ::ant_protocol::evm::PaymentQuote;
use ant_listings::{
    AddressRange, DigestEntry, FeedChange, Flags, Listing, ListingKey, ListingPayment, ListingV1,
    Metadata, SetHash,
};
use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions, MdbError, RoTxn, RwTxn};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::ops::{AddAssign, Bound};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::task::spawn_blocking;

/// First byte of every stored record: the format below.
const RECORD_VERSION: u8 = 2;

/// Names of the version 1 databases, read once to migrate them.
const V1_BY_KEY: &str = "by_key";
const V1_BY_ADDRESS: &str = "by_address";

/// The most named databases the environment opens: the nine above, and
/// version 1's two, which every start opens to move what they hold.
const MAX_DBS: u32 = 11;

/// How many version 1 listings one write transaction moves. A transaction
/// can't reuse the pages the one before it freed, so moving needs room for
/// about two batches of new records beyond the version 1 data: about 2 MiB.
const V1_BATCH: usize = 256;

/// First byte of each `change_of` record and of the change feed's state: the
/// format below.
const FEED_VERSION: u8 = 1;

/// The keys of `change_meta`: the change feed's state, and the id of the last
/// write transaction this version made.
const FEED_STATE: &[u8] = b"feed";
const LAST_WRITE: &[u8] = b"last_write";

/// How many listings one write transaction numbers when the change feed is
/// numbered afresh, as many as moving version 1 listings takes at once.
const FEED_BATCH: usize = V1_BATCH;

/// The listings this node holds: those in the index by key, the others by
/// address.
#[derive(Clone)]
pub struct ListingStore {
    env: Env,
    dir: PathBuf,
    /// Bytes of disk the map never grows into.
    disk_reserve: u64,
    /// Held shared by every transaction and exclusively to grow the map, which
    /// LMDB allows only while no transaction is open.
    env_lock: Arc<RwLock<()>>,
    index: Database<Bytes, Bytes>,
    index_addresses: Database<Bytes, Bytes>,
    unindexed: Database<Bytes, Bytes>,
    quote_keys: Database<Bytes, Bytes>,
    provisional: Database<Bytes, Bytes>,
    pending: Database<Bytes, Bytes>,
    changes: Database<Bytes, Bytes>,
    change_of: Database<Bytes, Bytes>,
    change_meta: Database<Bytes, Bytes>,
    /// The totals of the change feed's entries per address prefix. Each write
    /// commits, and applies what it added to the feed to them, under this
    /// lock, and a read of the feed takes its snapshot and reads the totals
    /// under it too, so the two always agree.
    sums: Arc<Mutex<FeedSums>>,
    /// Set at a start that found the change feed needing to be numbered
    /// afresh and couldn't number it, as on a full disk. Until the next
    /// start the feed can't be read, records nothing, and no write is
    /// recorded as this version's last, so the next start numbers it afresh
    /// whatever is written meanwhile.
    feed_behind: Arc<AtomicBool>,
    /// Told of each write that adds records to the change feed, as it
    /// commits: see [`Self::watch_feed`].
    feed_watcher: Arc<OnceLock<FeedWatcher>>,
    /// Tests: every write fails while set, as when the disk has no room.
    #[cfg(test)]
    failing: Arc<std::sync::atomic::AtomicBool>,
    /// Tests: the most bytes the map grows to, as if the disk had no room
    /// beyond (0: as the disk allows).
    #[cfg(test)]
    map_ceiling: Arc<std::sync::atomic::AtomicUsize>,
}

/// A failure inside a write transaction: LMDB's own, so a full map can be
/// told apart, or anything else.
enum TxnError {
    Lmdb(heed::Error),
    Other(Error),
}

impl From<heed::Error> for TxnError {
    fn from(e: heed::Error) -> Self {
        Self::Lmdb(e)
    }
}

impl From<Error> for TxnError {
    fn from(e: Error) -> Self {
        Self::Other(e)
    }
}

impl ListingStore {
    /// Open or create the store at `{root_dir}/listings.mdb/`, numbering the
    /// served listings in the change feed if it needs it and moving any
    /// version 1 listings into it. Its map never grows into the last
    /// `disk_reserve` bytes of the disk, as the chunk store's doesn't.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB environment or its databases can't be
    /// opened or created. Version 1 listings that can't be moved now, as on
    /// a full disk, are no error: they are kept to be moved at the next
    /// start. Nor is a change feed that can't be numbered now: it is
    /// numbered at the next start, and can't be read until then.
    pub async fn open(root_dir: &Path, disk_reserve: u64) -> Result<Self> {
        Self::open_with_map_size(root_dir, disk_reserve, None).await
    }

    /// [`Self::open`], with the map at `map_size` bytes instead of sized from
    /// the disk. It still grows when it fills.
    async fn open_with_map_size(
        root_dir: &Path,
        disk_reserve: u64,
        map_size: Option<usize>,
    ) -> Result<Self> {
        let dir = root_dir.join("listings.mdb");
        std::fs::create_dir_all(&dir)
            .map_err(|e| Error::Storage(format!("Failed to create listings directory: {e}")))?;
        spawn_blocking(move || -> Result<Self> {
            let store = Self::open_env(dir, disk_reserve, map_size)?;
            // Numbered first, so the listings moved next are recorded as
            // they move.
            store.number_feed_or_wait();
            store.migrate_v1_or_keep();
            store.load_sums_or_wait();
            Ok(store)
        })
        .await
        .map_err(|e| Error::Storage(format!("Listings init task failed: {e}")))?
    }

    /// Open or create the LMDB environment in `dir` and its databases, the
    /// map at `map_size` bytes or sized from the disk.
    #[allow(unsafe_code)]
    fn open_env(dir: PathBuf, disk_reserve: u64, map_size: Option<usize>) -> Result<Self> {
        let map_size = match map_size {
            Some(size) => size,
            None => compute_map_size(&dir, disk_reserve)?,
        };
        // SAFETY: `EnvOpenOptions::open()` is unsafe because LMDB relies on
        // file locking to stop two processes mapping the same environment.
        // As for the paid list, each node has its own `root_dir`, so no two
        // processes open this one.
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(map_size)
                .max_dbs(MAX_DBS)
                .open(&dir)
                .map_err(|e| Error::Storage(format!("Failed to open listings LMDB env: {e}")))?
        };
        let mut wtxn = env.write_txn().map_err(storage_error)?;
        let index = env
            .create_database(&mut wtxn, Some("index"))
            .map_err(storage_error)?;
        let index_addresses = env
            .create_database(&mut wtxn, Some("index_addresses"))
            .map_err(storage_error)?;
        let unindexed = env
            .create_database(&mut wtxn, Some("unindexed"))
            .map_err(storage_error)?;
        let quote_keys = env
            .create_database(&mut wtxn, Some("quote_keys"))
            .map_err(storage_error)?;
        let provisional = env
            .create_database(&mut wtxn, Some("provisional"))
            .map_err(storage_error)?;
        let pending = env
            .create_database(&mut wtxn, Some("pending"))
            .map_err(storage_error)?;
        let changes = env
            .create_database(&mut wtxn, Some("changes"))
            .map_err(storage_error)?;
        let change_of = env
            .create_database(&mut wtxn, Some("change_of"))
            .map_err(storage_error)?;
        let change_meta = env
            .create_database(&mut wtxn, Some("change_meta"))
            .map_err(storage_error)?;
        wtxn.commit().map_err(storage_error)?;
        Ok(Self {
            env,
            dir,
            disk_reserve,
            env_lock: Arc::new(RwLock::new(())),
            index,
            index_addresses,
            unindexed,
            quote_keys,
            provisional,
            pending,
            changes,
            change_of,
            change_meta,
            sums: Arc::new(Mutex::new(FeedSums::new())),
            feed_behind: Arc::default(),
            feed_watcher: Arc::default(),
            #[cfg(test)]
            failing: Arc::default(),
            #[cfg(test)]
            map_ceiling: Arc::default(),
        })
    }

    /// Add a listing: to the index if its flags say so, otherwise to the
    /// listings outside it. Returns `false`, changing nothing, if the address
    /// already has a listing: the first one stands.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding or the LMDB write fails.
    pub async fn insert(&self, listing: Listing) -> Result<bool> {
        let store = self.clone();
        spawn_blocking(move || store.insert_blocking(&listing, false))
            .await
            .map_err(join_error)?
    }

    /// [`Self::insert`], marking the listing provisional: kept, but neither
    /// listed nor returned by [`Self::served`] until it is replaced by a
    /// settled one or [`Self::settle`]d.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding or the LMDB write fails.
    pub async fn insert_provisional(&self, listing: Listing) -> Result<bool> {
        let store = self.clone();
        spawn_blocking(move || store.insert_blocking(&listing, true))
            .await
            .map_err(join_error)?
    }

    /// Replace the listing held for `listing.address` with `listing`,
    /// provisional or settled. Returns `false`, changing nothing, if the
    /// address has no listing (any more): a listing dropped with its chunk is
    /// not brought back.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding or the LMDB write fails.
    pub async fn replace(&self, listing: Listing, provisional: bool) -> Result<bool> {
        let store = self.clone();
        spawn_blocking(move || {
            let record = StoredRecord::new(&listing);
            store.write(|wtxn, feed| {
                if !store.remove_in(wtxn, &listing.address)? {
                    return Ok(false);
                }
                store.insert_in(wtxn, &listing, &record)?;
                if provisional {
                    store.provisional.put(wtxn, &listing.address, &[])?;
                }
                store.note_change(wtxn, feed, &listing.address)?;
                Ok(true)
            })
        })
        .await
        .map_err(join_error)?
    }

    /// Settle the provisional listing of `address`: it is listed and served
    /// from now on. Returns whether it was provisional.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB write fails.
    pub async fn settle(&self, address: XorName) -> Result<bool> {
        if !self.is_provisional(address).await? {
            return Ok(false);
        }
        let store = self.clone();
        spawn_blocking(move || {
            store.write(|wtxn, feed| {
                let settled = store.provisional.delete(wtxn, &address)?;
                if settled {
                    store.note_change(wtxn, feed, &address)?;
                }
                Ok(settled)
            })
        })
        .await
        .map_err(join_error)?
    }

    /// Whether the listing of `address` is provisional.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    pub async fn is_provisional(&self, address: XorName) -> Result<bool> {
        let store = self.clone();
        spawn_blocking(move || {
            let _shared = store.env_lock.read();
            let rtxn = store.env.read_txn().map_err(storage_error)?;
            let marked = store
                .provisional
                .get(&rtxn, &address)
                .map_err(storage_error)?;
            Ok(marked.is_some())
        })
        .await
        .map_err(join_error)?
    }

    /// Every address whose listing is provisional.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    pub async fn provisional_addresses(&self) -> Result<Vec<XorName>> {
        self.addresses_in(self.provisional).await
    }

    /// Mark the envelope at `address` as having its listing checked with the
    /// close group, until [`Self::clear_pending`]. Marks are kept across
    /// restarts.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB write fails.
    pub async fn mark_pending(&self, address: XorName) -> Result<()> {
        let store = self.clone();
        spawn_blocking(move || store.write(|wtxn, _| Ok(store.pending.put(wtxn, &address, &[])?)))
            .await
            .map_err(join_error)?
    }

    /// Clear the mark [`Self::mark_pending`] set.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB write fails.
    pub async fn clear_pending(&self, address: XorName) -> Result<()> {
        let store = self.clone();
        spawn_blocking(move || {
            store.write(|wtxn, _| {
                store.pending.delete(wtxn, &address)?;
                Ok(())
            })
        })
        .await
        .map_err(join_error)?
    }

    /// Mark the envelope at `address` as checked again past its schedule
    /// since `since` (Unix seconds), so that a restart keeps the end of its
    /// re-checks. Marked pending until [`Self::clear_pending`], as with
    /// [`Self::mark_pending`].
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB write fails.
    pub async fn mark_rechecking(&self, address: XorName, since: u64) -> Result<()> {
        let store = self.clone();
        spawn_blocking(move || {
            store.write(|wtxn, _| Ok(store.pending.put(wtxn, &address, &since.to_be_bytes())?))
        })
        .await
        .map_err(join_error)?
    }

    /// Every envelope marked pending.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    #[cfg(test)]
    pub async fn pending_addresses(&self) -> Result<Vec<XorName>> {
        self.addresses_in(self.pending).await
    }

    /// Every envelope marked pending, with when its re-checks began
    /// ([`Self::mark_rechecking`]), if they have.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    pub async fn pending_checks(&self) -> Result<Vec<(XorName, Option<u64>)>> {
        let store = self.clone();
        spawn_blocking(move || {
            let _shared = store.env_lock.read();
            let rtxn = store.env.read_txn().map_err(storage_error)?;
            let mut checks = Vec::new();
            for entry in store.pending.iter(&rtxn).map_err(storage_error)? {
                let (key, value) = entry.map_err(storage_error)?;
                if let Ok(address) = XorName::try_from(key) {
                    let since = <[u8; 8]>::try_from(value).ok().map(u64::from_be_bytes);
                    checks.push((address, since));
                }
            }
            Ok(checks)
        })
        .await
        .map_err(join_error)?
    }

    /// Every address in `table`, one of the tables of marks.
    async fn addresses_in(&self, table: Database<Bytes, Bytes>) -> Result<Vec<XorName>> {
        let store = self.clone();
        spawn_blocking(move || {
            let _shared = store.env_lock.read();
            let rtxn = store.env.read_txn().map_err(storage_error)?;
            let mut addresses = Vec::new();
            for entry in table.iter(&rtxn).map_err(storage_error)? {
                let (key, _) = entry.map_err(storage_error)?;
                if let Ok(address) = XorName::try_from(key) {
                    addresses.push(address);
                }
            }
            Ok(addresses)
        })
        .await
        .map_err(join_error)?
    }

    /// Up to `limit` addresses with a listing, in the index or not, in
    /// ascending order after `after` (from the first if `None`). Reads no
    /// records, so a record that doesn't decode is still found.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    pub async fn addresses_after(
        &self,
        after: Option<XorName>,
        limit: usize,
    ) -> Result<Vec<XorName>> {
        let store = self.clone();
        spawn_blocking(move || {
            let _shared = store.env_lock.read();
            let rtxn = store.env.read_txn().map_err(storage_error)?;
            let start = after
                .as_ref()
                .map_or(Bound::Unbounded, |after| Bound::Excluded(&after[..]));
            let bounds: (Bound<&[u8]>, Bound<&[u8]>) = (start, Bound::Unbounded);
            // Both tables are keyed by address: the first `limit` of each hold
            // the first `limit` of both.
            let mut addresses = Vec::new();
            for table in [store.index_addresses, store.unindexed] {
                for entry in table
                    .range(&rtxn, &bounds)
                    .map_err(storage_error)?
                    .take(limit)
                {
                    let (key, _) = entry.map_err(storage_error)?;
                    if let Ok(address) = XorName::try_from(key) {
                        addresses.push(address);
                    }
                }
            }
            addresses.sort_unstable();
            addresses.dedup();
            addresses.truncate(limit);
            Ok(addresses)
        })
        .await
        .map_err(join_error)?
    }

    /// The listing for `address`, in the index or not, if held, provisional
    /// or not. A record that doesn't decode reads as none.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    #[cfg(test)]
    pub async fn get(&self, address: XorName) -> Result<Option<Listing>> {
        let store = self.clone();
        spawn_blocking(move || store.get_blocking(&address, true))
            .await
            .map_err(join_error)?
    }

    /// [`Self::get`], but `None` for a provisional listing: the listing to
    /// give other nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    pub async fn served(&self, address: XorName) -> Result<Option<Listing>> {
        let store = self.clone();
        spawn_blocking(move || store.get_blocking(&address, false))
            .await
            .map_err(join_error)?
    }

    /// Whether `address` has a listing, in the index or not.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    pub async fn contains(&self, address: XorName) -> Result<bool> {
        let store = self.clone();
        spawn_blocking(move || store.contains_blocking(&address))
            .await
            .map_err(join_error)?
    }

    /// Remove the listing for `address`. Returns whether there was one.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB write fails.
    pub async fn remove(&self, address: XorName) -> Result<bool> {
        let store = self.clone();
        spawn_blocking(move || store.remove_blocking(&address))
            .await
            .map_err(join_error)?
    }

    /// Up to `limit` listings in the index with keys from `from` onwards, in
    /// key order, and whether later ones exist. Listings outside the index are
    /// never returned, nor are provisional ones or records that don't decode:
    /// they are skipped without counting towards `limit`.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    pub async fn range(&self, from: ListingKey, limit: usize) -> Result<(Vec<Listing>, bool)> {
        let store = self.clone();
        spawn_blocking(move || store.range_blocking(from, limit))
            .await
            .map_err(join_error)?
    }

    fn insert_blocking(&self, listing: &Listing, provisional: bool) -> Result<bool> {
        let record = StoredRecord::new(listing);
        self.write(|wtxn, feed| {
            if self.contains_in(wtxn, &listing.address)? {
                return Ok(false);
            }
            self.insert_in(wtxn, listing, &record)?;
            if provisional {
                self.provisional.put(wtxn, &listing.address, &[])?;
            }
            self.note_change(wtxn, feed, &listing.address)?;
            Ok(true)
        })
    }

    fn get_blocking(&self, address: &XorName, provisional_too: bool) -> Result<Option<Listing>> {
        let _shared = self.env_lock.read();
        let rtxn = self.env.read_txn().map_err(storage_error)?;
        if !provisional_too
            && self
                .provisional
                .get(&rtxn, address)
                .map_err(storage_error)?
                .is_some()
        {
            return Ok(None);
        }
        let value = match self
            .index_addresses
            .get(&rtxn, address)
            .map_err(storage_error)?
        {
            Some(key) => self.index.get(&rtxn, key).map_err(storage_error)?,
            None => self.unindexed.get(&rtxn, address).map_err(storage_error)?,
        };
        let Some(value) = value else {
            return Ok(None);
        };
        match self.decode(&rtxn, value) {
            Ok(listing) => Ok(Some(listing)),
            Err(e) => {
                warn!(
                    "The listing record of {} doesn't decode: {e}",
                    hex::encode(address)
                );
                Ok(None)
            }
        }
    }

    fn contains_blocking(&self, address: &XorName) -> Result<bool> {
        let _shared = self.env_lock.read();
        let rtxn = self.env.read_txn().map_err(storage_error)?;
        self.contains_in(&rtxn, address).map_err(storage_error)
    }

    fn remove_blocking(&self, address: &XorName) -> Result<bool> {
        // Most removals are for chunks that never had a listing: answer those
        // from a read, without a write transaction.
        if !self.contains_blocking(address)? {
            return Ok(false);
        }
        self.write(|wtxn, feed| {
            let removed = self.remove_in(wtxn, address)?;
            if removed {
                self.note_change(wtxn, feed, address)?;
            }
            Ok(removed)
        })
    }

    fn range_blocking(&self, from: ListingKey, limit: usize) -> Result<(Vec<Listing>, bool)> {
        let _shared = self.env_lock.read();
        let rtxn = self.env.read_txn().map_err(storage_error)?;
        let from = from.to_bytes();
        let bounds: (Bound<&[u8]>, Bound<&[u8]>) = (Bound::Included(&from[..]), Bound::Unbounded);
        let mut listings = Vec::new();
        let mut more = false;
        let mut undecodable = 0usize;
        for entry in self.index.range(&rtxn, &bounds).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            if listings.len() == limit {
                more = true;
                break;
            }
            // The address ends the key.
            let address = key.get(key.len().saturating_sub(32)..).unwrap_or_default();
            if self
                .provisional
                .get(&rtxn, address)
                .map_err(storage_error)?
                .is_some()
            {
                continue;
            }
            // One bad record must not stop every page from reaching past it.
            match self.decode(&rtxn, value) {
                Ok(listing) => listings.push(listing),
                Err(e) => {
                    undecodable += 1;
                    debug!(
                        "Skipping the listing record of {}: {e}",
                        hex::encode(address)
                    );
                }
            }
        }
        if undecodable > 0 {
            warn!("Skipped {undecodable} listing record(s) that don't decode");
        }
        Ok((listings, more))
    }

    fn contains_in(&self, txn: &RoTxn, address: &XorName) -> heed::Result<bool> {
        Ok(self.index_addresses.get(txn, address)?.is_some()
            || self.unindexed.get(txn, address)?.is_some())
    }

    fn insert_in(
        &self,
        wtxn: &mut RwTxn,
        listing: &Listing,
        record: &StoredRecord,
    ) -> std::result::Result<(), TxnError> {
        if let Some((hash, pub_key)) = &record.quote_key {
            self.add_key_reference(wtxn, hash, pub_key)?;
        }
        let value = record.encode()?;
        if listing.flags.in_index {
            let key = listing.key().to_bytes();
            self.index.put(wtxn, &key, &value)?;
            self.index_addresses.put(wtxn, &listing.address, &key)?;
        } else {
            self.unindexed.put(wtxn, &listing.address, &value)?;
        }
        Ok(())
    }

    /// Remove the listing for `address`, in the index or not, its reference
    /// to its quote's key and its marks. Returns whether there was one.
    fn remove_in(
        &self,
        wtxn: &mut RwTxn,
        address: &XorName,
    ) -> std::result::Result<bool, TxnError> {
        let index_key = self.index_addresses.get(wtxn, address)?.map(<[u8]>::to_vec);
        let value = if let Some(key) = index_key {
            let value = self.index.get(wtxn, &key)?.map(<[u8]>::to_vec);
            self.index.delete(wtxn, &key)?;
            self.index_addresses.delete(wtxn, address)?;
            value
        } else {
            let value = self.unindexed.get(wtxn, address)?.map(<[u8]>::to_vec);
            self.unindexed.delete(wtxn, address)?;
            value
        };
        let Some(value) = value else {
            return Ok(false);
        };
        self.provisional.delete(wtxn, address)?;
        self.pending.delete(wtxn, address)?;
        // A record that doesn't decode is still removed; only its key's count
        // is left as it was.
        if let Some(hash) = StoredRecord::decode(&value)
            .ok()
            .and_then(|r| r.key_reference())
        {
            self.drop_key_reference(wtxn, &hash)?;
        }
        Ok(true)
    }

    /// Count one more record using the key with this hash, storing the key
    /// if it is new.
    fn add_key_reference(
        &self,
        wtxn: &mut RwTxn,
        hash: &[u8; 32],
        pub_key: &[u8],
    ) -> std::result::Result<(), TxnError> {
        let count = self
            .quote_keys
            .get(wtxn, hash)?
            .and_then(key_count)
            .unwrap_or(0);
        let mut value = Vec::with_capacity(8 + pub_key.len());
        value.extend_from_slice(&count.saturating_add(1).to_be_bytes());
        value.extend_from_slice(pub_key);
        self.quote_keys.put(wtxn, hash, &value)?;
        Ok(())
    }

    /// Count one record fewer using the key with this hash, removing the key
    /// when none is left.
    fn drop_key_reference(&self, wtxn: &mut RwTxn, hash: &[u8; 32]) -> heed::Result<()> {
        let Some(value) = self.quote_keys.get(wtxn, hash)?.map(<[u8]>::to_vec) else {
            return Ok(());
        };
        match key_count(&value) {
            Some(count) if count > 1 => {
                let mut value = value;
                value[..8].copy_from_slice(&(count - 1).to_be_bytes());
                self.quote_keys.put(wtxn, hash, &value)?;
            }
            _ => {
                self.quote_keys.delete(wtxn, hash)?;
            }
        }
        Ok(())
    }

    /// The listing a stored record describes, with its quote rebuilt.
    fn decode(&self, txn: &RoTxn, value: &[u8]) -> Result<Listing> {
        let record = StoredRecord::decode(value)?;
        let quote = match record.quote {
            None => None,
            Some(StoredQuote::Verbatim(bytes)) => Some(bytes),
            Some(StoredQuote::Keyed {
                key,
                signature,
                quote,
            }) => {
                let pub_key = self
                    .quote_keys
                    .get(txn, &key)
                    .map_err(storage_error)?
                    .and_then(|value| value.get(8..))
                    .ok_or_else(|| {
                        Error::Storage("listings: a quote's public key is missing".into())
                    })?;
                Some(
                    join_quote(&quote, pub_key.to_vec(), signature).ok_or_else(|| {
                        Error::Serialization("bad listing: the quote cannot be rebuilt".into())
                    })?,
                )
            }
        };
        Ok(Listing {
            address: record.address,
            flags: record.flags,
            metadata: record.metadata,
            size: record.size,
            payment: ListingPayment {
                tx_hash: record.tx_hash,
                block_number: record.block_number,
                block_timestamp: record.block_timestamp,
                log_index: record.log_index,
                quote,
            },
            first_seen: record.first_seen,
        })
    }

    /// Run `op` in a write transaction and commit it, growing the map and
    /// trying again whenever it is full and could be grown. `op` records in
    /// the [`FeedWrites`] it is given what it adds to the change feed, which
    /// the feed's totals take on once the transaction commits, and the
    /// feed's watcher is told of. Every transaction also records its own id
    /// as this version's last write, unless the feed is behind the store
    /// ([`Self::feed_behind`]).
    fn write<T>(
        &self,
        op: impl Fn(&mut RwTxn, &mut FeedWrites) -> std::result::Result<T, TxnError>,
    ) -> Result<T> {
        #[cfg(test)]
        if self.failing.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::Storage("listings: writes are failing".into()));
        }
        loop {
            let (seen, outcome) = {
                let _shared = self.env_lock.read();
                let seen = self.env.info().map_size;
                let outcome = self
                    .env
                    .write_txn()
                    .map_err(TxnError::from)
                    .and_then(|mut wtxn| {
                        let mut feed = FeedWrites::default();
                        let value = op(&mut wtxn, &mut feed)?;
                        if !self.feed_behind.load(Ordering::Relaxed) {
                            let id = txn_id(&wtxn);
                            self.change_meta
                                .put(&mut wtxn, LAST_WRITE, &id.to_be_bytes())?;
                        }
                        // Readers of the feed take their snapshot and read
                        // the totals under this lock: they see both from
                        // before the commit, or both from after it. So does
                        // the watcher, which nothing else commits during.
                        let mut sums = self.sums.lock();
                        wtxn.commit()?;
                        sums.apply(&feed);
                        self.tell_watcher(&sums, &feed);
                        drop(sums);
                        Ok(value)
                    });
                (seen, outcome)
            };
            match outcome {
                Ok(value) => return Ok(value),
                // Each pass the map is larger than the last, and the disk
                // bounds it, so this ends.
                Err(TxnError::Lmdb(heed::Error::Mdb(MdbError::MapFull))) => self.grow(seen)?,
                Err(TxnError::Lmdb(e)) => return Err(storage_error(e)),
                Err(TxnError::Other(e)) => return Err(e),
            }
        }
    }

    /// Grow the map, which was `seen` bytes when a write found it full, to
    /// what the disk allows now, as the chunk store does. Nothing to do if
    /// another write has grown it since.
    #[allow(unsafe_code)]
    fn grow(&self, seen: usize) -> Result<()> {
        let _exclusive = self.env_lock.write();
        let current = self.env.info().map_size;
        if current > seen {
            debug!("Another write grew the listing store's map; trying again");
            return Ok(());
        }
        let from_disk = compute_map_size(&self.dir, self.disk_reserve)?;
        #[cfg(test)]
        let from_disk = match self.map_ceiling.load(std::sync::atomic::Ordering::Relaxed) {
            0 => from_disk,
            ceiling => from_disk.min(ceiling),
        };
        if from_disk <= current {
            return Err(Error::Storage(
                "listings: the store is full and the disk has no room to grow it".into(),
            ));
        }
        // SAFETY: the exclusive lock is held, so no transaction is open.
        unsafe {
            self.env
                .resize(from_disk)
                .map_err(|e| Error::Storage(format!("listings: failed to grow the map: {e}")))?;
        }
        info!("Grew the listing store's map from {current} to {from_disk} bytes");
        Ok(())
    }

    /// Move a version 1 store's listings into the new databases. One that
    /// can't be moved now, as when the disk is full, is left for the next
    /// start: the node runs, and records new listings, without it meanwhile.
    fn migrate_v1_or_keep(&self) {
        if let Err(e) = self.migrate_v1(V1_BATCH) {
            warn!(
                "Could not move the version 1 listings into the new listing store; \
                 the rest are kept to be moved at the next start: {e}"
            );
        }
    }

    /// Move a version 1 store's listings into the new databases, `batch` at a
    /// time, then empty its databases. Each batch deletes the records it
    /// moved in the same transaction, so later batches reuse the room it
    /// frees and the move needs little more than the version 1 listings
    /// take. A move cut short goes on from where it stopped. Records that
    /// don't decode or validate are dropped.
    fn migrate_v1(&self, batch: usize) -> Result<()> {
        let (old_by_key, old_by_address) = {
            let _shared = self.env_lock.read();
            let rtxn = self.env.read_txn().map_err(storage_error)?;
            let by_key: Option<Database<Bytes, Bytes>> = self
                .env
                .open_database(&rtxn, Some(V1_BY_KEY))
                .map_err(storage_error)?;
            let by_address: Option<Database<Bytes, Bytes>> = self
                .env
                .open_database(&rtxn, Some(V1_BY_ADDRESS))
                .map_err(storage_error)?;
            // Emptied once every listing is moved: no write at each start
            // after that.
            let by_address = match by_address {
                Some(table) if !table.is_empty(&rtxn).map_err(storage_error)? => Some(table),
                _ => None,
            };
            rtxn.commit().map_err(storage_error)?;
            (by_key, by_address)
        };
        let (mut read, mut moved) = (0usize, 0usize);
        if let Some(old_by_key) = old_by_key {
            loop {
                let old: Vec<(Vec<u8>, Vec<u8>)> = {
                    let _shared = self.env_lock.read();
                    let rtxn = self.env.read_txn().map_err(storage_error)?;
                    let mut entries = Vec::with_capacity(batch);
                    for entry in old_by_key.iter(&rtxn).map_err(storage_error)?.take(batch) {
                        let (key, value) = entry.map_err(storage_error)?;
                        entries.push((key.to_vec(), value.to_vec()));
                    }
                    entries
                };
                if old.is_empty() {
                    break;
                }
                let listings: Vec<Option<(Listing, StoredRecord)>> = old
                    .iter()
                    .map(|(_, value)| {
                        v1_listing(value).map(|listing| {
                            let record = StoredRecord::new(&listing);
                            (listing, record)
                        })
                    })
                    .collect();
                moved += self.write(|wtxn, feed| {
                    let mut moved = 0usize;
                    for ((key, _), listing) in old.iter().zip(&listings) {
                        if let Some((listing, record)) = listing {
                            if !self.contains_in(wtxn, &listing.address)? {
                                self.insert_in(wtxn, listing, record)?;
                                self.note_change(wtxn, feed, &listing.address)?;
                                moved += 1;
                            }
                        }
                        old_by_key.delete(wtxn, key)?;
                    }
                    Ok(moved)
                })?;
                read += old.len();
            }
        }
        if let Some(old_by_address) = old_by_address {
            self.write(|wtxn, _| Ok(old_by_address.clear(wtxn)?))?;
        }
        if read > 0 {
            info!("Moved {moved} of {read} version 1 listing(s) into the new listing store");
        }
        Ok(())
    }
}

/// Keeping the change feed.
impl ListingStore {
    /// Record in the change feed what the node serves at `address` now, if
    /// that differs from the address's record, in the transaction that
    /// changed it. Nothing is recorded while the feed's listings are not
    /// numbered, or the feed is behind the store: they are numbered afresh at
    /// the next start.
    fn note_change(
        &self,
        wtxn: &mut RwTxn,
        feed: &mut FeedWrites,
        address: &XorName,
    ) -> std::result::Result<(), TxnError> {
        if self.feed_behind.load(Ordering::Relaxed) {
            return Ok(());
        }
        let Some(mut state) = self.feed_state(wtxn)?.filter(|state| state.numbered) else {
            return Ok(());
        };
        if self.renew_record(wtxn, feed, &mut state, address)? {
            self.change_meta.put(wtxn, FEED_STATE, &state.encode())?;
        }
        Ok(())
    }

    /// Give `address` a new record, numbered `state.next`, if what the node
    /// serves there differs from its record: the digest entry of the listing
    /// it serves, or the address's removal. The old record's row goes, so the
    /// feed holds one per address. Returns whether there is a new record.
    fn renew_record(
        &self,
        wtxn: &mut RwTxn,
        feed: &mut FeedWrites,
        state: &mut FeedState,
        address: &XorName,
    ) -> std::result::Result<bool, TxnError> {
        let served = self.served_entry(wtxn, address)?;
        let old = self.change_of.get(wtxn, address)?.and_then(decode_change);
        let was = old.as_ref().and_then(FeedChange::entry).copied();
        if was == served {
            return Ok(false);
        }
        let seq = state.next;
        let change = served.map_or(
            FeedChange::Removed {
                seq,
                address: *address,
            },
            |entry| FeedChange::Entry { seq, entry },
        );
        let record = encode_change(&change)?;
        if let Some(old) = old {
            self.changes.delete(wtxn, &old.seq().to_be_bytes())?;
        }
        self.changes.put(wtxn, &seq.to_be_bytes(), address)?;
        self.change_of.put(wtxn, address, &record)?;
        state.next = seq.checked_add(1).ok_or_else(|| {
            Error::Storage("listings: the change feed has used every number".into())
        })?;
        feed.epoch = state.epoch;
        feed.records.push((was, change));
        Ok(true)
    }

    /// Hand each write that adds records to the change feed, as it commits,
    /// to `watcher`, while the feed can be read. It runs under the lock that
    /// orders commits with reads of the feed, so the totals it reads are
    /// those at the write's head and nothing else commits meanwhile: it must
    /// be quick, and must not write to the store or read the feed but
    /// through the [`FeedCommit`] it is given. Only the first call has an
    /// effect.
    pub fn watch_feed(&self, watcher: FeedWatcher) {
        if self.feed_watcher.set(watcher).is_err() {
            warn!("A change feed watcher is already set; keeping the first");
        }
    }

    /// Tell the feed's watcher, if one is set and the feed can be read, what
    /// a write that has just committed added to the feed, given the totals
    /// with that write taken on.
    fn tell_watcher(&self, sums: &FeedSums, feed: &FeedWrites) {
        let Some(watcher) = self.feed_watcher.get() else {
            return;
        };
        if !sums.ready {
            return;
        }
        let records: Vec<FeedChange> = feed.records.iter().map(|(_, change)| *change).collect();
        let Some(head) = records.last().map(FeedChange::seq) else {
            return;
        };
        watcher(&FeedCommit {
            epoch: feed.epoch,
            head,
            records: &records,
            store: self,
            sums,
        });
    }

    /// The digest entry of the listing the node serves in the index at
    /// `address`, as list requests return it: in the index, not provisional,
    /// with a record that decodes. `None` if there is none.
    fn served_entry(&self, txn: &RoTxn, address: &XorName) -> heed::Result<Option<DigestEntry>> {
        let Some(key) = self.index_addresses.get(txn, address)? else {
            return Ok(None);
        };
        if self.provisional.get(txn, address)?.is_some() {
            return Ok(None);
        }
        let Some(value) = self.index.get(txn, key)? else {
            return Ok(None);
        };
        Ok(self
            .decode(txn, value)
            .ok()
            .map(|listing| DigestEntry::new(&listing)))
    }

    /// The change feed's state, if it has one this version reads.
    fn feed_state(&self, txn: &RoTxn) -> heed::Result<Option<FeedState>> {
        Ok(self
            .change_meta
            .get(txn, FEED_STATE)?
            .and_then(FeedState::decode))
    }

    /// Number the served listings in the change feed under a new epoch if
    /// the feed needs it ([`Self::feed_needs_numbering`]). If they can't be
    /// numbered now, as on a full disk, they are numbered at the next start;
    /// meanwhile the node keeps its listings, and the feed is behind the
    /// store ([`Self::feed_behind`]): it can't be read.
    fn number_feed_or_wait(&self) {
        let why = match self.feed_needs_numbering() {
            Ok(None) => return,
            Ok(Some(why)) => why,
            Err(e) => {
                warn!("Could not read the state of the listing change feed: {e}");
                "its state could not be read"
            }
        };
        match self.number_feed(FEED_BATCH) {
            Ok(numbered) => info!(
                "Numbered {numbered} served listing(s) in the change feed under a new epoch: \
                 {why}"
            ),
            Err(e) => {
                self.feed_behind.store(true, Ordering::Relaxed);
                warn!(
                    "Could not number the served listings in the change feed ({why}); they \
                     are numbered at the next start, and the feed can't be read until then: {e}"
                );
            }
        }
    }

    /// Why the change feed's listings must be numbered afresh, if they must:
    /// the feed is new, numbering it was cut short, or another version has
    /// written to the store since this one last did. Such a version keeps no
    /// feed, so what it changed isn't in it.
    fn feed_needs_numbering(&self) -> Result<Option<&'static str>> {
        let _shared = self.env_lock.read();
        let rtxn = self.env.read_txn().map_err(storage_error)?;
        let Some(state) = self
            .change_meta
            .get(&rtxn, FEED_STATE)
            .map_err(storage_error)?
        else {
            return Ok(Some("the feed is new"));
        };
        let Some(state) = FeedState::decode(state) else {
            return Ok(Some("the feed was kept in another format"));
        };
        if !state.numbered {
            return Ok(Some("numbering them was cut short"));
        }
        let last_write = self
            .change_meta
            .get(&rtxn, LAST_WRITE)
            .map_err(storage_error)?
            .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
            .map(u64::from_be_bytes);
        if last_write != Some(txn_id(&rtxn)) {
            return Ok(Some(
                "another version has written to the store since this one last did",
            ));
        }
        Ok(None)
    }

    /// Empty the change feed, and number every listing served now in it
    /// under a new epoch, in address order, `batch` at a time. Until that
    /// ends no change is recorded in the feed, and a start that finds it
    /// unfinished numbers the listings afresh. Returns how many it numbered.
    /// The feed's totals are not its own afterwards until
    /// [`Self::load_sums`] has run again.
    fn number_feed(&self, batch: usize) -> Result<u64> {
        self.sums.lock().reset();
        let old_epoch = {
            let _shared = self.env_lock.read();
            let rtxn = self.env.read_txn().map_err(storage_error)?;
            self.feed_state(&rtxn)
                .map_err(storage_error)?
                .map(|state| state.epoch)
        };
        let epoch = new_epoch(old_epoch);
        self.write(|wtxn, _| {
            self.changes.clear(wtxn)?;
            self.change_of.clear(wtxn)?;
            let state = FeedState {
                epoch,
                next: 1,
                numbered: false,
            };
            self.change_meta.put(wtxn, FEED_STATE, &state.encode())?;
            Ok(())
        })?;
        let batch = batch.max(1);
        let mut after: Option<Vec<u8>> = None;
        loop {
            let (last, state) = self.write(|wtxn, feed| {
                let mut state = self.feed_state(wtxn)?.ok_or_else(|| {
                    Error::Storage("listings: the change feed's state is missing".into())
                })?;
                let keys = self.index_addresses_after(wtxn, after.as_deref(), batch)?;
                for key in &keys {
                    if let Ok(address) = XorName::try_from(key.as_slice()) {
                        self.renew_record(wtxn, feed, &mut state, &address)?;
                    }
                }
                state.numbered = keys.len() < batch;
                self.change_meta.put(wtxn, FEED_STATE, &state.encode())?;
                Ok((keys.last().cloned(), state))
            })?;
            if state.numbered {
                return Ok(state.head());
            }
            after = last;
        }
    }

    /// Up to `limit` keys of `index_addresses`, the addresses of the listings
    /// in the index, in order after `after` (from the first if `None`).
    fn index_addresses_after(
        &self,
        txn: &RoTxn,
        after: Option<&[u8]>,
        limit: usize,
    ) -> heed::Result<Vec<Vec<u8>>> {
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let bounds: (Bound<&[u8]>, Bound<&[u8]>) = (start, Bound::Unbounded);
        let mut keys = Vec::with_capacity(limit);
        for entry in self.index_addresses.range(txn, &bounds)?.take(limit) {
            let (key, _) = entry?;
            keys.push(key.to_vec());
        }
        Ok(keys)
    }

    /// Total the change feed's entries per address prefix, as each start
    /// does. If they can't be read, the feed can't be read until the next
    /// start.
    fn load_sums_or_wait(&self) {
        if let Err(e) = self.load_sums() {
            warn!(
                "Could not total the entries of the listing change feed; it can't be read until \
                 the next start: {e}"
            );
        }
    }

    /// Total the change feed's entries per address prefix, from the store.
    /// The totals are not the feed's, and the feed can't be read, unless
    /// this succeeds with the feed's listings numbered and the feed not
    /// behind the store.
    fn load_sums(&self) -> Result<()> {
        let _shared = self.env_lock.read();
        let mut sums = self.sums.lock();
        sums.reset();
        if self.feed_behind.load(Ordering::Relaxed) {
            return Ok(());
        }
        let rtxn = self.env.read_txn().map_err(storage_error)?;
        let numbered = self
            .feed_state(&rtxn)
            .map_err(storage_error)?
            .is_some_and(|state| state.numbered);
        if !numbered {
            return Ok(());
        }
        for record in self.change_of.iter(&rtxn).map_err(storage_error)? {
            let (_, value) = record.map_err(storage_error)?;
            if let Some(FeedChange::Entry { entry, .. }) = decode_change(value) {
                sums.add(&entry);
            }
        }
        sums.ready = true;
        drop(sums);
        Ok(())
    }
}

/// Reading the change feed.
impl ListingStore {
    /// The change feed's records in `range` after record `after`, as a
    /// `ChangesRequest` asks for them, with where the feed stands and the
    /// totals of `range` there, all from one snapshot. If `epoch` is not the
    /// feed's, or `after` is beyond its head, there are no records and `next`
    /// is the head. Otherwise the records come in order, at most `limit` of
    /// them, and reading stops once `max_read` records of the feed have been
    /// read, in the range or not. With a `limit` of 0 it stops before the
    /// first record in the range, so `next` never passes a record the page
    /// leaves out. `None` while the feed can't
    /// be read: before its listings are numbered, or when numbering or
    /// totalling them failed at this start.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    pub async fn feed_page(
        &self,
        epoch: u64,
        after: u64,
        range: AddressRange,
        limit: usize,
        max_read: usize,
    ) -> Result<Option<FeedPage>> {
        let store = self.clone();
        spawn_blocking(move || store.feed_page_blocking(epoch, after, range, limit, max_read))
            .await
            .map_err(join_error)?
    }

    /// The digest entries the change feed holds in `range`, in address order,
    /// after `after` (from the range's first address if `None`): at most
    /// `limit`, and whether it holds more there. `None` while the feed can't
    /// be read.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    pub async fn feed_entries(
        &self,
        range: AddressRange,
        after: Option<XorName>,
        limit: usize,
    ) -> Result<Option<(Vec<DigestEntry>, bool)>> {
        let store = self.clone();
        spawn_blocking(move || store.feed_entries_blocking(range, after, limit))
            .await
            .map_err(join_error)?
    }

    fn feed_page_blocking(
        &self,
        epoch: u64,
        after: u64,
        range: AddressRange,
        limit: usize,
        max_read: usize,
    ) -> Result<Option<FeedPage>> {
        let _shared = self.env_lock.read();
        // The snapshot and the totals together: see `sums`.
        let (rtxn, totals) = {
            let sums = self.sums.lock();
            if !sums.ready {
                return Ok(None);
            }
            let rtxn = self.env.read_txn().map_err(storage_error)?;
            (rtxn, sums.totals(&range))
        };
        let Some(state) = self
            .feed_state(&rtxn)
            .map_err(storage_error)?
            .filter(|state| state.numbered)
        else {
            return Ok(None);
        };
        let totals = match totals {
            Some(totals) => totals,
            None => self.scan_totals(&rtxn, &range)?,
        };
        let head = state.head();
        let mut page = FeedPage {
            epoch: state.epoch,
            head,
            next: head,
            count: totals.count,
            set_hash: totals.set_hash,
            changes: Vec::new(),
        };
        if epoch != state.epoch || after > head {
            return Ok(Some(page));
        }
        page.next = after;
        let from = after.to_be_bytes();
        let bounds: (Bound<&[u8]>, Bound<&[u8]>) = (Bound::Excluded(&from[..]), Bound::Unbounded);
        let mut read = 0usize;
        let mut to_the_end = true;
        for row in self.changes.range(&rtxn, &bounds).map_err(storage_error)? {
            let (key, address) = row.map_err(storage_error)?;
            let (Ok(seq), Ok(address)) = (
                <[u8; 8]>::try_from(key).map(u64::from_be_bytes),
                XorName::try_from(address),
            ) else {
                continue;
            };
            if limit == 0 && range.contains(&address) {
                to_the_end = false;
                break;
            }
            read += 1;
            page.next = seq;
            if range.contains(&address) {
                // A row its address's record no longer names is left over
                // from a record that didn't decode: skipped.
                let change = self
                    .change_of
                    .get(&rtxn, &address)
                    .map_err(storage_error)?
                    .and_then(decode_change)
                    .filter(|change| change.seq() == seq);
                page.changes.extend(change);
            }
            if (limit > 0 && page.changes.len() >= limit) || read >= max_read.max(1) {
                to_the_end = false;
                break;
            }
        }
        if to_the_end {
            page.next = head;
        }
        Ok(Some(page))
    }

    fn feed_entries_blocking(
        &self,
        range: AddressRange,
        after: Option<XorName>,
        limit: usize,
    ) -> Result<Option<(Vec<DigestEntry>, bool)>> {
        let _shared = self.env_lock.read();
        // Readable once totalled at this start, as for `feed_page`.
        let ready = self.sums.lock().ready;
        if !ready {
            return Ok(None);
        }
        let rtxn = self.env.read_txn().map_err(storage_error)?;
        let numbered = self
            .feed_state(&rtxn)
            .map_err(storage_error)?
            .is_some_and(|state| state.numbered);
        if !numbered {
            return Ok(None);
        }
        let (first, last) = (range.first(), range.last());
        let after = after.filter(|after| *after >= first);
        let start = after.as_ref().map_or(Bound::Included(&first[..]), |after| {
            Bound::Excluded(&after[..])
        });
        let bounds: (Bound<&[u8]>, Bound<&[u8]>) = (start, Bound::Included(&last[..]));
        let mut entries = Vec::new();
        for record in self
            .change_of
            .range(&rtxn, &bounds)
            .map_err(storage_error)?
        {
            let (_, value) = record.map_err(storage_error)?;
            if let Some(FeedChange::Entry { entry, .. }) = decode_change(value) {
                if entries.len() == limit {
                    return Ok(Some((entries, true)));
                }
                entries.push(entry);
            }
        }
        Ok(Some((entries, false)))
    }

    /// The totals of the change feed's entries in `range`, counted from the
    /// store: for a range finer than the prefixes the totals are kept for.
    fn scan_totals(&self, txn: &RoTxn, range: &AddressRange) -> Result<Totals> {
        let (first, last) = (range.first(), range.last());
        let bounds: (Bound<&[u8]>, Bound<&[u8]>) =
            (Bound::Included(&first[..]), Bound::Included(&last[..]));
        let mut totals = Totals::default();
        for record in self.change_of.range(txn, &bounds).map_err(storage_error)? {
            let (_, value) = record.map_err(storage_error)?;
            if let Some(FeedChange::Entry { entry, .. }) = decode_change(value) {
                totals.add(SetHash::of_entry(&entry));
            }
        }
        Ok(totals)
    }
}

/// A page of the change feed, read from one snapshot of the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedPage {
    /// The feed's epoch.
    pub epoch: u64,
    /// The number of its latest record, 0 if it has none.
    pub head: u64,
    /// The number of the last record read for the page: `head` once the
    /// feed was read to its end.
    pub next: u64,
    /// How many entries the feed holds in the range asked about, at `head`.
    pub count: u64,
    /// Their set hash.
    pub set_hash: SetHash,
    /// The records in the range after the one asked after, up to `next`, in
    /// order.
    pub changes: Vec<FeedChange>,
}

impl FeedPage {
    /// Whether the feed has records after `next`.
    #[must_use]
    pub const fn more(&self) -> bool {
        self.next < self.head
    }
}

/// Told of each write that adds records to the change feed, as it commits
/// ([`ListingStore::watch_feed`]).
pub type FeedWatcher = Box<dyn Fn(&FeedCommit<'_>) + Send + Sync>;

/// What one write added to the change feed, as it commits: the records, in
/// order, and where the feed stands after them. [`Self::totals`] gives any
/// range's totals there.
pub struct FeedCommit<'a> {
    /// The feed's epoch.
    pub epoch: u64,
    /// The number of the last record the write added: the feed's head.
    pub head: u64,
    /// The records the write added, in order.
    pub records: &'a [FeedChange],
    store: &'a ListingStore,
    sums: &'a FeedSums,
}

impl FeedCommit<'_> {
    /// How many entries the feed holds in `range` at [`Self::head`], and
    /// their set hash. `None` if they can't be read.
    #[must_use]
    pub fn totals(&self, range: &AddressRange) -> Option<(u64, SetHash)> {
        let totals = if let Some(totals) = self.sums.totals(range) {
            totals
        } else {
            let rtxn = self.store.env.read_txn().ok()?;
            self.store.scan_totals(&rtxn, range).ok()?
        };
        Some((totals.count, totals.set_hash))
    }
}

/// The change feed's state, as `change_meta` keeps it under [`FEED_STATE`]:
/// [`FEED_VERSION`], the epoch and the next number (each 8 bytes,
/// big-endian), and 1 once the epoch's first listings are all numbered, else
/// 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FeedState {
    /// Random, never 0, and new each time the listings are numbered afresh.
    epoch: u64,
    /// The number the next record takes: one more than the head's, the
    /// latest record's.
    next: u64,
    /// Whether every listing served when the epoch began has been numbered.
    /// Until then no change is recorded in the feed.
    numbered: bool,
}

impl FeedState {
    const LEN: usize = 18;

    fn encode(self) -> [u8; Self::LEN] {
        let mut bytes = [0; Self::LEN];
        bytes[0] = FEED_VERSION;
        bytes[1..9].copy_from_slice(&self.epoch.to_be_bytes());
        bytes[9..17].copy_from_slice(&self.next.to_be_bytes());
        bytes[17] = u8::from(self.numbered);
        bytes
    }

    /// The state `bytes` hold, if this version reads them.
    fn decode(bytes: &[u8]) -> Option<Self> {
        let (&FEED_VERSION, rest) = bytes.split_first()? else {
            return None;
        };
        let (epoch, rest) = rest.split_first_chunk::<8>()?;
        let (next, rest) = rest.split_first_chunk::<8>()?;
        let numbered = match rest {
            [0] => false,
            [1] => true,
            _ => return None,
        };
        let (epoch, next) = (u64::from_be_bytes(*epoch), u64::from_be_bytes(*next));
        (epoch != 0 && next != 0).then_some(Self {
            epoch,
            next,
            numbered,
        })
    }

    /// The number of the latest record, 0 if there is none.
    const fn head(self) -> u64 {
        self.next.saturating_sub(1)
    }
}

/// A `change_of` record: [`FEED_VERSION`], then the record's postcard
/// encoding, as a `ChangesResponse` carries it.
fn encode_change(change: &FeedChange) -> Result<Vec<u8>> {
    let body = postcard::to_stdvec(change)
        .map_err(|e| Error::Serialization(format!("feed record: {e}")))?;
    let mut value = Vec::with_capacity(1 + body.len());
    value.push(FEED_VERSION);
    value.extend_from_slice(&body);
    Ok(value)
}

/// The record a `change_of` value holds, if this version reads it.
fn decode_change(value: &[u8]) -> Option<FeedChange> {
    let (&FEED_VERSION, body) = value.split_first()? else {
        return None;
    };
    match postcard::take_from_bytes(body) {
        Ok((change, [])) => Some(change),
        _ => None,
    }
}

/// The id of a transaction: for a write, the id its commit makes the
/// store's last; for a read, its snapshot's.
fn txn_id(txn: &RoTxn) -> u64 {
    u64::try_from(txn.id()).unwrap_or(u64::MAX)
}

/// A new epoch for the change feed: random, never 0, and not `old`.
fn new_epoch(old: Option<u64>) -> u64 {
    loop {
        let epoch = rand::random::<u64>();
        if epoch != 0 && Some(epoch) != old {
            return epoch;
        }
    }
}

/// How many digest entries a set holds, and their set hash.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Totals {
    count: u64,
    set_hash: SetHash,
}

impl Totals {
    /// Add an entry whose part of a set hash is `hash`.
    fn add(&mut self, hash: SetHash) {
        self.count = self.count.wrapping_add(1);
        self.set_hash += hash;
    }

    /// Take out an entry whose part of a set hash is `hash`.
    fn take(&mut self, hash: SetHash) {
        self.count = self.count.wrapping_sub(1);
        self.set_hash -= hash;
    }
}

impl AddAssign for Totals {
    fn add_assign(&mut self, other: Self) {
        self.count = self.count.wrapping_add(other.count);
        self.set_hash += other.set_hash;
    }
}

/// The totals of the change feed's entries per address prefix: per 16-bit
/// prefix, and per 8-bit prefix to add those up, so the totals of any range
/// of at most 16 bits are a sum of at most 256.
struct FeedSums {
    /// Whether they are the feed's: set once they are totalled at a start.
    /// Until then writes leave them alone.
    ready: bool,
    /// By the first 16 bits of the address.
    fine: Vec<Totals>,
    /// By the first 8.
    coarse: Vec<Totals>,
}

impl FeedSums {
    fn new() -> Self {
        Self {
            ready: false,
            fine: vec![Totals::default(); 1 << 16],
            coarse: vec![Totals::default(); 1 << 8],
        }
    }

    /// Back to no entries, not the feed's.
    fn reset(&mut self) {
        self.ready = false;
        self.fine.fill(Totals::default());
        self.coarse.fill(Totals::default());
    }

    fn add(&mut self, entry: &DigestEntry) {
        let hash = SetHash::of_entry(entry);
        self.fine[fine_prefix(&entry.address)].add(hash);
        self.coarse[usize::from(entry.address[0])].add(hash);
    }

    fn take(&mut self, entry: &DigestEntry) {
        let hash = SetHash::of_entry(entry);
        self.fine[fine_prefix(&entry.address)].take(hash);
        self.coarse[usize::from(entry.address[0])].take(hash);
    }

    /// Take on what a committed transaction added to the feed.
    fn apply(&mut self, feed: &FeedWrites) {
        if !self.ready {
            return;
        }
        for (was, change) in &feed.records {
            if let Some(was) = was {
                self.take(was);
            }
            if let Some(entry) = change.entry() {
                self.add(entry);
            }
        }
    }

    /// The totals of `range`, if it fixes at most 16 bits.
    fn totals(&self, range: &AddressRange) -> Option<Totals> {
        let (first, last) = (range.first(), range.last());
        let prefixes = match range.bits() {
            0..=8 => &self.coarse[usize::from(first[0])..=usize::from(last[0])],
            9..=16 => &self.fine[fine_prefix(&first)..=fine_prefix(&last)],
            _ => return None,
        };
        let mut totals = Totals::default();
        for prefix in prefixes {
            totals += *prefix;
        }
        Some(totals)
    }
}

/// The first 16 bits of an address.
fn fine_prefix(address: &XorName) -> usize {
    usize::from(u16::from_be_bytes([address[0], address[1]]))
}

/// What a write transaction added to the change feed: each new record, with
/// the entry its address had before, if any, and the feed's epoch. The
/// totals take it on once the transaction commits.
#[derive(Default)]
struct FeedWrites {
    epoch: u64,
    records: Vec<(Option<DigestEntry>, FeedChange)>,
}

/// The listing a version 1 record describes, or `None` if it doesn't decode
/// or validate.
fn v1_listing(value: &[u8]) -> Option<Listing> {
    match postcard::from_bytes::<ListingV1>(value) {
        Ok(v1) => {
            let listing = Listing::from(v1);
            match listing.validate() {
                Ok(()) => Some(listing),
                Err(e) => {
                    warn!(
                        "Dropping version 1 listing {}: {e}",
                        hex::encode(listing.address)
                    );
                    None
                }
            }
        }
        Err(e) => {
            warn!("Dropping a version 1 listing that doesn't decode: {e}");
            None
        }
    }
}

/// How a listing is stored: its fields, with the quote's public key replaced
/// by a reference to `quote_keys`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredListing {
    address: XorName,
    flags: Flags,
    metadata: Metadata,
    size: Option<u64>,
    tx_hash: Option<[u8; 32]>,
    block_number: Option<u64>,
    block_timestamp: Option<u64>,
    log_index: Option<u64>,
    quote: Option<StoredQuote>,
    first_seen: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum StoredQuote {
    /// The quote, msgpack-encoded without its public key and signature; the
    /// signature's raw bytes; and the BLAKE3 hash of the key, under which
    /// `quote_keys` holds it. Raw, the signature takes about two thirds of
    /// the room msgpack gives it, so a record fits in one LMDB page.
    Keyed {
        key: [u8; 32],
        signature: Vec<u8>,
        quote: Vec<u8>,
    },
    /// A quote that doesn't rebuild to the same bytes without its key, kept
    /// as it came.
    Verbatim(Vec<u8>),
}

/// A quote's public key and its BLAKE3 hash, the key under which
/// `quote_keys` holds it.
type QuoteKey = ([u8; 32], Vec<u8>);

/// A listing ready to store, and the public key it refers to.
struct StoredRecord {
    listing: StoredListing,
    quote_key: Option<QuoteKey>,
}

impl StoredRecord {
    fn new(listing: &Listing) -> Self {
        let (quote, quote_key) = listing
            .payment
            .quote
            .as_deref()
            .map_or((None, None), |bytes| store_quote(&listing.address, bytes));
        let payment = &listing.payment;
        Self {
            listing: StoredListing {
                address: listing.address,
                flags: listing.flags,
                metadata: listing.metadata.clone(),
                size: listing.size,
                tx_hash: payment.tx_hash,
                block_number: payment.block_number,
                block_timestamp: payment.block_timestamp,
                log_index: payment.log_index,
                quote,
                first_seen: listing.first_seen,
            },
            quote_key,
        }
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let body =
            postcard::to_stdvec(&self.listing).map_err(|e| Error::Serialization(e.to_string()))?;
        let mut value = Vec::with_capacity(1 + body.len());
        value.push(RECORD_VERSION);
        value.extend_from_slice(&body);
        Ok(value)
    }

    fn decode(value: &[u8]) -> Result<StoredListing> {
        match value.split_first() {
            Some((&RECORD_VERSION, rest)) => postcard::from_bytes(rest)
                .map_err(|e| Error::Serialization(format!("bad listing: {e}"))),
            Some((version, _)) => Err(Error::Serialization(format!(
                "bad listing: unknown record version {version}"
            ))),
            None => Err(Error::Serialization("bad listing: empty record".into())),
        }
    }
}

impl StoredListing {
    fn key_reference(&self) -> Option<[u8; 32]> {
        match &self.quote {
            Some(StoredQuote::Keyed { key, .. }) => Some(*key),
            _ => None,
        }
    }
}

/// How to store the quote `bytes` of the listing of `address`, and the
/// public key the stored form refers to.
fn store_quote(address: &XorName, bytes: &[u8]) -> (Option<StoredQuote>, Option<QuoteKey>) {
    let Some(SplitQuote {
        pub_key,
        signature,
        rest,
    }) = split_quote(bytes)
    else {
        debug!("Storing the quote of {} as it came", hex::encode(address));
        return (Some(StoredQuote::Verbatim(bytes.to_vec())), None);
    };
    let key: [u8; 32] = blake3::hash(&pub_key).into();
    let stored = StoredQuote::Keyed {
        key,
        signature,
        quote: rest,
    };
    (Some(stored), Some((key, pub_key)))
}

/// A msgpack quote taken apart for storing.
struct SplitQuote {
    pub_key: Vec<u8>,
    signature: Vec<u8>,
    /// The quote without its public key and signature, msgpack-encoded.
    rest: Vec<u8>,
}

/// `bytes`, a msgpack quote, taken apart, if putting it back together gives
/// exactly `bytes`.
fn split_quote(bytes: &[u8]) -> Option<SplitQuote> {
    let mut quote: PaymentQuote = rmp_serde::from_slice(bytes).ok()?;
    let pub_key = std::mem::take(&mut quote.pub_key);
    let signature = std::mem::take(&mut quote.signature);
    let rest = rmp_serde::to_vec(&quote).ok()?;
    let rebuilt = join_quote(&rest, pub_key.clone(), signature.clone())?;
    (rebuilt.as_slice() == bytes).then_some(SplitQuote {
        pub_key,
        signature,
        rest,
    })
}

/// The quote [`split_quote`] took apart, whole again.
fn join_quote(rest: &[u8], pub_key: Vec<u8>, signature: Vec<u8>) -> Option<Vec<u8>> {
    let mut quote: PaymentQuote = rmp_serde::from_slice(rest).ok()?;
    quote.pub_key = pub_key;
    quote.signature = signature;
    rmp_serde::to_vec(&quote).ok()
}

/// The count at the front of a `quote_keys` value.
fn key_count(value: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(value.get(..8)?.try_into().ok()?))
}

#[allow(clippy::needless_pass_by_value)]
fn storage_error(e: heed::Error) -> Error {
    Error::Storage(format!("listings: {e}"))
}

#[allow(clippy::needless_pass_by_value)]
fn join_error(e: tokio::task::JoinError) -> Error {
    Error::Storage(format!("listings task failed: {e}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub mod tests {
    use super::*;
    use ant_listings::{PaymentField, ShownFields};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use saorsa_core::MlDsa65;
    use saorsa_pqc::pqc::types::MlDsaSecretKey;
    use saorsa_pqc::pqc::MlDsaOperations;
    use std::sync::OnceLock;
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;

    impl ListingStore {
        /// Make every write fail, or succeed again.
        pub fn fail_writes(&self, failing: bool) {
            self.failing
                .store(failing, std::sync::atomic::Ordering::Relaxed);
        }

        /// Make the change feed unreadable, as at a start that couldn't
        /// total it, until it is totalled again.
        pub fn hide_feed(&self) {
            self.sums.lock().ready = false;
        }

        /// Insert each of `listings` whose address has no listing yet, in
        /// one write, recording them in the change feed as
        /// [`Self::insert`] does one at a time.
        pub fn insert_all(&self, listings: &[Listing]) {
            let records: Vec<StoredRecord> = listings.iter().map(StoredRecord::new).collect();
            self.write(|wtxn, feed| {
                for (listing, record) in listings.iter().zip(&records) {
                    if !self.contains_in(wtxn, &listing.address)? {
                        self.insert_in(wtxn, listing, record)?;
                        self.note_change(wtxn, feed, &listing.address)?;
                    }
                }
                Ok(())
            })
            .unwrap();
        }
    }

    /// A quote for `content`, signed with one of two test keys.
    pub fn signed_quote(content: XorName, key: usize, price: u64) -> PaymentQuote {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(1_790_000_000 - 60);
        signed_quote_at(content, key, price, timestamp)
    }

    /// A quote for `content` dated `timestamp`, signed with one of two test
    /// keys.
    pub fn signed_quote_at(
        content: XorName,
        key: usize,
        price: u64,
        timestamp: SystemTime,
    ) -> PaymentQuote {
        static KEYS: OnceLock<Vec<(Vec<u8>, Vec<u8>)>> = OnceLock::new();
        let ml_dsa = MlDsa65::new();
        let keys = KEYS.get_or_init(|| {
            (0..2)
                .map(|_| {
                    let (public_key, secret_key) = ml_dsa.generate_keypair().unwrap();
                    (
                        public_key.as_bytes().to_vec(),
                        secret_key.as_bytes().to_vec(),
                    )
                })
                .collect()
        });
        let (public_key, secret_key) = &keys[key];
        let mut quote = PaymentQuote {
            content: ant_listings::self_encryption::XorName(content),
            timestamp,
            price: ::ant_protocol::evm::Amount::from(price),
            rewards_address: ::ant_protocol::evm::RewardsAddress::new([0xab; 20]),
            pub_key: public_key.clone(),
            signature: Vec::new(),
            committed_key_count: 0,
            commitment_pin: None,
        };
        let secret_key = MlDsaSecretKey::from_bytes(secret_key).unwrap();
        quote.signature = ml_dsa
            .sign(&secret_key, &quote.bytes_for_sig())
            .unwrap()
            .as_bytes()
            .to_vec();
        quote
    }

    fn listing(address: u8, time: u64, block: u64, log: u64, flags: Flags) -> Listing {
        let quote = rmp_serde::to_vec(&signed_quote([address; 32], 0, 87)).unwrap();
        let shown = |field| flags.shown.contains(field);
        Listing {
            address: [address; 32],
            flags,
            metadata: Metadata::default(),
            size: (!flags.withheld).then_some(10),
            payment: ListingPayment {
                tx_hash: shown(PaymentField::TxHash).then_some([address; 32]),
                block_number: shown(PaymentField::BlockNumber).then_some(block),
                block_timestamp: shown(PaymentField::BlockTimestamp).then_some(time),
                log_index: shown(PaymentField::LogIndex).then_some(log),
                quote: shown(PaymentField::Quote).then_some(quote),
            },
            first_seen: (flags.in_index && !shown(PaymentField::BlockTimestamp))
                .then_some(time - time % 3600),
        }
    }

    fn indexed(address: u8, time: u64, block: u64, log: u64) -> Listing {
        listing(address, time, block, log, Flags::V1)
    }

    fn unindexed_listing(address: u8) -> Listing {
        let flags = Flags {
            shown: ShownFields::ALL,
            in_index: false,
            withheld: true,
        };
        listing(address, 1_790_000_000, 10, 0, flags)
    }

    async fn key_count_of(store: &ListingStore, quote: &[u8]) -> Option<u64> {
        let hash: [u8; 32] = blake3::hash(&split_quote(quote).unwrap().pub_key).into();
        let store = store.clone();
        spawn_blocking(move || {
            let rtxn = store.env.read_txn().unwrap();
            store
                .quote_keys
                .get(&rtxn, &hash)
                .unwrap()
                .and_then(key_count)
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn keeps_listings_in_index_order() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        // Time first, then block, then log index.
        for (address, time, block, log) in [
            (1, 1_790_000_020, 20, 0),
            (2, 1_790_000_010, 10, 5),
            (3, 1_790_000_010, 10, 2),
            (4, 1_790_000_300, 300, 0),
        ] {
            assert!(store
                .insert(indexed(address, time, block, log))
                .await
                .unwrap());
        }
        // Hidden time: keyed by first_seen, before block 10's time.
        let hidden = Flags {
            shown: ShownFields::NONE,
            in_index: true,
            withheld: false,
        };
        assert!(store
            .insert(listing(5, 1_789_999_000, 0, 0, hidden))
            .await
            .unwrap());

        let (all, more) = store.range(ListingKey::MIN, 10).await.unwrap();
        let order: Vec<u8> = all.iter().map(|l| l.address[0]).collect();
        assert_eq!(order, vec![5, 3, 2, 1, 4]);
        assert!(!more);

        let (page, more) = store.range(ListingKey::MIN, 2).await.unwrap();
        assert_eq!(page.len(), 2);
        assert!(more);
        let (rest, more) = store
            .range(page[1].key().checked_next().unwrap(), 10)
            .await
            .unwrap();
        let order: Vec<u8> = rest.iter().map(|l| l.address[0]).collect();
        assert_eq!(order, vec![2, 1, 4]);
        assert!(!more);
    }

    #[tokio::test]
    async fn listings_outside_the_index_are_kept_apart() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let outside = unindexed_listing(7);
        let inside = indexed(8, 1_790_000_000, 10, 0);
        assert!(store.insert(outside.clone()).await.unwrap());
        assert!(store.insert(inside.clone()).await.unwrap());

        // List requests never see it; get requests do.
        let (all, more) = store.range(ListingKey::MIN, 10).await.unwrap();
        assert_eq!(all, vec![inside.clone()]);
        assert!(!more);
        assert_eq!(store.get([7; 32]).await.unwrap(), Some(outside.clone()));
        assert_eq!(store.get([8; 32]).await.unwrap(), Some(inside));
        assert!(store.contains([7; 32]).await.unwrap());

        // One listing per address, whichever table holds it.
        assert!(!store
            .insert(indexed(7, 1_790_000_000, 10, 0))
            .await
            .unwrap());
        assert!(store.remove([7; 32]).await.unwrap());
        assert!(!store.contains([7; 32]).await.unwrap());
        assert_eq!(store.get([7; 32]).await.unwrap(), None);
    }

    #[tokio::test]
    async fn the_first_listing_of_an_address_stands() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert!(store
            .insert(indexed(1, 1_790_000_010, 10, 0))
            .await
            .unwrap());
        assert!(!store
            .insert(indexed(1, 1_790_000_099, 99, 0))
            .await
            .unwrap());
        let held = store.get([1; 32]).await.unwrap().unwrap();
        assert_eq!(held.payment.block_number, Some(10));
        let (all, _) = store.range(ListingKey::MIN, 10).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn replace_moves_a_listing_between_tables() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let first = indexed(1, 1_790_000_010, 10, 0);
        store.insert(first.clone()).await.unwrap();
        let other = unindexed_listing(1);
        assert!(store.replace(other.clone(), false).await.unwrap());
        assert_eq!(store.get([1; 32]).await.unwrap(), Some(other));
        assert_eq!(
            store.range(ListingKey::MIN, 10).await.unwrap().0,
            Vec::<Listing>::new()
        );
        assert!(store.replace(first.clone(), false).await.unwrap());
        assert_eq!(
            store.range(ListingKey::MIN, 10).await.unwrap().0,
            vec![first.clone()]
        );

        // A listing dropped meanwhile, with its chunk, is not brought back.
        assert!(store.remove([1; 32]).await.unwrap());
        assert!(!store.replace(first, false).await.unwrap());
        assert!(!store.contains([1; 32]).await.unwrap());
    }

    #[tokio::test]
    async fn provisional_listings_are_kept_but_not_served_until_settled() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let listing = indexed(1, 1_790_000_010, 10, 0);
        assert!(store.insert_provisional(listing.clone()).await.unwrap());
        assert!(store.is_provisional([1; 32]).await.unwrap());
        assert!(store.contains([1; 32]).await.unwrap());
        assert_eq!(store.get([1; 32]).await.unwrap(), Some(listing.clone()));
        assert_eq!(store.served([1; 32]).await.unwrap(), None);
        assert_eq!(
            store.range(ListingKey::MIN, 10).await.unwrap().0,
            Vec::<Listing>::new()
        );
        // The first listing still stands.
        assert!(!store
            .insert(indexed(1, 1_790_000_099, 99, 0))
            .await
            .unwrap());

        // The mark survives a restart.
        drop(store);
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert_eq!(store.provisional_addresses().await.unwrap(), vec![[1; 32]]);

        // Replaced, still provisional; then settled.
        let other = indexed(1, 1_790_000_020, 20, 0);
        assert!(store.replace(other.clone(), true).await.unwrap());
        assert_eq!(store.served([1; 32]).await.unwrap(), None);
        assert!(store.replace(other.clone(), false).await.unwrap());
        assert!(!store.is_provisional([1; 32]).await.unwrap());
        assert_eq!(store.served([1; 32]).await.unwrap(), Some(other.clone()));
        assert_eq!(
            store.range(ListingKey::MIN, 10).await.unwrap().0,
            vec![other]
        );

        // Settling keeps the listing; removing it drops the mark too.
        let second = unindexed_listing(2);
        assert!(store.insert_provisional(second.clone()).await.unwrap());
        assert!(store.settle([2; 32]).await.unwrap());
        assert!(!store.settle([2; 32]).await.unwrap());
        assert_eq!(store.served([2; 32]).await.unwrap(), Some(second.clone()));
        assert!(store
            .insert_provisional(unindexed_listing(3))
            .await
            .unwrap());
        assert!(store.remove([3; 32]).await.unwrap());
        assert_eq!(
            store.provisional_addresses().await.unwrap(),
            Vec::<XorName>::new()
        );
    }

    #[tokio::test]
    async fn pending_marks_survive_a_restart_until_cleared() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        // An envelope without a listing yet, and one with a listing.
        store.mark_pending([1; 32]).await.unwrap();
        store.mark_pending([2; 32]).await.unwrap();
        store
            .insert(indexed(2, 1_790_000_010, 10, 0))
            .await
            .unwrap();
        // Marking twice keeps one mark.
        store.mark_pending([1; 32]).await.unwrap();

        drop(store);
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert_eq!(
            store.pending_addresses().await.unwrap(),
            vec![[1; 32], [2; 32]]
        );
        // Neither a listing nor provisional: only a mark.
        assert!(!store.contains([1; 32]).await.unwrap());
        assert_eq!(
            store.provisional_addresses().await.unwrap(),
            Vec::<XorName>::new()
        );

        // Cleared by hand, or with the listing.
        store.clear_pending([1; 32]).await.unwrap();
        store.clear_pending([1; 32]).await.unwrap();
        assert!(store.remove([2; 32]).await.unwrap());
        assert_eq!(
            store.pending_addresses().await.unwrap(),
            Vec::<XorName>::new()
        );
    }

    #[tokio::test]
    async fn walks_the_addresses_of_both_tables() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        for address in [5u8, 1, 9, 3] {
            store
                .insert(indexed(address, 1_790_000_010, 10, 0))
                .await
                .unwrap();
        }
        for address in [2u8, 8, 4] {
            store.insert(unindexed_listing(address)).await.unwrap();
        }
        let mut walked = Vec::new();
        let mut after = None;
        loop {
            let batch = store.addresses_after(after, 3).await.unwrap();
            let Some(&last) = batch.last() else {
                break;
            };
            assert!(batch.len() <= 3);
            walked.extend(batch.iter().map(|address| address[0]));
            after = Some(last);
        }
        assert_eq!(walked, vec![1, 2, 3, 4, 5, 8, 9]);
    }

    /// Store a record for `address` in the index that doesn't decode.
    async fn insert_undecodable(store: &ListingStore, address: u8, time: u64) {
        let store = store.clone();
        spawn_blocking(move || {
            let key = indexed(address, time, 10, 0).key().to_bytes();
            store
                .write(|wtxn, _| {
                    store.index.put(wtxn, &key, &[RECORD_VERSION, 0xff])?;
                    store.index_addresses.put(wtxn, &[address; 32], &key)?;
                    Ok(())
                })
                .unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn records_that_dont_decode_are_skipped_not_fatal() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let before = indexed(1, 1_790_000_010, 10, 0);
        let after = indexed(3, 1_790_000_030, 30, 0);
        store.insert(before.clone()).await.unwrap();
        insert_undecodable(&store, 2, 1_790_000_020).await;
        store.insert(after.clone()).await.unwrap();

        // Pages reach past it, and it doesn't count towards the limit.
        let next = before.key().checked_next().unwrap();
        let (all, more) = store.range(ListingKey::MIN, 2).await.unwrap();
        assert_eq!(all, vec![before, after.clone()]);
        assert!(!more);
        let (rest, more) = store.range(next, 1).await.unwrap();
        assert_eq!(rest, vec![after]);
        assert!(!more);
        // Read alone, it is no listing; it can still be removed.
        assert_eq!(store.get([2; 32]).await.unwrap(), None);
        assert!(store.remove([2; 32]).await.unwrap());
    }

    #[tokio::test]
    async fn quote_keys_are_stored_once_and_quotes_rebuilt_exactly() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let a = indexed(1, 1_790_000_010, 10, 0);
        let b = unindexed_listing(2);
        let quote_a = a.payment.quote.clone().unwrap();
        assert!(store.insert(a.clone()).await.unwrap());
        assert!(store.insert(b.clone()).await.unwrap());
        // Both quotes are signed with the same key: stored once, used twice.
        assert_eq!(key_count_of(&store, &quote_a).await, Some(2));

        // Read back byte for byte, including after reopening.
        assert_eq!(store.get(a.address).await.unwrap(), Some(a.clone()));
        drop(store);
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert_eq!(store.get(a.address).await.unwrap(), Some(a.clone()));
        assert_eq!(store.get(b.address).await.unwrap(), Some(b.clone()));

        // The record holds a reference, not the key, and the signature raw:
        // one 4 KiB page.
        let encoded = StoredRecord::new(&a).encode().unwrap();
        assert!(encoded.len() < 4000, "{} bytes", encoded.len());
        assert!(quote_a.len() > 7000);

        assert!(store.remove(a.address).await.unwrap());
        assert_eq!(key_count_of(&store, &quote_a).await, Some(1));
        assert!(store.remove(b.address).await.unwrap());
        assert_eq!(key_count_of(&store, &quote_a).await, None);
    }

    #[tokio::test]
    async fn quotes_that_dont_rebuild_are_kept_as_they_came() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let mut odd = indexed(1, 1_790_000_010, 10, 0);
        odd.payment.quote = Some(vec![1, 2, 3]);
        assert!(store.insert(odd.clone()).await.unwrap());
        assert_eq!(store.get(odd.address).await.unwrap(), Some(odd));
    }

    #[tokio::test]
    async fn removes_and_survives_reopening() {
        let dir = TempDir::new().unwrap();
        {
            let store = ListingStore::open(dir.path(), 0).await.unwrap();
            store
                .insert(indexed(1, 1_790_000_010, 10, 0))
                .await
                .unwrap();
            store
                .insert(indexed(2, 1_790_000_011, 11, 0))
                .await
                .unwrap();
            assert!(store.remove([1; 32]).await.unwrap());
            assert!(!store.remove([1; 32]).await.unwrap());
            assert!(!store.remove([9; 32]).await.unwrap());
        }
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert!(!store.contains([1; 32]).await.unwrap());
        assert!(store.contains([2; 32]).await.unwrap());
        let (all, _) = store.range(ListingKey::MIN, 10).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn the_map_grows_when_it_fills() {
        let dir = TempDir::new().unwrap();
        // 64 pages: a few listings fill it.
        let small = 64 * page_size::get();
        let store = ListingStore::open_with_map_size(dir.path(), 0, Some(small))
            .await
            .unwrap();
        for address in 0..200u8 {
            let mut listing = unindexed_listing(address);
            listing.metadata.name = Some("x".repeat(255));
            assert!(store.insert(listing).await.unwrap());
        }
        assert!(store.env.info().map_size > small);
        for address in 0..200u8 {
            assert!(store.contains([address; 32]).await.unwrap());
        }
    }

    #[tokio::test]
    async fn a_write_that_finds_the_map_already_grown_tries_again() {
        let dir = TempDir::new().unwrap();
        let small = 64 * page_size::get();
        let store = ListingStore::open_with_map_size(dir.path(), 0, Some(small))
            .await
            .unwrap();
        // Two writes found the map full at `small`; the first grows it.
        store.grow(small).unwrap();
        let grown = store.env.info().map_size;
        assert!(grown > small);
        // The second finds the disk allows no more than that, but the map has
        // grown since it looked: it tries again rather than failing.
        store.grow(small).unwrap();
        assert_eq!(store.env.info().map_size, grown);
    }

    #[tokio::test]
    async fn the_map_leaves_the_disk_reserve_free() {
        let dir = TempDir::new().unwrap();
        let reserved = {
            let store = ListingStore::open(dir.path(), u64::MAX).await.unwrap();
            store.env.info().map_size
        };
        let unreserved = {
            let store = ListingStore::open(dir.path(), 0).await.unwrap();
            store.env.info().map_size
        };
        assert_eq!(
            reserved,
            compute_map_size(&dir.path().join("listings.mdb"), u64::MAX).unwrap()
        );
        assert!(reserved <= unreserved);

        // A full map on a disk with no room beyond the reserve stays full.
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), u64::MAX).await.unwrap();
        let full = store.env.info().map_size;
        let error = store.grow(full).unwrap_err();
        assert!(error.to_string().contains("no room"), "{error}");
    }

    #[derive(Serialize)]
    struct V1Payment {
        tx_hash: [u8; 32],
        block_number: u64,
        block_timestamp: u64,
        log_index: u64,
        quote: Vec<u8>,
    }

    #[derive(Serialize)]
    struct V1Listing {
        address: XorName,
        metadata: Metadata,
        size: Option<u64>,
        payment: V1Payment,
    }

    /// A version 1 listing of `address`, paid in `block`.
    fn v1_listing_of(address: XorName, block: u64, quote: &[u8]) -> V1Listing {
        V1Listing {
            address,
            metadata: Metadata {
                content_type: Some("text/plain".into()),
                name: Some("a.txt".into()),
            },
            size: Some(30),
            payment: V1Payment {
                tx_hash: [7; 32],
                block_number: block,
                block_timestamp: 1_790_000_000 + block,
                log_index: 3,
                quote: quote.to_vec(),
            },
        }
    }

    /// Write `listings`, and `raw` records, under `root` as version 1 wrote
    /// them.
    fn write_v1_store(root: &Path, listings: &[V1Listing], raw: &[([u8; 16], Vec<u8>)]) {
        let path = root.join("listings.mdb");
        std::fs::create_dir_all(&path).unwrap();
        #[allow(unsafe_code)]
        // SAFETY: only this test opens this directory.
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1 << 30)
                .max_dbs(2)
                .open(&path)
                .unwrap()
        };
        let mut wtxn = env.write_txn().unwrap();
        let by_key: Database<Bytes, Bytes> =
            env.create_database(&mut wtxn, Some(V1_BY_KEY)).unwrap();
        let by_address: Database<Bytes, Bytes> =
            env.create_database(&mut wtxn, Some(V1_BY_ADDRESS)).unwrap();
        for listing in listings {
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&listing.payment.block_number.to_be_bytes());
            key[8..].copy_from_slice(&listing.payment.log_index.to_be_bytes());
            by_key
                .put(&mut wtxn, &key, &postcard::to_stdvec(listing).unwrap())
                .unwrap();
            by_address.put(&mut wtxn, &listing.address, &key).unwrap();
        }
        for (key, value) in raw {
            by_key.put(&mut wtxn, key, value).unwrap();
        }
        wtxn.commit().unwrap();
        env.prepare_for_closing().wait();
    }

    /// How many records the version 1 databases under `store` still hold.
    fn v1_records(store: &ListingStore) -> (u64, u64) {
        let rtxn = store.env.read_txn().unwrap();
        let count = |name| {
            let table: Option<Database<Bytes, Bytes>> =
                store.env.open_database(&rtxn, Some(name)).unwrap();
            table.map_or(0, |table| table.len(&rtxn).unwrap())
        };
        (count(V1_BY_KEY), count(V1_BY_ADDRESS))
    }

    #[tokio::test]
    async fn moves_a_version_1_store_into_the_index() {
        let dir = TempDir::new().unwrap();
        let quote = rmp_serde::to_vec(&signed_quote([1; 32], 0, 87)).unwrap();
        let v1 = v1_listing_of([1; 32], 10, &quote);
        // With a record that doesn't decode, dropped.
        write_v1_store(dir.path(), &[v1], &[([0u8; 16], vec![0xff, 0xff])]);

        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let moved = store.get([1; 32]).await.unwrap().unwrap();
        assert_eq!(moved.flags, Flags::V1);
        assert_eq!(moved.payment.quote, Some(quote));
        assert_eq!(moved.payment.log_index, Some(3));
        assert_eq!(moved.first_seen, None);
        assert_eq!(
            store.range(ListingKey::MIN, 10).await.unwrap().0,
            vec![moved.clone()]
        );
        assert_eq!(v1_records(&store), (0, 0));
        drop(store);
        // Reopening doesn't move anything twice.
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert_eq!(store.range(ListingKey::MIN, 10).await.unwrap().0.len(), 1);
    }

    /// A version 1 store of `count` listings under `root`, all with one
    /// quote, and the size of its data file.
    fn many_v1_listings(root: &Path, count: u32) -> u64 {
        let quote = rmp_serde::to_vec(&signed_quote([1; 32], 0, 87)).unwrap();
        let listings: Vec<V1Listing> = (0..count)
            .map(|i| {
                let mut address = [1u8; 32];
                address[..4].copy_from_slice(&i.to_be_bytes());
                v1_listing_of(address, u64::from(i), &quote)
            })
            .collect();
        write_v1_store(root, &listings, &[]);
        std::fs::metadata(root.join("listings.mdb").join("data.mdb"))
            .unwrap()
            .len()
    }

    /// The store under `root` as [`ListingStore::open`] opens it on a disk
    /// with no room beyond the reserve, before moving anything: its map at
    /// `map_size` bytes, never growing.
    fn open_without_room(root: &Path, map_size: usize) -> ListingStore {
        let store =
            ListingStore::open_env(root.join("listings.mdb"), u64::MAX, Some(map_size)).unwrap();
        store
            .map_ceiling
            .store(map_size, std::sync::atomic::Ordering::Relaxed);
        store
    }

    /// How many addresses have a listing in `store`.
    async fn addresses_held(store: &ListingStore) -> usize {
        let mut held = 0;
        let mut after = None;
        loop {
            let batch = store.addresses_after(after, 256).await.unwrap();
            let Some(&last) = batch.last() else {
                return held;
            };
            held += batch.len();
            after = Some(last);
        }
    }

    #[tokio::test]
    async fn a_version_1_store_is_moved_with_little_room_to_spare() {
        // A full node's disk: nothing beyond the reserve, so the map can't
        // grow past a quarter more than the version 1 listings take. Moved
        // in one transaction, they would need half as much again.
        let dir = TempDir::new().unwrap();
        let count = 600;
        let v1_size = usize::try_from(many_v1_listings(dir.path(), count)).unwrap();
        let page = page_size::get();
        let map_size = (v1_size + v1_size / 4).div_ceil(page) * page;
        let store = open_without_room(dir.path(), map_size);
        store.migrate_v1(50).unwrap();

        assert_eq!(store.env.info().map_size, map_size);
        assert_eq!(v1_records(&store), (0, 0));
        assert_eq!(
            addresses_held(&store).await,
            usize::try_from(count).unwrap()
        );
        let mut first = [1u8; 32];
        first[..4].copy_from_slice(&0u32.to_be_bytes());
        assert!(store.get(first).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn a_version_1_store_that_cant_be_moved_waits_for_the_next_start() {
        let dir = TempDir::new().unwrap();
        let count = 200;
        let v1_size = usize::try_from(many_v1_listings(dir.path(), count)).unwrap();
        // Room for the new databases and a few small listings, not a batch.
        let page = page_size::get();
        let map_size = v1_size.div_ceil(page) * page + 32 * page;
        let store = open_without_room(dir.path(), map_size);
        store.migrate_v1_or_keep();

        // Opened all the same, nothing lost, and new listings still taken.
        assert_eq!(v1_records(&store), (u64::from(count), u64::from(count)));
        let mut small = unindexed_listing(0xee);
        small.payment = ListingPayment::default();
        small.flags.shown = ShownFields::NONE;
        assert!(store.insert(small.clone()).await.unwrap());
        assert_eq!(store.get(small.address).await.unwrap(), Some(small));
        drop(store);

        // Room again: moved at the next start.
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert_eq!(v1_records(&store), (0, 0));
        assert_eq!(
            addresses_held(&store).await,
            usize::try_from(count).unwrap() + 1
        );
        // Each moved listing is recorded in the change feed as it moves.
        let served = served(&store).await;
        assert_eq!(served.len(), usize::try_from(count).unwrap());
        assert_eq!(entries_in(&feed_of(&store).1), served);
    }

    /// The change feed `store` holds: its state, and its records in order.
    /// Checks on the way that each row of `changes` is its address's record
    /// in `change_of` and the other way round, so the feed holds one row per
    /// address, and that the head is the latest record's number.
    fn feed_of(store: &ListingStore) -> (FeedState, Vec<FeedChange>) {
        let rtxn = store.env.read_txn().unwrap();
        let state = store.feed_state(&rtxn).unwrap().unwrap();
        let mut records: Vec<FeedChange> = Vec::new();
        for row in store.changes.iter(&rtxn).unwrap() {
            let (key, address) = row.unwrap();
            let seq = u64::from_be_bytes(key.try_into().unwrap());
            let address = XorName::try_from(address).unwrap();
            let record = store.change_of.get(&rtxn, &address).unwrap().unwrap();
            let change = decode_change(record).unwrap();
            assert_eq!((change.seq(), change.address()), (seq, address));
            records.push(change);
        }
        assert_eq!(
            store.change_of.len(&rtxn).unwrap(),
            u64::try_from(records.len()).unwrap()
        );
        assert_eq!(records.last().map_or(0, FeedChange::seq), state.head());
        (state, records)
    }

    /// The digest entries of the listings `store` serves in the index, as
    /// list requests return them, in address order.
    async fn served(store: &ListingStore) -> Vec<DigestEntry> {
        let (listings, more) = store.range(ListingKey::MIN, usize::MAX).await.unwrap();
        assert!(!more);
        let mut entries: Vec<DigestEntry> = listings.iter().map(DigestEntry::new).collect();
        entries.sort_unstable_by_key(|entry| entry.address);
        entries
    }

    /// The entries among `records`, in address order.
    fn entries_in(records: &[FeedChange]) -> Vec<DigestEntry> {
        let mut entries: Vec<DigestEntry> = records
            .iter()
            .filter_map(FeedChange::entry)
            .copied()
            .collect();
        entries.sort_unstable_by_key(|entry| entry.address);
        entries
    }

    /// Check the change feed's totals against a count of `served`: those of
    /// every prefix the store keeps them for, and those of ranges of every
    /// size around each entry, as a feed page gives them.
    async fn check_totals(store: &ListingStore, served: &[DigestEntry]) {
        let mut fine = vec![Totals::default(); 1 << 16];
        let mut coarse = vec![Totals::default(); 1 << 8];
        for entry in served {
            fine[fine_prefix(&entry.address)].add(SetHash::of_entry(entry));
            coarse[usize::from(entry.address[0])].add(SetHash::of_entry(entry));
        }
        let (ready, kept_fine, kept_coarse) = {
            let sums = store.sums.lock();
            (sums.ready, sums.fine.clone(), sums.coarse.clone())
        };
        assert!(ready);
        assert!(
            kept_fine == fine,
            "the 16-bit prefixes' totals are not a count's"
        );
        assert!(
            kept_coarse == coarse,
            "the 8-bit prefixes' totals are not a count's"
        );
        let (state, _) = feed_of(store);
        let mut ranges = vec![
            AddressRange::ALL,
            AddressRange::new([0; 32], 16),
            AddressRange::new([0xff; 32], 3),
            AddressRange::new([0x5a; 32], 200),
        ];
        for entry in served {
            for bits in [1, 7, 8, 9, 15, 16, 17, 24, 255] {
                ranges.push(AddressRange::new(entry.address, bits));
            }
        }
        for range in ranges {
            let page = store
                .feed_page(state.epoch, state.head(), range, 256, 4_096)
                .await
                .unwrap()
                .unwrap();
            let inside: Vec<&DigestEntry> = served
                .iter()
                .filter(|entry| range.contains(&entry.address))
                .collect();
            assert_eq!(
                page.count,
                u64::try_from(inside.len()).unwrap(),
                "{range:?}"
            );
            assert_eq!(page.set_hash, SetHash::of(inside), "{range:?}");
        }
    }

    /// A listing at `address` in the index, paid at `time`, showing every
    /// payment field but the quote.
    fn listed_at(address: XorName, time: u64) -> Listing {
        Listing {
            address,
            flags: Flags {
                shown: ShownFields::ALL.without(PaymentField::Quote),
                in_index: true,
                withheld: false,
            },
            metadata: Metadata::default(),
            size: Some(10),
            payment: ListingPayment {
                tx_hash: Some([0x5a; 32]),
                block_number: Some(time - 1_789_000_000),
                block_timestamp: Some(time),
                log_index: Some(0),
                quote: None,
            },
            first_seen: None,
        }
    }

    #[tokio::test]
    async fn the_feed_records_each_change_to_what_is_served() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let (state, records) = feed_of(&store);
        assert!(state.numbered);
        assert_eq!((state.head(), records), (0, Vec::new()));
        let entry = |seq, listing: &Listing| FeedChange::Entry {
            seq,
            entry: DigestEntry::new(listing),
        };
        let removed = |seq, address: u8| FeedChange::Removed {
            seq,
            address: [address; 32],
        };

        // In the index: an entry, with the quote as the store rebuilds it.
        let a = indexed(1, 1_790_000_010, 10, 0);
        assert!(store.insert(a.clone()).await.unwrap());
        assert_eq!(feed_of(&store).1, vec![entry(1, &a)]);
        // Outside the index, or provisional: not served, so no record.
        assert!(store.insert(unindexed_listing(2)).await.unwrap());
        let b = indexed(3, 1_790_000_020, 20, 0);
        assert!(store.insert_provisional(b.clone()).await.unwrap());
        assert_eq!(feed_of(&store).1, vec![entry(1, &a)]);
        // Settled: served from now on.
        assert!(store.settle([3; 32]).await.unwrap());
        assert_eq!(feed_of(&store).1, vec![entry(1, &a), entry(2, &b)]);

        // Replaced by another listing: a new record, and the old row goes.
        let a2 = indexed(1, 1_790_000_030, 30, 0);
        assert!(store.replace(a2.clone(), false).await.unwrap());
        assert_eq!(feed_of(&store).1, vec![entry(2, &b), entry(3, &a2)]);
        // By the same listing: nothing served changed, so no record.
        assert!(store.replace(a2.clone(), false).await.unwrap());
        assert_eq!(feed_of(&store).0.head(), 3);
        // By a provisional one: no longer served. Then settled by a replace.
        assert!(store.replace(a2.clone(), true).await.unwrap());
        assert_eq!(feed_of(&store).1, vec![entry(2, &b), removed(4, 1)]);
        assert!(store.replace(a2.clone(), false).await.unwrap());
        assert_eq!(feed_of(&store).1, vec![entry(2, &b), entry(5, &a2)]);
        // Moved out of the index.
        assert!(store.replace(unindexed_listing(3), false).await.unwrap());
        assert_eq!(feed_of(&store).1, vec![entry(5, &a2), removed(6, 3)]);

        // A copy dated earlier is another entry: each copy's own first_seen
        // dates it.
        let hidden = Flags {
            shown: ShownFields::NONE,
            in_index: true,
            withheld: false,
        };
        let c = listing(4, 1_790_003_700, 0, 0, hidden);
        assert!(store.insert(c.clone()).await.unwrap());
        let mut earlier = c.clone();
        earlier.first_seen = c.first_seen.map(|first_seen| first_seen - 3_600);
        assert!(store.replace(earlier.clone(), false).await.unwrap());
        assert_eq!(
            DigestEntry::new(&earlier).digest,
            DigestEntry::new(&c).digest
        );
        assert_eq!(
            feed_of(&store).1,
            vec![entry(5, &a2), removed(6, 3), entry(8, &earlier)]
        );

        // Removed with its chunk; removing it again changes nothing.
        assert!(store.remove([1; 32]).await.unwrap());
        assert!(!store.remove([1; 32]).await.unwrap());
        // Back again: an entry under the next number.
        assert!(store.insert(a.clone()).await.unwrap());
        let (state, records) = feed_of(&store);
        assert_eq!(
            records,
            vec![removed(6, 3), entry(8, &earlier), entry(10, &a)]
        );
        assert_eq!(state.head(), 10);
        // A failed write records nothing.
        store.fail_writes(true);
        assert!(store.remove([1; 32]).await.is_err());
        store.fail_writes(false);
        assert_eq!(feed_of(&store), (state, records));
        check_totals(&store, &served(&store).await).await;
    }

    #[tokio::test]
    async fn the_feed_keeps_one_row_per_address() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let mut rng = StdRng::seed_from_u64(11);
        let addresses: Vec<XorName> = (0..6).map(|_| rng.gen()).collect();
        for step in 0..200 {
            let address = addresses[rng.gen_range(0..addresses.len())];
            let mut listing = listed_at(address, 1_790_000_000 + rng.gen_range(0..4) * 1_000);
            listing.flags.in_index = rng.gen_bool(0.8);
            let _changed = match rng.gen_range(0..5) {
                0 => store.insert(listing).await.unwrap(),
                1 => store.insert_provisional(listing).await.unwrap(),
                2 => store.replace(listing, rng.gen_bool(0.3)).await.unwrap(),
                3 => store.settle(address).await.unwrap(),
                _ => store.remove(address).await.unwrap(),
            };
            // `feed_of` checks one row per address.
            let (_, records) = feed_of(&store);
            assert_eq!(entries_in(&records), served(&store).await, "step {step}");
        }
        // Many changes, numbered as they came, and still a row per address.
        let (state, records) = feed_of(&store);
        assert!(records.len() <= addresses.len());
        let changes = state.head();
        assert!(
            changes > 3 * u64::try_from(addresses.len()).unwrap(),
            "{changes}"
        );
        check_totals(&store, &served(&store).await).await;
    }

    #[tokio::test]
    async fn the_feed_totals_equal_a_full_count() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        // Addresses at the edges of the prefixes the totals are kept for, and
        // more at random.
        let mut addresses: Vec<XorName> = Vec::new();
        for first in [
            [0x00, 0x00],
            [0x00, 0x01],
            [0x00, 0xff],
            [0x01, 0x00],
            [0x7f, 0xff],
            [0x80, 0x00],
            [0xfe, 0xff],
            [0xff, 0x00],
            [0xff, 0xff],
        ] {
            for _ in 0..3 {
                let mut address: XorName = rng.gen();
                address[..2].copy_from_slice(&first);
                addresses.push(address);
            }
        }
        while addresses.len() < 160 {
            addresses.push(rng.gen());
        }
        for (i, address) in (0u64..).zip(&addresses) {
            let mut listing = listed_at(*address, 1_790_000_000 + i);
            listing.flags.in_index = i % 7 != 1;
            let kept = if i % 7 == 0 {
                store.insert_provisional(listing).await.unwrap()
            } else {
                store.insert(listing).await.unwrap()
            };
            assert!(kept);
        }
        for (i, address) in (0u64..).zip(&addresses) {
            let _changed = match i % 5 {
                0 => store.settle(*address).await.unwrap(),
                1 => store.remove(*address).await.unwrap(),
                2 => store
                    .replace(listed_at(*address, 1_790_100_000 + i), false)
                    .await
                    .unwrap(),
                _ => false,
            };
        }
        let served = served(&store).await;
        assert!(served.len() > 80, "{}", served.len());
        assert_eq!(entries_in(&feed_of(&store).1), served);
        check_totals(&store, &served).await;

        // Totalled afresh at the next start, the same.
        drop(store);
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        check_totals(&store, &served).await;
    }

    #[tokio::test]
    async fn the_feed_and_its_epoch_survive_a_restart() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        for address in 1..=3u8 {
            store
                .insert(listed_at([address; 32], 1_790_000_000))
                .await
                .unwrap();
        }
        store.remove([2; 32]).await.unwrap();
        let (state, records) = feed_of(&store);
        assert_eq!(state.head(), 4);
        drop(store);

        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert_eq!(feed_of(&store), (state, records));
        // The next change takes the next number, in the same epoch.
        store
            .insert(listed_at([4; 32], 1_790_000_000))
            .await
            .unwrap();
        let (now, records) = feed_of(&store);
        assert_eq!((now.epoch, now.head()), (state.epoch, 5));
        assert_eq!(records.last().map(FeedChange::address), Some([4; 32]));
        check_totals(&store, &served(&store).await).await;
    }

    /// Open the store under `root` as a version without the change feed
    /// does, with its own databases only, run `op` on them in a write
    /// transaction, and close it again.
    fn as_an_older_version(root: &Path, op: impl FnOnce(&mut RwTxn, &OlderTables)) {
        #[allow(unsafe_code)]
        // SAFETY: only this test opens this directory, and the store that
        // had it open is gone.
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1 << 30)
                .max_dbs(10)
                .open(root.join("listings.mdb"))
                .unwrap()
        };
        let mut wtxn = env.write_txn().unwrap();
        let mut table = |name| env.create_database(&mut wtxn, Some(name)).unwrap();
        let tables = OlderTables {
            index: table("index"),
            index_addresses: table("index_addresses"),
        };
        for name in ["unindexed", "quote_keys", "provisional", "pending"] {
            let _: Database<Bytes, Bytes> = table(name);
        }
        op(&mut wtxn, &tables);
        wtxn.commit().unwrap();
        env.prepare_for_closing().wait();
    }

    /// The tables a version without the change feed writes listings in.
    struct OlderTables {
        index: Database<Bytes, Bytes>,
        index_addresses: Database<Bytes, Bytes>,
    }

    #[tokio::test]
    async fn a_store_another_version_wrote_to_is_numbered_afresh() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        for address in [3u8, 1] {
            store
                .insert(listed_at([address; 32], 1_790_000_000))
                .await
                .unwrap();
        }
        store.remove([3; 32]).await.unwrap();
        let (before, records) = feed_of(&store);
        drop(store);

        // A version that only opens the store changes nothing.
        as_an_older_version(dir.path(), |_, _| {});
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert_eq!(feed_of(&store), (before, records));
        drop(store);

        // One that stores a listing, as an older version does, without the
        // feed: the feed is numbered afresh under a new epoch.
        as_an_older_version(dir.path(), |wtxn, tables| {
            let listing = listed_at([2; 32], 1_790_000_100);
            let key = listing.key().to_bytes();
            let record = StoredRecord::new(&listing).encode().unwrap();
            tables.index.put(wtxn, &key, &record).unwrap();
            tables.index_addresses.put(wtxn, &[2; 32], &key).unwrap();
        });
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let (state, records) = feed_of(&store);
        assert_ne!(state.epoch, before.epoch);
        assert!(state.numbered);
        // What is served now, numbered from 1 in address order, and nothing
        // else: no removals carried over.
        let served = served(&store).await;
        assert_eq!(
            served
                .iter()
                .map(|entry| entry.address[0])
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let numbered: Vec<FeedChange> = (1..)
            .zip(&served)
            .map(|(seq, entry)| FeedChange::Entry { seq, entry: *entry })
            .collect();
        assert_eq!(records, numbered);
        check_totals(&store, &served).await;

        // The next start keeps the new epoch.
        drop(store);
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert_eq!(feed_of(&store), (state, records));
    }

    #[tokio::test]
    async fn a_numbering_cut_short_starts_again() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        for address in 1..=3u8 {
            store
                .insert(listed_at([address; 32], 1_790_000_000))
                .await
                .unwrap();
        }
        let (before, _) = feed_of(&store);
        // As if a start had stopped part-way through numbering them.
        let unfinished = FeedState {
            numbered: false,
            ..before
        };
        let writer = store.clone();
        spawn_blocking(move || {
            writer.write(|wtxn, _| {
                writer
                    .change_meta
                    .put(wtxn, FEED_STATE, &unfinished.encode())?;
                Ok(())
            })
        })
        .await
        .unwrap()
        .unwrap();
        // Changes meanwhile are not recorded.
        store
            .insert(listed_at([4; 32], 1_790_000_000))
            .await
            .unwrap();
        assert_eq!(feed_of(&store).1.len(), 3);
        drop(store);

        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let (state, records) = feed_of(&store);
        assert!(state.numbered);
        assert_ne!(state.epoch, before.epoch);
        assert_eq!(state.head(), 4);
        assert_eq!(entries_in(&records), served(&store).await);
    }

    #[tokio::test]
    async fn a_start_that_cant_number_the_feed_leaves_it_to_the_next() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        for address in [1u8, 3] {
            store
                .insert(listed_at([address; 32], 1_790_000_000))
                .await
                .unwrap();
        }
        let (before, _) = feed_of(&store);
        drop(store);
        // Another version stores a listing, without the feed.
        as_an_older_version(dir.path(), |wtxn, tables| {
            let listing = listed_at([2; 32], 1_790_000_100);
            let key = listing.key().to_bytes();
            let record = StoredRecord::new(&listing).encode().unwrap();
            tables.index.put(wtxn, &key, &record).unwrap();
            tables.index_addresses.put(wtxn, &[2; 32], &key).unwrap();
        });

        // A start, as `open` makes it, that can't write while it would
        // number the feed afresh, as on a full disk.
        let store = ListingStore::open_env(dir.path().join("listings.mdb"), 0, None).unwrap();
        let starting = store.clone();
        spawn_blocking(move || {
            starting.fail_writes(true);
            starting.number_feed_or_wait();
            starting.fail_writes(false);
            starting.migrate_v1_or_keep();
            starting.load_sums_or_wait();
        })
        .await
        .unwrap();
        // The node keeps and takes listings, but its feed, which lacks the
        // other version's listing, can't be read, and records nothing: its
        // watcher is told of nothing.
        let told = keep_told(&store, vec![AddressRange::ALL]);
        assert!(store
            .insert(listed_at([4; 32], 1_790_000_000))
            .await
            .unwrap());
        assert!(store.remove([1; 32]).await.unwrap());
        assert_eq!(*told.lock(), Vec::new());
        assert_eq!(
            store
                .feed_page(before.epoch, 0, AddressRange::ALL, 256, 4_096)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .feed_entries(AddressRange::ALL, None, 256)
                .await
                .unwrap(),
            None
        );
        assert_eq!(feed_of(&store).0, before);
        drop(store);

        // The next start numbers it afresh, with the writes made since.
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        let (state, records) = feed_of(&store);
        assert_ne!(state.epoch, before.epoch);
        let served = served(&store).await;
        assert_eq!(
            served
                .iter()
                .map(|entry| entry.address[0])
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        let numbered: Vec<FeedChange> = (1..)
            .zip(&served)
            .map(|(seq, entry)| FeedChange::Entry { seq, entry: *entry })
            .collect();
        assert_eq!(records, numbered);
        check_totals(&store, &served).await;
    }

    #[tokio::test]
    async fn numbering_afresh_goes_in_batches() {
        for count in [5u8, 4] {
            let dir = TempDir::new().unwrap();
            let store = ListingStore::open(dir.path(), 0).await.unwrap();
            for address in (1..=count).rev() {
                store
                    .insert(listed_at([address; 32], 1_790_000_000))
                    .await
                    .unwrap();
            }
            // Provisional and outside the index: not numbered.
            store
                .insert_provisional(listed_at([0xa0; 32], 1_790_000_000))
                .await
                .unwrap();
            store.insert(unindexed_listing(0xb0)).await.unwrap();
            let (before, _) = feed_of(&store);

            assert_eq!(store.number_feed(2).unwrap(), u64::from(count));
            let (state, records) = feed_of(&store);
            assert!(state.numbered);
            assert_ne!(state.epoch, before.epoch);
            let served = served(&store).await;
            let numbered: Vec<FeedChange> = (1..)
                .zip(&served)
                .map(|(seq, entry)| FeedChange::Entry { seq, entry: *entry })
                .collect();
            assert_eq!(records, numbered);
            // Totalled again, as a start does after numbering.
            store.load_sums().unwrap();
            check_totals(&store, &served).await;
        }
    }

    #[tokio::test]
    async fn feed_pages_follow_the_feed() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        for address in [0x10u8, 0x90, 0x20, 0xa0, 0x30] {
            store
                .insert(listed_at([address; 32], 1_790_000_000))
                .await
                .unwrap();
        }
        // Record 6, the removal of 0x10, replaces its first.
        store.remove([0x10; 32]).await.unwrap();
        let (state, records) = feed_of(&store);
        let seqs: Vec<u64> = records.iter().map(FeedChange::seq).collect();
        assert_eq!(seqs, vec![2, 3, 4, 5, 6]);
        let record = |seq: u64| records[records.iter().position(|r| r.seq() == seq).unwrap()];
        let epoch = state.epoch;
        // Addresses whose first bit is 0: 0x10, 0x20 and 0x30.
        let lower = AddressRange::new([0; 32], 1);
        let page = |after, limit, max_read| store.feed_page(epoch, after, lower, limit, max_read);

        // Two at a time, then the rest.
        let first = page(0, 2, 4_096).await.unwrap().unwrap();
        assert_eq!((first.epoch, first.head, first.next), (epoch, 6, 5));
        assert_eq!(first.changes, vec![record(3), record(5)]);
        assert!(first.more());
        let lower_entries = [records[1], records[3]].map(|r| *r.entry().unwrap());
        assert_eq!(first.count, 2);
        assert_eq!(first.set_hash, SetHash::of(&lower_entries));
        let rest = page(5, 2, 4_096).await.unwrap().unwrap();
        assert_eq!((rest.next, rest.changes.clone()), (6, vec![record(6)]));
        assert!(!rest.more());
        // Reading stops after so many records, in the range or not.
        let short = page(0, 256, 2).await.unwrap().unwrap();
        assert_eq!((short.next, short.changes.clone()), (3, vec![record(3)]));
        assert!(short.more());
        // With no room for records, it stops before the first in the range,
        // never passing a record it leaves out.
        let none = page(0, 0, 3).await.unwrap().unwrap();
        assert_eq!((none.next, none.changes.len()), (2, 0));
        assert!(none.more());
        let none = page(3, 0, 3).await.unwrap().unwrap();
        assert_eq!((none.next, none.changes.len()), (4, 0));
        assert!(none.more());
        // At the head, nothing more; another epoch, or a number beyond the
        // head, gets no records and the head.
        for (epoch, after) in [(epoch, 6), (epoch ^ 1, 0), (epoch, 7)] {
            let page = store
                .feed_page(epoch, after, lower, 256, 4_096)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((page.next, page.changes.len(), page.count), (6, 0, 2));
            assert!(!page.more());
        }

        // Entries in address order, removals left out.
        let entry = |address: u8| {
            *records
                .iter()
                .find(|r| r.address() == [address; 32])
                .and_then(FeedChange::entry)
                .unwrap()
        };
        let entries = |range, after, limit| store.feed_entries(range, after, limit);
        assert_eq!(
            entries(AddressRange::ALL, None, 256)
                .await
                .unwrap()
                .unwrap(),
            (
                vec![entry(0x20), entry(0x30), entry(0x90), entry(0xa0)],
                false
            )
        );
        assert_eq!(
            entries(AddressRange::ALL, None, 2).await.unwrap().unwrap(),
            (vec![entry(0x20), entry(0x30)], true)
        );
        assert_eq!(
            entries(AddressRange::ALL, Some([0x30; 32]), 2)
                .await
                .unwrap()
                .unwrap(),
            (vec![entry(0x90), entry(0xa0)], false)
        );
        // After an address before the range: from the range's start.
        let upper = AddressRange::new([0x80; 32], 1);
        assert_eq!(
            entries(upper, Some([0x10; 32]), 256)
                .await
                .unwrap()
                .unwrap(),
            (vec![entry(0x90), entry(0xa0)], false)
        );
        assert_eq!(
            entries(lower, Some([0x90; 32]), 256)
                .await
                .unwrap()
                .unwrap(),
            (Vec::new(), false)
        );

        // A store whose feed isn't numbered can't be read.
        let other = TempDir::new().unwrap();
        let path = other.path().join("listings.mdb");
        std::fs::create_dir_all(&path).unwrap();
        let unnumbered = ListingStore::open_env(path, 0, None).unwrap();
        assert_eq!(
            unnumbered.feed_page(1, 0, lower, 256, 4_096).await.unwrap(),
            None
        );
        assert_eq!(
            unnumbered.feed_entries(lower, None, 256).await.unwrap(),
            None
        );
    }

    /// The bytes the change feed keeps, pinned: a change to them would change
    /// what the stores nodes already hold mean. The entry and removal records
    /// are worked out by hand from postcard's encoding.
    #[test]
    fn feed_records_are_pinned() {
        let entry = DigestEntry {
            address: [0x11; 32],
            time: 1_790_000_000,
            block_number: 81_897_121,
            log_index: 3,
            first_seen: Some(1_789_999_200),
            digest: [0x22; 32],
        };
        let added = FeedChange::Entry { seq: 300, entry };
        // Version, variant 0, seq 300, the address, the time, block and log
        // index, Some(first_seen), the digest.
        let added_hex = format!(
            "0100ac02{}80f7c4d506a1cd862703\
             01e0f0c4d506{}",
            "11".repeat(32),
            "22".repeat(32)
        );
        assert_eq!(hex::encode(encode_change(&added).unwrap()), added_hex);
        assert_eq!(
            decode_change(&hex::decode(&added_hex).unwrap()),
            Some(added)
        );
        let removed = FeedChange::Removed {
            seq: 1,
            address: [0x33; 32],
        };
        // Version, variant 1, seq 1, the address.
        let removed_hex = format!("010101{}", "33".repeat(32));
        assert_eq!(hex::encode(encode_change(&removed).unwrap()), removed_hex);
        assert_eq!(
            decode_change(&hex::decode(&removed_hex).unwrap()),
            Some(removed)
        );
        // Another version, a record cut short or one with more after it:
        // not read.
        let mut other = hex::decode(&removed_hex).unwrap();
        other[0] = 2;
        assert_eq!(decode_change(&other), None);
        let bytes = hex::decode(&removed_hex).unwrap();
        assert_eq!(decode_change(&bytes[..bytes.len() - 1]), None);
        assert_eq!(decode_change(&[bytes.as_slice(), &[0]].concat()), None);

        // The state: version, epoch, next number, numbered.
        let state = FeedState {
            epoch: 0x0123_4567_89ab_cdef,
            next: 301,
            numbered: true,
        };
        assert_eq!(
            hex::encode(state.encode()),
            "010123456789abcdef000000000000012d01"
        );
        assert_eq!(FeedState::decode(&state.encode()), Some(state));
        assert_eq!(state.head(), 300);
        for bad in [
            "020123456789abcdef000000000000012d01",
            "010000000000000000000000000000012d01",
            "010123456789abcdef000000000000000001",
            "010123456789abcdef000000000000012d02",
            "010123456789abcdef000000000000012d",
            "010123456789abcdef000000000000012d0100",
        ] {
            assert_eq!(FeedState::decode(&hex::decode(bad).unwrap()), None, "{bad}");
        }

        // A listing's record, its digest as the listings crate makes it.
        let listing = Listing {
            address: [0x29; 32],
            flags: Flags {
                shown: ShownFields::ALL.without(PaymentField::Quote),
                in_index: true,
                withheld: false,
            },
            metadata: Metadata {
                content_type: Some("text/plain".into()),
                name: Some("notes.txt".into()),
            },
            size: Some(11_999),
            payment: ListingPayment {
                tx_hash: Some([0x5a; 32]),
                block_number: Some(81_897_121),
                block_timestamp: Some(1_790_000_000),
                log_index: Some(3),
                quote: None,
            },
            first_seen: None,
        };
        let entry = DigestEntry::new(&listing);
        // The digest: BLAKE3 of the listing as it travels, without first_seen.
        let wire = postcard::to_stdvec(&listing).unwrap();
        assert_eq!(entry.digest, *blake3::hash(&wire).as_bytes());
        let record = FeedChange::Entry { seq: 1, entry };
        assert_eq!(
            hex::encode(encode_change(&record).unwrap()),
            format!(
                "010001{}80f7c4d506a1cd86270300\
                 e87050248609ea5cecef0ed73f9bb95a3f80985dc703b7fca10d3c7291b07b3f",
                "29".repeat(32)
            )
        );
    }

    /// What the feed's watcher was told of one write.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Told {
        epoch: u64,
        head: u64,
        records: Vec<FeedChange>,
        /// The totals of each range watched, at the write's head.
        totals: Vec<(u64, SetHash)>,
    }

    /// Set a watcher on `store`'s feed that keeps what it is told, with the
    /// totals of `ranges`.
    fn keep_told(store: &ListingStore, ranges: Vec<AddressRange>) -> Arc<Mutex<Vec<Told>>> {
        let told = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&told);
        store.watch_feed(Box::new(move |commit| {
            let totals = ranges
                .iter()
                .map(|range| commit.totals(range).unwrap())
                .collect();
            kept.lock().push(Told {
                epoch: commit.epoch,
                head: commit.head,
                records: commit.records.to_vec(),
                totals,
            });
        }));
        told
    }

    #[tokio::test]
    async fn the_watcher_is_told_of_each_write_that_adds_records() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        // The whole space, a range the prefix totals cover, and one finer
        // than they go, which is counted from the store.
        let ranges = vec![
            AddressRange::ALL,
            AddressRange::new([0x10; 32], 4),
            AddressRange::new([0x10; 32], 24),
        ];
        let told = keep_told(&store, ranges.clone());
        let epoch = feed_of(&store).0.epoch;

        // An entry, then a listing outside the index, a provisional one and
        // a mark, none of which the feed records, then a removal.
        let a = listed_at([0x10; 32], 1_790_000_000);
        store.insert(a.clone()).await.unwrap();
        store.insert(unindexed_listing(0x20)).await.unwrap();
        store
            .insert_provisional(listed_at([0x30; 32], 1_790_000_000))
            .await
            .unwrap();
        store.mark_pending([0x40; 32]).await.unwrap();
        store.remove([0x10; 32]).await.unwrap();
        let entry = DigestEntry::new(&a);
        let with_a = (1, SetHash::of([&entry]));
        let empty = (0, SetHash::EMPTY);
        assert_eq!(
            std::mem::take(&mut *told.lock()),
            vec![
                Told {
                    epoch,
                    head: 1,
                    records: vec![FeedChange::Entry { seq: 1, entry }],
                    totals: vec![with_a; 3],
                },
                Told {
                    epoch,
                    head: 2,
                    records: vec![FeedChange::Removed {
                        seq: 2,
                        address: [0x10; 32],
                    }],
                    totals: vec![empty; 3],
                },
            ]
        );

        // One write that records several: told once, of all of them, with
        // the totals a read of the feed gives at its head.
        let many: Vec<Listing> = [0x11u8, 0x90, 0x12]
            .iter()
            .map(|&address| listed_at([address; 32], 1_790_000_000))
            .collect();
        let writer = store.clone();
        let batch = many.clone();
        spawn_blocking(move || writer.insert_all(&batch))
            .await
            .unwrap();
        let told = std::mem::take(&mut *told.lock());
        assert_eq!(told.len(), 1);
        let (state, records) = feed_of(&store);
        assert_eq!((told[0].epoch, told[0].head), (epoch, state.head()));
        assert_eq!(told[0].records, records[records.len() - 3..].to_vec());
        for (range, totals) in ranges.iter().zip(&told[0].totals) {
            let page = store
                .feed_page(epoch, state.head(), *range, 0, 1)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(*totals, (page.count, page.set_hash), "{range:?}");
        }
        assert_eq!(told[0].totals[1].0, 2);
    }

    #[tokio::test]
    async fn pending_marks_keep_when_rechecks_began() {
        let dir = TempDir::new().unwrap();
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        store.mark_pending([1; 32]).await.unwrap();
        store.mark_pending([2; 32]).await.unwrap();
        store.mark_rechecking([2; 32], 1_790_000_000).await.unwrap();
        // Marked by the re-checks alone.
        store.mark_rechecking([3; 32], 1_790_000_100).await.unwrap();
        let expected = vec![
            ([1; 32], None),
            ([2; 32], Some(1_790_000_000)),
            ([3; 32], Some(1_790_000_100)),
        ];
        assert_eq!(store.pending_checks().await.unwrap(), expected);
        assert_eq!(
            store.pending_addresses().await.unwrap(),
            vec![[1; 32], [2; 32], [3; 32]]
        );
        // As kept: empty, as every version marks an envelope, or the time,
        // 8 bytes big-endian. A version that reads only the addresses reads
        // these as it always has.
        {
            let rtxn = store.env.read_txn().unwrap();
            let kept = |address: XorName| store.pending.get(&rtxn, &address).unwrap().unwrap();
            assert_eq!(kept([1; 32]), b"");
            assert_eq!(hex::encode(kept([2; 32])), "000000006ab13b80");
        }

        // Kept across a restart. Stored again, an envelope's checks start
        // afresh; checked, its mark goes.
        drop(store);
        let store = ListingStore::open(dir.path(), 0).await.unwrap();
        assert_eq!(store.pending_checks().await.unwrap(), expected);
        store.mark_pending([2; 32]).await.unwrap();
        store.clear_pending([3; 32]).await.unwrap();
        assert_eq!(
            store.pending_checks().await.unwrap(),
            vec![([1; 32], None), ([2; 32], None)]
        );
    }

    /// How many listings one write of the benchmark's fill takes.
    const BENCH_FILL_BATCH: usize = 10_000;

    /// A listing at a random address, in the index, every payment field
    /// shown, its paid quote's bytes `quote`: as most listings on the network
    /// are, about 3.6 KB stored.
    fn bench_listing(rng: &mut StdRng, n: u64, quote: &[u8]) -> Listing {
        Listing {
            address: rng.gen(),
            flags: Flags::V1,
            metadata: Metadata {
                content_type: Some("text/plain".into()),
                name: Some(format!("file-{n}.txt")),
            },
            size: Some(1_000 + n),
            payment: ListingPayment {
                tx_hash: Some(rng.gen()),
                block_number: Some(80_000_000 + n),
                block_timestamp: Some(1_790_000_000 + n),
                log_index: Some(0),
                quote: Some(quote.to_vec()),
            },
            first_seen: None,
        }
    }

    /// The median and the longest of `runs` runs of `op`.
    fn timed<T>(runs: usize, mut op: impl FnMut() -> T) -> (Duration, Duration) {
        let mut times: Vec<Duration> = (0..runs)
            .map(|_| {
                let started = std::time::Instant::now();
                std::hint::black_box(op());
                started.elapsed()
            })
            .collect();
        times.sort_unstable();
        (times[times.len() / 2], times[times.len() - 1])
    }

    /// Bytes `table` takes in `store`'s map: its pages, of every kind.
    fn table_bytes(store: &ListingStore, table: Database<Bytes, Bytes>) -> u64 {
        let rtxn = store.env.read_txn().unwrap();
        let stat = table.stat(&rtxn).unwrap();
        let pages = stat.branch_pages + stat.leaf_pages + stat.overflow_pages;
        u64::from(stat.page_size) * u64::try_from(pages).unwrap()
    }

    /// The change feed at scale: a store with `ANT_LISTINGS_BENCH` synthetic
    /// listings (100,000 if unset), filled as a version without the feed
    /// leaves them, then the first start's numbering of them, the rebuild of
    /// the prefix sums each start makes, `ChangesRequest` and `RangeList`
    /// pages as the node reads them, and writes with their feed rows.
    /// Nothing joins a network. Run by hand, in release mode, as
    ///
    /// ```text
    /// ANT_LISTINGS_BENCH=1000000 cargo test --release --lib \
    ///     listings::store::tests::the_change_feed_at_scale -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "a benchmark of the change feed at scale, run by hand"]
    #[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
    fn the_change_feed_at_scale() {
        let count: u64 = std::env::var("ANT_LISTINGS_BENCH")
            .ok()
            .and_then(|count| count.parse().ok())
            .unwrap_or(100_000);
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("listings.mdb");
        std::fs::create_dir_all(&path).unwrap();
        let report = |what: &str, time: Duration| println!("{what:<58} {time:>12.3?}");
        println!("The change feed with {count} listings");

        // Filled as a version without the feed leaves a store: listings in
        // the index, no feed, and no write recorded as this version's.
        let store = ListingStore::open_env(path.clone(), 0, None).unwrap();
        store.feed_behind.store(true, Ordering::Relaxed);
        let quote = rmp_serde::to_vec(&signed_quote([0; 32], 0, 87)).unwrap();
        let mut rng = StdRng::seed_from_u64(1);
        let started = std::time::Instant::now();
        let mut batch = Vec::with_capacity(BENCH_FILL_BATCH);
        for n in 0..count {
            batch.push(bench_listing(&mut rng, n, &quote));
            if batch.len() == BENCH_FILL_BATCH || n + 1 == count {
                store.insert_all(&batch);
                batch.clear();
            }
        }
        report("Fill, without the feed (setup)", started.elapsed());
        let listings_bytes = [store.index, store.index_addresses, store.quote_keys]
            .into_iter()
            .map(|table| table_bytes(&store, table))
            .sum::<u64>();
        drop(store);

        // The first start of this version: the check, the numbering of every
        // listing served in a new epoch, and the totals.
        let started = std::time::Instant::now();
        let store = ListingStore::open_env(path.clone(), 0, None).unwrap();
        let why = store.feed_needs_numbering().unwrap();
        assert_eq!(why, Some("the feed is new"));
        let numbering = std::time::Instant::now();
        assert_eq!(store.number_feed(FEED_BATCH).unwrap(), count);
        let numbering = numbering.elapsed();
        store.migrate_v1_or_keep();
        let totalling = std::time::Instant::now();
        store.load_sums().unwrap();
        let totalling = totalling.elapsed();
        report(
            "First start: open, number and total, in all",
            started.elapsed(),
        );
        report("  numbering every listing in the feed", numbering);
        report("  totalling the prefix sums", totalling);
        let feed_bytes = [store.changes, store.change_of, store.change_meta]
            .into_iter()
            .map(|table| table_bytes(&store, table))
            .sum::<u64>();
        drop(store);

        // A start after that: the feed is the store's, so only its totals
        // are rebuilt.
        let started = std::time::Instant::now();
        let store = ListingStore::open_env(path, 0, None).unwrap();
        store.number_feed_or_wait();
        store.migrate_v1_or_keep();
        store.load_sums_or_wait();
        report("A later start: open and total", started.elapsed());
        let (median, longest) = timed(5, || store.load_sums().unwrap());
        report("  rebuilding the prefix sums, median", median);
        report("  rebuilding the prefix sums, longest", longest);
        let state = {
            let rtxn = store.env.read_txn().unwrap();
            store.feed_state(&rtxn).unwrap().unwrap()
        };
        assert!(state.numbered);
        assert_eq!(state.head(), count);

        // ChangesRequest pages, as the node reads them: at most 256 records
        // from at most 4,096 read.
        let (epoch, head) = (state.epoch, state.head());
        let mut leaf: XorName = rng.gen();
        leaf[0] = 0x5a;
        let pages = [
            ("ALL, from the start", AddressRange::ALL, 0),
            ("ALL, from the middle", AddressRange::ALL, head / 2),
            (
                "ALL, 10 records before the head",
                AddressRange::ALL,
                head - 10,
            ),
            (
                "ALL, at the head (a poll with nothing new)",
                AddressRange::ALL,
                head,
            ),
            ("8-bit range, from the start", AddressRange::new(leaf, 8), 0),
            (
                "16-bit range, from the start",
                AddressRange::new(leaf, 16),
                0,
            ),
            (
                "16-bit range, at the head",
                AddressRange::new(leaf, 16),
                head,
            ),
            (
                "24-bit range, from the start",
                AddressRange::new(leaf, 24),
                0,
            ),
            (
                "24-bit range, at the head",
                AddressRange::new(leaf, 24),
                head,
            ),
        ];
        println!("ChangesRequest pages (median of 21):");
        for (what, range, after) in pages {
            let page = store
                .feed_page_blocking(epoch, after, range, 256, 4_096)
                .unwrap()
                .unwrap();
            let (median, _) = timed(21, || {
                store
                    .feed_page_blocking(epoch, after, range, 256, 4_096)
                    .unwrap()
            });
            let read = page.next - after.min(page.next);
            let what = format!(
                "  {what}: {} records, {read} read, count {}",
                page.changes.len(),
                page.count
            );
            report(&what, median);
        }

        // RangeList pages: at most 256 entries in address order.
        let middle: XorName = [0x80; 32];
        let lists = [
            ("ALL, from the start", AddressRange::ALL, None),
            ("ALL, from the middle", AddressRange::ALL, Some(middle)),
            (
                "8-bit range, from the start",
                AddressRange::new(leaf, 8),
                None,
            ),
            (
                "16-bit range, from the start",
                AddressRange::new(leaf, 16),
                None,
            ),
        ];
        println!("RangeList pages (median of 21):");
        for (what, range, after) in lists {
            let (entries, _) = store
                .feed_entries_blocking(range, after, 256)
                .unwrap()
                .unwrap();
            let (median, _) = timed(21, || {
                store.feed_entries_blocking(range, after, 256).unwrap()
            });
            report(&format!("  {what}: {} entries", entries.len()), median);
        }

        // Writes as a node records new listings, one to a transaction: with
        // their feed rows, and without, as a version without the feed.
        let writes = 1_000u32;
        let mut write_all = |store: &ListingStore| {
            let started = std::time::Instant::now();
            for n in 0..writes {
                let listing = bench_listing(&mut rng, count + u64::from(n), &quote);
                assert!(store.insert_blocking(&listing, false).unwrap());
            }
            started.elapsed() / writes
        };
        report("A write with its feed row, mean", write_all(&store));
        store.feed_behind.store(true, Ordering::Relaxed);
        report("A write without the feed, mean", write_all(&store));

        let per = |bytes: u64| bytes as f64 / count as f64;
        println!(
            "Disk: listings {:.0} B each, the feed {:.0} B each; data file {} MB",
            per(listings_bytes),
            per(feed_bytes),
            store.env.real_disk_size().unwrap() / 1_000_000
        );
        println!(
            "Memory: the prefix sums take {} KB, whatever the count",
            std::mem::size_of::<Totals>() * ((1 << 16) + (1 << 8)) / 1_000
        );
    }
}

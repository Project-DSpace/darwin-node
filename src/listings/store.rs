//! Persistent listings, backed by LMDB.
//!
//! ```text
//! {root}/listings.mdb/   -- LMDB environment directory
//! ```
//!
//! Six databases:
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
//!   close group, so the checks go on after a restart.
//!
//! The map is sized from the disk, as the chunk store's is, leaving the
//! operator's disk reserve free, and grows when it fills. A store written by
//! version 1 (`by_key` and `by_address`) is moved into these databases when it
//! is opened, a batch at a time; what can't be moved then, as on a full disk,
//! is moved at a later start.

use crate::ant_protocol::XorName;
use crate::error::{Error, Result};
use crate::logging::{debug, info, warn};
use crate::storage::lmdb::compute_map_size;
use ::ant_protocol::evm::PaymentQuote;
use ant_listings::{Flags, Listing, ListingKey, ListingPayment, ListingV1, Metadata};
use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions, MdbError, RoTxn, RwTxn};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::spawn_blocking;

/// First byte of every stored record: the format below.
const RECORD_VERSION: u8 = 2;

/// Names of the version 1 databases, read once to migrate them.
const V1_BY_KEY: &str = "by_key";
const V1_BY_ADDRESS: &str = "by_address";

/// How many version 1 listings one write transaction moves. A transaction
/// can't reuse the pages the one before it freed, so moving needs room for
/// about two batches of new records beyond the version 1 data: about 2 MiB.
const V1_BATCH: usize = 256;

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
    /// Open or create the store at `{root_dir}/listings.mdb/`, moving any
    /// version 1 listings into it. Its map never grows into the last
    /// `disk_reserve` bytes of the disk, as the chunk store's doesn't.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB environment or its databases can't be
    /// opened or created. Version 1 listings that can't be moved now, as on
    /// a full disk, are no error: they are kept to be moved at the next
    /// start.
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
            store.migrate_v1_or_keep();
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
                .max_dbs(10)
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
            store.write(|wtxn| {
                if !store.remove_in(wtxn, &listing.address)? {
                    return Ok(false);
                }
                store.insert_in(wtxn, &listing, &record)?;
                if provisional {
                    store.provisional.put(wtxn, &listing.address, &[])?;
                }
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
        spawn_blocking(move || store.write(|wtxn| Ok(store.provisional.delete(wtxn, &address)?)))
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
        spawn_blocking(move || store.write(|wtxn| Ok(store.pending.put(wtxn, &address, &[])?)))
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
            store.write(|wtxn| {
                store.pending.delete(wtxn, &address)?;
                Ok(())
            })
        })
        .await
        .map_err(join_error)?
    }

    /// Every envelope marked pending.
    ///
    /// # Errors
    ///
    /// Returns an error if the LMDB read fails.
    pub async fn pending_addresses(&self) -> Result<Vec<XorName>> {
        self.addresses_in(self.pending).await
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
        self.write(|wtxn| {
            if self.contains_in(wtxn, &listing.address)? {
                return Ok(false);
            }
            self.insert_in(wtxn, listing, &record)?;
            if provisional {
                self.provisional.put(wtxn, &listing.address, &[])?;
            }
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
        self.write(|wtxn| self.remove_in(wtxn, address))
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
    /// trying again whenever it is full and could be grown.
    fn write<T>(&self, op: impl Fn(&mut RwTxn) -> std::result::Result<T, TxnError>) -> Result<T> {
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
                        let value = op(&mut wtxn)?;
                        wtxn.commit()?;
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
                moved += self.write(|wtxn| {
                    let mut moved = 0usize;
                    for ((key, _), listing) in old.iter().zip(&listings) {
                        if let Some((listing, record)) = listing {
                            if !self.contains_in(wtxn, &listing.address)? {
                                self.insert_in(wtxn, listing, record)?;
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
            self.write(|wtxn| Ok(old_by_address.clear(wtxn)?))?;
        }
        if read > 0 {
            info!("Moved {moved} of {read} version 1 listing(s) into the new listing store");
        }
        Ok(())
    }
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
                .write(|wtxn| {
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
    }
}

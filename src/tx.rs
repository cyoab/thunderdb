//! Summary: Read and write transaction types.
//! Copyright (c) YOAB. All rights reserved.

use std::ops::RangeBounds;

use crate::btree::{BTree, BTreeIter, BTreeRangeIter, Bound};
use crate::bucket::{self, BucketRef, NestedBucketRef, bucket_exists, list_buckets};
use crate::db::Database;
use crate::error::{Error, Result};
use crate::iter::{IterOptions, MetricsIter, PrefetchIter};
use crate::value::{BorrowedValue, OwnedValue};

/// A read-only transaction.
///
/// Provides a consistent snapshot view of the database at the time
/// the transaction was started. Read transactions never block other
/// read transactions.
///
/// # Lifetime
///
/// The transaction holds a reference to the database and must not
/// outlive it.
pub struct ReadTx<'db> {
    db: &'db Database,
}

impl<'db> ReadTx<'db> {
    /// Creates a new read transaction.
    pub(crate) fn new(db: &'db Database) -> Self {
        Self { db }
    }

    /// Retrieves the value associated with the given key.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Note
    ///
    /// This method clones the value. For zero-copy access, use [`get_ref()`](Self::get_ref).
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Fast path: bloom filter says key definitely not present.
        if !self.db.may_contain_key(key) {
            return None;
        }
        self.db.tree().get(key).map(|v| v.to_vec())
    }

    /// Retrieves a reference to the value associated with the given key.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Zero-Copy
    ///
    /// Unlike [`get()`](Self::get), this method returns a reference to the value
    /// without copying. The reference is valid for the lifetime of the transaction.
    ///
    /// Use this method when you need to read large values or when you want to
    /// avoid allocation overhead.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let rtx = db.read_tx();
    /// if let Some(value) = rtx.get_ref(b"large_key") {
    ///     // value is a &[u8], no allocation occurred
    ///     println!("value length: {}", value.len());
    /// }
    /// ```
    #[inline]
    pub fn get_ref(&self, key: &[u8]) -> Option<&[u8]> {
        // Fast path: bloom filter says key definitely not present.
        if !self.db.may_contain_key(key) {
            return None;
        }
        self.db.tree().get(key)
    }

    /// Retrieves a borrowed value with explicit lifetime marker.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Zero-Copy
    ///
    /// This method returns a `BorrowedValue<'_>` type that explicitly
    /// indicates the value is borrowed and cannot outlive the transaction.
    /// This provides compile-time safety guarantees.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let rtx = db.read_tx();
    /// let borrowed: Option<BorrowedValue<'_>> = rtx.get_borrowed(b"key");
    /// // borrowed cannot outlive rtx
    /// ```
    #[inline]
    pub fn get_borrowed(&self, key: &[u8]) -> Option<BorrowedValue<'_>> {
        self.get_ref(key).map(BorrowedValue::new)
    }

    /// Retrieves an owned copy of the value that can outlive the transaction.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Allocation
    ///
    /// This method allocates a new `OwnedValue` containing a copy of the data.
    /// Use `get_ref()` or `get_borrowed()` to avoid allocation when possible.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let owned: Option<OwnedValue>;
    /// {
    ///     let rtx = db.read_tx();
    ///     owned = rtx.get_owned(b"key");
    /// } // Transaction dropped
    ///
    /// // owned is still valid
    /// if let Some(val) = owned {
    ///     println!("value: {:?}", val.as_ref());
    /// }
    /// ```
    #[inline]
    pub fn get_owned(&self, key: &[u8]) -> Option<OwnedValue> {
        self.get_ref(key).map(OwnedValue::from_slice)
    }

    /// Returns a read-only reference to a bucket.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the bucket does not exist.
    /// Returns `InvalidBucketName` if the name is invalid.
    pub fn bucket(&self, name: &[u8]) -> Result<BucketRef<'_>> {
        BucketRef::new(self.db.tree(), name)
    }

    /// Checks if a bucket exists.
    pub fn bucket_exists(&self, name: &[u8]) -> bool {
        bucket_exists(self.db.tree(), name)
    }

    /// Lists all bucket names.
    pub fn list_buckets(&self) -> Vec<Vec<u8>> {
        list_buckets(self.db.tree())
    }

    /// Returns an iterator over all key-value pairs in the database.
    ///
    /// Keys are returned in sorted (lexicographic) order.
    pub fn iter(&self) -> BTreeIter<'_> {
        self.db.tree().iter()
    }

    /// Returns an iterator with custom options for optimized scanning.
    ///
    /// Use this when you need control over prefetching behavior or
    /// want to collect scan metrics.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let options = IterOptions::default()
    ///     .prefetch_count(32);
    /// let iter = rtx.iter_with_options(options);
    /// for (key, value) in iter {
    ///     // Process entries with prefetching
    /// }
    /// ```
    pub fn iter_with_options(&self, options: IterOptions) -> PrefetchIter<'_, BTreeIter<'_>> {
        PrefetchIter::new(self.db.tree().iter(), options.prefetch_count)
    }

    /// Returns an iterator that collects scan metrics.
    ///
    /// Use this for monitoring and performance analysis.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut iter = rtx.iter_with_metrics();
    /// for (key, value) in iter.by_ref() {
    ///     // Process entries
    /// }
    /// let metrics = iter.metrics();
    /// println!("Scanned {} keys in {:?}",
    ///     metrics.keys_scanned,
    ///     std::time::Duration::from_nanos(metrics.scan_duration_ns));
    /// ```
    pub fn iter_with_metrics(&self) -> MetricsIter<BTreeIter<'_>> {
        MetricsIter::new(self.db.tree().iter())
    }

    /// Returns an iterator over a range of key-value pairs.
    ///
    /// The range can be specified using standard Rust range syntax:
    /// - `..` for all keys
    /// - `start..` for keys >= start
    /// - `..end` for keys < end
    /// - `..=end` for keys <= end
    /// - `start..end` for keys >= start and < end
    /// - `start..=end` for keys >= start and <= end
    ///
    /// Keys are returned in sorted (lexicographic) order.
    pub fn range<'a, R>(&'a self, range: R) -> BTreeRangeIter<'a>
    where
        R: RangeBounds<&'a [u8]>,
    {
        let start = match range.start_bound() {
            std::ops::Bound::Unbounded => Bound::Unbounded,
            std::ops::Bound::Included(k) => Bound::Included(k),
            std::ops::Bound::Excluded(k) => Bound::Excluded(k),
        };
        let end = match range.end_bound() {
            std::ops::Bound::Unbounded => Bound::Unbounded,
            std::ops::Bound::Included(k) => Bound::Included(k),
            std::ops::Bound::Excluded(k) => Bound::Excluded(k),
        };
        self.db.tree().range(start, end)
    }

    // ==================== Nested Bucket Methods ====================

    /// Returns a read-only reference to a nested bucket.
    ///
    /// # Arguments
    ///
    /// * `parent` - The name of the parent (top-level) bucket.
    /// * `child` - The name of the nested bucket within the parent.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the parent bucket or nested bucket does not exist.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn nested_bucket(&self, parent: &[u8], child: &[u8]) -> Result<NestedBucketRef<'_>> {
        NestedBucketRef::new(self.db.tree(), &[parent, child])
    }

    /// Returns a read-only reference to a deeply nested bucket by path.
    ///
    /// # Arguments
    ///
    /// * `path` - The full path to the nested bucket, starting with the top-level bucket.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if any bucket in the path does not exist.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn nested_bucket_at_path(&self, path: &[&[u8]]) -> Result<NestedBucketRef<'_>> {
        NestedBucketRef::new(self.db.tree(), path)
    }

    /// Checks if a nested bucket exists.
    ///
    /// # Arguments
    ///
    /// * `parent` - The name of the parent (top-level) bucket.
    /// * `child` - The name of the nested bucket within the parent.
    pub fn nested_bucket_exists(&self, parent: &[u8], child: &[u8]) -> bool {
        bucket::nested_bucket_exists(self.db.tree(), &[parent, child])
    }

    /// Checks if a deeply nested bucket exists by path.
    ///
    /// # Arguments
    ///
    /// * `path` - The full path to the nested bucket, starting with the top-level bucket.
    pub fn nested_bucket_exists_at_path(&self, path: &[&[u8]]) -> bool {
        if path.len() < 2 {
            return false;
        }
        bucket::nested_bucket_exists(self.db.tree(), path)
    }

    /// Lists all nested bucket names directly under a parent bucket.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the parent bucket does not exist.
    /// Returns `InvalidBucketName` if the name is invalid.
    pub fn list_nested_buckets(&self, parent: &[u8]) -> Result<Vec<Vec<u8>>> {
        bucket::validate_bucket_name(parent)?;
        if !bucket_exists(self.db.tree(), parent) {
            return Err(Error::BucketNotFound {
                name: parent.to_vec(),
            });
        }
        Ok(bucket::list_nested_buckets(self.db.tree(), &[parent]))
    }

    /// Lists all nested bucket names directly under a nested bucket by path.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if any bucket in the path does not exist.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn list_nested_buckets_at_path(&self, path: &[&[u8]]) -> Result<Vec<Vec<u8>>> {
        bucket::validate_nested_bucket_path(path)?;
        if path.len() == 1 {
            if !bucket_exists(self.db.tree(), path[0]) {
                return Err(Error::BucketNotFound {
                    name: path[0].to_vec(),
                });
            }
        } else if !bucket::nested_bucket_exists(self.db.tree(), path) {
            return Err(Error::BucketNotFound {
                name: path.last().unwrap().to_vec(),
            });
        }
        Ok(bucket::list_nested_buckets(self.db.tree(), path))
    }
}

/// A read-write transaction.
///
/// Provides exclusive write access to the database. Changes are not
/// visible to other transactions until `commit()` is called.
///
/// # Lifetime
///
/// The transaction holds a mutable reference to the database and must
/// not outlive it. Only one write transaction can exist at a time.
pub struct WriteTx<'db> {
    db: &'db mut Database,
    /// Pending changes stored in a scratch B+ tree.
    /// Only applied to the main tree on commit.
    pending: BTree,
    /// Keys marked for deletion.
    deleted: Vec<Vec<u8>>,
    /// Whether this transaction has been committed.
    committed: bool,
}

impl<'db> WriteTx<'db> {
    /// Creates a new write transaction.
    pub(crate) fn new(db: &'db mut Database) -> Self {
        Self {
            db,
            pending: BTree::new(),
            deleted: Vec::new(),
            committed: false,
        }
    }

    /// Inserts or updates a key-value pair.
    ///
    /// If the key already exists, its value will be overwritten.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        // Remove from deleted list if present.
        self.deleted.retain(|k| k.as_slice() != key);
        // Add to pending changes.
        self.pending.insert(key.to_vec(), value.to_vec());
    }

    /// Inserts or updates a key-value pair, taking ownership of the data.
    ///
    /// This is more efficient than `put()` for large values as it avoids
    /// copying the value data.
    ///
    /// If the key already exists, its value will be overwritten.
    #[inline]
    pub fn put_owned(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Remove from deleted list if present.
        self.deleted.retain(|k| k.as_slice() != key);
        // Add to pending changes without copying.
        self.pending.insert(key, value);
    }

    /// Inserts multiple key-value pairs in bulk.
    ///
    /// This is significantly more efficient than calling `put()` in a loop
    /// for large batches because:
    /// - Pre-allocates memory for all entries
    /// - Uses parallel processing for data preparation (large batches)
    /// - Minimizes per-operation overhead
    ///
    /// # Arguments
    ///
    /// * `entries` - Iterator of (key, value) pairs to insert.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut wtx = db.write_tx();
    /// wtx.batch_put(vec![
    ///     (b"key1".to_vec(), b"value1".to_vec()),
    ///     (b"key2".to_vec(), b"value2".to_vec()),
    ///     (b"key3".to_vec(), b"value3".to_vec()),
    /// ]);
    /// wtx.commit()?;
    /// ```
    pub fn batch_put<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        for (key, value) in entries {
            // Remove from deleted list if present.
            self.deleted.retain(|k| k.as_slice() != key);
            // Add to pending changes.
            self.pending.insert(key, value);
        }
    }

    /// Inserts multiple key-value pairs in bulk from borrowed slices.
    ///
    /// Similar to `batch_put` but works with borrowed data, which may
    /// involve more copying but is convenient when you don't own the data.
    ///
    /// # Arguments
    ///
    /// * `entries` - Iterator of (&key, &value) pairs to insert.
    pub fn batch_put_ref<'a, I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
    {
        for (key, value) in entries {
            self.deleted.retain(|k| k.as_slice() != key);
            self.pending.insert(key.to_vec(), value.to_vec());
        }
    }

    /// Deletes a key from the database.
    ///
    /// Does nothing if the key does not exist.
    pub fn delete(&mut self, key: &[u8]) {
        // Remove from pending if present.
        self.pending.remove(key);
        // Mark for deletion from main tree.
        if !self.deleted.iter().any(|k| k.as_slice() == key) {
            self.deleted.push(key.to_vec());
        }
    }

    /// Deletes multiple keys in bulk.
    ///
    /// # Arguments
    ///
    /// * `keys` - Iterator of keys to delete.
    pub fn batch_delete<'a, I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        for key in keys {
            self.pending.remove(key);
            if !self.deleted.iter().any(|k| k.as_slice() == key) {
                self.deleted.push(key.to_vec());
            }
        }
    }

    /// Creates a new bucket.
    ///
    /// # Errors
    ///
    /// Returns `BucketAlreadyExists` if a bucket with the same name already exists.
    /// Returns `InvalidBucketName` if the name is invalid.
    pub fn create_bucket(&mut self, name: &[u8]) -> Result<()> {
        bucket::validate_bucket_name(name)?;

        // Check if bucket effectively exists (not marked for deletion).
        if self.is_bucket_present(name) {
            return Err(Error::BucketAlreadyExists {
                name: name.to_vec(),
            });
        }

        // If bucket was deleted, remove from deleted list.
        let meta_key = bucket::bucket_meta_key(name);
        self.deleted.retain(|k| k.as_slice() != meta_key.as_slice());

        // Add bucket metadata to pending.
        self.pending.insert(meta_key, Vec::new());
        Ok(())
    }

    /// Creates a bucket if it doesn't exist.
    ///
    /// Returns `true` if a new bucket was created, `false` if it already existed.
    ///
    /// # Errors
    ///
    /// Returns `InvalidBucketName` if the name is invalid.
    pub fn create_bucket_if_not_exists(&mut self, name: &[u8]) -> Result<bool> {
        bucket::validate_bucket_name(name)?;

        // Check if bucket effectively exists.
        if self.is_bucket_present(name) {
            return Ok(false);
        }

        // If bucket was deleted, remove from deleted list.
        let meta_key = bucket::bucket_meta_key(name);
        self.deleted.retain(|k| k.as_slice() != meta_key.as_slice());

        self.pending.insert(meta_key, Vec::new());
        Ok(true)
    }

    /// Deletes a bucket and all its contents.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the bucket doesn't exist.
    /// Returns `InvalidBucketName` if the name is invalid.
    pub fn delete_bucket(&mut self, name: &[u8]) -> Result<()> {
        bucket::validate_bucket_name(name)?;

        let meta_key = bucket::bucket_meta_key(name);
        let exists_in_main = self.db.tree().get(&meta_key).is_some();
        let exists_in_pending = self.pending.get(&meta_key).is_some();

        if !exists_in_main && !exists_in_pending {
            return Err(Error::BucketNotFound {
                name: name.to_vec(),
            });
        }

        // Collect all data keys to delete from main tree.
        let prefix = bucket::bucket_data_prefix(name);
        let keys_from_main: Vec<Vec<u8>> = self
            .db
            .tree()
            .iter()
            .filter_map(|(k, _)| {
                // Direct bucket data.
                if k.starts_with(&prefix) {
                    return Some(k.to_vec());
                }
                // Nested bucket metadata under this bucket.
                if k.first() == Some(&0x02) && self.is_nested_child_of_bucket(k, name) {
                    return Some(k.to_vec());
                }
                // Nested bucket data under this bucket.
                if k.first() == Some(&0x03) && self.is_nested_child_of_bucket(k, name) {
                    return Some(k.to_vec());
                }
                None
            })
            .collect();

        // Mark main tree keys for deletion.
        for key in keys_from_main {
            if !self.deleted.iter().any(|k| k.as_slice() == key.as_slice()) {
                self.deleted.push(key);
            }
        }

        // Remove any pending data for this bucket and nested buckets.
        let pending_keys: Vec<Vec<u8>> = self
            .pending
            .iter()
            .filter_map(|(k, _)| {
                // Direct bucket data.
                if k.starts_with(&prefix) {
                    return Some(k.to_vec());
                }
                // Nested bucket metadata under this bucket.
                if k.first() == Some(&0x02) && self.is_nested_child_of_bucket(k, name) {
                    return Some(k.to_vec());
                }
                // Nested bucket data under this bucket.
                if k.first() == Some(&0x03) && self.is_nested_child_of_bucket(k, name) {
                    return Some(k.to_vec());
                }
                None
            })
            .collect();

        for key in pending_keys {
            self.pending.remove(&key);
        }

        // Delete bucket metadata.
        if exists_in_main {
            self.deleted.push(meta_key.clone());
        }
        self.pending.remove(&meta_key);

        Ok(())
    }

    /// Checks if a bucket is effectively present (exists and not marked for deletion).
    fn is_bucket_present(&self, name: &[u8]) -> bool {
        let meta_key = bucket::bucket_meta_key(name);

        // Check if marked for deletion.
        if self
            .deleted
            .iter()
            .any(|k| k.as_slice() == meta_key.as_slice())
        {
            return false;
        }

        // Check pending and main tree.
        self.pending.get(&meta_key).is_some() || self.db.tree().get(&meta_key).is_some()
    }

    /// Returns a read-only reference to a bucket.
    ///
    /// Note: This returns a view of the committed state, not including
    /// pending changes in this transaction.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the bucket does not exist.
    /// Returns `InvalidBucketName` if the name is invalid.
    pub fn bucket(&self, name: &[u8]) -> Result<BucketRef<'_>> {
        bucket::validate_bucket_name(name)?;

        if !self.is_bucket_present(name) {
            return Err(Error::BucketNotFound {
                name: name.to_vec(),
            });
        }

        // Return view of committed tree (pending changes not visible).
        BucketRef::new(self.db.tree(), name)
    }

    /// Puts a key-value pair into a bucket.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the bucket doesn't exist.
    /// Returns `InvalidBucketName` if the bucket name is invalid.
    pub fn bucket_put(&mut self, bucket_name: &[u8], key: &[u8], value: &[u8]) -> Result<()> {
        bucket::validate_bucket_name(bucket_name)?;

        if !self.is_bucket_present(bucket_name) {
            return Err(Error::BucketNotFound {
                name: bucket_name.to_vec(),
            });
        }

        let internal_key = bucket::bucket_data_key(bucket_name, key);

        // Remove from deleted list if present.
        self.deleted
            .retain(|k| k.as_slice() != internal_key.as_slice());

        // Add to pending.
        self.pending.insert(internal_key, value.to_vec());
        Ok(())
    }

    /// Gets a value from a bucket.
    ///
    /// Note: This returns from the committed state, not including pending changes.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the bucket doesn't exist.
    /// Returns `InvalidBucketName` if the bucket name is invalid.
    pub fn bucket_get(&self, bucket_name: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        bucket::validate_bucket_name(bucket_name)?;

        if !self.is_bucket_present(bucket_name) {
            return Err(Error::BucketNotFound {
                name: bucket_name.to_vec(),
            });
        }

        let internal_key = bucket::bucket_data_key(bucket_name, key);

        // Check pending first, then main tree.
        if let Some(value) = self.pending.get(&internal_key) {
            return Ok(Some(value.to_vec()));
        }

        // Check if deleted.
        if self
            .deleted
            .iter()
            .any(|k| k.as_slice() == internal_key.as_slice())
        {
            return Ok(None);
        }

        Ok(self.db.tree().get(&internal_key).map(|v| v.to_vec()))
    }

    /// Deletes a key from a bucket.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the bucket doesn't exist.
    /// Returns `InvalidBucketName` if the bucket name is invalid.
    pub fn bucket_delete(&mut self, bucket_name: &[u8], key: &[u8]) -> Result<()> {
        bucket::validate_bucket_name(bucket_name)?;

        if !self.is_bucket_present(bucket_name) {
            return Err(Error::BucketNotFound {
                name: bucket_name.to_vec(),
            });
        }

        let internal_key = bucket::bucket_data_key(bucket_name, key);

        // Remove from pending if present.
        self.pending.remove(&internal_key);

        // Mark for deletion from main tree.
        if !self
            .deleted
            .iter()
            .any(|k| k.as_slice() == internal_key.as_slice())
        {
            self.deleted.push(internal_key);
        }

        Ok(())
    }

    /// Checks if a bucket exists.
    pub fn bucket_exists(&self, name: &[u8]) -> bool {
        if bucket::validate_bucket_name(name).is_err() {
            return false;
        }
        self.is_bucket_present(name)
    }

    /// Lists all bucket names.
    pub fn list_buckets(&self) -> Vec<Vec<u8>> {
        // Get buckets from main tree.
        let mut buckets = list_buckets(self.db.tree());

        // Add buckets from pending that aren't in main tree.
        for (k, _) in self.pending.iter() {
            if k.first() == Some(&0x00) && k.len() > 1 {
                let name_len = k[1] as usize;
                if k.len() >= 2 + name_len {
                    let name = k[2..2 + name_len].to_vec();
                    if !buckets.contains(&name) {
                        buckets.push(name);
                    }
                }
            }
        }

        // Remove buckets that are marked for deletion.
        buckets.retain(|name| {
            let meta_key = bucket::bucket_meta_key(name);
            !self
                .deleted
                .iter()
                .any(|k| k.as_slice() == meta_key.as_slice())
        });

        buckets
    }

    // ==================== Nested Bucket Methods ====================

    /// Creates a nested bucket under a parent bucket.
    ///
    /// # Arguments
    ///
    /// * `parent` - The name of the parent (top-level) bucket.
    /// * `child` - The name of the nested bucket to create.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the parent bucket doesn't exist.
    /// Returns `BucketAlreadyExists` if the nested bucket already exists.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn create_nested_bucket(&mut self, parent: &[u8], child: &[u8]) -> Result<()> {
        bucket::validate_bucket_name(parent)?;
        bucket::validate_bucket_name(child)?;

        // Ensure parent bucket exists.
        if !self.is_bucket_present(parent) {
            return Err(Error::BucketNotFound {
                name: parent.to_vec(),
            });
        }

        // Check if nested bucket already exists.
        if self.is_nested_bucket_present(&[parent, child]) {
            return Err(Error::BucketAlreadyExists {
                name: child.to_vec(),
            });
        }

        let meta_key = bucket::nested_bucket_meta_key(&[parent, child]);
        self.deleted.retain(|k| k.as_slice() != meta_key.as_slice());
        self.pending.insert(meta_key, Vec::new());
        Ok(())
    }

    /// Creates a nested bucket at a specific path.
    ///
    /// # Arguments
    ///
    /// * `parent_path` - The path to the parent bucket (can be nested).
    /// * `child` - The name of the nested bucket to create.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the parent bucket doesn't exist.
    /// Returns `BucketAlreadyExists` if the nested bucket already exists.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn create_nested_bucket_at_path(
        &mut self,
        parent_path: &[&[u8]],
        child: &[u8],
    ) -> Result<()> {
        bucket::validate_bucket_name(child)?;
        for component in parent_path {
            bucket::validate_bucket_name(component)?;
        }

        // Ensure parent exists.
        if parent_path.len() == 1 {
            if !self.is_bucket_present(parent_path[0]) {
                return Err(Error::BucketNotFound {
                    name: parent_path[0].to_vec(),
                });
            }
        } else if !self.is_nested_bucket_present(parent_path) {
            return Err(Error::BucketNotFound {
                name: parent_path.last().unwrap().to_vec(),
            });
        }

        // Build full path for the new bucket.
        let mut full_path: Vec<&[u8]> = parent_path.to_vec();
        full_path.push(child);

        // Check if nested bucket already exists.
        if self.is_nested_bucket_present(&full_path) {
            return Err(Error::BucketAlreadyExists {
                name: child.to_vec(),
            });
        }

        let meta_key = bucket::nested_bucket_meta_key(&full_path);
        self.deleted.retain(|k| k.as_slice() != meta_key.as_slice());
        self.pending.insert(meta_key, Vec::new());
        Ok(())
    }

    /// Creates a nested bucket if it doesn't exist.
    ///
    /// Returns `true` if a new bucket was created, `false` if it already existed.
    pub fn create_nested_bucket_if_not_exists(
        &mut self,
        parent: &[u8],
        child: &[u8],
    ) -> Result<bool> {
        bucket::validate_bucket_name(parent)?;
        bucket::validate_bucket_name(child)?;

        if !self.is_bucket_present(parent) {
            return Err(Error::BucketNotFound {
                name: parent.to_vec(),
            });
        }

        if self.is_nested_bucket_present(&[parent, child]) {
            return Ok(false);
        }

        let meta_key = bucket::nested_bucket_meta_key(&[parent, child]);
        self.deleted.retain(|k| k.as_slice() != meta_key.as_slice());
        self.pending.insert(meta_key, Vec::new());
        Ok(true)
    }

    /// Deletes a nested bucket and all its contents.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the nested bucket doesn't exist.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn delete_nested_bucket(&mut self, parent: &[u8], child: &[u8]) -> Result<()> {
        bucket::validate_bucket_name(parent)?;
        bucket::validate_bucket_name(child)?;

        let path: [&[u8]; 2] = [parent, child];
        if !self.is_nested_bucket_present(&path) {
            return Err(Error::BucketNotFound {
                name: child.to_vec(),
            });
        }

        self.delete_nested_bucket_internal(&path)
    }

    /// Deletes a nested bucket at a specific path and all its contents.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the nested bucket doesn't exist.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn delete_nested_bucket_at_path(
        &mut self,
        parent_path: &[&[u8]],
        child: &[u8],
    ) -> Result<()> {
        bucket::validate_bucket_name(child)?;
        for component in parent_path {
            bucket::validate_bucket_name(component)?;
        }

        let mut full_path: Vec<&[u8]> = parent_path.to_vec();
        full_path.push(child);

        if !self.is_nested_bucket_present(&full_path) {
            return Err(Error::BucketNotFound {
                name: child.to_vec(),
            });
        }

        self.delete_nested_bucket_internal(&full_path)
    }

    /// Internal helper to delete a nested bucket and its children.
    fn delete_nested_bucket_internal(&mut self, path: &[&[u8]]) -> Result<()> {
        let meta_key = bucket::nested_bucket_meta_key(path);
        let data_prefix = bucket::nested_bucket_data_prefix(path);

        // Collect keys to delete from main tree.
        let keys_from_main: Vec<Vec<u8>> = self
            .db
            .tree()
            .iter()
            .filter_map(|(k, _)| {
                // Data entries in this bucket.
                if k.starts_with(&data_prefix) {
                    return Some(k.to_vec());
                }
                // Nested bucket metadata under this bucket.
                if k.first() == Some(&0x02) && self.is_child_key(k, path) {
                    return Some(k.to_vec());
                }
                // Nested bucket data under this bucket.
                if k.first() == Some(&0x03) && self.is_child_key(k, path) {
                    return Some(k.to_vec());
                }
                None
            })
            .collect();

        // Mark main tree keys for deletion.
        for key in keys_from_main {
            if !self.deleted.iter().any(|k| k.as_slice() == key.as_slice()) {
                self.deleted.push(key);
            }
        }

        // Remove any pending data for this bucket and children.
        let pending_keys: Vec<Vec<u8>> = self
            .pending
            .iter()
            .filter_map(|(k, _)| {
                if k.starts_with(&data_prefix) {
                    return Some(k.to_vec());
                }
                if k.first() == Some(&0x02) && self.is_child_key(k, path) {
                    return Some(k.to_vec());
                }
                if k.first() == Some(&0x03) && self.is_child_key(k, path) {
                    return Some(k.to_vec());
                }
                None
            })
            .collect();

        for key in pending_keys {
            self.pending.remove(&key);
        }

        // Delete bucket metadata.
        if self.db.tree().get(&meta_key).is_some() {
            self.deleted.push(meta_key.clone());
        }
        self.pending.remove(&meta_key);

        Ok(())
    }

    /// Helper to check if a key represents a child of the given path.
    fn is_child_key(&self, key: &[u8], parent_path: &[&[u8]]) -> bool {
        if key.len() < 2 {
            return false;
        }

        let component_count = key[1] as usize;
        if component_count <= parent_path.len() {
            return false;
        }

        let mut offset = 2;
        for parent_component in parent_path {
            if offset >= key.len() {
                return false;
            }
            let len = key[offset] as usize;
            offset += 1;
            if offset + len > key.len() {
                return false;
            }
            if &key[offset..offset + len] != *parent_component {
                return false;
            }
            offset += len;
        }

        true
    }

    /// Helper to check if a nested bucket key belongs to a top-level bucket.
    ///
    /// For keys with prefix 0x02 (nested meta) or 0x03 (nested data),
    /// checks if the first path component matches the given bucket name.
    fn is_nested_child_of_bucket(&self, key: &[u8], bucket_name: &[u8]) -> bool {
        if key.len() < 4 {
            return false;
        }

        // key[0] = prefix (0x02 or 0x03), key[1] = component_count
        let component_count = key[1] as usize;
        if component_count < 2 {
            // Must have at least parent + child
            return false;
        }

        // Check first component matches bucket_name.
        let mut offset = 2;
        if offset >= key.len() {
            return false;
        }
        let first_len = key[offset] as usize;
        offset += 1;
        if offset + first_len > key.len() {
            return false;
        }
        &key[offset..offset + first_len] == bucket_name
    }

    /// Checks if a nested bucket is effectively present.
    fn is_nested_bucket_present(&self, path: &[&[u8]]) -> bool {
        let meta_key = bucket::nested_bucket_meta_key(path);

        // Check if marked for deletion.
        if self
            .deleted
            .iter()
            .any(|k| k.as_slice() == meta_key.as_slice())
        {
            return false;
        }

        // Check pending and main tree.
        self.pending.get(&meta_key).is_some() || bucket::nested_bucket_exists(self.db.tree(), path)
    }

    /// Puts a key-value pair into a nested bucket.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the nested bucket doesn't exist.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn nested_bucket_put(
        &mut self,
        parent: &[u8],
        child: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        bucket::validate_bucket_name(parent)?;
        bucket::validate_bucket_name(child)?;

        let path: [&[u8]; 2] = [parent, child];
        if !self.is_nested_bucket_present(&path) {
            return Err(Error::BucketNotFound {
                name: child.to_vec(),
            });
        }

        let internal_key = bucket::nested_bucket_data_key(&path, key);
        self.deleted
            .retain(|k| k.as_slice() != internal_key.as_slice());
        self.pending.insert(internal_key, value.to_vec());
        Ok(())
    }

    /// Puts a key-value pair into a nested bucket at a specific path.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the nested bucket doesn't exist.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn nested_bucket_put_at_path(
        &mut self,
        path: &[&[u8]],
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        bucket::validate_nested_bucket_path(path)?;

        if !self.is_nested_bucket_present(path) {
            return Err(Error::BucketNotFound {
                name: path.last().unwrap().to_vec(),
            });
        }

        let internal_key = bucket::nested_bucket_data_key(path, key);
        self.deleted
            .retain(|k| k.as_slice() != internal_key.as_slice());
        self.pending.insert(internal_key, value.to_vec());
        Ok(())
    }

    /// Gets a value from a nested bucket.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the nested bucket doesn't exist.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn nested_bucket_get(
        &self,
        parent: &[u8],
        child: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        bucket::validate_bucket_name(parent)?;
        bucket::validate_bucket_name(child)?;

        let path: [&[u8]; 2] = [parent, child];
        if !self.is_nested_bucket_present(&path) {
            return Err(Error::BucketNotFound {
                name: child.to_vec(),
            });
        }

        let internal_key = bucket::nested_bucket_data_key(&path, key);

        // Check pending first.
        if let Some(value) = self.pending.get(&internal_key) {
            return Ok(Some(value.to_vec()));
        }

        // Check if deleted.
        if self
            .deleted
            .iter()
            .any(|k| k.as_slice() == internal_key.as_slice())
        {
            return Ok(None);
        }

        Ok(self.db.tree().get(&internal_key).map(|v| v.to_vec()))
    }

    /// Deletes a key from a nested bucket.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the nested bucket doesn't exist.
    /// Returns `InvalidBucketName` if any name is invalid.
    pub fn nested_bucket_delete(&mut self, parent: &[u8], child: &[u8], key: &[u8]) -> Result<()> {
        bucket::validate_bucket_name(parent)?;
        bucket::validate_bucket_name(child)?;

        let path: [&[u8]; 2] = [parent, child];
        if !self.is_nested_bucket_present(&path) {
            return Err(Error::BucketNotFound {
                name: child.to_vec(),
            });
        }

        let internal_key = bucket::nested_bucket_data_key(&path, key);
        self.pending.remove(&internal_key);

        if !self
            .deleted
            .iter()
            .any(|k| k.as_slice() == internal_key.as_slice())
        {
            self.deleted.push(internal_key);
        }

        Ok(())
    }

    /// Checks if a nested bucket exists.
    pub fn nested_bucket_exists(&self, parent: &[u8], child: &[u8]) -> bool {
        if bucket::validate_bucket_name(parent).is_err()
            || bucket::validate_bucket_name(child).is_err()
        {
            return false;
        }
        self.is_nested_bucket_present(&[parent, child])
    }

    /// Checks if a nested bucket exists at a specific path.
    pub fn nested_bucket_exists_at_path(&self, path: &[&[u8]]) -> bool {
        if path.len() < 2 {
            return false;
        }
        if bucket::validate_nested_bucket_path(path).is_err() {
            return false;
        }
        self.is_nested_bucket_present(path)
    }

    /// Lists all nested bucket names directly under a parent bucket.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the parent bucket does not exist.
    /// Returns `InvalidBucketName` if the name is invalid.
    pub fn list_nested_buckets(&self, parent: &[u8]) -> Result<Vec<Vec<u8>>> {
        bucket::validate_bucket_name(parent)?;
        if !self.is_bucket_present(parent) {
            return Err(Error::BucketNotFound {
                name: parent.to_vec(),
            });
        }

        // Get from main tree.
        let mut buckets = bucket::list_nested_buckets(self.db.tree(), &[parent]);

        // Add from pending.
        let expected_depth = 2; // parent + child
        for (k, _) in self.pending.iter() {
            if k.first() != Some(&0x02) || k.len() < 2 {
                continue;
            }
            let component_count = k[1] as usize;
            if component_count != expected_depth {
                continue;
            }
            // Verify parent matches and extract child name.
            if let Some(child_name) = self.extract_nested_child_name(k, parent)
                && !buckets.contains(&child_name)
            {
                buckets.push(child_name);
            }
        }

        // Remove deleted.
        buckets.retain(|child| {
            let path: [&[u8]; 2] = [parent, child];
            let meta_key = bucket::nested_bucket_meta_key(&path);
            !self
                .deleted
                .iter()
                .any(|k| k.as_slice() == meta_key.as_slice())
        });

        Ok(buckets)
    }

    /// Helper to extract child bucket name from a nested bucket meta key.
    fn extract_nested_child_name(&self, key: &[u8], parent: &[u8]) -> Option<Vec<u8>> {
        if key.len() < 4 {
            return None;
        }
        // key[0] = prefix, key[1] = component_count
        let mut offset = 2;
        // Read parent component.
        if offset >= key.len() {
            return None;
        }
        let parent_len = key[offset] as usize;
        offset += 1;
        if offset + parent_len > key.len() {
            return None;
        }
        if &key[offset..offset + parent_len] != parent {
            return None;
        }
        offset += parent_len;
        // Read child component.
        if offset >= key.len() {
            return None;
        }
        let child_len = key[offset] as usize;
        offset += 1;
        if offset + child_len > key.len() {
            return None;
        }
        Some(key[offset..offset + child_len].to_vec())
    }

    /// Commits the transaction, persisting all changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails due to I/O errors
    /// or other issues. On error, the transaction is effectively
    /// rolled back (changes are not persisted).
    pub fn commit(mut self) -> Result<()> {
        // Record the number of operations for error context.
        let deletion_count = self.deleted.len();
        let insertion_count = self.pending.len();
        let has_deletions = !self.deleted.is_empty();

        // Check if any pending writes are updates to existing keys
        // Updates require full rewrite (especially for overflow values)
        let has_updates = self
            .pending
            .iter()
            .any(|(k, _)| self.db.tree().get(k).is_some());

        // Apply deletions to main tree.
        for key in &self.deleted {
            self.db.tree_mut().remove(key);
        }

        // Use incremental persist only for pure insert workloads.
        // Updates and deletions require a full rewrite.
        let persist_result = if has_deletions || has_updates {
            // Collect new entries for full rewrite path
            let new_entries: Vec<(Vec<u8>, Vec<u8>)> = self
                .pending
                .iter()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect();

            // Apply pending insertions to main tree.
            for (key, value) in self.pending.iter() {
                self.db.tree_mut().insert(key.to_vec(), value.to_vec());
            }

            // Deletions or updates require a full rewrite.
            let result = self.db.persist_tree();

            // Update bloom filter with newly inserted keys on success.
            if result.is_ok() {
                for (key, _) in &new_entries {
                    self.db.bloom_mut().insert(key);
                }
            }
            result
        } else {
            // Append-only: use incremental persist for massive speedup.
            // First persist using references to avoid cloning for I/O.
            let result = self.db.persist_incremental(self.pending.iter(), false);

            if result.is_ok() {
                // Apply pending insertions to main tree and update bloom filter.
                // Note: We have to clone here because BTree.iter() returns references.
                // A future optimization would be to use a consuming iterator.
                for (key, value) in self.pending.iter() {
                    self.db.bloom_mut().insert(key);
                    self.db.tree_mut().insert(key.to_vec(), value.to_vec());
                }
            }
            result
        };

        match persist_result {
            Ok(()) => {
                self.committed = true;
                Ok(())
            }
            Err(e) => {
                // Note: The in-memory tree has already been modified.
                // A future improvement would be to maintain a copy for rollback.
                // For now, we report the error with context.
                Err(Error::TxCommitFailed {
                    reason: format!(
                        "failed to persist {} deletions and {} insertions",
                        deletion_count, insertion_count
                    ),
                    source: Some(Box::new(e)),
                })
            }
        }
    }
}

impl Drop for WriteTx<'_> {
    fn drop(&mut self) {
        // If not committed, changes are automatically discarded
        // since they're only in the pending tree.
        if !self.committed {
            // Nothing to do - pending changes are dropped with self.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_db_path(name: &str) -> String {
        format!("/tmp/thunder_tx_test_{name}.db")
    }

    fn cleanup(path: &str) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_read_tx_basic() {
        let path = test_db_path("read_basic");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Insert data first
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key1", b"value1");
            wtx.put(b"key2", b"value2");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(rtx.get(b"key2"), Some(b"value2".to_vec()));
        assert!(rtx.get(b"nonexistent").is_none());

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_put_delete_commit() {
        let path = test_db_path("write_crud");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Put multiple keys
        {
            let mut wtx = db.write_tx();
            wtx.put(b"k1", b"v1");
            wtx.put(b"k2", b"v2");
            wtx.put(b"k3", b"v3");
            wtx.commit().expect("commit should succeed");
        }

        // Delete and overwrite
        {
            let mut wtx = db.write_tx();
            wtx.delete(b"k2");
            wtx.put(b"k1", b"updated");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"k1"), Some(b"updated".to_vec()));
        assert!(rtx.get(b"k2").is_none());
        assert_eq!(rtx.get(b"k3"), Some(b"v3".to_vec()));

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_rollback_on_drop() {
        let path = test_db_path("rollback");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Commit initial data
        {
            let mut wtx = db.write_tx();
            wtx.put(b"existing", b"data");
            wtx.commit().expect("commit should succeed");
        }

        // Start new tx, modify, but don't commit (drop)
        {
            let mut wtx = db.write_tx();
            wtx.put(b"new", b"value");
            wtx.delete(b"existing");
            // Drops without commit
        }

        // Verify existing data preserved, new data not present
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"existing"), Some(b"data".to_vec()));
        assert!(rtx.get(b"new").is_none());

        cleanup(&path);
    }

    #[test]
    fn test_write_tx_many_keys() {
        let path = test_db_path("many_keys");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            for i in 0..200 {
                let key = format!("key_{i:05}");
                let value = format!("value_{i}");
                wtx.put(key.as_bytes(), value.as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        for i in 0..200 {
            let key = format!("key_{i:05}");
            let expected = format!("value_{i}");
            assert_eq!(rtx.get(key.as_bytes()), Some(expected.into_bytes()));
        }

        cleanup(&path);
    }
}

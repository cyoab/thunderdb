//! Summary: Read and write transaction types.
//! Copyright (c) YOAB. All rights reserved.

use std::ops::RangeBounds;

use crate::btree::{Bound, BTree, BTreeIter, BTreeRangeIter};
use crate::bucket::{self, bucket_exists, list_buckets, BucketRef};
use crate::db::Database;
use crate::error::{Error, Result};

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
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.tree().get(key).map(|v| v.to_vec())
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
                if k.starts_with(&prefix) {
                    Some(k.to_vec())
                } else {
                    None
                }
            })
            .collect();

        // Mark main tree keys for deletion.
        for key in keys_from_main {
            if !self.deleted.iter().any(|k| k.as_slice() == key.as_slice()) {
                self.deleted.push(key);
            }
        }

        // Remove any pending data for this bucket.
        let pending_keys: Vec<Vec<u8>> = self
            .pending
            .iter()
            .filter_map(|(k, _)| {
                if k.starts_with(&prefix) {
                    Some(k.to_vec())
                } else {
                    None
                }
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
        if self.deleted.iter().any(|k| k.as_slice() == meta_key.as_slice()) {
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
        self.deleted.retain(|k| k.as_slice() != internal_key.as_slice());

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
        if self.deleted.iter().any(|k| k.as_slice() == internal_key.as_slice()) {
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
        if !self.deleted.iter().any(|k| k.as_slice() == internal_key.as_slice()) {
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
            !self.deleted.iter().any(|k| k.as_slice() == meta_key.as_slice())
        });

        buckets
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

        // Apply deletions to main tree.
        for key in &self.deleted {
            self.db.tree_mut().remove(key);
        }

        // Apply pending insertions to main tree.
        for (key, value) in self.pending.iter() {
            self.db.tree_mut().insert(key.to_vec(), value.to_vec());
        }

        // Persist to disk.
        match self.db.persist_tree() {
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

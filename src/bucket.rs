//! Summary: Bucket implementation for namespaced key-value storage.
//! Copyright (c) YOAB. All rights reserved.
//!
//! Buckets provide logical namespacing for keys within the database.
//! Each bucket has a unique name and stores its own set of key-value pairs
//! isolated from other buckets. This design is similar to bbolt/boltdb
//! used in etcd.
//!
//! # Design
//!
//! Buckets are implemented by prefixing all keys with a bucket identifier.
//! The format is: `[BUCKET_PREFIX][bucket_name_len][bucket_name][KEY_PREFIX][key]`
//!
//! This allows efficient range scans within a bucket while keeping all
//! data in a single B+ tree.

use crate::btree::BTree;
use crate::error::{Error, Result};

/// Magic prefix byte for bucket metadata entries.
const BUCKET_META_PREFIX: u8 = 0x00;

/// Magic prefix byte for bucket data entries.
const BUCKET_DATA_PREFIX: u8 = 0x01;

/// Maximum allowed bucket name length in bytes.
pub const MAX_BUCKET_NAME_LEN: usize = 255;

/// Validates a bucket name.
///
/// # Errors
///
/// Returns `InvalidBucketName` if the name is empty or exceeds the maximum length.
pub fn validate_bucket_name(name: &[u8]) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidBucketName {
            reason: "bucket name cannot be empty",
        });
    }
    if name.len() > MAX_BUCKET_NAME_LEN {
        return Err(Error::InvalidBucketName {
            reason: "bucket name exceeds maximum length of 255 bytes",
        });
    }
    Ok(())
}

/// Creates the internal key for bucket metadata.
///
/// Format: `[BUCKET_META_PREFIX][name_len:u8][name]`
#[inline]
pub fn bucket_meta_key(name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + name.len());
    key.push(BUCKET_META_PREFIX);
    key.push(name.len() as u8);
    key.extend_from_slice(name);
    key
}

/// Creates the internal key for a data entry within a bucket.
///
/// Format: `[BUCKET_DATA_PREFIX][bucket_name_len:u8][bucket_name][user_key]`
#[inline]
pub fn bucket_data_key(bucket_name: &[u8], user_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + bucket_name.len() + user_key.len());
    key.push(BUCKET_DATA_PREFIX);
    key.push(bucket_name.len() as u8);
    key.extend_from_slice(bucket_name);
    key.extend_from_slice(user_key);
    key
}

/// Returns the prefix for all data keys in a bucket.
///
/// Used for iteration and range queries.
#[inline]
pub fn bucket_data_prefix(bucket_name: &[u8]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + bucket_name.len());
    prefix.push(BUCKET_DATA_PREFIX);
    prefix.push(bucket_name.len() as u8);
    prefix.extend_from_slice(bucket_name);
    prefix
}

/// Extracts the user key from a full internal key.
///
/// Returns `None` if the key doesn't belong to the specified bucket.
#[inline]
pub fn extract_user_key<'a>(bucket_name: &[u8], internal_key: &'a [u8]) -> Option<&'a [u8]> {
    let prefix = bucket_data_prefix(bucket_name);
    if internal_key.starts_with(&prefix) {
        Some(&internal_key[prefix.len()..])
    } else {
        None
    }
}

/// Checks if a bucket exists in the tree.
pub fn bucket_exists(tree: &BTree, name: &[u8]) -> bool {
    let meta_key = bucket_meta_key(name);
    tree.get(&meta_key).is_some()
}

/// Creates a bucket in the tree.
///
/// # Errors
///
/// Returns `BucketAlreadyExists` if a bucket with the same name already exists.
/// Returns `InvalidBucketName` if the name is invalid.
pub fn create_bucket(tree: &mut BTree, name: &[u8]) -> Result<()> {
    validate_bucket_name(name)?;

    let meta_key = bucket_meta_key(name);
    if tree.get(&meta_key).is_some() {
        return Err(Error::BucketAlreadyExists {
            name: name.to_vec(),
        });
    }

    // Store bucket metadata (empty value for now, can add stats later).
    tree.insert(meta_key, Vec::new());
    Ok(())
}

/// Creates a bucket if it doesn't exist.
///
/// Returns `true` if a new bucket was created, `false` if it already existed.
///
/// # Errors
///
/// Returns `InvalidBucketName` if the name is invalid.
pub fn create_bucket_if_not_exists(tree: &mut BTree, name: &[u8]) -> Result<bool> {
    validate_bucket_name(name)?;

    let meta_key = bucket_meta_key(name);
    if tree.get(&meta_key).is_some() {
        return Ok(false);
    }

    tree.insert(meta_key, Vec::new());
    Ok(true)
}

/// Deletes a bucket and all its contents from the tree.
///
/// # Errors
///
/// Returns `BucketNotFound` if the bucket doesn't exist.
/// Returns `InvalidBucketName` if the name is invalid.
pub fn delete_bucket(tree: &mut BTree, name: &[u8]) -> Result<()> {
    validate_bucket_name(name)?;

    let meta_key = bucket_meta_key(name);
    if tree.get(&meta_key).is_none() {
        return Err(Error::BucketNotFound {
            name: name.to_vec(),
        });
    }

    // Collect all keys to delete (bucket metadata + all data entries).
    let prefix = bucket_data_prefix(name);
    let keys_to_delete: Vec<Vec<u8>> = tree
        .iter()
        .filter_map(|(k, _)| {
            if k.starts_with(&prefix) {
                Some(k.to_vec())
            } else {
                None
            }
        })
        .collect();

    // Delete all data entries.
    for key in keys_to_delete {
        tree.remove(&key);
    }

    // Delete bucket metadata.
    tree.remove(&meta_key);

    Ok(())
}

/// Lists all bucket names in the tree.
pub fn list_buckets(tree: &BTree) -> Vec<Vec<u8>> {
    tree.iter()
        .filter_map(|(k, _)| {
            if k.first() == Some(&BUCKET_META_PREFIX) && k.len() > 1 {
                let name_len = k[1] as usize;
                if k.len() >= 2 + name_len {
                    Some(k[2..2 + name_len].to_vec())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

/// A read-only view of a bucket.
///
/// Provides read access to key-value pairs within the bucket's namespace.
#[derive(Debug)]
pub struct BucketRef<'a> {
    tree: &'a BTree,
    name: Vec<u8>,
}

impl<'a> BucketRef<'a> {
    /// Creates a new bucket reference.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the bucket doesn't exist.
    pub fn new(tree: &'a BTree, name: &[u8]) -> Result<Self> {
        validate_bucket_name(name)?;

        if !bucket_exists(tree, name) {
            return Err(Error::BucketNotFound {
                name: name.to_vec(),
            });
        }

        Ok(Self {
            tree,
            name: name.to_vec(),
        })
    }

    /// Returns the bucket name.
    #[inline]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Retrieves the value associated with the given key.
    ///
    /// Returns `None` if the key does not exist in this bucket.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let internal_key = bucket_data_key(&self.name, key);
        self.tree.get(&internal_key)
    }

    /// Returns an iterator over all key-value pairs in the bucket.
    ///
    /// Keys are returned without the bucket prefix.
    pub fn iter(&self) -> BucketIter<'_> {
        BucketIter::new(self.tree, &self.name)
    }

    /// Returns an iterator over a range of key-value pairs in the bucket.
    ///
    /// The range can be specified using standard Rust range syntax.
    /// Keys are returned without the bucket prefix.
    pub fn range<R>(&self, range: R) -> BucketRangeIter<'_>
    where
        R: std::ops::RangeBounds<&'a [u8]>,
    {
        BucketRangeIter::new(self.tree, &self.name, range)
    }
}

/// A mutable view of a bucket for write transactions.
///
/// Provides read and write access to key-value pairs within the bucket's namespace.
pub struct BucketMut<'a> {
    tree: &'a mut BTree,
    name: Vec<u8>,
}

impl<'a> BucketMut<'a> {
    /// Creates a new mutable bucket reference.
    ///
    /// # Errors
    ///
    /// Returns `BucketNotFound` if the bucket doesn't exist.
    pub fn new(tree: &'a mut BTree, name: &[u8]) -> Result<Self> {
        validate_bucket_name(name)?;

        if !bucket_exists(tree, name) {
            return Err(Error::BucketNotFound {
                name: name.to_vec(),
            });
        }

        Ok(Self {
            tree,
            name: name.to_vec(),
        })
    }

    /// Returns the bucket name.
    #[inline]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Retrieves the value associated with the given key.
    ///
    /// Returns `None` if the key does not exist in this bucket.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let internal_key = bucket_data_key(&self.name, key);
        self.tree.get(&internal_key)
    }

    /// Inserts or updates a key-value pair in the bucket.
    ///
    /// If the key already exists, its value will be overwritten.
    /// Returns the old value if the key existed.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
        let internal_key = bucket_data_key(&self.name, key);
        self.tree.insert(internal_key, value.to_vec())
    }

    /// Deletes a key from the bucket.
    ///
    /// Returns the deleted value, or `None` if the key did not exist.
    pub fn delete(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let internal_key = bucket_data_key(&self.name, key);
        self.tree.remove(&internal_key)
    }
}

/// Iterator over key-value pairs in a bucket.
pub struct BucketIter<'a> {
    inner: crate::btree::BTreeIter<'a>,
    prefix: Vec<u8>,
    prefix_len: usize,
}

impl<'a> BucketIter<'a> {
    fn new(tree: &'a BTree, bucket_name: &[u8]) -> Self {
        let prefix = bucket_data_prefix(bucket_name);
        let prefix_len = prefix.len();
        Self {
            inner: tree.iter(),
            prefix,
            prefix_len,
        }
    }
}

impl<'a> Iterator for BucketIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (key, value) = self.inner.next()?;

            // Skip keys before our prefix.
            if key < self.prefix.as_slice() {
                continue;
            }

            // Stop when we've passed our prefix range.
            if !key.starts_with(&self.prefix) {
                return None;
            }

            // Return the user key (without prefix).
            let user_key = &key[self.prefix_len..];
            return Some((user_key, value));
        }
    }
}

/// Bound type for bucket range queries.
#[derive(Debug, Clone)]
pub enum BucketBound<'a> {
    /// No bound (unbounded).
    Unbounded,
    /// Inclusive bound.
    Included(&'a [u8]),
    /// Exclusive bound.
    Excluded(&'a [u8]),
}

/// Iterator over a range of key-value pairs in a bucket.
pub struct BucketRangeIter<'a> {
    inner: crate::btree::BTreeIter<'a>,
    prefix: Vec<u8>,
    prefix_len: usize,
    start_bound: BucketBound<'a>,
    end_bound: BucketBound<'a>,
    started: bool,
    finished: bool,
}

impl<'a> BucketRangeIter<'a> {
    fn new<R>(tree: &'a BTree, bucket_name: &[u8], range: R) -> Self
    where
        R: std::ops::RangeBounds<&'a [u8]>,
    {
        let prefix = bucket_data_prefix(bucket_name);
        let prefix_len = prefix.len();

        let start_bound = match range.start_bound() {
            std::ops::Bound::Unbounded => BucketBound::Unbounded,
            std::ops::Bound::Included(k) => BucketBound::Included(k),
            std::ops::Bound::Excluded(k) => BucketBound::Excluded(k),
        };
        let end_bound = match range.end_bound() {
            std::ops::Bound::Unbounded => BucketBound::Unbounded,
            std::ops::Bound::Included(k) => BucketBound::Included(k),
            std::ops::Bound::Excluded(k) => BucketBound::Excluded(k),
        };

        Self {
            inner: tree.iter(),
            prefix,
            prefix_len,
            start_bound,
            end_bound,
            started: false,
            finished: false,
        }
    }

    /// Checks if a user key is at or past the start bound.
    #[inline]
    fn is_at_or_past_start(&self, user_key: &[u8]) -> bool {
        match &self.start_bound {
            BucketBound::Unbounded => true,
            BucketBound::Included(start) => user_key >= *start,
            BucketBound::Excluded(start) => user_key > *start,
        }
    }

    /// Checks if a user key is past the end bound.
    #[inline]
    fn is_past_end(&self, user_key: &[u8]) -> bool {
        match &self.end_bound {
            BucketBound::Unbounded => false,
            BucketBound::Included(end) => user_key > *end,
            BucketBound::Excluded(end) => user_key >= *end,
        }
    }
}

impl<'a> Iterator for BucketRangeIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            let (key, value) = self.inner.next()?;

            // Skip keys before our bucket prefix.
            if key < self.prefix.as_slice() {
                continue;
            }

            // Stop when we've passed our bucket prefix range.
            if !key.starts_with(&self.prefix) {
                self.finished = true;
                return None;
            }

            // Extract user key.
            let user_key = &key[self.prefix_len..];

            // Skip keys before start bound.
            if !self.started {
                if self.is_at_or_past_start(user_key) {
                    self.started = true;
                } else {
                    continue;
                }
            }

            // Stop at end bound.
            if self.is_past_end(user_key) {
                self.finished = true;
                return None;
            }

            return Some((user_key, value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_bucket_name() {
        assert!(validate_bucket_name(b"").is_err());
        assert!(validate_bucket_name(&vec![b'x'; MAX_BUCKET_NAME_LEN + 1]).is_err());
        assert!(validate_bucket_name(&vec![b'x'; MAX_BUCKET_NAME_LEN]).is_ok());
        assert!(validate_bucket_name(b"test").is_ok());
    }

    #[test]
    fn test_bucket_key_format() {
        let meta_key = bucket_meta_key(b"test");
        assert_eq!(meta_key[0], 0x00); // BUCKET_META_PREFIX
        assert_eq!(meta_key[1], 4); // "test" length
        assert_eq!(&meta_key[2..], b"test");

        let data_key = bucket_data_key(b"mybucket", b"mykey");
        assert_eq!(data_key[0], 0x01); // BUCKET_DATA_PREFIX
        assert_eq!(data_key[1], 8); // "mybucket" length
        assert_eq!(&data_key[2..10], b"mybucket");
        assert_eq!(&data_key[10..], b"mykey");
    }

    #[test]
    fn test_create_delete_bucket() {
        let mut tree = BTree::new();

        assert!(create_bucket(&mut tree, b"test").is_ok());
        assert!(bucket_exists(&tree, b"test"));
        assert!(matches!(create_bucket(&mut tree, b"test").unwrap_err(), Error::BucketAlreadyExists { .. }));

        assert!(delete_bucket(&mut tree, b"test").is_ok());
        assert!(!bucket_exists(&tree, b"test"));
        assert!(matches!(delete_bucket(&mut tree, b"test").unwrap_err(), Error::BucketNotFound { .. }));
    }

    #[test]
    fn test_bucket_ref_operations() {
        let mut tree = BTree::new();
        create_bucket(&mut tree, b"test").unwrap();

        let key = bucket_data_key(b"test", b"key");
        tree.insert(key, b"value".to_vec());

        let bucket = BucketRef::new(&tree, b"test").unwrap();
        assert_eq!(bucket.get(b"key"), Some(&b"value"[..]));
        assert_eq!(bucket.get(b"missing"), None);

        let items: Vec<_> = bucket.iter().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], (&b"key"[..], &b"value"[..]));
    }

    #[test]
    fn test_bucket_isolation() {
        let mut tree = BTree::new();
        create_bucket(&mut tree, b"bucket1").unwrap();
        create_bucket(&mut tree, b"bucket2").unwrap();

        tree.insert(bucket_data_key(b"bucket1", b"key"), b"value1".to_vec());
        tree.insert(bucket_data_key(b"bucket2", b"key"), b"value2".to_vec());

        let b1 = BucketRef::new(&tree, b"bucket1").unwrap();
        let b2 = BucketRef::new(&tree, b"bucket2").unwrap();

        assert_eq!(b1.get(b"key"), Some(&b"value1"[..]));
        assert_eq!(b2.get(b"key"), Some(&b"value2"[..]));

        assert_eq!(b1.iter().count(), 1);
        assert_eq!(b2.iter().count(), 1);
    }
}

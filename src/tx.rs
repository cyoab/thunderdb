//! Summary: Read and write transaction types.
//! Copyright (c) YOAB. All rights reserved.

use crate::btree::BTree;
use crate::db::Database;
use crate::error::Result;

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

    /// Commits the transaction, persisting all changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails due to I/O errors
    /// or other issues.
    pub fn commit(mut self) -> Result<()> {
        // Apply deletions to main tree.
        for key in &self.deleted {
            self.db.tree_mut().remove(key);
        }

        // Apply pending insertions to main tree.
        for (key, value) in self.pending.iter() {
            self.db.tree_mut().insert(key.to_vec(), value.to_vec());
        }

        // Persist to disk.
        self.db.persist_tree()?;

        self.committed = true;
        Ok(())
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

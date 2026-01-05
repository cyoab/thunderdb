//! Summary: Snapshot isolation for Phase 2 read dominance.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides:
//! - `Snapshot`: A consistent, immutable view of the database at a point in time
//! - `SnapshotManager`: Manages snapshot lifecycle and memory reclamation
//! - `SnapshotStats`: Statistics about active snapshots
//!
//! # Design
//!
//! Thunder snapshots provide snapshot isolation semantics:
//! - Snapshots see a consistent view from when they were created
//! - Writers do not block readers
//! - Multiple snapshots can coexist
//! - Memory is reclaimed when snapshots are released
//!
//! # Performance Considerations
//!
//! - Snapshot creation is O(1) (just an Arc clone, ~microseconds)
//! - Copy-on-write: tree is only cloned when mutations occur while snapshots exist
//! - No data copying occurs during iteration (zero-copy reads)
//! - Long-lived snapshots may prevent page reclamation

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::btree::{BTree, BTreeIter, BTreeRangeIter, Bound};

/// A unique identifier for a snapshot.
pub type SnapshotId = u64;

/// Global snapshot ID counter.
static NEXT_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

/// Generate a new unique snapshot ID.
fn next_snapshot_id() -> SnapshotId {
    NEXT_SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed)
}

/// A consistent, immutable view of the database at a point in time.
///
/// Snapshots provide snapshot isolation: they see the database state
/// as it was when the snapshot was created, regardless of subsequent
/// modifications.
///
/// # Ownership
///
/// `Snapshot` is fully owned and can outlive the database reference
/// that created it. This allows snapshots to coexist with write transactions.
///
/// # Thread Safety
///
/// Snapshots are `Send` and can be transferred between threads.
/// Use `Arc<Snapshot>` if sharing across threads is needed.
///
/// # Example
///
/// ```ignore
/// let snapshot = db.snapshot();
///
/// // Modifications after snapshot creation are not visible
/// {
///     let mut wtx = db.write_tx();
///     wtx.put(b"new_key", b"new_value");
///     wtx.commit().unwrap();
/// }
///
/// // Snapshot still sees the old state
/// assert!(snapshot.get(b"new_key").is_none());
/// ```
pub struct Snapshot {
    /// Unique ID for this snapshot.
    id: SnapshotId,

    /// Owned reference to the tree state at snapshot time.
    /// This is a cloned tree for true snapshot isolation.
    tree: Arc<BTree>,

    /// When this snapshot was created.
    created_at: Instant,

    /// Optional reference to snapshot manager for tracking.
    /// Uses Arc for thread-safe reference counting.
    manager: Option<Arc<SnapshotManager>>,
}

impl Snapshot {
    /// Creates a new snapshot from a tree reference.
    ///
    /// This clones the tree to provide true snapshot isolation.
    /// The snapshot is fully owned and independent of the source.
    ///
    /// # Performance Note
    ///
    /// This method performs an O(n) clone of the tree. Prefer `with_arc()`
    /// for O(1) snapshot creation when the tree is already in an `Arc`.
    #[allow(dead_code)]
    pub(crate) fn new(tree: &BTree, manager: Option<Arc<SnapshotManager>>) -> Self {
        let id = next_snapshot_id();

        // Register with manager if available
        if let Some(ref mgr) = manager {
            mgr.register_snapshot(id);
        }

        Self {
            id,
            tree: Arc::new(tree.clone()),
            created_at: Instant::now(),
            manager,
        }
    }

    /// Creates a snapshot with an existing Arc<BTree>.
    ///
    /// This is the preferred method for O(1) snapshot creation.
    /// The snapshot shares the tree data via `Arc` reference counting.
    pub(crate) fn with_arc(tree: Arc<BTree>, manager: Option<Arc<SnapshotManager>>) -> Self {
        let id = next_snapshot_id();

        if let Some(ref mgr) = manager {
            mgr.register_snapshot(id);
        }

        Self {
            id,
            tree,
            created_at: Instant::now(),
            manager,
        }
    }

    /// Returns the unique ID of this snapshot.
    #[inline]
    pub fn id(&self) -> SnapshotId {
        self.id
    }

    /// Returns when this snapshot was created.
    #[inline]
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Returns how long ago this snapshot was created.
    #[inline]
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Retrieves the value associated with the given key.
    ///
    /// Returns `None` if the key does not exist in the snapshot.
    ///
    /// # Note
    ///
    /// This method clones the value. For zero-copy access, use [`get_ref()`](Self::get_ref).
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.tree.get(key).map(|v| v.to_vec())
    }

    /// Retrieves a reference to the value associated with the given key.
    ///
    /// Returns `None` if the key does not exist in the snapshot.
    ///
    /// # Zero-Copy
    ///
    /// This method returns a reference without copying the data.
    /// The reference is valid for the lifetime of the snapshot.
    #[inline]
    pub fn get_ref(&self, key: &[u8]) -> Option<&[u8]> {
        self.tree.get(key)
    }

    /// Returns an iterator over all key-value pairs in the snapshot.
    ///
    /// Keys are returned in sorted (lexicographic) order.
    pub fn iter(&self) -> BTreeIter<'_> {
        self.tree.iter()
    }

    /// Returns an iterator over a range of key-value pairs.
    ///
    /// Keys are returned in sorted (lexicographic) order.
    pub fn range<'a>(&'a self, start: Bound<'a>, end: Bound<'a>) -> BTreeRangeIter<'a> {
        self.tree.range(start, end)
    }

    /// Returns the number of key-value pairs visible in this snapshot.
    #[inline]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Returns true if the snapshot is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        // Unregister from manager if present
        if let Some(ref mgr) = self.manager {
            mgr.unregister_snapshot(self.id);
        }
    }
}

// Snapshot is fully owned and can be sent between threads
unsafe impl Send for Snapshot {}
// Snapshot can also be shared (read-only access to Arc<BTree>)
unsafe impl Sync for Snapshot {}

/// Clone implementation for BTree to support snapshot isolation.
/// This performs a deep clone of the tree structure.
impl Clone for BTree {
    fn clone(&self) -> Self {
        // Create a new tree and insert all entries
        let mut new_tree = BTree::new();
        for (key, value) in self.iter() {
            new_tree.insert(key.to_vec(), value.to_vec());
        }
        new_tree
    }
}

/// Manages snapshot lifecycle and tracks active snapshots.
///
/// The manager keeps track of all active snapshots to:
/// - Prevent premature page reclamation
/// - Provide statistics about snapshot usage
/// - Support explicit snapshot management APIs
pub struct SnapshotManager {
    /// Active snapshot IDs and their creation times.
    active: RwLock<HashMap<SnapshotId, Instant>>,

    /// Total snapshots created.
    total_created: AtomicU64,

    /// Total snapshots released.
    total_released: AtomicU64,
}

impl SnapshotManager {
    /// Creates a new snapshot manager.
    pub fn new() -> Self {
        Self {
            active: RwLock::new(HashMap::new()),
            total_created: AtomicU64::new(0),
            total_released: AtomicU64::new(0),
        }
    }

    /// Registers a new snapshot.
    pub(crate) fn register_snapshot(&self, id: SnapshotId) {
        let mut active = self.active.write().unwrap();
        active.insert(id, Instant::now());
        self.total_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Unregisters a snapshot.
    pub(crate) fn unregister_snapshot(&self, id: SnapshotId) {
        let mut active = self.active.write().unwrap();
        if active.remove(&id).is_some() {
            self.total_released.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Returns the number of active snapshots.
    pub fn active_count(&self) -> usize {
        self.active.read().unwrap().len()
    }

    /// Returns the age of the oldest active snapshot in milliseconds.
    ///
    /// Returns 0 if no snapshots are active.
    pub fn oldest_snapshot_age_ms(&self) -> u64 {
        let active = self.active.read().unwrap();
        active
            .values()
            .map(|created| created.elapsed().as_millis() as u64)
            .max()
            .unwrap_or(0)
    }

    /// Returns statistics about snapshots.
    pub fn stats(&self) -> SnapshotStats {
        let active = self.active.read().unwrap();
        SnapshotStats {
            active_snapshots: active.len(),
            total_created: self.total_created.load(Ordering::Relaxed),
            total_released: self.total_released.load(Ordering::Relaxed),
            oldest_snapshot_age_ms: active
                .values()
                .map(|created| created.elapsed().as_millis() as u64)
                .max()
                .unwrap_or(0) as i64,
        }
    }

    /// Checks if a specific snapshot ID is still active.
    pub fn is_active(&self, id: SnapshotId) -> bool {
        self.active.read().unwrap().contains_key(&id)
    }
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about active snapshots.
///
/// Used for monitoring and debugging snapshot-related issues.
#[derive(Debug, Clone, Default)]
pub struct SnapshotStats {
    /// Number of currently active snapshots.
    pub active_snapshots: usize,

    /// Total snapshots created since database open.
    pub total_created: u64,

    /// Total snapshots released since database open.
    pub total_released: u64,

    /// Age of the oldest active snapshot in milliseconds.
    /// -1 if no snapshots are active.
    pub oldest_snapshot_age_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_manager_basic() {
        let manager = SnapshotManager::new();
        assert_eq!(manager.active_count(), 0);

        let id1 = next_snapshot_id();
        manager.register_snapshot(id1);
        assert_eq!(manager.active_count(), 1);

        let id2 = next_snapshot_id();
        manager.register_snapshot(id2);
        assert_eq!(manager.active_count(), 2);

        manager.unregister_snapshot(id1);
        assert_eq!(manager.active_count(), 1);

        manager.unregister_snapshot(id2);
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn test_snapshot_stats() {
        let manager = SnapshotManager::new();

        let id1 = next_snapshot_id();
        let id2 = next_snapshot_id();

        manager.register_snapshot(id1);
        manager.register_snapshot(id2);
        manager.unregister_snapshot(id1);

        let stats = manager.stats();
        assert_eq!(stats.active_snapshots, 1);
        assert_eq!(stats.total_created, 2);
        assert_eq!(stats.total_released, 1);
    }
}

//! Summary: Object pool for B+ tree nodes.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides an object pool for B+ tree leaf and branch nodes.
//! Instead of allocating new nodes for every insert and deallocating on
//! every delete, nodes are recycled through the pool.
//!
//! # Design
//!
//! - Maintains separate pools for leaf and branch nodes
//! - Released nodes are cleared before being returned to pool
//! - Pool has configurable maximum size to bound memory usage
//! - Statistics track hit/miss rates for monitoring
//!
//! # Security
//!
//! Nodes are cleared when released to prevent data leakage. This ensures
//! that sensitive key/value data from one transaction cannot leak to
//! subsequent transactions through reused nodes.
//!
//! # Performance
//!
//! - Pool hit: O(1) node acquisition
//! - Pool miss: O(1) allocation (standard heap allocation)
//! - Release: O(n) where n is keys in node (due to clearing)

/// Default maximum number of nodes to keep in each pool.
pub const DEFAULT_MAX_POOLED: usize = 256;

/// A leaf node for the B+ tree.
///
/// Leaf nodes store actual key-value pairs. This struct is designed
/// to be poolable - it can be cleared and reused.
#[derive(Debug, Clone)]
pub struct PooledLeafNode {
    /// Keys stored in this leaf (sorted).
    pub keys: Vec<Vec<u8>>,
    /// Values corresponding to each key.
    pub values: Vec<Vec<u8>>,
}

impl PooledLeafNode {
    /// Creates a new empty leaf node.
    #[inline]
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Creates a new leaf node with pre-allocated capacity.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
        }
    }

    /// Clears the node for reuse.
    ///
    /// This removes all keys and values but retains the allocated
    /// capacity for reuse.
    #[inline]
    pub fn clear(&mut self) {
        self.keys.clear();
        self.values.clear();
    }

    /// Returns true if the node contains no keys.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns the number of keys in the node.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Inserts a key-value pair without bounds checking.
    ///
    /// This is used by tests and internal code that manages
    /// node splitting separately.
    #[inline]
    pub fn insert_unchecked(&mut self, key: Vec<u8>, value: Vec<u8>) {
        match self.keys.binary_search_by(|k| k.as_slice().cmp(&key)) {
            Ok(idx) => {
                self.values[idx] = value;
            }
            Err(idx) => {
                self.keys.insert(idx, key);
                self.values.insert(idx, value);
            }
        }
    }

    /// Gets a value by key.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(idx) => Some(&self.values[idx]),
            Err(_) => None,
        }
    }
}

impl Default for PooledLeafNode {
    fn default() -> Self {
        Self::new()
    }
}

/// A branch node for the B+ tree.
///
/// Branch nodes store separator keys and child pointers. This struct
/// is designed to be poolable - it can be cleared and reused.
#[derive(Debug)]
pub struct PooledBranchNode {
    /// Separator keys (n keys for n+1 children).
    pub keys: Vec<Vec<u8>>,
    /// Child node indices or pointers.
    /// In pool context, this stores indices into a node array.
    pub children: Vec<usize>,
}

impl PooledBranchNode {
    /// Creates a new empty branch node.
    #[inline]
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Creates a new branch node with pre-allocated capacity.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            children: Vec::with_capacity(capacity + 1),
        }
    }

    /// Clears the node for reuse.
    #[inline]
    pub fn clear(&mut self) {
        self.keys.clear();
        self.children.clear();
    }

    /// Returns true if the node contains no keys.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns the number of keys in the node.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }
}

impl Default for PooledBranchNode {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for the node pool.
///
/// Tracks hit/miss rates to help tune pool sizing.
#[derive(Debug, Clone, Copy, Default)]
pub struct PoolStats {
    /// Number of times a leaf node was acquired from the pool.
    pub leaf_hits: u64,
    /// Number of times a new leaf node had to be allocated.
    pub leaf_misses: u64,
    /// Number of times a branch node was acquired from the pool.
    pub branch_hits: u64,
    /// Number of times a new branch node had to be allocated.
    pub branch_misses: u64,
}

impl PoolStats {
    /// Returns the leaf pool hit rate (0.0 to 1.0).
    #[inline]
    pub fn leaf_hit_rate(&self) -> f64 {
        let total = self.leaf_hits + self.leaf_misses;
        if total == 0 {
            0.0
        } else {
            self.leaf_hits as f64 / total as f64
        }
    }

    /// Returns the branch pool hit rate (0.0 to 1.0).
    #[inline]
    pub fn branch_hit_rate(&self) -> f64 {
        let total = self.branch_hits + self.branch_misses;
        if total == 0 {
            0.0
        } else {
            self.branch_hits as f64 / total as f64
        }
    }
}

/// Object pool for B+ tree nodes.
///
/// Maintains pools of leaf and branch nodes for reuse. When a node is
/// released back to the pool, it is cleared to prevent data leakage
/// and then stored for future use.
///
/// # Example
///
/// ```
/// use thunderdb::node_pool::NodePool;
///
/// let mut pool = NodePool::new(16);
///
/// // Acquire a leaf node (allocation on first acquire)
/// let mut leaf = pool.acquire_leaf();
/// leaf.insert_unchecked(b"key".to_vec(), b"value".to_vec());
///
/// // Release back to pool
/// pool.release_leaf(leaf);
///
/// // Next acquire reuses the pooled node
/// let leaf2 = pool.acquire_leaf();
/// assert!(leaf2.is_empty()); // Node was cleared on release
/// ```
#[derive(Debug)]
pub struct NodePool {
    /// Pool of available leaf nodes.
    /// Note: Using Box to allow returning owned nodes that can be independently managed.
    #[allow(clippy::vec_box)]
    leaf_nodes: Vec<Box<PooledLeafNode>>,
    /// Pool of available branch nodes.
    #[allow(clippy::vec_box)]
    branch_nodes: Vec<Box<PooledBranchNode>>,
    /// Maximum number of nodes to keep in each pool.
    max_pooled: usize,
    /// Statistics for monitoring pool efficiency.
    stats: PoolStats,
}

impl NodePool {
    /// Creates a new node pool with the specified maximum size.
    ///
    /// # Arguments
    ///
    /// * `max_pooled` - Maximum number of nodes to keep in each pool.
    ///   Nodes released when the pool is full are dropped instead.
    #[inline]
    pub fn new(max_pooled: usize) -> Self {
        Self {
            leaf_nodes: Vec::with_capacity(max_pooled),
            branch_nodes: Vec::with_capacity(max_pooled),
            max_pooled,
            stats: PoolStats::default(),
        }
    }

    /// Creates a new node pool with default maximum size.
    #[inline]
    pub fn with_default_size() -> Self {
        Self::new(DEFAULT_MAX_POOLED)
    }

    /// Acquires a leaf node from the pool.
    ///
    /// If the pool has available nodes, one is returned. Otherwise,
    /// a new node is allocated.
    ///
    /// The returned node is guaranteed to be empty.
    #[inline]
    pub fn acquire_leaf(&mut self) -> Box<PooledLeafNode> {
        match self.leaf_nodes.pop() {
            Some(node) => {
                self.stats.leaf_hits += 1;
                // Node was already cleared on release
                debug_assert!(node.is_empty());
                node
            }
            None => {
                self.stats.leaf_misses += 1;
                Box::new(PooledLeafNode::new())
            }
        }
    }

    /// Releases a leaf node back to the pool.
    ///
    /// The node is cleared before being added to the pool to prevent
    /// data leakage. If the pool is full, the node is dropped.
    ///
    /// # Security
    ///
    /// The node's keys and values are cleared to ensure no sensitive
    /// data remains accessible through the pooled node.
    #[inline]
    pub fn release_leaf(&mut self, mut node: Box<PooledLeafNode>) {
        // Clear node data for security
        node.clear();

        if self.leaf_nodes.len() < self.max_pooled {
            self.leaf_nodes.push(node);
        }
        // Otherwise node is dropped
    }

    /// Acquires a branch node from the pool.
    ///
    /// If the pool has available nodes, one is returned. Otherwise,
    /// a new node is allocated.
    ///
    /// The returned node is guaranteed to be empty.
    #[inline]
    pub fn acquire_branch(&mut self) -> Box<PooledBranchNode> {
        match self.branch_nodes.pop() {
            Some(node) => {
                self.stats.branch_hits += 1;
                debug_assert!(node.is_empty());
                node
            }
            None => {
                self.stats.branch_misses += 1;
                Box::new(PooledBranchNode::new())
            }
        }
    }

    /// Releases a branch node back to the pool.
    ///
    /// The node is cleared before being added to the pool.
    #[inline]
    pub fn release_branch(&mut self, mut node: Box<PooledBranchNode>) {
        node.clear();

        if self.branch_nodes.len() < self.max_pooled {
            self.branch_nodes.push(node);
        }
    }

    /// Returns the current pool statistics.
    #[inline]
    pub fn stats(&self) -> &PoolStats {
        &self.stats
    }

    /// Returns the number of leaf nodes currently in the pool.
    #[inline]
    pub fn leaf_pool_size(&self) -> usize {
        self.leaf_nodes.len()
    }

    /// Returns the number of branch nodes currently in the pool.
    #[inline]
    pub fn branch_pool_size(&self) -> usize {
        self.branch_nodes.len()
    }

    /// Clears all nodes from the pool.
    ///
    /// This drops all pooled nodes, freeing their memory.
    /// Statistics are preserved.
    #[inline]
    pub fn clear(&mut self) {
        self.leaf_nodes.clear();
        self.branch_nodes.clear();
    }

    /// Resets statistics.
    #[inline]
    pub fn reset_stats(&mut self) {
        self.stats = PoolStats::default();
    }
}

impl Default for NodePool {
    fn default() -> Self {
        Self::with_default_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_basic() {
        let mut leaf = PooledLeafNode::new();
        assert!(leaf.is_empty());

        leaf.insert_unchecked(b"key1".to_vec(), b"value1".to_vec());
        leaf.insert_unchecked(b"key2".to_vec(), b"value2".to_vec());

        assert_eq!(leaf.len(), 2);
        assert_eq!(leaf.get(b"key1"), Some(b"value1".as_slice()));
        assert_eq!(leaf.get(b"key2"), Some(b"value2".as_slice()));
        assert_eq!(leaf.get(b"key3"), None);
    }

    #[test]
    fn test_leaf_node_clear() {
        let mut leaf = PooledLeafNode::new();
        leaf.insert_unchecked(b"key".to_vec(), b"value".to_vec());

        assert!(!leaf.is_empty());
        leaf.clear();
        assert!(leaf.is_empty());
    }

    #[test]
    fn test_branch_node_basic() {
        let mut branch = PooledBranchNode::new();
        assert!(branch.is_empty());

        branch.keys.push(b"separator".to_vec());
        branch.children.push(0);
        branch.children.push(1);

        assert_eq!(branch.len(), 1);
        assert!(!branch.is_empty());
    }

    #[test]
    fn test_pool_acquire_release() {
        let mut pool = NodePool::new(8);

        // First acquire is a miss
        let leaf = pool.acquire_leaf();
        assert!(leaf.is_empty());

        let stats = pool.stats();
        assert_eq!(stats.leaf_misses, 1);
        assert_eq!(stats.leaf_hits, 0);

        // Release back
        pool.release_leaf(leaf);

        // Second acquire is a hit
        let leaf2 = pool.acquire_leaf();
        assert!(leaf2.is_empty());

        let stats = pool.stats();
        assert_eq!(stats.leaf_hits, 1);

        pool.release_leaf(leaf2);
    }

    #[test]
    fn test_pool_security_clearing() {
        let mut pool = NodePool::new(8);

        let mut leaf = pool.acquire_leaf();
        leaf.insert_unchecked(b"secret_key".to_vec(), b"secret_value".to_vec());

        pool.release_leaf(leaf);

        // Reacquired node must be empty
        let leaf2 = pool.acquire_leaf();
        assert!(leaf2.is_empty());
        assert!(leaf2.get(b"secret_key").is_none());

        pool.release_leaf(leaf2);
    }

    #[test]
    fn test_pool_max_size() {
        let mut pool = NodePool::new(2);

        // Acquire 3 nodes
        let leaf1 = pool.acquire_leaf();
        let leaf2 = pool.acquire_leaf();
        let leaf3 = pool.acquire_leaf();

        // Release all 3
        pool.release_leaf(leaf1);
        pool.release_leaf(leaf2);
        pool.release_leaf(leaf3); // This one should be dropped

        // Pool should only hold 2
        assert_eq!(pool.leaf_pool_size(), 2);
    }

    #[test]
    fn test_pool_statistics() {
        let mut pool = NodePool::new(4);

        // 3 misses
        let l1 = pool.acquire_leaf();
        let l2 = pool.acquire_leaf();
        let l3 = pool.acquire_leaf();

        pool.release_leaf(l1);
        pool.release_leaf(l2);
        pool.release_leaf(l3);

        // 3 hits
        let _ = pool.acquire_leaf();
        let _ = pool.acquire_leaf();
        let _ = pool.acquire_leaf();

        let stats = pool.stats();
        assert_eq!(stats.leaf_misses, 3);
        assert_eq!(stats.leaf_hits, 3);
        assert!((stats.leaf_hit_rate() - 0.5).abs() < 0.01);
    }
}

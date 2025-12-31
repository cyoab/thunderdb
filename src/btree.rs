//! Summary: B+ tree implementation for the thunder database.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module implements an in-memory B+ tree for key-value storage.
//! The tree maintains keys in sorted order and supports efficient
//! insertion, deletion, and lookup operations.
//!
//! # Design
//!
//! - Leaf nodes store key-value pairs.
//! - Branch nodes store keys and child pointers.
//! - All values are stored in leaf nodes only.
//! - Keys are ordered lexicographically.

/// Maximum number of keys in a leaf node before splitting.
/// Chosen to fit well within a 4KB page with reasonable key/value sizes.
pub const LEAF_MAX_KEYS: usize = 32;

/// Maximum number of keys in a branch node before splitting.
pub const BRANCH_MAX_KEYS: usize = 32;

/// Minimum number of keys in a node (except root).
pub const MIN_KEYS: usize = LEAF_MAX_KEYS / 2;

/// A B+ tree for in-memory key-value storage.
///
/// Keys and values are arbitrary byte slices. Keys are ordered
/// lexicographically.
#[derive(Debug)]
pub struct BTree {
    root: Option<Box<Node>>,
    len: usize,
}

/// A node in the B+ tree.
#[derive(Debug)]
enum Node {
    /// Leaf node containing key-value pairs.
    Leaf(LeafNode),
    /// Branch node containing keys and child pointers.
    Branch(BranchNode),
}

/// A leaf node storing key-value pairs.
#[derive(Debug)]
struct LeafNode {
    /// Keys stored in this leaf (sorted).
    keys: Vec<Vec<u8>>,
    /// Values corresponding to each key.
    values: Vec<Vec<u8>>,
}

/// A branch node storing keys and child pointers.
#[derive(Debug)]
struct BranchNode {
    /// Separator keys (n keys for n+1 children).
    keys: Vec<Vec<u8>>,
    /// Child nodes.
    /// Note: Box is needed here to break the recursive type and allow Node to have a known size.
    #[allow(clippy::vec_box)]
    children: Vec<Box<Node>>,
}

impl BTree {
    /// Creates a new empty B+ tree.
    pub fn new() -> Self {
        Self { root: None, len: 0 }
    }

    /// Returns the number of key-value pairs in the tree.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the tree is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Looks up a key in the tree.
    ///
    /// Returns the value associated with the key, or `None` if not found.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let root = self.root.as_ref()?;
        Self::search_node(root, key)
    }

    /// Inserts a key-value pair into the tree.
    ///
    /// If the key already exists, the old value is replaced and returned.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        if self.root.is_none() {
            // Create first leaf node.
            self.root = Some(Box::new(Node::Leaf(LeafNode {
                keys: vec![key],
                values: vec![value],
            })));
            self.len = 1;
            return None;
        }

        let root = self.root.take().unwrap();
        let (new_root, old_value, split) = Self::insert_into_node(root, key, value);

        self.root = Some(if let Some((median_key, right_child)) = split {
            // Root was split, create new root.
            Box::new(Node::Branch(BranchNode {
                keys: vec![median_key],
                children: vec![new_root, right_child],
            }))
        } else {
            new_root
        });

        if old_value.is_none() {
            self.len += 1;
        }

        old_value
    }

    /// Removes a key from the tree.
    ///
    /// Returns the removed value, or `None` if the key was not found.
    pub fn remove(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let root = self.root.take()?;
        let (new_root, removed_value) = Self::remove_from_node(root, key);

        self.root = new_root;

        if removed_value.is_some() {
            self.len -= 1;
        }

        removed_value
    }

    /// Searches for a key in a node recursively.
    fn search_node<'a>(node: &'a Node, key: &[u8]) -> Option<&'a [u8]> {
        match node {
            Node::Leaf(leaf) => {
                // Binary search for the key.
                match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                    Ok(idx) => Some(&leaf.values[idx]),
                    Err(_) => None,
                }
            }
            Node::Branch(branch) => {
                // Find the child to descend into.
                let child_idx = Self::find_child_index(&branch.keys, key);
                Self::search_node(&branch.children[child_idx], key)
            }
        }
    }

    /// Finds the index of the child to descend into for a given key.
    fn find_child_index(keys: &[Vec<u8>], key: &[u8]) -> usize {
        match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(idx) => idx + 1, // Key found, go right.
            Err(idx) => idx,    // Key not found, go to appropriate child.
        }
    }

    /// Inserts into a node, potentially splitting it.
    ///
    /// Returns (updated_node, old_value, optional_split).
    /// If split occurs, returns (median_key, right_child).
    #[allow(clippy::type_complexity)]
    fn insert_into_node(
        mut node: Box<Node>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> (Box<Node>, Option<Vec<u8>>, Option<(Vec<u8>, Box<Node>)>) {
        match node.as_mut() {
            Node::Leaf(leaf) => {
                // Find insertion point.
                match leaf.keys.binary_search_by(|k| k.as_slice().cmp(&key)) {
                    Ok(idx) => {
                        // Key exists, replace value.
                        let old_value = std::mem::replace(&mut leaf.values[idx], value);
                        (node, Some(old_value), None)
                    }
                    Err(idx) => {
                        // Insert new key-value pair.
                        leaf.keys.insert(idx, key);
                        leaf.values.insert(idx, value);

                        if leaf.keys.len() > LEAF_MAX_KEYS {
                            // Split the leaf.
                            let split = Self::split_leaf(leaf);
                            (node, None, Some(split))
                        } else {
                            (node, None, None)
                        }
                    }
                }
            }
            Node::Branch(branch) => {
                // Find the child to insert into.
                let child_idx = Self::find_child_index(&branch.keys, &key);
                let child = branch.children.remove(child_idx);

                let (updated_child, old_value, child_split) =
                    Self::insert_into_node(child, key, value);

                branch.children.insert(child_idx, updated_child);

                if let Some((median_key, right_child)) = child_split {
                    // Child was split, insert median key and right child.
                    branch.keys.insert(child_idx, median_key);
                    branch.children.insert(child_idx + 1, right_child);

                    if branch.keys.len() > BRANCH_MAX_KEYS {
                        // Split the branch.
                        let split = Self::split_branch(branch);
                        (node, old_value, Some(split))
                    } else {
                        (node, old_value, None)
                    }
                } else {
                    (node, old_value, None)
                }
            }
        }
    }

    /// Splits a leaf node, returning (median_key, right_node).
    fn split_leaf(leaf: &mut LeafNode) -> (Vec<u8>, Box<Node>) {
        let mid = leaf.keys.len() / 2;

        let right_keys = leaf.keys.split_off(mid);
        let right_values = leaf.values.split_off(mid);

        // The first key of the right node becomes the separator.
        let median_key = right_keys[0].clone();

        let right_node = Box::new(Node::Leaf(LeafNode {
            keys: right_keys,
            values: right_values,
        }));

        (median_key, right_node)
    }

    /// Splits a branch node, returning (median_key, right_node).
    fn split_branch(branch: &mut BranchNode) -> (Vec<u8>, Box<Node>) {
        let mid = branch.keys.len() / 2;

        // The middle key becomes the separator (moved up, not copied).
        let median_key = branch.keys.remove(mid);
        let right_keys = branch.keys.split_off(mid);
        let right_children = branch.children.split_off(mid + 1);

        let right_node = Box::new(Node::Branch(BranchNode {
            keys: right_keys,
            children: right_children,
        }));

        (median_key, right_node)
    }

    /// Removes a key from a node.
    ///
    /// Returns (optional_updated_node, optional_removed_value).
    fn remove_from_node(
        mut node: Box<Node>,
        key: &[u8],
    ) -> (Option<Box<Node>>, Option<Vec<u8>>) {
        match node.as_mut() {
            Node::Leaf(leaf) => {
                match leaf.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                    Ok(idx) => {
                        leaf.keys.remove(idx);
                        let value = leaf.values.remove(idx);

                        if leaf.keys.is_empty() {
                            (None, Some(value))
                        } else {
                            (Some(node), Some(value))
                        }
                    }
                    Err(_) => (Some(node), None),
                }
            }
            Node::Branch(branch) => {
                let child_idx = Self::find_child_index(&branch.keys, key);
                let child = branch.children.remove(child_idx);

                let (updated_child, removed_value) = Self::remove_from_node(child, key);

                match updated_child {
                    Some(child) => {
                        branch.children.insert(child_idx, child);

                        // Check if child needs rebalancing.
                        if Self::node_key_count(&branch.children[child_idx]) < MIN_KEYS {
                            Self::rebalance_child(branch, child_idx);
                        }
                    }
                    None => {
                        // Child became empty, remove the separator key.
                        if child_idx > 0 {
                            branch.keys.remove(child_idx - 1);
                        } else if !branch.keys.is_empty() {
                            branch.keys.remove(0);
                        }
                    }
                }

                // If branch has only one child left, promote it.
                if branch.keys.is_empty() && branch.children.len() == 1 {
                    let only_child = branch.children.pop().unwrap();
                    (Some(only_child), removed_value)
                } else if branch.children.is_empty() {
                    (None, removed_value)
                } else {
                    (Some(node), removed_value)
                }
            }
        }
    }

    /// Returns the number of keys in a node.
    fn node_key_count(node: &Node) -> usize {
        match node {
            Node::Leaf(leaf) => leaf.keys.len(),
            Node::Branch(branch) => branch.keys.len(),
        }
    }

    /// Rebalances a child node by borrowing from siblings or merging.
    fn rebalance_child(branch: &mut BranchNode, child_idx: usize) {
        // Try to borrow from left sibling.
        if child_idx > 0 {
            let left_count = Self::node_key_count(&branch.children[child_idx - 1]);
            if left_count > MIN_KEYS {
                Self::borrow_from_left(branch, child_idx);
                return;
            }
        }

        // Try to borrow from right sibling.
        if child_idx < branch.children.len() - 1 {
            let right_count = Self::node_key_count(&branch.children[child_idx + 1]);
            if right_count > MIN_KEYS {
                Self::borrow_from_right(branch, child_idx);
                return;
            }
        }

        // Merge with a sibling.
        if child_idx > 0 {
            Self::merge_with_left(branch, child_idx);
        } else if child_idx < branch.children.len() - 1 {
            Self::merge_with_right(branch, child_idx);
        }
    }

    /// Borrows a key-value pair from the left sibling.
    fn borrow_from_left(branch: &mut BranchNode, child_idx: usize) {
        let separator_idx = child_idx - 1;

        // Use split_at_mut to get mutable references to both children.
        let (left_slice, right_slice) = branch.children.split_at_mut(child_idx);
        let left = left_slice.last_mut().unwrap();
        let right = right_slice.first_mut().unwrap();

        match (left.as_mut(), right.as_mut()) {
            (Node::Leaf(left), Node::Leaf(right)) => {
                let key = left.keys.pop().unwrap();
                let value = left.values.pop().unwrap();
                right.keys.insert(0, key.clone());
                right.values.insert(0, value);
                branch.keys[separator_idx] = key;
            }
            (Node::Branch(left), Node::Branch(right)) => {
                let key = left.keys.pop().unwrap();
                let child = left.children.pop().unwrap();
                let separator = std::mem::replace(&mut branch.keys[separator_idx], key);
                right.keys.insert(0, separator);
                right.children.insert(0, child);
            }
            _ => unreachable!("mismatched node types"),
        }
    }

    /// Borrows a key-value pair from the right sibling.
    fn borrow_from_right(branch: &mut BranchNode, child_idx: usize) {
        let separator_idx = child_idx;

        // Use split_at_mut to get mutable references to both children.
        let (left_slice, right_slice) = branch.children.split_at_mut(child_idx + 1);
        let left = left_slice.last_mut().unwrap();
        let right = right_slice.first_mut().unwrap();

        match (left.as_mut(), right.as_mut()) {
            (Node::Leaf(left), Node::Leaf(right)) => {
                let key = right.keys.remove(0);
                let value = right.values.remove(0);
                left.keys.push(key);
                left.values.push(value);
                branch.keys[separator_idx] = right.keys[0].clone();
            }
            (Node::Branch(left), Node::Branch(right)) => {
                let key = right.keys.remove(0);
                let child = right.children.remove(0);
                let separator = std::mem::replace(&mut branch.keys[separator_idx], key);
                left.keys.push(separator);
                left.children.push(child);
            }
            _ => unreachable!("mismatched node types"),
        }
    }

    /// Merges a node with its left sibling.
    fn merge_with_left(branch: &mut BranchNode, child_idx: usize) {
        let separator_idx = child_idx - 1;
        let separator = branch.keys.remove(separator_idx);
        let right = branch.children.remove(child_idx);

        match (branch.children[child_idx - 1].as_mut(), *right) {
            (Node::Leaf(left), Node::Leaf(right)) => {
                left.keys.extend(right.keys);
                left.values.extend(right.values);
            }
            (Node::Branch(left), Node::Branch(right)) => {
                left.keys.push(separator);
                left.keys.extend(right.keys);
                left.children.extend(right.children);
            }
            _ => unreachable!("mismatched node types"),
        }
    }

    /// Merges a node with its right sibling.
    fn merge_with_right(branch: &mut BranchNode, child_idx: usize) {
        let separator = branch.keys.remove(child_idx);
        let right = branch.children.remove(child_idx + 1);

        match (branch.children[child_idx].as_mut(), *right) {
            (Node::Leaf(left), Node::Leaf(right)) => {
                left.keys.extend(right.keys);
                left.values.extend(right.values);
            }
            (Node::Branch(left), Node::Branch(right)) => {
                left.keys.push(separator);
                left.keys.extend(right.keys);
                left.children.extend(right.children);
            }
            _ => unreachable!("mismatched node types"),
        }
    }

    /// Returns an iterator over all key-value pairs in sorted order.
    pub fn iter(&self) -> BTreeIter<'_> {
        BTreeIter::new(self.root.as_deref())
    }

    /// Returns an iterator over a range of key-value pairs in sorted order.
    ///
    /// The range is specified by start and end bounds.
    pub fn range<'a>(&'a self, start: Bound<'a>, end: Bound<'a>) -> BTreeRangeIter<'a> {
        BTreeRangeIter::new(self, start, end)
    }
}

impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over B+ tree key-value pairs.
pub struct BTreeIter<'a> {
    /// Stack of (node, index) pairs for traversal.
    stack: Vec<(&'a Node, usize)>,
    /// Current leaf and position within it.
    current_leaf: Option<(&'a LeafNode, usize)>,
}

impl<'a> BTreeIter<'a> {
    fn new(root: Option<&'a Node>) -> Self {
        let mut iter = Self {
            stack: Vec::new(),
            current_leaf: None,
        };

        if let Some(node) = root {
            iter.descend_to_leftmost(node);
        }

        iter
    }

    /// Descends to the leftmost leaf from the given node.
    fn descend_to_leftmost(&mut self, mut node: &'a Node) {
        loop {
            match node {
                Node::Leaf(leaf) => {
                    if !leaf.keys.is_empty() {
                        self.current_leaf = Some((leaf, 0));
                    }
                    break;
                }
                Node::Branch(branch) => {
                    self.stack.push((node, 0));
                    node = &branch.children[0];
                }
            }
        }
    }

    /// Advances to the next leaf.
    fn advance_to_next_leaf(&mut self) {
        self.current_leaf = None;

        while let Some((node, mut idx)) = self.stack.pop() {
            if let Node::Branch(branch) = node {
                idx += 1;
                if idx < branch.children.len() {
                    self.stack.push((node, idx));
                    self.descend_to_leftmost(&branch.children[idx]);
                    return;
                }
            }
        }
    }
}

impl<'a> Iterator for BTreeIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let (leaf, idx) = self.current_leaf.as_mut()?;

        let key = &leaf.keys[*idx];
        let value = &leaf.values[*idx];

        *idx += 1;
        if *idx >= leaf.keys.len() {
            self.advance_to_next_leaf();
        }

        Some((key.as_slice(), value.as_slice()))
    }
}

/// Bound type for range queries.
#[derive(Debug, Clone)]
pub enum Bound<'a> {
    /// No bound (unbounded).
    Unbounded,
    /// Inclusive bound.
    Included(&'a [u8]),
    /// Exclusive bound.
    Excluded(&'a [u8]),
}

/// Iterator over a range of B+ tree key-value pairs.
pub struct BTreeRangeIter<'a> {
    /// The underlying full iterator.
    inner: BTreeIter<'a>,
    /// Start bound for filtering.
    start_bound: Bound<'a>,
    /// End bound for filtering.
    end_bound: Bound<'a>,
    /// Whether we've started yielding (past start bound).
    started: bool,
    /// Whether we've finished (past end bound).
    finished: bool,
}

impl<'a> BTreeRangeIter<'a> {
    /// Creates a new range iterator.
    pub fn new(tree: &'a BTree, start: Bound<'a>, end: Bound<'a>) -> Self {
        Self {
            inner: tree.iter(),
            start_bound: start,
            end_bound: end,
            started: false,
            finished: false,
        }
    }

    /// Checks if a key is past the start bound.
    #[inline]
    fn is_at_or_past_start(&self, key: &[u8]) -> bool {
        match &self.start_bound {
            Bound::Unbounded => true,
            Bound::Included(start) => key >= *start,
            Bound::Excluded(start) => key > *start,
        }
    }

    /// Checks if a key is past the end bound.
    #[inline]
    fn is_past_end(&self, key: &[u8]) -> bool {
        match &self.end_bound {
            Bound::Unbounded => false,
            Bound::Included(end) => key > *end,
            Bound::Excluded(end) => key >= *end,
        }
    }
}

impl<'a> Iterator for BTreeRangeIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            let (key, value) = self.inner.next()?;

            // Skip keys before start bound.
            if !self.started {
                if self.is_at_or_past_start(key) {
                    self.started = true;
                } else {
                    continue;
                }
            }

            // Stop at end bound.
            if self.is_past_end(key) {
                self.finished = true;
                return None;
            }

            return Some((key, value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Basic Operations ====================

    #[test]
    fn test_btree_new_is_empty() {
        let tree = BTree::new();
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_btree_insert_single() {
        let mut tree = BTree::new();
        let old = tree.insert(b"key".to_vec(), b"value".to_vec());

        assert!(old.is_none());
        assert_eq!(tree.len(), 1);
        assert!(!tree.is_empty());
    }

    #[test]
    fn test_btree_get_existing() {
        let mut tree = BTree::new();
        tree.insert(b"hello".to_vec(), b"world".to_vec());

        let value = tree.get(b"hello");
        assert_eq!(value, Some(b"world".as_slice()));
    }

    #[test]
    fn test_btree_get_nonexistent() {
        let mut tree = BTree::new();
        tree.insert(b"hello".to_vec(), b"world".to_vec());

        let value = tree.get(b"nonexistent");
        assert!(value.is_none());
    }

    #[test]
    fn test_btree_get_empty_tree() {
        let tree = BTree::new();
        let value = tree.get(b"anything");
        assert!(value.is_none());
    }

    #[test]
    fn test_btree_insert_overwrite() {
        let mut tree = BTree::new();
        tree.insert(b"key".to_vec(), b"value1".to_vec());
        let old = tree.insert(b"key".to_vec(), b"value2".to_vec());

        assert_eq!(old, Some(b"value1".to_vec()));
        assert_eq!(tree.len(), 1);
        assert_eq!(tree.get(b"key"), Some(b"value2".as_slice()));
    }

    #[test]
    fn test_btree_remove_existing() {
        let mut tree = BTree::new();
        tree.insert(b"key".to_vec(), b"value".to_vec());

        let removed = tree.remove(b"key");
        assert_eq!(removed, Some(b"value".to_vec()));
        assert!(tree.is_empty());
        assert!(tree.get(b"key").is_none());
    }

    #[test]
    fn test_btree_remove_nonexistent() {
        let mut tree = BTree::new();
        tree.insert(b"key".to_vec(), b"value".to_vec());

        let removed = tree.remove(b"nonexistent");
        assert!(removed.is_none());
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_btree_remove_empty_tree() {
        let mut tree = BTree::new();
        let removed = tree.remove(b"anything");
        assert!(removed.is_none());
    }

    // ==================== Multiple Keys ====================

    #[test]
    fn test_btree_multiple_keys() {
        let mut tree = BTree::new();

        tree.insert(b"charlie".to_vec(), b"3".to_vec());
        tree.insert(b"alpha".to_vec(), b"1".to_vec());
        tree.insert(b"bravo".to_vec(), b"2".to_vec());

        assert_eq!(tree.len(), 3);
        assert_eq!(tree.get(b"alpha"), Some(b"1".as_slice()));
        assert_eq!(tree.get(b"bravo"), Some(b"2".as_slice()));
        assert_eq!(tree.get(b"charlie"), Some(b"3".as_slice()));
    }

    #[test]
    fn test_btree_remove_middle_key() {
        let mut tree = BTree::new();

        tree.insert(b"a".to_vec(), b"1".to_vec());
        tree.insert(b"b".to_vec(), b"2".to_vec());
        tree.insert(b"c".to_vec(), b"3".to_vec());

        tree.remove(b"b");

        assert_eq!(tree.len(), 2);
        assert_eq!(tree.get(b"a"), Some(b"1".as_slice()));
        assert!(tree.get(b"b").is_none());
        assert_eq!(tree.get(b"c"), Some(b"3".as_slice()));
    }

    // ==================== Ordering ====================

    #[test]
    fn test_btree_lexicographic_order() {
        let mut tree = BTree::new();

        // Insert out of order.
        tree.insert(b"zebra".to_vec(), b"z".to_vec());
        tree.insert(b"apple".to_vec(), b"a".to_vec());
        tree.insert(b"mango".to_vec(), b"m".to_vec());

        // Verify iteration is in sorted order.
        let pairs: Vec<_> = tree.iter().collect();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, b"apple");
        assert_eq!(pairs[1].0, b"mango");
        assert_eq!(pairs[2].0, b"zebra");
    }

    #[test]
    fn test_btree_empty_keys_and_values() {
        let mut tree = BTree::new();

        tree.insert(b"".to_vec(), b"empty_key".to_vec());
        tree.insert(b"key".to_vec(), b"".to_vec());

        assert_eq!(tree.get(b""), Some(b"empty_key".as_slice()));
        assert_eq!(tree.get(b"key"), Some(b"".as_slice()));
    }

    #[test]
    fn test_btree_binary_keys() {
        let mut tree = BTree::new();

        // Keys with null bytes and non-UTF8 data.
        tree.insert(vec![0, 1, 2], b"binary1".to_vec());
        tree.insert(vec![0, 0, 0], b"binary2".to_vec());
        tree.insert(vec![255, 255], b"binary3".to_vec());

        assert_eq!(tree.get(&[0, 1, 2]), Some(b"binary1".as_slice()));
        assert_eq!(tree.get(&[0, 0, 0]), Some(b"binary2".as_slice()));
        assert_eq!(tree.get(&[255, 255]), Some(b"binary3".as_slice()));
    }

    // ==================== Splitting (Large Insertions) ====================

    #[test]
    fn test_btree_many_insertions_sequential() {
        let mut tree = BTree::new();

        // Insert enough keys to trigger multiple splits.
        for i in 0..200u32 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i}");
            tree.insert(key.into_bytes(), value.into_bytes());
        }

        assert_eq!(tree.len(), 200);

        // Verify all keys are retrievable.
        for i in 0..200u32 {
            let key = format!("key_{i:05}");
            let expected_value = format!("value_{i}");
            assert_eq!(
                tree.get(key.as_bytes()),
                Some(expected_value.as_bytes()),
                "key {key} not found or wrong value"
            );
        }
    }

    #[test]
    fn test_btree_many_insertions_random_order() {
        let mut tree = BTree::new();

        // Insert in a pseudo-random order to stress the tree.
        let mut indices: Vec<u32> = (0..200).collect();
        // Simple shuffle using a fixed pattern.
        for i in 0..indices.len() {
            let j = (i * 7 + 13) % indices.len();
            indices.swap(i, j);
        }

        for i in indices.iter() {
            let key = format!("key_{i:05}");
            let value = format!("value_{i}");
            tree.insert(key.into_bytes(), value.into_bytes());
        }

        assert_eq!(tree.len(), 200);

        // Verify sorted iteration.
        let pairs: Vec<_> = tree.iter().collect();
        for (idx, (key, _)) in pairs.iter().enumerate() {
            let expected_key = format!("key_{idx:05}");
            assert_eq!(
                *key,
                expected_key.as_bytes(),
                "iteration order incorrect at index {idx}"
            );
        }
    }

    #[test]
    fn test_btree_causes_root_split() {
        let mut tree = BTree::new();

        // Insert more than LEAF_MAX_KEYS to force at least one split.
        for i in 0..(LEAF_MAX_KEYS + 10) {
            let key = format!("k{i:04}");
            tree.insert(key.into_bytes(), vec![i as u8]);
        }

        assert_eq!(tree.len(), LEAF_MAX_KEYS + 10);

        // Verify all keys present.
        for i in 0..(LEAF_MAX_KEYS + 10) {
            let key = format!("k{i:04}");
            assert!(tree.get(key.as_bytes()).is_some(), "missing key {key}");
        }
    }

    // ==================== Deletion and Rebalancing ====================

    #[test]
    fn test_btree_delete_all_keys() {
        let mut tree = BTree::new();

        for i in 0..50u32 {
            tree.insert(format!("key{i:03}").into_bytes(), vec![i as u8]);
        }

        // Delete all keys.
        for i in 0..50u32 {
            let key = format!("key{i:03}");
            let removed = tree.remove(key.as_bytes());
            assert!(removed.is_some(), "failed to remove {key}");
        }

        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_btree_delete_in_reverse_order() {
        let mut tree = BTree::new();

        for i in 0..100u32 {
            tree.insert(format!("key{i:03}").into_bytes(), vec![i as u8]);
        }

        // Delete in reverse order to stress rebalancing.
        for i in (0..100u32).rev() {
            let key = format!("key{i:03}");
            tree.remove(key.as_bytes());
        }

        assert!(tree.is_empty());
    }

    #[test]
    fn test_btree_interleaved_insert_delete() {
        let mut tree = BTree::new();

        // Insert some keys.
        for i in 0..50u32 {
            tree.insert(format!("key{i:03}").into_bytes(), vec![i as u8]);
        }

        // Delete every other key.
        for i in (0..50u32).step_by(2) {
            tree.remove(format!("key{i:03}").as_bytes());
        }

        assert_eq!(tree.len(), 25);

        // Insert more keys.
        for i in 50..100u32 {
            tree.insert(format!("key{i:03}").into_bytes(), vec![i as u8]);
        }

        assert_eq!(tree.len(), 75);

        // Verify odd keys from first batch + all from second batch.
        for i in (1..50u32).step_by(2) {
            assert!(tree.get(format!("key{i:03}").as_bytes()).is_some());
        }
        for i in 50..100u32 {
            assert!(tree.get(format!("key{i:03}").as_bytes()).is_some());
        }
    }

    // ==================== Iterator ====================

    #[test]
    fn test_btree_iter_empty() {
        let tree = BTree::new();
        let pairs: Vec<_> = tree.iter().collect();
        assert!(pairs.is_empty());
    }

    #[test]
    fn test_btree_iter_single() {
        let mut tree = BTree::new();
        tree.insert(b"key".to_vec(), b"value".to_vec());

        let pairs: Vec<_> = tree.iter().collect();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0], (b"key".as_slice(), b"value".as_slice()));
    }

    #[test]
    fn test_btree_iter_many() {
        let mut tree = BTree::new();

        for i in 0..100u32 {
            tree.insert(format!("{i:03}").into_bytes(), vec![i as u8]);
        }

        let pairs: Vec<_> = tree.iter().collect();
        assert_eq!(pairs.len(), 100);

        // Verify sorted order.
        for (idx, (key, value)) in pairs.iter().enumerate() {
            let expected_key = format!("{idx:03}");
            assert_eq!(*key, expected_key.as_bytes());
            assert_eq!(*value, &[idx as u8]);
        }
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_btree_duplicate_insert_maintains_count() {
        let mut tree = BTree::new();

        tree.insert(b"key".to_vec(), b"v1".to_vec());
        tree.insert(b"key".to_vec(), b"v2".to_vec());
        tree.insert(b"key".to_vec(), b"v3".to_vec());

        assert_eq!(tree.len(), 1);
        assert_eq!(tree.get(b"key"), Some(b"v3".as_slice()));
    }

    #[test]
    fn test_btree_large_keys_and_values() {
        let mut tree = BTree::new();

        let large_key = vec![b'k'; 1000];
        let large_value = vec![b'v'; 10000];

        tree.insert(large_key.clone(), large_value.clone());

        assert_eq!(tree.get(&large_key), Some(large_value.as_slice()));
    }

    #[test]
    fn test_btree_prefix_keys() {
        let mut tree = BTree::new();

        // Keys that are prefixes of each other.
        tree.insert(b"a".to_vec(), b"1".to_vec());
        tree.insert(b"aa".to_vec(), b"2".to_vec());
        tree.insert(b"aaa".to_vec(), b"3".to_vec());
        tree.insert(b"ab".to_vec(), b"4".to_vec());

        assert_eq!(tree.get(b"a"), Some(b"1".as_slice()));
        assert_eq!(tree.get(b"aa"), Some(b"2".as_slice()));
        assert_eq!(tree.get(b"aaa"), Some(b"3".as_slice()));
        assert_eq!(tree.get(b"ab"), Some(b"4".as_slice()));

        let pairs: Vec<_> = tree.iter().collect();
        assert_eq!(pairs[0].0, b"a");
        assert_eq!(pairs[1].0, b"aa");
        assert_eq!(pairs[2].0, b"aaa");
        assert_eq!(pairs[3].0, b"ab");
    }

    // ==================== Stress Test ====================

    #[test]
    fn test_btree_stress_1000_operations() {
        let mut tree = BTree::new();

        // Insert 1000 keys.
        for i in 0..1000u32 {
            tree.insert(format!("stress_{i:06}").into_bytes(), vec![0; 100]);
        }
        assert_eq!(tree.len(), 1000);

        // Update half of them.
        for i in (0..1000u32).step_by(2) {
            tree.insert(format!("stress_{i:06}").into_bytes(), vec![1; 100]);
        }
        assert_eq!(tree.len(), 1000);

        // Delete a quarter.
        for i in (0..1000u32).step_by(4) {
            tree.remove(format!("stress_{i:06}").as_bytes());
        }
        assert_eq!(tree.len(), 750);

        // Verify remaining keys.
        let mut count = 0;
        for i in 0..1000u32 {
            if i % 4 != 0 {
                assert!(
                    tree.get(format!("stress_{i:06}").as_bytes()).is_some(),
                    "missing key {i}"
                );
                count += 1;
            } else {
                assert!(
                    tree.get(format!("stress_{i:06}").as_bytes()).is_none(),
                    "key {i} should be deleted"
                );
            }
        }
        assert_eq!(count, 750);
    }
}

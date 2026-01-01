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

    #[test]
    fn test_btree_basic_crud() {
        let mut tree = BTree::new();
        assert!(tree.is_empty());

        // Insert
        tree.insert(b"key".to_vec(), b"value".to_vec());
        assert_eq!(tree.len(), 1);
        assert_eq!(tree.get(b"key"), Some(b"value".as_slice()));

        // Overwrite
        let old = tree.insert(b"key".to_vec(), b"new_value".to_vec());
        assert_eq!(old, Some(b"value".to_vec()));
        assert_eq!(tree.get(b"key"), Some(b"new_value".as_slice()));

        // Remove
        let removed = tree.remove(b"key");
        assert_eq!(removed, Some(b"new_value".to_vec()));
        assert!(tree.is_empty());
        assert!(tree.get(b"key").is_none());
    }

    #[test]
    fn test_btree_ordering_and_iteration() {
        let mut tree = BTree::new();

        tree.insert(b"zebra".to_vec(), b"z".to_vec());
        tree.insert(b"apple".to_vec(), b"a".to_vec());
        tree.insert(b"mango".to_vec(), b"m".to_vec());

        let pairs: Vec<_> = tree.iter().collect();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, b"apple");
        assert_eq!(pairs[1].0, b"mango");
        assert_eq!(pairs[2].0, b"zebra");
    }

    #[test]
    fn test_btree_many_insertions_causes_splits() {
        let mut tree = BTree::new();

        for i in 0..200u32 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i}");
            tree.insert(key.into_bytes(), value.into_bytes());
        }

        assert_eq!(tree.len(), 200);

        for i in 0..200u32 {
            let key = format!("key_{i:05}");
            let expected = format!("value_{i}");
            assert_eq!(tree.get(key.as_bytes()), Some(expected.as_bytes()));
        }

        // Verify sorted iteration
        let pairs: Vec<_> = tree.iter().collect();
        for (idx, (key, _)) in pairs.iter().enumerate() {
            let expected_key = format!("key_{idx:05}");
            assert_eq!(*key, expected_key.as_bytes());
        }
    }

    #[test]
    fn test_btree_interleaved_insert_delete() {
        let mut tree = BTree::new();

        for i in 0..100u32 {
            tree.insert(format!("key{i:03}").into_bytes(), vec![i as u8]);
        }

        // Delete every other key
        for i in (0..100u32).step_by(2) {
            tree.remove(format!("key{i:03}").as_bytes());
        }

        assert_eq!(tree.len(), 50);

        // Verify remaining keys
        for i in (1..100u32).step_by(2) {
            assert!(tree.get(format!("key{i:03}").as_bytes()).is_some());
        }
    }
}

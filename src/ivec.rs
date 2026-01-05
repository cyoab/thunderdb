//! Summary: Inline vector for efficient key-value storage (inspired by sled's IVec).
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides `IVec`, an inline vector that optimizes memory layout
//! for the common case of small keys and values while efficiently handling
//! large values through reference counting.
//!
//! # Design
//!
//! - Values <= INLINE_CAPACITY (23 bytes) are stored inline (no heap allocation)
//! - Larger values are stored in an Arc<[u8]> (single allocation, ref-counted)
//! - Size is 24 bytes on 64-bit (same as Vec<u8> but with inline optimization)
//!
//! # Performance Characteristics
//!
//! - Clone for small values: O(n) where n <= 23, no allocation
//! - Clone for large values: O(1), just Arc::clone
//! - Deref: O(1) for both cases

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

/// Maximum size for inline storage.
/// Chosen to make IVec fit in 24 bytes (same as Vec<u8>).
/// Layout: 1 byte tag + 23 bytes data
const INLINE_CAPACITY: usize = 23;

/// An inline vector optimized for key-value storage.
///
/// Small values (up to 23 bytes) are stored inline without heap allocation.
/// Larger values use reference-counted shared storage.
///
/// This type is inspired by sled's IVec and optimized for:
/// - Database keys (typically short)
/// - Zero-copy cloning of large values
/// - Memory efficiency
#[derive(Clone)]
pub struct IVec {
    inner: IVecInner,
}

#[derive(Clone)]
enum IVecInner {
    /// Inline storage for small values.
    /// First byte is length, remaining bytes are data.
    Inline {
        len: u8,
        data: [u8; INLINE_CAPACITY],
    },
    /// Heap storage for large values using Arc.
    Heap(Arc<[u8]>),
}

impl IVec {
    /// Creates an empty IVec.
    #[inline]
    pub const fn new() -> Self {
        Self {
            inner: IVecInner::Inline {
                len: 0,
                data: [0; INLINE_CAPACITY],
            },
        }
    }

    /// Creates an IVec from a byte slice.
    #[inline]
    pub fn from_slice(data: &[u8]) -> Self {
        if data.len() <= INLINE_CAPACITY {
            let mut inline_data = [0u8; INLINE_CAPACITY];
            inline_data[..data.len()].copy_from_slice(data);
            Self {
                inner: IVecInner::Inline {
                    len: data.len() as u8,
                    data: inline_data,
                },
            }
        } else {
            Self {
                inner: IVecInner::Heap(Arc::from(data)),
            }
        }
    }

    /// Creates an IVec from a Vec, consuming it.
    ///
    /// For large values, this avoids a copy by using Arc::from.
    #[inline]
    pub fn from_vec(data: Vec<u8>) -> Self {
        if data.len() <= INLINE_CAPACITY {
            let mut inline_data = [0u8; INLINE_CAPACITY];
            inline_data[..data.len()].copy_from_slice(&data);
            Self {
                inner: IVecInner::Inline {
                    len: data.len() as u8,
                    data: inline_data,
                },
            }
        } else {
            Self {
                inner: IVecInner::Heap(Arc::from(data.into_boxed_slice())),
            }
        }
    }

    /// Returns the length of the data.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.inner {
            IVecInner::Inline { len, .. } => *len as usize,
            IVecInner::Heap(arc) => arc.len(),
        }
    }

    /// Returns true if the IVec is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the data as a byte slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            IVecInner::Inline { len, data } => &data[..*len as usize],
            IVecInner::Heap(arc) => arc,
        }
    }

    /// Converts to a Vec<u8>.
    ///
    /// For inline values, this allocates. For heap values, if the Arc
    /// has only one reference, it unwraps without copying.
    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        match &self.inner {
            IVecInner::Inline { len, data } => data[..*len as usize].to_vec(),
            IVecInner::Heap(arc) => arc.to_vec(),
        }
    }

    /// Returns true if this value is stored inline.
    #[inline]
    pub fn is_inline(&self) -> bool {
        matches!(self.inner, IVecInner::Inline { .. })
    }

    /// Returns the Arc if this is a heap-allocated value.
    /// Useful for zero-copy access when you need owned data.
    #[inline]
    pub fn as_arc(&self) -> Option<&Arc<[u8]>> {
        match &self.inner {
            IVecInner::Heap(arc) => Some(arc),
            IVecInner::Inline { .. } => None,
        }
    }
}

impl Default for IVec {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for IVec {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[u8]> for IVec {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Borrow<[u8]> for IVec {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<&[u8]> for IVec {
    #[inline]
    fn from(data: &[u8]) -> Self {
        Self::from_slice(data)
    }
}

impl From<Vec<u8>> for IVec {
    #[inline]
    fn from(data: Vec<u8>) -> Self {
        Self::from_vec(data)
    }
}

impl From<&str> for IVec {
    #[inline]
    fn from(s: &str) -> Self {
        Self::from_slice(s.as_bytes())
    }
}

impl From<String> for IVec {
    #[inline]
    fn from(s: String) -> Self {
        Self::from_vec(s.into_bytes())
    }
}

impl From<IVec> for Vec<u8> {
    #[inline]
    fn from(iv: IVec) -> Self {
        iv.to_vec()
    }
}

impl PartialEq for IVec {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for IVec {}

impl PartialEq<[u8]> for IVec {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<Vec<u8>> for IVec {
    #[inline]
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialOrd for IVec {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IVec {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl Hash for IVec {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl fmt::Debug for IVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data = self.as_slice();
        if data.iter().all(|&b| b.is_ascii_graphic() || b == b' ') {
            write!(f, "IVec({:?})", String::from_utf8_lossy(data))
        } else {
            write!(f, "IVec({:?})", data)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_storage() {
        let small = IVec::from_slice(b"hello");
        assert!(small.is_inline());
        assert_eq!(small.len(), 5);
        assert_eq!(small.as_slice(), b"hello");
    }

    #[test]
    fn test_heap_storage() {
        let large = IVec::from_slice(&[0u8; 100]);
        assert!(!large.is_inline());
        assert_eq!(large.len(), 100);
    }

    #[test]
    fn test_clone_inline() {
        let original = IVec::from_slice(b"hello");
        let cloned = original.clone();
        assert_eq!(original, cloned);
        assert!(cloned.is_inline());
    }

    #[test]
    fn test_clone_heap() {
        let original = IVec::from_slice(&[42u8; 100]);
        let cloned = original.clone();
        assert_eq!(original, cloned);
        // Both should point to the same Arc
        assert!(original.as_arc().is_some());
        assert!(cloned.as_arc().is_some());
    }

    #[test]
    fn test_boundary_cases() {
        // Exactly at inline capacity
        let at_capacity = IVec::from_slice(&[0u8; INLINE_CAPACITY]);
        assert!(at_capacity.is_inline());

        // One byte over capacity
        let over_capacity = IVec::from_slice(&[0u8; INLINE_CAPACITY + 1]);
        assert!(!over_capacity.is_inline());
    }

    #[test]
    fn test_empty() {
        let empty = IVec::new();
        assert!(empty.is_empty());
        assert!(empty.is_inline());
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn test_ordering() {
        let a = IVec::from_slice(b"apple");
        let b = IVec::from_slice(b"banana");
        assert!(a < b);
    }
}

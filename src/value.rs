//! Summary: Zero-copy value types for Phase 2 read dominance.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides distinct types for borrowed vs owned values:
//! - `BorrowedValue`: A reference tied to a transaction's lifetime
//! - `OwnedValue`: An owned copy that can outlive the transaction
//!
//! # Design Philosophy
//!
//! Thunder provides explicit type-level distinction between borrowed and
//! owned data. This makes the zero-copy contract clear at compile time:
//!
//! - `BorrowedValue<'tx>` - Cannot outlive the transaction
//! - `OwnedValue` - Owns its data, can be stored freely
//!
//! # Performance Considerations
//!
//! - `BorrowedValue` has zero allocation overhead
//! - `OwnedValue` allocates when created but can be passed around freely
//! - Use `BorrowedValue` when processing data within a transaction
//! - Use `OwnedValue` when data needs to outlive the transaction

use std::borrow::Cow;
use std::ops::Deref;

/// A borrowed reference to a value, tied to a transaction's lifetime.
///
/// This type provides zero-copy access to values stored in the database.
/// The reference is valid for the lifetime of the transaction that created it.
///
/// # Lifetime
///
/// The lifetime parameter `'tx` represents the transaction lifetime.
/// The value cannot outlive the transaction:
///
/// ```ignore
/// let borrowed: BorrowedValue<'_>;
/// {
///     let rtx = db.read_tx();
///     borrowed = rtx.get_borrowed(b"key").unwrap();
/// } // Error: rtx dropped but borrowed still in use
/// ```
///
/// # Zero-Copy
///
/// No data is copied when creating a `BorrowedValue`. The internal slice
/// points directly to the database's memory.
#[derive(Debug, Clone, Copy)]
pub struct BorrowedValue<'tx> {
    /// The underlying byte slice.
    data: &'tx [u8],
}

impl<'tx> BorrowedValue<'tx> {
    /// Creates a new borrowed value from a slice.
    #[inline]
    pub(crate) fn new(data: &'tx [u8]) -> Self {
        Self { data }
    }

    /// Returns the value as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.data
    }

    /// Returns the length of the value in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the value is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Converts to an owned value.
    ///
    /// This allocates a new `Vec<u8>` with a copy of the data.
    #[inline]
    pub fn to_owned(&self) -> OwnedValue {
        OwnedValue::new(self.data.to_vec())
    }

    /// Converts to a `Cow` for flexible ownership.
    #[inline]
    pub fn to_cow(&self) -> Cow<'tx, [u8]> {
        Cow::Borrowed(self.data)
    }
}

impl<'tx> Deref for BorrowedValue<'tx> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<'tx> AsRef<[u8]> for BorrowedValue<'tx> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

impl<'tx> PartialEq<[u8]> for BorrowedValue<'tx> {
    fn eq(&self, other: &[u8]) -> bool {
        self.data == other
    }
}

impl<'tx> PartialEq<&[u8]> for BorrowedValue<'tx> {
    fn eq(&self, other: &&[u8]) -> bool {
        self.data == *other
    }
}

impl<'tx> PartialEq<Vec<u8>> for BorrowedValue<'tx> {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.data == other.as_slice()
    }
}

impl<'tx> From<&'tx [u8]> for BorrowedValue<'tx> {
    fn from(data: &'tx [u8]) -> Self {
        Self::new(data)
    }
}

/// An owned value that can outlive the transaction.
///
/// This type holds an owned copy of the data, allowing it to be stored
/// and used after the transaction has ended.
///
/// # Allocation
///
/// Creating an `OwnedValue` allocates memory for the data. Use
/// `BorrowedValue` when possible to avoid this overhead.
///
/// # Example
///
/// ```ignore
/// let owned: OwnedValue;
/// {
///     let rtx = db.read_tx();
///     owned = rtx.get_owned(b"key").unwrap();
/// } // Transaction dropped
///
/// // OwnedValue is still valid
/// println!("Value: {:?}", owned.as_ref());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnedValue {
    /// The owned byte data.
    data: Vec<u8>,
}

impl OwnedValue {
    /// Creates a new owned value.
    #[inline]
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Creates an owned value from a slice (copies the data).
    #[inline]
    pub fn from_slice(data: &[u8]) -> Self {
        Self {
            data: data.to_vec(),
        }
    }

    /// Returns the value as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Returns the length of the value in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the value is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Consumes self and returns the inner Vec.
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }

    /// Returns a borrowed view of this value.
    #[inline]
    pub fn as_borrowed(&self) -> BorrowedValue<'_> {
        BorrowedValue::new(&self.data)
    }
}

impl Deref for OwnedValue {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl AsRef<[u8]> for OwnedValue {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl From<Vec<u8>> for OwnedValue {
    fn from(data: Vec<u8>) -> Self {
        Self::new(data)
    }
}

impl From<&[u8]> for OwnedValue {
    fn from(data: &[u8]) -> Self {
        Self::from_slice(data)
    }
}

impl<const N: usize> From<&[u8; N]> for OwnedValue {
    fn from(data: &[u8; N]) -> Self {
        Self::from_slice(data.as_slice())
    }
}

impl PartialEq<[u8]> for OwnedValue {
    fn eq(&self, other: &[u8]) -> bool {
        self.data.as_slice() == other
    }
}

impl PartialEq<&[u8]> for OwnedValue {
    fn eq(&self, other: &&[u8]) -> bool {
        self.data.as_slice() == *other
    }
}

impl PartialEq<Vec<u8>> for OwnedValue {
    fn eq(&self, other: &Vec<u8>) -> bool {
        &self.data == other
    }
}

/// A value that may be either borrowed or owned.
///
/// This enum provides flexibility when the ownership semantics
/// depend on runtime conditions.
///
/// # Performance
///
/// The borrowed variant has zero allocation overhead.
/// The owned variant allocates once on creation.
#[derive(Debug, Clone)]
pub enum MaybeOwnedValue<'a> {
    /// A borrowed reference.
    Borrowed(BorrowedValue<'a>),
    /// An owned value.
    Owned(OwnedValue),
}

impl<'a> MaybeOwnedValue<'a> {
    /// Returns the value as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Borrowed(b) => b.as_bytes(),
            Self::Owned(o) => o.as_bytes(),
        }
    }

    /// Returns the length of the value.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Borrowed(b) => b.len(),
            Self::Owned(o) => o.len(),
        }
    }

    /// Returns true if the value is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Converts to an owned value, cloning if necessary.
    pub fn into_owned(self) -> OwnedValue {
        match self {
            Self::Borrowed(b) => b.to_owned(),
            Self::Owned(o) => o,
        }
    }

    /// Returns true if this is a borrowed value.
    #[inline]
    pub fn is_borrowed(&self) -> bool {
        matches!(self, Self::Borrowed(_))
    }

    /// Returns true if this is an owned value.
    #[inline]
    pub fn is_owned(&self) -> bool {
        matches!(self, Self::Owned(_))
    }
}

impl<'a> AsRef<[u8]> for MaybeOwnedValue<'a> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> From<BorrowedValue<'a>> for MaybeOwnedValue<'a> {
    fn from(value: BorrowedValue<'a>) -> Self {
        Self::Borrowed(value)
    }
}

impl<'a> From<OwnedValue> for MaybeOwnedValue<'a> {
    fn from(value: OwnedValue) -> Self {
        Self::Owned(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_borrowed_value() {
        let data = b"hello world";
        let borrowed = BorrowedValue::new(data);

        assert_eq!(borrowed.len(), 11);
        assert!(!borrowed.is_empty());
        assert_eq!(borrowed.as_bytes(), b"hello world");
        assert!(borrowed == b"hello world".as_slice());
    }

    #[test]
    fn test_borrowed_to_owned() {
        let data = b"test data";
        let borrowed = BorrowedValue::new(data);
        let owned = borrowed.to_owned();

        assert_eq!(owned.as_bytes(), borrowed.as_bytes());
        assert_eq!(owned.len(), 9);
    }

    #[test]
    fn test_owned_value() {
        let owned = OwnedValue::new(b"test".to_vec());

        assert_eq!(owned.len(), 4);
        assert!(!owned.is_empty());
        assert_eq!(owned.as_bytes(), b"test");
    }

    #[test]
    fn test_owned_value_conversions() {
        let owned = OwnedValue::from_slice(b"slice");
        assert_eq!(owned.as_bytes(), b"slice");

        let owned2: OwnedValue = b"array".into();
        assert_eq!(owned2.as_bytes(), b"array");

        let vec: Vec<u8> = owned2.into_vec();
        assert_eq!(vec, b"array");
    }

    #[test]
    fn test_maybe_owned_value() {
        let data = b"borrowed";
        let borrowed = BorrowedValue::new(data);
        let maybe_borrowed = MaybeOwnedValue::Borrowed(borrowed);

        assert!(maybe_borrowed.is_borrowed());
        assert!(!maybe_borrowed.is_owned());
        assert_eq!(maybe_borrowed.as_bytes(), b"borrowed");

        let owned = OwnedValue::new(b"owned".to_vec());
        let maybe_owned = MaybeOwnedValue::Owned(owned);

        assert!(!maybe_owned.is_borrowed());
        assert!(maybe_owned.is_owned());
        assert_eq!(maybe_owned.as_bytes(), b"owned");
    }

    #[test]
    fn test_maybe_owned_into_owned() {
        let data = b"test";
        let borrowed = BorrowedValue::new(data);
        let maybe = MaybeOwnedValue::Borrowed(borrowed);
        let owned = maybe.into_owned();

        assert_eq!(owned.as_bytes(), b"test");
    }
}

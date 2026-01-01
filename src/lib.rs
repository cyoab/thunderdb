//! Summary: thunder - A minimal, embedded, transactional key-value database engine.
//! Copyright (c) YOAB. All rights reserved.

pub mod btree;
pub mod bucket;
pub mod db;
pub mod error;
pub mod freelist;
pub mod meta;
pub mod mmap;
pub mod page;
pub mod tx;

// Re-export public API at crate root for convenience.
pub use btree::{Bound, BTreeIter, BTreeRangeIter};
pub use bucket::{BucketBound, BucketIter, BucketMut, BucketRangeIter, BucketRef, MAX_BUCKET_NAME_LEN};
pub use db::Database;
pub use error::{Error, Result};
pub use tx::{ReadTx, WriteTx};

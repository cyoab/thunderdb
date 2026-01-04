//! Summary: thunder - A minimal, embedded, transactional key-value database engine.
//! Copyright (c) YOAB. All rights reserved.

pub mod aligned;
#[cfg(feature = "failpoint")]
pub mod failpoint;
pub mod arena;
pub mod bloom;
pub mod btree;
pub mod bucket;
pub mod checkpoint;
pub mod coalescer;
pub mod concurrent;
pub mod db;
pub mod error;
pub mod freelist;
pub mod group_commit;
pub mod io_backend;
pub mod ivec;
pub mod meta;
pub mod mmap;
pub mod node_pool;
pub mod overflow;
pub mod page;
pub mod parallel;
pub mod tx;
pub mod wal;
pub mod wal_record;

// io_uring backend (Linux only, feature-gated)
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub mod uring;

// Re-export public API at crate root for convenience.
pub use aligned::{AlignedBuffer, AlignedBufferPool, DEFAULT_ALIGNMENT};
pub use arena::{Arena, DEFAULT_ARENA_SIZE, TypedArena};
pub use btree::{BTreeIter, BTreeRangeIter, Bound};
pub use bucket::{
    BucketBound, BucketIter, BucketMut, BucketRangeIter, BucketRef, MAX_BUCKET_NAME_LEN,
    MAX_NESTING_DEPTH, NestedBucketIter, NestedBucketRef,
};
pub use checkpoint::{CheckpointConfig, CheckpointInfo, CheckpointManager};
pub use concurrent::{PARALLEL_THRESHOLD, ParallelWriteStats, prepare_entries_parallel};
pub use db::{Database, DatabaseOptions};
pub use error::{Error, Result};
pub use group_commit::{GroupCommitConfig, GroupCommitManager};
pub use io_backend::{IoBackend, ReadOp, ReadResult, SyncBackend, WriteOp};
pub use mmap::{AccessPattern, Mmap, MmapOptions};
pub use node_pool::{DEFAULT_MAX_POOLED, NodePool, PoolStats, PooledBranchNode, PooledLeafNode};
pub use overflow::{DEFAULT_OVERFLOW_THRESHOLD, OverflowRef};
pub use page::PageSizeConfig;
pub use parallel::{ParallelConfig, ParallelWriter, partition_for_parallel};
pub use tx::{ReadTx, WriteTx};
pub use wal::{Lsn, SyncPolicy, Wal, WalConfig};
pub use wal_record::{RECORD_HEADER_SIZE, WalRecord};

#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use uring::UringBackend;

#[cfg(feature = "failpoint")]
pub use failpoint::{
    FailAction, FailpointBuilder, FailpointGuard, FailpointRegistry, FailpointScopeGuard,
    RandomFailpointSelector,
};

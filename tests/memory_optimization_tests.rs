//! Summary: Tests for memory management optimizations.
//! Copyright (c) YOAB. All rights reserved.
//!
//! These tests verify that memory management optimizations (mmap hints,
//! prefetching, alignment) are correctly implemented and functional.

use std::fs;
use std::time::Instant;

fn test_db_path(name: &str) -> String {
    format!("/tmp/thunder_memory_opt_test_{name}.db")
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
    // Also cleanup WAL directory if it exists
    let wal_dir = format!("{path}.wal");
    let _ = fs::remove_dir_all(&wal_dir);
}

// ==================== Mmap Optimization Tests ====================

#[test]
fn test_mmap_access_hint_sequential() {
    use thunder::mmap::{Mmap, MmapOptions, AccessPattern};
    use thunder::page::PAGE_SIZE;
    use std::io::Write;
    use std::fs::OpenOptions;

    let path = test_db_path("mmap_sequential");
    cleanup(&path);

    // Create a test file with multiple pages
    let pages = 32;
    let file_size = pages * PAGE_SIZE;
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create file");

        // Write pattern to each page
        for page_num in 0..pages {
            let mut page = [0u8; PAGE_SIZE];
            page[0..8].copy_from_slice(&(page_num as u64).to_le_bytes());
            // Fill with pattern to exercise page access
            for (i, byte) in page[8..].iter_mut().enumerate() {
                *byte = ((page_num + i) % 256) as u8;
            }
            file.write_all(&page).expect("write page");
        }
        file.sync_all().expect("sync");
    }

    // Open with sequential access hint
    let file = OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("open file");

    let options = MmapOptions::new()
        .with_access_pattern(AccessPattern::Sequential);
    
    let mmap = Mmap::with_options(&file, file_size, options)
        .expect("mmap with sequential hint");

    // Read pages sequentially - kernel should prefetch ahead
    let mut sum: u64 = 0;
    for page_num in 0..pages as u64 {
        let page = mmap.page(page_num).expect("page should exist");
        let stored_num = u64::from_le_bytes(page[0..8].try_into().unwrap());
        assert_eq!(stored_num, page_num);
        sum += stored_num;
    }

    // Verify we read all pages
    assert_eq!(sum, (0..pages as u64).sum());

    cleanup(&path);
}

#[test]
fn test_mmap_access_hint_random() {
    use thunder::mmap::{Mmap, MmapOptions, AccessPattern};
    use thunder::page::PAGE_SIZE;
    use std::io::Write;
    use std::fs::OpenOptions;

    let path = test_db_path("mmap_random");
    cleanup(&path);

    // Create test file
    let pages = 32;
    let file_size = pages * PAGE_SIZE;
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create file");

        for page_num in 0..pages {
            let mut page = [0u8; PAGE_SIZE];
            page[0..8].copy_from_slice(&(page_num as u64).to_le_bytes());
            file.write_all(&page).expect("write page");
        }
        file.sync_all().expect("sync");
    }

    let file = OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("open file");

    let options = MmapOptions::new()
        .with_access_pattern(AccessPattern::Random);
    
    let mmap = Mmap::with_options(&file, file_size, options)
        .expect("mmap with random hint");

    // Access pages in random order
    let random_order = [15, 3, 28, 7, 22, 1, 30, 10, 19, 5];
    for &page_num in &random_order {
        let page = mmap.page(page_num as u64).expect("page should exist");
        let stored_num = u64::from_le_bytes(page[0..8].try_into().unwrap());
        assert_eq!(stored_num, page_num as u64);
    }

    cleanup(&path);
}

#[test]
fn test_mmap_prefetch() {
    use thunder::mmap::Mmap;
    use thunder::page::PAGE_SIZE;
    use std::io::Write;
    use std::fs::OpenOptions;

    let path = test_db_path("mmap_prefetch");
    cleanup(&path);

    // Create test file with enough pages for prefetch to matter
    let pages = 64;
    let file_size = pages * PAGE_SIZE;
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create file");

        for page_num in 0..pages {
            let mut page = [0u8; PAGE_SIZE];
            page[0..8].copy_from_slice(&(page_num as u64).to_le_bytes());
            file.write_all(&page).expect("write page");
        }
        file.sync_all().expect("sync");
    }

    let file = OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("open file");

    let mmap = Mmap::new(&file, file_size).expect("create mmap");

    // Prefetch a range of pages
    let prefetch_start = 10;
    let prefetch_pages = 8;
    let prefetch_len = prefetch_pages * PAGE_SIZE;
    
    // This should hint to the kernel to bring pages into memory
    mmap.prefetch(prefetch_start * PAGE_SIZE, prefetch_len);

    // Access the prefetched pages - should be faster (though we don't measure here)
    for page_num in prefetch_start..(prefetch_start + prefetch_pages) {
        let page = mmap.page(page_num as u64).expect("page should exist");
        let stored_num = u64::from_le_bytes(page[0..8].try_into().unwrap());
        assert_eq!(stored_num, page_num as u64);
    }

    cleanup(&path);
}

#[test]
fn test_mmap_populate_flag() {
    use thunder::mmap::{Mmap, MmapOptions};
    use thunder::page::PAGE_SIZE;
    use std::io::Write;
    use std::fs::OpenOptions;

    let path = test_db_path("mmap_populate");
    cleanup(&path);

    // Create test file
    let pages = 16;
    let file_size = pages * PAGE_SIZE;
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create file");

        for page_num in 0..pages {
            let mut page = [0u8; PAGE_SIZE];
            page[0..8].copy_from_slice(&(page_num as u64).to_le_bytes());
            file.write_all(&page).expect("write page");
        }
        file.sync_all().expect("sync");
    }

    let file = OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("open file");

    // Create mmap with populate flag - pages should be pre-faulted
    let options = MmapOptions::new().with_populate(true);
    let mmap = Mmap::with_options(&file, file_size, options)
        .expect("mmap with populate");

    // All pages should already be resident - verify they're accessible
    for page_num in 0..pages as u64 {
        let page = mmap.page(page_num).expect("page should exist");
        let stored_num = u64::from_le_bytes(page[0..8].try_into().unwrap());
        assert_eq!(stored_num, page_num);
    }

    cleanup(&path);
}

// ==================== Arena Alignment Tests ====================

#[test]
fn test_arena_cache_line_alignment() {
    use thunder::arena::Arena;

    let mut arena = Arena::new(4096);
    
    // Allocate with cache line alignment (64 bytes on most modern CPUs)
    // Store pointers before making new allocations to avoid borrow conflicts
    let ptr1 = {
        let slice = arena.alloc_aligned(128, 64);
        slice.as_ptr() as usize
    };
    
    let ptr2 = {
        let slice = arena.alloc_aligned(256, 64);
        slice.as_ptr() as usize
    };
    
    let ptr3 = {
        let slice = arena.alloc_aligned(64, 64);
        slice.as_ptr() as usize
    };

    // Verify alignment
    assert_eq!(ptr1 % 64, 0, "first allocation not cache-line aligned");
    assert_eq!(ptr2 % 64, 0, "second allocation not cache-line aligned");
    assert_eq!(ptr3 % 64, 0, "third allocation not cache-line aligned");

    // Verify allocations are independent
    assert_ne!(ptr1, ptr2);
    assert_ne!(ptr2, ptr3);
}

#[test]
fn test_arena_page_alignment() {
    use thunder::arena::Arena;
    use thunder::page::PAGE_SIZE;

    let mut arena = Arena::new(PAGE_SIZE * 4);

    // Allocate with page alignment
    let page_aligned = arena.alloc_aligned(PAGE_SIZE, PAGE_SIZE);
    
    assert_eq!(page_aligned.len(), PAGE_SIZE);
    assert_eq!(
        page_aligned.as_ptr() as usize % PAGE_SIZE, 
        0, 
        "allocation not page-aligned"
    );
}

// ==================== Memory Advice Tests ====================

#[test]
fn test_mmap_dontneed_hint() {
    use thunder::mmap::Mmap;
    use thunder::page::PAGE_SIZE;
    use std::io::Write;
    use std::fs::OpenOptions;

    let path = test_db_path("mmap_dontneed");
    cleanup(&path);

    // Create test file
    let pages = 32;
    let file_size = pages * PAGE_SIZE;
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create file");

        for _page_num in 0..pages {
            let page = [0u8; PAGE_SIZE];
            file.write_all(&page).expect("write page");
        }
        file.sync_all().expect("sync");
    }

    let file = OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("open file");

    let mmap = Mmap::new(&file, file_size).expect("create mmap");

    // Read some pages
    for page_num in 0..16 {
        let _ = mmap.page(page_num as u64);
    }

    // Mark pages 0-8 as not needed anymore
    // This hints to the kernel that it can reclaim this memory
    mmap.dontneed(0, 8 * PAGE_SIZE);

    // Should still be able to read (kernel may re-read from file)
    let page = mmap.page(0).expect("page should still be accessible");
    assert_eq!(page.len(), PAGE_SIZE);

    cleanup(&path);
}

// ==================== Integration with Database ====================

#[test]
fn test_database_with_sequential_mmap() {
    use thunder::{Database, DatabaseOptions};

    let path = test_db_path("db_seq_mmap");
    cleanup(&path);

    // Create database with sequential access pattern
    {
        let mut db = Database::open_with_options(&path, DatabaseOptions::default())
            .expect("open should succeed");
        
        // Insert enough keys to span multiple pages
        {
            let mut wtx = db.write_tx();
            for i in 0..1000 {
                let key = format!("key_{i:05}");
                let value = format!("value_{i}");
                wtx.put(key.as_bytes(), value.as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }
    }

    // Reopen and verify - mmap optimizations should be active
    {
        let db = Database::open(&path).expect("reopen should succeed");
        let rtx = db.read_tx();
        
        // Sequential scan (benefits from sequential hint)
        for i in 0..1000 {
            let key = format!("key_{i:05}");
            let expected = format!("value_{i}");
            assert_eq!(rtx.get(key.as_bytes()), Some(expected.into_bytes()));
        }
    }

    cleanup(&path);
}

#[test]
fn test_large_file_mmap_performance() {
    use thunder::mmap::{Mmap, MmapOptions, AccessPattern};
    use thunder::page::PAGE_SIZE;
    use std::io::Write;
    use std::fs::OpenOptions;

    let path = test_db_path("mmap_large_perf");
    cleanup(&path);

    // Create a larger file to test prefetch impact
    let pages = 256; // 8MB with 32KB pages
    let file_size = pages * PAGE_SIZE;
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create file");

        for page_num in 0..pages {
            let mut page = [0u8; PAGE_SIZE];
            page[0..8].copy_from_slice(&(page_num as u64).to_le_bytes());
            file.write_all(&page).expect("write page");
        }
        file.sync_all().expect("sync");
    }

    let file = OpenOptions::new()
        .read(true)
        .open(&path)
        .expect("open file");

    // Test with sequential hint
    let options = MmapOptions::new()
        .with_access_pattern(AccessPattern::Sequential);
    let mmap = Mmap::with_options(&file, file_size, options)
        .expect("mmap with sequential");

    // Sequential read - should benefit from kernel readahead
    let start = Instant::now();
    let mut checksum: u64 = 0;
    for page_num in 0..pages as u64 {
        let page = mmap.page(page_num).expect("page exists");
        checksum ^= u64::from_le_bytes(page[0..8].try_into().unwrap());
    }
    let sequential_time = start.elapsed();

    // Verify we touched all pages
    assert_eq!(checksum, (0..pages as u64).fold(0, |acc, x| acc ^ x));

    // Print timing for benchmarking purposes (not a hard assertion)
    println!("Sequential read of {pages} pages ({} MB): {:?}", 
             (pages * PAGE_SIZE) / (1024 * 1024), sequential_time);

    cleanup(&path);
}

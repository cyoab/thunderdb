use std::time::Instant;
use thunder::Database;

fn main() {
    let db_path = "/tmp/thunder_bench.db";
    let _ = std::fs::remove_file(db_path);

    let mut db = Database::open(db_path).expect("open should succeed");

    println!("=== Thunder Benchmark Results ===");
    println!();

    // Benchmark 1: Sequential writes (single transaction)
    {
        let count = 100_000;
        let start = Instant::now();
        {
            let mut wtx = db.write_tx();
            for i in 0..count {
                let key = format!("key_{i:08}");
                let value = format!("value_{i:08}_padding_data");
                wtx.put(key.as_bytes(), value.as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }
        let elapsed = start.elapsed();
        let ops_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "Sequential writes (100K keys, 1 tx): {:?} ({:.0} ops/sec)",
            elapsed, ops_per_sec
        );
    }

    // Benchmark 2: Sequential reads
    {
        let count = 100_000;
        let start = Instant::now();
        {
            let rtx = db.read_tx();
            for i in 0..count {
                let key = format!("key_{i:08}");
                let _ = rtx.get(key.as_bytes());
            }
        }
        let elapsed = start.elapsed();
        let ops_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "Sequential reads (100K keys): {:?} ({:.0} ops/sec)",
            elapsed, ops_per_sec
        );
    }

    // Benchmark 3: Random reads
    {
        let count = 100_000;
        let mut state: u64 = 12345;
        let mut next_rand = || -> u64 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state >> 33
        };

        let start = Instant::now();
        {
            let rtx = db.read_tx();
            for _ in 0..count {
                let idx = next_rand() % 100_000;
                let key = format!("key_{idx:08}");
                let _ = rtx.get(key.as_bytes());
            }
        }
        let elapsed = start.elapsed();
        let ops_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "Random reads (100K lookups): {:?} ({:.0} ops/sec)",
            elapsed, ops_per_sec
        );
    }

    // Benchmark 4: Iterator scan
    {
        let start = Instant::now();
        let count = {
            let rtx = db.read_tx();
            rtx.iter().count()
        };
        let elapsed = start.elapsed();
        let ops_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "Iterator scan (100K keys): {:?} ({:.0} ops/sec)",
            elapsed, ops_per_sec
        );
    }

    // Benchmark 5: Mixed read/write workload
    {
        let ops = 10_000;
        let mut state: u64 = 54321;
        let mut next_rand = || -> u64 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state >> 33
        };

        let start = Instant::now();
        for i in 0..ops {
            if next_rand() % 10 < 7 {
                // 70% reads
                let rtx = db.read_tx();
                let idx = next_rand() % 100_000;
                let key = format!("key_{idx:08}");
                let _ = rtx.get(key.as_bytes());
            } else {
                // 30% writes
                let mut wtx = db.write_tx();
                let key = format!("mixed_{i:08}");
                wtx.put(key.as_bytes(), b"mixed_value");
                wtx.commit().expect("commit should succeed");
            }
        }
        let elapsed = start.elapsed();
        let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
        println!(
            "Mixed workload (10K ops, 70% read): {:?} ({:.0} ops/sec)",
            elapsed, ops_per_sec
        );
    }

    // Benchmark 6: Batch transactions (100 ops per tx)
    {
        let tx_count = 1000;
        let ops_per_tx = 100;
        let start = Instant::now();
        for t in 0..tx_count {
            let mut wtx = db.write_tx();
            for i in 0..ops_per_tx {
                let key = format!("batch_{t:06}_{i:03}");
                wtx.put(key.as_bytes(), b"batch_value");
            }
            wtx.commit().expect("commit should succeed");
        }
        let elapsed = start.elapsed();
        let total_ops = tx_count * ops_per_tx;
        let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
        let tx_per_sec = tx_count as f64 / elapsed.as_secs_f64();
        println!(
            "Batch writes (1K tx, 100 ops/tx): {:?} ({:.0} ops/sec, {:.1} tx/sec)",
            elapsed, ops_per_sec, tx_per_sec
        );
    }

    // Benchmark 7: Large values
    {
        let sizes = [1024, 10240, 102400, 1048576]; // 1KB, 10KB, 100KB, 1MB
        for size in sizes {
            let value: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

            let count = 100;
            let start = Instant::now();
            {
                let mut wtx = db.write_tx();
                for i in 0..count {
                    let key = format!("large_{size}_{i:03}");
                    wtx.put(key.as_bytes(), &value);
                }
                wtx.commit().expect("commit should succeed");
            }
            let elapsed = start.elapsed();
            let mb_per_sec = (size * count) as f64 / elapsed.as_secs_f64() / 1048576.0;
            println!(
                "Large values ({}KB x 100): {:?} ({:.1} MB/sec)",
                size / 1024,
                elapsed,
                mb_per_sec
            );
        }
    }

    println!();

    let _ = std::fs::remove_file(db_path);
}

package main

import (
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

func main() {
	dbPath := "/tmp/bbolt_bench.db"
	os.Remove(dbPath)

	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer os.Remove(dbPath)

	bucketName := []byte("bench")

	// Create bucket
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})

	fmt.Println("=== BBolt Benchmark Results ===")
	fmt.Println()

	// Benchmark 1: Sequential writes (single transaction)
	{
		count := 100_000
		start := time.Now()
		db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			for i := 0; i < count; i++ {
				key := fmt.Sprintf("key_%08d", i)
				value := fmt.Sprintf("value_%08d_padding_data", i)
				b.Put([]byte(key), []byte(value))
			}
			return nil
		})
		elapsed := time.Since(start)
		opsPerSec := float64(count) / elapsed.Seconds()
		fmt.Printf("Sequential writes (100K keys, 1 tx): %v (%.0f ops/sec)\n", elapsed, opsPerSec)
	}

	// Benchmark 2: Sequential reads
	{
		count := 100_000
		start := time.Now()
		db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			for i := 0; i < count; i++ {
				key := fmt.Sprintf("key_%08d", i)
				_ = b.Get([]byte(key))
			}
			return nil
		})
		elapsed := time.Since(start)
		opsPerSec := float64(count) / elapsed.Seconds()
		fmt.Printf("Sequential reads (100K keys): %v (%.0f ops/sec)\n", elapsed, opsPerSec)
	}

	// Benchmark 3: Random reads
	{
		count := 100_000
		// Simple PRNG
		state := uint64(12345)
		nextRand := func() uint64 {
			state = state*6364136223846793005 + 1
			return state >> 33
		}

		start := time.Now()
		db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			for i := 0; i < count; i++ {
				idx := nextRand() % 100_000
				key := fmt.Sprintf("key_%08d", idx)
				_ = b.Get([]byte(key))
			}
			return nil
		})
		elapsed := time.Since(start)
		opsPerSec := float64(count) / elapsed.Seconds()
		fmt.Printf("Random reads (100K lookups): %v (%.0f ops/sec)\n", elapsed, opsPerSec)
	}

	// Benchmark 4: Iterator scan
	{
		start := time.Now()
		count := 0
		db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			c := b.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				count++
			}
			return nil
		})
		elapsed := time.Since(start)
		opsPerSec := float64(count) / elapsed.Seconds()
		fmt.Printf("Iterator scan (100K keys): %v (%.0f ops/sec)\n", elapsed, opsPerSec)
	}

	// Benchmark 5: Mixed read/write workload
	{
		ops := 10_000
		state := uint64(54321)
		nextRand := func() uint64 {
			state = state*6364136223846793005 + 1
			return state >> 33
		}

		start := time.Now()
		for i := 0; i < ops; i++ {
			if nextRand()%10 < 7 { // 70% reads
				db.View(func(tx *bolt.Tx) error {
					b := tx.Bucket(bucketName)
					idx := nextRand() % 100_000
					key := fmt.Sprintf("key_%08d", idx)
					_ = b.Get([]byte(key))
					return nil
				})
			} else { // 30% writes
				db.Update(func(tx *bolt.Tx) error {
					b := tx.Bucket(bucketName)
					key := fmt.Sprintf("mixed_%08d", i)
					b.Put([]byte(key), []byte("mixed_value"))
					return nil
				})
			}
		}
		elapsed := time.Since(start)
		opsPerSec := float64(ops) / elapsed.Seconds()
		fmt.Printf("Mixed workload (10K ops, 70%% read): %v (%.0f ops/sec)\n", elapsed, opsPerSec)
	}

	// Benchmark 6: Batch transactions (100 ops per tx)
	{
		txCount := 1000
		opsPerTx := 100
		start := time.Now()
		for t := 0; t < txCount; t++ {
			db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(bucketName)
				for i := 0; i < opsPerTx; i++ {
					key := fmt.Sprintf("batch_%06d_%03d", t, i)
					b.Put([]byte(key), []byte("batch_value"))
				}
				return nil
			})
		}
		elapsed := time.Since(start)
		totalOps := txCount * opsPerTx
		opsPerSec := float64(totalOps) / elapsed.Seconds()
		txPerSec := float64(txCount) / elapsed.Seconds()
		fmt.Printf("Batch writes (1K tx, 100 ops/tx): %v (%.0f ops/sec, %.1f tx/sec)\n", elapsed, opsPerSec, txPerSec)
	}

	// Benchmark 7: Large values
	{
		sizes := []int{1024, 10240, 102400, 1048576} // 1KB, 10KB, 100KB, 1MB
		for _, size := range sizes {
			value := make([]byte, size)
			for i := range value {
				value[i] = byte(i % 256)
			}

			count := 100
			start := time.Now()
			db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(bucketName)
				for i := 0; i < count; i++ {
					key := fmt.Sprintf("large_%d_%03d", size, i)
					b.Put([]byte(key), value)
				}
				return nil
			})
			elapsed := time.Since(start)
			mbPerSec := float64(size*count) / elapsed.Seconds() / 1048576
			fmt.Printf("Large values (%dKB x 100): %v (%.1f MB/sec)\n", size/1024, elapsed, mbPerSec)
		}
	}

	fmt.Println()
}

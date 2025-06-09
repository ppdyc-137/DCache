package store

import (
	"strconv"
	"testing"
	"time"
)

type intValue int

func (v intValue) Len() int {
	return 1
}

func Benchmark_LRU2Cache_Set(b *testing.B) {
	c := newLRU2Cache(Options{
		CleanupInterval: time.Minute,
		BucketCount:     256,
		CapPerBucket:    32,
		Expiration:      10 * time.Second,
	})

	for i := range b.N {
		c.Set(strconv.FormatInt(int64(i), 10), intValue(i))
	}
}

func Benchmark_LRU2Cache_Get(b *testing.B) {
	c := newLRU2Cache(Options{
		CleanupInterval: time.Minute,
		BucketCount:     256,
		CapPerBucket:    32,
		Expiration:      10 * time.Second,
	})

	c.Set("0", intValue(0));
	for b.Loop() {
		c.Get("0")
	}
}

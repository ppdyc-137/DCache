package store

import (
	"strconv"
	"testing"
	"time"
)

type stringValue string

func (v stringValue) Len() int {
	return len(v)
}

func BenchmarkLRUCacheSet(b *testing.B) {
	c := newLRUCache(Options{
		MaxBytes:        4096,
		CleanupInterval: time.Minute,
	})

	for i := range b.N {
		c.Set(strconv.FormatInt(int64(i), 10), intValue(i))
	}
}

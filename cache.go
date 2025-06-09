package dcache

import (
	"DCache/store"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Cache struct {
	mutex       sync.Mutex
	store       store.Store
	opts        CacheOptions
	hits        atomic.Int64
	misses      atomic.Int64
	initialized atomic.Bool
	closed      atomic.Bool
}

type CacheOptions struct {
	CacheType       store.CacheType
	MaxBytes        int64
	CleanupInterval time.Duration
}

var DefaultCacheOptions = CacheOptions{
	CacheType:       store.LRU,
	MaxBytes:        8 * 1024 * 1024, // 8MB
	CleanupInterval: time.Minute,
}

func NewCache(opts CacheOptions) *Cache {
	return &Cache{
		opts: opts,
	}
}

func (c *Cache) Get(ctx context.Context, key string) (ByteView, bool) {
	if c.closed.Load() {
		return ByteView{}, false
	}

	if !c.initialized.Load() {
		c.misses.Add(1)
		return ByteView{}, false
	}

	value, ok := c.store.Get(key)
	if !ok {
		c.misses.Add(1)
		return ByteView{}, false
	}

	if view, ok := value.(ByteView); ok {
		c.hits.Add(1)
		return view, true
	} else {
		c.misses.Add(1)
		return ByteView{}, false
	}
}

func (c *Cache) ensuerInitialized() {
	if c.initialized.Load() {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.initialized.Load() {
		storeOpts := store.Options{
			MaxBytes:        c.opts.MaxBytes,
			CleanupInterval: c.opts.CleanupInterval,
		}
		c.store = store.NewStore(c.opts.CacheType, storeOpts)

		c.initialized.Store(true)
	}
}

func (c *Cache) Set(key string, val ByteView) {
	if c.closed.Load() {
		return
	}

	c.ensuerInitialized()

	c.store.Set(key, val)
}

func (c *Cache) SetWithExpiration(key string, val ByteView, expiration time.Time) {
	if c.closed.Load() {
		return
	}

	c.ensuerInitialized()

	duration := time.Until(expiration)
	if duration > 0 {
		c.store.SetWithExpiration(key, val, duration)
	}
}

func (c *Cache) Delete(key string) bool {
	if c.closed.Load() || !c.initialized.Load() {
		return false
	}

	return c.store.Delete(key)
}

func (c *Cache) Clear() {
	if c.closed.Load() || !c.initialized.Load() {
		return
	}

	c.store.Clear()

	c.hits.Store(0)
	c.misses.Store(0)
}

func (c *Cache) Len() int {
	if c.closed.Load() || !c.initialized.Load() {
		return 0
	}

	return c.store.Len()
}

func (c *Cache) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}

	if c.store != nil {
		if closer, ok := c.store.(interface{ Close() }); ok {
			closer.Close()
		}
		c.store = nil
	}

	c.initialized.Store(false)
}

func (c *Cache) Stats() map[string]any {
	stats := map[string]any{
		"initialized": c.initialized.Load(),
		"closed":      c.closed.Load(),
		"hits":        c.hits.Load(),
		"misses":      c.misses.Load(),
	}

	if c.initialized.Load() {
		stats["size"] = c.Len()

		totalRequests := stats["hits"].(int64) + stats["misses"].(int64)
		if totalRequests > 0 {
			stats["hit_rate"] = float64(stats["hits"].(int64)) / float64(totalRequests)
		} else {
			stats["hit_rate"] = 0.0
		}
	}

	return stats
}

package store

import (
	"container/list"
	"sync"
	"time"
)

type lruCache struct {
	mutex           sync.RWMutex
	list            *list.List
	items           map[string]*list.Element
	expires         map[string]time.Time
	maxBytes        int64
	usedBytes       int64
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
}

type lruEntry struct {
	key   string
	value Value
}

func newLRUCache(opts Options) *lruCache {
	c := &lruCache{
		list:            list.New(),
		items:           make(map[string]*list.Element),
		expires:         make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		cleanupInterval: opts.CleanupInterval,
		cleanupTicker:   time.NewTicker(opts.CleanupInterval),
	}
	go c.cleanupLoop()
	return c
}

func (c *lruCache) Get(key string) (Value, bool) {
	c.mutex.RLock()
	ele, ok := c.items[key]
	if !ok {
		c.mutex.RUnlock()
		return nil, false
	}

	if expiration, hasExp := c.expires[key]; hasExp && time.Now().After(expiration) {
		c.mutex.RUnlock()

		go c.Delete(key)
		return nil, false
	}
	c.mutex.RUnlock()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	ele, ok = c.items[key]
	if !ok {
		return nil, false
	}

	c.list.MoveToFront(ele)
	return ele.Value.(*lruEntry).value, true
}

func (c *lruCache) Set(key string, val Value) {
	c.SetWithExpiration(key, val, 0)
}

func (c *lruCache) SetWithExpiration(key string, val Value, expiration time.Duration) {
	if val == nil {
		c.Delete(key)
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if expiration > 0 {
		c.expires[key] = time.Now().Add(expiration)
	} else {
		delete(c.expires, key)
	}

	if ele, ok := c.items[key]; ok {
		entry := ele.Value.(*lruEntry)
		c.usedBytes += int64(val.Len() - entry.value.Len())

		entry.value = val
		c.list.MoveToFront(ele)
		return
	}

	entry := &lruEntry{key, val}
	ele := c.list.PushFront(entry)
	c.items[key] = ele
	c.usedBytes += int64(len(entry.key) + entry.value.Len())

	c.cleanup()
}

func (c *lruCache) Delete(key string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if ele, ok := c.items[key]; ok {
		c.removeElement(ele)
		return true
	}
	return false
}

func (c *lruCache) Len() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.list.Len()
}

func (c *lruCache) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.list.Init()
	c.items = make(map[string]*list.Element)
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
}

func (c *lruCache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
	}
}

func (c *lruCache) cleanup() {
	// remove the expired keys
	now := time.Now()
	for key, expTime := range c.expires {
		if now.After(expTime) {
			c.removeElement(c.items[key])
		}
	}

	for c.maxBytes > 0 && c.usedBytes > c.maxBytes && c.list.Len() > 0 {
		ele := c.list.Back()
		c.removeElement(ele)
	}
}

func (c *lruCache) cleanupLoop() {
	for range c.cleanupTicker.C {
		c.mutex.Lock()
		c.cleanup()
		c.mutex.Unlock()
	}
}

func (c *lruCache) removeElement(ele *list.Element) {
	entry := ele.Value.(*lruEntry)
	c.list.Remove(ele)
	delete(c.items, entry.key)
	delete(c.expires, entry.key)
	c.usedBytes -= int64(len(entry.key) + entry.value.Len())
}

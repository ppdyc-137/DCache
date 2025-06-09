package store

import (
	"container/list"
	"sync"
	"time"
)

func hashBKRD(s string) (hash int32) {
	for i := range len(s) {
		hash = hash*131 + int32(s[i])
	}
	return hash
}

func maskOfNextPowOf2(cap uint16) uint32 {
	if cap > 0 && cap&(cap-1) == 0 {
		return uint32(cap - 1)
	}
	cap |= (cap >> 1)
	cap |= (cap >> 2)
	cap |= (cap >> 4)
	return uint32(cap | (cap >> 8))
}

type lru2Cache struct {
	locks         []sync.Mutex
	buckets       [][2]*cache
	expiration    time.Duration
	mask          int32
	cleanupTicker *time.Ticker
}

func newLRU2Cache(opts Options) *lru2Cache {
	mask := maskOfNextPowOf2(opts.BucketCount)
	c := &lru2Cache{
		locks:      make([]sync.Mutex, mask+1),
		buckets:    make([][2]*cache, mask+1),
		expiration: opts.Expiration,
		mask:       int32(mask),
	}
	for i := range c.buckets {
		c.buckets[i][0] = create(opts.CapPerBucket)
		c.buckets[i][1] = create(opts.CapPerBucket)
	}
	if opts.CleanupInterval > 0 {
		c.cleanupTicker = time.NewTicker(opts.CleanupInterval)
		go c.cleanupLoop()
	}
	return c
}

func (c *lru2Cache) Get(key string) (Value, bool) {
	idx := hashBKRD(key) & c.mask
	c.locks[idx].Lock()
	defer c.locks[idx].Unlock()

	if node, ok := c.buckets[idx][0].del(key); !ok {
		// refind in level-1
		node, ok := c.buckets[idx][1].get(key)
		if ok && (c.expiration == 0 || node.expireAt.After(time.Now())) {
			node.expireAt.Add(c.expiration)
			return node.val, true
		}
	} else {
		c.buckets[idx][1].set(key, node.val, node.expireAt)
		return node.val, true
	}
	return nil, false
}

func (c *lru2Cache) Set(key string, val Value) {
	c.SetWithExpiration(key, val, c.expiration)
}

func (c *lru2Cache) SetWithExpiration(key string, val Value, expiration time.Duration) {
	idx := hashBKRD(key) & c.mask
	c.locks[idx].Lock()
	if expiration == 0 {
		c.buckets[idx][0].set(key, val, nil)
	} else {
		time := time.Now().Add(expiration)
		c.buckets[idx][0].set(key, val, &time)
	}
	c.locks[idx].Unlock()
}

func (c *lru2Cache) Delete(key string) bool {
	idx := hashBKRD(key) & c.mask
	c.locks[idx].Lock()
	defer c.locks[idx].Unlock()

	_, ok1 := c.buckets[idx][0].del(key)
	_, ok2 := c.buckets[idx][1].del(key)
	return ok1 || ok2
}

func (c *lru2Cache) Len() int {
	count := 0
	for i := range c.buckets {
		c.locks[i].Lock()
		count += c.buckets[i][0].list.Len()
		count += c.buckets[i][1].list.Len()
		c.locks[i].Unlock()
	}
	return count
}

func (c *lru2Cache) Clear() {
	for i := range c.buckets {
		c.locks[i].Lock()
		c.buckets[i][0].clear()
		c.buckets[i][1].clear()
		c.locks[i].Unlock()
	}
}

func (c *lru2Cache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
	}
}

func (c *lru2Cache) cleanupLoop() {
	for range c.cleanupTicker.C {
		currentTime := time.Now()

		for i := range c.buckets {
			c.locks[i].Lock()

			c.buckets[i][0].cleanup(currentTime)
			c.buckets[i][1].cleanup(currentTime)

			c.locks[i].Unlock()
		}
	}
}

type node struct {
	key      string
	val      Value
	expireAt *time.Time
}

type cache struct {
	list    *list.List
	hashmap map[string]*list.Element
	cap     int
}

func create(cap int) *cache {
	return &cache{
		list:    list.New(),
		hashmap: make(map[string]*list.Element),
		cap:     cap,
	}
}

// update return false, add return true
func (c *cache) set(key string, val Value, expireAt *time.Time) bool {
	if ele, exists := c.hashmap[key]; exists {
		node := ele.Value.(*node)
		node.val, node.expireAt = val, expireAt
		c.list.MoveToFront(ele)
		return false
	}

	if c.list.Len() == c.cap {
		ele := c.list.Back()
		node := ele.Value.(*node)
		delete(c.hashmap, node.key)
		c.list.Remove(ele)
	}

	ele := c.list.PushFront(&node{key, val, expireAt})
	c.hashmap[key] = ele
	return true
}

func (c *cache) get(key string) (*node, bool) {
	if ele, exists := c.hashmap[key]; exists {
		node := ele.Value.(*node)
		c.list.MoveToFront(ele)
		return node, true
	}
	return nil, false
}

func (c *cache) del(key string) (*node, bool) {
	if ele, exists := c.hashmap[key]; exists {
		node := ele.Value.(*node)
		delete(c.hashmap, node.key)
		c.list.Remove(ele)
		return node, true
	}
	return nil, false
}

func (c *cache) clear() {
	c.list.Init()
	for k := range c.hashmap {
		delete(c.hashmap, k)
	}
}

func (c *cache) cleanup(time time.Time) {
	var expired []*list.Element
	for ele := c.list.Front(); ele != nil; ele = ele.Next() {
		node := ele.Value.(*node)
		if node.expireAt != nil && node.expireAt.Before(time) {
			delete(c.hashmap, node.key)
			expired = append(expired, ele)
		}
	}
	for _, ele := range expired {
		c.list.Remove(ele)
	}
}

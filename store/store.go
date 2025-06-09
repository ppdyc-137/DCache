package store

import "time"

type Value interface {
	Len() int
}

type Store interface {
	Get(key string) (Value, bool)
	Set(key string, val Value)
	SetWithExpiration(key string, val Value, expiration time.Duration)
	Delete(key string) bool
	Len() int
	Clear()
	Close()
}

type CacheType string

const (
	LRU  CacheType = "lru"
	LRU2 CacheType = "lru2"
)

type Options struct {
	MaxBytes        int64
	CleanupInterval time.Duration
	BucketCount     uint16
	CapPerBucket    int
	Expiration      time.Duration
}

func NewStore(cacheType CacheType, opts Options) Store {
	switch cacheType {
	case LRU:
		return newLRUCache(opts)
	case LRU2:
		return newLRU2Cache(opts)
	default:
		return newLRUCache(opts)
	}
}

package dcache

import (
	"DCache/singleflight"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	groupsMutex sync.RWMutex
	groups      = make(map[string]*Group)
)

var (
	ErrKeyRequired   = errors.New("key is required")
	ErrValueRequired = errors.New("value is required")
	ErrGroupClosed   = errors.New("cache group is closed")
)

type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

type GetterFunc func(ctx context.Context, key string) ([]byte, error)

func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

type Group struct {
	name       string
	source     Getter
	loaclCache *Cache
	peerPicker *PeerPicker
	loader     *singleflight.Group
	expiration time.Duration
	closed     atomic.Bool
	stats      GroupStats
}

type GroupStats struct {
	loads        atomic.Int64
	localHits    atomic.Int64
	localMisses  atomic.Int64
	peerHits     atomic.Int64
	peerMisses   atomic.Int64
	loaderHits   atomic.Int64
	loaderErrors atomic.Int64
	loadDuration atomic.Int64
}

func NewGroup(name string, source Getter, peerPicker *PeerPicker) *Group {
	g := &Group{
		name:       name,
		source:     source,
		loaclCache: NewCache(DefaultCacheOptions),
		loader:     &singleflight.Group{},
		peerPicker: peerPicker,
	}

	groupsMutex.Lock()
	defer groupsMutex.Unlock()

	if _, exists := groups[name]; exists {
		logrus.Warnf("Group with name %s already exists, will be replaced", name)
	}

	groups[name] = g
	return g
}

func GetGroup(name string) *Group {
	groupsMutex.RLock()
	defer groupsMutex.RUnlock()
	return groups[name]
}

func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	if g.closed.Load() {
		return ByteView{}, ErrGroupClosed
	}

	if key == "" {
		return ByteView{}, ErrKeyRequired
	}

	view, ok := g.loaclCache.Get(ctx, key)
	if ok {
		g.stats.localHits.Add(1)
		return view, nil
	}

	return g.load(ctx, key)
}

func (g *Group) Set(ctx context.Context, key string, val []byte) error {
	if g.closed.Load() {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}
	if len(val) == 0 {
		return ErrValueRequired
	}

	view := ByteView{CloneBytes(val)}
	if g.expiration > 0 {
		g.loaclCache.SetWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.loaclCache.Set(key, view)
	}

	isPeerRequest := ctx.Value("from_peer") != nil
	if !isPeerRequest && g.peerPicker != nil {
		go g.syncToPeers(ctx, "set", key, val)
	}

	return nil
}

func (g *Group) Delete(ctx context.Context, key string) error {
	if g.closed.Load() {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}

	g.loaclCache.Delete(key)

	isPeerRequest := ctx.Value("from_peer") != nil
	if !isPeerRequest && g.peerPicker != nil {
		go g.syncToPeers(ctx, "delete", key, nil)
	}

	return nil
}

func (g *Group) Clear() {
	if g.closed.Load() {
		return
	}

	g.loaclCache.Clear()
}

func (g *Group) Close() {
	if !g.closed.CompareAndSwap(false, true) {
		return
	}

	if g.loaclCache != nil {
		g.loaclCache.Close()
	}

	groupsMutex.Lock()
	delete(groups, g.name)
	groupsMutex.Unlock()
}

func (g *Group) close() {
	if !g.closed.CompareAndSwap(false, true) {
		return
	}

	if g.loaclCache != nil {
		g.loaclCache.Close()
	}
}

func ListGroups() []string {
	groupsMutex.RLock()
	defer groupsMutex.RUnlock()

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}

	return names
}

func DestroyGroup(name string) bool {
	groupsMutex.Lock()
	defer groupsMutex.Unlock()

	if g, exists := groups[name]; exists {
		g.close()
		delete(groups, name)
		return true
	}

	return false
}

func DestroyAllGroups() {
	groupsMutex.Lock()
	defer groupsMutex.Unlock()

	for name, g := range groups {
		g.close()
		delete(groups, name)
	}
}

func (g *Group) load(ctx context.Context, key string) (ByteView, error) {
	startTime := time.Now()
	val, err := g.loader.Do(key, func() (any, error) {
		return g.loadData(ctx, key)
	})

	loadDuration := time.Since(startTime).Nanoseconds()
	g.stats.loadDuration.Add(loadDuration)
	g.stats.loads.Add(1)

	if err != nil {
		g.stats.loaderErrors.Add(1)
		return ByteView{}, nil
	}

	view := val.(ByteView)
	// if g.expiration > 0 {
	// 	g.loaclCache.SetWithExpiration(key, view, time.Now().Add(g.expiration))
	// } else {
	// 	g.loaclCache.Set(key, view)
	// }
	return view, nil
}

func (g *Group) loadData(ctx context.Context, key string) (ByteView, error) {
	// load data from peer
	if g.peerPicker != nil {
		peer, ok, self := g.peerPicker.PickPeer(key)
		if ok && !self {
			value, err := g.loadDataFromPeer(ctx, peer, key)
			if err == nil {
				g.stats.peerHits.Add(1)
				return value, nil
			}

			g.stats.peerMisses.Add(1)
		}
	}

	// load data from source
	value, err := g.source.Get(ctx, key)
	if err != nil {
		g.stats.loaderErrors.Add(1)
		return ByteView{}, err
	}

	g.stats.loaderHits.Add(1)
	return ByteView{CloneBytes(value)}, nil
}

func (g *Group) loadDataFromPeer(ctx context.Context, peer *Peer, key string) (ByteView, error) {
	value, err := peer.Get(g.name, key)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{CloneBytes(value)}, nil
}

func (g *Group) syncToPeers(ctx context.Context, op string, key string, val []byte) {
	if g.peerPicker == nil {
		return
	}

	peer, ok, self := g.peerPicker.PickPeer(key)
	if !ok || self {
		return
	}

	syncCtx := context.WithValue(context.Background(), "from_peer", true)

	switch op {
	case "set":
		peer.Set(syncCtx, g.name, key, val)
	case "delete":
		peer.Delete(g.name, key)
	}
}

func (g *Group) Stats() map[string]any {
	stats := map[string]any{
		"name":          g.name,
		"closed":        g.closed.Load(),
		"expiration":    g.expiration,
		"loads":         g.stats.loads.Load(),
		"local_hits":    g.stats.localHits.Load(),
		"local_misses":  g.stats.localMisses.Load(),
		"peer_hits":     g.stats.peerHits.Load(),
		"peer_misses":   g.stats.peerMisses.Load(),
		"loader_hits":   g.stats.loaderHits.Load(),
		"loader_errors": g.stats.loaderErrors.Load(),
	}

	// 计算各种命中率
	totalGets := stats["local_hits"].(int64) + stats["local_misses"].(int64)
	if totalGets > 0 {
		stats["hit_rate"] = float64(stats["local_hits"].(int64)) / float64(totalGets)
	}

	totalLoads := stats["loads"].(int64)
	if totalLoads > 0 {
		stats["avg_load_time_ms"] = float64(g.stats.loadDuration.Load()) / float64(totalLoads) / float64(time.Millisecond)
	}

	// 添加缓存大小
	if g.loaclCache != nil {
		cacheStats := g.loaclCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}

	return stats
}

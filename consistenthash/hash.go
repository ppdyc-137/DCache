package consistenthash

import (
	"fmt"
	"hash/crc32"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
)

type Map struct {
	mutex         sync.Mutex
	config        Config
	keys          []int
	hashMap       map[int]string
	nodeReplicas  map[string]int
	nodeCounts    map[string]int64
	totalRequests atomic.Int64
}

type Config struct {
	DefaultReplicas int
	HashFunc        func([]byte) uint32
}

var DefaultConfig = Config{
	DefaultReplicas: 50,
	HashFunc:        crc32.ChecksumIEEE,
}

func New() *Map {
	return &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]int64),
	}
}

func (m *Map) Add(nodes ...string) {
	if len(nodes) == 0 {
		return
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, node := range nodes {
		m.addNode(node, m.config.DefaultReplicas)
	}
	sort.Ints(m.keys)
}

func (m *Map) Remove(node string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	replicas, ok := m.nodeReplicas[node]
	if !ok {
		return fmt.Errorf("node %s not found", node)
	}

	for i := range replicas {
		hash := int(m.config.HashFunc(fmt.Appendf(nil, "%s-%d", node, i)))
		delete(m.hashMap, hash)
		for j := range m.keys {
			if m.keys[j] == hash {
				m.keys = slices.Delete(m.keys, j, j+1)
				break
			}
		}
	}
	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node)
	return nil
}

func (m *Map) Get(key string) (string, bool) {
	if len(key) == 0 {
		return "", false
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.keys) == 0 {
		return "", false
	}

	hash := int(m.config.HashFunc([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if idx == len(m.keys) {
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]

	m.nodeCounts[node]++
	m.totalRequests.Add(1)
	return node, true
}

func (m *Map) addNode(node string, replicas int) {
	for i := range replicas {
		hash := int(m.config.HashFunc(fmt.Appendf(nil, "%s-%d", node, i)))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] = replicas
}

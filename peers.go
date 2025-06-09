package dcache

import (
	"DCache/consistenthash"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type PeerPicker struct {
	addr        string
	serviceName string
	mutex       sync.RWMutex
	consHash    *consistenthash.Map
	peers       map[string]*Peer
	etcdClient  *clientv3.Client
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewPeerPicker(addr string, serviceName string) (*PeerPicker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	picker := &PeerPicker{
		addr:        addr,
		serviceName: serviceName,
		peers:       make(map[string]*Peer),
		consHash:    consistenthash.New(),
		ctx:         ctx,
		cancel:      cancel,
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   Endpoints,
		DialTimeout: DialTimeout,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}
	picker.etcdClient = cli

	if err := picker.startServiceDiscovery(); err != nil {
		cancel()
		cli.Close()
		return nil, err
	}
	return picker, nil
}

func (p *PeerPicker) startServiceDiscovery() error {
	if err := p.fetchAllServices(); err != nil {
		return err
	}
	go p.watchServiceChanges()
	return nil
}

func (p *PeerPicker) watchServiceChanges() {
	watcher := clientv3.NewWatcher(p.etcdClient)
	watchCh := watcher.Watch(p.ctx, "/services/"+p.serviceName, clientv3.WithPrefix())
	for {
		select {
		case <-p.ctx.Done():
			watcher.Close()
			return
		case resp := <-watchCh:
			p.handleWatchEvents(resp.Events)
		}
	}
}

func (p *PeerPicker) handleWatchEvents(events []*clientv3.Event) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, event := range events {
		logrus.Infof("Receive event: %v", event)

		splits := strings.Split(string(event.Kv.Key), "/")
		addr := splits[len(splits)-1]

		// if addr == p.addr {
		// 	continue
		// }

		switch event.Type {
		case clientv3.EventTypePut:
			if _, exists := p.peers[addr]; !exists {
				p.add(addr)
				logrus.Infof("New service Discovered at %s", addr)
			}
		case clientv3.EventTypeDelete:
			if client, exists := p.peers[addr]; exists {
				client.Close()
				p.remove(addr)
				logrus.Infof("Service removed at %s", addr)
			}
		}
	}
}

func (p *PeerPicker) fetchAllServices() error {
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()

	resp, err := p.etcdClient.Get(ctx, "/services/"+p.serviceName, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to fetch services: %v", err)
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, kv := range resp.Kvs {
		addr := string(kv.Value)
		if addr != "" && addr != p.addr {
			p.add(addr)
			logrus.Infof("Discover service at %s", addr)
		}
	}
	return nil
}

func (p *PeerPicker) add(addr string) {
	if client, err := NewPeer(addr, p.serviceName); err == nil {
		p.consHash.Add(addr)
		p.peers[addr] = client
		logrus.Infof("add client for %s", addr)
	} else {
		logrus.Errorf("failed to add client for %s: %v", addr, err)
	}
}

func (p *PeerPicker) remove(addr string) {
	p.consHash.Remove(addr)
	delete(p.peers, addr)
}

func (p *PeerPicker) PickPeer(key string) (*Peer, bool, bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if addr, ok := p.consHash.Get(key); ok {
		if client, ok := p.peers[addr]; ok {
			return client, true, addr == p.addr
		}

	}
	return nil, false, false
}

func (p *PeerPicker) Close() error {
	p.cancel()
	p.mutex.Lock()
	defer p.mutex.Unlock()

	var errs []error
	for addr, peer := range p.peers {
		if err := peer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close %s: %v", addr, err))
		}
	}
	if err := p.etcdClient.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close etcd client: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("erros while closing: %v", errs)
	}
	return nil
}

func (p *PeerPicker) PrintPeers() {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	logrus.Infof("peers:")
	for addr, cli := range p.peers {
		logrus.Infof("- %s, %v", addr, cli)
	}
}

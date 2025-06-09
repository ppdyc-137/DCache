package registry

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	Endpoints   = []string{"localhost:2379"}
	DialTimeout = 5 * time.Second
)

func Register(serviceName, addr string, stopCh <-chan error) error {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   Endpoints,
		DialTimeout: DialTimeout,
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %v", err)
	}

	lease, err := client.Grant(context.Background(), 10)
	if err != nil {
		client.Close()
		return fmt.Errorf("failed to create lease: %v", err)
	}

	key := fmt.Sprintf("/services/%s/%s", serviceName, addr)
	_, err = client.Put(context.Background(), key, addr, clientv3.WithLease(lease.ID))
	if err != nil {
		client.Close()
		return fmt.Errorf("failed to put key-value to etcd: %v", err)
	}

	keepAliveCh, err := client.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		client.Close()
		return fmt.Errorf("failed to keep lease alive: %v", err)
	}

	go func() {
		defer client.Close()
		for {
			select {
			case <-stopCh:
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				client.Revoke(ctx, lease.ID)
				cancel()
				return
			case _, ok := <-keepAliveCh:
				if !ok {
					logrus.Warnf("keep alive channel close")
					return
				}
				// logrus.Infof("renewed lease: %d", resp.ID)
			}
		}
	}()
	logrus.Infof("Service registered: %s at %s", serviceName, addr)
	return nil
}

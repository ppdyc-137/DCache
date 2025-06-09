package dcache

import (
	pb "DCache/pb"
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	addr        string
	serviceName string
	grpcConn    *grpc.ClientConn
	grpcClient  pb.DCacheClient
}

func NewPeer(addr string, serviceName string) (*Peer, error) {
	var err error

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	if err != nil {
		return nil, fmt.Errorf("failed to dial server: %v", err)
	}

	grpcClient := pb.NewDCacheClient(conn)

	client := &Peer{
		addr:        addr,
		serviceName: serviceName,
		grpcConn:    conn,
		grpcClient:  grpcClient,
	}

	return client, nil
}

func (c *Peer) Get(group, key string) ([]byte, error) {
	logrus.Infof("Client: Get %s", key)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := c.grpcClient.Get(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get value from %s: %v", c.addr, err)
	}

	return resp.GetValue(), nil
}

func (c *Peer) Delete(group, key string) (bool, error) {
	logrus.Infof("Client: Del %s", key)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := c.grpcClient.Delete(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return false, fmt.Errorf("failed to del value to %s: %v", c.addr, err)
	}

	return true, nil
}

func (c *Peer) Set(ctx context.Context, group, key string, value []byte) error {
	logrus.Infof("Client: Set %s %s", key, string(value))
	_, err := c.grpcClient.Set(ctx, &pb.Request{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to set value to %s: %v", c.addr, err)
	}
	return nil
}

func (c *Peer) Close() error {
	if c.grpcConn != nil {
		return c.grpcConn.Close()
	}
	return nil
}

func (c *Peer) Addr() string {
	return c.addr
}

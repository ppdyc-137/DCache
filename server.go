package dcache

import (
	pb "DCache/pb"
	"DCache/registry"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pb.UnimplementedDCacheServer
	addr        string
	serviceName string
	grpcServer  *grpc.Server
	etcdClient  *clientv3.Client
	stopCh      chan error
}

var (
	Endpoints   = []string{"localhost:2379"}
	DialTimeout = 5 * time.Second
)

func NewServer(addr string, serviceName string) (*Server, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   Endpoints,
		DialTimeout: DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	grpcServer := grpc.NewServer()
	service := &Server{
		addr:        addr,
		serviceName: serviceName,
		grpcServer:  grpcServer,
		etcdClient:  etcdClient,
		stopCh:      make(chan error),
	}
	pb.RegisterDCacheServer(grpcServer, service)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(service.grpcServer, healthServer)
	healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)

	return service, nil
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen at: %v", err)
	}

	if err := registry.Register(s.serviceName, s.addr, s.stopCh); err != nil {
		logrus.Errorf("failed to register service: %v", err)
		return err
	}

	logrus.Infof("Server starting at %s", s.addr)
	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	close(s.stopCh)
	s.grpcServer.GracefulStop()
}

func (s *Server) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	logrus.Infof("Server: Get %s", req.Key)
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	value, err := group.Get(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: value.Clone()}, nil
}

func (s *Server) Set(ctx context.Context, req *pb.Request) (*emptypb.Empty, error) {
	logrus.Infof("Server: Set %s %s", req.Key, req.Value)
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	fromPeer := ctx.Value("from_peer")
	if fromPeer == nil {
		ctx = context.WithValue(ctx, "from_peer", true)
	}

	if err := group.Set(ctx, req.Key, req.Value); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.Request) (*emptypb.Empty, error) {
	logrus.Infof("Server: Del %s", req.Key)
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	if err := group.Delete(ctx, req.Key); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

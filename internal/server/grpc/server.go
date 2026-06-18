package grpcserver

import (
	"context"
	"errors"

	kv "kv_store_demo"
	kvv1 "kv_store_demo/gen/kv/v1"
	raftnode "kv_store_demo/internal/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	kvv1.UnimplementedKVServiceServer

	store Store
}

func NewServer(store Store) *Server {
	return &Server{store: store}
}

func Register(registrar grpc.ServiceRegistrar, store Store) {
	kvv1.RegisterKVServiceServer(registrar, NewServer(store))
}

func mapError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, kv.ErrEmptyKey):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, kv.ErrDbClosed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, raftnode.ErrNotLeader):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s *Server) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	if err := s.store.Put(ctx, req.GetKey(), req.GetValue()); err != nil {
		return nil, mapError(err)
	}

	return &kvv1.PutResponse{}, nil
}

func (s *Server) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	value, err := s.store.Get(ctx, req.GetKey())
	if err != nil {
		return nil, mapError(err)
	}

	return &kvv1.GetResponse{Value: value}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	if err := s.store.Delete(ctx, req.GetKey()); err != nil {
		return nil, mapError(err)
	}

	return &kvv1.DeleteResponse{}, nil
}

func (s *Server) Flush(ctx context.Context, req *kvv1.FlushRequest) (*kvv1.FlushResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	if err := s.store.Flush(ctx); err != nil {
		return nil, mapError(err)
	}

	return &kvv1.FlushResponse{}, nil
}

func (s *Server) Compact(ctx context.Context, req *kvv1.CompactRequest) (*kvv1.CompactResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	if err := s.store.Compact(ctx); err != nil {
		return nil, mapError(err)
	}

	return &kvv1.CompactResponse{}, nil
}

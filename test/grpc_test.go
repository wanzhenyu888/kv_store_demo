package test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	kv "kv_store_demo"
	"kv_store_demo/internal/client"
	grpcserver "kv_store_demo/internal/server/grpc"

	"google.golang.org/grpc"
)

type grpcTestServer struct {
	db      *kv.DB
	server  *grpc.Server
	serveCh chan error
	addr    string
}

func startGrpcTestServer(t *testing.T, dir string) *grpcTestServer {
	t.Helper()

	db, err := kv.Open(kv.Options{
		Dir:          dir,
		MemTableSize: kv.DefaultMemTableSize,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = db.Close()
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("skip real TCP gRPC test because local listen is not permitted: %v", err)
		}
		t.Fatalf("Listen() error = %v", err)
	}

	server := grpc.NewServer()
	grpcserver.Register(server, grpcserver.NewStandaloneStore(db))
	serveCh := make(chan error, 1)
	go func() {
		serveCh <- server.Serve(listener)
	}()

	return &grpcTestServer{
		db:      db,
		server:  server,
		serveCh: serveCh,
		addr:    listener.Addr().String(),
	}
}

func (s *grpcTestServer) stop(t *testing.T) {
	t.Helper()

	s.server.GracefulStop()
	select {
	case err := <-s.serveCh:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for gRPC server to stop")
	}

	if err := s.db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func dialGrpcTestClient(t *testing.T, addr string) *client.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c, err := client.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	return c
}

func TestGRPCPutGetDelete(t *testing.T) {
	server := startGrpcTestServer(t, t.TempDir())
	defer server.stop(t)

	c := dialGrpcTestClient(t, server.addr)
	defer c.Close()

	ctx := context.Background()
	if err := c.Put(ctx, []byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	got, err := c.Get(ctx, []byte("name"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(got, []byte("alice")) {
		t.Fatalf("Get() = %q, want %q", got, "alice")
	}

	if err := c.Delete(ctx, []byte("name")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	got, err = c.Get(ctx, []byte("name"))
	if err != nil {
		t.Fatalf("Get() after Delete error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get() after Delete = %q, want empty value", got)
	}
}

func TestGRPCFlushAndRestartRestoresData(t *testing.T) {
	dir := t.TempDir()

	server := startGrpcTestServer(t, dir)
	c := dialGrpcTestClient(t, server.addr)
	ctx := context.Background()

	if err := c.Put(ctx, []byte("city"), []byte("hangzhou")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("client Close() error = %v", err)
	}
	server.stop(t)

	server = startGrpcTestServer(t, dir)
	defer server.stop(t)

	c = dialGrpcTestClient(t, server.addr)
	defer c.Close()

	got, err := c.Get(ctx, []byte("city"))
	if err != nil {
		t.Fatalf("Get() after restart error = %v", err)
	}
	if !bytes.Equal(got, []byte("hangzhou")) {
		t.Fatalf("Get() after restart = %q, want %q", got, "hangzhou")
	}
}

func TestGRPCCompactKeepsLatestValue(t *testing.T) {
	server := startGrpcTestServer(t, t.TempDir())
	defer server.stop(t)

	c := dialGrpcTestClient(t, server.addr)
	defer c.Close()

	ctx := context.Background()
	if err := c.Put(ctx, []byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put(alice) error = %v", err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush() after alice error = %v", err)
	}
	if err := c.Put(ctx, []byte("name"), []byte("bob")); err != nil {
		t.Fatalf("Put(bob) error = %v", err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush() after bob error = %v", err)
	}
	if err := c.Compact(ctx); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	got, err := c.Get(ctx, []byte("name"))
	if err != nil {
		t.Fatalf("Get() after Compact error = %v", err)
	}
	if !bytes.Equal(got, []byte("bob")) {
		t.Fatalf("Get() after Compact = %q, want %q", got, "bob")
	}
}

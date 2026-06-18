package client

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"

	kv "kv_store_demo"
	raftnode "kv_store_demo/internal/raft"
	grpcserver "kv_store_demo/internal/server/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func newBufconnClient(t *testing.T) (*Client, func()) {
	t.Helper()

	db, err := kv.Open(kv.Options{
		Dir:          t.TempDir(),
		MemTableSize: kv.DefaultMemTableSize,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	listener := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	grpcserver.Register(grpcServer, grpcserver.NewStandaloneStore(db))

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		_ = db.Close()
		t.Fatalf("NewClient() error = %v", err)
	}

	c := newClient(conn)
	cleanup := func() {
		_ = c.Close()
		grpcServer.Stop()
		_ = db.Close()
		select {
		case err := <-serveErr:
			if err != nil {
				t.Logf("Serve() stopped: %v", err)
			}
		default:
		}
	}

	return c, cleanup
}

type failingStore struct {
	err error
}

func (s failingStore) Put(ctx context.Context, key, value []byte) error {
	return s.err
}

func (s failingStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	return nil, s.err
}

func (s failingStore) Delete(ctx context.Context, key []byte) error {
	return s.err
}

func (s failingStore) Flush(ctx context.Context) error {
	return s.err
}

func (s failingStore) Compact(ctx context.Context) error {
	return s.err
}

func newBufconnClientWithStore(t *testing.T, store grpcserver.Store) (*Client, func()) {
	t.Helper()

	listener := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	grpcserver.Register(grpcServer, store)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		t.Fatalf("NewClient() error = %v", err)
	}

	c := newClient(conn)
	cleanup := func() {
		_ = c.Close()
		grpcServer.Stop()
		select {
		case err := <-serveErr:
			if err != nil {
				t.Logf("Serve() stopped: %v", err)
			}
		default:
		}
	}

	return c, cleanup
}

func TestClientPutGet(t *testing.T) {
	c, cleanup := newBufconnClient(t)
	defer cleanup()

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
}

func TestClientDeleteThenGetReturnsEmptyValue(t *testing.T) {
	c, cleanup := newBufconnClient(t)
	defer cleanup()

	ctx := context.Background()
	if err := c.Put(ctx, []byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := c.Delete(ctx, []byte("name")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got, err := c.Get(ctx, []byte("name"))
	if err != nil {
		t.Fatalf("Get() after Delete error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get() after Delete = %q, want empty value", got)
	}
}

func TestClientFlushAndCompact(t *testing.T) {
	c, cleanup := newBufconnClient(t)
	defer cleanup()

	ctx := context.Background()
	if err := c.Put(ctx, []byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := c.Compact(ctx); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	got, err := c.Get(ctx, []byte("name"))
	if err != nil {
		t.Fatalf("Get() after Compact error = %v", err)
	}
	if !bytes.Equal(got, []byte("alice")) {
		t.Fatalf("Get() after Compact = %q, want %q", got, "alice")
	}
}

func TestClientEmptyKeyReturnsInvalidArgument(t *testing.T) {
	c, cleanup := newBufconnClient(t)
	defer cleanup()

	err := c.Put(context.Background(), nil, []byte("alice"))
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("Put(empty key) code = %v, want %v, err = %v", got, codes.InvalidArgument, err)
	}
}

func TestMapErrorConvertsNotLeader(t *testing.T) {
	err := status.Error(codes.Unavailable, `raft node is not leader: leader id="n1" addr="127.0.0.1:10001"`)

	mapped := mapError(err)
	if !errors.Is(mapped, ErrNotLeader) {
		t.Fatalf("mapError() = %v, want ErrNotLeader", mapped)
	}
	if !IsNotLeader(mapped) {
		t.Fatalf("IsNotLeader(%v) = false, want true", mapped)
	}

	leaderID, leaderAddr, ok := LeaderHint(mapped)
	if !ok {
		t.Fatalf("LeaderHint() ok = false, want true")
	}
	if leaderID != "n1" || leaderAddr != "127.0.0.1:10001" {
		t.Fatalf("LeaderHint() = %q %q, want n1 127.0.0.1:10001", leaderID, leaderAddr)
	}
}

func TestMapErrorKeepsOrdinaryUnavailable(t *testing.T) {
	err := status.Error(codes.Unavailable, "connection unavailable")

	mapped := mapError(err)
	if errors.Is(mapped, ErrNotLeader) {
		t.Fatalf("mapError() = %v, should not be ErrNotLeader", mapped)
	}
	if status.Code(mapped) != codes.Unavailable {
		t.Fatalf("status.Code(mapError()) = %v, want %v", status.Code(mapped), codes.Unavailable)
	}
}

func TestClientMethodsConvertNotLeader(t *testing.T) {
	c, cleanup := newBufconnClientWithStore(t, failingStore{err: &raftnode.NotLeaderError{
		LeaderID:   "n1",
		LeaderAddr: "127.0.0.1:10001",
	}})
	defer cleanup()

	ctx := context.Background()
	tests := []struct {
		name string
		fn   func() error
	}{
		{name: "put", fn: func() error { return c.Put(ctx, []byte("name"), []byte("alice")) }},
		{name: "get", fn: func() error {
			_, err := c.Get(ctx, []byte("name"))
			return err
		}},
		{name: "delete", fn: func() error { return c.Delete(ctx, []byte("name")) }},
		{name: "flush", fn: func() error { return c.Flush(ctx) }},
		{name: "compact", fn: func() error { return c.Compact(ctx) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if !errors.Is(err, ErrNotLeader) {
				t.Fatalf("method error = %v, want ErrNotLeader", err)
			}
			leaderID, leaderAddr, ok := LeaderHint(err)
			if !ok {
				t.Fatalf("LeaderHint() ok = false, want true")
			}
			if leaderID != "n1" || leaderAddr != "127.0.0.1:10001" {
				t.Fatalf("LeaderHint() = %q %q, want n1 127.0.0.1:10001", leaderID, leaderAddr)
			}
		})
	}
}

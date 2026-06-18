package grpcserver

import (
	"bytes"
	"context"
	"testing"

	kv "kv_store_demo"
	kvv1 "kv_store_demo/gen/kv/v1"
	raftnode "kv_store_demo/internal/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func openTestServer(t *testing.T) (*Server, *kv.DB) {
	t.Helper()

	db, err := kv.Open(kv.Options{
		Dir:          t.TempDir(),
		MemTableSize: kv.DefaultMemTableSize,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	return NewServer(NewStandaloneStore(db)), db
}

func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()

	if got := status.Code(err); got != want {
		t.Fatalf("status.Code(err) = %v, want %v, err = %v", got, want, err)
	}
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

func TestServerPutGet(t *testing.T) {
	srv, db := openTestServer(t)
	defer db.Close()

	_, err := srv.Put(context.Background(), &kvv1.PutRequest{
		Key:   []byte("name"),
		Value: []byte("alice"),
	})
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	resp, err := srv.Get(context.Background(), &kvv1.GetRequest{
		Key: []byte("name"),
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(resp.GetValue(), []byte("alice")) {
		t.Fatalf("Get() value = %q, want %q", resp.GetValue(), "alice")
	}
}

func TestServerRejectsEmptyKey(t *testing.T) {
	srv, db := openTestServer(t)
	defer db.Close()

	if _, err := srv.Put(context.Background(), &kvv1.PutRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Put(empty key) code = %v, want %v, err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if _, err := srv.Get(context.Background(), &kvv1.GetRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Get(empty key) code = %v, want %v, err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if _, err := srv.Delete(context.Background(), &kvv1.DeleteRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Delete(empty key) code = %v, want %v, err = %v", status.Code(err), codes.InvalidArgument, err)
	}
}

func TestServerDeleteThenGetReturnsEmptyValue(t *testing.T) {
	srv, db := openTestServer(t)
	defer db.Close()

	if _, err := srv.Put(context.Background(), &kvv1.PutRequest{
		Key:   []byte("name"),
		Value: []byte("alice"),
	}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if _, err := srv.Delete(context.Background(), &kvv1.DeleteRequest{
		Key: []byte("name"),
	}); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	resp, err := srv.Get(context.Background(), &kvv1.GetRequest{
		Key: []byte("name"),
	})
	if err != nil {
		t.Fatalf("Get() after Delete error = %v", err)
	}
	if len(resp.GetValue()) != 0 {
		t.Fatalf("Get() after Delete value = %q, want empty value", resp.GetValue())
	}
}

func TestServerFlushAndCompact(t *testing.T) {
	srv, db := openTestServer(t)
	defer db.Close()

	if _, err := srv.Put(context.Background(), &kvv1.PutRequest{
		Key:   []byte("name"),
		Value: []byte("alice"),
	}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if _, err := srv.Flush(context.Background(), &kvv1.FlushRequest{}); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if _, err := srv.Compact(context.Background(), &kvv1.CompactRequest{}); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	resp, err := srv.Get(context.Background(), &kvv1.GetRequest{
		Key: []byte("name"),
	})
	if err != nil {
		t.Fatalf("Get() after Compact error = %v", err)
	}
	if !bytes.Equal(resp.GetValue(), []byte("alice")) {
		t.Fatalf("Get() after Compact value = %q, want %q", resp.GetValue(), "alice")
	}
}

func TestServerRejectsOperationsAfterClose(t *testing.T) {
	srv, db := openTestServer(t)
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err := srv.Put(context.Background(), &kvv1.PutRequest{
		Key:   []byte("name"),
		Value: []byte("alice"),
	})
	assertCode(t, err, codes.FailedPrecondition)

	_, err = srv.Flush(context.Background(), &kvv1.FlushRequest{})
	assertCode(t, err, codes.FailedPrecondition)
}

func TestServerMapsNotLeaderToUnavailable(t *testing.T) {
	srv := NewServer(failingStore{err: &raftnode.NotLeaderError{
		LeaderID:   "n1",
		LeaderAddr: "127.0.0.1:10001",
	}})

	_, err := srv.Put(context.Background(), &kvv1.PutRequest{
		Key:   []byte("name"),
		Value: []byte("alice"),
	})
	assertCode(t, err, codes.Unavailable)
}

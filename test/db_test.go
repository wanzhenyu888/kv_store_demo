package test

import (
	"errors"
	"testing"

	kv "kv_store_demo"
)

func TestOpenReturnsDB(t *testing.T) {
	db, err := kv.Open(kv.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if db == nil {
		t.Fatal("Open() returned nil DB")
	}
}

func TestSkeletonMethodsReturnNotImplemented(t *testing.T) {
	db, err := kv.Open(kv.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := db.Put([]byte("name"), []byte("alice")); !errors.Is(err, kv.ErrNotImplemented) {
		t.Fatalf("Put() error = %v, want ErrNotImplemented", err)
	}

	if _, err := db.Get([]byte("name")); !errors.Is(err, kv.ErrNotImplemented) {
		t.Fatalf("Get() error = %v, want ErrNotImplemented", err)
	}

	if err := db.Delete([]byte("name")); !errors.Is(err, kv.ErrNotImplemented) {
		t.Fatalf("Delete() error = %v, want ErrNotImplemented", err)
	}

	if err := db.Flush(); !errors.Is(err, kv.ErrNotImplemented) {
		t.Fatalf("Flush() error = %v, want ErrNotImplemented", err)
	}

	if err := db.Compact(); !errors.Is(err, kv.ErrNotImplemented) {
		t.Fatalf("Compact() error = %v, want ErrNotImplemented", err)
	}
}

func TestClosedDBReturnsErrClosed(t *testing.T) {
	db, err := kv.Open(kv.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := db.Put([]byte("name"), []byte("alice")); !errors.Is(err, kv.ErrClosed) {
		t.Fatalf("Put() after Close error = %v, want ErrClosed", err)
	}

	if _, err := db.Get([]byte("name")); !errors.Is(err, kv.ErrClosed) {
		t.Fatalf("Get() after Close error = %v, want ErrClosed", err)
	}
}

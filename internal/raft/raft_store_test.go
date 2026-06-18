package raftnode

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	kv "kv_store_demo"
)

type fakeRaftNode struct {
	leader     bool
	leaderID   string
	leaderAddr string

	appliedData    []byte
	appliedTimeout time.Duration
	barrierTimeout time.Duration
	applyCalled    bool
	barrierCalled  bool
	applyErr       error
	barrierErr     error
	operations     []string
}

func (n *fakeRaftNode) IsLeader() bool {
	return n.leader
}

func (n *fakeRaftNode) Leader() (string, string) {
	return n.leaderID, n.leaderAddr
}

func (n *fakeRaftNode) Apply(data []byte, timeout time.Duration) error {
	n.applyCalled = true
	n.appliedData = append([]byte(nil), data...)
	n.appliedTimeout = timeout
	n.operations = append(n.operations, "apply")
	return n.applyErr
}

func (n *fakeRaftNode) Barrier(timeout time.Duration) error {
	n.barrierCalled = true
	n.barrierTimeout = timeout
	n.operations = append(n.operations, "barrier")
	return n.barrierErr
}

func openRaftStoreTestDB(t *testing.T) *kv.DB {
	t.Helper()

	db, err := kv.Open(kv.Options{
		Dir:          t.TempDir(),
		MemTableSize: kv.DefaultMemTableSize,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return db
}

func TestRaftStorePutAppliesCommand(t *testing.T) {
	db := openRaftStoreTestDB(t)
	defer db.Close()

	node := &fakeRaftNode{leader: true}
	store := newRaftStore(node, db, RaftStoreConfig{})

	if err := store.Put(context.Background(), []byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if !node.applyCalled {
		t.Fatal("Put() did not call Apply")
	}
	if node.appliedTimeout != defaultApplyTimeout {
		t.Fatalf("Apply timeout = %s, want %s", node.appliedTimeout, defaultApplyTimeout)
	}

	cmd, err := DecodeCommand(node.appliedData)
	if err != nil {
		t.Fatalf("DecodeCommand() error = %v", err)
	}
	if cmd.Type != CommandTypePut || !bytes.Equal(cmd.Key, []byte("name")) || !bytes.Equal(cmd.Value, []byte("alice")) {
		t.Fatalf("applied command = %+v, want put name=alice", cmd)
	}
}

func TestRaftStoreDeleteAppliesCommand(t *testing.T) {
	db := openRaftStoreTestDB(t)
	defer db.Close()

	node := &fakeRaftNode{leader: true}
	store := newRaftStore(node, db, RaftStoreConfig{})

	if err := store.Delete(context.Background(), []byte("name")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	cmd, err := DecodeCommand(node.appliedData)
	if err != nil {
		t.Fatalf("DecodeCommand() error = %v", err)
	}
	if cmd.Type != CommandTypeDelete || !bytes.Equal(cmd.Key, []byte("name")) || len(cmd.Value) != 0 {
		t.Fatalf("applied command = %+v, want delete name", cmd)
	}
}

func TestRaftStoreGetBarriersBeforeRead(t *testing.T) {
	db := openRaftStoreTestDB(t)
	defer db.Close()

	if err := db.Put([]byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() seed error = %v", err)
	}

	node := &fakeRaftNode{leader: true}
	store := newRaftStore(node, db, RaftStoreConfig{})

	got, err := store.Get(context.Background(), []byte("name"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(got, []byte("alice")) {
		t.Fatalf("Get() = %q, want %q", got, "alice")
	}
	if !node.barrierCalled {
		t.Fatal("Get() did not call Barrier")
	}
	if node.barrierTimeout != defaultBarrierTimeout {
		t.Fatalf("Barrier timeout = %s, want %s", node.barrierTimeout, defaultBarrierTimeout)
	}
}

func TestRaftStoreFollowerRejectsOperations(t *testing.T) {
	db := openRaftStoreTestDB(t)
	defer db.Close()

	node := &fakeRaftNode{
		leader:     false,
		leaderID:   "n1",
		leaderAddr: "127.0.0.1:10001",
	}
	store := newRaftStore(node, db, RaftStoreConfig{})

	tests := []struct {
		name string
		fn   func() error
	}{
		{name: "put", fn: func() error { return store.Put(context.Background(), []byte("name"), []byte("alice")) }},
		{name: "get", fn: func() error {
			_, err := store.Get(context.Background(), []byte("name"))
			return err
		}},
		{name: "delete", fn: func() error { return store.Delete(context.Background(), []byte("name")) }},
		{name: "flush", fn: func() error { return store.Flush(context.Background()) }},
		{name: "compact", fn: func() error { return store.Compact(context.Background()) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if !errors.Is(err, ErrNotLeader) {
				t.Fatalf("operation error = %v, want ErrNotLeader", err)
			}

			var notLeader *NotLeaderError
			if !errors.As(err, &notLeader) {
				t.Fatalf("operation error = %T, want NotLeaderError", err)
			}
			if notLeader.LeaderID != "n1" || notLeader.LeaderAddr != "127.0.0.1:10001" {
				t.Fatalf("leader = %q %q, want n1 127.0.0.1:10001", notLeader.LeaderID, notLeader.LeaderAddr)
			}
		})
	}

	if node.applyCalled {
		t.Fatal("follower operation should not call Apply")
	}
	if node.barrierCalled {
		t.Fatal("follower operation should not call Barrier")
	}
}

func TestRaftStoreRejectsEmptyKeyBeforeLeaderCheck(t *testing.T) {
	db := openRaftStoreTestDB(t)
	defer db.Close()

	node := &fakeRaftNode{leader: false}
	store := newRaftStore(node, db, RaftStoreConfig{})

	if err := store.Put(context.Background(), nil, []byte("alice")); !errors.Is(err, kv.ErrEmptyKey) {
		t.Fatalf("Put(empty key) error = %v, want ErrEmptyKey", err)
	}
	if _, err := store.Get(context.Background(), nil); !errors.Is(err, kv.ErrEmptyKey) {
		t.Fatalf("Get(empty key) error = %v, want ErrEmptyKey", err)
	}
	if err := store.Delete(context.Background(), nil); !errors.Is(err, kv.ErrEmptyKey) {
		t.Fatalf("Delete(empty key) error = %v, want ErrEmptyKey", err)
	}
}

func TestRaftStoreFlushAndCompactRequireLeader(t *testing.T) {
	db := openRaftStoreTestDB(t)
	defer db.Close()

	node := &fakeRaftNode{leader: true}
	store := newRaftStore(node, db, RaftStoreConfig{})

	if err := db.Put([]byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() seed error = %v", err)
	}
	if err := store.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := store.Compact(context.Background()); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
}

func TestRaftStoreUsesContextDeadlineWhenShorter(t *testing.T) {
	db := openRaftStoreTestDB(t)
	defer db.Close()

	node := &fakeRaftNode{leader: true}
	store := newRaftStore(node, db, RaftStoreConfig{ApplyTimeout: time.Hour})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := store.Put(ctx, []byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if node.appliedTimeout <= 0 || node.appliedTimeout >= time.Hour {
		t.Fatalf("Apply timeout = %s, want positive timeout shorter than fallback", node.appliedTimeout)
	}
}

func TestRaftStoreContextCanceled(t *testing.T) {
	db := openRaftStoreTestDB(t)
	defer db.Close()

	node := &fakeRaftNode{leader: true}
	store := newRaftStore(node, db, RaftStoreConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := store.Put(ctx, []byte("name"), []byte("alice")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put(canceled) error = %v, want context.Canceled", err)
	}
	if node.applyCalled {
		t.Fatal("canceled Put should not call Apply")
	}
}

package raftnode

import (
	"bytes"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	kv "kv_store_demo"
)

func freeTCPAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("skip TCP raft test because local listen is not permitted: %v", err)
		}
		t.Fatalf("Listen() error = %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("Close() listener error = %v", err)
	}
	return addr
}

func openTestDB(t *testing.T) *kv.DB {
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

func waitForLeader(t *testing.T, node *Node) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	leaderID, leaderAddr := node.Leader()
	t.Fatalf("node did not become leader, leader id=%q addr=%q", leaderID, leaderAddr)
}

func TestValidateNodeConfig(t *testing.T) {
	valid := NodeConfig{
		NodeID:   "n1",
		RaftAddr: "127.0.0.1:10001",
		RaftDir:  t.TempDir(),
		Peers: []Peer{
			{ID: "n1", RaftAddr: "127.0.0.1:10001"},
		},
	}

	tests := []struct {
		name    string
		config  NodeConfig
		want    error
		wantErr bool
	}{
		{name: "valid", config: valid},
		{name: "empty node id", config: NodeConfig{RaftAddr: valid.RaftAddr, RaftDir: valid.RaftDir, Peers: valid.Peers}, want: ErrEmptyNodeID},
		{name: "empty raft addr", config: NodeConfig{NodeID: valid.NodeID, RaftDir: valid.RaftDir, Peers: valid.Peers}, want: ErrEmptyRaftAddr},
		{name: "empty raft dir", config: NodeConfig{NodeID: valid.NodeID, RaftAddr: valid.RaftAddr, Peers: valid.Peers}, want: ErrEmptyRaftDir},
		{name: "empty peers", config: NodeConfig{NodeID: valid.NodeID, RaftAddr: valid.RaftAddr, RaftDir: valid.RaftDir}, want: ErrEmptyPeers},
		{
			name: "missing local peer",
			config: NodeConfig{
				NodeID:   "n1",
				RaftAddr: "127.0.0.1:10001",
				RaftDir:  valid.RaftDir,
				Peers:    []Peer{{ID: "n2", RaftAddr: "127.0.0.1:10002"}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNodeConfig(tt.config)
			if tt.want == nil && !tt.wantErr {
				if err != nil {
					t.Fatalf("validateNodeConfig() error = %v, want nil", err)
				}
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Fatal("validateNodeConfig() error = nil, want error")
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("validateNodeConfig() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestNodeBootstrapSingleNodeAndApply(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	addr := freeTCPAddr(t)
	node, err := NewNode(NodeConfig{
		NodeID:    "n1",
		RaftAddr:  addr,
		RaftDir:   t.TempDir(),
		Peers:     []Peer{{ID: "n1", RaftAddr: addr}},
		Bootstrap: true,
	}, db)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	defer node.Close()

	waitForLeader(t, node)

	encoded, err := EncodeCommand(Command{
		Type:  CommandTypePut,
		Key:   []byte("name"),
		Value: []byte("alice"),
	})
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}
	if err := node.Apply(encoded, 3*time.Second); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if err := node.Barrier(3 * time.Second); err != nil {
		t.Fatalf("Barrier() error = %v", err)
	}

	got, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(got, []byte("alice")) {
		t.Fatalf("Get() = %q, want %q", got, "alice")
	}
}

func TestNodeBootstrapSkipsExistingState(t *testing.T) {
	raftDir := t.TempDir()
	addr := freeTCPAddr(t)
	config := NodeConfig{
		NodeID:    "n1",
		RaftAddr:  addr,
		RaftDir:   raftDir,
		Peers:     []Peer{{ID: "n1", RaftAddr: addr}},
		Bootstrap: true,
	}

	db := openTestDB(t)
	node, err := NewNode(config, db)
	if err != nil {
		t.Fatalf("first NewNode() error = %v", err)
	}
	waitForLeader(t, node)
	if err := node.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first DB Close() error = %v", err)
	}

	db = openTestDB(t)
	defer db.Close()

	config.RaftAddr = freeTCPAddr(t)
	config.Peers = []Peer{{ID: "n1", RaftAddr: config.RaftAddr}}
	node, err = NewNode(config, db)
	if err != nil {
		t.Fatalf("second NewNode() error = %v", err)
	}
	defer node.Close()
}

func TestNodeSnapshotAndRestartRestoresData(t *testing.T) {
	raftDir := t.TempDir()
	dbDir := t.TempDir()
	addr := freeTCPAddr(t)
	config := NodeConfig{
		NodeID:    "n1",
		RaftAddr:  addr,
		RaftDir:   raftDir,
		Peers:     []Peer{{ID: "n1", RaftAddr: addr}},
		Bootstrap: true,
	}

	db, err := kv.Open(kv.Options{
		Dir:          dbDir,
		MemTableSize: kv.DefaultMemTableSize,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := NewNode(config, db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewNode() error = %v", err)
	}
	waitForLeader(t, node)

	encoded, err := EncodeCommand(Command{
		Type:  CommandTypePut,
		Key:   []byte("name"),
		Value: []byte("alice"),
	})
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}
	if err := node.Apply(encoded, 3*time.Second); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if err := node.Barrier(3 * time.Second); err != nil {
		t.Fatalf("Barrier() error = %v", err)
	}
	if err := node.Snapshot(); err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close node error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close db error = %v", err)
	}

	if err := os.RemoveAll(dbDir); err != nil {
		t.Fatalf("RemoveAll(dbDir) error = %v", err)
	}
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatalf("MkdirAll(dbDir) error = %v", err)
	}

	db, err = kv.Open(kv.Options{
		Dir:          dbDir,
		MemTableSize: kv.DefaultMemTableSize,
	})
	if err != nil {
		t.Fatalf("reopen db error = %v", err)
	}
	defer db.Close()

	config.RaftAddr = freeTCPAddr(t)
	config.Peers = []Peer{{ID: "n1", RaftAddr: config.RaftAddr}}
	node, err = NewNode(config, db)
	if err != nil {
		t.Fatalf("restart NewNode() error = %v", err)
	}
	defer node.Close()

	waitForLeader(t, node)
	if err := node.Barrier(3 * time.Second); err != nil {
		t.Fatalf("restart Barrier() error = %v", err)
	}

	got, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(got, []byte("alice")) {
		t.Fatalf("Get() = %q, want alice", got)
	}
}

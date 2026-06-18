package raftnode

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	kv "kv_store_demo"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	defaultTransportMaxPool  = 3
	defaultTransportTimeout  = 10 * time.Second
	defaultSnapshotRetain    = 1
	defaultSnapshotThreshold = 1024
	defaultSnapshotInterval  = 30 * time.Second
)

var (
	ErrEmptyNodeID   = errors.New("raft node id is empty")
	ErrEmptyRaftAddr = errors.New("raft address is empty")
	ErrEmptyRaftDir  = errors.New("raft dir is empty")
	ErrEmptyPeers    = errors.New("raft peers are empty")
)

type Peer struct {
	ID       string
	RaftAddr string
}

type NodeConfig struct {
	NodeID            string
	RaftAddr          string
	RaftDir           string
	Peers             []Peer
	Bootstrap         bool
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration
}

type Node struct {
	raft      *raft.Raft
	store     *raftboltdb.BoltStore
	transport *raft.NetworkTransport
}

func NewNode(config NodeConfig, db *kv.DB) (*Node, error) {
	if err := validateNodeConfig(config); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(config.RaftDir, 0755); err != nil {
		return nil, err
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	raftConfig.NoSnapshotRestoreOnStart = false
	raftConfig.SnapshotThreshold = config.SnapshotThreshold
	if raftConfig.SnapshotThreshold == 0 {
		raftConfig.SnapshotThreshold = defaultSnapshotThreshold
	}
	raftConfig.SnapshotInterval = config.SnapshotInterval
	if raftConfig.SnapshotInterval <= 0 {
		raftConfig.SnapshotInterval = defaultSnapshotInterval
	}

	store, err := raftboltdb.NewBoltStore(filepath.Join(config.RaftDir, "raft.db"))
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(config.RaftDir, "snapshots"),
		defaultSnapshotRetain,
		os.Stderr,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	advertiseAddr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	transport, err := raft.NewTCPTransport(
		config.RaftAddr,
		advertiseAddr,
		defaultTransportMaxPool,
		defaultTransportTimeout,
		os.Stderr,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	if config.Bootstrap {
		hasState, err := raft.HasExistingState(store, store, snapshotStore)
		if err != nil {
			_ = transport.Close()
			_ = store.Close()
			return nil, err
		}
		if !hasState {
			if err := raft.BootstrapCluster(raftConfig, store, store, snapshotStore, transport, newRaftConfiguration(config.Peers)); err != nil {
				_ = transport.Close()
				_ = store.Close()
				return nil, err
			}
		}
	}

	raftNode, err := raft.NewRaft(raftConfig, NewFSM(db), store, store, snapshotStore, transport)
	if err != nil {
		_ = transport.Close()
		_ = store.Close()
		return nil, err
	}

	return &Node{
		raft:      raftNode,
		store:     store,
		transport: transport,
	}, nil
}

func validateNodeConfig(config NodeConfig) error {
	if config.NodeID == "" {
		return ErrEmptyNodeID
	}
	if config.RaftAddr == "" {
		return ErrEmptyRaftAddr
	}
	if config.RaftDir == "" {
		return ErrEmptyRaftDir
	}
	if len(config.Peers) == 0 {
		return ErrEmptyPeers
	}

	seenLocal := false
	seenIDs := make(map[string]struct{}, len(config.Peers))
	for _, peer := range config.Peers {
		if peer.ID == "" {
			return ErrEmptyNodeID
		}
		if peer.RaftAddr == "" {
			return ErrEmptyRaftAddr
		}
		if _, ok := seenIDs[peer.ID]; ok {
			return fmt.Errorf("duplicate raft peer id %q", peer.ID)
		}
		seenIDs[peer.ID] = struct{}{}
		if peer.ID == config.NodeID {
			seenLocal = true
		}
	}
	if !seenLocal {
		return fmt.Errorf("raft peers do not contain local node %q", config.NodeID)
	}

	return nil
}

func newRaftConfiguration(peers []Peer) raft.Configuration {
	servers := make([]raft.Server, 0, len(peers))
	for _, peer := range peers {
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(peer.ID),
			Address:  raft.ServerAddress(peer.RaftAddr),
		})
	}
	return raft.Configuration{Servers: servers}
}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) Leader() (string, string) {
	addr, id := n.raft.LeaderWithID()
	return string(id), string(addr)
}

func (n *Node) Apply(data []byte, timeout time.Duration) error {
	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return err
	}
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
		return fmt.Errorf("unexpected raft apply response %T", resp)
	}
	return nil
}

func (n *Node) Barrier(timeout time.Duration) error {
	return n.raft.Barrier(timeout).Error()
}

func (n *Node) Snapshot() error {
	return n.raft.Snapshot().Error()
}

func (n *Node) Close() error {
	shutdownErr := n.raft.Shutdown().Error()
	transportErr := n.transport.Close()
	storeErr := n.store.Close()

	if shutdownErr != nil {
		return shutdownErr
	}
	if transportErr != nil {
		return transportErr
	}
	return storeErr
}

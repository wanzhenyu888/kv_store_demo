package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	kv "kv_store_demo"
	"kv_store_demo/internal/client"
	raftnode "kv_store_demo/internal/raft"
	grpcserver "kv_store_demo/internal/server/grpc"
	"kv_store_demo/internal/shard"

	"google.golang.org/grpc"
)

type raftGRPCNode struct {
	id       string
	db       *kv.DB
	raft     *raftnode.Node
	grpc     *grpc.Server
	listener net.Listener
	serveCh  chan error
	addr     string
}

type multiRaftGRPCNode struct {
	id       string
	groups   map[shard.GroupID]*multiRaftGroup
	grpc     *grpc.Server
	listener net.Listener
	serveCh  chan error
	addr     string
}

type multiRaftGroup struct {
	db    *kv.DB
	raft  *raftnode.Node
	store *raftnode.RaftStore
}

func freeLocalAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("skip TCP raft gRPC test because local listen is not permitted: %v", err)
		}
		t.Fatalf("Listen() error = %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("Close() listener error = %v", err)
	}
	return addr
}

func startMultiRaftGRPCNode(
	t *testing.T,
	id string,
	clientAddr string,
	groupRaftAddrs map[shard.GroupID]string,
	groupPeers map[shard.GroupID][]raftnode.Peer,
	bootstrap bool,
	router *shard.Router,
) *multiRaftGRPCNode {
	t.Helper()

	node := &multiRaftGRPCNode{
		id:     id,
		groups: make(map[shard.GroupID]*multiRaftGroup, len(groupRaftAddrs)),
	}
	success := false
	defer func() {
		if !success {
			node.stop(t)
		}
	}()

	groupStores := make(map[shard.GroupID]shard.Store, len(groupRaftAddrs))
	for groupID, raftAddr := range groupRaftAddrs {
		db, err := kv.Open(kv.Options{
			Dir:          t.TempDir(),
			MemTableSize: kv.DefaultMemTableSize,
		})
		if err != nil {
			t.Fatalf("Open(%s/%s) error = %v", id, groupID, err)
		}

		raftNode, err := raftnode.NewNode(raftnode.NodeConfig{
			NodeID:    id,
			RaftAddr:  raftAddr,
			RaftDir:   t.TempDir(),
			Peers:     groupPeers[groupID],
			Bootstrap: bootstrap,
		}, db)
		if err != nil {
			_ = db.Close()
			t.Fatalf("NewNode(%s/%s) error = %v", id, groupID, err)
		}

		store := raftnode.NewRaftStore(raftNode, db, raftnode.RaftStoreConfig{})
		node.groups[groupID] = &multiRaftGroup{
			db:    db,
			raft:  raftNode,
			store: store,
		}
		groupStores[groupID] = store
	}

	listener, err := net.Listen("tcp", clientAddr)
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("skip TCP multi raft gRPC test because local listen is not permitted: %v", err)
		}
		t.Fatalf("Listen(%s) error = %v", clientAddr, err)
	}

	shardStore, err := shard.NewShardStore(shard.ShardStoreConfig{
		Router: router,
		Groups: groupStores,
	})
	if err != nil {
		_ = listener.Close()
		t.Fatalf("NewShardStore(%s) error = %v", id, err)
	}

	grpcServer := grpc.NewServer()
	grpcserver.Register(grpcServer, shardStore)
	serveCh := make(chan error, 1)
	go func() {
		serveCh <- grpcServer.Serve(listener)
	}()

	node.grpc = grpcServer
	node.listener = listener
	node.serveCh = serveCh
	node.addr = listener.Addr().String()
	success = true
	return node
}

type grpcStoreFactory func(t *testing.T, baseStore grpcserver.Store) grpcserver.Store

func startRaftGRPCNode(t *testing.T, id string, clientAddr string, raftAddr string, peers []raftnode.Peer, bootstrap bool) *raftGRPCNode {
	return startRaftGRPCNodeWithStore(t, id, clientAddr, raftAddr, peers, bootstrap, nil)
}

func startRaftGRPCNodeWithStore(t *testing.T, id string, clientAddr string, raftAddr string, peers []raftnode.Peer, bootstrap bool, storeFactory grpcStoreFactory) *raftGRPCNode {
	t.Helper()

	db, err := kv.Open(kv.Options{
		Dir:          t.TempDir(),
		MemTableSize: kv.DefaultMemTableSize,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	raftNode, err := raftnode.NewNode(raftnode.NodeConfig{
		NodeID:    id,
		RaftAddr:  raftAddr,
		RaftDir:   t.TempDir(),
		Peers:     peers,
		Bootstrap: bootstrap,
	}, db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewNode(%s) error = %v", id, err)
	}

	listener, err := net.Listen("tcp", clientAddr)
	if err != nil {
		_ = raftNode.Close()
		_ = db.Close()
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("skip TCP raft gRPC test because local listen is not permitted: %v", err)
		}
		t.Fatalf("Listen(%s) error = %v", clientAddr, err)
	}

	grpcServer := grpc.NewServer()
	baseStore := raftnode.NewRaftStore(raftNode, db, raftnode.RaftStoreConfig{})
	var store grpcserver.Store = baseStore
	if storeFactory != nil {
		store = storeFactory(t, baseStore)
	}
	grpcserver.Register(grpcServer, store)
	serveCh := make(chan error, 1)
	go func() {
		serveCh <- grpcServer.Serve(listener)
	}()

	return &raftGRPCNode{
		id:       id,
		db:       db,
		raft:     raftNode,
		grpc:     grpcServer,
		listener: listener,
		serveCh:  serveCh,
		addr:     listener.Addr().String(),
	}
}

func newShardGRPCStoreFactory(router *shard.Router, groupIDs ...shard.GroupID) grpcStoreFactory {
	return func(t *testing.T, baseStore grpcserver.Store) grpcserver.Store {
		t.Helper()

		groups := make(map[shard.GroupID]shard.Store, len(groupIDs))
		for _, groupID := range groupIDs {
			groups[groupID] = baseStore
		}

		store, err := shard.NewShardStore(shard.ShardStoreConfig{
			Router: router,
			Groups: groups,
		})
		if err != nil {
			t.Fatalf("NewShardStore() error = %v", err)
		}
		return store
	}
}

func newRaftGRPCShardRouter(t *testing.T) *shard.Router {
	t.Helper()

	router, err := shard.NewRouter(shard.RouterConfig{
		Ring: shard.HashRingConfig{
			ShardCount:   4,
			VirtualNodes: 8,
		},
		ShardGroups: map[int]shard.GroupID{
			0: "g1",
			1: "g1",
			2: "g2",
			3: "g2",
		},
	})
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	return router
}

func keyForShardGroup(t *testing.T, router *shard.Router, groupID shard.GroupID) []byte {
	t.Helper()

	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("sharded-key-%d", i))
		route, err := router.RouteKey(key)
		if err != nil {
			t.Fatalf("RouteKey() error = %v", err)
		}
		if route.GroupID == groupID {
			return key
		}
	}
	t.Fatalf("no key found for shard group %q", groupID)
	return nil
}

func (n *raftGRPCNode) stop(t *testing.T) {
	t.Helper()

	if n.grpc != nil {
		n.grpc.GracefulStop()
		select {
		case err := <-n.serveCh:
			if err != nil {
				t.Fatalf("Serve(%s) error = %v", n.id, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout waiting for gRPC server %s to stop", n.id)
		}
		n.grpc = nil
	}
	if n.raft != nil {
		if err := n.raft.Close(); err != nil {
			t.Fatalf("Close raft node %s error = %v", n.id, err)
		}
		n.raft = nil
	}
	if n.db != nil {
		if err := n.db.Close(); err != nil {
			t.Fatalf("Close db %s error = %v", n.id, err)
		}
		n.db = nil
	}
}

func (n *multiRaftGRPCNode) stop(t *testing.T) {
	t.Helper()

	if n.grpc != nil {
		n.grpc.GracefulStop()
		select {
		case err := <-n.serveCh:
			if err != nil {
				t.Fatalf("Serve(%s) error = %v", n.id, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout waiting for multi gRPC server %s to stop", n.id)
		}
		n.grpc = nil
	}

	for groupID, group := range n.groups {
		if group.raft != nil {
			if err := group.raft.Close(); err != nil {
				t.Fatalf("Close raft node %s/%s error = %v", n.id, groupID, err)
			}
			group.raft = nil
		}
		if group.db != nil {
			if err := group.db.Close(); err != nil {
				t.Fatalf("Close db %s/%s error = %v", n.id, groupID, err)
			}
			group.db = nil
		}
	}
}

func waitForRaftLeader(t *testing.T, nodes []*raftGRPCNode) *raftGRPCNode {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.raft != nil && node.raft.IsLeader() {
				return node
			}
		}
		time.Sleep(25 * time.Millisecond)
	}

	for _, node := range nodes {
		if node.raft == nil {
			t.Logf("%s stopped", node.id)
			continue
		}
		leaderID, leaderAddr := node.raft.Leader()
		t.Logf("%s leader hint id=%q addr=%q", node.id, leaderID, leaderAddr)
	}
	t.Fatal("no raft leader elected")
	return nil
}

func waitForMultiRaftLeader(t *testing.T, nodes []*multiRaftGRPCNode, groupID shard.GroupID) *multiRaftGRPCNode {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			group := node.groups[groupID]
			if group != nil && group.raft != nil && group.raft.IsLeader() {
				return node
			}
		}
		time.Sleep(25 * time.Millisecond)
	}

	for _, node := range nodes {
		group := node.groups[groupID]
		if group == nil || group.raft == nil {
			t.Logf("%s/%s stopped", node.id, groupID)
			continue
		}
		leaderID, leaderAddr := group.raft.Leader()
		t.Logf("%s/%s leader hint id=%q addr=%q", node.id, groupID, leaderID, leaderAddr)
	}
	t.Fatalf("no raft leader elected for group %s", groupID)
	return nil
}

func waitForLocalValue(t *testing.T, db *kv.DB, key []byte, want []byte) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
		if bytes.Equal(got, want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for local value key=%q value=%q", key, want)
}

func requireLocalMissing(t *testing.T, db *kv.DB, key []byte) {
	t.Helper()

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	if len(got) != 0 {
		t.Fatalf("Get(%q) = %q, want empty", key, got)
	}
}

func TestRaftGRPCThreeNodeReplication(t *testing.T) {
	clientAddrs := []string{freeLocalAddr(t), freeLocalAddr(t), freeLocalAddr(t)}
	raftAddrs := []string{freeLocalAddr(t), freeLocalAddr(t), freeLocalAddr(t)}
	peers := []raftnode.Peer{
		{ID: "n1", RaftAddr: raftAddrs[0]},
		{ID: "n2", RaftAddr: raftAddrs[1]},
		{ID: "n3", RaftAddr: raftAddrs[2]},
	}

	nodes := []*raftGRPCNode{
		startRaftGRPCNode(t, "n1", clientAddrs[0], raftAddrs[0], peers, true),
		startRaftGRPCNode(t, "n2", clientAddrs[1], raftAddrs[1], peers, false),
		startRaftGRPCNode(t, "n3", clientAddrs[2], raftAddrs[2], peers, false),
	}
	defer func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			nodes[i].stop(t)
		}
	}()

	leader := waitForRaftLeader(t, nodes)
	leaderClient := dialGrpcTestClient(t, leader.addr)
	defer leaderClient.Close()

	ctx := context.Background()
	if err := leaderClient.Put(ctx, []byte("name"), []byte("alice")); err != nil {
		t.Fatalf("leader Put() error = %v", err)
	}
	got, err := leaderClient.Get(ctx, []byte("name"))
	if err != nil {
		t.Fatalf("leader Get() error = %v", err)
	}
	if !bytes.Equal(got, []byte("alice")) {
		t.Fatalf("leader Get() = %q, want %q", got, "alice")
	}

	for _, node := range nodes {
		waitForLocalValue(t, node.db, []byte("name"), []byte("alice"))
	}

	var follower *raftGRPCNode
	for _, node := range nodes {
		if node != leader {
			follower = node
			break
		}
	}
	followerClient := dialGrpcTestClient(t, follower.addr)
	err = followerClient.Put(ctx, []byte("city"), []byte("hangzhou"))
	_ = followerClient.Close()
	if !client.IsNotLeader(err) {
		t.Fatalf("follower Put() error = %v, want NotLeader", err)
	}
	if _, leaderAddr, ok := client.LeaderHint(err); !ok || leaderAddr == "" {
		t.Fatalf("LeaderHint() = ok:%v addr:%q, want leader raft addr", ok, leaderAddr)
	}

	follower.stop(t)
	if err := leaderClient.Put(ctx, []byte("after-stop"), []byte("ok")); err != nil {
		t.Fatalf("leader Put() after stopping one follower error = %v", err)
	}
	got, err = leaderClient.Get(ctx, []byte("after-stop"))
	if err != nil {
		t.Fatalf("leader Get() after stopping one follower error = %v", err)
	}
	if !bytes.Equal(got, []byte("ok")) {
		t.Fatalf("leader Get() after stopping one follower = %q, want %q", got, "ok")
	}
}

func TestRaftGRPCShardRouting(t *testing.T) {
	clientAddrs := []string{freeLocalAddr(t), freeLocalAddr(t), freeLocalAddr(t)}
	raftAddrs := []string{freeLocalAddr(t), freeLocalAddr(t), freeLocalAddr(t)}
	peers := []raftnode.Peer{
		{ID: "n1", RaftAddr: raftAddrs[0]},
		{ID: "n2", RaftAddr: raftAddrs[1]},
		{ID: "n3", RaftAddr: raftAddrs[2]},
	}
	router := newRaftGRPCShardRouter(t)
	storeFactory := newShardGRPCStoreFactory(router, "g1", "g2")

	nodes := []*raftGRPCNode{
		startRaftGRPCNodeWithStore(t, "n1", clientAddrs[0], raftAddrs[0], peers, true, storeFactory),
		startRaftGRPCNodeWithStore(t, "n2", clientAddrs[1], raftAddrs[1], peers, false, storeFactory),
		startRaftGRPCNodeWithStore(t, "n3", clientAddrs[2], raftAddrs[2], peers, false, storeFactory),
	}
	defer func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			nodes[i].stop(t)
		}
	}()

	leader := waitForRaftLeader(t, nodes)
	leaderClient := dialGrpcTestClient(t, leader.addr)
	defer leaderClient.Close()

	keyG1 := keyForShardGroup(t, router, "g1")
	keyG2 := keyForShardGroup(t, router, "g2")

	ctx := context.Background()
	if err := leaderClient.Put(ctx, keyG1, []byte("value-g1")); err != nil {
		t.Fatalf("leader Put(g1 key) error = %v", err)
	}
	if err := leaderClient.Put(ctx, keyG2, []byte("value-g2")); err != nil {
		t.Fatalf("leader Put(g2 key) error = %v", err)
	}

	got, err := leaderClient.Get(ctx, keyG1)
	if err != nil {
		t.Fatalf("leader Get(g1 key) error = %v", err)
	}
	if !bytes.Equal(got, []byte("value-g1")) {
		t.Fatalf("leader Get(g1 key) = %q, want value-g1", got)
	}
	got, err = leaderClient.Get(ctx, keyG2)
	if err != nil {
		t.Fatalf("leader Get(g2 key) error = %v", err)
	}
	if !bytes.Equal(got, []byte("value-g2")) {
		t.Fatalf("leader Get(g2 key) = %q, want value-g2", got)
	}

	for _, node := range nodes {
		waitForLocalValue(t, node.db, keyG1, []byte("value-g1"))
		waitForLocalValue(t, node.db, keyG2, []byte("value-g2"))
	}

	var follower *raftGRPCNode
	for _, node := range nodes {
		if node != leader {
			follower = node
			break
		}
	}
	followerClient := dialGrpcTestClient(t, follower.addr)
	err = followerClient.Put(ctx, keyG1, []byte("follower-write"))
	_ = followerClient.Close()
	if !client.IsNotLeader(err) {
		t.Fatalf("follower Put() error = %v, want NotLeader", err)
	}
}

func TestRaftGRPCMultiShardGroups(t *testing.T) {
	clientAddrs := map[string]string{
		"n1": freeLocalAddr(t),
		"n2": freeLocalAddr(t),
		"n3": freeLocalAddr(t),
	}
	g1Addrs := map[string]string{
		"n1": freeLocalAddr(t),
		"n2": freeLocalAddr(t),
		"n3": freeLocalAddr(t),
	}
	g2Addrs := map[string]string{
		"n1": freeLocalAddr(t),
		"n2": freeLocalAddr(t),
		"n3": freeLocalAddr(t),
	}
	groupPeers := map[shard.GroupID][]raftnode.Peer{
		"g1": {
			{ID: "n1", RaftAddr: g1Addrs["n1"]},
			{ID: "n2", RaftAddr: g1Addrs["n2"]},
			{ID: "n3", RaftAddr: g1Addrs["n3"]},
		},
		"g2": {
			{ID: "n1", RaftAddr: g2Addrs["n1"]},
			{ID: "n2", RaftAddr: g2Addrs["n2"]},
			{ID: "n3", RaftAddr: g2Addrs["n3"]},
		},
	}
	router := newRaftGRPCShardRouter(t)

	nodes := []*multiRaftGRPCNode{
		startMultiRaftGRPCNode(t, "n1", clientAddrs["n1"], map[shard.GroupID]string{
			"g1": g1Addrs["n1"],
			"g2": g2Addrs["n1"],
		}, groupPeers, true, router),
		startMultiRaftGRPCNode(t, "n2", clientAddrs["n2"], map[shard.GroupID]string{
			"g1": g1Addrs["n2"],
			"g2": g2Addrs["n2"],
		}, groupPeers, false, router),
		startMultiRaftGRPCNode(t, "n3", clientAddrs["n3"], map[shard.GroupID]string{
			"g1": g1Addrs["n3"],
			"g2": g2Addrs["n3"],
		}, groupPeers, false, router),
	}
	defer func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			nodes[i].stop(t)
		}
	}()

	leaderG1 := waitForMultiRaftLeader(t, nodes, "g1")
	leaderG2 := waitForMultiRaftLeader(t, nodes, "g2")
	clientG1 := dialGrpcTestClient(t, leaderG1.addr)
	defer clientG1.Close()
	clientG2 := dialGrpcTestClient(t, leaderG2.addr)
	defer clientG2.Close()

	keyG1 := keyForShardGroup(t, router, "g1")
	keyG2 := keyForShardGroup(t, router, "g2")

	ctx := context.Background()
	if err := clientG1.Put(ctx, keyG1, []byte("value-g1")); err != nil {
		t.Fatalf("leader g1 Put() error = %v", err)
	}
	if err := clientG2.Put(ctx, keyG2, []byte("value-g2")); err != nil {
		t.Fatalf("leader g2 Put() error = %v", err)
	}

	got, err := clientG1.Get(ctx, keyG1)
	if err != nil {
		t.Fatalf("leader g1 Get() error = %v", err)
	}
	if !bytes.Equal(got, []byte("value-g1")) {
		t.Fatalf("leader g1 Get() = %q, want value-g1", got)
	}
	got, err = clientG2.Get(ctx, keyG2)
	if err != nil {
		t.Fatalf("leader g2 Get() error = %v", err)
	}
	if !bytes.Equal(got, []byte("value-g2")) {
		t.Fatalf("leader g2 Get() = %q, want value-g2", got)
	}

	for _, node := range nodes {
		waitForLocalValue(t, node.groups["g1"].db, keyG1, []byte("value-g1"))
		requireLocalMissing(t, node.groups["g2"].db, keyG1)
		waitForLocalValue(t, node.groups["g2"].db, keyG2, []byte("value-g2"))
		requireLocalMissing(t, node.groups["g1"].db, keyG2)
	}

	followerG1 := firstMultiFollower(nodes, leaderG1, leaderG2)
	followerClient := dialGrpcTestClient(t, followerG1.addr)
	err = followerClient.Put(ctx, keyG1, []byte("follower-write"))
	_ = followerClient.Close()
	if !client.IsNotLeader(err) {
		t.Fatalf("follower g1 Put() error = %v, want NotLeader", err)
	}

	followerG1.stop(t)
	if err := clientG1.Put(ctx, keyG1, []byte("value-g1-after-stop")); err != nil {
		t.Fatalf("leader g1 Put() after stopping follower error = %v", err)
	}
	if err := clientG2.Put(ctx, keyG2, []byte("value-g2-after-stop")); err != nil {
		t.Fatalf("leader g2 Put() after stopping follower error = %v", err)
	}
	got, err = clientG1.Get(ctx, keyG1)
	if err != nil {
		t.Fatalf("leader g1 Get() after stopping follower error = %v", err)
	}
	if !bytes.Equal(got, []byte("value-g1-after-stop")) {
		t.Fatalf("leader g1 Get() after stopping follower = %q, want value-g1-after-stop", got)
	}
	got, err = clientG2.Get(ctx, keyG2)
	if err != nil {
		t.Fatalf("leader g2 Get() after stopping follower error = %v", err)
	}
	if !bytes.Equal(got, []byte("value-g2-after-stop")) {
		t.Fatalf("leader g2 Get() after stopping follower = %q, want value-g2-after-stop", got)
	}
}

func firstMultiFollower(nodes []*multiRaftGRPCNode, leaders ...*multiRaftGRPCNode) *multiRaftGRPCNode {
	for _, node := range nodes {
		isLeader := false
		for _, leader := range leaders {
			if node == leader {
				isLeader = true
				break
			}
		}
		if !isLeader {
			return node
		}
	}
	for _, node := range nodes {
		if node != leaders[0] {
			return node
		}
	}
	return nil
}

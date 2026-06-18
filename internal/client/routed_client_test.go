package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"kv_store_demo/internal/cluster"
	raftnode "kv_store_demo/internal/raft"
	grpcserver "kv_store_demo/internal/server/grpc"
	"kv_store_demo/internal/shard"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type routedFakeStore struct {
	data    map[string][]byte
	putErr  error
	getErr  error
	puts    int
	gets    int
	deletes int
	flushes int
}

func newRoutedFakeStore() *routedFakeStore {
	return &routedFakeStore{data: make(map[string][]byte)}
}

func (s *routedFakeStore) Put(ctx context.Context, key, value []byte) error {
	if s.putErr != nil {
		return s.putErr
	}
	s.puts++
	s.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (s *routedFakeStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	s.gets++
	return append([]byte(nil), s.data[string(key)]...), nil
}

func (s *routedFakeStore) Delete(ctx context.Context, key []byte) error {
	s.deletes++
	delete(s.data, string(key))
	return nil
}

func (s *routedFakeStore) Flush(ctx context.Context) error {
	s.flushes++
	return nil
}

func (s *routedFakeStore) Compact(ctx context.Context) error {
	return nil
}

type routedBufNode struct {
	target   string
	store    *routedFakeStore
	server   *grpc.Server
	listener *bufconn.Listener
	serveCh  chan error
}

func startRoutedBufNode(t *testing.T, target string, store *routedFakeStore) *routedBufNode {
	t.Helper()

	listener := bufconn.Listen(bufSize)
	server := grpc.NewServer()
	grpcserver.Register(server, store)
	serveCh := make(chan error, 1)
	go func() {
		serveCh <- server.Serve(listener)
	}()

	return &routedBufNode{
		target:   target,
		store:    store,
		server:   server,
		listener: listener,
		serveCh:  serveCh,
	}
}

func (n *routedBufNode) stop(t *testing.T) {
	t.Helper()

	n.server.Stop()
	select {
	case err := <-n.serveCh:
		if err != nil {
			t.Logf("Serve(%s) stopped: %v", n.target, err)
		}
	default:
	}
}

func newRoutedBufDialer(t *testing.T, nodes map[string]*routedBufNode) routedDialFunc {
	t.Helper()

	return func(ctx context.Context, target string) (*Client, error) {
		node := nodes[target]
		if node == nil {
			return nil, errors.New("target not found")
		}
		conn, err := grpc.NewClient(
			"passthrough:///"+target,
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return node.listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
		return newClient(conn), nil
	}
}

func TestRoutedClientRoutesKeysToGroupLeaders(t *testing.T) {
	config := routedTestClusterConfig()
	router, err := config.NewRouter()
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	storeN1 := newRoutedFakeStore()
	storeN2 := newRoutedFakeStore()
	nodeN1 := startRoutedBufNode(t, "n1-client", storeN1)
	defer nodeN1.stop(t)
	nodeN2 := startRoutedBufNode(t, "n2-client", storeN2)
	defer nodeN2.stop(t)

	client, err := newRoutedClient(context.Background(), config, newRoutedBufDialer(t, map[string]*routedBufNode{
		"n1-client": nodeN1,
		"n2-client": nodeN2,
	}))
	if err != nil {
		t.Fatalf("newRoutedClient() error = %v", err)
	}
	defer client.Close()

	keyG1 := routedKeyForGroup(t, router, "g1")
	keyG2 := routedKeyForGroup(t, router, "g2")

	if err := client.Put(context.Background(), keyG1, []byte("value-g1")); err != nil {
		t.Fatalf("Put(g1) error = %v", err)
	}
	if err := client.Put(context.Background(), keyG2, []byte("value-g2")); err != nil {
		t.Fatalf("Put(g2) error = %v", err)
	}

	if storeN1.puts != 1 || storeN2.puts != 1 {
		t.Fatalf("puts n1=%d n2=%d, want one write per group", storeN1.puts, storeN2.puts)
	}
	got, err := client.Get(context.Background(), keyG1)
	if err != nil {
		t.Fatalf("Get(g1) error = %v", err)
	}
	if !bytes.Equal(got, []byte("value-g1")) {
		t.Fatalf("Get(g1) = %q, want value-g1", got)
	}
}

func TestRoutedClientRefreshesLeaderFromNotLeaderHint(t *testing.T) {
	config := routedSingleGroupClusterConfig()
	storeN1 := newRoutedFakeStore()
	storeN1.putErr = &raftnode.NotLeaderError{
		LeaderID:   "n2",
		LeaderAddr: "n2-raft",
	}
	storeN2 := newRoutedFakeStore()
	nodeN1 := startRoutedBufNode(t, "n1-client", storeN1)
	defer nodeN1.stop(t)
	nodeN2 := startRoutedBufNode(t, "n2-client", storeN2)
	defer nodeN2.stop(t)

	client, err := newRoutedClient(context.Background(), config, newRoutedBufDialer(t, map[string]*routedBufNode{
		"n1-client": nodeN1,
		"n2-client": nodeN2,
	}))
	if err != nil {
		t.Fatalf("newRoutedClient() error = %v", err)
	}
	defer client.Close()

	if err := client.Put(context.Background(), []byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if storeN1.puts != 0 {
		t.Fatalf("n1 puts = %d, want 0 because it returned NotLeader", storeN1.puts)
	}
	if storeN2.puts != 1 {
		t.Fatalf("n2 puts = %d, want retry write on leader", storeN2.puts)
	}
	if client.leaders["g1"] != "n2" {
		t.Fatalf("leader cache = %q, want n2", client.leaders["g1"])
	}

	if err := client.Put(context.Background(), []byte("name2"), []byte("bob")); err != nil {
		t.Fatalf("second Put() error = %v", err)
	}
	if storeN2.puts != 2 {
		t.Fatalf("n2 puts after second write = %d, want 2", storeN2.puts)
	}
}

func routedTestClusterConfig() cluster.Config {
	return cluster.Config{
		Nodes: []cluster.Node{
			{ID: "n1", ClientAddr: "n1-client"},
			{ID: "n2", ClientAddr: "n2-client"},
		},
		ShardConfig: cluster.Shards{
			ShardCount:   4,
			VirtualNodes: 8,
		},
		ShardRoutes: []cluster.Shard{
			{ID: 0, GroupID: "g1"},
			{ID: 1, GroupID: "g1"},
			{ID: 2, GroupID: "g2"},
			{ID: 3, GroupID: "g2"},
		},
		Groups: []cluster.Group{
			{
				ID: "g1",
				Replicas: []cluster.GroupReplica{
					{NodeID: "n1", RaftAddr: "n1-raft"},
				},
			},
			{
				ID: "g2",
				Replicas: []cluster.GroupReplica{
					{NodeID: "n2", RaftAddr: "n2-raft"},
				},
			},
		},
	}
}

func routedSingleGroupClusterConfig() cluster.Config {
	return cluster.Config{
		Nodes: []cluster.Node{
			{ID: "n1", ClientAddr: "n1-client"},
			{ID: "n2", ClientAddr: "n2-client"},
		},
		ShardConfig: cluster.Shards{
			ShardCount:   1,
			VirtualNodes: 8,
		},
		ShardRoutes: []cluster.Shard{
			{ID: 0, GroupID: "g1"},
		},
		Groups: []cluster.Group{
			{
				ID: "g1",
				Replicas: []cluster.GroupReplica{
					{NodeID: "n1", RaftAddr: "n1-raft"},
					{NodeID: "n2", RaftAddr: "n2-raft"},
				},
			},
		},
	}
}

func routedKeyForGroup(t *testing.T, router *shard.Router, groupID shard.GroupID) []byte {
	t.Helper()

	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("routed-key-%d", i))
		route, err := router.RouteKey(key)
		if err != nil {
			t.Fatalf("RouteKey() error = %v", err)
		}
		if route.GroupID == groupID {
			return key
		}
	}
	t.Fatalf("no key found for group %q", groupID)
	return nil
}

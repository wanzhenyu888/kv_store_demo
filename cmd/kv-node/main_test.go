package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kv "kv_store_demo"
	"kv_store_demo/internal/shard"
)

func writeClusterConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}

func TestLoadClusterConfigSingleGroup(t *testing.T) {
	config, err := loadClusterConfig(writeClusterConfig(t, singleGroupClusterJSON()))
	if err != nil {
		t.Fatalf("loadClusterConfig() error = %v", err)
	}
	if len(config.Nodes) != 3 {
		t.Fatalf("len(nodes) = %d, want 3", len(config.Nodes))
	}

	node, err := config.FindNode("n2")
	if err != nil {
		t.Fatalf("findNode() error = %v", err)
	}
	if node.ClientAddr != "127.0.0.1:9002" {
		t.Fatalf("node n2 = %+v, want client address", node)
	}

	group, err := config.SingleGroup()
	if err != nil {
		t.Fatalf("singleGroup() error = %v", err)
	}
	if group.ID != "g1" {
		t.Fatalf("singleGroup().ID = %q, want g1", group.ID)
	}

	peers, err := config.RaftPeersForGroup("g1")
	if err != nil {
		t.Fatalf("raftPeersForGroup() error = %v", err)
	}
	if len(peers) != 3 || peers[0].ID != "n1" || peers[1].RaftAddr != "127.0.0.1:10002" {
		t.Fatalf("raftPeersForGroup() = %+v, want peers from group replicas", peers)
	}

	raftAddr, err := config.LocalRaftAddr("g1", "n3")
	if err != nil {
		t.Fatalf("localRaftAddr() error = %v", err)
	}
	if raftAddr != "127.0.0.1:10003" {
		t.Fatalf("localRaftAddr() = %q, want 127.0.0.1:10003", raftAddr)
	}
}

func TestLoadClusterConfigMultiGroup(t *testing.T) {
	config, err := loadClusterConfig(writeClusterConfig(t, multiGroupClusterJSON()))
	if err != nil {
		t.Fatalf("loadClusterConfig() error = %v", err)
	}

	routerConfig := config.RouterConfig()
	if routerConfig.Ring.ShardCount != 4 {
		t.Fatalf("ShardCount = %d, want 4", routerConfig.Ring.ShardCount)
	}
	if routerConfig.Ring.VirtualNodes != 8 {
		t.Fatalf("VirtualNodes = %d, want 8", routerConfig.Ring.VirtualNodes)
	}
	if routerConfig.ShardGroups[0] != shard.GroupID("g1") {
		t.Fatalf("shard 0 group = %q, want g1", routerConfig.ShardGroups[0])
	}
	if routerConfig.ShardGroups[3] != shard.GroupID("g2") {
		t.Fatalf("shard 3 group = %q, want g2", routerConfig.ShardGroups[3])
	}

	router, err := config.NewRouter()
	if err != nil {
		t.Fatalf("newRouter() error = %v", err)
	}
	route, err := router.RouteShard(2)
	if err != nil {
		t.Fatalf("RouteShard() error = %v", err)
	}
	if route.GroupID != shard.GroupID("g2") {
		t.Fatalf("RouteShard(2).GroupID = %q, want g2", route.GroupID)
	}

	groupIDs := config.GroupIDs()
	if len(groupIDs) != 2 || groupIDs[0] != shard.GroupID("g1") || groupIDs[1] != shard.GroupID("g2") {
		t.Fatalf("groupIDs() = %+v, want [g1 g2]", groupIDs)
	}

	localGroups := config.LocalGroups("n1")
	if len(localGroups) != 2 || localGroups[0].ID != "g1" || localGroups[1].ID != "g2" {
		t.Fatalf("localGroups(n1) = %+v, want g1 and g2", localGroups)
	}

	peers, err := config.RaftPeersForGroup("g2")
	if err != nil {
		t.Fatalf("raftPeersForGroup(g2) error = %v", err)
	}
	if len(peers) != 3 || peers[0].RaftAddr != "127.0.0.1:12001" {
		t.Fatalf("raftPeersForGroup(g2) = %+v, want g2 peers", peers)
	}
}

func TestLoadClusterConfigRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "empty nodes", content: `{"nodes":[]}`, want: "nodes is empty"},
		{name: "empty id", content: `{"nodes":[{"client_addr":"127.0.0.1:9001"}]}`, want: "id is empty"},
		{name: "empty client addr", content: `{"nodes":[{"id":"n1"}]}`, want: "client_addr is empty"},
		{name: "duplicate node id", content: withClusterSuffix(`"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"},{"id":"n1","client_addr":"127.0.0.1:9002"}]`), want: "duplicate cluster node id"},
		{name: "invalid json", content: `{`, want: "decode cluster config"},
		{name: "missing shard count", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"}],"groups":[{"id":"g1","replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10001"}]}]}`, want: "shard_config.shard_count"},
		{name: "missing virtual nodes", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":1},"shards":[{"id":0,"group_id":"g1"}],"groups":[{"id":"g1","replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10001"}]}]}`, want: "shard_config.virtual_nodes"},
		{name: "empty shards", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":1,"virtual_nodes":8},"groups":[{"id":"g1","replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10001"}]}]}`, want: "shards is empty"},
		{name: "empty groups", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":1,"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"}],"groups":[]}`, want: "groups is empty"},
		{name: "empty group id", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":1,"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"}],"groups":[{"replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10001"}]}]}`, want: "group id is empty"},
		{name: "duplicate group id", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":1,"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"}],"groups":[{"id":"g1","replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10001"}]},{"id":"g1","replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10002"}]}]}`, want: "duplicate cluster group id"},
		{name: "empty replicas", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":1,"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"}],"groups":[{"id":"g1","replicas":[]}]}`, want: "replicas is empty"},
		{name: "replica node not found", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":1,"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"}],"groups":[{"id":"g1","replicas":[{"node_id":"n2","raft_addr":"127.0.0.1:10001"}]}]}`, want: "not found"},
		{name: "duplicate replica node", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":1,"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"}],"groups":[{"id":"g1","replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10001"},{"node_id":"n1","raft_addr":"127.0.0.1:10002"}]}]}`, want: "duplicate replica node"},
		{name: "empty replica raft addr", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":1,"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"}],"groups":[{"id":"g1","replicas":[{"node_id":"n1"}]}]}`, want: "raft_addr is empty"},
		{name: "duplicate replica raft addr", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"},{"id":"n2","client_addr":"127.0.0.1:9002"}],"shard_config":{"shard_count":1,"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"}],"groups":[{"id":"g1","replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10001"},{"node_id":"n2","raft_addr":"127.0.0.1:10001"}]}]}`, want: "duplicate replica raft_addr"},
		{name: "duplicate shard id", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":2,"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"},{"id":0,"group_id":"g1"}],"groups":[{"id":"g1","replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10001"}]}]}`, want: "duplicate cluster shard id"},
		{name: "shard group not found", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":1,"virtual_nodes":8},"shards":[{"id":0,"group_id":"missing"}],"groups":[{"id":"g1","replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10001"}]}]}`, want: "group \"missing\" not found"},
		{name: "missing shard", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001"}],"shard_config":{"shard_count":2,"virtual_nodes":8},"shards":[{"id":0,"group_id":"g1"}],"groups":[{"id":"g1","replicas":[{"node_id":"n1","raft_addr":"127.0.0.1:10001"}]}]}`, want: "shard not assigned"},
		{name: "old node raft addr only", content: `{"nodes":[{"id":"n1","client_addr":"127.0.0.1:9001","raft_addr":"127.0.0.1:10001"}]}`, want: "shard_config.shard_count"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadClusterConfig(writeClusterConfig(t, tt.content))
			if err == nil {
				t.Fatal("loadClusterConfig() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("loadClusterConfig() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestClusterConfigHelpersRejectMissingValues(t *testing.T) {
	config, err := loadClusterConfig(writeClusterConfig(t, singleGroupClusterJSON()))
	if err != nil {
		t.Fatalf("loadClusterConfig() error = %v", err)
	}

	_, err = config.FindNode("missing")
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("findNode(missing) error = %v, want missing node", err)
	}

	_, err = config.FindGroup("missing")
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("findGroup(missing) error = %v, want missing group", err)
	}

	_, err = config.RaftPeersForGroup("missing")
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("raftPeersForGroup(missing) error = %v, want missing group", err)
	}

	_, err = config.LocalRaftAddr("g1", "missing")
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("localRaftAddr(missing node) error = %v, want missing node", err)
	}

	multi, err := loadClusterConfig(writeClusterConfig(t, multiGroupClusterJSON()))
	if err != nil {
		t.Fatalf("loadClusterConfig(multi) error = %v", err)
	}
	_, err = multi.SingleGroup()
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("singleGroup(multi) error = %v, want exactly one error", err)
	}
}

func TestResolveDataDirs(t *testing.T) {
	standalone := resolveDataDirs("/tmp/node", modeStandalone)
	if standalone.engineDir != "/tmp/node" || standalone.raftDir != "" {
		t.Fatalf("standalone dirs = %+v, want engine root and empty raft dir", standalone)
	}

	raft := resolveDataDirs("/tmp/node", modeRaft)
	if raft.engineDir != filepath.Join("/tmp/node", "engine") {
		t.Fatalf("raft engine dir = %q, want /tmp/node/engine", raft.engineDir)
	}
	if raft.raftDir != filepath.Join("/tmp/node", "raft") {
		t.Fatalf("raft dir = %q, want /tmp/node/raft", raft.raftDir)
	}
}

func TestNewNodeRuntimeRejectsInvalidMode(t *testing.T) {
	_, err := newNodeRuntime(runtimeConfig{mode: "bad"})
	if err == nil {
		t.Fatal("newNodeRuntime() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "unknown mode") {
		t.Fatalf("newNodeRuntime() error = %v, want unknown mode", err)
	}
}

func TestNewRaftRuntimeRequiresNodeIDAndCluster(t *testing.T) {
	_, err := newNodeRuntime(runtimeConfig{mode: modeRaft})
	if err == nil {
		t.Fatal("newNodeRuntime(raft without node-id) error = nil, want error")
	}
	if !strings.Contains(err.Error(), "node-id") {
		t.Fatalf("newNodeRuntime() error = %v, want node-id error", err)
	}

	_, err = newNodeRuntime(runtimeConfig{mode: modeRaft, nodeID: "n1"})
	if err == nil {
		t.Fatal("newNodeRuntime(raft without cluster) error = nil, want error")
	}
	if !strings.Contains(err.Error(), "cluster") {
		t.Fatalf("newNodeRuntime() error = %v, want cluster error", err)
	}
}

func TestNewStandaloneRuntime(t *testing.T) {
	runtime, err := newNodeRuntime(runtimeConfig{
		mode:         modeStandalone,
		addr:         "127.0.0.1:9001",
		dir:          t.TempDir(),
		memTableSize: kv.DefaultMemTableSize,
	})
	if err != nil {
		t.Fatalf("newNodeRuntime(standalone) error = %v", err)
	}
	defer closeRuntime(runtime)

	if runtime.listenAddr != "127.0.0.1:9001" {
		t.Fatalf("listenAddr = %q, want 127.0.0.1:9001", runtime.listenAddr)
	}
	if runtime.store == nil || runtime.db == nil {
		t.Fatalf("runtime = %+v, want store and db", runtime)
	}
	if len(runtime.groups) != 0 {
		t.Fatalf("groups = %+v, want empty in standalone mode", runtime.groups)
	}
	if runtime.router != nil {
		t.Fatalf("router = %v, want nil in standalone mode", runtime.router)
	}
}

func TestResolveGroupDataDirs(t *testing.T) {
	dirs := resolveGroupDataDirs("/tmp/node", "g1")
	if dirs.engineDir != filepath.Join("/tmp/node", "engine", "group-g1") {
		t.Fatalf("engine dir = %q, want group engine dir", dirs.engineDir)
	}
	if dirs.raftDir != filepath.Join("/tmp/node", "raft", "group-g1") {
		t.Fatalf("raft dir = %q, want group raft dir", dirs.raftDir)
	}
}

func TestNewShardRuntimeStoreRoutesToGroupStores(t *testing.T) {
	config, err := loadClusterConfig(writeClusterConfig(t, multiGroupClusterJSON()))
	if err != nil {
		t.Fatalf("loadClusterConfig() error = %v", err)
	}
	router, err := config.NewRouter()
	if err != nil {
		t.Fatalf("newRouter() error = %v", err)
	}

	storeG1 := newFakeRuntimeStore()
	storeG2 := newFakeRuntimeStore()
	store, err := newShardRuntimeStore(router, []*shardGroupRuntime{
		{groupID: "g1", store: storeG1},
		{groupID: "g2", store: storeG2},
	})
	if err != nil {
		t.Fatalf("newShardRuntimeStore() error = %v", err)
	}

	keyG2 := keyForGroup(t, router, "g2")
	if err := store.Put(context.Background(), keyG2, []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if storeG1.puts != 0 {
		t.Fatalf("g1 puts = %d, want 0", storeG1.puts)
	}
	if storeG2.puts != 1 {
		t.Fatalf("g2 puts = %d, want 1", storeG2.puts)
	}
}

func TestNewRaftRuntimeStartsMultipleLocalGroups(t *testing.T) {
	clusterPath, clientAddrs := writeDynamicMultiGroupClusterConfig(t)
	baseDir := t.TempDir()
	runtimes := make([]*nodeRuntime, 0, 3)
	defer func() {
		for i := len(runtimes) - 1; i >= 0; i-- {
			closeRuntime(runtimes[i])
		}
	}()

	for _, nodeID := range []string{"n1", "n2", "n3"} {
		runtime, err := newNodeRuntime(runtimeConfig{
			mode:         modeRaft,
			dir:          filepath.Join(baseDir, nodeID),
			memTableSize: kv.DefaultMemTableSize,
			nodeID:       nodeID,
			clusterPath:  clusterPath,
			bootstrap:    nodeID == "n1",
		})
		if err != nil {
			t.Fatalf("newNodeRuntime(%s) error = %v", nodeID, err)
		}
		runtimes = append(runtimes, runtime)

		if runtime.listenAddr != clientAddrs[nodeID] {
			t.Fatalf("%s listenAddr = %q, want %q", nodeID, runtime.listenAddr, clientAddrs[nodeID])
		}
		if runtime.router == nil || runtime.store == nil {
			t.Fatalf("%s runtime = %+v, want router and store", nodeID, runtime)
		}
		if len(runtime.groups) != 2 {
			t.Fatalf("%s groups = %d, want 2", nodeID, len(runtime.groups))
		}
	}

	leaderG1 := waitRuntimeLeader(t, runtimes, "g1")
	leaderG2 := waitRuntimeLeader(t, runtimes, "g2")
	keyG1 := keyForGroup(t, leaderG1.router, "g1")
	keyG2 := keyForGroup(t, leaderG2.router, "g2")

	if err := leaderG1.store.Put(context.Background(), keyG1, []byte("runtime-g1")); err != nil {
		t.Fatalf("leader g1 Put() error = %v", err)
	}
	if err := leaderG2.store.Put(context.Background(), keyG2, []byte("runtime-g2")); err != nil {
		t.Fatalf("leader g2 Put() error = %v", err)
	}

	for _, runtime := range runtimes {
		groupG1 := runtimeGroup(t, runtime, "g1")
		groupG2 := runtimeGroup(t, runtime, "g2")
		waitRuntimeLocalValue(t, groupG1.db, keyG1, []byte("runtime-g1"))
		waitRuntimeLocalMissing(t, groupG2.db, keyG1)
		waitRuntimeLocalValue(t, groupG2.db, keyG2, []byte("runtime-g2"))
		waitRuntimeLocalMissing(t, groupG1.db, keyG2)
	}
}

func TestLoadClusterConfigRejectsMissingFile(t *testing.T) {
	_, err := loadClusterConfig(filepath.Join(t.TempDir(), "missing.json"))
	if err == nil {
		t.Fatal("loadClusterConfig(missing) error = nil, want error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("loadClusterConfig(missing) error = %v, want os.ErrNotExist", err)
	}
}

func singleGroupClusterJSON() string {
	return `{
		"nodes": [
			{"id": "n1", "client_addr": "127.0.0.1:9001"},
			{"id": "n2", "client_addr": "127.0.0.1:9002"},
			{"id": "n3", "client_addr": "127.0.0.1:9003"}
		],
		"shard_config": {
			"shard_count": 4,
			"virtual_nodes": 8
		},
		"shards": [
			{"id": 0, "group_id": "g1"},
			{"id": 1, "group_id": "g1"},
			{"id": 2, "group_id": "g1"},
			{"id": 3, "group_id": "g1"}
		],
		"groups": [
			{
				"id": "g1",
				"replicas": [
					{"node_id": "n1", "raft_addr": "127.0.0.1:10001"},
					{"node_id": "n2", "raft_addr": "127.0.0.1:10002"},
					{"node_id": "n3", "raft_addr": "127.0.0.1:10003"}
				]
			}
		]
	}`
}

func multiGroupClusterJSON() string {
	return `{
		"nodes": [
			{"id": "n1", "client_addr": "127.0.0.1:9001"},
			{"id": "n2", "client_addr": "127.0.0.1:9002"},
			{"id": "n3", "client_addr": "127.0.0.1:9003"}
		],
		"shard_config": {
			"shard_count": 4,
			"virtual_nodes": 8
		},
		"shards": [
			{"id": 0, "group_id": "g1"},
			{"id": 1, "group_id": "g1"},
			{"id": 2, "group_id": "g2"},
			{"id": 3, "group_id": "g2"}
		],
		"groups": [
			{
				"id": "g1",
				"replicas": [
					{"node_id": "n1", "raft_addr": "127.0.0.1:11001"},
					{"node_id": "n2", "raft_addr": "127.0.0.1:11002"},
					{"node_id": "n3", "raft_addr": "127.0.0.1:11003"}
				]
			},
			{
				"id": "g2",
				"replicas": [
					{"node_id": "n1", "raft_addr": "127.0.0.1:12001"},
					{"node_id": "n2", "raft_addr": "127.0.0.1:12002"},
					{"node_id": "n3", "raft_addr": "127.0.0.1:12003"}
				]
			}
		]
	}`
}

func writeDynamicMultiGroupClusterConfig(t *testing.T) (string, map[string]string) {
	t.Helper()

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

	content := fmt.Sprintf(`{
		"nodes": [
			{"id": "n1", "client_addr": %q},
			{"id": "n2", "client_addr": %q},
			{"id": "n3", "client_addr": %q}
		],
		"shard_config": {
			"shard_count": 4,
			"virtual_nodes": 8
		},
		"shards": [
			{"id": 0, "group_id": "g1"},
			{"id": 1, "group_id": "g1"},
			{"id": 2, "group_id": "g2"},
			{"id": 3, "group_id": "g2"}
		],
		"groups": [
			{
				"id": "g1",
				"replicas": [
					{"node_id": "n1", "raft_addr": %q},
					{"node_id": "n2", "raft_addr": %q},
					{"node_id": "n3", "raft_addr": %q}
				]
			},
			{
				"id": "g2",
				"replicas": [
					{"node_id": "n1", "raft_addr": %q},
					{"node_id": "n2", "raft_addr": %q},
					{"node_id": "n3", "raft_addr": %q}
				]
			}
		]
	}`, clientAddrs["n1"], clientAddrs["n2"], clientAddrs["n3"],
		g1Addrs["n1"], g1Addrs["n2"], g1Addrs["n3"],
		g2Addrs["n1"], g2Addrs["n2"], g2Addrs["n3"])

	return writeClusterConfig(t, content), clientAddrs
}

func withClusterSuffix(nodes string) string {
	return `{
		` + nodes + `,
		"shard_config": {
			"shard_count": 1,
			"virtual_nodes": 8
		},
		"shards": [
			{"id": 0, "group_id": "g1"}
		],
		"groups": [
			{
				"id": "g1",
				"replicas": [
					{"node_id": "n1", "raft_addr": "127.0.0.1:10001"}
				]
			}
		]
	}`
}

func freeLocalAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("skip TCP raft runtime test because local listen is not permitted: %v", err)
		}
		t.Fatalf("Listen() error = %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("Close() listener error = %v", err)
	}
	return addr
}

func keyForGroup(t *testing.T, router *shard.Router, groupID shard.GroupID) []byte {
	t.Helper()

	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
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

func waitRuntimeLeader(t *testing.T, runtimes []*nodeRuntime, groupID shard.GroupID) *nodeRuntime {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, runtime := range runtimes {
			group := runtimeGroup(t, runtime, groupID)
			if group.raftNode.IsLeader() {
				return runtime
			}
		}
		time.Sleep(25 * time.Millisecond)
	}

	for _, runtime := range runtimes {
		group := runtimeGroup(t, runtime, groupID)
		leaderID, leaderAddr := group.raftNode.Leader()
		t.Logf("group %s leader hint id=%q addr=%q", groupID, leaderID, leaderAddr)
	}
	t.Fatalf("no raft leader elected for group %s", groupID)
	return nil
}

func runtimeGroup(t *testing.T, runtime *nodeRuntime, groupID shard.GroupID) *shardGroupRuntime {
	t.Helper()

	for _, group := range runtime.groups {
		if group.groupID == groupID {
			return group
		}
	}
	t.Fatalf("runtime missing group %s", groupID)
	return nil
}

func waitRuntimeLocalValue(t *testing.T, db *kv.DB, key, want []byte) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := db.Get(key)
		if err == nil && string(got) == string(want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	got, err := db.Get(key)
	t.Fatalf("db.Get(%q) = %q, %v; want %q", key, got, err, want)
}

func waitRuntimeLocalMissing(t *testing.T, db *kv.DB, key []byte) {
	t.Helper()

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("db.Get(%q) error = %v, want nil for missing key", key, err)
	}
	if got != nil {
		t.Fatalf("db.Get(%q) = %q, want nil for missing key", key, got)
	}
}

type fakeRuntimeStore struct {
	puts     int
	gets     int
	deletes  int
	flushes  int
	compacts int
}

func newFakeRuntimeStore() *fakeRuntimeStore {
	return &fakeRuntimeStore{}
}

func (s *fakeRuntimeStore) Put(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.puts++
	return nil
}

func (s *fakeRuntimeStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.gets++
	return nil, nil
}

func (s *fakeRuntimeStore) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.deletes++
	return nil
}

func (s *fakeRuntimeStore) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.flushes++
	return nil
}

func (s *fakeRuntimeStore) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.compacts++
	return nil
}

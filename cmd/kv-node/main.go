package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	kv "kv_store_demo"
	"kv_store_demo/internal/cluster"
	raftnode "kv_store_demo/internal/raft"
	grpcserver "kv_store_demo/internal/server/grpc"
	"kv_store_demo/internal/shard"

	"google.golang.org/grpc"
)

const (
	modeStandalone = "standalone"
	modeRaft       = "raft"
)

type clusterConfig = cluster.Config
type clusterGroup = cluster.Group

type dataDirs struct {
	engineDir string
	raftDir   string
}

type groupDataDirs struct {
	engineDir string
	raftDir   string
}

type nodeRuntime struct {
	listenAddr string
	store      grpcserver.Store
	db         *kv.DB
	groups     []*shardGroupRuntime
	router     *shard.Router
}

type shardGroupRuntime struct {
	groupID  shard.GroupID
	db       *kv.DB
	raftNode *raftnode.Node
	store    shard.Store
}

func main() {
	mode := flag.String("mode", modeStandalone, "node mode: standalone or raft")
	addr := flag.String("addr", ":9001", "gRPC listen address")
	dir := flag.String("dir", "/tmp/kv_store_demo/node", "DB data directory")
	memTableSize := flag.Uint64("memtable-size", kv.DefaultMemTableSize, "memtable size")
	nodeID := flag.String("node-id", "", "raft node id")
	clusterPath := flag.String("cluster", "", "raft cluster JSON config path")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap raft cluster if it has no existing state")
	flag.Parse()

	runtime, err := newNodeRuntime(runtimeConfig{
		mode:         *mode,
		addr:         *addr,
		dir:          *dir,
		memTableSize: *memTableSize,
		nodeID:       *nodeID,
		clusterPath:  *clusterPath,
		bootstrap:    *bootstrap,
	})
	if err != nil {
		log.Fatalf("start kv-node: %v", err)
	}

	listener, err := net.Listen("tcp", runtime.listenAddr)
	if err != nil {
		closeRuntime(runtime)
		log.Fatalf("listen %s: %v", runtime.listenAddr, err)
	}

	grpcServer := grpc.NewServer()
	grpcserver.Register(grpcServer, runtime.store)

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("kv-node mode=%s listening on %s", *mode, runtime.listenAddr)
		serveErr <- grpcServer.Serve(listener)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down", sig)
		grpcServer.GracefulStop()
	case err := <-serveErr:
		if err != nil {
			log.Printf("serve grpc: %v", err)
		}
	}

	closeRuntime(runtime)
}

type runtimeConfig struct {
	mode         string
	addr         string
	dir          string
	memTableSize uint64
	nodeID       string
	clusterPath  string
	bootstrap    bool
}

func newNodeRuntime(config runtimeConfig) (*nodeRuntime, error) {
	switch config.mode {
	case modeStandalone:
		return newStandaloneRuntime(config)
	case modeRaft:
		return newRaftRuntime(config)
	default:
		return nil, fmt.Errorf("unknown mode %q", config.mode)
	}
}

func newStandaloneRuntime(config runtimeConfig) (*nodeRuntime, error) {
	db, err := kv.Open(kv.Options{
		Dir:          config.dir,
		MemTableSize: config.memTableSize,
	})
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	return &nodeRuntime{
		listenAddr: config.addr,
		store:      grpcserver.NewStandaloneStore(db),
		db:         db,
	}, nil
}

func newRaftRuntime(config runtimeConfig) (*nodeRuntime, error) {
	if config.nodeID == "" {
		return nil, errors.New("node-id is required in raft mode")
	}
	if config.clusterPath == "" {
		return nil, errors.New("cluster is required in raft mode")
	}

	cluster, err := loadClusterConfig(config.clusterPath)
	if err != nil {
		return nil, err
	}
	router, err := cluster.NewRouter()
	if err != nil {
		return nil, err
	}
	current, err := cluster.FindNode(config.nodeID)
	if err != nil {
		return nil, err
	}
	groups, err := newShardGroupRuntimes(config, cluster)
	if err != nil {
		return nil, err
	}
	runtimeStore, err := newShardRuntimeStore(router, groups)
	if err != nil {
		closeShardGroupRuntimes(groups)
		return nil, err
	}

	return &nodeRuntime{
		listenAddr: current.ClientAddr,
		store:      runtimeStore,
		groups:     groups,
		router:     router,
	}, nil
}

func newShardGroupRuntimes(config runtimeConfig, cluster clusterConfig) ([]*shardGroupRuntime, error) {
	localGroups := cluster.LocalGroups(config.nodeID)
	if len(localGroups) == 0 {
		return nil, fmt.Errorf("cluster node %q is not a replica of any group", config.nodeID)
	}

	runtimes := make([]*shardGroupRuntime, 0, len(localGroups))
	success := false
	defer func() {
		if !success {
			closeShardGroupRuntimes(runtimes)
		}
	}()

	for _, group := range localGroups {
		runtime, err := newShardGroupRuntime(config, cluster, group)
		if err != nil {
			return nil, err
		}
		runtimes = append(runtimes, runtime)
	}

	success = true
	return runtimes, nil
}

func newShardGroupRuntime(config runtimeConfig, cluster clusterConfig, group clusterGroup) (*shardGroupRuntime, error) {
	groupID := shard.GroupID(group.ID)
	dirs := resolveGroupDataDirs(config.dir, groupID)
	db, err := kv.Open(kv.Options{
		Dir:          dirs.engineDir,
		MemTableSize: config.memTableSize,
	})
	if err != nil {
		return nil, fmt.Errorf("open db for group %q: %w", groupID, err)
	}

	raftAddr, err := cluster.LocalRaftAddr(groupID, config.nodeID)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("close db after group %q raft addr failure: %v", groupID, closeErr)
		}
		return nil, err
	}
	peers, err := cluster.RaftPeersForGroup(groupID)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("close db after group %q peers failure: %v", groupID, closeErr)
		}
		return nil, err
	}

	node, err := raftnode.NewNode(raftnode.NodeConfig{
		NodeID:    config.nodeID,
		RaftAddr:  raftAddr,
		RaftDir:   dirs.raftDir,
		Peers:     peers,
		Bootstrap: config.bootstrap,
	}, db)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("close db after group %q raft node failure: %v", groupID, closeErr)
		}
		return nil, fmt.Errorf("open raft node for group %q: %w", groupID, err)
	}

	return &shardGroupRuntime{
		groupID:  groupID,
		db:       db,
		raftNode: node,
		store:    raftnode.NewRaftStore(node, db, raftnode.RaftStoreConfig{}),
	}, nil
}

func newShardRuntimeStore(router *shard.Router, runtimes []*shardGroupRuntime) (grpcserver.Store, error) {
	groups := make(map[shard.GroupID]shard.Store, len(runtimes))
	for _, runtime := range runtimes {
		groups[runtime.groupID] = runtime.store
	}

	store, err := shard.NewShardStore(shard.ShardStoreConfig{
		Router: router,
		Groups: groups,
	})
	if err != nil {
		return nil, fmt.Errorf("create shard store: %w", err)
	}
	return store, nil
}

func closeRuntime(runtime *nodeRuntime) {
	if runtime == nil {
		return
	}
	closeShardGroupRuntimes(runtime.groups)
	if runtime.db != nil {
		if err := runtime.db.Close(); err != nil {
			log.Printf("close db: %v", err)
		}
	}
}

func closeShardGroupRuntimes(groups []*shardGroupRuntime) {
	for i := len(groups) - 1; i >= 0; i-- {
		groups[i].close()
	}
}

func (r *shardGroupRuntime) close() {
	if r == nil {
		return
	}
	if r.raftNode != nil {
		if err := r.raftNode.Close(); err != nil {
			log.Printf("close raft node for group %q: %v", r.groupID, err)
		}
		r.raftNode = nil
	}
	if r.db != nil {
		if err := r.db.Close(); err != nil {
			log.Printf("close db for group %q: %v", r.groupID, err)
		}
		r.db = nil
	}
}

func resolveDataDirs(root string, mode string) dataDirs {
	if mode != modeRaft {
		return dataDirs{engineDir: root}
	}
	return dataDirs{
		engineDir: filepath.Join(root, "engine"),
		raftDir:   filepath.Join(root, "raft"),
	}
}

func resolveGroupDataDirs(root string, groupID shard.GroupID) groupDataDirs {
	groupDir := "group-" + string(groupID)
	return groupDataDirs{
		engineDir: filepath.Join(root, "engine", groupDir),
		raftDir:   filepath.Join(root, "raft", groupDir),
	}
}

func loadClusterConfig(path string) (clusterConfig, error) {
	return cluster.Load(path)
}

package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"

	raftnode "kv_store_demo/internal/raft"
	"kv_store_demo/internal/shard"
)

type Config struct {
	Nodes       []Node  `json:"nodes"`
	ShardConfig Shards  `json:"shard_config"`
	ShardRoutes []Shard `json:"shards"`
	Groups      []Group `json:"groups"`
}

type Node struct {
	ID         string `json:"id"`
	ClientAddr string `json:"client_addr"`
}

type Shards struct {
	ShardCount   int `json:"shard_count"`
	VirtualNodes int `json:"virtual_nodes"`
}

type Shard struct {
	ID      int    `json:"id"`
	GroupID string `json:"group_id"`
}

type Group struct {
	ID       string         `json:"id"`
	Replicas []GroupReplica `json:"replicas"`
}

type GroupReplica struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read cluster config: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return Config{}, fmt.Errorf("decode cluster config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Validate() error {
	if len(c.Nodes) == 0 {
		return errors.New("cluster config nodes is empty")
	}

	seen := make(map[string]struct{}, len(c.Nodes))
	for _, node := range c.Nodes {
		if node.ID == "" {
			return errors.New("cluster node id is empty")
		}
		if node.ClientAddr == "" {
			return fmt.Errorf("cluster node %q client_addr is empty", node.ID)
		}
		if _, ok := seen[node.ID]; ok {
			return fmt.Errorf("duplicate cluster node id %q", node.ID)
		}
		seen[node.ID] = struct{}{}
	}

	if c.ShardConfig.ShardCount <= 0 {
		return errors.New("cluster config shard_config.shard_count is required")
	}
	if c.ShardConfig.VirtualNodes <= 0 {
		return errors.New("cluster config shard_config.virtual_nodes is required")
	}
	if len(c.ShardRoutes) == 0 {
		return errors.New("cluster config shards is empty")
	}
	if len(c.Groups) == 0 {
		return errors.New("cluster config groups is empty")
	}

	seenGroups := make(map[string]struct{}, len(c.Groups))
	for _, group := range c.Groups {
		if group.ID == "" {
			return errors.New("cluster group id is empty")
		}
		if _, ok := seenGroups[group.ID]; ok {
			return fmt.Errorf("duplicate cluster group id %q", group.ID)
		}
		seenGroups[group.ID] = struct{}{}
		if len(group.Replicas) == 0 {
			return fmt.Errorf("cluster group %q replicas is empty", group.ID)
		}

		seenReplicaNodes := make(map[string]struct{}, len(group.Replicas))
		seenReplicaAddrs := make(map[string]struct{}, len(group.Replicas))
		for _, replica := range group.Replicas {
			if replica.NodeID == "" {
				return fmt.Errorf("cluster group %q replica node_id is empty", group.ID)
			}
			if _, ok := seen[replica.NodeID]; !ok {
				return fmt.Errorf("cluster group %q replica node %q not found", group.ID, replica.NodeID)
			}
			if _, ok := seenReplicaNodes[replica.NodeID]; ok {
				return fmt.Errorf("duplicate replica node %q in cluster group %q", replica.NodeID, group.ID)
			}
			seenReplicaNodes[replica.NodeID] = struct{}{}
			if replica.RaftAddr == "" {
				return fmt.Errorf("cluster group %q replica %q raft_addr is empty", group.ID, replica.NodeID)
			}
			if _, ok := seenReplicaAddrs[replica.RaftAddr]; ok {
				return fmt.Errorf("duplicate replica raft_addr %q in cluster group %q", replica.RaftAddr, group.ID)
			}
			seenReplicaAddrs[replica.RaftAddr] = struct{}{}
		}
	}

	seenShards := make(map[int]struct{}, len(c.ShardRoutes))
	for _, s := range c.ShardRoutes {
		if _, ok := seenShards[s.ID]; ok {
			return fmt.Errorf("duplicate cluster shard id %d", s.ID)
		}
		seenShards[s.ID] = struct{}{}
		if _, ok := seenGroups[s.GroupID]; !ok {
			return fmt.Errorf("cluster shard %d group %q not found", s.ID, s.GroupID)
		}
	}

	if _, err := shard.NewRouter(c.RouterConfig()); err != nil {
		return fmt.Errorf("validate shard config: %w", err)
	}
	return nil
}

func (c Config) RouterConfig() shard.RouterConfig {
	shardGroups := make(map[int]shard.GroupID, len(c.ShardRoutes))
	for _, s := range c.ShardRoutes {
		shardGroups[s.ID] = shard.GroupID(s.GroupID)
	}

	return shard.RouterConfig{
		Ring: shard.HashRingConfig{
			ShardCount:   c.ShardConfig.ShardCount,
			VirtualNodes: c.ShardConfig.VirtualNodes,
		},
		ShardGroups: shardGroups,
	}
}

func (c Config) GroupIDs() []shard.GroupID {
	groupIDs := make([]shard.GroupID, 0, len(c.Groups))
	for _, group := range c.Groups {
		groupID := shard.GroupID(group.ID)
		groupIDs = append(groupIDs, groupID)
	}

	sort.Slice(groupIDs, func(i, j int) bool {
		return groupIDs[i] < groupIDs[j]
	})
	return groupIDs
}

func (c Config) NewRouter() (*shard.Router, error) {
	router, err := shard.NewRouter(c.RouterConfig())
	if err != nil {
		return nil, fmt.Errorf("create shard router: %w", err)
	}
	return router, nil
}

func (c Config) FindGroup(id shard.GroupID) (Group, error) {
	for _, group := range c.Groups {
		if shard.GroupID(group.ID) == id {
			return group, nil
		}
	}
	return Group{}, fmt.Errorf("cluster group %q not found", id)
}

func (c Config) LocalGroups(nodeID string) []Group {
	groups := make([]Group, 0, len(c.Groups))
	for _, group := range c.Groups {
		for _, replica := range group.Replicas {
			if replica.NodeID == nodeID {
				groups = append(groups, group)
				break
			}
		}
	}
	return groups
}

func (c Config) RaftPeersForGroup(groupID shard.GroupID) ([]raftnode.Peer, error) {
	group, err := c.FindGroup(groupID)
	if err != nil {
		return nil, err
	}

	peers := make([]raftnode.Peer, 0, len(group.Replicas))
	for _, replica := range group.Replicas {
		peers = append(peers, raftnode.Peer{
			ID:       replica.NodeID,
			RaftAddr: replica.RaftAddr,
		})
	}
	return peers, nil
}

func (c Config) LocalRaftAddr(groupID shard.GroupID, nodeID string) (string, error) {
	group, err := c.FindGroup(groupID)
	if err != nil {
		return "", err
	}

	for _, replica := range group.Replicas {
		if replica.NodeID == nodeID {
			return replica.RaftAddr, nil
		}
	}
	return "", fmt.Errorf("cluster group %q does not contain node %q", groupID, nodeID)
}

func (c Config) SingleGroup() (Group, error) {
	if len(c.Groups) != 1 {
		return Group{}, fmt.Errorf("expected exactly one cluster group, got %d", len(c.Groups))
	}
	return c.Groups[0], nil
}

func (c Config) FindNode(id string) (Node, error) {
	for _, node := range c.Nodes {
		if node.ID == id {
			return node, nil
		}
	}
	return Node{}, fmt.Errorf("cluster node %q not found", id)
}

package client

import (
	"context"
	"fmt"
	"sync"

	"kv_store_demo/internal/cluster"
	"kv_store_demo/internal/shard"
)

type routedDialFunc func(ctx context.Context, target string) (*Client, error)

type RoutedClient struct {
	router       *shard.Router
	nodes        map[string]cluster.Node
	groups       map[shard.GroupID][]string
	raftAddrNode map[shard.GroupID]map[string]string
	clients      map[string]*Client
	leaders      map[shard.GroupID]string
	dial         routedDialFunc
	mu           sync.Mutex
}

func NewRoutedClient(ctx context.Context, config cluster.Config) (*RoutedClient, error) {
	return newRoutedClient(ctx, config, Dial)
}

func newRoutedClient(ctx context.Context, config cluster.Config, dial routedDialFunc) (*RoutedClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dial == nil {
		return nil, fmt.Errorf("routed client dialer is nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	router, err := config.NewRouter()
	if err != nil {
		return nil, err
	}

	nodes := make(map[string]cluster.Node, len(config.Nodes))
	for _, node := range config.Nodes {
		nodes[node.ID] = node
	}

	groups := make(map[shard.GroupID][]string, len(config.Groups))
	raftAddrNode := make(map[shard.GroupID]map[string]string, len(config.Groups))
	for _, group := range config.Groups {
		groupID := shard.GroupID(group.ID)
		replicas := make([]string, 0, len(group.Replicas))
		raftAddrs := make(map[string]string, len(group.Replicas))
		for _, replica := range group.Replicas {
			replicas = append(replicas, replica.NodeID)
			raftAddrs[replica.RaftAddr] = replica.NodeID
		}
		groups[groupID] = replicas
		raftAddrNode[groupID] = raftAddrs
	}

	return &RoutedClient{
		router:       router,
		nodes:        nodes,
		groups:       groups,
		raftAddrNode: raftAddrNode,
		clients:      make(map[string]*Client),
		leaders:      make(map[shard.GroupID]string),
		dial:         dial,
	}, nil
}

func LoadRoutedClient(ctx context.Context, clusterPath string) (*RoutedClient, error) {
	config, err := cluster.Load(clusterPath)
	if err != nil {
		return nil, err
	}
	return NewRoutedClient(ctx, config)
}

func (c *RoutedClient) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var closeErr error
	for nodeID, client := range c.clients {
		if err := client.Close(); err != nil && closeErr == nil {
			closeErr = fmt.Errorf("close client for node %q: %w", nodeID, err)
		}
		delete(c.clients, nodeID)
	}
	return closeErr
}

func (c *RoutedClient) Put(ctx context.Context, key, value []byte) error {
	return c.withKeyClient(ctx, key, func(client *Client) error {
		return client.Put(ctx, key, value)
	})
}

func (c *RoutedClient) Get(ctx context.Context, key []byte) ([]byte, error) {
	var value []byte
	err := c.withKeyClient(ctx, key, func(client *Client) error {
		got, err := client.Get(ctx, key)
		if err != nil {
			return err
		}
		value = got
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (c *RoutedClient) Delete(ctx context.Context, key []byte) error {
	return c.withKeyClient(ctx, key, func(client *Client) error {
		return client.Delete(ctx, key)
	})
}

func (c *RoutedClient) FlushGroup(ctx context.Context, groupID shard.GroupID) error {
	return c.withGroupClient(ctx, groupID, func(client *Client) error {
		return client.Flush(ctx)
	})
}

func (c *RoutedClient) CompactGroup(ctx context.Context, groupID shard.GroupID) error {
	return c.withGroupClient(ctx, groupID, func(client *Client) error {
		return client.Compact(ctx)
	})
}

func (c *RoutedClient) Flush(ctx context.Context) error {
	for groupID := range c.groups {
		if err := c.FlushGroup(ctx, groupID); err != nil {
			return err
		}
	}
	return nil
}

func (c *RoutedClient) Compact(ctx context.Context) error {
	for groupID := range c.groups {
		if err := c.CompactGroup(ctx, groupID); err != nil {
			return err
		}
	}
	return nil
}

func (c *RoutedClient) withKeyClient(ctx context.Context, key []byte, fn func(*Client) error) error {
	if c == nil {
		return fmt.Errorf("routed client is nil")
	}
	route, err := c.router.RouteKey(key)
	if err != nil {
		return err
	}
	return c.withGroupClient(ctx, route.GroupID, fn)
}

func (c *RoutedClient) withGroupClient(ctx context.Context, groupID shard.GroupID, fn func(*Client) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	replicas := c.groups[groupID]
	if len(replicas) == 0 {
		return fmt.Errorf("cluster group %q has no client replicas", groupID)
	}

	attempts := len(replicas) + 1
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		nodeID := c.pickNode(groupID, replicas, attempt)
		client, err := c.clientForNode(ctx, nodeID)
		if err != nil {
			lastErr = err
			c.clearLeader(groupID, nodeID)
			continue
		}

		err = fn(client)
		if err == nil {
			c.setLeader(groupID, nodeID)
			return nil
		}
		lastErr = err
		if IsNotLeader(err) {
			if c.updateLeaderFromHint(groupID, err) {
				continue
			}
			c.clearLeader(groupID, nodeID)
			continue
		}
		return err
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("no client attempt made for group %q", groupID)
}

func (c *RoutedClient) pickNode(groupID shard.GroupID, replicas []string, attempt int) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if attempt == 0 {
		if leader := c.leaders[groupID]; leader != "" {
			return leader
		}
	}
	return replicas[attempt%len(replicas)]
}

func (c *RoutedClient) clientForNode(ctx context.Context, nodeID string) (*Client, error) {
	c.mu.Lock()
	if client := c.clients[nodeID]; client != nil {
		c.mu.Unlock()
		return client, nil
	}
	node, ok := c.nodes[nodeID]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("cluster node %q not found", nodeID)
	}

	client, err := c.dial(ctx, node.ClientAddr)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if existing := c.clients[nodeID]; existing != nil {
		c.mu.Unlock()
		_ = client.Close()
		return existing, nil
	}
	c.clients[nodeID] = client
	c.mu.Unlock()
	return client, nil
}

func (c *RoutedClient) setLeader(groupID shard.GroupID, nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaders[groupID] = nodeID
}

func (c *RoutedClient) clearLeader(groupID shard.GroupID, nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaders[groupID] == nodeID {
		delete(c.leaders, groupID)
	}
}

func (c *RoutedClient) updateLeaderFromHint(groupID shard.GroupID, err error) bool {
	leaderID, leaderAddr, ok := LeaderHint(err)
	if !ok {
		return false
	}

	nodeID := leaderID
	if nodeID == "" && leaderAddr != "" {
		nodeID = c.raftAddrNode[groupID][leaderAddr]
	}
	if nodeID == "" {
		return false
	}
	if _, ok := c.nodes[nodeID]; !ok {
		return false
	}

	c.setLeader(groupID, nodeID)
	return true
}

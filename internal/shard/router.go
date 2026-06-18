package shard

import "errors"

var (
	ErrInvalidShardID   = errors.New("invalid shard id")
	ErrShardNotAssigned = errors.New("shard not assigned")
	ErrEmptyGroupID     = errors.New("empty group id")
)

type GroupID string

type ShardRoute struct {
	ShardID int
	GroupID GroupID
}

type RouterConfig struct {
	Ring        HashRingConfig
	ShardGroups map[int]GroupID
}

type Router struct {
	ring        *HashRing
	shardCount  int
	shardGroups map[int]GroupID
}

func NewRouter(config RouterConfig) (*Router, error) {
	ring, err := NewHashRing(config.Ring)
	if err != nil {
		return nil, err
	}

	shardGroups := make(map[int]GroupID, len(config.ShardGroups))
	for shardID, groupID := range config.ShardGroups {
		if shardID < 0 || shardID >= config.Ring.ShardCount {
			return nil, ErrInvalidShardID
		}
		if groupID == "" {
			return nil, ErrEmptyGroupID
		}
		shardGroups[shardID] = groupID
	}

	for shardID := 0; shardID < config.Ring.ShardCount; shardID++ {
		if _, ok := shardGroups[shardID]; !ok {
			return nil, ErrShardNotAssigned
		}
	}

	return &Router{
		ring:        ring,
		shardCount:  config.Ring.ShardCount,
		shardGroups: shardGroups,
	}, nil
}

func (r *Router) RouteKey(key []byte) (ShardRoute, error) {
	if r == nil {
		return ShardRoute{}, ErrShardNotAssigned
	}

	shardID, err := r.ring.PickShard(key)
	if err != nil {
		return ShardRoute{}, err
	}

	return r.RouteShard(shardID)
}

func (r *Router) RouteShard(shardID int) (ShardRoute, error) {
	if r == nil {
		return ShardRoute{}, ErrShardNotAssigned
	}
	if shardID < 0 || shardID >= r.shardCount {
		return ShardRoute{}, ErrInvalidShardID
	}

	groupID, ok := r.shardGroups[shardID]
	if !ok {
		return ShardRoute{}, ErrShardNotAssigned
	}

	return ShardRoute{
		ShardID: shardID,
		GroupID: groupID,
	}, nil
}

package shard

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"

	"kv_store_demo/internal/platform/kv_errors"
)

var (
	ErrInvalidShardCount   = errors.New("invalid shard count")
	ErrInvalidVirtualNodes = errors.New("invalid virtual nodes")
	ErrEmptyRing           = errors.New("empty hash ring")
)

type HashRingConfig struct {
	ShardCount   int
	VirtualNodes int
}

type HashRing struct {
	points []ringPoint
}

type ringPoint struct {
	hash    uint32
	shardID int
}

func NewHashRing(config HashRingConfig) (*HashRing, error) {
	if config.ShardCount <= 0 {
		return nil, ErrInvalidShardCount
	}
	if config.VirtualNodes <= 0 {
		return nil, ErrInvalidVirtualNodes
	}

	points := make([]ringPoint, 0, config.ShardCount*config.VirtualNodes)
	for shardID := 0; shardID < config.ShardCount; shardID++ {
		for vnodeID := 0; vnodeID < config.VirtualNodes; vnodeID++ {
			nodeKey := fmt.Sprintf("shard-%d#%d", shardID, vnodeID)
			points = append(points, ringPoint{
				hash:    hashBytes([]byte(nodeKey)),
				shardID: shardID,
			})
		}
	}

	sort.Slice(points, func(i, j int) bool {
		if points[i].hash == points[j].hash {
			return points[i].shardID < points[j].shardID
		}
		return points[i].hash < points[j].hash
	})

	return &HashRing{points: points}, nil
}

func (r *HashRing) PickShard(key []byte) (int, error) {
	if len(key) == 0 {
		return 0, kv_errors.ErrEmptyKey
	}
	if r == nil || len(r.points) == 0 {
		return 0, ErrEmptyRing
	}

	keyHash := hashBytes(key)
	idx := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= keyHash
	})
	if idx == len(r.points) {
		idx = 0
	}

	return r.points[idx].shardID, nil
}

func hashBytes(data []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(data)
	return h.Sum32()
}

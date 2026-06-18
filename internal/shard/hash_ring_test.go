package shard

import (
	"errors"
	"fmt"
	"testing"

	"kv_store_demo/internal/platform/kv_errors"
)

func TestNewHashRingRejectsInvalidShardCount(t *testing.T) {
	_, err := NewHashRing(HashRingConfig{
		ShardCount:   0,
		VirtualNodes: 8,
	})
	if !errors.Is(err, ErrInvalidShardCount) {
		t.Fatalf("expected ErrInvalidShardCount, got %v", err)
	}
}

func TestNewHashRingRejectsInvalidVirtualNode(t *testing.T) {
	_, err := NewHashRing(HashRingConfig{
		ShardCount:   4,
		VirtualNodes: 0,
	})
	if !errors.Is(err, ErrInvalidVirtualNodes) {
		t.Fatalf("expected ErrInvalidVirtualNodes, got %v", err)
	}
}

func TestHashRingPickShardStableForSameKey(t *testing.T) {
	ring, err := NewHashRing(HashRingConfig{
		ShardCount:   16,
		VirtualNodes: 32,
	})
	if err != nil {
		t.Fatalf("new hash ring: %v", err)
	}

	first, err := ring.PickShard([]byte("user:1001"))
	if err != nil {
		t.Fatalf("pick shard: %v", err)
	}

	for i := 0; i < 100; i++ {
		got, err := ring.PickShard([]byte("user:1001"))
		if err != nil {
			t.Fatalf("pick shard: %v", err)
		}
		if got != first {
			t.Fatalf("expected stable shard %d, got %d", first, got)
		}
	}
}

func TestHashRingPickShardInRange(t *testing.T) {
	const shardCount = 16
	ring, err := NewHashRing(HashRingConfig{
		ShardCount:   shardCount,
		VirtualNodes: 32,
	})
	if err != nil {
		t.Fatalf("new hash ring: %v", err)
	}

	for i := 0; i < 1000; i++ {
		shardID, err := ring.PickShard([]byte(fmt.Sprintf("key-%d", i)))
		if err != nil {
			t.Fatalf("pick shard: %v", err)
		}
		if shardID < 0 || shardID >= shardCount {
			t.Fatalf("shard id out of range: %d", shardID)
		}
	}
}

func TestHashRingRejectsEmptyKey(t *testing.T) {
	ring, err := NewHashRing(HashRingConfig{
		ShardCount:   4,
		VirtualNodes: 8,
	})
	if err != nil {
		t.Fatalf("new hash ring: %v", err)
	}

	_, err = ring.PickShard(nil)
	if !errors.Is(err, kv_errors.ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey, got %v", err)
	}
}

func TestHashRingRejectsEmptyRing(t *testing.T) {
	var ring HashRing

	_, err := ring.PickShard([]byte("key"))
	if !errors.Is(err, ErrEmptyRing) {
		t.Fatalf("expected ErrEmptyRing, got %v", err)
	}
}

func TestHashRingDistributesKeysToMultipleShards(t *testing.T) {
	const shardCount = 16
	ring, err := NewHashRing(HashRingConfig{
		ShardCount:   shardCount,
		VirtualNodes: 32,
	})
	if err != nil {
		t.Fatalf("new hash ring: %v", err)
	}

	used := make(map[int]struct{})
	for i := 0; i < 1000; i++ {
		shardID, err := ring.PickShard([]byte(fmt.Sprintf("key-%d", i)))
		if err != nil {
			t.Fatalf("pick shard: %v", err)
		}
		used[shardID] = struct{}{}
	}

	if len(used) < shardCount/2 {
		t.Fatalf("expected keys to spread across multiple shards, got %d shards", len(used))
	}
}

func TestHashRingSameConfigKeepsRouteStable(t *testing.T) {
	config := HashRingConfig{
		ShardCount:   16,
		VirtualNodes: 32,
	}

	ringA, err := NewHashRing(config)
	if err != nil {
		t.Fatalf("new hash ring a: %v", err)
	}
	ringB, err := NewHashRing(config)
	if err != nil {
		t.Fatalf("new hash ring b: %v", err)
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))

		shardA, err := ringA.PickShard(key)
		if err != nil {
			t.Fatalf("pick shard a: %v", err)
		}
		shardB, err := ringB.PickShard(key)
		if err != nil {
			t.Fatalf("pick shard b: %v", err)
		}
		if shardA != shardB {
			t.Fatalf("expected same route for %q, got %d and %d", key, shardA, shardB)
		}
	}
}

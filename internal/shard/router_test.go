package shard

import (
	"errors"
	"testing"

	"kv_store_demo/internal/platform/kv_errors"
)

func TestRouterRouteShard(t *testing.T) {
	router := newTestRouter(t)

	route, err := router.RouteShard(2)
	if err != nil {
		t.Fatalf("route shard: %v", err)
	}
	if route.ShardID != 2 {
		t.Fatalf("expected shard 2, got %d", route.ShardID)
	}
	if route.GroupID != GroupID("g2") {
		t.Fatalf("expected group g2, got %q", route.GroupID)
	}
}

func TestRouterRouteKeyStable(t *testing.T) {
	router := newTestRouter(t)

	first, err := router.RouteKey([]byte("user:1001"))
	if err != nil {
		t.Fatalf("route key: %v", err)
	}

	for i := 0; i < 100; i++ {
		got, err := router.RouteKey([]byte("user:1001"))
		if err != nil {
			t.Fatalf("route key: %v", err)
		}
		if got != first {
			t.Fatalf("expected stable route %+v, got %+v", first, got)
		}
	}
}

func TestRouterRouteKeyRejectsEmptyKey(t *testing.T) {
	router := newTestRouter(t)

	_, err := router.RouteKey(nil)
	if !errors.Is(err, kv_errors.ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey, got %v", err)
	}
}

func TestNewRouterRejectsInvalidShardID(t *testing.T) {
	_, err := NewRouter(RouterConfig{
		Ring: HashRingConfig{
			ShardCount:   4,
			VirtualNodes: 8,
		},
		ShardGroups: map[int]GroupID{
			0: "g1",
			1: "g1",
			2: "g2",
			3: "g2",
			4: "g3",
		},
	})
	if !errors.Is(err, ErrInvalidShardID) {
		t.Fatalf("expected ErrInvalidShardID, got %v", err)
	}
}

func TestNewRouterRejectsEmptyGroupID(t *testing.T) {
	_, err := NewRouter(RouterConfig{
		Ring: HashRingConfig{
			ShardCount:   4,
			VirtualNodes: 8,
		},
		ShardGroups: map[int]GroupID{
			0: "g1",
			1: "g1",
			2: "",
			3: "g2",
		},
	})
	if !errors.Is(err, ErrEmptyGroupID) {
		t.Fatalf("expected ErrEmptyGroupID, got %v", err)
	}
}

func TestNewRouterRejectsUnassignedShard(t *testing.T) {
	_, err := NewRouter(RouterConfig{
		Ring: HashRingConfig{
			ShardCount:   4,
			VirtualNodes: 8,
		},
		ShardGroups: map[int]GroupID{
			0: "g1",
			1: "g1",
			3: "g2",
		},
	})
	if !errors.Is(err, ErrShardNotAssigned) {
		t.Fatalf("expected ErrShardNotAssigned, got %v", err)
	}
}

func TestRouterRouteShardRejectsInvalidShardID(t *testing.T) {
	router := newTestRouter(t)

	_, err := router.RouteShard(4)
	if !errors.Is(err, ErrInvalidShardID) {
		t.Fatalf("expected ErrInvalidShardID, got %v", err)
	}
}

func TestRouterSameConfigKeepsRouteStable(t *testing.T) {
	routerA := newTestRouter(t)
	routerB := newTestRouter(t)

	for i := 0; i < 100; i++ {
		key := []byte("stable-key")
		routeA, err := routerA.RouteKey(key)
		if err != nil {
			t.Fatalf("route key a: %v", err)
		}
		routeB, err := routerB.RouteKey(key)
		if err != nil {
			t.Fatalf("route key b: %v", err)
		}
		if routeA != routeB {
			t.Fatalf("expected same route, got %+v and %+v", routeA, routeB)
		}
	}
}

func newTestRouter(t *testing.T) *Router {
	t.Helper()

	router, err := NewRouter(RouterConfig{
		Ring: HashRingConfig{
			ShardCount:   4,
			VirtualNodes: 8,
		},
		ShardGroups: map[int]GroupID{
			0: "g1",
			1: "g1",
			2: "g2",
			3: "g2",
		},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	return router
}

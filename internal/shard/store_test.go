package shard

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"kv_store_demo/internal/platform/kv_errors"
)

func TestShardStorePutRoutesToGroup(t *testing.T) {
	store, groups, router := newTestShardStore(t)
	key := keyForGroup(t, router, "g2")

	if err := store.Put(context.Background(), key, []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if groups["g1"].puts != 0 {
		t.Fatalf("g1 puts = %d, want 0", groups["g1"].puts)
	}
	if groups["g2"].puts != 1 {
		t.Fatalf("g2 puts = %d, want 1", groups["g2"].puts)
	}
	if got := string(groups["g2"].data[string(key)]); got != "alice" {
		t.Fatalf("g2 stored value = %q, want alice", got)
	}
}

func TestShardStoreGetRoutesToGroup(t *testing.T) {
	store, groups, router := newTestShardStore(t)
	key := keyForGroup(t, router, "g1")
	groups["g1"].data[string(key)] = []byte("bob")
	groups["g2"].data[string(key)] = []byte("wrong")

	value, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if string(value) != "bob" {
		t.Fatalf("Get() = %q, want bob", value)
	}
	if groups["g1"].gets != 1 {
		t.Fatalf("g1 gets = %d, want 1", groups["g1"].gets)
	}
	if groups["g2"].gets != 0 {
		t.Fatalf("g2 gets = %d, want 0", groups["g2"].gets)
	}
}

func TestShardStoreDeleteRoutesToGroup(t *testing.T) {
	store, groups, router := newTestShardStore(t)
	key := keyForGroup(t, router, "g2")
	groups["g2"].data[string(key)] = []byte("alice")

	if err := store.Delete(context.Background(), key); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if groups["g1"].deletes != 0 {
		t.Fatalf("g1 deletes = %d, want 0", groups["g1"].deletes)
	}
	if groups["g2"].deletes != 1 {
		t.Fatalf("g2 deletes = %d, want 1", groups["g2"].deletes)
	}
	if _, ok := groups["g2"].data[string(key)]; ok {
		t.Fatal("g2 key still exists after Delete()")
	}
}

func TestShardStoreRejectsEmptyKey(t *testing.T) {
	store, _, _ := newTestShardStore(t)

	err := store.Put(context.Background(), nil, []byte("value"))
	if !errors.Is(err, kv_errors.ErrEmptyKey) {
		t.Fatalf("Put(empty key) error = %v, want ErrEmptyKey", err)
	}
}

func TestShardStoreReturnsGroupNotFound(t *testing.T) {
	router := newStoreTestRouter(t)
	key := keyForGroup(t, router, "g2")
	group := newFakeStore()

	store, err := NewShardStore(ShardStoreConfig{
		Router: router,
		Groups: map[GroupID]Store{
			"g1": group,
		},
	})
	if err != nil {
		t.Fatalf("NewShardStore() error = %v", err)
	}

	_, err = store.Get(context.Background(), key)
	if !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("Get() error = %v, want ErrGroupNotFound", err)
	}
}

func TestNewShardStoreRejectsInvalidConfig(t *testing.T) {
	router := newStoreTestRouter(t)

	tests := []struct {
		name   string
		config ShardStoreConfig
		want   error
	}{
		{
			name: "nil router",
			config: ShardStoreConfig{
				Groups: map[GroupID]Store{"g1": newFakeStore()},
			},
			want: ErrNilRouter,
		},
		{
			name: "no groups",
			config: ShardStoreConfig{
				Router: router,
			},
			want: ErrNoShardGroup,
		},
		{
			name: "empty group id",
			config: ShardStoreConfig{
				Router: router,
				Groups: map[GroupID]Store{"": newFakeStore()},
			},
			want: ErrEmptyGroupID,
		},
		{
			name: "nil group store",
			config: ShardStoreConfig{
				Router: router,
				Groups: map[GroupID]Store{"g1": nil},
			},
			want: ErrGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewShardStore(tt.config)
			if !errors.Is(err, tt.want) {
				t.Fatalf("NewShardStore() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestShardStoreFlushCallsAllGroups(t *testing.T) {
	store, groups, _ := newTestShardStore(t)

	if err := store.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if groups["g1"].flushes != 1 {
		t.Fatalf("g1 flushes = %d, want 1", groups["g1"].flushes)
	}
	if groups["g2"].flushes != 1 {
		t.Fatalf("g2 flushes = %d, want 1", groups["g2"].flushes)
	}
}

func TestShardStoreCompactCallsAllGroups(t *testing.T) {
	store, groups, _ := newTestShardStore(t)

	if err := store.Compact(context.Background()); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if groups["g1"].compacts != 1 {
		t.Fatalf("g1 compacts = %d, want 1", groups["g1"].compacts)
	}
	if groups["g2"].compacts != 1 {
		t.Fatalf("g2 compacts = %d, want 1", groups["g2"].compacts)
	}
}

func newTestShardStore(t *testing.T) (*ShardStore, map[GroupID]*fakeStore, *Router) {
	t.Helper()

	router := newStoreTestRouter(t)
	groups := map[GroupID]*fakeStore{
		"g1": newFakeStore(),
		"g2": newFakeStore(),
	}
	store, err := NewShardStore(ShardStoreConfig{
		Router: router,
		Groups: map[GroupID]Store{
			"g1": groups["g1"],
			"g2": groups["g2"],
		},
	})
	if err != nil {
		t.Fatalf("NewShardStore() error = %v", err)
	}

	return store, groups, router
}

func newStoreTestRouter(t *testing.T) *Router {
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
		t.Fatalf("NewRouter() error = %v", err)
	}

	return router
}

func keyForGroup(t *testing.T, router *Router, groupID GroupID) []byte {
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

type fakeStore struct {
	puts     int
	gets     int
	deletes  int
	flushes  int
	compacts int
	data     map[string][]byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{data: make(map[string][]byte)}
}

func (s *fakeStore) Put(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.puts++
	s.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (s *fakeStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.gets++
	return append([]byte(nil), s.data[string(key)]...), nil
}

func (s *fakeStore) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.deletes++
	delete(s.data, string(key))
	return nil
}

func (s *fakeStore) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.flushes++
	return nil
}

func (s *fakeStore) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.compacts++
	return nil
}

package shard

import (
	"context"
	"errors"
)

var (
	ErrNilRouter     = errors.New("nil shard router")
	ErrNoShardGroup  = errors.New("no shard group")
	ErrGroupNotFound = errors.New("shard group not found")
)

type Store interface {
	Put(ctx context.Context, key, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Delete(ctx context.Context, key []byte) error
	Flush(ctx context.Context) error
	Compact(ctx context.Context) error
}

type ShardStoreConfig struct {
	Router *Router
	Groups map[GroupID]Store
}

type ShardStore struct {
	router *Router
	groups map[GroupID]Store
}

func NewShardStore(config ShardStoreConfig) (*ShardStore, error) {
	if config.Router == nil {
		return nil, ErrNilRouter
	}
	if len(config.Groups) == 0 {
		return nil, ErrNoShardGroup
	}

	groups := make(map[GroupID]Store, len(config.Groups))
	for groupID, store := range config.Groups {
		if groupID == "" {
			return nil, ErrEmptyGroupID
		}
		if store == nil {
			return nil, ErrGroupNotFound
		}
		groups[groupID] = store
	}

	return &ShardStore{
		router: config.Router,
		groups: groups,
	}, nil
}

func (s *ShardStore) Put(ctx context.Context, key, value []byte) error {
	store, _, err := s.route(key)
	if err != nil {
		return err
	}
	return store.Put(ctx, key, value)
}

func (s *ShardStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	store, _, err := s.route(key)
	if err != nil {
		return nil, err
	}
	return store.Get(ctx, key)
}

func (s *ShardStore) Delete(ctx context.Context, key []byte) error {
	store, _, err := s.route(key)
	if err != nil {
		return err
	}
	return store.Delete(ctx, key)
}

func (s *ShardStore) Flush(ctx context.Context) error {
	if s == nil {
		return ErrNoShardGroup
	}
	for _, store := range s.groups {
		if err := store.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardStore) Compact(ctx context.Context) error {
	if s == nil {
		return ErrNoShardGroup
	}
	for _, store := range s.groups {
		if err := store.Compact(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardStore) route(key []byte) (Store, ShardRoute, error) {
	if s == nil {
		return nil, ShardRoute{}, ErrNoShardGroup
	}

	route, err := s.router.RouteKey(key)
	if err != nil {
		return nil, ShardRoute{}, err
	}

	store, ok := s.groups[route.GroupID]
	if !ok {
		return nil, ShardRoute{}, ErrGroupNotFound
	}
	return store, route, nil
}

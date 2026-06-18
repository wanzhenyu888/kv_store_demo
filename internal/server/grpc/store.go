package grpcserver

import (
	"context"

	kv "kv_store_demo"
)

type Store interface {
	Put(ctx context.Context, key, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Delete(ctx context.Context, key []byte) error
	Flush(ctx context.Context) error
	Compact(ctx context.Context) error
}

type standaloneStore struct {
	db *kv.DB
}

func NewStandaloneStore(db *kv.DB) Store {
	return &standaloneStore{db: db}
}

func (s *standaloneStore) Put(ctx context.Context, key, value []byte) error {
	return s.db.Put(key, value)
}

func (s *standaloneStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	return s.db.Get(key)
}

func (s *standaloneStore) Delete(ctx context.Context, key []byte) error {
	return s.db.Delete(key)
}

func (s *standaloneStore) Flush(ctx context.Context) error {
	return s.db.Flush()
}

func (s *standaloneStore) Compact(ctx context.Context) error {
	return s.db.Compact()
}

package kv

import "sync"

type DB struct {
	mu      sync.RWMutex
	options Options
	closed  bool
}

func Open(options Options) (*DB, error) {
	return &DB{
		options: normalizeOptions(options),
	}, nil
}

func (db *DB) Put(key, value []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	return ErrNotImplemented
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	return nil, ErrNotImplemented
}

func (db *DB) Delete(key []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	return ErrNotImplemented
}

func (db *DB) Flush() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	return ErrNotImplemented
}

func (db *DB) Compact() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	return ErrNotImplemented
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.closed = true
	return nil
}

func (db *DB) ensureOpen() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrClosed
	}
	return nil
}

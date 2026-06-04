package sstable

import (
	"errors"

	"kv_store_demo/internal/record"
)

var ErrNotImplemented = errors.New("sstable: not implemented")

type SSTable struct {
	id    int
	path  string
	index map[string]int64
}

func Create(path string, records []record.Record) (*SSTable, error) {
	return nil, ErrNotImplemented
}

func Open(id int, path string) (*SSTable, error) {
	return nil, ErrNotImplemented
}

func (s *SSTable) Get(key []byte) (record.Record, bool, error) {
	return record.Record{}, false, ErrNotImplemented
}

func (s *SSTable) Records() ([]record.Record, error) {
	return nil, ErrNotImplemented
}

func (s *SSTable) Close() error {
	return ErrNotImplemented
}

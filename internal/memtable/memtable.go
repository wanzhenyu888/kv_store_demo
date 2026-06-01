package memtable

import (
	"errors"

	"kv_store_demo/internal/record"
)

var ErrNotImplemented = errors.New("memtable: not implemented")

type MemTable struct {
	records map[string]record.Record
	size    int
}

func New() *MemTable {
	return &MemTable{
		records: make(map[string]record.Record),
	}
}

func (m *MemTable) Put(key, value []byte) error {
	return ErrNotImplemented
}

func (m *MemTable) Get(key []byte) (record.Record, bool) {
	return record.Record{}, false
}

func (m *MemTable) Delete(key []byte) error {
	return ErrNotImplemented
}

func (m *MemTable) Size() int {
	return m.size
}

func (m *MemTable) Records() []record.Record {
	return nil
}

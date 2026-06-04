package wal

import (
	"errors"

	"kv_store_demo/internal/record"
)

var ErrNotImplemented = errors.New("wal: not implemented")

type WAL struct {
	path string
}

func Open(path string) (*WAL, error) {
	return nil, ErrNotImplemented
}

func (w *WAL) Append(record record.Record) error {
	return ErrNotImplemented
}

func (w *WAL) Replay() ([]record.Record, error) {
	return nil, ErrNotImplemented
}

func (w *WAL) Reset() error {
	return ErrNotImplemented
}

func (w *WAL) Close() error {
	return ErrNotImplemented
}

package kv

import "errors"

var (
	ErrNotFound       = errors.New("kv: key not found")
	ErrClosed         = errors.New("kv: db is closed")
	ErrNotImplemented = errors.New("kv: not implemented")
)

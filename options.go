package kv

import (
	"log/slog"
	"path/filepath"
)

const DefaultMemTableSize = 4 * 1024 * 1024

type Options struct {
	Dir          string
	sstDir       string
	MemTableSize uint64
	Logger       *slog.Logger
}

func normalizeOptions(options Options) Options {
	if options.MemTableSize <= 0 {
		options.MemTableSize = DefaultMemTableSize
	}

	options.sstDir = filepath.Join(options.Dir, "./sst/")
	return options
}

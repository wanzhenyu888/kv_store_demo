package kv

import (
	"io"
	"log/slog"
)

const DefaultMemTableSize = 4 * 1024 * 1024

type Options struct {
	Dir          string
	MemTableSize int
	Logger	     *slog.Logger
}

func normalizeOptions(options Options) Options {
	if options.MemTableSize <= 0 {
		options.MemTableSize = DefaultMemTableSize
	}

	if options.Logger == nil {
		options.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	
	return options
}

package kv

const defaultMemTableSize = 4 * 1024 * 1024

type Options struct {
	Dir          string
	MemTableSize int
}

func normalizeOptions(options Options) Options {
	if options.MemTableSize <= 0 {
		options.MemTableSize = defaultMemTableSize
	}
	return options
}

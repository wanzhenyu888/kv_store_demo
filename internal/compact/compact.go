package compact

import (
	"errors"

	"kv_store_demo/internal/sstable"
)

var ErrNotImplemented = errors.New("compact: not implemented")

func Run(sstables []*sstable.SSTable, outputPath string) (*sstable.SSTable, error) {
	return nil, ErrNotImplemented
}

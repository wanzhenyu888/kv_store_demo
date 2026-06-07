package utils

import (
	"os"
	"path/filepath"

	"kv_store_demo/internal/kv_errors"
)

func OpenFileWithFlagMode(path string, flag int, perm os.FileMode) (*os.File, error){
	if path == "" {
		return nil, kv_errors.ErrInvalidPara
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}

	return f, nil
}
package infra

import (
	"os"
	"path/filepath"
	"strings"

	"kv_store_demo/infra/kv_errors"
)

func OpenFileWithFlagMode(path string, flag int, perm os.FileMode) (*os.File, error) {
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

func IsValidDir(dir string) bool {
	if dir == "" {
		return false
	}

	dir = filepath.Clean(dir)
	if !strings.ContainsRune(dir, '\x00') {
		return false
	}

	return true
}

func CreateDir(dir string) error {
	if IsValidDir(dir) {
		return kv_errors.ErrInvalidPara
	}

	return os.MkdirAll(dir, 0755)
}

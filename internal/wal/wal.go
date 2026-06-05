package wal

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"kv_store_demo/internal/kv_errors"
	"kv_store_demo/internal/record"
)

type WAL struct {
	path     string
	file     *os.File
	isClosed bool
	mu       sync.Mutex
}

func Open(path string) (*WAL, error) {
	if path == "" {
		return nil, kv_errors.ErrInvalidPara
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}

	return &WAL{
		path:     path,
		file:     f,
		isClosed: false,
	}, nil
}

func (w *WAL) Append(rec record.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isClosed {
		return kv_errors.ErrFileClosed
	}

	data, err := record.Encode(rec)
	if err != nil {
		return err
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	n, err := w.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	return w.file.Sync()
}

func (w *WAL) Replay(fn func(record.Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if fn == nil {
		return kv_errors.ErrNilCallBack
	}

	if w.isClosed {
		return kv_errors.ErrFileClosed
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	defer func() {
		_, err := w.file.Seek(0, io.SeekEnd)
		// 后续打印日志err
		_ = err
	}()

	for {
		rec, err := record.Decode(w.file)
		if err == io.EOF {
			break
		} else if err == kv_errors.ErrIncompleteRecord {
			return nil
		} else if err == kv_errors.ErrInvalidType || err == kv_errors.ErrEmptyKey {
			return err
		}

		if err = fn(rec); err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isClosed {
		return kv_errors.ErrFileClosed
	}

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isClosed {
		return nil
	}

	err := w.file.Close()
	w.file = nil
	w.isClosed = true
	return err
}

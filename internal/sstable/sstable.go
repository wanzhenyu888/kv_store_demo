package sstable

import (
	"io"
	"os"
	"slices"

	"kv_store_demo/infra"
	"kv_store_demo/infra/kv_errors"
	"kv_store_demo/internal/record"
)

type SSTable struct {
	id     int
	path   string
	file   *os.File
	index  map[string]int64
	closed bool
}

type SSTableWriter struct {
	file   *os.File
	closed bool
}

type Iterator struct {
	file   *os.File
	cur    record.Record
	valid  bool
	err    error
	closed bool
}

func CreateSSTableWriter(path string) (*SSTableWriter, error) {
	f, err := infra.OpenFileWithFlagMode(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	writer := SSTableWriter{
		file:   f,
		closed: false,
	}

	return &writer, nil
}

func (w *SSTableWriter) Append(rec record.Record) error {
	if w.closed {
		return kv_errors.ErrFileClosed
	}

	data, err := record.Encode(rec)
	if err != nil {
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

func (w *SSTableWriter) Close() error {
	if w.closed {
		return nil
	}

	err := w.file.Close()
	w.file = nil
	w.closed = true
	return err
}

func Open(id int, path string) (*SSTable, error) {
	if path == "" {
		return nil, kv_errors.ErrInvalidPara
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	offset := int64(0)
	index := make(map[string]int64)
	for {
		rec, err := record.Decode(f)
		if err == io.EOF {
			break
		} else if err == kv_errors.ErrIncompleteRecord {
			_ = f.Close()
			return nil, err
		} else if err == kv_errors.ErrInvalidType || err == kv_errors.ErrEmptyKey {
			_ = f.Close()
			return nil, err
		}

		index[string(rec.Key)] = offset
		offset += int64(record.RecordHeaderSize + len(rec.Key) + len(rec.Value))
	}

	return &SSTable{
		id:     id,
		path:   path,
		file:   f,
		index:  index,
		closed: false,
	}, nil
}

func (s *SSTable) Get(key []byte) (record.Record, bool, error) {
	if s.closed {
		return record.Record{}, false, kv_errors.ErrFileClosed
	}

	if len(key) == 0 {
		return record.Record{}, false, kv_errors.ErrInvalidPara
	}

	offset, ok := s.index[string(key)]
	if !ok {
		return record.Record{}, false, nil
	}

	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return record.Record{}, false, err
	}
	defer func() {
		_, err := s.file.Seek(0, io.SeekEnd)
		// 后续打印日志err
		_ = err
	}()

	rec, err := record.Decode(s.file)
	if err != nil {
		return record.Record{}, false, err
	}
	return rec, true, nil
}

func (s *SSTable) ForEachRecord(fn func(record.Record) error) error {
	if fn == nil {
		return kv_errors.ErrNilCallBack
	}

	keysList := make([]string, 0, len(s.index))
	for key := range s.index {
		keysList = append(keysList, key)
	}
	slices.Sort(keysList)

	for _, key := range keysList {
		rec, _, err := s.Get([]byte(key))
		if err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

func (s *SSTable) Path() string {
	return s.path
}

func (s *SSTable) NewIterator() (*Iterator, error) {
	if s.closed {
		return nil, kv_errors.ErrFileClosed
	}

	f, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}

	it := &Iterator{
		file: f,
	}
	it.Next()
	return it, nil
}

func (it *Iterator) Valid() bool {
	return it != nil && !it.closed && it.err == nil && it.valid
}

func (it *Iterator) Record() record.Record {
	if it == nil {
		return record.Record{}
	}
	return it.cur
}

func (it *Iterator) Next() {
	if it == nil || it.closed || it.err != nil {
		return
	}

	rec, err := record.Decode(it.file)
	if err == io.EOF {
		it.valid = false
		return
	}
	if err != nil {
		it.err = err
		it.valid = false
		return
	}

	it.cur = rec
	it.valid = true
}

func (it *Iterator) Err() error {
	if it == nil {
		return nil
	}
	return it.err
}

func (it *Iterator) Close() error {
	if it == nil || it.closed {
		return nil
	}

	err := it.file.Close()
	it.file = nil
	it.valid = false
	it.closed = true
	return err
}

func (s *SSTable) Close() error {
	if s.closed {
		return nil
	}

	err := s.file.Close()
	s.id = 0
	s.path = ""
	s.file = nil
	s.closed = true
	return err
}

package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"kv_store_demo/infra/kv_errors"
	"kv_store_demo/internal/record"
)

func walPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "nested", "wal.log")
}

func collectReplay(t *testing.T, w *WAL) []record.Record {
	t.Helper()

	var got []record.Record
	if err := w.Replay(func(r record.Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	return got
}

func sameRecord(a, b record.Record) bool {
	return a.Type == b.Type &&
		bytes.Equal(a.Key, b.Key) &&
		bytes.Equal(a.Value, b.Value)
}

func TestOpenCreatesWALFile(t *testing.T) {
	path := walPath(t)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer w.Close()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("WAL file was not created: %v", err)
	}
}

func TestOpenRejectsEmptyPath(t *testing.T) {
	w, err := Open("")
	if !errors.Is(err, kv_errors.ErrInvalidPara) {
		t.Fatalf("Open(\"\") error = %v, want ErrInvalidPara", err)
	}
	if w != nil {
		t.Fatalf("Open(\"\") WAL = %+v, want nil", w)
	}
}

func TestAppendAndReplaySinglePut(t *testing.T) {
	w, err := Open(walPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer w.Close()

	want := record.Record{Type: record.TypePut, Key: []byte("name"), Value: []byte("alice")}
	if err := w.Append(want); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	got := collectReplay(t, w)
	if len(got) != 1 || !sameRecord(got[0], want) {
		t.Fatalf("Replay() = %+v, want [%+v]", got, want)
	}
}

func TestAppendAndReplayMultipleRecordsInOrder(t *testing.T) {
	w, err := Open(walPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer w.Close()

	want := []record.Record{
		{Type: record.TypePut, Key: []byte("a"), Value: []byte("1")},
		{Type: record.TypePut, Key: []byte("b"), Value: []byte("2")},
		{Type: record.TypeDelete, Key: []byte("a")},
	}
	for _, r := range want {
		if err := w.Append(r); err != nil {
			t.Fatalf("Append(%+v) error = %v", r, err)
		}
	}

	got := collectReplay(t, w)
	if len(got) != len(want) {
		t.Fatalf("Replay() len = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if !sameRecord(got[i], want[i]) {
			t.Fatalf("Replay()[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestAppendPropagatesRecordValidationError(t *testing.T) {
	w, err := Open(walPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer w.Close()

	err = w.Append(record.Record{Type: record.TypePut})
	if !errors.Is(err, kv_errors.ErrEmptyKey) {
		t.Fatalf("Append() error = %v, want ErrEmptyKey", err)
	}

	err = w.Append(record.Record{Type: record.TypeDelete, Key: []byte("name"), Value: []byte("alice")})
	if !errors.Is(err, kv_errors.ErrUnexpectedValue) {
		t.Fatalf("Append() error = %v, want ErrUnexpectedValue", err)
	}
}

func TestReplayRejectsNilCallback(t *testing.T) {
	w, err := Open(walPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer w.Close()

	err = w.Replay(nil)
	if !errors.Is(err, kv_errors.ErrNilCallBack) {
		t.Fatalf("Replay(nil) error = %v, want ErrNilCallBack", err)
	}
}

func TestReplayStopsAndReturnsCallbackError(t *testing.T) {
	w, err := Open(walPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer w.Close()

	for _, key := range []string{"a", "b", "c"} {
		if err := w.Append(record.Record{Type: record.TypePut, Key: []byte(key), Value: []byte(key)}); err != nil {
			t.Fatalf("Append(%q) error = %v", key, err)
		}
	}

	wantErr := errors.New("stop")
	var visited []string
	err = w.Replay(func(r record.Record) error {
		visited = append(visited, string(r.Key))
		if string(r.Key) == "b" {
			return wantErr
		}
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Replay() error = %v, want %v", err, wantErr)
	}
	if !reflect.DeepEqual(visited, []string{"a", "b"}) {
		t.Fatalf("visited = %v, want [a b]", visited)
	}
}

func TestReplayIgnoresTrailingIncompleteRecord(t *testing.T) {
	path := walPath(t)
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer w.Close()

	want := record.Record{Type: record.TypePut, Key: []byte("name"), Value: []byte("alice")}
	if err := w.Append(want); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("OpenFile append tail error = %v", err)
	}
	if _, err := f.Write([]byte{record.TypePut, 0, 0}); err != nil {
		_ = f.Close()
		t.Fatalf("Write incomplete tail error = %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close tail writer error = %v", err)
	}

	got := collectReplay(t, w)
	if len(got) != 1 || !sameRecord(got[0], want) {
		t.Fatalf("Replay() = %+v, want only complete record %+v", got, want)
	}
}

func TestReplayReturnsInvalidRecordError(t *testing.T) {
	path := walPath(t)
	data := make([]byte, record.RecordHeaderSize+len("name"))
	data[0] = 99
	data[4] = byte(len("name"))
	copy(data[record.RecordHeaderSize:], []byte("name"))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer w.Close()

	err = w.Replay(func(r record.Record) error {
		t.Fatalf("Replay callback should not be called for invalid record: %+v", r)
		return nil
	})
	if !errors.Is(err, kv_errors.ErrInvalidType) {
		t.Fatalf("Replay() error = %v, want ErrInvalidType", err)
	}
}

func TestResetClearsWAL(t *testing.T) {
	path := walPath(t)
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer w.Close()

	if err := w.Append(record.Record{Type: record.TypePut, Key: []byte("name"), Value: []byte("alice")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := w.Reset(); err != nil {
		t.Fatalf("Reset() error = %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("WAL size after Reset = %d, want 0", info.Size())
	}

	got := collectReplay(t, w)
	if len(got) != 0 {
		t.Fatalf("Replay() after Reset = %+v, want empty", got)
	}
}

func TestResetThenAppendKeepsOnlyNewRecords(t *testing.T) {
	w, err := Open(walPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer w.Close()

	oldRecord := record.Record{Type: record.TypePut, Key: []byte("old"), Value: []byte("1")}
	newRecord := record.Record{Type: record.TypePut, Key: []byte("new"), Value: []byte("2")}
	if err := w.Append(oldRecord); err != nil {
		t.Fatalf("Append old error = %v", err)
	}
	if err := w.Reset(); err != nil {
		t.Fatalf("Reset() error = %v", err)
	}
	if err := w.Append(newRecord); err != nil {
		t.Fatalf("Append new error = %v", err)
	}

	got := collectReplay(t, w)
	if len(got) != 1 || !sameRecord(got[0], newRecord) {
		t.Fatalf("Replay() = %+v, want only %+v", got, newRecord)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	w, err := Open(walPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}
}

func TestClosedWALRejectsOperations(t *testing.T) {
	w, err := Open(walPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	err = w.Append(record.Record{Type: record.TypePut, Key: []byte("name"), Value: []byte("alice")})
	if !errors.Is(err, kv_errors.ErrFileClosed) {
		t.Fatalf("Append() after Close error = %v, want ErrFileClosed", err)
	}

	err = w.Replay(func(r record.Record) error { return nil })
	if !errors.Is(err, kv_errors.ErrFileClosed) {
		t.Fatalf("Replay() after Close error = %v, want ErrFileClosed", err)
	}

	err = w.Reset()
	if !errors.Is(err, kv_errors.ErrFileClosed) {
		t.Fatalf("Reset() after Close error = %v, want ErrFileClosed", err)
	}
}

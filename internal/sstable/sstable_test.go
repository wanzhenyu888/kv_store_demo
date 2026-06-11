package sstable

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

func sstablePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "table.sst")
}

func createTable(t *testing.T, records []record.Record) *SSTable {
	t.Helper()

	path := sstablePath(t)
	w, err := CreateSSTableWriter(path)
	if err != nil {
		t.Fatalf("CreateSSTableWriter() error = %v", err)
	}
	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append(%+v) error = %v", rec, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer Close() error = %v", err)
	}

	table, err := Open(1, path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return table
}

func sameRecord(a, b record.Record) bool {
	return a.Type == b.Type &&
		bytes.Equal(a.Key, b.Key) &&
		bytes.Equal(a.Value, b.Value)
}

func TestGetReturnsMissWithoutError(t *testing.T) {
	table := createTable(t, []record.Record{
		{Type: record.TypePut, Key: []byte("name"), Value: []byte("alice")},
	})
	defer table.Close()

	got, ok, err := table.Get([]byte("missing"))
	if err != nil {
		t.Fatalf("Get(missing) error = %v, want nil", err)
	}
	if ok {
		t.Fatalf("Get(missing) ok = true, want false")
	}
	if !sameRecord(got, record.Record{}) {
		t.Fatalf("Get(missing) record = %+v, want zero value", got)
	}
}

func TestGetRejectsEmptyKey(t *testing.T) {
	table := createTable(t, []record.Record{
		{Type: record.TypePut, Key: []byte("name"), Value: []byte("alice")},
	})
	defer table.Close()

	_, ok, err := table.Get(nil)
	if !errors.Is(err, kv_errors.ErrInvalidPara) {
		t.Fatalf("Get(nil) error = %v, want ErrInvalidPara", err)
	}
	if ok {
		t.Fatalf("Get(nil) ok = true, want false")
	}
}

func TestForEachRecordPropagatesGetError(t *testing.T) {
	table := createTable(t, []record.Record{
		{Type: record.TypePut, Key: []byte("name"), Value: []byte("alice")},
	})
	if err := table.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	called := false
	err := table.ForEachRecord(func(record.Record) error {
		called = true
		return nil
	})
	if !errors.Is(err, kv_errors.ErrFileClosed) {
		t.Fatalf("ForEachRecord() error = %v, want ErrFileClosed", err)
	}
	if called {
		t.Fatal("ForEachRecord() called callback after Get failed")
	}
}

func TestForEachRecordOrdersByKey(t *testing.T) {
	table := createTable(t, []record.Record{
		{Type: record.TypePut, Key: []byte("b"), Value: []byte("2")},
		{Type: record.TypePut, Key: []byte("a"), Value: []byte("1")},
		{Type: record.TypeDelete, Key: []byte("c")},
	})
	defer table.Close()

	var keys []string
	err := table.ForEachRecord(func(r record.Record) error {
		keys = append(keys, string(r.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachRecord() error = %v", err)
	}
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("ForEachRecord() keys = %v, want %v", keys, want)
	}
}

func TestOpenRejectsIncompleteRecord(t *testing.T) {
	path := sstablePath(t)
	if err := os.WriteFile(path, []byte{record.TypePut}, 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	table, err := Open(1, path)
	if !errors.Is(err, kv_errors.ErrIncompleteRecord) {
		t.Fatalf("Open() error = %v, want ErrIncompleteRecord", err)
	}
	if table != nil {
		t.Fatalf("Open() table = %+v, want nil", table)
	}
}

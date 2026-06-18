package sstable

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"kv_store_demo/internal/engine/record"
	"kv_store_demo/internal/platform/kv_errors"
)

func sstablePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "table.sst")
}

func TestConcurrentGetIsStable(t *testing.T) {
	const (
		recordCount = 64
		valueSize   = 16 * 1024
		workers     = 16
		iterations  = 200
	)

	records := make([]record.Record, 0, recordCount)
	values := make(map[string][]byte, recordCount)
	for i := 0; i < recordCount; i++ {
		key := []byte("key-" + string(rune('a'+i%26)) + "-" + string(rune('a'+i/26)))
		value := bytes.Repeat([]byte{byte(i)}, valueSize)
		records = append(records, record.Record{
			Type:  record.TypePut,
			Key:   key,
			Value: value,
		})
		values[string(key)] = value
	}

	table := createTable(t, records)
	defer table.Close()

	var failures atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for workerID := 0; workerID < workers; workerID++ {
		workerID := workerID
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				rec := records[(workerID+i)%len(records)]
				got, ok, err := table.Get(rec.Key)
				if err != nil || !ok || !bytes.Equal(got.Value, values[string(rec.Key)]) {
					failures.Add(1)
					return
				}
			}
		}()
	}
	wg.Wait()

	if failures.Load() != 0 {
		t.Fatalf("Concurrent Get had %d failures", failures.Load())
	}
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

func TestIteratorReadsRecordsInFileOrder(t *testing.T) {
	table := createTable(t, []record.Record{
		{Type: record.TypePut, Key: []byte("a"), Value: []byte("1")},
		{Type: record.TypeDelete, Key: []byte("b")},
		{Type: record.TypePut, Key: []byte("c"), Value: []byte("3")},
	})
	defer table.Close()

	it, err := table.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator() error = %v", err)
	}
	defer it.Close()

	var got []record.Record
	for it.Valid() {
		got = append(got, it.Record())
		it.Next()
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Iterator Err() = %v, want nil", err)
	}

	want := []record.Record{
		{Type: record.TypePut, Key: []byte("a"), Value: []byte("1")},
		{Type: record.TypeDelete, Key: []byte("b")},
		{Type: record.TypePut, Key: []byte("c"), Value: []byte("3")},
	}
	if len(got) != len(want) {
		t.Fatalf("iterator read %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !sameRecord(got[i], want[i]) {
			t.Fatalf("record[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestNewIteratorRejectsClosedTable(t *testing.T) {
	table := createTable(t, []record.Record{
		{Type: record.TypePut, Key: []byte("name"), Value: []byte("alice")},
	})
	if err := table.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	it, err := table.NewIterator()
	if !errors.Is(err, kv_errors.ErrFileClosed) {
		t.Fatalf("NewIterator() error = %v, want ErrFileClosed", err)
	}
	if it != nil {
		t.Fatalf("NewIterator() iterator = %+v, want nil", it)
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

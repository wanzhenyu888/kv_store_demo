package compact

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"kv_store_demo/internal/record"
	"kv_store_demo/internal/sstable"
)

func createSSTable(t *testing.T, dir string, id int, records []record.Record) *sstable.SSTable {
	t.Helper()

	path := filepath.Join(dir, fmt.Sprintf("%04d.sst", id))
	writer, err := sstable.CreateSSTableWriter(path)
	if err != nil {
		t.Fatalf("CreateSSTableWriter() error = %v", err)
	}
	for _, rec := range records {
		if err := writer.Append(rec); err != nil {
			t.Fatalf("Append(%+v) error = %v", rec, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer Close() error = %v", err)
	}

	table, err := sstable.Open(id, path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return table
}

func readRecords(t *testing.T, table *sstable.SSTable) []record.Record {
	t.Helper()

	var records []record.Record
	if err := table.ForEachRecord(func(rec record.Record) error {
		records = append(records, rec)
		return nil
	}); err != nil {
		t.Fatalf("ForEachRecord() error = %v", err)
	}
	return records
}

func assertRecord(t *testing.T, got record.Record, typ byte, key, value string) {
	t.Helper()

	if got.Type != typ || !bytes.Equal(got.Key, []byte(key)) || !bytes.Equal(got.Value, []byte(value)) {
		t.Fatalf("record = %+v, want type=%d key=%q value=%q", got, typ, key, value)
	}
}

func TestRunKeepsNewestPutAndOrdersOutput(t *testing.T) {
	dir := t.TempDir()
	old := createSSTable(t, dir, 0, []record.Record{
		{Type: record.TypePut, Key: []byte("a"), Value: []byte("old-a")},
		{Type: record.TypePut, Key: []byte("b"), Value: []byte("old-b")},
		{Type: record.TypePut, Key: []byte("d"), Value: []byte("old-d")},
	})
	defer old.Close()
	newer := createSSTable(t, dir, 1, []record.Record{
		{Type: record.TypePut, Key: []byte("a"), Value: []byte("new-a")},
		{Type: record.TypePut, Key: []byte("c"), Value: []byte("new-c")},
	})
	defer newer.Close()

	output := filepath.Join(dir, "out.sst")
	got, err := Run([]*sstable.SSTable{old, newer}, output)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer got.Close()

	records := readRecords(t, got)
	if len(records) != 4 {
		t.Fatalf("compacted record count = %d, want 4", len(records))
	}
	assertRecord(t, records[0], record.TypePut, "a", "new-a")
	assertRecord(t, records[1], record.TypePut, "b", "old-b")
	assertRecord(t, records[2], record.TypePut, "c", "new-c")
	assertRecord(t, records[3], record.TypePut, "d", "old-d")
}

func TestRunDropsKeyWhenNewestRecordIsTombstone(t *testing.T) {
	dir := t.TempDir()
	old := createSSTable(t, dir, 0, []record.Record{
		{Type: record.TypePut, Key: []byte("name"), Value: []byte("alice")},
	})
	defer old.Close()
	newer := createSSTable(t, dir, 1, []record.Record{
		{Type: record.TypeDelete, Key: []byte("name")},
	})
	defer newer.Close()

	output := filepath.Join(dir, "out.sst")
	got, err := Run([]*sstable.SSTable{old, newer}, output)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got != nil {
		defer got.Close()
		t.Fatalf("Run() table = %+v, want nil for empty compacted output", got)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("output file stat error = %v, want not exist", err)
	}
}

func TestRunOldTombstoneDoesNotHideNewPut(t *testing.T) {
	dir := t.TempDir()
	old := createSSTable(t, dir, 0, []record.Record{
		{Type: record.TypeDelete, Key: []byte("name")},
	})
	defer old.Close()
	newer := createSSTable(t, dir, 1, []record.Record{
		{Type: record.TypePut, Key: []byte("name"), Value: []byte("bob")},
	})
	defer newer.Close()

	output := filepath.Join(dir, "out.sst")
	got, err := Run([]*sstable.SSTable{old, newer}, output)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer got.Close()

	records := readRecords(t, got)
	if len(records) != 1 {
		t.Fatalf("compacted record count = %d, want 1", len(records))
	}
	assertRecord(t, records[0], record.TypePut, "name", "bob")
}

func TestRunWithNoInputReturnsNilAndRemovesOutput(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "out.sst")

	got, err := Run(nil, output)
	if err != nil {
		t.Fatalf("Run(nil) error = %v", err)
	}
	if got != nil {
		defer got.Close()
		t.Fatalf("Run(nil) table = %+v, want nil", got)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("output file stat error = %v, want not exist", err)
	}
}

func TestRecordHeapOrdersByKeyThenNewestTable(t *testing.T) {
	h := &recordHeap{
		{rec: record.Record{Key: []byte("b")}, tableID: 0},
		{rec: record.Record{Key: []byte("a")}, tableID: 0},
		{rec: record.Record{Key: []byte("a")}, tableID: 2},
	}

	got := []string{}
	for _, item := range *h {
		got = append(got, string(item.rec.Key))
	}
	if !reflect.DeepEqual(got, []string{"b", "a", "a"}) {
		t.Fatalf("test setup changed: heap slice keys = %v", got)
	}

	if !h.Less(2, 1) {
		t.Fatal("Less() should order same key by newer tableID first")
	}
	if !h.Less(1, 0) {
		t.Fatal("Less() should order smaller key first")
	}
}

package memtable

import (
	"errors"
	"reflect"
	"testing"

	"kv_store_demo/internal/engine/record"
	"kv_store_demo/internal/platform/kv_errors"
)

func TestPutAndGet(t *testing.T) {
	m := New()

	if err := m.Put([]byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	got, ok := m.Get([]byte("name"))
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if got.Type != record.TypePut || string(got.Key) != "name" || string(got.Value) != "alice" {
		t.Fatalf("Get() = %+v, want PUT name=alice", got)
	}
}

func TestPutAllowsEmptyValue(t *testing.T) {
	m := New()

	if err := m.Put([]byte("empty"), nil); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	got, ok := m.Get([]byte("empty"))
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if got.Type != record.TypePut || len(got.Value) != 0 {
		t.Fatalf("Get() = %+v, want PUT with empty value", got)
	}
}

func TestPutRejectsEmptyKey(t *testing.T) {
	m := New()

	err := m.Put(nil, []byte("alice"))
	if !errors.Is(err, kv_errors.ErrEmptyKey) {
		t.Fatalf("Put() error = %v, want ErrEmptyKey", err)
	}
}

func TestPutOverwritesExistingKeyAndUpdatesSize(t *testing.T) {
	m := New()

	if err := m.Put([]byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	firstSize := m.Size()

	if err := m.Put([]byte("name"), []byte("bob")); err != nil {
		t.Fatalf("Put() overwrite error = %v", err)
	}

	got, ok := m.Get([]byte("name"))
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if string(got.Value) != "bob" {
		t.Fatalf("Get().Value = %q, want bob", got.Value)
	}

	wantSize := uint64(record.RecordHeaderSize + len("name") + len("bob"))
	if m.Size() != wantSize {
		t.Fatalf("Size() = %d, want %d; first size was %d", m.Size(), wantSize, firstSize)
	}
}

func TestPutCopiesInputSlices(t *testing.T) {
	m := New()
	key := []byte("name")
	value := []byte("alice")

	if err := m.Put(key, value); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	key[0] = 'X'
	value[0] = 'X'

	got, ok := m.Get([]byte("name"))
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if string(got.Key) != "name" || string(got.Value) != "alice" {
		t.Fatalf("Get() = %+v, want original key/value", got)
	}
}

func TestGetReturnsCopy(t *testing.T) {
	m := New()
	if err := m.Put([]byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	got, ok := m.Get([]byte("name"))
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	got.Key[0] = 'X'
	got.Value[0] = 'X'

	again, ok := m.Get([]byte("name"))
	if !ok {
		t.Fatal("Get() second ok = false, want true")
	}
	if string(again.Key) != "name" || string(again.Value) != "alice" {
		t.Fatalf("Get() after external mutation = %+v, want original key/value", again)
	}
}

func TestGetMissingKey(t *testing.T) {
	m := New()

	got, ok := m.Get([]byte("missing"))
	if ok {
		t.Fatalf("Get() = %+v, true; want false", got)
	}
}

func TestDeleteStoresTombstone(t *testing.T) {
	m := New()

	if err := m.Delete([]byte("name")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got, ok := m.Get([]byte("name"))
	if !ok {
		t.Fatal("Get() ok = false, want true for tombstone")
	}
	if got.Type != record.TypeDelete || string(got.Key) != "name" || len(got.Value) != 0 {
		t.Fatalf("Get() = %+v, want DELETE tombstone", got)
	}
}

func TestDeleteRejectsEmptyKey(t *testing.T) {
	m := New()

	err := m.Delete(nil)
	if !errors.Is(err, kv_errors.ErrEmptyKey) {
		t.Fatalf("Delete() error = %v, want ErrEmptyKey", err)
	}
}

func TestDeleteOverwritesPutAndUpdatesSize(t *testing.T) {
	m := New()

	if err := m.Put([]byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := m.Delete([]byte("name")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got, ok := m.Get([]byte("name"))
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if got.Type != record.TypeDelete {
		t.Fatalf("Get().Type = %d, want TypeDelete", got.Type)
	}

	wantSize := uint64(record.RecordHeaderSize + len("name"))
	if m.Size() != wantSize {
		t.Fatalf("Size() = %d, want %d", m.Size(), wantSize)
	}
}

func TestDeleteCopiesInputKey(t *testing.T) {
	m := New()
	key := []byte("name")

	if err := m.Delete(key); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	key[0] = 'X'

	got, ok := m.Get([]byte("name"))
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if string(got.Key) != "name" {
		t.Fatalf("Get().Key = %q, want name", got.Key)
	}
}

func TestForEachRecordOrdersByKey(t *testing.T) {
	m := New()
	for _, item := range []struct {
		key   string
		value string
	}{
		{"c", "3"},
		{"a", "1"},
		{"b", "2"},
	} {
		if err := m.Put([]byte(item.key), []byte(item.value)); err != nil {
			t.Fatalf("Put(%q) error = %v", item.key, err)
		}
	}

	var keys []string
	err := m.ForEachRecord(func(r record.Record) error {
		keys = append(keys, string(r.Key))
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachRecord() error = %v", err)
	}

	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("ForEachRecord() keys = %v, want %v", keys, want)
	}
}

func TestForEachRecordIncludesTombstone(t *testing.T) {
	m := New()
	if err := m.Put([]byte("alive"), []byte("yes")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := m.Delete([]byte("dead")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	seenDelete := false
	err := m.ForEachRecord(func(r record.Record) error {
		if string(r.Key) == "dead" {
			seenDelete = true
			if r.Type != record.TypeDelete || len(r.Value) != 0 {
				t.Fatalf("dead record = %+v, want tombstone", r)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachRecord() error = %v", err)
	}
	if !seenDelete {
		t.Fatal("ForEachRecord() did not include tombstone")
	}
}

func TestForEachRecordStopsOnError(t *testing.T) {
	m := New()
	for _, key := range []string{"a", "b", "c"} {
		if err := m.Put([]byte(key), []byte(key)); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	wantErr := errors.New("stop")
	var visited []string
	err := m.ForEachRecord(func(r record.Record) error {
		visited = append(visited, string(r.Key))
		if string(r.Key) == "b" {
			return wantErr
		}
		return nil
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("ForEachRecord() error = %v, want %v", err, wantErr)
	}
	if !reflect.DeepEqual(visited, []string{"a", "b"}) {
		t.Fatalf("visited = %v, want [a b]", visited)
	}
}

func TestForEachRecordRejectsNilCallback(t *testing.T) {
	m := New()

	err := m.ForEachRecord(nil)
	if !errors.Is(err, kv_errors.ErrNilCallBack) {
		t.Fatalf("ForEachRecord(nil) error = %v, want ErrNilCallBack", err)
	}
}

func TestForEachRecordPassesCopies(t *testing.T) {
	m := New()
	if err := m.Put([]byte("name"), []byte("alice")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	err := m.ForEachRecord(func(r record.Record) error {
		r.Key[0] = 'X'
		r.Value[0] = 'X'
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachRecord() error = %v", err)
	}

	got, ok := m.Get([]byte("name"))
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if string(got.Key) != "name" || string(got.Value) != "alice" {
		t.Fatalf("Get() after ForEachRecord mutation = %+v, want original key/value", got)
	}
}

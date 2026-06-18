package test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	kv "kv_store_demo"
)

func openDB(t *testing.T, dir string, memTableSize uint64) *kv.DB {
	t.Helper()

	db, err := kv.Open(kv.Options{
		Dir:          dir,
		MemTableSize: memTableSize,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if db == nil {
		t.Fatal("Open() returned nil DB")
	}
	return db
}

func mustPut(t *testing.T, db *kv.DB, key, value string) {
	t.Helper()

	if err := db.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q, %q) error = %v", key, value, err)
	}
}

func mustDelete(t *testing.T, db *kv.DB, key string) {
	t.Helper()

	if err := db.Delete([]byte(key)); err != nil {
		t.Fatalf("Delete(%q) error = %v", key, err)
	}
}

func mustFlush(t *testing.T, db *kv.DB) {
	t.Helper()

	if err := db.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func mustClose(t *testing.T, db *kv.DB) {
	t.Helper()

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func assertGet(t *testing.T, db *kv.DB, key, want string) {
	t.Helper()

	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func assertMissing(t *testing.T, db *kv.DB, key string) {
	t.Helper()

	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q) error = %v, want nil error for missing key", key, err)
	}
	if len(got) != 0 {
		t.Fatalf("Get(%q) = %q, want empty value", key, got)
	}
}

func countSSTables(t *testing.T, dir string) int {
	t.Helper()

	entries, err := os.ReadDir(filepath.Join(dir, "sst"))
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatalf("ReadDir(sst) error = %v", err)
	}

	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".sst" {
			count++
		}
	}
	return count
}

func TestOpenCreatesUsableDB(t *testing.T) {
	dir := t.TempDir()

	db := openDB(t, dir, kv.DefaultMemTableSize)
	defer db.Close()

	if _, err := os.Stat(filepath.Join(dir, "wal.log")); err != nil {
		t.Fatalf("wal.log was not created in DB dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "sst")); err != nil {
		t.Fatalf("sst dir was not created in DB dir: %v", err)
	}
}

func TestPutGetFromMemTable(t *testing.T) {
	db := openDB(t, t.TempDir(), kv.DefaultMemTableSize)
	defer db.Close()

	mustPut(t, db, "name", "alice")
	assertGet(t, db, "name", "alice")

	mustPut(t, db, "name", "bob")
	assertGet(t, db, "name", "bob")
}

func TestPutGetDeleteRejectEmptyKey(t *testing.T) {
	db := openDB(t, t.TempDir(), kv.DefaultMemTableSize)
	defer db.Close()

	if err := db.Put(nil, []byte("alice")); !errors.Is(err, kv.ErrEmptyKey) {
		t.Fatalf("Put(nil) error = %v, want ErrEmptyKey", err)
	}
	if _, err := db.Get(nil); !errors.Is(err, kv.ErrEmptyKey) {
		t.Fatalf("Get(nil) error = %v, want ErrEmptyKey", err)
	}
	if err := db.Delete(nil); !errors.Is(err, kv.ErrEmptyKey) {
		t.Fatalf("Delete(nil) error = %v, want ErrEmptyKey", err)
	}
}

func TestPutAllowsEmptyValue(t *testing.T) {
	db := openDB(t, t.TempDir(), kv.DefaultMemTableSize)
	defer db.Close()

	if err := db.Put([]byte("nil"), nil); err != nil {
		t.Fatalf("Put(nil value) error = %v", err)
	}
	got, err := db.Get([]byte("nil"))
	if err != nil {
		t.Fatalf("Get(nil value key) error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get(nil value key) = %q, want empty value", got)
	}

	if err := db.Put([]byte("empty"), []byte{}); err != nil {
		t.Fatalf("Put(empty value) error = %v", err)
	}
	got, err = db.Get([]byte("empty"))
	if err != nil {
		t.Fatalf("Get(empty value key) error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get(empty value key) = %q, want empty value", got)
	}
}

func TestGetMissingReturnsNil(t *testing.T) {
	db := openDB(t, t.TempDir(), kv.DefaultMemTableSize)
	defer db.Close()

	assertMissing(t, db, "missing")
}

func TestDeleteWritesTombstone(t *testing.T) {
	db := openDB(t, t.TempDir(), kv.DefaultMemTableSize)
	defer db.Close()

	mustPut(t, db, "name", "alice")
	mustDelete(t, db, "name")
	assertMissing(t, db, "name")
}

func TestDeleteMissingKeySucceeds(t *testing.T) {
	db := openDB(t, t.TempDir(), kv.DefaultMemTableSize)
	defer db.Close()

	mustDelete(t, db, "missing")
	assertMissing(t, db, "missing")
}

func TestFlushPersistsDataAndResetsWAL(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, kv.DefaultMemTableSize)

	mustPut(t, db, "name", "alice")
	mustFlush(t, db)
	assertGet(t, db, "name", "alice")

	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("Stat(wal.log) error = %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("wal.log size = %d, want 0 after Flush", info.Size())
	}
	if got := countSSTables(t, dir); got != 1 {
		t.Fatalf("sstable count = %d, want 1", got)
	}

	mustClose(t, db)
	db = openDB(t, dir, kv.DefaultMemTableSize)
	defer db.Close()
	assertGet(t, db, "name", "alice")
}

func TestFlushEmptyMemTableIsNoop(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, kv.DefaultMemTableSize)
	defer db.Close()

	mustFlush(t, db)
	if got := countSSTables(t, dir); got != 0 {
		t.Fatalf("sstable count after empty Flush = %d, want 0", got)
	}
}

func TestReopenRestoresFromWAL(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, kv.DefaultMemTableSize)

	mustPut(t, db, "name", "alice")
	mustClose(t, db)

	db = openDB(t, dir, kv.DefaultMemTableSize)
	defer db.Close()
	assertGet(t, db, "name", "alice")
}

func TestReopenRestoresDeleteFromWAL(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, kv.DefaultMemTableSize)

	mustPut(t, db, "name", "alice")
	mustDelete(t, db, "name")
	mustClose(t, db)

	db = openDB(t, dir, kv.DefaultMemTableSize)
	defer db.Close()
	assertMissing(t, db, "name")
}

func TestNewestSSTableWins(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, kv.DefaultMemTableSize)

	mustPut(t, db, "name", "alice")
	mustFlush(t, db)
	mustPut(t, db, "name", "bob")
	mustFlush(t, db)
	mustClose(t, db)

	db = openDB(t, dir, kv.DefaultMemTableSize)
	defer db.Close()
	assertGet(t, db, "name", "bob")
}

func TestTombstoneInNewerSSTableHidesOlderValue(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, kv.DefaultMemTableSize)

	mustPut(t, db, "name", "alice")
	mustFlush(t, db)
	mustDelete(t, db, "name")
	mustFlush(t, db)
	mustClose(t, db)

	db = openDB(t, dir, kv.DefaultMemTableSize)
	defer db.Close()
	assertMissing(t, db, "name")
}

func TestCompactFlushesMemTableAndKeepsLatestValues(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, kv.DefaultMemTableSize)

	mustPut(t, db, "name", "alice")
	mustFlush(t, db)
	mustPut(t, db, "name", "bob")
	mustPut(t, db, "city", "shanghai")

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	assertGet(t, db, "name", "bob")
	assertGet(t, db, "city", "shanghai")
	if got := countSSTables(t, dir); got != 1 {
		t.Fatalf("sstable count after Compact = %d, want 1", got)
	}

	mustClose(t, db)
	db = openDB(t, dir, kv.DefaultMemTableSize)
	defer db.Close()
	assertGet(t, db, "name", "bob")
	assertGet(t, db, "city", "shanghai")
}

func TestCompactDropsDeletedKeysAndOldVersions(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, kv.DefaultMemTableSize)

	mustPut(t, db, "name", "alice")
	mustFlush(t, db)
	mustPut(t, db, "name", "bob")
	mustFlush(t, db)
	mustDelete(t, db, "name")
	mustFlush(t, db)

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	assertMissing(t, db, "name")
	if got := countSSTables(t, dir); got != 0 {
		t.Fatalf("sstable count after compacting deleted key = %d, want 0", got)
	}

	mustClose(t, db)
	db = openDB(t, dir, kv.DefaultMemTableSize)
	defer db.Close()
	assertMissing(t, db, "name")
	if got := countSSTables(t, dir); got != 0 {
		t.Fatalf("sstable count after reopen = %d, want 0", got)
	}
}

func TestCompactRejectsClosedDB(t *testing.T) {
	db := openDB(t, t.TempDir(), kv.DefaultMemTableSize)
	mustClose(t, db)

	if err := db.Compact(); !errors.Is(err, kv.ErrDbClosed) {
		t.Fatalf("Compact() after Close error = %v, want ErrDbClosed", err)
	}
}

func TestDBCreateAndRestoreSnapshot(t *testing.T) {
	sourceDir := t.TempDir()
	source := openDB(t, sourceDir, kv.DefaultMemTableSize)
	defer source.Close()

	mustPut(t, source, "name", "alice")
	mustPut(t, source, "city", "paris")
	mustDelete(t, source, "city")

	var snapshot bytes.Buffer
	if err := source.CreateSnapshot(&snapshot); err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	if snapshot.Len() == 0 {
		t.Fatal("CreateSnapshot() wrote empty snapshot")
	}

	targetDir := t.TempDir()
	target := openDB(t, targetDir, kv.DefaultMemTableSize)
	defer target.Close()
	mustPut(t, target, "stale", "value")

	if err := target.RestoreSnapshot(bytes.NewReader(snapshot.Bytes())); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}

	assertGet(t, target, "name", "alice")
	assertMissing(t, target, "city")
	assertMissing(t, target, "stale")

	info, err := os.Stat(filepath.Join(targetDir, "wal.log"))
	if err != nil {
		t.Fatalf("Stat(wal.log) error = %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("restored wal.log size = %d, want 0", info.Size())
	}

	mustPut(t, target, "after", "restore")
	assertGet(t, target, "after", "restore")
}

func TestAutoFlushOnPut(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, 1)
	defer db.Close()

	mustPut(t, db, "name", "alice")
	assertGet(t, db, "name", "alice")
	if got := countSSTables(t, dir); got == 0 {
		t.Fatal("sstable count = 0, want auto Flush to create an SSTable")
	}
}

func TestAutoFlushOnDelete(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, 1)
	defer db.Close()

	mustDelete(t, db, "name")
	assertMissing(t, db, "name")
	if got := countSSTables(t, dir); got == 0 {
		t.Fatal("sstable count = 0, want delete tombstone to trigger auto Flush")
	}
}

func TestCloseRejectsOperations(t *testing.T) {
	db := openDB(t, t.TempDir(), kv.DefaultMemTableSize)
	mustClose(t, db)

	if err := db.Put([]byte("name"), []byte("alice")); !errors.Is(err, kv.ErrDbClosed) {
		t.Fatalf("Put() after Close error = %v, want ErrDbClosed", err)
	}
	if _, err := db.Get([]byte("name")); !errors.Is(err, kv.ErrDbClosed) {
		t.Fatalf("Get() after Close error = %v, want ErrDbClosed", err)
	}
	if err := db.Delete([]byte("name")); !errors.Is(err, kv.ErrDbClosed) {
		t.Fatalf("Delete() after Close error = %v, want ErrDbClosed", err)
	}
	if err := db.Flush(); !errors.Is(err, kv.ErrDbClosed) {
		t.Fatalf("Flush() after Close error = %v, want ErrDbClosed", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}
}

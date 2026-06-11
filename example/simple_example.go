package main

import (
	"bytes"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"gopkg.in/natefinch/lumberjack.v2"

	kv "kv_store_demo"
)

const (
	dataDir      = "/tmp/kv_store_demo/example_data"
	userCount    = 1000
	memTableSize = 8 * 1024
)

func initLogger() *slog.Logger {
	writer := &lumberjack.Logger{
		Filename:   "/tmp/kv_store_demo/logs/app.log",
		MaxSize:    200,
		MaxBackups: 7,
		MaxAge:     30,
		Compress:   true,
	}

	handler := slog.NewJSONHandler(
		writer,
		&slog.HandlerOptions{
			Level:     slog.LevelDebug,
			AddSource: true,
		},
	)
	return slog.New(handler)
}

func mustOpen(logger *slog.Logger) *kv.DB {
	db, err := kv.Open(kv.Options{
		Dir:          dataDir,
		MemTableSize: memTableSize,
		Logger:       logger,
	})
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	return db
}

func userNameKey(id int) string {
	return fmt.Sprintf("user:%04d:name", id)
}

func userEmailKey(id int) string {
	return fmt.Sprintf("user:%04d:email", id)
}

func userStatusKey(id int) string {
	return fmt.Sprintf("user:%04d:status", id)
}

func initialName(id int) string {
	return fmt.Sprintf("name-%04d", id)
}

func updatedName(id int) string {
	return fmt.Sprintf("updated-name-%04d", id)
}

func email(id int) string {
	return fmt.Sprintf("user%04d@example.com", id)
}

func mustPut(db *kv.DB, key, value string) {
	if err := db.Put([]byte(key), []byte(value)); err != nil {
		log.Fatalf("put %q: %v", key, err)
	}
}

func mustDelete(db *kv.DB, key string) {
	if err := db.Delete([]byte(key)); err != nil {
		log.Fatalf("delete %q: %v", key, err)
	}
}

func mustFlush(db *kv.DB) {
	if err := db.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
}

func mustClose(db *kv.DB) {
	if err := db.Close(); err != nil {
		log.Fatalf("close db: %v", err)
	}
}

func assertValue(db *kv.DB, key, want string) {
	got, err := db.Get([]byte(key))
	if err != nil {
		log.Fatalf("get %q: %v", key, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		log.Fatalf("get %q = %q, want %q", key, got, want)
	}
}

func assertEmpty(db *kv.DB, key string) {
	got, err := db.Get([]byte(key))
	if err != nil {
		log.Fatalf("get %q: %v", key, err)
	}
	if len(got) != 0 {
		log.Fatalf("get %q = %q, want empty or missing", key, got)
	}
}

func countSSTables() int {
	entries, err := os.ReadDir(filepath.Join(dataDir, "sst"))
	if err != nil {
		return 0
	}

	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".sst" {
			count++
		}
	}
	return count
}

func insertUsers(db *kv.DB) int {
	writes := 0
	for id := 1; id <= userCount; id++ {
		mustPut(db, userNameKey(id), initialName(id))
		mustPut(db, userEmailKey(id), email(id))
		mustPut(db, userStatusKey(id), "active")
		writes += 3
	}
	return writes
}

func updateEveryTenthUserName(db *kv.DB) int {
	updates := 0
	for id := 10; id <= userCount; id += 10 {
		mustPut(db, userNameKey(id), updatedName(id))
		updates++
	}
	return updates
}

func deleteEveryFifteenthStatus(db *kv.DB) int {
	deletes := 0
	for id := 15; id <= userCount; id += 15 {
		mustDelete(db, userStatusKey(id))
		deletes++
	}
	return deletes
}

func verifySamples(db *kv.DB) {
	assertValue(db, userNameKey(1), initialName(1))
	assertValue(db, userEmailKey(500), email(500))
	assertValue(db, userStatusKey(1000), "active")
	assertValue(db, userNameKey(10), updatedName(10))
	assertValue(db, userNameKey(11), initialName(11))
	assertEmpty(db, userStatusKey(15))
	assertValue(db, userStatusKey(16), "active")
}

func main() {
	if err := os.RemoveAll(dataDir); err != nil {
		log.Fatalf("clean data dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(dataDir), 0755); err != nil {
		log.Fatalf("create parent dir: %v", err)
	}

	logger := initLogger()
	db := mustOpen(logger)

	writes := insertUsers(db)
	fmt.Printf("stage 1: inserted %d user records for %d users\n", writes, userCount)

	assertValue(db, userNameKey(1), initialName(1))
	assertValue(db, userEmailKey(500), email(500))
	assertValue(db, userStatusKey(1000), "active")
	fmt.Println("stage 2: verified sample reads after initial load")

	updates := updateEveryTenthUserName(db)
	fmt.Printf("stage 3: updated %d user names\n", updates)

	deletes := deleteEveryFifteenthStatus(db)
	fmt.Printf("stage 4: deleted %d status records with tombstones\n", deletes)

	mustFlush(db)
	fmt.Printf("stage 5: flushed memtable, sstable files=%d\n", countSSTables())

	mustClose(db)
	fmt.Println("stage 6: closed database")

	db = mustOpen(logger)
	defer db.Close()
	fmt.Printf("stage 7: reopened database, sstable files=%d\n", countSSTables())

	verifySamples(db)
	fmt.Println("verification succeeded")
	fmt.Println("compaction is not included in this example yet")
}

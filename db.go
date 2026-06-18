package kv

import (
	"archive/tar"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"kv_store_demo/internal/engine/compact"
	"kv_store_demo/internal/engine/memtable"
	"kv_store_demo/internal/engine/record"
	"kv_store_demo/internal/engine/sstable"
	"kv_store_demo/internal/engine/wal"
	"kv_store_demo/internal/platform"
	"kv_store_demo/internal/platform/kv_errors"
)

type DB struct {
	options      Options
	Dir          string
	MemTableSize uint64
	Memtable     *memtable.MemTable

	sstDir      string
	SstablesNum int
	Sstables    []*sstable.SSTable

	Wal *wal.WAL

	mu     sync.RWMutex
	closed bool
}

func loadSstables(dir string) ([]*sstable.SSTable, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		platform.Logger.Error("Mkdir failed", "dir", dir, "err", err)
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		platform.Logger.Error("Read dir failed", "dir", dir, "err", err)
		return nil, err
	}

	var sstables []*sstable.SSTable
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		nameWithOutExt, ok := strings.CutSuffix(entry.Name(), ".sst")
		if ok {
			path := filepath.Join(dir, entry.Name())
			id, _ := strconv.Atoi(nameWithOutExt)
			sstable, err := sstable.Open(id, path)
			if err != nil {
				platform.Logger.Error("Open sstable failed", "id", id, "path", path, "err", err)
				return nil, err
			}
			sstables = append(sstables, sstable)
		}
	}

	return sstables, nil
}

func closeSstables(sstables []*sstable.SSTable) error {
	for _, sst := range sstables {
		if err := sst.Close(); err != nil {
			return err
		}
	}
	return nil
}

func Open(options Options) (*DB, error) {
	// 检查并创建目录
	if err := platform.CreateDir(options.Dir); err != nil {
		platform.Logger.Error("platform.CreateDir failed", "dir", options.Dir, "err", err)
	}

	// 基础初始化
	options = normalizeOptions(options)
	platform.InitLogger(options.Logger)

	// 加载已有的SSTable
	sstables, err := loadSstables(options.sstDir)
	if err != nil {
		platform.Logger.Error("Open DB failed", slog.Any("err", err))
		return nil, err
	}

	// 加载已有的WAL File，如果没有则创建
	walFilePath := filepath.Join(options.Dir, "wal.log")
	walIns, err := wal.Open(walFilePath)
	if err != nil {
		closeSstables(sstables)
		platform.Logger.Error("Open Wal failed", "wal path", walFilePath, "err", err)
		return nil, err
	}

	// 创建内存中的Memtable
	memtableIns := memtable.New()

	// 通过WAL回放更新Memtable
	err = walIns.Replay(func(rec record.Record) error {
		if rec.Type == record.TypePut {
			return memtableIns.Put(rec.Key, rec.Value)
		}
		return memtableIns.Delete(rec.Key)
	})
	if err != nil {
		closeSstables(sstables)
		platform.Logger.Error("Wal Replay failed", "err", err)
		return nil, err
	}

	// 返回DB实例
	platform.Logger.Info("Open DB succeeded")
	return &DB{
		options:      options,
		Dir:          options.Dir,
		MemTableSize: options.MemTableSize,
		Memtable:     memtableIns,
		sstDir:       options.sstDir,
		SstablesNum:  len(sstables),
		Sstables:     sstables,
		Wal:          walIns,
	}, nil
}

func triggerFlush(db *DB) error {
	if db.Memtable.Size() == 0 {
		platform.Logger.Debug("memtable is nil")
		return nil
	}

	newSstId := db.SstablesNum
	newSstPath := filepath.Join(db.sstDir, fmt.Sprintf("%04d.sst", newSstId))
	sstWriter, err := sstable.CreateSSTableWriter(newSstPath)
	if err != nil {
		platform.Logger.Error("Create new sst writer failed", "newSstPath", newSstPath, "err", err)
		return err
	}

	err = db.Memtable.ForEachRecord(func(rec record.Record) error {
		if err = sstWriter.Append(rec); err != nil {
			platform.Logger.Error("Append to sst failed", "newSstPath", newSstPath, "err", err)
			return err
		}
		return nil
	})
	if err != nil {
		platform.Logger.Error("Flush memtable to sst failed", "newSstPath", newSstPath, "err", err)
		return err
	}

	if err := sstWriter.Close(); err != nil {
		platform.Logger.Error("Close sst writer failed", "newSstPath", newSstPath, "err", err)
		return err
	}

	sst, err := sstable.Open(newSstId, newSstPath)
	if err != nil {
		platform.Logger.Error("Open sstable failed", "newSstPath", newSstPath, "err", err)
		return err
	}
	db.Sstables = append(db.Sstables, sst)
	db.SstablesNum++

	if err := db.Wal.Reset(); err != nil {
		platform.Logger.Error("Reset wal file failed", "newSstPath", newSstPath, "err", err)
		return err
	}

	db.Memtable.Reset()
	return nil
}

func (db *DB) Put(key, value []byte) error {
	// 检查key是否有效
	if len(key) == 0 {
		return kv_errors.ErrEmptyKey
	}

	// 检查DB是否打开
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return kv_errors.ErrDbClosed
	}

	// 构造record
	rec := record.Record{
		Type:  record.TypePut,
		Key:   key,
		Value: value,
	}

	// 写入WAL日志
	if err := db.Wal.Append(rec); err != nil {
		platform.Logger.Error("append one record to wal failed", "err", err)
		return err
	}

	// 写入Memtable
	if err := db.Memtable.Put(key, value); err != nil {
		platform.Logger.Error("Put one record to memtable failed", "err", err)
		return err
	}

	// 如果Memtable大小超过限制，触发flush下刷
	if db.Memtable.Size() >= uint64(db.MemTableSize) {
		return triggerFlush(db)
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// 检查key是否有效
	if len(key) == 0 {
		return nil, kv_errors.ErrEmptyKey
	}

	// 加读锁，并检查db是否打开
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, kv_errors.ErrDbClosed
	}

	// 从memtable中搜索
	if rec, ok := db.Memtable.Get(key); ok == true {
		platform.Logger.Debug("Get rec from memtable succeeded", "key", key)
		return rec.Value, nil
	}

	// 从sstable中按照文件序号从大到小搜索
	for i := db.SstablesNum - 1; i >= 0; i-- {
		sst := db.Sstables[i]
		rec, ok, err := sst.Get(key)
		if err != nil {
			platform.Logger.Error("Get rec from sstable succeeded", "key", key, "sst id", i)
			return nil, err
		}
		if ok {
			platform.Logger.Debug("Get rec from sstable succeeded", "key", key, "sst id", i)
			return rec.Value, nil
		}
	}

	return nil, nil
}

func (db *DB) Delete(key []byte) error {
	// 检查key是否有效
	if len(key) == 0 {
		return kv_errors.ErrEmptyKey
	}

	// 检查DB是否打开
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return kv_errors.ErrDbClosed
	}

	// 构造record
	rec := record.Record{
		Type:  record.TypeDelete,
		Key:   key,
		Value: nil,
	}

	// 写入WAL日志
	if err := db.Wal.Append(rec); err != nil {
		platform.Logger.Error("append one record to wal failed", "rec", rec, "err", err)
		return err
	}

	// 写入Memtable
	if err := db.Memtable.Delete(key); err != nil {
		platform.Logger.Error("Put one record to memtable failed", "rec", rec, "err", err)
		return err
	}

	// 如果Memtable大小超过限制，触发flush下刷
	if db.Memtable.Size() >= uint64(db.MemTableSize) {
		return triggerFlush(db)
	}

	return nil
}

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return kv_errors.ErrDbClosed
	}

	return triggerFlush(db)
}

func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return kv_errors.ErrDbClosed
	}

	if err := triggerFlush(db); err != nil {
		return err
	}
	if len(db.Sstables) == 0 {
		return nil
	}

	oldPaths := make([]string, 0, len(db.Sstables))
	for _, sst := range db.Sstables {
		oldPaths = append(oldPaths, sst.Path())
	}

	tmpPath := filepath.Join(db.sstDir, "compact.tmp")
	finalPath := filepath.Join(db.sstDir, "0000.sst")
	newSst, err := compact.Run(db.Sstables, tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if newSst != nil {
		if err := newSst.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return err
		}
	}

	for _, sst := range db.Sstables {
		if err := sst.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return err
		}
	}
	for _, path := range oldPaths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			_ = os.Remove(tmpPath)
			return err
		}
	}

	if newSst == nil {
		db.Sstables = nil
		db.SstablesNum = 0
		return nil
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}

	reopenedSst, err := sstable.Open(0, finalPath)
	if err != nil {
		return err
	}
	db.Sstables = []*sstable.SSTable{reopenedSst}
	db.SstablesNum = 1
	return nil
}

func (db *DB) CreateSnapshot(w io.Writer) error {
	if w == nil {
		return kv_errors.ErrInvalidPara
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return kv_errors.ErrDbClosed
	}

	if err := triggerFlush(db); err != nil {
		return err
	}

	tw := tar.NewWriter(w)
	for _, sst := range db.Sstables {
		if err := writeSnapshotSSTable(tw, sst.Path()); err != nil {
			_ = tw.Close()
			return err
		}
	}
	return tw.Close()
}

func writeSnapshotSSTable(tw *tar.Writer, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return nil
	}

	name := filepath.Base(path)
	if filepath.Ext(name) != ".sst" {
		return nil
	}

	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0644,
		Size: info.Size(),
	}); err != nil {
		return err
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(tw, f)
	return err
}

func (db *DB) RestoreSnapshot(r io.Reader) error {
	if r == nil {
		return kv_errors.ErrInvalidPara
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return kv_errors.ErrDbClosed
	}

	if err := closeSstables(db.Sstables); err != nil {
		return err
	}
	db.Sstables = nil
	db.SstablesNum = 0

	if err := db.Wal.Close(); err != nil {
		return err
	}

	if err := os.RemoveAll(db.sstDir); err != nil {
		return err
	}
	if err := os.MkdirAll(db.sstDir, 0755); err != nil {
		return err
	}
	if err := restoreSnapshotSSTables(r, db.sstDir); err != nil {
		return err
	}

	walPath := filepath.Join(db.Dir, "wal.log")
	if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	walIns, err := wal.Open(walPath)
	if err != nil {
		return err
	}
	db.Wal = walIns
	db.Memtable = memtable.New()

	sstables, err := loadSstables(db.sstDir)
	if err != nil {
		return err
	}
	db.Sstables = sstables
	db.SstablesNum = len(sstables)
	return nil
}

func restoreSnapshotSSTables(r io.Reader, sstDir string) error {
	tr := tar.NewReader(r)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}

		name := filepath.Base(header.Name)
		if name == "." || filepath.Ext(name) != ".sst" {
			return kv_errors.ErrInvalidPara
		}
		path := filepath.Join(sstDir, name)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, tr); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}

	if err := triggerFlush(db); err != nil {
		platform.Logger.Error("Flush failed")
	}

	for id, sst := range db.Sstables {
		if err := sst.Close(); err != nil {
			platform.Logger.Error("Close sst failed", "id", id)
			return err
		}
	}

	if err := db.Wal.Close(); err != nil {
		platform.Logger.Error("Close wal failed")
		return err
	}
	db.closed = true
	return nil
}

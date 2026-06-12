# 测试规范

## 测试分工

模块单元测试跟随对应包放置：

- `internal/record/record_test.go`
- `internal/memtable/memtable_test.go`
- `internal/wal/wal_test.go`
- `internal/sstable/sstable_test.go`
- `internal/compact/compact_test.go`

外部 API / 集成测试放在：

- `test/`

## 测试方式

每次实现或重构后运行：

```bash
go test ./...
```

如果 Go build cache 因权限问题失败，可以在获得授权后重跑同一条命令。

## 测试重点

第一版测试已覆盖以下重点，后续改动应继续保持：

- Record 编码/解码
- MemTable `Put` / `Get` / `Delete`
- WAL replay
- SSTable 查询
- DB Flush 和恢复
- tombstone 覆盖旧值
- Compaction 清理旧版本
- example 可运行

## 占位测试

第一版真实实现后不应再新增长期 `ErrNotImplemented` 占位测试。新增能力应优先补行为测试。

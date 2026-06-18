# 测试规范

## 测试分工

模块单元测试跟随对应包放置：

- `internal/engine/record/record_test.go`
- `internal/engine/memtable/memtable_test.go`
- `internal/engine/wal/wal_test.go`
- `internal/engine/sstable/sstable_test.go`
- `internal/engine/compact/compact_test.go`

外部 API / 集成测试放在：

- `test/`

## 测试方式

每次实现或重构后运行：

```bash
go test ./...
```

如果 Go build cache 因权限问题失败，可以在获得授权后重跑同一条命令。

## 测试重点

测试重点不要在本文重复维护：

- v1 单机引擎测试重点见 `docs/design.md`。
- v2 分布式阶段测试重点见 `docs/distributed.md`。

本文只约束测试放置和执行方式。

## 占位测试

第一版真实实现后不应再新增长期 `ErrNotImplemented` 占位测试。新增能力应优先补行为测试。

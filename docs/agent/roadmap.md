# 阶段路线

本路线用于约束后续实现节奏：用户按模块自行填充代码，Codex 负责阶段性审查、解释和测试分析。不要跳过当前阶段直接生成后续模块的完整实现。

## 1. Record 阶段

实现范围：

- `internal/record/record.go`
- `internal/record/record_test.go`

目标：

- 定义记录类型，支持 PUT 和 DELETE。
- 实现 Record 编码和解码。
- 覆盖非法类型、空 key、不完整数据等边界。

Codex 审查重点：

- 编码格式是否清晰稳定。
- 错误处理是否完整。
- 测试是否覆盖正常路径和边界条件。

## 2. MemTable 阶段

实现范围：

- `internal/memtable/memtable.go`
- `internal/memtable/memtable_test.go`

目标：

- 支持内存 `Put` / `Get` / `Delete`。
- 支持 tombstone。
- 维护大小估算。
- 支持按 key 有序导出 records。

Codex 审查重点：

- `[]byte` 数据所有权是否清楚。
- Put 是否覆盖旧值。
- Delete 语义是否通过 tombstone 表达。
- 导出 records 是否稳定有序。

## 3. WAL 阶段

实现范围：

- `internal/wal/wal.go`
- `internal/wal/wal_test.go`

目标：

- 支持追加写 Record。
- 支持 Replay 恢复 records。
- 支持 Reset 和 Close。

Codex 审查重点：

- 写入顺序和 flush/sync 语义是否合理。
- 文件关闭和重复关闭是否安全。
- 遇到半条记录时的行为是否符合学习版预期。

## 4. SSTable 阶段

实现范围：

- `internal/sstable/sstable.go`
- `internal/sstable/sstable_test.go`

目标：

- 写入有序 records。
- 打开已有 SSTable。
- 扫描文件建立内存索引。
- 支持按 key 查询。
- 支持遍历 records。

Codex 审查重点：

- 是否复用 Record 文件格式。
- 索引构建是否正确。
- 查询 tombstone 时是否保留删除语义。
- 遍历顺序是否稳定。

## 5. DB 串联阶段

实现范围：

- `db.go`
- `test/db_test.go`
- 必要时补充根包测试

目标：

- 串联 WAL、MemTable、SSTable。
- 实现 `Open` / `Put` / `Get` / `Delete` / `Flush` / `Close`。
- 支持 Flush 后 WAL reset。
- 支持关闭后重启恢复。

Codex 审查重点：

- API 行为是否符合 README 和设计文档。
- 锁和关闭状态是否安全。
- Get 查询顺序是否符合 LSM Tree：MemTable -> 新 SSTable -> 旧 SSTable。
- tombstone 是否能覆盖旧值。

## 6. Compaction 阶段

实现范围：

- `internal/compact/compact.go`
- `internal/compact/compact_test.go`
- `db.Compact()`
- `test/db_test.go` 集成场景

目标：

- 合并多个 SSTable。
- 同 key 保留最新版本。
- tombstone 覆盖旧值。
- 最终清理可丢弃的 tombstone。

Codex 审查重点：

- 新旧 SSTable 处理顺序是否正确。
- 删除语义是否正确。
- 旧文件替换策略是否清楚。

## 7. Example 阶段

实现范围：

- `example/main.go`
- README 快速开始说明

目标：

- 展示 Open、Put、Get、Delete、Flush、Close、重启恢复和 Compact。
- 支持 `go run ./example`。

Codex 审查重点：

- 示例是否能独立运行。
- 输出是否适合学习者理解。
- 是否与 README 和设计文档一致。

## 每阶段完成后的请求模板

```text
我完成了 <阶段名> 阶段，请 review。
重点看：
1. 是否符合 docs/design.md 和 docs/agent/roadmap.md
2. 错误处理和边界条件是否完整
3. 测试是否覆盖关键场景
只审查，不修改。
```

## 每阶段最低验证

```bash
go test ./...
```

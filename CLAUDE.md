# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目快照

- **Go 单机 KV 存储引擎学习 Demo**,基于 LSM Tree 主干流程(WAL → MemTable → SSTable → Compaction)。
- **第一版主流程已实现**:支持 `Open` / `Put` / `Get` / `Delete` / `Flush` / `Compact` / `Close`,包含 WAL、MemTable、SSTable、恢复和手动 Compaction。
- 这是一个**库**,不是服务:没有 `cmd/` 目录,没有网络层。`example/simple_example.go` 是可运行 API 示例。
- Go 1.22。`go.mod` 当前包含示例日志轮转依赖 `gopkg.in/natefinch/lumberjack.v2`。

## 常用命令

```bash
go build ./...                                   # 编译全部包
go test ./...                                    # 跑全部测试(每次改完必跑)
go run ./example                                 # 运行完整 API 示例
go test ./internal/record/...                    # 跑单个包
go test -run TestCompactFlushesMemTableAndKeepsLatestValues ./test/   # 跑单个测试
go test -v -run TestCompactFlushesMemTableAndKeepsLatestValues ./test/ # 单个测试 + 详细输出
go test ./test/...                               # 跑外部 API / 集成测试
```

项目**没有** `Makefile`、`task`、linter 配置或 pre-commit 钩子。如有需要,使用 `gofmt` / `go vet`。

## 目录结构

- 根目录 `package kv` 是**对外 API**(`db.go` / `options.go`)。使用者这样导入:

  ```go
  import kv "kv_store_demo"
  ```

- `internal/*` 是内部实现模块,Go 编译器会**阻止外部项目**导入这些包,这是有意为之。

- `test/` **只放外部 API / 集成测试**(如 `test/db_test.go`),不放置单元测试。

- 单元测试**跟随包放置**:`internal/<pkg>/<pkg>_test.go`,不要放到 `test/`。

- `example/simple_example.go` 是当前可运行示例,覆盖 Open、Put、Get、Delete、Flush、Close、重启恢复和 Compact。

- `.vscode/settings.json` 是本地编辑器配置,已被 `.gitignore` 忽略,不要提交。

## 硬性规则(踩了会出问题)

以下规则在 `AGENTS.md` / `docs/agent/*` 中已有,这里只列**最容易违反、且会破坏项目**的几条:

1. **不要创建 `src/` 目录**。在 Go modules 项目中它没有特殊访问控制意义,只会污染导入路径。
2. **不要在代码或文档的导入示例中使用 `github.com/wanzhenyu888/kv_store_demo`**。统一使用本地模块名 `kv_store_demo` 与 `import kv "kv_store_demo"`。
3. **不要一次性生成完整引擎**。每个模块都要先讲:模块职责、与 LSM Tree 的关系、数据结构、伪代码、测试点,**等用户确认后再写代码**(详见 `docs/agent/workflow.md`)。
4. **测试放对位置**:
   - 单元测试 → `internal/<pkg>/<pkg>_test.go`
   - 外部 API / 集成测试 → `test/`
   - 第一版真实实现后不要新增长期 `ErrNotImplemented` 占位测试,新增能力应优先补行为测试。
5. **Git 卫生**(详见 `docs/agent/git.md`):
   - **不要**自动 commit / push,除非用户明确要求。
   - **不要**改动与当前任务无关的文件。
   - **不要**回滚用户已有的改动。
   - 提交信息需要用户确认后再写。
6. **`.gitignore` 已忽略 `.vscode/` 和 `*.exe`**,不要尝试把本地编辑器配置或编译产物提交进去。

## 详细文档在哪(请按需阅读)

架构、数据流、文件布局等**细节都不要在这里重复**,去读对应文档:

- `README.md` — 项目入口,包含 mermaid 架构图、API 示例、设计简化点。
- `docs/design.md` — **完整设计文档**(LSM 核心思路、组件、数据流、实现顺序、测试策略、刻意简化项)。
- `AGENTS.md` — AI / Agent 协作总入口,带 `docs/agent/*` 子文档的索引。
- `docs/agent/project.md` — 项目目标与非目标,学习节奏。
- `docs/agent/layout.md` — 包布局、导入路径、目录命名。
- `docs/agent/workflow.md` — 每个模块的实现工作流(设计 → 伪代码 → 测试点 → 用户确认 → 编码)。
- `docs/agent/testing.md` — 测试放置规范与命令。
- `docs/agent/git.md` — Git 与仓库卫生规则。

## 推荐阅读 / 实现顺序

与 `README.md` / `docs/design.md` §7 一致:

1. `internal/record/record.go`
2. `internal/memtable/memtable.go`
3. `internal/wal/wal.go`
4. `internal/sstable/sstable.go`
5. `db.go` — 串联 WAL / MemTable / SSTable 的协调层。
6. `internal/compact/compact.go`
7. `example/simple_example.go`

## 起步 checklist(新会话建议)

1. 读 `README.md` → `AGENTS.md` → `docs/design.md`。
2. 按需读 `docs/agent/*` 对应子文档(布局 / 工作流 / 测试 / Git)。
3. 跑一次 `go test ./...` 确认基线绿。
4. 如涉及示例或完整流程,跑 `go run ./example`。
5. 后续扩展按模块推进:先说明设计、伪代码和测试点,再修改代码。

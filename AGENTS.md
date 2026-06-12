# AGENTS.md

## 项目定位

本项目是一个使用 Go 实现的单机 KV 存储引擎学习 Demo，基于 LSM Tree 主干流程。目标是帮助理解 WAL、MemTable、SSTable、Flush 和 Compaction，而不是实现生产级数据库。

## 必读上下文

进入项目后先阅读：

- `README.md`
- `docs/design.md`

## 当前目录结构

```text
.
├── db.go
├── options.go
├── infra/
│   ├── log.go
│   ├── utils.go
│   └── kv_errors/
├── internal/
│   ├── record/
│   ├── memtable/
│   ├── wal/
│   ├── sstable/
│   └── compact/
├── test/
├── docs/
│   ├── design.md
│   └── agent/
├── example/
│   └── simple_example.go
└── AGENTS.md
```

`example/simple_example.go` 是当前可运行示例，覆盖 Open、Put、Get、Delete、Flush、Close、重启恢复和 Compact。

## 按任务加载

- 目录/包布局任务：读取 `docs/agent/layout.md`
- 实现任务：读取 `docs/agent/workflow.md`、`docs/agent/roadmap.md` 和 `docs/agent/testing.md`
- 阶段路线/进度任务：读取 `docs/agent/roadmap.md`
- 测试任务：读取 `docs/agent/testing.md`
- Git/提交任务：读取 `docs/agent/git.md`
- 项目目标/范围任务：读取 `docs/agent/project.md`

## 硬性规则

- 不使用 `src/` 作为源码目录。
- 根目录 `package kv` 是对外 API。
- `internal/*` 是内部实现模块。
- 不在代码或文档导入示例中使用 `github.com/wanzhenyu888/kv_store_demo`。
- 第一版主流程已实现；后续扩展仍按模块先讲设计、伪代码和测试点，再实现。

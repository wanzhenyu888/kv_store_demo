# AGENTS.md

## 项目定位

本项目是一个使用 Go 实现的 KV 存储系统学习 Demo。v1 基于 LSM Tree 主干流程，目标是理解 WAL、MemTable、SSTable、Flush 和 Compaction；v2 在此基础上启动分布式演进，目标是学习 gRPC 服务化、Raft 复制一致性、一致性哈希和数据分片，而不是实现生产级数据库。

## 必读上下文

进入项目后先阅读：

- `README.md`
- `docs/design.md`
- 分布式相关任务还要阅读 `docs/distributed.md`

## 当前目录结构

```text
.
├── db.go
├── options.go
├── errors.go
├── internal/
│   ├── engine/
│   │   ├── record/
│   │   ├── memtable/
│   │   ├── wal/
│   │   ├── sstable/
│   │   └── compact/
│   └── platform/
│       ├── log.go
│       ├── utils.go
│       └── kv_errors/
├── test/
├── docs/
│   ├── design.md
│   ├── distributed.md
│   └── agent/
├── example/
│   └── simple_example.go
└── AGENTS.md
```

`example/simple_example.go` 是当前可运行示例，覆盖 Open、Put、Get、Delete、Flush、Close、重启恢复和 Compact。

## 按任务加载

- 目录/包布局任务：读取 `docs/agent/layout.md`
- 实现任务：读取 `docs/agent/workflow.md`、`docs/agent/roadmap.md` 和 `docs/agent/testing.md`
- 分布式任务：读取 `docs/distributed.md`、`docs/agent/roadmap.md`、`docs/agent/layout.md` 和 `docs/agent/testing.md`
- 阶段路线/进度任务：读取 `docs/agent/roadmap.md`
- 测试任务：读取 `docs/agent/testing.md`
- Git/提交任务：读取 `docs/agent/git.md`
- 项目目标/范围任务：读取 `docs/agent/project.md`

## 硬性规则

- 不使用 `src/` 作为源码目录。
- 根目录 `package kv` 是对外 API。
- `internal/*` 是内部实现模块。
- 不在代码或文档导入示例中使用 `github.com/wanzhenyu888/kv_store_demo`。
- v1 单机主流程已实现；v2 先做 gRPC 单节点服务，再做 HashiCorp Raft 单复制组，再做一致性哈希静态分片和多 shard group。
- 后续扩展仍按模块先讲设计、伪代码和测试点，再实现。

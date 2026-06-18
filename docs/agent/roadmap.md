# 阶段路线

本文只记录阶段索引和协作约束。具体设计、目录、数据流和测试策略分别放在：

- v1 单机引擎：`docs/design.md`
- v2 分布式演进：`docs/distributed.md`
- 测试规范：`docs/agent/testing.md`
- 目录规范：`docs/agent/layout.md`

避免在多个文档重复维护同一套阶段目标。更新阶段细节时，优先修改对应设计文档。

## v1 单机引擎阶段

v1 已完成，核心流程是：

```text
Record -> MemTable -> WAL -> SSTable -> DB -> Compaction -> Example
```

对应实现位置：

- `internal/engine/record/`
- `internal/engine/memtable/`
- `internal/engine/wal/`
- `internal/engine/sstable/`
- `db.go`
- `internal/engine/compact/`
- `example/simple_example.go`

v1 详细设计、数据流和测试重点见 `docs/design.md`。

## v2 分布式演进阶段

v2 当前路线以 `docs/distributed.md` 为准：

```text
1. gRPC 单节点服务
2. HashiCorp Raft 单复制组
3. 一致性哈希静态分片
4. 多 shard group
```

执行约束：

- 每个阶段先写设计、伪代码和测试点，再实现。
- 不跳过 gRPC 单节点服务直接进入 Raft 或分片。
- 不把分布式逻辑写进 `internal/engine/*`。
- Raft 阶段必须保证 commit 前不修改本地 `kv.DB`。
- 分片阶段必须保证一致性哈希映射到 shard 或 shard group，而不是直接映射到单节点。

## Review 请求模板

```text
我完成了 <阶段名> 阶段，请 review。
重点看：
1. 是否符合 docs/design.md / docs/distributed.md
2. 是否符合 docs/agent/layout.md 和 docs/agent/testing.md
3. 错误处理和边界条件是否完整
4. 测试是否覆盖关键场景
只审查，不修改。
```

## 每阶段最低验证

```bash
go test ./...
```

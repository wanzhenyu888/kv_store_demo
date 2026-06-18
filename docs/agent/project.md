# 项目上下文

## 项目目标

`kv-store-demo` 是一个 Go KV 存储系统学习项目。v1 使用 LSM Tree 的核心思路串联 WAL、MemTable、SSTable、Flush 和 Compaction；v2 在此基础上启动分布式演进。

第一版已实现目标：

- 支持 `Put` / `Get` / `Delete`
- 支持 WAL 持久化
- 支持 MemTable
- 支持 Flush 到 SSTable
- 支持 Compaction
- 支持 Open 后恢复数据
- 通过 `example/` 展示 API 使用

## v2 分布式演进目标

v2 按阶段学习：

- gRPC 服务化：把单机 `kv.DB` 暴露为远程服务。
- HashiCorp Raft：先实现单 Raft 复制组。
- 一致性哈希：实现 key 到 shard 的稳定路由。
- 多 shard group：每个 shard group 一个 Raft 复制组。

详细设计见 `docs/distributed.md`。

## 非目标

第一版不追求：

- 生产级可靠性
- 完整 RocksDB 兼容
- 事务
- 高并发优化
- 后台自动 Compaction
- Bloom Filter
- Block Cache
- 多层 Level Compaction
- 生产级自动扩缩容
- 在线 shard 迁移

## 当前阶段

当前项目 v1 单机主流程已经实现，并已完成目录收敛：根目录提供单机对外 API，`internal/engine/` 放 WAL、MemTable、SSTable、Compaction 等内部实现，`internal/platform/` 放内部基础设施，`example/simple_example.go` 提供可运行示例。

下一阶段是 v2 第一阶段：gRPC 单节点服务。先不要直接实现 Raft 或分片。

## 学习节奏

后续扩展仍保持学习型节奏，不要一次性生成大块生产级能力。每个新增模块或重要改动都应先说明：

- 这个模块解决什么问题
- 它和 LSM Tree 的关系
- 数据结构设计
- 伪代码
- 测试点

用户确认后再进入代码实现。

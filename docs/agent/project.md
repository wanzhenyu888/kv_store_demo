# 项目上下文

## 项目目标

`kv-store-demo` 是一个 Go 单机 KV 存储引擎学习项目，使用 LSM Tree 的核心思路串联 WAL、MemTable、SSTable、Flush 和 Compaction。

第一版目标：

- 支持 `Put` / `Get` / `Delete`
- 支持 WAL 持久化
- 支持 MemTable
- 支持 Flush 到 SSTable
- 支持 Compaction
- 支持 Open 后恢复数据
- 通过 `example/` 展示 API 使用

## 非目标

第一版不追求：

- 生产级可靠性
- 完整 RocksDB 兼容
- 分布式
- 事务
- 网络服务
- 高并发优化
- 后台自动 Compaction
- Bloom Filter
- Block Cache
- 多层 Level Compaction

## 当前阶段

当前项目处于学习型骨架阶段。根目录提供对外 API 骨架，`internal/` 下按模块放置内部实现骨架。真实 LSM 逻辑应按模块逐步实现。

## 学习节奏

不要一次性生成完整引擎。每个模块都应先说明：

- 这个模块解决什么问题
- 它和 LSM Tree 的关系
- 数据结构设计
- 伪代码
- 测试点

用户确认后再进入代码实现。

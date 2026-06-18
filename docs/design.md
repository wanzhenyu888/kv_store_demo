# kv-store-demo 设计文档

## 1. 项目目标

`kv-store-demo` 是一个使用 Go 实现的 KV 存储系统学习 Demo。

项目第一阶段目标是帮助理解 LSM Tree 的核心流程，而不是实现一个生产级数据库。第一版已经用尽量少的工程复杂度，把以下能力串起来：

- 支持 `Put` / `Get` / `Delete`
- 支持 WAL 预写日志
- 支持 MemTable 内存表
- 支持 Flush 到 SSTable
- 支持 Compaction
- 支持 Open 后恢复数据
- 通过 `example` 展示 API 使用方式

第二阶段开始进入 v2 分布式演进，目标是在保留单机 LSM 引擎边界的前提下，逐步学习：

- gRPC 服务化
- Raft 复制一致性
- 一致性哈希分片
- shard group 和路由

分布式设计详见 `docs/distributed.md`。本文继续作为单机 LSM 引擎设计文档。

## 2. 非目标

单机引擎第一版不追求以下能力：

- 事务
- 后台线程
- Bloom Filter
- Block Cache
- 多层 Level Compaction
- Manifest 元数据文件
- 高并发优化
- 生产级可靠性

这些能力都可以作为后续扩展方向，但不进入单机引擎第一版实现范围。v2 分布式演进会引入网络服务、Raft 和分片，但仍不追求生产级可靠性、自动扩缩容和复杂事务。

## 3. LSM Tree 核心思路

LSM Tree 的核心思想是：把随机写转换成顺序写。

在本 Demo 中，写入不会直接修改磁盘上的旧数据，而是先写入 WAL 和 MemTable。当 MemTable 达到阈值，或者调用者手动触发 Flush 时，MemTable 会被写成新的 SSTable。

核心流程：

- 写入先进入 WAL 和 MemTable
- MemTable 达到阈值或手动触发后 Flush 成 SSTable
- SSTable 是有序且不可变的磁盘文件
- 读取按新到旧查找
- 删除通过 tombstone 表示
- Compaction 合并 SSTable，清理旧版本和删除标记

## 4. 核心组件

### 4.1 Record

Record 表示存储引擎中的一条操作记录。

第一版中，Record 用来表达两类操作：

- `PUT`：写入或更新一个 key/value
- `DELETE`：删除一个 key

Record 包含：

- `RecordType`
- `Key`
- `Value`

`DELETE` 记录的 `Value` 为空，它表示一个 tombstone。

### 4.2 MemTable

MemTable 是内存中的最新数据视图。

第一版为了降低学习成本，使用 `map` 作为 MemTable 的内部结构。真实系统中，MemTable 通常会使用 SkipList 等有序结构，但本 Demo 会在 Flush 时对 key 排序导出。

MemTable 负责：

- 保存最新的 key/value
- 保存 tombstone
- 支持内存中的 `Put` / `Get` / `Delete`
- 维护大小估算，用于判断是否需要 Flush
- Flush 时导出有序 records

### 4.3 WAL

WAL 是 Write-Ahead Log，预写日志。

它的作用是保证 MemTable 中尚未 Flush 的数据在进程重启后可以恢复。

WAL 负责：

- 追加写 Record
- 在修改 MemTable 前先落盘
- Open 时 replay WAL，恢复 MemTable
- Flush 后 reset

第一版 WAL 不实现 checksum。遇到不完整记录时，学习版可以停止读取后续内容。

### 4.4 SSTable

SSTable 是 Flush 后生成的磁盘文件。

它有两个关键特性：

- 文件内 key 有序
- 文件写入完成后不可变

第一版 SSTable 负责：

- 写入有序 records
- 打开已有 SSTable
- 扫描文件建立内存索引
- 按 key 查询 record
- 遍历所有 records，供 Compaction 使用

### 4.5 DB

DB 是对外 API 和内部协调层。

它负责把 WAL、MemTable、SSTable、Compaction 串起来，对外提供统一接口：

- `Open`
- `Put`
- `Get`
- `Delete`
- `Flush`
- `Compact`
- `Close`

### 4.6 Compaction

Compaction 用于合并多个 SSTable。

第一版 Compaction 采用单层全量合并策略：

- 合并所有 SSTable
- 新版本覆盖旧版本
- tombstone 覆盖旧值
- 最终不把 tombstone 写入新的 SSTable

当前实现借鉴 RocksDB 的流式归并思路：每个 SSTable 通过 iterator 顺序读取，使用小根堆按 key 归并，同 key 选择更新的 SSTable 记录。这个策略不是完整 RocksDB 的多层 Compaction，但足够表达 LSM Tree 整理历史数据的核心思想。

## 5. 数据流概览

### 5.1 写入路径

```text
Put/Delete -> WAL -> MemTable -> 可选 Flush
```

### 5.2 读取路径

```text
MemTable -> 新 SSTable -> 旧 SSTable
```

读取时如果先遇到 tombstone，就认为 key 不存在，不继续读取旧版本。

### 5.3 Flush 路径

```text
MemTable -> 排序 records -> 新 SSTable -> Reset WAL
```

Flush 后，当前 MemTable 会被清空，新的写入会进入新的 WAL 和 MemTable。

### 5.4 恢复路径

```text
加载 SSTable -> 构建索引 -> replay WAL -> 恢复 MemTable
```

SSTable 表示已经 Flush 到磁盘的数据，WAL 表示尚未 Flush 的最新修改。

### 5.5 Compaction 路径

```text
读取所有 SSTable iterator -> 堆归并同 key 版本 -> 写新 SSTable -> 删除旧 SSTable
```

Compaction 后，旧版本数据会被清理，SSTable 数量会减少。

## 6. 文件和模块划分

第一版主要文件：

- `options.go`：配置项
- `errors.go`：对外错误别名
- `internal/platform/kv_errors/errors.go`：内部错误定义
- `db.go`：对外 API 和整体协调
- `internal/engine/record/record.go`：记录类型和编码/解码
- `internal/engine/memtable/memtable.go`：内存表
- `internal/engine/wal/wal.go`：预写日志
- `internal/engine/sstable/sstable.go`：有序不可变文件
- `internal/engine/compact/compact.go`：SSTable 合并
- `test/db_test.go`：API 形状测试
- `example/simple_example.go`：API 使用示例

v2 分布式演进建议新增目录：

- `api/kv/v1/`：gRPC proto 定义。
- `gen/kv/v1/`：protobuf / gRPC 生成代码。
- `cmd/kv-node/`：KV 节点服务启动入口。
- `internal/server/grpc/`：gRPC server，负责把 RPC 请求适配到内部服务。
- `internal/client/`：示例和测试使用的轻量客户端封装。
- `internal/raft/`：HashiCorp Raft 接入层和 FSM。
- `internal/shard/`：一致性哈希、shard 元数据和路由。

这些目录按阶段创建，不提前放空壳。

## 7. 实现顺序

推荐实现顺序：

```text
1. internal/engine/record/record.go
2. internal/engine/memtable/memtable.go
3. internal/engine/wal/wal.go
4. internal/engine/sstable/sstable.go
5. db.go
6. internal/engine/compact/compact.go
7. example/simple_example.go
```

这个顺序先定义数据，再实现内存表，然后实现持久化和磁盘表，最后通过 DB API 串联完整流程。

## 8. 测试策略

第一版每个模块都应该有对应测试。

模块单元测试跟随对应包放置，例如：

- `internal/engine/record/record_test.go`
- `internal/engine/memtable/memtable_test.go`
- `internal/engine/wal/wal_test.go`
- `internal/engine/sstable/sstable_test.go`
- `internal/engine/compact/compact_test.go`

`test/` 目录只放外部 API / 集成测试，例如：

- `test/db_test.go`

测试重点：

- Record 编码/解码
- MemTable `Put` / `Get` / `Delete`
- WAL replay
- SSTable 查询
- DB Flush 和恢复
- tombstone 覆盖旧值
- Compaction 清理旧版本
- example 可运行

完成每个模块后运行：

```bash
go test ./...
```

## 9. 第一版设计简化

为了让项目更适合作为学习 Demo，第一版会刻意简化：

- MemTable 使用 `map`，而不是 SkipList
- SSTable 启动时全量扫描建立内存索引
- Compaction 合并所有 SSTable，使用 iterator + 堆做流式归并
- WAL 不加 checksum
- 不做后台线程
- 不做多层 Level
- 不做 Manifest

这些简化不会改变 LSM Tree 的主干流程，但能让代码更容易读懂。

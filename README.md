# kv-store-demo

## 项目简介

`kv-store-demo` 是一个使用 Go 实现的单机 KV 存储引擎学习项目。

项目采用 LSM Tree 的核心思路，目标不是实现一个生产级数据库，而是通过一个足够小、足够清晰的 Demo，帮助理解现代 KV 存储引擎中的几个关键概念。

## 项目目标

本项目用于学习和验证以下内容：

- KV 存储引擎的基本使用方式
- WAL 预写日志
- MemTable 内存表
- SSTable 有序不可变文件
- Flush 机制
- Compaction 机制
- 数据关闭后重新打开的恢复流程

## 功能特性

第一版已经支持：

- `Put` 写入 key/value
- `Get` 读取 key/value
- `Delete` 删除 key
- WAL 持久化
- MemTable
- SSTable
- 手动 Flush
- 手动 Compaction
- Open 后恢复数据
- 通过 `example` 展示 API 使用

## V1.0 版本

`v1.0` 是本项目的第一个完整学习版，实现了单机 LSM Tree KV 存储引擎的主干流程：

- 写入路径：`Put` / `Delete` 先写 WAL，再更新 MemTable。
- 读取路径：`Get` 按 MemTable、最新 SSTable 到旧 SSTable 的顺序查找。
- 持久化路径：MemTable 可手动或自动 Flush 为有序 SSTable，并在 Flush 后 reset WAL。
- 恢复路径：`Open` 加载已有 SSTable，并 replay WAL 恢复未 Flush 的数据。
- 整理路径：`Compact` 合并所有 SSTable，保留同 key 最新版本并清理 tombstone。
- 示例路径：`go run ./example` 展示写入、读取、删除、Flush、重启恢复和 Compaction。

## 非目标

本项目暂不追求以下能力：

- 生产级可靠性
- 完整 RocksDB 兼容
- 分布式存储
- 事务
- 网络服务
- 高并发优化
- 后台自动 Compaction
- Bloom Filter
- Block Cache
- 多层 Level Compaction

## 架构概览

```mermaid
flowchart TD
    A["Client API"] --> B["DB"]
    B --> C["WAL"]
    B --> D["MemTable"]
    D -->|Flush| E["SSTable Files"]
    E -->|Compaction| F["Merged SSTable"]
    C -->|Replay on Open| D
```

## 快速开始

运行测试确认当前实现：

```bash
go test ./...
```

运行完整 API 示例：

```bash
go run ./example
```

## API 示例片段

下面是 API 使用片段。完整可运行示例见 `example/simple_example.go`。

```go
import kv "kv_store_demo"

db, err := kv.Open(kv.Options{
    Dir:          "./data",
    MemTableSize: 1024,
})
if err != nil {
    panic(err)
}
defer db.Close()

if err := db.Put([]byte("name"), []byte("alice")); err != nil {
    panic(err)
}

value, err := db.Get([]byte("name"))
if err != nil {
    panic(err)
}
```

## 项目结构

当前第一版结构：

```text
.
├── db.go              # 对外 API 和整体协调
├── options.go         # 配置项
├── infra/             # 日志、文件工具和公共错误
├── internal/
│   ├── record/        # 记录类型和编码
│   ├── memtable/      # 内存表
│   ├── wal/           # 预写日志
│   ├── sstable/       # 有序不可变文件
│   └── compact/       # SSTable 合并
├── test/
│   └── db_test.go     # API 形状测试
├── docs/
│   └── design.md  # 设计文档
└── example/
    └── simple_example.go # API 使用示例
```

## 学习路线

推荐后续阅读代码时按以下顺序：

```text
1. internal/record/record.go
2. internal/memtable/memtable.go
3. internal/wal/wal.go
4. internal/sstable/sstable.go
5. db.go
6. internal/compact/compact.go
7. example/simple_example.go
```

## 设计简化

为了让项目更适合作为学习 Demo，第一版会刻意做一些简化：

- MemTable 使用 `map`，而不是 SkipList
- Flush 时对 key 排序后写入 SSTable
- SSTable 启动时扫描文件建立内存索引
- Compaction 合并所有 SSTable，使用 iterator + 堆做流式归并，而不是实现多层 Level
- WAL 不实现 checksum
- 不引入后台线程

## 后续计划

后续可以逐步扩展：

- SkipList MemTable
- SSTable 稀疏索引
- Bloom Filter
- Manifest 元数据文件
- WAL segment
- 分层 Compaction
- Benchmark
- HTTP API

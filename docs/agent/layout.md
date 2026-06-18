# 目录和包布局规范

## Go 模块

当前模块名：

```go
module kv_store_demo
```

项目代码和文档中的导入示例应使用本地学习型模块名，不使用 GitHub 远端路径。

## 对外 API

根目录是唯一对外 API 包：

```go
package kv
```

使用者和 `example/` 应这样导入：

```go
import kv "kv_store_demo"
```

根目录保留：

- `db.go`
- `options.go`
- `errors.go`

公共错误别名由根包暴露：

- `errors.go`

内部错误定义放在：

- `internal/platform/kv_errors/errors.go`

## 内部实现

内部实现放在 `internal/` 下：

```text
internal/
├── engine/
│   ├── record/
│   ├── memtable/
│   ├── wal/
│   ├── sstable/
│   └── compact/
└── platform/
    ├── log.go
    ├── utils.go
    └── kv_errors/
```

Go 会阻止项目外部代码导入 `internal/*`，因此这些目录适合放实现细节。

## v2 分布式目录

分布式目录按阶段创建，不提前创建空目录。推荐目标结构：

```text
api/
└── kv/v1/          # proto 定义
gen/
└── kv/v1/          # protobuf / gRPC 生成代码
cmd/
└── kv-node/        # 节点进程入口
internal/
├── server/grpc/    # gRPC server 适配层
├── client/         # 示例和测试用客户端封装
├── raft/           # HashiCorp Raft 接入层
└── shard/          # 一致性哈希和 shard 路由
```

`internal/engine/*` 不放分布式逻辑，继续只负责单机存储引擎。

## 禁止使用 src

不要创建或恢复 `src/` 作为源码目录。`src/` 在 Go modules 项目中没有特殊访问控制意义，还会污染导入路径。

## example 和 test

- `example/simple_example.go`：当前可运行 API 示例，支持 `go run ./example`。
- `test/`：只放外部 API / 集成测试。
- 模块单元测试放在对应包目录下，例如 `internal/engine/record/record_test.go`。

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

## 内部实现

内部实现放在 `internal/` 下：

```text
internal/
├── record/
├── memtable/
├── wal/
├── sstable/
└── compact/
```

Go 会阻止项目外部代码导入 `internal/*`，因此这些目录适合放实现细节。

## 禁止使用 src

不要创建或恢复 `src/` 作为源码目录。`src/` 在 Go modules 项目中没有特殊访问控制意义，还会污染导入路径。

## example 和 test

- `example/`：等真实 API 可运行后创建，放 API 使用示例。
- `test/`：只放外部 API / 集成测试。
- 模块单元测试放在对应包目录下，例如 `internal/record/record_test.go`。

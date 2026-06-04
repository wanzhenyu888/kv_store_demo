# 实现工作流

## 基本节奏

本项目是学习型 Demo，工作节奏要慢而清楚。实现每个模块前，先输出设计说明，不要直接写代码。

每个模块实现前应说明：

- 模块职责
- 与 LSM Tree 的关系
- 数据结构设计
- 核心流程伪代码
- 测试用例计划

用户确认后再修改文件。

## 推荐实现顺序

详细阶段目标和审查重点见 `docs/agent/roadmap.md`。

```text
1. internal/record/record.go
2. internal/memtable/memtable.go
3. internal/wal/wal.go
4. internal/sstable/sstable.go
5. db.go
6. internal/compact/compact.go
7. example/main.go
```

## 文档协作

- README 只作为入口文档，不展开接口内部实现。
- 详细设计放在 `docs/design.md`。
- Agent 协作规范放在 `AGENTS.md` 和 `docs/agent/`。

## 每轮结束

每轮结束时说明：

- 完成了什么
- 新增或修改了哪些文件
- 如何验证
- 下一步建议

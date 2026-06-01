# Git 和仓库卫生

## 忽略规则

`.gitignore` 应忽略：

- `.vscode/`
- `*.exe`

不要提交本地编辑器配置、编译产物或临时运行数据。

## 提交规则

- commit message 需要用户确认后再填写。
- 不要自动提交或推送，除非用户明确要求。
- 不要改动与当前任务无关的文件。
- 不要回滚用户已有改动，除非用户明确要求。

## 远端说明

项目可以关联 GitHub 远端，但代码和文档导入示例不要使用 `github.com/wanzhenyu888/kv_store_demo`。当前学习阶段使用：

```go
module kv_store_demo
```

示例导入：

```go
import kv "kv_store_demo"
```

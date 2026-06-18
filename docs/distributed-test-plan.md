# 分布式 KV 功能与性能压测计划

本文基于 `example/distributed/main.go` 制定，用于验证当前学习版分布式 KV 的功能正确性、一致性、基础性能和故障恢复能力。

当前 example 已具备：

- 自动生成三节点、两 shard group 的 `cluster.json`。
- 构建并启动 3 个 `kv-node` 进程。
- 通过 `RoutedClient` 按 `key -> shard -> group -> leader` 执行串行 `Put/Get`。
- 支持 `--concurrency` 并发 worker。
- 支持 `--value-size` 控制 value 大小。
- 支持 `--read-ratio` 控制读写比例，`-1` 表示兼容原有串行写后读模式。
- 支持 `--duration` 和 `--warmup` 运行时间控制。
- 支持平均延迟、P50、P95、P99、最大延迟、错误数、重试数和 group 分布统计。
- 支持 `--report` 输出 JSON 报告。
- 对所有 group 执行 `Flush`。
- 停止节点后离线打开每个副本本地 DB，校验多节点最终一致性。

当前 example 还不具备：

- 自动故障注入。
- CSV 压测报告。
- 删除、Compact、热点 key 等操作模型。
- 进程 RSS、SSTable 数量、Raft log 大小等资源观测。

## 1. 测试目标

功能目标：

- 验证三节点分布式集群能正常启动并对外提供 gRPC 服务。
- 验证 `RoutedClient` 能把请求定位到正确 shard group 的 leader。
- 验证 `Put/Get/Delete/Flush/Compact` 在 Raft 复制路径下行为正确。
- 验证不同 shard group 的数据目录、Raft log 和本地 DB 相互隔离。
- 验证停止集群后，每个副本本地 DB 的最终数据一致。

性能目标：

- 建立当前串行 `Put/Get` 的基础吞吐基线。
- 扩展并发压测后，观察不同并发、value size、读写比例下的吞吐和延迟。
- 记录平均延迟、P50、P95、P99、最大延迟、错误数、重试数和 NotLeader 次数。
- 对比不同数据规模下的性能变化，识别 Raft、gRPC、WAL、SSTable 或 Flush 带来的瓶颈。

稳定性目标：

- 验证 follower 宕机后，多数派仍可写入。
- 验证 leader 宕机后，client 能在重试后恢复读写。
- 验证节点重启后可通过本地数据、Raft log 或 snapshot 恢复。
- 验证长时间混合读写后无数据不一致、进程退出或明显资源泄漏。

## 2. 基线运行

基础命令：

```bash
go run ./example/distributed --requests 200
```

大数据量基线：

```bash
go run ./example/distributed --requests 10000 --timeout 2m
```

验收标准：

- 三个 `kv-node` 均启动成功。
- 两个 shard group 均能选出 leader。
- 所有 `Put` 成功。
- 所有 `Get` 返回预期 value。
- `Flush` 成功。
- 程序输出 `client verification succeeded`。
- 程序输出 `replica consistency verification succeeded`。
- 退出后无残留 `kv-node` 进程。

当前 example 输出中的 `write ops/sec` 和 `read ops/sec` 只能作为串行基线，不代表系统极限吞吐。

## 3. 功能测试矩阵

| 编号 | 场景 | 操作 | 验收标准 |
| --- | --- | --- | --- |
| F-01 | 集群启动 | 启动 3 节点、2 group | 三个 gRPC 地址可连接，两个 group 有 leader |
| F-02 | 基础写读 | 写入 N 个 key 后逐个读取 | value 与期望完全一致 |
| F-03 | 覆盖写 | 同一 key 连续写 `v1/v2/v3` | 最终读取 `v3` |
| F-04 | 删除 | `Put` 后 `Delete` 再 `Get` | 返回空 value |
| F-05 | 空 key | 对空 key 执行 `Put/Get/Delete` | 返回明确错误 |
| F-06 | 跨 group 路由 | 构造分别落到 `g1/g2` 的 key | key 被写入对应 group |
| F-07 | group 隔离 | 停止后检查本地 `engine/group-*` | 不同 group 数据目录独立 |
| F-08 | Flush 后读取 | 写入后执行 `Flush`，再读取 | 数据不丢失 |
| F-09 | Compact 后读取 | 多次覆盖写后执行 `Compact` | 只保留最新可见值 |
| F-10 | 重启恢复 | 写入、停止、重新启动、读取 | 所有成功写入的数据仍可读 |
| F-11 | client leader 定位 | 首次请求可能打到非 leader | client 根据 NotLeader hint 重试并成功 |
| F-12 | 本地副本校验 | 停止集群后打开每个副本 DB | 每个副本数据一致 |

## 4. 一致性测试矩阵

| 编号 | 场景 | 操作 | 验收标准 |
| --- | --- | --- | --- |
| C-01 | 写后读 | 每次 `Put(k,v)` 成功后立即 `Get(k)` | 读取到 `v` |
| C-02 | 批量写后读 | 写完所有 key 后再批量读取 | 全量匹配 |
| C-03 | 覆盖写一致性 | 对同一批 key 多轮覆盖写 | 只能读到最后一轮 value |
| C-04 | 删除一致性 | 批量删除部分 key | client 与本地副本均读不到 |
| C-05 | 副本最终一致 | 停止集群后逐个打开副本 DB | 三个副本结果一致 |
| C-06 | group 间一致性边界 | `g1/g2` 分别写入不同 key | group 间不串数据 |
| C-07 | 重启后一致性 | 停止全部节点再启动 | client 读回与停止前一致 |
| C-08 | snapshot 恢复一致性 | 触发 snapshot 后删除 engine 数据并重启 | 节点能恢复 snapshot 中的数据 |

一致性统计字段：

- `total_keys`
- `verified_keys`
- `missing_count`
- `mismatch_count`
- `deleted_key_visible_count`
- `group_distribution`
- `replica_verified_count`

## 5. 性能压测矩阵

当前 example 支持：

```bash
go run ./example/distributed --requests <N> --timeout <duration>
```

兼容模式下，`--read-ratio` 默认为 `-1`，执行原有串行写 N 个 key、再读 N 个 key 的流程。

当前压测模式支持：

```bash
go run ./example/distributed \
  --requests 100000 \
  --concurrency 32 \
  --value-size 1024 \
  --read-ratio 80 \
  --duration 5m \
  --warmup 30s \
  --report /tmp/kv_store_demo_report.json
```

进入压测模式需要显式设置 `--read-ratio 0..100`。其中 `0` 表示纯写，`100` 表示纯读，其他值表示混合读写。

核心指标：

| 指标 | 含义 |
| --- | --- |
| `write_ops_sec` | 写吞吐 |
| `read_ops_sec` | 读吞吐 |
| `mixed_ops_sec` | 混合读写吞吐 |
| `avg_latency` | 平均延迟 |
| `p50_latency` | P50 延迟 |
| `p95_latency` | P95 延迟 |
| `p99_latency` | P99 延迟 |
| `max_latency` | 最大延迟 |
| `error_count` | 请求失败数 |
| `retry_count` | client 重试次数 |
| `not_leader_count` | NotLeader 次数 |
| `bytes_written` | 写入字节数 |
| `group_distribution` | key 在 group 间的分布 |

压测场景：

| 编号 | 场景 | requests | concurrency | value size | read ratio | 目标 |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| P-01 | 串行写基线 | 1k | 1 | 128B | 0 | 建立当前 example 写入基线 |
| P-02 | 串行读基线 | 1k | 1 | 128B | 100 | 建立当前 example 读取基线 |
| P-03 | 中等数据量 | 10k | 1 | 1KB | 50 | 验证串行规模增长 |
| P-04 | 并发写 | 100k | 8/16/32/64 | 1KB | 0 | 观察写吞吐扩展性 |
| P-05 | 并发读 | 100k | 8/16/32/64 | 1KB | 100 | 观察读吞吐扩展性 |
| P-06 | 读多写少 | 100k | 32 | 1KB | 80 | 模拟常见 KV 负载 |
| P-07 | 写多读少 | 100k | 32 | 1KB | 20 | 观察 Raft 写入瓶颈 |
| P-08 | 大 value | 10k | 16 | 16KB/64KB | 50 | 观察网络、WAL、SSTable 压力 |
| P-09 | 热点 key | 100k | 32 | 512B | 80 | 观察热点覆盖写表现 |
| P-10 | 均匀 key | 100k | 32 | 512B | 80 | 验证 shard/group 分布 |
| P-11 | Flush 干扰 | 100k | 32 | 1KB | 50 | 周期性 Flush 对延迟的影响 |
| P-12 | Compact 干扰 | 100k | 32 | 1KB | 50 | Compact 对长尾延迟的影响 |

性能验收标准：

- 功能压测中 `error_count == 0`。
- 所有成功写入的数据最终校验通过。
- 并发从 `1 -> 8 -> 16 -> 32` 时吞吐应有上升趋势，直到系统瓶颈出现。
- P99 延迟不能随请求数线性增长。
- `g1/g2` 的 key 分布不应严重倾斜，除非测试刻意构造热点。
- 每轮测试必须记录命令、commit、机器配置、请求量、并发数、value size 和结果文件。

## 6. 故障与恢复测试矩阵

| 编号 | 场景 | 操作 | 验收标准 |
| --- | --- | --- | --- |
| R-01 | follower 停止 | 压测中停止一个 follower | 多数派仍在，读写继续成功 |
| R-02 | follower 重启 | 停止 follower 后重新启动 | 节点追上日志，最终一致 |
| R-03 | leader 停止 | 压测中停止当前 leader | 短暂失败或重试后恢复 |
| R-04 | leader 重启 | leader 停止后重新启动 | 集群稳定，数据最终一致 |
| R-05 | 单 group leader 故障 | 只停止 `g1` leader | `g2` 应继续可用 |
| R-06 | 少数派故障 | 每个 group 停 1 个节点 | 仍可读写 |
| R-07 | 多数派故障 | 同 group 停 2 个节点 | 写入失败，错误明确 |
| R-08 | 全集群重启 | 写入后停止全部节点再启动 | 数据可恢复 |
| R-09 | snapshot 恢复 | 触发 snapshot 后删除 engine 目录 | 重启后通过 snapshot 恢复 |
| R-10 | 长稳运行 | 混合读写 30 分钟以上 | 无数据不一致、无进程退出 |

故障测试建议在压测过程中注入，而不是空闲时注入。否则只能证明恢复流程可用，不能证明业务流量下的可用性。

## 7. 压测工具改造建议

为了把 `example/distributed` 从演示程序扩展为压测工具，建议按以下顺序演进：

1. 增加并发 worker。
2. 增加 `--concurrency`、`--value-size`、`--read-ratio`、`--duration`、`--warmup`。
3. 增加延迟采样和分位数统计。
4. 增加 JSON 报告输出。
5. 增加操作模型：`write-only`、`read-only`、`mixed`、`overwrite`、`delete`。
6. 增加故障注入参数：`--kill-follower-at`、`--kill-leader-at`、`--restart-node-at`。
7. 增加在线校验和离线副本校验开关。
8. 增加资源观测：进程 RSS、数据目录大小、SSTable 数量、Raft log 大小、snapshot 数量。

建议报告结构：

```json
{
  "cluster": {
    "nodes": 3,
    "groups": 2,
    "shards": 4
  },
  "workload": {
    "requests": 100000,
    "concurrency": 32,
    "value_size": 1024,
    "read_ratio": 80
  },
  "result": {
    "ops_sec": 1200.5,
    "avg_latency_ms": 12.3,
    "p50_latency_ms": 8.1,
    "p95_latency_ms": 32.4,
    "p99_latency_ms": 88.7,
    "error_count": 0,
    "retry_count": 15,
    "not_leader_count": 2
  },
  "consistency": {
    "verified_keys": 100000,
    "missing_count": 0,
    "mismatch_count": 0
  }
}
```

## 8. 推荐执行顺序

第一阶段：直接使用当前 example。

```bash
go run ./example/distributed --requests 200
go run ./example/distributed --requests 10000 --timeout 2m
```

第二阶段：扩展压测参数后跑并发基线。

```bash
go run ./example/distributed --requests 100000 --concurrency 1 --value-size 1024 --read-ratio 50
go run ./example/distributed --requests 100000 --concurrency 8 --value-size 1024 --read-ratio 50
go run ./example/distributed --requests 100000 --concurrency 32 --value-size 1024 --read-ratio 50
go run ./example/distributed --requests 100000 --concurrency 64 --value-size 1024 --read-ratio 50
```

第三阶段：跑读写比例和 value size 矩阵。

```bash
go run ./example/distributed --requests 100000 --concurrency 32 --value-size 128 --read-ratio 80
go run ./example/distributed --requests 100000 --concurrency 32 --value-size 1024 --read-ratio 80
go run ./example/distributed --requests 10000 --concurrency 16 --value-size 16384 --read-ratio 50
go run ./example/distributed --requests 10000 --concurrency 16 --value-size 65536 --read-ratio 50
```

第四阶段：加入故障注入。

```bash
go run ./example/distributed --duration 5m --concurrency 32 --read-ratio 80 --kill-follower-at 1m
go run ./example/distributed --duration 5m --concurrency 32 --read-ratio 80 --kill-leader-at 1m
go run ./example/distributed --duration 5m --concurrency 32 --read-ratio 80 --restart-node-at 2m
```

第五阶段：长稳测试。

```bash
go run ./example/distributed --duration 30m --concurrency 32 --value-size 1024 --read-ratio 80 --report /tmp/kv_store_demo_long_run.json
```

每轮测试结束后必须执行：

- client 在线读回校验。
- 离线副本一致性校验。
- 检查是否有残留 `kv-node` 进程。
- 保存 `cluster.json`、命令参数、程序日志和报告文件。

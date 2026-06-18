# v2 分布式演进设计

## 目标

v2 的目标是在当前单机 LSM KV 引擎之上，逐步演进出一个学习型分布式 KV 系统。

核心路线：

```text
gRPC 服务化
-> Raft 复制一致性
-> 一致性哈希分片
-> 多 shard group
-> client 路由和 leader 定位
-> snapshot、恢复和运维能力
```

当前 `kv.DB` 继续作为本地存储引擎，不在第一步改写 WAL、MemTable、SSTable、Flush 和 Compaction 的核心逻辑。

## 总体架构

阶段 4 之前的基础形态：

```text
Client
  |
  | gRPC Put/Get/Delete
  v
kv-node
  |
  | shard.Router: key -> shard
  v
Raft Group
  |
  | committed command
  v
local kv.DB
```

阶段 5 之后的目标形态：

```text
Business code
  |
  | client.Put/Get/Delete
  v
RoutedClient
  |
  | key -> shardID -> groupID -> leader node
  v
kv-node
  |
  | local group store
  v
Raft Group
  |
  | committed command / snapshot
  v
local kv.DB
```

职责边界：

- `internal/engine/*`：单机 LSM 引擎内部组件。
- 根包 `kv`：单机嵌入式 API。
- `internal/server/grpc`：网络协议适配，不直接实现存储算法。
- `internal/raft`：复制日志、leader 判断、FSM apply。
- `internal/shard`：key 到 shard、shard 到 raft group 的路由。
- `internal/client`：客户端连接、重试、leader 缓存和后续智能路由。

## 阶段路线

### 阶段 1：gRPC 单节点服务

目标：

- 新增 `KVService`，通过 gRPC 暴露 `Put`、`Get`、`Delete`。
- 新增 `cmd/kv-node` 启动单节点服务。
- 服务内部持有一个本地 `*kv.DB`。
- 提供 `internal/client` 封装和 `example/grpc` 客户端示例。
- 通过 `test/grpc_test.go` 覆盖真实 TCP gRPC 端到端路径。

建议目录：

```text
api/kv/v1/kv.proto
gen/kv/v1/
cmd/kv-node/
internal/server/grpc/
internal/client/
example/grpc/
```

数据流：

```text
gRPC client -> KVService -> kv.DB -> WAL/MemTable/SSTable
```

运行方式：

```bash
go run ./cmd/kv-node --addr 127.0.0.1:9001 --dir /tmp/kv_store_demo/grpc_node
go run ./example/grpc --addr 127.0.0.1:9001
```

暂不实现：

- Raft
- 分片
- 节点发现

### 阶段 2：单 Raft 复制组

目标：

- 使用 `github.com/hashicorp/raft` 接入单个 Raft group。
- 所有 key 仍属于同一个复制组。
- 写请求必须先进入 Raft log，commit 后再 apply 到 `kv.DB`。
- 读请求第一版只走 leader，避免 stale read 复杂度。
- `cmd/kv-node` 支持 `standalone` 和 `raft` 两种模式。

写入路径：

```text
client Put
-> gRPC leader
-> raft.Apply(command)
-> Raft 多数派提交
-> FSM.Apply(command)
-> kv.DB.Put/Delete
-> 返回结果
```

关键设计点：

- command 复用单机 `record` 二进制格式：`PUT` / `DELETE` + key + value。
- follower 收到请求时返回 `NotLeader`，gRPC 映射为 `codes.Unavailable`，client 可通过 `client.IsNotLeader(err)` 识别。
- 读写请求第一版都只由 leader 承担；follower 不转发。
- 每个节点有独立数据目录，raft 模式下区分 engine 数据和 raft 数据。

raft 模式本地目录：

```text
<node-dir>/
  engine/          # kv.DB 数据：WAL、SSTable
  raft/
    raft.db        # Raft log + stable metadata
    snapshots/     # 预留；阶段 2 不实现 snapshot restore
```

raft 模式使用静态 JSON 配置：

```json
{
  "nodes": [
    {
      "id": "n1",
      "client_addr": "127.0.0.1:9001"
    },
    {
      "id": "n2",
      "client_addr": "127.0.0.1:9002"
    },
    {
      "id": "n3",
      "client_addr": "127.0.0.1:9003"
    }
  ],
  "shard_config": {
    "shard_count": 4,
    "virtual_nodes": 8
  },
  "shards": [
    {
      "id": 0,
      "group_id": "g1"
    },
    {
      "id": 1,
      "group_id": "g1"
    },
    {
      "id": 2,
      "group_id": "g1"
    },
    {
      "id": 3,
      "group_id": "g1"
    }
  ],
  "groups": [
    {
      "id": "g1",
      "replicas": [
        {
          "node_id": "n1",
          "raft_addr": "127.0.0.1:10001"
        },
        {
          "node_id": "n2",
          "raft_addr": "127.0.0.1:10002"
        },
        {
          "node_id": "n3",
          "raft_addr": "127.0.0.1:10003"
        }
      ]
    }
  ]
}
```

三节点本地启动示例：

```bash
go run ./cmd/kv-node \
  --mode raft \
  --node-id n1 \
  --cluster ./cluster.json \
  --bootstrap \
  --dir /tmp/kv_store_demo/n1

go run ./cmd/kv-node \
  --mode raft \
  --node-id n2 \
  --cluster ./cluster.json \
  --dir /tmp/kv_store_demo/n2

go run ./cmd/kv-node \
  --mode raft \
  --node-id n3 \
  --cluster ./cluster.json \
  --dir /tmp/kv_store_demo/n3
```

`--bootstrap` 只用于新集群初始化。它会在空 Raft state 上写入初始 voter 配置；已有 `raft.db` 时会跳过重复 bootstrap。

暂不实现：

- 多 shard
- 自动 snapshot restore 到 LSM 文件集
- follower 转发请求
- client 自动重定向到 leader

### 阶段 3：一致性哈希静态分片

目标：

- 引入固定 shard 数量，例如 16 或 64。
- 使用一致性哈希或 hash ring 计算 `key -> shardID`。
- 第一版 shard 配置静态写在配置文件中。
- gRPC 请求在进入 RaftStore 前先经过 `ShardStore` 和 `Router`。

路由模型：

```text
key -> shardID -> shard group -> RaftStore
```

注意：一致性哈希应该映射到 shard 或 shard group，不直接映射到单个节点。

当前实现：

- `internal/shard.HashRing`：固定 `shard_count` 和 `virtual_nodes`，计算 `key -> shardID`。
- `internal/shard.Router`：静态映射 `shardID -> groupID`。
- `internal/shard.ShardStore`：在 gRPC store 前按 key 路由到对应 group store。
- `cmd/kv-node` 支持在 `cluster.json` 中配置 `shard_config`、`shards` 和 `groups`。
- 如果配置了 shard，raft 模式请求路径变为：

```text
gRPC
-> ShardStore
-> Router.RouteKey(key)
-> groupID
-> RaftStore
-> Raft group
```

阶段 3 仍然只有一个物理 Raft 复制组。阶段 4 已经把配置模型扩展为 `groups` 拓扑，并让 `kv-node` 能按当前节点参与的 group 启动多套本地 runtime；三节点多 group 端到端路径已由 `test/raft_grpc_test.go` 覆盖。

暂不实现：

- 每个 shard group 独立 Raft 复制组
- group leader 定位

### 阶段 4：多 shard group

当前实现：

- 每个 shard group 拥有独立 Raft 复制组。
- 每个 shard group 独立 apply 到自己的本地存储命名空间。
- 一个节点可以承载多个 shard replica。
- `cmd/kv-node` 会按 `groups[].replicas` 找出当前节点参与的所有 group，并为每个 group 创建一套本地 runtime。

当前本地目录形态：

```text
data/
  engine/
    group-g1/
    group-g2/
  raft/
    group-g1/
      raft.db
      snapshots/
    group-g2/
      raft.db
      snapshots/
```

请求路径：

```text
gRPC Put/Get/Delete
-> ShardStore
-> Router.RouteKey(key)
-> shardID -> groupID
-> 当前节点本地的 group RaftStore
-> 对应 group 的 leader 检查
-> 对应 group 的 Raft log commit
-> 对应 group 的 FSM.Apply
-> 对应 group 的 kv.DB
```

当前边界：

- 客户端需要通过 `RoutedClient` 把请求发到目标 group 的 leader 所在节点。
- 如果当前节点不承载目标 group，本地 `ShardStore` 会返回 group not found。
- `Flush` 和 `Compact` 是管理操作，当前 `ShardStore` 会对本节点承载的所有 group 逐个执行。

### 阶段 5：client 路由和 leader 定位

当前实现：

- `internal/cluster` 提供共享 `cluster.json` 配置模型、校验和 `Router` 构建能力。
- `internal/client.RoutedClient` 读取静态 cluster 配置，复用与服务端一致的 `HashRing` 和 `Router`。
- `RoutedClient` 能根据 `key -> shardID -> groupID` 找到目标 group。
- `RoutedClient` 维护每个 group 的 leader 缓存，优先把请求发到目标 group leader。
- 请求遇到 `NotLeader` 时，会使用服务端返回的 leader hint 更新缓存并重试。

新增目录：

```text
internal/client/
  routed_client.go      # key 到 group，再到目标节点的封装
internal/cluster/
  config.go             # 服务端和 client 共享的静态配置解析
```

配置模型复用阶段 4 的 `nodes`、`shard_config`、`shards` 和 `groups`。client 会从配置中拿到：

- `nodes[].client_addr`：用于发 gRPC 请求。
- `groups[].replicas[].node_id`：用于知道一个 group 有哪些候选节点。
- `shards[].group_id`：用于知道一个 shard 属于哪个 group。

写入路径：

```text
business code
-> client.Put(key, value)
-> Router.RouteKey(key)
-> groupID
-> leader cache[groupID]
-> gRPC Put(target leader)
-> success
```

遇到 leader 错误时：

```text
gRPC Put(target node)
-> NotLeader(leader id/addr)
-> 更新 leader cache
-> 对新 leader 重试
```

公开入口：

- `client.NewRoutedClient(ctx, cluster.Config)`：从已加载配置创建 routed client。
- `client.LoadRoutedClient(ctx, clusterPath)`：从 `cluster.json` 加载配置并创建 routed client。
- `RoutedClient.Put/Get/Delete`：按 key 路由到目标 group leader。
- `RoutedClient.FlushGroup/CompactGroup`：对指定 group leader 执行管理操作。
- `RoutedClient.Flush/Compact`：对配置中的所有 group 逐个执行管理操作。

测试覆盖：

- `internal/client` 覆盖不同 key 路由到不同 group leader。
- `internal/client` 覆盖收到 NotLeader 后根据 leader hint 刷新缓存并重试。

暂不实现：

- 配置热更新。
- group leader 主动推送。

### 阶段 6：snapshot、恢复和运维能力

当前实现：

- Raft log 不能无限增长，需要 snapshot。
- 节点重启后能从 snapshot + Raft log 正确恢复。
- `kv.DB` 支持文件级 snapshot：创建 snapshot 前先 Flush，然后把当前 `.sst` 文件打包。
- `kv.DB` 支持从 snapshot 恢复：关闭现有 WAL/SSTable，清理 engine 目录，解包 `.sst` 文件，重建空 WAL 和 MemTable。
- Raft FSM 已实现 `Snapshot()`、`Persist()` 和 `Restore()`，由 HashiCorp Raft 管理 snapshot 生命周期。
- Raft 节点已打开 snapshot restore，并提供 `Node.Snapshot()` 用于手动触发 snapshot。
- `example/distributed` 提供三节点、两 shard group 的端到端示例：自动启动三个 `kv-node`，通过 `RoutedClient` 做批量 Put/Get 并输出吞吐，停止节点后离线校验每个副本本地 DB 的最终数据。

设计选择：

```text
Raft snapshot
-> FSM.Snapshot()
-> kv.DB CreateSnapshot()
-> 写入 Raft snapshot sink
-> FSM.Restore()
-> kv.DB RestoreSnapshot()
-> Raft replay snapshot 之后的 log
```

文件级 snapshot 内容：

```text
snapshot archive
  0000.sst
  0001.sst
  ...
```

当前没有 manifest。恢复时根据 `.sst` 文件名重新加载 SSTable，并创建空 `wal.log`。

测试覆盖：

- DB 层：创建 snapshot，恢复到另一个 DB，验证旧数据被替换、删除标记有效、恢复后可继续写。
- FSM 层：Apply 写入/删除后 snapshot，Persist 到 sink，再 Restore 到新 FSM。
- Raft Node 层：单节点写入，手动触发 `Node.Snapshot()`，删除 engine 数据后重启，验证通过 Raft snapshot 恢复。
- Example 层：`go run ./example/distributed --requests 200` 启动三节点分布式存储系统，对外提供 gRPC 服务，通过客户端完成写入、读取性能统计和多副本最终一致性校验。

后续可补的运维能力：

- `Status`：查看节点承载哪些 group、每个 group 的 leader 和 term。
- `Route`：查看某个 key 对应 shard/group/leader。
- `Metrics`：记录请求数、NotLeader 次数、Raft apply 延迟。
- `Health`：区分 gRPC 存活、Raft 可用、group 是否有 leader。

暂不实现：

- 完整监控系统。
- 自动备份恢复。
- 多版本 snapshot 兼容。
- 新副本通过 leader 发送 snapshot 快速追赶的端到端测试。

## 配置

阶段 4 开始，raft 模式统一使用 `nodes`、`shard_config`、`shards` 和 `groups`。`nodes` 只描述客户端 gRPC 地址，Raft 地址由 `groups[].replicas[].raft_addr` 描述。

```json
{
  "nodes": [
    {
      "id": "n1",
      "client_addr": "127.0.0.1:9001"
    }
  ],
  "shard_config": {
    "shard_count": 1,
    "virtual_nodes": 8
  },
  "shards": [
    {
      "id": 0,
      "group_id": "g1"
    }
  ],
  "groups": [
    {
      "id": "g1",
      "replicas": [
        {
          "node_id": "n1",
          "raft_addr": "127.0.0.1:10001"
        }
      ]
    }
  ]
}
```

多 group 配置示例：

```json
{
  "nodes": [
    {
      "id": "n1",
      "client_addr": "127.0.0.1:9001"
    },
    {
      "id": "n2",
      "client_addr": "127.0.0.1:9002"
    },
    {
      "id": "n3",
      "client_addr": "127.0.0.1:9003"
    }
  ],
  "shard_config": {
    "shard_count": 4,
    "virtual_nodes": 8
  },
  "shards": [
    {
      "id": 0,
      "group_id": "g1"
    },
    {
      "id": 1,
      "group_id": "g1"
    },
    {
      "id": 2,
      "group_id": "g2"
    },
    {
      "id": 3,
      "group_id": "g2"
    }
  ],
  "groups": [
    {
      "id": "g1",
      "replicas": [
        {
          "node_id": "n1",
          "raft_addr": "127.0.0.1:11001"
        },
        {
          "node_id": "n2",
          "raft_addr": "127.0.0.1:11002"
        },
        {
          "node_id": "n3",
          "raft_addr": "127.0.0.1:11003"
        }
      ]
    },
    {
      "id": "g2",
      "replicas": [
        {
          "node_id": "n1",
          "raft_addr": "127.0.0.1:12001"
        },
        {
          "node_id": "n2",
          "raft_addr": "127.0.0.1:12002"
        },
        {
          "node_id": "n3",
          "raft_addr": "127.0.0.1:12003"
        }
      ]
    }
  ]
}
```

`shard_config.shard_count` 表示固定 shard 数量，`virtual_nodes` 表示每个 shard 在哈希环上的虚拟节点数量。`shards` 必须完整覆盖 `0` 到 `shard_count-1`，且每个 shard 必须配置非空 `group_id`。`groups` 是物理 Raft group 拓扑来源：`len(groups) == 1` 表示单 group，`len(groups) > 1` 表示多 group。

当前 runtime 会为本节点参与的每个 group 创建独立的本地 engine 目录、Raft 目录、`kv.DB`、`raft.Node` 和 `RaftStore`。`cmd/kv-node` 的 runtime 测试覆盖真实配置解析和多 group 启动；三节点端到端测试验证不同 key 落入不同 group 的独立 DB/Raft group。

## 测试策略

阶段 1：

- gRPC server 可以启动和关闭。
- `Put` 后 `Get` 返回正确 value。
- `Delete` 后 `Get` 返回空 value。
- `Flush` 后重启服务能恢复数据。
- `Compact` 后保留最新值。
- 重启服务后能恢复数据。

阶段 2：

- 3 节点 Raft group 能选出 leader。
- leader 写入后，多数派提交并 apply。
- follower 写入返回 leader 信息。
- 停掉 1 个 follower 后仍可写入。
- `test/raft_grpc_test.go` 覆盖 3 节点 Raft + gRPC 端到端路径。

阶段 3：

- 相同 key 稳定路由到同一 shard。
- 不同 key 能分布到不同 shard。
- shard 配置错误时返回明确错误。
- 带 shard 配置的 gRPC 请求可以经过 `ShardStore` 后正常写入单 Raft group。
- `test/raft_grpc_test.go` 覆盖三节点 Raft + gRPC + ShardStore 的端到端路径。

阶段 4：

- 多 shard group 独立写入和读取。
- 每个 group 都有独立 Raft leader、Raft log 和本地 DB。
- 每个 group 的本地数据目录相互隔离。
- `cmd/kv-node` runtime 测试覆盖一个进程承载多个 group。
- `test/raft_grpc_test.go` 覆盖三节点、两个 shard group 的端到端路径。

阶段 5：

- client 按 key 计算 shard 和 group。
- client 能把不同 group 的 key 发到不同 group leader。
- client 收到 NotLeader 后能使用 leader hint 更新缓存并重试。
- leader 切换后，client 能在有限重试内恢复成功。
- client 配置与服务端配置不一致时返回明确错误。

阶段 6：

- Raft log 达到阈值后能生成 snapshot。
- 节点重启能通过本地 engine 数据和 Raft log 恢复。
- 删除本地 engine 数据后，节点能通过 snapshot restore 恢复。

## 实现原则

- 每个阶段先写设计、伪代码和测试点，再实现。
- 不把分布式逻辑写进 `internal/engine/*`。
- `kv.DB` 继续保持单机引擎语义。
- Raft commit 前不修改本地 `kv.DB`。
- 当前路线保持静态配置，优先覆盖请求路由、复制一致性和恢复能力。

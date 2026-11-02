# Actor 路径：从定时落库到可恢复的内存资产服务

这个版本保留原来的同步写库基线，新增可对照的 actor 服务。目标是学习游戏资产服务中的执行顺序、持久化确认、写入合并、恢复和过载处理；没有宣称已经具备大规模集群的容量或高可用能力。

选型依据及官方源码见 [research.md](research.md)，本机实际验证与测量记录见 [validation.md](validation.md)。

## 两个入口分别学什么

| 项目 | 同步基线 `cmd/server` | 内存路径 `cmd/actor-server` |
| --- | --- | --- |
| 默认端口 | `127.0.0.1:8080` | `127.0.0.1:8081` |
| 默认数据库 | `game_assets` | `game_assets_actor` |
| 请求确认 | WAL → MongoDB 更新 → WAL 完成标记 | WAL 结果事件落盘 → 发布内存状态 → 返回 |
| 读取资产 | MongoDB | 所属 shard 的内存状态 |
| MongoDB 写入 | 每笔业务写入 | 后台合并保存玩家快照 |
| 并发模型 | 一把引擎锁串行写入 | 固定分片，每片单写者、有界 mailbox |
| 对照意义 | 直接观察数据库往返开销 | 观察批量落盘、快照合并和积压背压 |

两个入口保留相同的玩家、奖励、购买、目录接口。使用不同数据库和日志路径，不要让两个模型共同修改一套玩家状态。

## 运行 actor 服务

以下命令从 `wal-demo/` 执行，使用已有云环境检出和本机 MongoDB：

```bash
bash scripts/setup.sh
bash scripts/mongo.sh
source scripts/env.sh
go build -o bin/actor-server ./cmd/actor-server
go build -o bin/loadgen ./cmd/loadgen
./bin/actor-server
```

另一个终端执行：

```bash
cd /workspace/GPT/wal-demo
API_URL=http://127.0.0.1:8081 python3 scripts/smoke.py
curl --fail http://127.0.0.1:8081/healthz
curl --fail http://127.0.0.1:8081/debug/stats
curl --fail http://127.0.0.1:8081/metrics
```

API 默认只绑定本机。若绑定非 loopback 地址，程序要求 `APP_API_TOKEN` 至少 16 个字符；除了 `/livez`，请求都必须带 `Authorization: Bearer ...`。`scripts/smoke.py` 和 loadgen 会读取同名环境变量。不要将真实 token 写进文档、日志或提交。

共享 bearer token 仅是演示服务入口保护：它没有玩家身份绑定、玩家级授权、充值凭证验证和管理员权限划分。也没有内置 TLS。即使配置了 token，也不能把它直接当作公网游戏资产 API。

## 默认配置与容量边界

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `HTTP_ADDR` | `127.0.0.1:8081` | 监听地址 |
| `MONGO_URI` | `mongodb://127.0.0.1:27017` | 快照数据库连接 |
| `MONGO_DATABASE` | `game_assets_actor` | actor 独立数据库 |
| `WAL_DIRECTORY` | `./data/actor` | 布局元数据、目录锁与分片 WAL |
| `ACTOR_SHARDS` | `4` | 固定分片数 |
| `ACTOR_QUEUE` | `256` | 每片等待命令数上限 |
| `ACTOR_BATCH` | `64` | 每片单批命令数上限 |
| `ACTOR_BATCH_WINDOW` | `1ms` | 收集一批命令的最长等待窗口 |
| `ACTOR_FLUSH_INTERVAL` | `1s` | 每片后台快照周期 |
| `ACTOR_MAX_PLAYERS` | `10000` | 每片驻留玩家记录上限 |
| `ACTOR_MAX_RECEIPTS` | `1000` | 每玩家幂等回执上限 |
| `ACTOR_WAL_SEGMENT_BYTES` | `67108864` | 每片日志段软阈值，64 MiB；CLI允许1024..1073741824字节 |
| `ACTOR_MAX_UNCHECKPOINTED` | `10000` | 每片未被快照确认的事件上限 |
| `APP_API_TOKEN` | 未设置 | 可选入口 token；非 loopback 监听时必须设置 |

已有数据不能通过直接修改 `ACTOR_SHARDS` 来重分片。日志目录与 MongoDB 都记录布局，布局不一致会拒绝启动，需要显式迁移。恢复后的玩家数和回执数也必须满足配置容量。

队列满、未落库积压达到上限、玩家容量或回执容量耗尽时，服务显式返回 `429`。回执不会悄悄过期以腾空间，因为那会改变旧请求重试的安全性；本版本也没有冷玩家淘汰。容量到顶是需要处理的运行状态，不能靠无限调大上限长期解决。

## 一笔请求如何执行

玩家 ID 经过确定的哈希映射到 shard。同一玩家的读取和写入都进入同一片队列，由该片的 goroutine 串行执行。不同片可以并行，但一个热点玩家仍然串行。

```text
玩家命令 → 有界 mailbox → 在隔离副本上决定结果
                              ↓
                 一批确定结果事件写入 WAL + fsync
                              ↓
                     发布内存状态并返回
                              ↓
                  定时捕获不可变玩家快照
                              ↓
          MongoDB 保存状态 + 回执 + seq，单文档原子提交
                              ↓
                  WAL 标记已被快照覆盖的事件
```

当前批次的内存状态在落盘成功前不会发布。单批中的多笔资产事件共享一次同步落盘；它们都等待该次同步成功后才能收到确认。增加批量窗口可能提高合并率，也会增加低负载时的等待延迟，必须结合 p99 测量选择。

事件包含 schema、已决定的玩家状态和幂等回执。恢复直接应用这些结果，不会用当前价格重新执行历史购买。余额不足等业务拒绝也有回执；相同请求 ID 和参数返回原结果，不因之后领到金币而改变。相同 ID 换参数返回冲突。

批量落盘不是每秒异步 fsync：这个版本仍然要求已确认的事件先完成本地持久化。调用方超时或断开连接时，已经入队的命令仍可能执行，结果属于不确定状态；必须用原请求 ID 和原参数重试，不能假定失败后换新 ID。

## 后台快照为什么不会漏掉新变动

每个快照携带该玩家最后一个 WAL 事件的 `seq`。MongoDB 将资产、回执和 `seq` 一起写入，同一个玩家的旧序号不会覆盖新序号；批量保存可能部分成功，重试必须保持单调。

例如开始保存 `seq=100` 时，玩家又产生了 `seq=102`：保存 100 成功只确认 100，内存的 102 仍然是脏状态，下次继续保存。不能把“这轮写库成功”粗略理解成“这个玩家已经没有新变动”。

重启先加载 MongoDB 快照，再应用快照序号之后仍待处理的 WAL 结果事件。如果数据库已保存、WAL 尚未记录快照完成，序号检查会避免重复应用。本地布局中的随机 journal ID 还会绑定到 MongoDB 元数据，防止把另一个日志历史误接到当前快照库。日志水位与快照不匹配、日志身份不匹配、布局改变、完整记录损坏会拒绝启动，而不是继续猜测资产状态。已有本地历史而数据库身份丢失时会拒绝重新绑定；启动还会流式检查已完成事件是否均有对应的、不落后的 MongoDB 快照，防止误删快照后静默丢数据。该身份绑定不是分布式租约或 fencing。

MongoDB 在启动时必须可访问；服务启动后，普通资产请求和 `/healthz` 不逐次 ping MongoDB。数据库暂时不可用时，已加载的内存服务可以继续处理，WAL 积压增加；达到配置上限后拒绝新的资产写入。快照恢复成功并确认进度后，积压减少。这里并没有承诺数据库长期不可用时无限接纳请求。

## 看哪些指标

`/livez` 只表达 HTTP 进程能响应，不表示能够持久化资产。`/healthz` 检查引擎故障及未落库积压预算；它不等同于数据库连通性检查，也不是整套生产 readiness 探针。

`/debug/stats` 提供每片的：

- `wal_segments`、`wal_bytes`、`wal_reclaimed_segments`、`wal_reclaimed_bytes`：保留段与进程本轮成功回收的数量和字节；不含元数据大小，不是设备物理空间核算。

- `queue`、`players`、`pending`：排队、内存驻留和未确认快照的规模。
- `pending_age_millis`：本次持续积压区间的年龄，不是对每条事件时间戳计算的精确最大延迟。
- `wal_records`、`wal_batches`：运行期间事件与批次数；两者比值可以观察合并效果。
- `checkpoints`、`checkpoint_errors`、`last_checkpoint_error`：后台保存情况。
- `last_checkpoint_unix_milli`、`fault`：最近快照成功时间与不可继续写入的故障。

顶层 `accepted` 统计进入 mailbox 的命令，包含读取，不能直接当成成功资产写入数；`overloaded` 记录队列或积压拒绝，不包含所有业务容量拒绝。各计数在进程重启后重置。

`/metrics` 提供 HTTP 请求数、错误数、累计耗时、goroutine 数，以及每分片的队列、积压、积压年龄、WAL 批次、检查点和错误计数，采用 Prometheus 文本格式。累计耗时不是延迟直方图，不能直接从中计算 p99；p50/p95/p99 由 loadgen 采样输出。生产系统还应补充磁盘空间、fsync 延迟、快照时延直方图和告警。

## 动手练习：确认成功后 kill -9

可以先运行真正启动子进程并发送 SIGKILL 的集成测试：

```bash
source scripts/env.sh
MONGO_TEST_URI=mongodb://127.0.0.1:27017 go test -race -count=1 \
  -run TestAcknowledgedAssetsSurviveSIGKILL -v ./cmd/actor-server
```

测试在强制终止前检查 MongoDB 快照数仍为零，恢复后验证创建、奖励、失败购买的原状态码以及 70 金币和一把剑，确认没有重复应用。手动练习时创建自己独立的数据库、日志目录和端口，不要停止共享 MongoDB，也不要删除现有 Demo 数据。

在 `wal-demo/` 下启动独立实例：

```bash
case_id="$(date +%s)_$$"
drill_db="actor_drill_${case_id}"
drill_wal="./data/drill-${case_id}"
HTTP_ADDR=127.0.0.1:8083 MONGO_DATABASE="$drill_db" WAL_DIRECTORY="$drill_wal" \
  ACTOR_FLUSH_INTERVAL=1h ./bin/actor-server >"/tmp/actor-drill-${case_id}.log" 2>&1 &
actor_pid=$!
```

等待该实例 `/healthz` 成功后创建玩家并领取奖励。只有 curl 明确返回成功，才执行 kill：

```bash
curl --fail http://127.0.0.1:8083/players -H 'Content-Type: application/json' \
  -d '{"player_id":"drill","request_id":"create"}'
curl --fail http://127.0.0.1:8083/players/drill/rewards -H 'Content-Type: application/json' \
  -d '{"request_id":"reward-1","amount":25}'
# 确认上面的成功响应余额为 125；只终止自己刚启动的 PID。
kill -KILL "$actor_pid"
wait "$actor_pid" || true
```

使用原来的 `$drill_db`、`$drill_wal`、分片数和端口重新运行同一启动命令。再次读取玩家应为 125；重试 `reward-1` 和金额 25 应返回 `replayed=true`，余额仍为 125。`ACTOR_FLUSH_INTERVAL=1h` 是为了在首次周期保存前演练本地 WAL 恢复，正常关闭会尝试保存快照，不能代替这个强制终止练习。

这个实验验证的是进程被杀后保留本地磁盘的恢复；没有模拟断电、文件系统损坏、云盘故障或整台机器丢失。

## 动手练习：对照吞吐和长尾延迟

先在两个独立终端运行同步基线 `./bin/game-server` 和 actor `./bin/actor-server`。在第三个终端依次运行，避免两组负载互相争抢资源：

```bash
./bin/loadgen -url http://127.0.0.1:8080 -players 100 -requests 2000 -concurrency 32
./bin/loadgen -url http://127.0.0.1:8081 -players 100 -requests 2000 -concurrency 32
```

负载工具为每轮生成独立玩家，统计创建、奖励和最终校验读取；输出成功吞吐、所有已尝试请求的 p50/p95/p99、HTTP 状态、网络与协议错误，以及最终余额是否匹配。它不自动重试；发生网络结果不确定时，校验可能不匹配，需要调查，不能简单把错误样本删除。命令未完整成功会以非零状态退出。

这是单机闭环 HTTP 合成负载，客户端、服务端和 MongoDB 可能共享 CPU 与磁盘。它没有开放环流量模型、真实玩家行为、跨机网络、长时间稳定性和故障接管测试。不要把某次本机成功请求数解读成在线人数或集群容量。还应分别观察少量热点玩家与均匀玩家分布，确认热点没有被平均值掩盖。

## 仍然缺少哪些生产能力

当前学习实现有本机日志目录锁、固定分片和快照水位检查，但没有分布式 ownership/fencing、双主隔离、跨节点迁移和复制日志。新的独立日志目录访问已绑定数据库会被 journal ID 检查拒绝；但如果把同一目录及身份复制到另一台机器并同时运行，仍可能形成双主。文件锁、身份绑定与序号不能代替分布式 fencing，也不能解决 split brain。

已实现 [日志分段与安全回收](wal-reclamation.md)，同步基线仍不回收。尚无回执生命周期、玩家淘汰、跨玩家交易、充值对账、完整身份认证和完整生产告警。玩家快照中的回执会增长，当前采用容量上限显式拒绝，不能理解成解决了长期运营问题。

成功响应承诺的是所用文件系统和存储契约下的本地日志落盘。只有本地盘还在，恢复协议才有依据；已完成事件不会用来重建丢失的整个 MongoDB。生产选型应先明确 RPO/RTO，再决定可靠持久卷、远端事务或复制日志，而不是单纯调高并发数。

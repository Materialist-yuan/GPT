# 游戏资产 Demo：Go + MongoDB + WAL

一个可在本地运行的游戏资产后端：创建玩家后获得 **100 金币**，可以领取金币奖励、购买道具、查询余额和背包。写操作经过应用层 WAL，并通过 MongoDB 中的幂等回执避免重复扣款或发奖。

这是用于理解资产写入和故障恢复的小型 Demo。

## 生产设计学习入口

现在保留两个可以运行对照的版本：下文的 `cmd/server` 是同步写 MongoDB 的一致性基线；新增的 `cmd/actor-server` 采用分片单写者、内存资产、批量 WAL 和后台 MongoDB 快照。

- [调研依据](docs/research.md)：实际核对 Orleans、Nakama、Redis 官方源码，解释取舍。
- [Actor 学习与运行指南](docs/actor-guide.md)：启动、背压、恢复协议、指标、故障演练与能力边界。
- [验证及性能测量记录](docs/validation.md)：真实数据库测试、SIGKILL 恢复和单机对照数据。
- [WAL / 无 WAL 基准与资源](docs/wal-benchmark.md)：相同 Actor 路径的延迟成本、批量同步、CPU 与 RSS。

```bash
# 在 wal-demo 目录，准备环境后运行新版本（默认端口 8081）
bash scripts/setup.sh
bash scripts/mongo.sh
source scripts/env.sh
./bin/actor-server
# 另一终端：API_URL=http://127.0.0.1:8081 python3 scripts/smoke.py
```

新增路径是学习生产机制的单机实现，尚不提供跨节点复制、分布式 fencing、玩家迁移、日志压缩或完整身份认证。下面的逐笔 MongoDB 写入顺序只描述同步基线，Actor 的确认点与恢复协议请阅读专门指南。

## 启动

当前安装脚本面向此云环境的 **Linux amd64**，使用固定版本 Go 1.27.3，并在下载时验证官方 SHA-256。MongoDB 通过仓库级共享 Docker 服务运行，使用固定镜像摘要和独立数据卷。

```bash
cd /workspace/GPT/wal-demo
bash scripts/setup.sh
bash scripts/mongo.sh
source scripts/env.sh
./bin/game-server
```

`setup.sh` 安装或复用 `/workspace/.toolchains/go1.27.3` 下的 Go，校验模块并生成 `bin/game-server`；`env.sh` 让后续终端命令使用这个 Go 工具链。`mongo.sh` 可重复运行，它会启动仓库内 [共享 MongoDB](../infra/mongo/README.md) 并等待 ping 成功。共享数据保存在 Docker 卷 `gpt-demo-mongo-data`；`wal-demo` 使用其中的 `game_assets` 数据库，其他 Demo 应使用不同数据库名。

在另一个终端执行功能检查：

```bash
cd /workspace/GPT/wal-demo
python3 scripts/smoke.py
```

检查脚本每次创建一个新玩家，检查健康状态、商品目录、创建玩家、购买道具、发奖、重复请求、余额不足和幂等冲突。

默认配置如下；也可以通过进程环境变量覆盖，参见 `.env.example`。程序不会自动加载 `.env` 文件。

| 变量 | 默认值 | 用途 |
| --- | --- | --- |
| `HTTP_ADDR` | `127.0.0.1:8080` | API 监听地址 |
| `MONGO_URI` | `mongodb://127.0.0.1:27017` | MongoDB 连接地址 |
| `MONGO_DATABASE` | `game_assets` | wal-demo 在共享 MongoDB 中使用的数据库 |
| `WAL_PATH` | `./data/assets.wal` | WAL 文件路径，相对路径基于启动目录 |

停止 API 使用 `Ctrl+C`。再次运行 `./bin/game-server` 会先恢复 WAL，再开始监听请求。执行 `bash scripts/mongo.sh down` 可以停止共享 MongoDB 容器，数据卷会保留；以后运行 `bash scripts/mongo.sh` 恢复服务。

## 调用接口

下面示例使用固定玩家 `alice`。每个新的业务操作应使用一个新的 `request_id`；重新执行相同示例会返回原操作的结果。

```bash
# 查看可购买道具：potion=30、sword=80、shield=50 金币
curl -sS http://127.0.0.1:8080/catalog

# 创建玩家，初始 100 金币；HTTP 201
curl -sS http://127.0.0.1:8080/players \
  -H 'Content-Type: application/json' \
  -d '{"player_id":"alice","request_id":"create-1"}'

# 购买一把剑，剩余 20 金币
curl -sS http://127.0.0.1:8080/players/alice/purchases \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"buy-sword-1","item":"sword"}'

# 重复相同请求：返回 replayed=true，背包仍然只有一把剑
curl -sS http://127.0.0.1:8080/players/alice/purchases \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"buy-sword-1","item":"sword"}'

# 奖励 50 金币，余额变为 70
curl -sS http://127.0.0.1:8080/players/alice/rewards \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"reward-1","amount":50}'

# 查看当前状态
curl -sS http://127.0.0.1:8080/players/alice

# 同一 request_id 改为另一笔业务：HTTP 409，idempotency_conflict
curl -sS http://127.0.0.1:8080/players/alice/rewards \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"reward-1","amount":51}'
```

当前玩家状态示例：

```json
{"id":"alice","coins":70,"inventory":{"sword":1},"version":3}
```

| 方法 | 路径 | 请求内容 |
| --- | --- | --- |
| `GET` | `/healthz` | 检查 MongoDB 连通性及引擎是否需要恢复 |
| `GET` | `/catalog` | 查询商品及金币价格 |
| `POST` | `/players` | `player_id`、`request_id` |
| `GET` | `/players/{id}` | 查询当前玩家资产 |
| `POST` | `/players/{id}/rewards` | `request_id`、`amount` |
| `POST` | `/players/{id}/purchases` | `request_id`、`item` |

玩家 ID 允许 1–64 个字母、数字、下划线或连字符；`request_id` 使用相同字符集，长度为 1–128。单次奖励必须是 1–1,000,000,000 的整数，余额上限为 1,000,000,000,000。请求体上限为 8 KiB，未知字段和多段 JSON 会被拒绝。

幂等键的作用域是 **同一玩家的全部写操作**。相同玩家、相同 `request_id`、相同参数会返回原来的 HTTP 状态和资产快照，并设置 `replayed=true`；这份快照可能早于玩家当前状态，要看最新状态请调用查询接口。相同键换用不同参数或操作类型会返回 `409 idempotency_conflict`。

业务拒绝也保存回执。例如某次购买因余额不足返回 `409 insufficient_coins`，即使之后领到了奖励，重试原来的购买请求仍返回原来的拒绝结果；要发起新的购买应使用新的 `request_id`。其他业务错误包括 `404 player_not_found`、`409 player_exists`、`409 balance_limit`。

## WAL 和资产一致性

这里的 WAL 是应用自己维护的顺序日志，MongoDB 同时使用自己的持久化机制。一次有效写请求的顺序为：

1. 将完整操作追加到本地 WAL，调用 `fsync` 后才访问数据库。
2. 读取 MongoDB 中的玩家文档，通过版本号比较并更新（CAS），将金币、背包、版本及幂等回执放在同一次原子文档更新中提交。写关注配置为 `majority` 和 `journal=true`。Demo 不需要 MongoDB 副本集事务。
3. 将完成标记追加到 WAL 并再次 `fsync`，然后向调用方返回结果。

启动时按原顺序重放未完成的 WAL 操作。如果崩溃发生在数据库提交前，会重新执行操作；如果数据库已提交但 WAL 尚未写入完成标记，数据库中的回执会返回原结果，资产不会再次变动。

WAL 使用文件排他锁保证同一文件只有一个进程打开，并通过 CRC32 检查完整日志记录。仅末尾未写完整的记录会被截断；完整记录校验失败时拒绝启动，避免带着损坏日志继续写资产。

数据库响应丢失或落盘失败时，请求可能返回 `503`，但业务也可能已经提交。引擎会停止接收后续资产写入，健康检查也会失败。恢复数据库或磁盘后重启服务，由启动流程完成重放。之后可以查询当前资产；若要重试原操作，必须使用**原来的 `request_id`**。不要因为返回 `503` 就换新 ID 发放奖励或扣款。

## 开发和验证

```bash
cd /workspace/GPT/wal-demo
source scripts/env.sh
bash scripts/mongo.sh

# 包含真实 MongoDB 集成测试，以及数据竞争检查
MONGO_TEST_URI=mongodb://127.0.0.1:27017 go test -race ./...
go vet ./...

# 修改代码后重新构建
go build -o bin/game-server ./cmd/server
```

没有设置 `MONGO_TEST_URI` 时，MongoDB 集成测试会跳过；普通 `go test` 全绿不能代表数据库链路已被验证。集成测试为每次运行创建独立测试数据库，完成后清理它。测试覆盖 WAL 持久化与损坏检测、故障后重放、幂等结果、并发重复奖励及 HTTP 输入校验等行为。

目录分工：

```text
cmd/server/       配置、启动恢复、HTTP 服务和优雅退出
internal/wal/     顺序日志、校验、fsync、文件锁、恢复
internal/game/    资产操作、MongoDB 原子更新、幂等回执
internal/httpapi/ JSON API 与请求校验
scripts/         Go/MongoDB 环境脚本及功能检查
```

## Demo 的边界

- 没有登录鉴权；奖励接口也没有管理员权限校验。API 和 MongoDB 默认仅绑定本机地址，适合本地开发，不能直接作为公网游戏后端使用。
- 采用单写入进程，资产写操作由引擎串行处理。文件锁只保护同一个 WAL 文件，不是多实例协调机制。
- 不含跨玩家转账或交易，也不提供多个玩家文档之间的原子事务。
- WAL 没有压缩或归档，幂等回执也没有清理策略。两者会持续增长，且 MongoDB 单文档有 **16 MiB** 上限；长期运行前需要另外设计日志和回执生命周期。
- 必须同时保留 WAL 所在的持久磁盘和 MongoDB 数据卷。恢复只重放未完成记录，**已完成 WAL 不是数据库备份**；数据库丢失时，现有完成记录不会自动重建全部资产。
- 商品目录在代码中固定。存在未完成 WAL 时不要更改商品价格或操作语义，应先使用原版本完成恢复。

已有云任务使用隔离的现有检出目录即可，无需额外创建 Git worktree。

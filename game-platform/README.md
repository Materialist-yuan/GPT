# 游戏登录服务、TCP 网关与压测机器人

一套可运行的 Go 后端基础：玩家注册与登录、区服查询、角色创建与选择、一次性入场票据、自定义 TCP 长连接，以及覆盖整条接入流程的压测机器人。账号和角色保存在 PostgreSQL，会话、票据、限流和在线租约使用 Redis。

网关已实现身份绑定、心跳与 Echo 消息通路。真实的场景、战斗、背包等逻辑需要继续接入；生产部署工作见 [上线说明](docs/production.md)。

## 本地启动

需要 Docker Engine、Docker Compose v2、OpenSSL。Docker 构建使用 Go 1.26.10，本地开发需要 Go 1.26 或更新版本。

```sh
./scripts/init-env.sh
docker compose up --build -d
docker compose ps
curl --retry 20 --retry-delay 1 --retry-connrefused -fsS http://127.0.0.1:8080/readyz
curl --retry 20 --retry-delay 1 --retry-connrefused -fsS http://127.0.0.1:9091/readyz
```

初始化脚本生成随机数据库密码、Redis 密码和 JWT 密钥，写入 `.env`；已有 `.env` 会保留。**直接运行脚本，不要先复制 `.env.example`**，模板中的占位符需要由脚本替换。Compose 先等待 PostgreSQL 就绪，执行独立迁移任务，再启动登录服务。

| 服务 | 本地地址 | 用途 |
| --- | --- | --- |
| 登录 API | `http://127.0.0.1:8080` | HTTP 注册、登录与角色操作 |
| 游戏网关 | `127.0.0.1:9000` | 自定义 TCP 协议 |
| 网关管理 | `http://127.0.0.1:9091` | 健康与 Prometheus 指标 |

默认只绑定本机；PostgreSQL 和 Redis 不映射宿主机端口。`docker compose down` 停止服务并保留数据卷。需要远程客户端接入时，应先完成 TLS 和网络访问控制，再调整绑定地址与 `GATEWAY_PUBLIC_ADDR`。

## 走通玩家登录

以下示例需要 `curl` 和 `jq`。示例账号、密码仅用于本地联调；同名账号或同区服角色已存在时会返回冲突，请换一个名称。

```sh
# 1. 注册账号
curl -fsS http://127.0.0.1:8080/v1/accounts \
  -H 'Content-Type: application/json' \
  -d '{"username":"demo_player","password":"demo-pass-2026"}'

# 2. 登录，保留短期访问令牌与刷新令牌
session=$(curl -fsS http://127.0.0.1:8080/v1/sessions \
  -H 'Content-Type: application/json' \
  -d '{"username":"demo_player","password":"demo-pass-2026"}')
access=$(printf '%s' "$session" | jq -r .access_token)
refresh=$(printf '%s' "$session" | jq -r .refresh_token)

# 3. 查询区服与角色
curl -fsS http://127.0.0.1:8080/v1/servers \
  -H "Authorization: Bearer $access"
curl -fsS 'http://127.0.0.1:8080/v1/roles?server_id=s1' \
  -H "Authorization: Bearer $access"

# 4. 创建角色；已有角色时从查询结果选择 ID 即可
role=$(curl -fsS http://127.0.0.1:8080/v1/roles \
  -H "Authorization: Bearer $access" \
  -H 'Content-Type: application/json' \
  -d '{"server_id":"s1","name":"新手勇士","class":"warrior"}')
role_id=$(printf '%s' "$role" | jq -r .id)

# 5. 选择角色，领取 30 秒有效的一次性入场票据
entry=$(curl -fsS http://127.0.0.1:8080/v1/tickets \
  -H "Authorization: Bearer $access" \
  -H 'Content-Type: application/json' \
  -d "{\"role_id\":$role_id}")
```

`entry` 包含 `ticket`、`expires_in` 和 `gateway`。客户端连接 `gateway`，以 `AUTH` 消息提交票据；收到 `AUTH_OK` 后才可发送心跳与游戏消息。完整帧格式和 Python 最小客户端见 [TCP 协议](docs/protocol.md)。票据只能使用一次，过期或断线重连需要重新申请。

刷新令牌会轮换旧的 Refresh Token；保存响应中的新令牌：

```sh
session=$(curl -fsS http://127.0.0.1:8080/v1/sessions/refresh \
  -H 'Content-Type: application/json' \
  -d "{\"refresh_token\":\"$refresh\"}")
access=$(printf '%s' "$session" | jq -r .access_token)
refresh=$(printf '%s' "$session" | jq -r .refresh_token)

# 退出当前会话，HTTP 令牌立即失效，网关在后续会话检查时断开
curl -fsS -X DELETE http://127.0.0.1:8080/v1/sessions \
  -H "Authorization: Bearer $access"
unset session access refresh entry
```

## 压测网关

机器人先准备账号和角色，再领取票据、连接并认证 TCP 网关，随后持续发送协议消息。准备阶段和网关负载阶段分开计时，报告包含连接、认证、消息、错误、吞吐与延迟分位数。

```sh
# 50 个机器人，5 秒爬坡，负载阶段共 30 秒（含爬坡），每机器人每秒 2 条消息
docker compose --profile tools run --rm robot \
  -bots 50 -duration 30s -ramp 5s -rate 2 \
  -report /reports/run.json
```

结果保存在本地 `reports/run.json`。此命令在 Compose 网络中访问登录服务与网关，不需要修改返回给宿主机客户端的公网地址。Compose 会把 `.env` 中的 `SERVER_ID` 同时传给登录服务、网关和机器人；直接运行机器人时，可用 `-server-id` 指定对应区服。更多参数、阶段含义和压测方法见 [压测说明](docs/loadtest.md)。

本地 `.env` 默认 `AUTH_RATE_LIMIT=600`，便于少量机器人准备账号。注册、登录、刷新共享每 IP 每分钟的认证配额；受保护 API 还有每账号每分钟 600 次配额。较大规模应分批准备或使用独立压测环境，不能仅提高机器人数量就把准备阶段失败归因于网关容量。bcrypt 也会消耗登录服务 CPU；密码计算并发已满时会返回 503 与 `Retry-After: 1`。

## API 与校验

| 方法与路径 | 认证 | 请求 / 响应 |
| --- | --- | --- |
| `POST /v1/accounts` | 无 | `{username,password}` → `{id,username}`，201 |
| `POST /v1/sessions` | 无 | `{username,password}` → `{access_token,refresh_token,expires_in,token_type}` |
| `POST /v1/sessions/refresh` | Refresh Token | `{refresh_token}` → 新令牌对 |
| `DELETE /v1/sessions` | Bearer | 撤销当前会话，204 |
| `GET /v1/servers` | Bearer | `{servers:[...]}` |
| `GET /v1/roles?server_id=s1` | Bearer | `{roles:[...]}` |
| `POST /v1/roles` | Bearer | `{server_id,name,class}` → 角色，201 |
| `POST /v1/tickets` | Bearer | `{role_id}` → `{ticket,expires_in,gateway}` |

- 用户名统一为小写，允许 3–32 个 ASCII 字母、数字或下划线；密码为 12–72 字节。
- 角色名为 2–24 个 Unicode 字母、数字或下划线；职业为 `warrior`、`mage`、`ranger`。
- 每个账号每个区服最多 5 个角色；角色名在区服内唯一。票据只能为当前账号拥有的角色签发。
- JSON 请求拒绝未知字段和多余 JSON 值。认证失败不会区分账号不存在或密码错误。
- 业务 API 错误格式为 `{"error":{"code":"...","message":"...","request_id":"..."}}`；限流返回 429 和 `Retry-After`。

登录服务与网关管理服务均提供 `/healthz`、`/readyz` 和 `/metrics`。存活检查只说明进程可响应；就绪检查会探测所依赖的存储。指标端点输出 Prometheus 文本格式，应通过可信网络抓取。

## 开发与配置

```sh
go mod download
make build
make check
```

`make check` 执行静态检查与竞态测试。单元与流程测试使用内存存储，不需要运行依赖；存储集成测试需要设置 `TEST_DATABASE_URL` 与 `TEST_REDIS_URL`，指向可写的专用测试实例：

```sh
TEST_DATABASE_URL='postgres://user:password@127.0.0.1:5432/test_game?sslmode=disable' \
TEST_REDIS_URL='redis://127.0.0.1:6379/1' \
go test -race -count=1 ./internal/storage
```

未设置时对应集成测试会跳过。测试会执行迁移并创建临时记录，应使用独立测试数据库；不要把线上连接地址填入示例。仓库的 GitHub Actions 工作流会启动 PostgreSQL 和 Redis 测试服务，执行静态检查、竞态与存储集成测试，并构建所有命令。迁移命令为 `bin/migrate`，登录进程不会自动迁移生产数据库。

| 变量 | 默认值 / 要求 | 说明 |
| --- | --- | --- |
| `JWT_SECRET` | 必填，至少 32 字节 | HTTP JWT 签名密钥 |
| `DATABASE_URL` | 登录与迁移必填 | PostgreSQL 连接地址 |
| `REDIS_URL` | 登录必填；网关本机开发有默认值 | 会话、票据、租约和限流 |
| `LOGIN_ADDR` | `:8080` | 登录 HTTP 监听地址 |
| `SERVER_ID` | `s1` | 登录服务和对应网关必须一致 |
| `GATEWAY_PUBLIC_ADDR` | `127.0.0.1:9000` | 返回给客户端的 TCP 接入地址 |
| `AUTH_RATE_LIMIT` | 进程默认 60；本地 Compose 600 | 每 IP 每分钟认证请求配额 |
| `BCRYPT_COST` / `BCRYPT_CONCURRENCY` | `12` / `4` | 密码计算成本与并发上限 |
| `GATEWAY_ADDR` | `:9000` | TCP 监听地址 |
| `GATEWAY_ADMIN_ADDR` | `127.0.0.1:9091` | 管理 HTTP 监听地址 |
| `GATEWAY_MAX_CONNECTIONS` | `10000` | 单进程连接上限，需结合机器容量调节 |
| `GATEWAY_NOFILE` | Compose 默认 `16384` | 容器文件描述符限制，不得超过 Docker 宿主允许的硬限制 |
| `GATEWAY_MAX_BODY` | `65536` | 最大消息体字节数 |
| `GATEWAY_MESSAGES_PER_SECOND` | `100` | 每连接消息速率上限 |
| `GATEWAY_TLS_CERT` / `GATEWAY_TLS_KEY` | 可选，必须同时配置 | 网关原生 TLS 1.3 证书与私钥路径 |

表格描述进程参数；Compose 只传入 `compose.yaml` 中声明的变量。启用原生 TLS、调整其他进程参数或注入证书时，需要相应修改 Compose 环境与只读挂载。普通本地示例使用裸 TCP；公网接入需启用安全传输，客户端也要匹配 TLS 配置。

本次全量测试、依赖扫描和 50 机器人实测结果见 [验证记录](docs/validation.md)。

代码结构与边界见 [架构说明](docs/architecture.md)，协议见 [TCP 协议](docs/protocol.md)，上线与运维工作见 [上线说明](docs/production.md)。

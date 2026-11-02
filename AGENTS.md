# 编码助手开发指引

本文件适用于整个仓库。与用户协作时默认使用中文；代码中的公开标识符使用英文。

## 项目定位和目录

这是游戏资产与持久化机制的 Demo 仓库。先阅读根目录 `README.md` 和 `wal-demo/README.md`。

- `wal-demo/cmd/server/`：配置、启动时恢复、HTTP 服务和退出流程。
- `wal-demo/internal/game/`：资产规则、MongoDB 存储、幂等与写入编排。
- `wal-demo/internal/wal/`：日志格式、校验、同步落盘、文件锁和恢复。
- `wal-demo/internal/httpapi/`：JSON 接口及输入校验。
- `wal-demo/scripts/`：工具链、MongoDB 启动和 HTTP 功能检查。

新 Demo 放入独立子目录。不要将模块、构建产物或数据库文件搬到仓库根目录。当前 Go 模块为 `github.com/Materialist-yuan/GPT/wal-demo`。

## 环境和命令

使用当前隔离环境的已有检出；除非用户要求，不创建 Git worktree。以下命令从仓库根目录进入 `wal-demo/` 后执行：

```bash
cd wal-demo
bash scripts/setup.sh
bash scripts/mongo.sh
source scripts/env.sh
```

云环境里的 `/usr/bin/go` 不是 Go 编译器，先用 `scripts/env.sh` 激活工具链。脚本固定使用 `/workspace/.toolchains` 与 `/workspace/.cache`，面向 Linux amd64。保留 TLS、模块校验和与安装包校验，不用关闭验证来解决安装问题。

```bash
# 构建与静态检查
go build -o bin/game-server ./cmd/server
go vet ./...

# 包含真实 MongoDB 的测试与竞态检测
MONGO_TEST_URI=mongodb://127.0.0.1:27017 go test -race -count=1 ./...

# 启动 API；另一个终端执行功能检查
./bin/game-server
# python3 scripts/smoke.py
```

不要在仓库根目录直接运行 `go test ./...`。未设置 `MONGO_TEST_URI` 会跳过数据库集成测试，报告验证结果时必须区分通过、失败、跳过和未执行。MongoDB 不可用时先检查 `scripts/mongo.sh` 的输出，不通过跳过集成测试来宣称完整验证成功。

仅修改文档时，检查相对链接、目录、命令和事实即可。修改资产逻辑、WAL 或存储时，运行相关测试与完整数据库链路检查；补充能揭示行为错误的测试。

## 必须保持的资产一致性约束

1. 资产写入顺序为：WAL intent 同步落盘 → MongoDB 提交 → WAL complete 同步落盘 → 返回结果。不能提前返回成功。
2. 金币、背包、版本号和幂等回执必须在同一条 MongoDB 文档更新中原子提交，保留版本号 CAS 和持久化写关注。
3. 幂等键作用域是同一玩家的全部写操作。相同键与参数返回原状态和快照；换参数返回冲突。业务拒绝也保存回执，不因后续余额变化而重新执行。
4. 发生结果不确定的写入后，引擎拒绝后续写操作；重启先恢复未完成 WAL，再接收新写入。重试必须保留请求 ID。
5. 保留 WAL 排他锁、逐条校验和与 fsync。只允许截断末尾不完整记录，不能悄悄跳过完整但损坏的日志。
6. 未完成 WAL 使用原代码语义恢复。更改商品价格、操作语义或日志格式前，必须处理与现有日志的兼容性，不能让重放产生另一笔资产变动。

当前是单写者、单玩家文档的示例。文件锁不协调不同 WAL 的多实例，不具备跨玩家事务。扩展转账或交易时，需要单独设计原子性和恢复协议。

## 数据和提交

- 保留 `wal-demo/data/` 和 Docker 卷 `game-assets-mongo-data`，不要为排查问题删除真实资产数据。测试使用独立测试数据库并清理自己的数据。
- WAL 只恢复未完成操作；已完成记录不能自动恢复丢失的 MongoDB。WAL 与回执当前没有清理机制，需考虑磁盘增长和 MongoDB 的 16 MiB 单文档限制。
- 不提交 `bin/`、`data/`、`.env`、凭据、缓存和运行日志；`.env.example` 只含非敏感示例。
- 使用 `gofmt`，依赖变化同步更新 `go.mod` 和 `go.sum`。接口或运行方式改变时同步更新文档。
- 提交前检查 `git status` 和差异，保留用户已有修改。推送按用户授权执行，不强推、不覆盖远端历史。

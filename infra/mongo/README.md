# 共享 MongoDB 开发服务

仓库内的 Demo 共用一个本地 MongoDB 服务，但各自使用不同的数据库名。服务通过 Docker Compose 启动，只绑定 `127.0.0.1:27017`。

## 启停

从仓库根目录执行：

```bash
bash infra/mongo/mongo.sh up
bash infra/mongo/mongo.sh status
bash infra/mongo/mongo.sh logs
bash infra/mongo/mongo.sh down
```

`down` 只停止并移除 MongoDB 容器和网络，不会删除数据卷。数据保存在 Docker 卷 `gpt-demo-mongo-data` 中。

服务启动后，各 Demo 使用以下连接地址：

```text
mongodb://127.0.0.1:27017
```

如果本机 `27017` 已被其他 MongoDB 占用，应先停止那个服务。临时改用其他端口时可设置 `MONGO_DEV_PORT`，同时调整 Demo 的 `MONGO_URI`。

## Demo 接入约定

- 每个 Demo 使用独立数据库名；当前 `wal-demo` 使用 `game_assets`。
- 集成测试使用唯一测试数据库，并在测试结束后只清理该数据库。
- 不要删除 `gpt-demo-mongo-data`，也不要通过删除数据卷来重置单个 Demo。
- 如果某个 Demo 需要不兼容的 MongoDB 大版本，应单独定义服务，不要改变其他 Demo 共用的版本。

## 版本和服务配置

当前镜像固定为 MongoDB 4.4 的 amd64 摘要。MongoDB 5.0 及以上要求 CPU 支持 AVX；需要在这类不提供 AVX 的云主机上运行的 Demo，应继续使用兼容镜像。

该服务没有启用鉴权，只适合绑定本机地址的本地开发。它不提供副本集、生产备份或远程访问能力，不能直接作为生产数据库使用。MongoDB 4.4 已停止官方维护；部署到生产环境时应使用仍受支持的版本和独立的安全配置。

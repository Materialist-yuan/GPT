# 自定义 TCP 协议 v1

登录 HTTP API 返回 `ticket` 与 `gateway`。客户端连接该地址后，第一帧必须发送 `AUTH`。默认裸 TCP 不提供加密；公网部署前应配置 TLS 终止，或使用网关原生 TLS 1.3，并同步配置客户端。

## 帧格式

所有多字节整数使用大端序。每帧头部固定 16 字节。

| 偏移 | 大小 | 字段 | 规则 |
| --- | --- | --- | --- |
| 0 | 4 | `length`，uint32 | **不包含自身 4 字节**，值为 `12 + 消息体字节数` |
| 4 | 1 | `version`，uint8 | 固定为 `1` |
| 5 | 1 | `opcode`，uint8 | 消息类型，见下表 |
| 6 | 2 | `reserved`，uint16 | 固定为 `0` |
| 8 | 8 | `seq`，uint64 | 请求序号；从大于 0 的值开始，单连接内严格递增 |
| 16 | 可变 | `body` | UTF-8 JSON 消息体 |

默认消息体上限为 65,536 字节，因此 `length` 的范围为 12–65,548。长度、版本和保留字段均在分配消息体缓冲区前校验。TCP 读取可能拆分一帧或一次返回多帧；接收方必须按长度逐帧读取，不能把一次 `Read` 等同于一条消息。

响应沿用对应请求的 `seq`。客户端应同时检查响应序号与类型。协议版本变更需要升级编码与解码两端，当前没有自动协商或压缩。

## 消息类型

| opcode | 名称 | 方向 | 消息体 |
| --- | --- | --- | --- |
| 1 | `AUTH` | 客户端 → 网关 | `{"ticket":"一次性入场票据"}` |
| 2 | `AUTH_OK` | 网关 → 客户端 | `{"account_id":1,"role_id":1,"server_id":"s1"}` |
| 3 | `PING` | 客户端 → 网关 | 合法 JSON，例如 `{}` |
| 4 | `PONG` | 网关 → 客户端 | `{"server_time":1730000000000}`，Unix 毫秒时间戳 |
| 5 | `ECHO` | 客户端 → 网关 | 任意合法 JSON，用于联调和压测 |
| 6 | `ECHO_OK` | 网关 → 客户端 | 原样回传请求消息体 |
| 255 | `ERROR` | 网关 → 客户端 | `{"code":"错误码","message":"错误说明"}` |

Echo 仅用于演示网关消息通路，不代表游戏业务已经实现。错误码包括 `invalid_auth`、`invalid_ticket`、`invalid_session`、`already_online`、`invalid_message`、`rate_limited` 和 `unknown_opcode`。畸形帧直接关闭连接；客户端不能依赖每次异常都收到完整错误帧，例如对端强制断开或写入超时。

## 连接生命周期

1. 建立 TCP 连接后，在默认 5 秒首帧读取期限内完整发送 `AUTH`。
2. 网关原子消费票据，验证票据关联会话，并获取账号在该区服的在线租约。这些 Redis 调用分别有 2 秒超时，因此首帧读取期限不等于整个认证流程的总时限。
3. 收到 `AUTH_OK` 后发送心跳或业务消息。默认 60 秒无入站消息视为闲置；建议每 15–20 秒主动发送一次心跳。
4. 每条请求使用递增序号；重复或倒退序号、非法帧、超大包、认证失败、速率超限会触发错误处理或断开。
5. 客户端断开连接，或服务端注销、续租失败、关闭服务时，连接结束。重连必须重新申请入场票据。

响应默认写超时为 5 秒。单条连接按顺序处理请求与响应，客户端应持续读取响应，避免形成慢消费者。当前协议没有客户端流水线确认窗口、断线续传和业务幂等保证。

## Python 最小客户端

以下代码接收已经通过 HTTP 获取的票据。只使用 Python 标准库，不包含账号密码，适合验证本地默认裸 TCP。连接与写入超时为 5 秒，整帧读取使用 5 秒绝对期限，避免对端持续发送零碎字节使读取无限延长。终端历史和日志中应避免保留真实票据。

```python
import json
import socket
import struct
import time

HEADER = struct.Struct("!IBBHQ")

def read_exact(sock, size, deadline):
    chunks = bytearray()
    while len(chunks) < size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("整帧读取超时")
        sock.settimeout(remaining)
        block = sock.recv(size - len(chunks))
        if not block:
            raise EOFError("连接已关闭")
        chunks.extend(block)
    return bytes(chunks)

def send_frame(sock, opcode, seq, payload):
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    if len(body) > 65536:
        raise ValueError("消息体过大")
    sock.settimeout(5)
    sock.sendall(HEADER.pack(12 + len(body), 1, opcode, 0, seq) + body)

def recv_frame(sock, timeout=5):
    deadline = time.monotonic() + timeout
    length, version, opcode, reserved, seq = HEADER.unpack(read_exact(sock, 16, deadline))
    if version != 1 or reserved != 0 or not 12 <= length <= 12 + 65536:
        raise ValueError("非法帧头")
    body = read_exact(sock, length - 12, deadline)
    return opcode, seq, json.loads(body)

ticket = input("请输入一次性 ticket: ").strip()
with socket.create_connection(("127.0.0.1", 9000), timeout=5) as sock:
    send_frame(sock, 1, 1, {"ticket": ticket})
    opcode, seq, payload = recv_frame(sock)
    if opcode != 2 or seq != 1:
        raise RuntimeError((opcode, seq, payload))
    print("进入游戏：", payload)
    send_frame(sock, 5, 2, {"message": "hello game"})
    opcode, seq, payload = recv_frame(sock)
    if opcode != 6 or seq != 2:
        raise RuntimeError((opcode, seq, payload))
    print("Echo：", payload)
```

启用网关原生 TLS 时，同时设置 `GATEWAY_TLS_CERT` 与 `GATEWAY_TLS_KEY`。客户端必须校验证书链和域名，例如 Python 使用 `ssl.create_default_context().wrap_socket(sock, server_hostname="gateway.example.com")`，再发送相同的应用层帧。

Go 客户端可以复用本项目 `internal/wire` 包的 `ReadFrame` 和 `WriteFrame`；跨项目使用时应将公共协议代码移到独立模块，或按照本协议生成其他语言实现。

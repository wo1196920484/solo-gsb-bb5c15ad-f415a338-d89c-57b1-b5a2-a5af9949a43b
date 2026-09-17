# 乱序事件归并 API（Python · FastAPI · SQLite）

客户端按 `stream_id` 提交带有 `event_id`、`sequence`（从 0 开始递增）和整数 `delta`
的事件。服务端只把**从当前 `next_sequence` 起连续**的事件依次累加到余额；遇到缺号
时，后续事件先暂存。当缺失事件到达时，在**同一个数据库事务**内自动完成追赶
（catch-up）。

核心特性：

- **幂等重放**：相同 `event_id` 以相同内容重复提交返回 `200`，不会重复累加；
  内容不一致返回 `409 Conflict`。
- **序号唯一**：同一 `stream_id` 下每个 `sequence` 只能属于一个 `event_id`，
  重复序号（即使 `delta` 相同）返回 `409 Conflict`。
- **原子追赶**：缺失事件到达后，连续事件链的累加、`applied` 标记和
  `next_sequence`/`balance` 更新全部在同一事务内提交；冲突时整体回滚，已暂存
  事件不受影响。
- **并发安全**：SQLite WAL 模式 + `BEGIN IMMEDIATE`，并发提交串行化，每个序号
  恰好应用一次。
- **消费游标与租约**：每个 `consumer_id` 维护独立游标，按 `sequence` 领取从游标
  开始、**已应用且连续**的事件批次（`max_events` 限量），返回带 `expires_at` 的
  `lease_token`；租约有效期内重复领取返回**同一批次**，只有持有当前未过期 token
  才能 `ack` 原子推进游标；token 过期或陈旧返回 `409`，租约到期后同批事件可再次
  领取。游标与租约持久化在 SQLite 中，多实例并发领取不会产生并行有效租约。
- 无外部账号、无外部依赖，`docker compose up --build` 一条命令启动（监听
  **8000** 端口）。

## 一键启动

```bash
docker compose up --build
```

服务监听 **8000** 端口，数据保存在命名卷 `events-data`（容器内 `/data`）中，
容器重启不丢数据。

健康检查：

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>

> 也可不使用 Docker 本地运行：
> `pip install -r requirements.txt && uvicorn app.main:app --host 0.0.0.0 --port 8000`

## 接口

### 1. 提交事件

```
POST /streams/{stream_id}/events
Content-Type: application/json
```

请求体：

| 字段       | 类型   | 约束                  | 说明                       |
| ---------- | ------ | --------------------- | -------------------------- |
| `event_id` | string | 非空，流内唯一        | 客户端分配的幂等键         |
| `sequence` | int    | `>= 0`，流内唯一      | 从 0 开始的单调递增序号    |
| `delta`    | int    | 整数                  | 事件应用时累加到余额的值   |

响应：

- `201 Created`：事件首次写入；
- `200 OK`：幂等重放（`replayed: true`）；
- `409 Conflict`：同一 `event_id` 内容不一致（`replay_conflict`），或
  `sequence` 已被其他 `event_id` 占用（`sequence_conflict`）；
- `422 Unprocessable Entity`：请求体校验失败。

### 2. 查询流状态

```
GET /streams/{stream_id}
```

```json
{
  "stream_id": "acct-1",
  "balance": 250,
  "next_sequence": 2,
  "pending_sequences": [3, 5]
}
```

- `balance`：已连续应用事件的累加余额；
- `next_sequence`：下一个可应用的序号（即连续前缀的长度）；
- `pending_sequences`：已暂存、但因缺号尚未应用的序号（升序）。

流不存在时返回 `404`。

### 3. 领取事件批次（claim）

```
POST /streams/{stream_id}/consumers/{consumer_id}/claim
Content-Type: application/json
```

按 `sequence` 返回**从该消费者游标开始、已应用（applied）且连续**的事件，
最多 `max_events` 条，并生成一个带 `expires_at` 过期时间的 `lease_token`。
请求体可省略（`max_events` 默认 100，取值范围 `1..10000`），也可以为空 `{}`。

请求体：

| 字段         | 类型 | 约束                 | 说明             |
| ------------ | ---- | -------------------- | ---------------- |
| `max_events` | int  | `1 <= max_events <= 10000`，默认 `100` | 单批事件上限 |

响应 `200 OK`：

```json
{
  "lease_token": "OWZrZT_Plrgy3WW7ORoyIspAqfqWBaqS2CaQ1KwB9js",
  "expires_at": 1789675289,
  "start_sequence": 0,
  "event_count": 3,
  "events": [
    {"event_id": "e0", "sequence": 0, "delta": 10},
    {"event_id": "e1", "sequence": 1, "delta": 20},
    {"event_id": "e2", "sequence": 2, "delta": 2}
  ]
}
```

- 游标处暂时没有已应用事件时（流为空或遇到缺号），返回
  `event_count: 0`、`lease_token: null`、`expires_at: null`，
  `start_sequence` 为当前游标；
- 首次领取会自动注册该 `consumer_id`，游标从 `0` 开始；
- **租约未到期时重复领取**（无论 `max_events` 是什么）返回**完全相同**的
  token、批次与 `expires_at`；
- 不同 `consumer_id` 相互独立，各自维护游标与租约；
- 流不存在时返回 `404`（`stream_not_found`）。

### 4. 确认批次并推进游标（ack）

```
POST /streams/{stream_id}/consumers/{consumer_id}/ack
Content-Type: application/json
```

请求体：

| 字段          | 类型   | 约束   | 说明                          |
| ------------- | ------ | ------ | ----------------------------- |
| `lease_token` | string | 非空   | claim 时返回的租约 token      |

只有 token **匹配当前未过期租约**时，才会在同一个事务中把游标原子推进到本批
次之后并删除租约：

```json
{
  "stream_id": "acct-1",
  "consumer_id": "worker-1",
  "cursor_sequence": 3
}
```

冲突一律返回 `409 Conflict`：

| `detail.code`       | 场景                                                           |
| ------------------- | -------------------------------------------------------------- |
| `lease_not_found`   | 没有未完成租约（从未领取，或该 token 已确认过）                |
| `stale_lease_token` | token 与当前租约不符（旧批次 token、其他消费者的 token）       |
| `lease_expired`     | token 所属租约已过期，同批事件已可被重新领取                   |

租约到期后，旧 token 无法再 ack（`lease_expired`），同批事件可用新 token 再次
领取，保证至少投递一次（at-least-once）。流或消费者不存在时返回 `404`。

## 工作机制示例

```bash
# 序号 1 先到：缺 0，暂存，余额不变
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","sequence":1,"delta":200}'
# 201 -> balance=0, next_sequence=0, pending=[1]

# 序号 2 再到：仍然暂存
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e2","sequence":2,"delta":50}'
# 201 -> balance=0, next_sequence=0, pending=[1,2]

# 序号 0 到达：同一事务内自动追赶 0 -> 1 -> 2
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":100}'
# 201 -> balance=350, next_sequence=3, pending=[]

# 相同 event_id 重放：幂等，返回 200，余额不变
curl -s -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":100}'
# 200 -> replayed=true, balance=350, next_sequence=3

# 同一 event_id 内容不一致：409
curl -i -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"e0","sequence":0,"delta":999}'
# HTTP/1.1 409
# {"detail":{"code":"replay_conflict","message":"event_id already exists with different sequence/delta"}}

# 序号 0 被另一个 event_id 占用：409
curl -i -X POST localhost:8000/streams/acct-1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"other","sequence":0,"delta":100}'
# HTTP/1.1 409, code=sequence_conflict

# 查询
curl -s localhost:8000/streams/acct-1

# 消费者 worker-1 领取一批（最多 2 条），得到 lease_token
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-1/claim \
  -H 'Content-Type: application/json' \
  -d '{"max_events":2}'
# {"lease_token":"<token-1>","expires_at":1789675289,"start_sequence":0,
#  "event_count":2,"events":[{"event_id":"e0","sequence":0,"delta":100},
#                            {"event_id":"e1","sequence":1,"delta":200}]}

# 租约有效期内重复领取：token 与批次完全相同（即使改大 max_events）
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-1/claim \
  -H 'Content-Type: application/json' -d '{"max_events":100}'
# lease_token 仍为 <token-1>，events 仍是 sequence 0、1

# 用旧 token / 其他消费者的 token 确认：409 stale_lease_token
curl -i -X POST localhost:8000/streams/acct-1/consumers/worker-1/ack \
  -H 'Content-Type: application/json' -d '{"lease_token":"not-the-current-token"}'
# HTTP/1.1 409, code=stale_lease_token

# 用当前未过期 token 确认：游标原子推进到 2
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-1/ack \
  -H 'Content-Type: application/json' -d '{"lease_token":"<token-1>"}'
# {"stream_id":"acct-1","consumer_id":"worker-1","cursor_sequence":2}

# 再次领取，从游标 2 继续；全部消费完后 event_count=0、lease_token=null
curl -s -X POST localhost:8000/streams/acct-1/consumers/worker-1/claim
# {"lease_token":null,"expires_at":null,"start_sequence":3,"event_count":0,"events":[]}
```

## 数据模型

```text
streams(stream_id PK, balance, next_sequence)
events(stream_id, event_id, sequence, delta, applied,
       PK(stream_id, event_id), UNIQUE(stream_id, sequence))
consumers(stream_id, consumer_id, cursor_sequence,
          PK(stream_id, consumer_id), FK(stream_id) -> streams)
consumer_leases(stream_id, consumer_id, lease_token, start_sequence,
                next_sequence, event_count, created_at, expires_at,
                PK(stream_id, consumer_id),
                FK(stream_id, consumer_id) -> consumers)
```

- `UNIQUE(stream_id, event_id)` 保证重放幂等；
- `UNIQUE(stream_id, sequence)` 保证序号唯一；
- `applied=0` 表示暂存，`applied=1` 表示已计入余额；
- `consumers.cursor_sequence` 是该消费者下一次领取的起始序号（已确认到的位置）；
- `consumer_leases` 每个消费者至多一行：主键约束配合 `BEGIN IMMEDIATE`
  串行化，保证多个实例并发领取时**不会产生两个并行有效租约**；
  `expires_at` 为 Unix 秒级时间戳。

提交事件的处理在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 若流不存在则创建（`balance=0, next_sequence=0`）；
2. 若 `event_id` 已存在：比对 `sequence`/`delta`，不一致即 `409`，一致则直接
   返回当前状态（幂等）；
3. 否则检查 `sequence` 是否已被占用，占用则 `409`，未占用则写入
   `applied=0` 的事件；
4. 从 `next_sequence` 开始循环取 `applied=0` 的连续事件累加 `delta`、标记
   `applied=1`，直到遇到缺号；
5. 更新 `balance` 与 `next_sequence`，提交事务。

领取/确认同样在单个 `BEGIN IMMEDIATE` 事务内完成：

1. **claim**：流不存在即 `404`；消费者不存在则以游标 `0` 注册。若已有未过期
   租约，直接返回原批次（同一 token、同一 `expires_at`，忽略本次
   `max_events`）；租约已过期则删除，同批事件可重新发放。否则从游标起取
   `applied=1` 的连续事件（`LIMIT max_events`），无事件返回空批次，有事件则
   生成随机 token 并写入 `expires_at = now + LEASE_TTL_SECONDS`；
2. **ack**：流/消费者不存在即 `404`；无租约 `lease_not_found`、token 不符
   `stale_lease_token`、租约过期 `lease_expired`，均为 `409` 且游标不变。
   校验通过后在同一事务内推进游标到 `next_sequence` 并删除租约。

## 配置

| 环境变量            | 默认值                | 说明                          |
| ------------------- | --------------------- | ----------------------------- |
| `APP_PORT`          | `8000`                | 服务监听端口                  |
| `DATABASE_PATH`     | `/data/events.db`     | SQLite 文件路径               |
| `LEASE_TTL_SECONDS` | `30`                  | claim 租约 token 的有效秒数   |

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

`tests/test_api.py` 覆盖：顺序累加、缺号暂存与自动追赶、幂等重放、两种 409、
404、422 参数校验、大批量追赶以及 20 线程并发。

`tests/test_consumers.py` 覆盖：按游标领取连续已应用事件、租约期内重复领取返回
同一批次、ack 原子推进游标与继续领取、错误/重复/过期 token 的 409、流与消费者
404、空批次、缺号阻塞、多消费者独立、TTL 到期重新投递，以及 20 线程并发只产生
一个有效租约。

仅用 Python 标准库验证核心归并算法与消费游标（无需安装 FastAPI）：

```bash
python3 verify_core.py
```

## 项目结构

```
.
├── app/
│   ├── __init__.py
│   ├── main.py        # FastAPI 路由与请求/响应模型
│   ├── repo.py        # 归并/追赶算法（纯 sqlite3，事务边界清晰）
│   ├── consumers.py   # 消费游标与 claim/ack 租约（纯 sqlite3）
│   ├── db.py          # 连接管理、WAL、schema 初始化
│   └── config.py      # 环境变量配置
├── tests/test_api.py
├── tests/test_consumers.py
├── verify_core.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

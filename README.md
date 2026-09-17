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
- 无外部账号、无外部依赖，`docker compose up --build` 一条命令启动。

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
```

## 数据模型

```text
streams(stream_id PK, balance, next_sequence)
events(stream_id, event_id, sequence, delta, applied,
       PK(stream_id, event_id), UNIQUE(stream_id, sequence))
```

- `UNIQUE(stream_id, event_id)` 保证重放幂等；
- `UNIQUE(stream_id, sequence)` 保证序号唯一；
- `applied=0` 表示暂存，`applied=1` 表示已计入余额。

提交事件的处理在单个 `BEGIN IMMEDIATE` 事务内完成：

1. 若流不存在则创建（`balance=0, next_sequence=0`）；
2. 若 `event_id` 已存在：比对 `sequence`/`delta`，不一致即 `409`，一致则直接
   返回当前状态（幂等）；
3. 否则检查 `sequence` 是否已被占用，占用则 `409`，未占用则写入
   `applied=0` 的事件；
4. 从 `next_sequence` 开始循环取 `applied=0` 的连续事件累加 `delta`、标记
   `applied=1`，直到遇到缺号；
5. 更新 `balance` 与 `next_sequence`，提交事务。

## 配置

| 环境变量        | 默认值                | 说明            |
| --------------- | --------------------- | --------------- |
| `APP_PORT`      | `8000`                | 服务监听端口    |
| `DATABASE_PATH` | `/data/events.db`     | SQLite 文件路径 |

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

`tests/test_api.py` 覆盖：顺序累加、缺号暂存与自动追赶、幂等重放、两种 409、
404、422 参数校验、大批量追赶以及 20 线程并发。

仅用 Python 标准库验证核心归并算法（无需安装 FastAPI）：

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
│   ├── db.py          # 连接管理、WAL、schema 初始化
│   └── config.py      # 环境变量配置
├── tests/test_api.py
├── verify_core.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

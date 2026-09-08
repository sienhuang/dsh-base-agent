# PostgreSQL ControlStore

PostgreSQL 是多进程生产部署的业务真相源。SQLite 仍然保留，仅用于本地开发和单进程测试。
数据库配置与 DSH `RuntimeConfig` 分离。

设置 PostgreSQL 后，API 自动进入只提交模式，必须同时启动独立 Worker：

```bash
dsh-base-agent-server
dsh-base-agent-worker
```

Worker 直接轮询 PostgreSQL 中的 `queued` Run，不依赖 Kafka 或 Outbox。Kafka 只承载可选的
DSH Notification 观测流。

`PostgresControlStore` 禁止和 `auto_execute=True` 组合。构造 `ControlPlane` 时若检测到该组合
会立即失败，避免多个 API/ControlPlane 进程绕过 Worker lease 直接执行同一个 Run。SQLite
本地模式仍允许 `auto_execute=True`。

## 配置

```dotenv
DSH_BASE_AGENT_DATABASE_URL=postgresql://agent:secret@postgres:5432/dsh_base_agent
DSH_BASE_AGENT_DATABASE_SCHEMA=dsh_base_agent
DSH_BASE_AGENT_DATABASE_POOL_MIN_SIZE=1
DSH_BASE_AGENT_DATABASE_POOL_MAX_SIZE=10
DSH_BASE_AGENT_DATABASE_COMMAND_TIMEOUT_SECONDS=30
DSH_BASE_AGENT_DATABASE_STATEMENT_CACHE_SIZE=100
```

`DSH_BASE_AGENT_DATABASE_URL` 未设置时使用
`<workspace>/.dsh-base-agent/control.db`。数据库 URL 不会出现在 `ControlStoreConfig` 的 repr
中。

若 PostgreSQL 前面使用 transaction/statement 模式的 PgBouncer，应根据部署方式将
`DSH_BASE_AGENT_DATABASE_STATEMENT_CACHE_SIZE` 设置为 `0`。

## Schema 与迁移

默认 schema 是 `dsh_base_agent`。初始化时会：

1. 创建 schema（不存在时）；
2. 获取 schema 级 PostgreSQL advisory transaction lock；
3. 校验 `schema_migrations` 中已执行版本的名称和 SHA-256；
4. 在同一事务中执行尚未应用的迁移。

生产环境可以由 DBA 预先创建 schema，并给运行账号授予 schema 和表的 DDL/DML 权限。当前
迁移入口在
[`../src/dsh_base_agent/store/postgres_migrations.py`](../src/dsh_base_agent/store/postgres_migrations.py)。

业务关系同时保存为明确列和 JSONB snapshot。明确列用于约束、检索和 CAS；snapshot 用于
Pydantic 模型无损还原。当前数据库约束包括：

- `(conversation_id, sequence)` 唯一；
- 一个 Conversation 最多一个 `RUNNING`/`WAITING` Run；
- `(tenant_id, idempotency_key)` 唯一；
- Run、Attempt、Event 和 Artifact 外键；
- Run、Attempt、Conversation 的 revision CAS；
- Conversation sequence 和同一 Run 的 Event sequence 通过行锁串行分配。

`worker_leases` 已用于 `SKIP LOCKED` 领取、heartbeat、过期检测和 fencing。Lease 正常释放
时只将记录置为过期，不删除 token 历史；下一次领取必须递增 fencing token。Heartbeat 使用
独立短超时，续租异常时 fail-closed 并取消 owner Task。

Worker 还提供两个互不替代的期限：

```dotenv
DSH_BASE_AGENT_WORKER_RUN_TIMEOUT_SECONDS=600
DSH_BASE_AGENT_WORKER_CONVERSATION_IDLE_SECONDS=60
```

前者限制单个已领取 Run 的总执行时间；后者让暂时没有新 Run 的 Conversation 释放 Runtime
和 Lease，但不会删除持久化 DSH Session。替代 Worker 仍可用相同的
`DSH_HOME + dsh_session_id` 继续下一轮。

过期恢复会在同一个 PostgreSQL 事务中写入 Run、RunAttempt、`run.worker_lease_expired` Event
和 AuditRecord，避免出现业务状态已经恢复但审计事实缺失。`outbox` 表仍然保留但当前链路不
使用；PostgreSQL 模式的 FastAPI 不执行 DSH，SQLite 模式才保留进程内执行。

## 集成测试

集成测试默认跳过，避免误连开发者数据库。指定一个允许创建和删除临时 schema 的测试库：

```bash
DSH_BASE_AGENT_TEST_POSTGRES_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  uv run pytest tests/test_postgres_store.py
```

每个测试都会使用随机的 `dsh_test_<uuid>` schema，结束后删除该测试 schema，不操作其他
schema。

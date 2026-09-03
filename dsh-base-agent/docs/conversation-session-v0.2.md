# Conversation / DSH Session 多轮模型（v0.2 设计）

状态：本地单进程垂直切片已实现；跨进程恢复和生产调度仍是设计。

## 1. 目标与边界

v0.2 引入 `Conversation`，支持一个 DSH Session 中按顺序执行多个业务 Run：

```text
Tenant + Principal
└── Conversation（业务会话和串行边界）
    ├── 一个固定的 AgentDefinition 版本
    ├── 一个固定的 DSH Home 作用域
    ├── 一个固定的 DSH Session ID
    ├── Run 1（业务 Turn）
    │   └── RunAttempt 1..n
    ├── Run 2（业务 Turn）
    │   └── RunAttempt 1..n
    └── Run 3（业务 Turn）
        └── RunAttempt 1..n
```

职责边界不变：

- DSH Session/Event 是 Agent 实际执行和上下文的真相；
- base-agent Conversation/Run/Audit 是业务所有权、顺序和治理状态的真相；
- base-agent 不复制 DSH 的完整消息历史，也不实现模型循环；
- 客户端只能使用 `conversation_id`，不能提交或选择任意 `dsh_session_id`。

## 2. 核心语义

### 2.1 Conversation 是 Session 的所有者

一个 Conversation 在创建时固定以下属性：

- `tenant_id`；
- `principal_id`；
- `agent_id`、`agent_version`、`agent_fingerprint`；
- `dsh_home_key`；
- `dsh_session_id`。

默认不支持更换所有者、租户或 Agent 版本。Agent 升级后应创建新 Conversation，不能让旧
Session 在新的 Prompt、Tool 或 Skill 配置下继续运行。

Session 的实际唯一键是：

```text
(dsh_home_key, dsh_session_id)
```

仅凭 `dsh_session_id` 不能标识跨 DSH Home 的同一个 Session。

### 2.2 Run 是一次业务 Turn 请求

每次向 Conversation 提交输入都会创建一个 Run。Run 的 `sequence` 在提交事务中单调递增，
它表示用户请求的确定顺序，而不是依赖进程收到请求的时间推断顺序。

同一个 Conversation 可以积压多个 `QUEUED` Run，但最多只能有一个 Run 处于会占用 DSH
Session 的状态：

```text
RUNNING 或 WAITING
```

RunAttempt 仍表示对同一个业务 Run 的一次实际执行尝试。Attempt 保存本次使用的
`dsh_session_id` 快照、DSH Prompt 接收状态和执行结果，但 Session 的规范所有者仍是
Conversation。

### 2.3 严格串行

同一 Conversation 的 Run 必须按照 `sequence` 串行执行：

```text
Run 1: QUEUED → RUNNING → SUCCEEDED
Run 2: QUEUED ──────────→ RUNNING → SUCCEEDED
Run 3: QUEUED ─────────────────────→ RUNNING
```

不同 Conversation 之间可以并行：

```text
Conversation A / Run 2 ─┐
                         ├── 并行
Conversation B / Run 7 ─┘
```

## 3. 数据模型

### 3.1 ConversationRecord

```python
class ConversationStatus(StrEnum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    CLOSED = "closed"


class ConversationRecord(BaseModel):
    conversation_id: str
    tenant_id: str
    principal_id: str
    agent_id: str
    agent_version: str
    agent_fingerprint: str
    dsh_home_key: str
    dsh_session_id: str
    status: ConversationStatus
    next_sequence: int
    revision: int
    blocked_reason: str | None
    metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime
```

`dsh_home_key` 是平台内部的稳定路由键，不在业务 API 中暴露绝对文件路径。默认应至少按
租户、Principal 和 Agent 指纹隔离，例如：

```text
sha256(canonical_json({tenant_id, principal_id, agent_fingerprint}))
```

### 3.2 RunRecord 变更

Run 增加：

```python
conversation_id: str
sequence: int
```

Run 继续保存租户、Principal 和 Agent 快照，以便独立审计。数据库必须校验这些字段与
Conversation 一致，不能只相信请求提供的关联 ID。

### 3.3 RunAttempt 变更

Attempt 增加规范化的 DSH 投递状态：

```python
class DispatchState(StrEnum):
    NOT_SENT = "not_sent"
    DISPATCHING = "dispatching"
    ACCEPTED = "accepted"
    SETTLED = "settled"
    UNKNOWN = "unknown"


class RunAttempt(BaseModel):
    ...
    dsh_session_id: str
    dispatch_state: DispatchState
    dsh_message_id: str | None
    dsh_turn_id: str | None
```

DSH 适配层负责把 `agent/inbox/spliced` 等底层通知规范化为 `prompt.accepted`，ControlPlane
不直接依赖不稳定的 DSH JSON 结构。

### 3.4 数据库约束

至少需要以下约束：

```text
UNIQUE (dsh_home_key, dsh_session_id)
UNIQUE (conversation_id, sequence)
FOREIGN KEY runs.conversation_id → conversations.conversation_id
FOREIGN KEY run_attempts.run_id → runs.run_id
```

PostgreSQL 生产实现还应增加部分唯一索引，防止同一 Conversation 同时出现多个活动 Run：

```sql
CREATE UNIQUE INDEX one_active_run_per_conversation
ON runs (conversation_id)
WHERE status IN ('running', 'waiting');
```

这一约束是最后防线，不能只依靠 Python 进程内的 `asyncio.Lock`。

## 4. API 契约

新增接口：

```text
POST /v1/conversations
GET  /v1/conversations/{conversation_id}
POST /v1/conversations/{conversation_id}/runs
GET  /v1/conversations/{conversation_id}/runs
POST /v1/conversations/{conversation_id}/close
```

创建 Conversation：

```json
POST /v1/conversations
{
  "agent_id": "order-assistant",
  "metadata": {}
}
```

向已有 Conversation 提交新 Turn：

```text
POST /v1/conversations/conv_123/runs
Idempotency-Key: message-002

{"input": "继续查询这个订单的物流"}
```

响应只返回 `conversation_id` 和业务 Run 信息，不向普通客户端暴露
`dsh_home_key`/`dsh_session_id`。内部诊断接口可在受控权限下查看映射。

现有 `POST /v1/runs` 保留兼容：它创建一个隐式、单次使用的 Conversation，再在其中创建
第一个 Run。旧客户端不需要理解多轮会话。

## 5. 提交与调度事务

### 5.1 提交新 Run

一次提交必须在同一个数据库事务中完成：

1. 按租户和 Principal 查询并锁定 Conversation；
2. 检查 Conversation 为 `ACTIVE`；
3. 检查 Agent 指纹仍与 Conversation 固定版本一致；
4. 检查租户级幂等键；
5. 读取并递增 `next_sequence`；
6. 插入 `QUEUED` Run；
7. 追加业务事件或写入事务 Outbox。

幂等摘要至少包括：

```text
tenant_id + principal_id + conversation_id + input + metadata
```

### 5.2 Worker 获取下一个 Run

生产 Worker 不能扫描后直接运行。它需要：

1. 使用 `SELECT ... FOR UPDATE SKIP LOCKED` 选择有可执行 Run 的 Conversation；
2. 获取带 fencing token 的 Conversation lease；
3. 选择最小 `sequence` 的可执行 Run；
4. 在事务中把 Run/Attempt 改为 `RUNNING`；
5. 提交事务后才调用 DSH；
6. 只有持有当前 fencing token 的 Worker 才能提交状态变更。

本地 SQLite 实现可以使用 `_conversation_locks[conversation_id]` 模拟串行，但该锁不构成
多进程保证。

### 5.3 哪个 Run 可以执行

调度器从最小 sequence 开始：

- 已成功的 Run 跳过；
- 在 DSH 投递前取消的 Run 可以跳过；
- 第一个 `QUEUED` Run 可以执行；
- 遇到 `RUNNING`、`WAITING` 或不确定失败即停止；
- Conversation 为 `BLOCKED`/`CLOSED` 时不调度任何 Run。

## 6. DSH Runtime 生命周期

DSH SDK 支持对同一 Session 顺序调用多次 `run()`。v0.2 增加
`ConversationRuntimeManager`：

```text
ConversationRuntimeManager
└── conversation_id → RuntimeHolder
    ├── dsh_home_key
    ├── dsh_session_id
    ├── lease token
    ├── DshRuntime
    └── idle deadline
```

行为规则：

- 同一 Conversation 同时最多一个 `runtime.run()`；
- 连续有积压 Run 时复用同一个 Harness；
- 空闲超过 TTL 后关闭 Harness，但不删除 DSH Session；
- 再次运行时使用相同 `dsh_home_key` 和 `dsh_session_id` 打开；
- `runtime.close()` 只释放进程资源，不代表关闭业务 Conversation；
- `Conversation.close` 才是业务终止语义。

在启用跨进程 Worker 前，必须用真实 DSH 做验收测试，确认相同 DSH Home 的持久化 Session
可在 Harness 重建后继续。若 DSH Session 只能依赖本地 Home，则调度器必须提供 sticky
placement/共享卷；不能仅在 PostgreSQL 保存一个 Session ID 就认为任意 Worker 都能恢复。

## 7. Event 与外部存储

底层通知流保持以下方向：

```text
DSH on_notification
→ DSH Adapter 规范化
→ on_event(RuntimeEvent)
→ 当前 Run 的有界事件投影
→ ControlStore / Transactional Outbox
→ Kafka、Elasticsearch 或其他外部 Sink
```

由于同一 Session 会包含多个 Run，事件必须携带或由当前 lease 上下文补充：

```text
tenant_id
principal_id
conversation_id
run_id
attempt_id
dsh_home_key
dsh_session_id
dsh_event_sequence
```

建议以 `(dsh_home_key, dsh_session_id, dsh_event_sequence)` 去重。完整 DSH 消息仍由 DSH
保存；base-agent 只保存稳定的业务投影、工具审计和必要摘要。向非事务外部系统分发应使用
Outbox，避免因为 Kafka/Elasticsearch 暂时不可用而阻塞 DSH 的通知线程。

## 8. 权限规则

每次 Conversation/Run 操作都必须验证：

```text
request.tenant_id == conversation.tenant_id
request.principal_id == conversation.principal_id
run.conversation_id == conversation.conversation_id
run.agent_fingerprint == conversation.agent_fingerprint
```

默认是严格的单 Principal 所有权。未来若要支持共享 Conversation，应显式增加成员表和角色，
不能通过忽略 `principal_id` 检查实现。

Tool 调用继续携带：

```text
tenant_id + principal_id + conversation_id + run_id + attempt_id + tool_name
```

## 9. 失败、取消和重试

### 9.1 安全失败分类

```text
NOT_SENT
└── 可以安全重新入队，没有向 DSH 增加 Turn

DISPATCHING / UNKNOWN
└── 不知道 Prompt 是否进入 Session，Conversation → BLOCKED

ACCEPTED
└── Prompt 已进入 Session；结果不确定时 Conversation → BLOCKED

SETTLED
└── 根据最终结果推进队列
```

不能因为 Worker 重启就重新发送原始 Prompt，否则会产生重复 Turn 和重复副作用。

### 9.2 取消

- 取消尚未投递的 Run：标记 `CANCELLED`，队列可继续；
- 取消正在执行但尚未 ACCEPTED 的 Run：确认未投递后可继续；
- 取消已经 ACCEPTED 的 Run：若 DSH 不能确认终止边界，Conversation 进入 `BLOCKED`；
- 取消 Run 不等于关闭 Conversation。

### 9.3 重试

- `NOT_SENT` 失败可以自动或显式重试；
- `ACCEPTED/UNKNOWN` 不允许静默重放；
- Tool 副作用必须使用业务幂等键，不能以新的 Attempt ID 作为唯一去重依据；
- 无法确认 Session 状态时，只允许管理员关闭 Conversation 并创建新 Conversation，不伪造
  DSH 原会话恢复。

## 10. 启动恢复

启动或 lease 过期时按以下规则恢复：

```text
QUEUED
└── 保留队列，重新竞争 Conversation lease

RUNNING + NOT_SENT
└── Attempt → INTERRUPTED，Run 可重新入队

RUNNING + ACCEPTED/DISPATCHING/UNKNOWN
└── Attempt → INTERRUPTED
    Run → FAILED
    Conversation → BLOCKED
    等待 DSH 对账或人工处理

WAITING
└── 保持阻塞；只能通过经过 capability 验证的 DSH 扩展恢复
```

后续采用公司自有 DSH 插件和 JSON-RPC 扩展，把 DSH 核心的 Session resume、status 和
approval answerer 适配给 Python 控制面，详见
[`dsh-resume-extension.md`](dsh-resume-extension.md)。`ctx.agents.resume()` 只表示重新加载持久化
Session，不代表恢复中断的 Tool 调用栈。适配层恢复前必须与 DSH 对账，再决定发送新 Turn、
继续监听或保持阻塞。ControlPlane 不读取和推断 DSH 内部 JSONL 文件来伪造状态。

## 11. Store 与部署

`ControlStore` 继续是依赖倒置边界：

```text
SqliteControlStore
└── 本地开发、单进程测试

PostgresControlStore
└── 生产、多 Worker、行锁、lease、fencing、Outbox
```

生产数据库通过独立控制面配置提供，例如：

```text
DSH_BASE_AGENT_DATABASE_URL=postgresql://...
```

它不属于 `RuntimeConfig`；`RuntimeConfig` 只负责 DSH Runtime。

## 12. 兼容迁移

旧数据不能把多个既有 Run 自动合并为一个 Conversation。迁移规则为：

1. 每个旧 Run 创建一个隐式 `legacy_single_run` Conversation；
2. Conversation 继承该 Run 的租户、Principal 和 Agent 快照；
3. 保留原 Attempt 的 DSH Session ID 作为审计事实；
4. 旧 Run 不自动获得后续多轮能力；
5. 新建 Conversation 才使用稳定的单 Session 多 Run 语义。

## 13. v0.2 验收条件

实现完成必须通过以下测试：

1. 同一 Conversation 连续两个 Run 使用相同 DSH Session，第二轮能引用第一轮上下文；
2. 并发提交两个 Run 时得到唯一且递增的 sequence；
3. 同一 Conversation 永远不出现两个同时运行的 DSH Prompt；
4. 不同 Conversation 可以并行；
5. 其他租户或 Principal 无法读取、追加或取消该 Conversation；
6. Agent 版本变化不能污染已有 Conversation；
7. QUEUED Run 重启后自动恢复；
8. ACCEPTED 后发生 Worker 崩溃时不自动重放，Conversation 被阻塞；
9. PostgreSQL 下两个 Worker 竞争同一 Conversation 时只有一个获得有效 fencing token；
10. DSH Harness 关闭并重建后，相同 Home/Session 的多轮上下文仍可继续。

## 14. 实施顺序

建议按以下顺序落地：

1. 新增 Conversation 模型、Store 协议和 SQLite schema migration；
2. 新增 Conversation API 和旧 `/v1/runs` 兼容层；
3. 将进程内锁从 `run_id` 上移到 `conversation_id`；
4. 增加 RuntimeManager，同一 Conversation 串行复用 Session；
5. 增加 Prompt accepted/dispatch state 规范化与保守恢复；
6. 实现 PostgresControlStore、lease、fencing 和 Outbox；
7. 做真实 DSH 多轮、重建 Harness 和双 Worker 竞争验收。

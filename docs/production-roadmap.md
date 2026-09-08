# dsh-base-agent 逐步生产化路线图

状态：已确定实施方向，按阶段交付。

## 1. 当前基线

当前版本同时支持本地单进程和 PostgreSQL API/Worker 分离：

```text
Python Agent 定义
-> FastAPI
-> Run / Conversation
-> DSH Runtime / Session
-> Skill / MCP Tool
-> SQLite 进程内执行，或 PostgreSQL Pull Queue + 独立 Worker
-> PostgreSQL Event / Audit + 可选 Kafka Notification
```

它适合本地开发和语义验证，不是生产部署形态。已知边界包括：

- SQLite 模式仍由 FastAPI 进程内执行，只用于本地开发；
- PostgreSQL 模式下 FastAPI 只提交任务，独立 Worker 已通过 Lease/Heartbeat/Fencing 执行；
- Conversation 通过共享持久化 `DSH_HOME + dsh_session_id` 跨 Worker 继续新的 Turn；
- Worker 丢失时不会恢复或自动重放正在执行的原 Run；
- Tool Observation 有 64 KiB 边界，超限正文已写入 LocalArtifactStore；
- Run input、metadata、最终输出、Event 和 Audit 尚无统一的累计数据预算；
- Kafka 有单消息摘要和有界条数队列，但原始对象在入队前尚无字节预算；
- Tool Result 正文不进入 ControlStore；Event/Audit 保存哈希、大小和 Artifact 引用；
- Skill 生命周期、公司 IAM、审批、配额和生产审计尚未实现。

## 2. 目标架构

```text
Client
  -> dsh-base-agent-api
       -> PostgreSQL（业务真相源）
            <- PostgreSQL Pull Worker
                 -> DSH Runtime / Session
                 -> MCP Tool Gateway
                 -> ArtifactStore（S3 / MinIO，目标态必备）
                 -> Kafka Notification（可选、直发、非真相源）
```

真相源边界保持不变：

- DSH Session/Event 是 Agent 实际执行过程的真相；
- base-agent Conversation/Run/Audit 是业务任务、所有权和治理状态的真相；
- base-agent 不重写模型循环，不解析 DSH JSONL 伪造执行状态。

## 3. P0：生产阻塞项

### 3.1 PostgreSQL ControlStore

实施状态：持久化底座、PostgreSQL Worker 协议、Heartbeat fail-closed、Run timeout、空闲
Conversation release 和恢复事务原子性已实现；生产数据库压测尚未完成。

将重要关系从 JSON payload 提升为明确数据库列：

```text
conversations
  conversation_id, tenant_id, principal_id
  agent_id, agent_version, agent_fingerprint
  dsh_home_key, dsh_session_id
  status, revision

runs
  run_id, conversation_id, sequence
  status, active_attempt_id, revision

run_attempts
  attempt_id, run_id, dsh_session_id
  dispatch_state, worker_id, lease_token, revision

run_events, audit_log, artifacts, worker_leases, outbox
```

数据库至少保证：

```text
UNIQUE(conversation_id, sequence)
一个 Conversation 最多一个 RUNNING/WAITING Run
基于 revision 的 CAS 更新
Run、Attempt、Artifact 外键完整性
Tenant/Principal 所有权字段不可隐式继承
```

交付内容：

- `PostgresControlStore`；
- 可重复执行、有版本的 Schema migration；
- SQLite 保留为本地开发 Backend；
- PostgreSQL 集成测试覆盖并发 sequence、CAS 和约束冲突。

### 3.2 API 与 Worker 分离

FastAPI 只接受、验证和查询任务，不在 Web 进程内运行 DSH：

```text
POST Run
-> PostgreSQL 写 QUEUED Run
-> Worker 通过 FOR UPDATE SKIP LOCKED 获取任务
-> Worker 执行 DSH
```

Worker 必须实现：

- lease、heartbeat 和超时回收；
- fencing token，阻止过期 Worker 提交结果；
- Conversation 级串行；
- 幂等领取和重复消息处理；
- 取消和关闭时的安全清理；
- Worker 崩溃后的保守恢复。

不得依靠进程内 `asyncio.Lock` 作为多副本并发保证。

### 3.3 DSH Prompt 投递状态

每个 Attempt 明确记录：

```text
NOT_SENT
-> DISPATCHING
-> ACCEPTED
-> SETTLED

任意不确定边界
-> UNKNOWN
```

处理语义：

- `NOT_SENT`：可以安全重新入队；
- `DISPATCHING/UNKNOWN`：不能自动重放；
- `ACCEPTED`：DSH 已接收 Prompt，必须对账；
- `SETTLED`：Turn 已到确定终态。

DSH Adapter 负责把 inbox receipt、Session event 和 status 规范化；ControlPlane 不直接依赖
不稳定的底层事件结构。

### 3.4 PayloadLimits、ObservationPolicy 与 TracePolicy

LocalArtifactStore 已实现，Tool 大结果会写本地正文并返回有界预览和引用。P0 继续保证其他
数据不能无界进入 PostgreSQL、Kafka、进程内队列或模型上下文。详细决策和第一版默认预算见
[`payload-limits.md`](payload-limits.md)，本地实现见 [`artifacts.md`](artifacts.md)。

新增稳定接口：

```python
class PayloadLimits:
    ...


class ObservationPolicy(Protocol):
    async def transform(...) -> JsonValue: ...


class TracePolicy(Protocol):
    async def capture(...) -> TraceValue: ...
```

三种数据边界必须分开：

- ObservationPolicy：给 DSH/模型看什么；
- TracePolicy：运行事件中保存什么；
- AuditPolicy：审计中保存什么。

P0 大结果处理：

```text
Tool 完整结果
-> 脱敏和大小判断
-> 未超限：返回有界 Observation
-> 超限：完整 JSON 写 LocalArtifactStore
-> DSH 收到明确标记为不完整的预览和 Artifact 引用
-> Event/Audit/Kafka：仅保存摘要、大小和 sha256
```

限制必须覆盖单次 Payload 和每个 Attempt 的累计预算，并在 API、ToolGateway、ControlStore 和
Kafka 入队前强制执行。不能通过简单增大 `max_observation_bytes` 解决大结果问题。

当前 ControlPlane 已保存 ArtifactRecord，DSH 只收到预览、`artifact_id`、`sha256` 和大小。
Tool 仍应优先支持过滤、字段投影和分页。后续补充模型按需读取 Artifact chunk，并让
S3ArtifactStore 实现相同协议。

### 3.5 DSH 原 Turn 对账与 Resume 扩展

按照 [`dsh-resume-extension.md`](dsh-resume-extension.md) 扩展当前能力：

- 验证 DSH Plugin 是否能扩展或替代 SDK JSON-RPC Server；
- 查询并对账中断时的 DSH Session/Turn 状态；
- 暴露 capability、Session status、cancel 和 approval answerer；
- 明确 live approval 与 durable Turn resume 的边界。

跨 Worker 的新 Turn 继续直接使用共享持久化 `DSH_HOME + dsh_session_id`。该扩展用于处理中
断的原 Turn、状态查询、取消和审批，不是普通 Conversation 换手的前置条件。

## 4. P1：生产治理

P0 执行底座通过验收后，再依次实现：

1. 公司 IAM、Agent 运行权限和 Tool 资源级授权；
2. 副作用 Tool 审批、业务幂等键和下游去重；
3. Tenant 并发量、Token、Tool 调用和 Artifact 容量配额；
4. Skill 来源注册、版本锁定、签名、内容摘要和严格 allowlist；
5. Conversation/RunAttempt Workspace 隔离及远程物化；
6. Audit 防篡改、脱敏、保留周期和长期归档；
7. SSE 事件流和 Kafka Notification 消费治理；
8. S3ArtifactStore 及 Artifact 容量、TTL 和孤儿回收；
9. 密钥管理、网络出口策略和数据加密。

## 5. P2：可运维性与 SLA

- Run、Conversation、Worker、DSH Session 和 Tool 指标；
- OpenTelemetry trace 以及跨 API、Queue、Worker、Tool 的 correlation id；
- readiness、liveness、降级和熔断；
- 模型、Tool、ArtifactStore 和 Queue 超时分类；
- 负载、容量、混沌和故障恢复测试；
- 灰度发布、Schema 前后兼容和回滚手册；
- 租户级成本与 Token 用量报表；
- 值班告警、Run 对账和人工处置工具。

## 6. 推荐的下一条纵向切片

下一条交付不分别堆砌孤立模块，而是打通：

```text
创建 Conversation
-> PostgreSQL 持久化
-> 连续提交两个 Run
-> 两个 Worker 竞争但只有一个取得 Conversation lease
-> 按 sequence 串行复用 DSH Session
-> Tool 产生一个超限结果
-> ToolGateway 在进入 DSH 前稳定拒绝
-> PostgreSQL Event/Audit 与 Kafka 只收到有界摘要和 sha256
-> API 查询到明确超限错误，所有存储均无完整大正文
```

建议实施顺序：

1. PostgreSQL Schema、migration 和 `PostgresControlStore`；
2. PostgreSQL Pull Queue、Worker lease/heartbeat/fencing；
3. `DispatchState` 和 DSH 投递边界；
4. PayloadLimits、ObservationPolicy、TracePolicy；
5. DSH 原 Turn 对账与 Resume 扩展；
6. IAM、审批、Skill 治理和运维能力；
7. S3ArtifactStore 与 Artifact 生命周期管理。

## 7. 生产验收门槛

P0 必须证明：

- API Pod 重启不影响正在执行的 Worker；
- Worker 在 `NOT_SENT` 崩溃后可安全接管；
- Worker 在 `ACCEPTED/UNKNOWN` 崩溃后不会重复发送 Prompt；
- 两个 Worker 竞争同一 Conversation 时只有有效 fencing token 能提交；
- 替代 Worker 使用相同 `DSH_HOME + dsh_session_id` 接受 Conversation 的下一个 Turn；
- 同一 Conversation 的 Run 严格按 sequence 执行；
- 重复幂等请求不会创建重复 Run；
- 大 Tool Result 不进入 PostgreSQL Event 正文或无界模型上下文；
- ArtifactStore 未实现时，大结果被稳定拒绝；实现后其故障不会产生虚假的成功状态；
- Kafka 在入队前完成大小控制，进程内队列同时受条数和总字节数约束；
- 其他 Tenant/Principal 无法读取或操作 Conversation、Run、Event 和 Artifact；
- 每次 Tool 调用、授权、审批、取消和恢复都有可关联审计记录。

P0 未满足前，只能作为开发或受控试运行版本，不能声明为生产可用。

## 8. 明确不做

- 不在 FastAPI 中继续扩展自研 Agent Runtime；
- 不复制 DSH 的完整对话状态机；
- 不读取 DSH 内部 JSONL 推断业务终态；
- 不把 retry、followup 和 resume 合并成一个含糊操作；
- 不把完整 Tool Result 默认写入 Event、Audit 或模型上下文；
- 共享持久卷只保存 DSH Session；并发正确性仍由 PostgreSQL lease 和 fencing 保证。

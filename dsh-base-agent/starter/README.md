# dsh-base-agent starter

这是一个基于当前 `dsh-base-agent` v0.1 的可复制业务应用骨架。它不实现模型循环，运行时仍
由 DSH 负责。

## 运行

```bash
cd starter
cp .env.example .env
# 编辑 .env，填入 DSH_API_KEY
uv sync --all-groups
uv run company-agent-server
```

默认关闭 MOA 认证，方便没有前端和 `X-MOA-Token` 的本地开发。此时调用方式保持不变，仍需传入
`X-Tenant-ID` 和 `X-Principal-ID`。开启 MOA 认证时配置：

```dotenv
DSH_BASE_AGENT_AUTH_ENABLED=true
DSH_BASE_AGENT_AUTH_TENANT_ID=demo-tenant
DSH_BASE_AGENT_MOA_AUTH_URL=https://login.moa.moonton.net
DSH_BASE_AGENT_MOA_PROJECT_ID=<为本应用审批的 MOA project id>
```

开启后，业务 API 只使用 `X-MOA-Token` 建立可信 `Principal`；请求中的
`X-Tenant-ID/X-Principal-ID` 不参与身份判定。API 不保存原始 Token，Worker 仍然只从 Run 或
Conversation 读取已认证的 `tenant_id/principal_id`，不需要 MOA 配置。

```bash
curl -X POST http://127.0.0.1:8000/v1/runs \
  -H 'Content-Type: application/json' \
  -H "X-MOA-Token: ${MOA_TOKEN}" \
  -d '{"agent_id":"iris-assistant-1","input":"查询 order-001"}'
```

无 Token、无效 Token或 MOA project 不匹配返回 `401`；MOA 超时、限流、5xx 或畸形响应返回
`503`。原始 Token 不写入 PostgreSQL、Audit、Kafka、日志或模型上下文。

默认使用 `workspace/.dsh-base-agent/control.db`。需要 PostgreSQL 时，在 `.env` 中增加：

```dotenv
DSH_BASE_AGENT_DATABASE_URL=postgresql://agent:secret@localhost:5432/dsh_base_agent
DSH_BASE_AGENT_DATABASE_SCHEMA=dsh_base_agent
```

starter 启动器会通过 `ControlStoreConfig` 自动选择数据库；数据库配置不会进入 DSH
`RuntimeConfig`。

Tool 返回的 JSON 超过 64 KiB 时会自动保存为本地 Artifact，默认目录为
`workspace/.dsh-base-agent/artifacts`。可以指定其他目录：

```dotenv
DSH_BASE_AGENT_ARTIFACT_LOCAL_ROOT=/shared-pvc/dsh-base-agent/artifacts
```

PostgreSQL API/Worker 分离时，该目录必须是 API 和所有 Worker 共同挂载的持久卷，否则 API
可以查到 ArtifactRecord，但无法下载 Worker 所在节点上的正文。

未配置 PostgreSQL 时，`company-agent-server` 保留 SQLite 进程内执行，方便本地开发。配置
PostgreSQL 后，API 自动切换为只提交模式，还必须在另一个终端启动 Worker：

```bash
uv run company-agent-server
uv run company-agent-worker
```

API 只写入 `QUEUED` Run；Worker 从 PostgreSQL 领取任务、维护 Lease/Heartbeat 并执行 DSH。
不要在 PostgreSQL 模式下只启动 API，否则 Run 会一直保持 `queued`。

需要把 DSH 原始 Notification 直接发送到 Kafka 时增加：

```dotenv
DSH_BASE_AGENT_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
DSH_BASE_AGENT_KAFKA_TOPIC=dsh.notifications.v1
```

Publisher 使用有界内存队列且不使用 Outbox。Kafka 默认是非必需依赖：发送失败会记录日志，
不会让 Agent Run 失败；进程异常退出时允许丢失尚未发送的数据。

提交一个 Run：

```bash
curl -X POST http://127.0.0.1:8000/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user' \
  -H 'Idempotency-Key: starter-order-001' \
  -d '{"agent_id":"iris-assistant-1","input":"查询 order-001"}'
```

查询结果：

```bash
curl http://127.0.0.1:8000/v1/runs/{run_id} \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user'
```

查看和下载大 Tool Result：

```bash
curl http://127.0.0.1:8000/v1/runs/{run_id}/artifacts \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user'

curl -OJ http://127.0.0.1:8000/v1/runs/{run_id}/artifacts/{artifact_id}/content \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user'
```

## 多轮 Conversation

`POST /v1/runs` 仍是单次 Run，每次使用独立 DSH Session。需要让后续消息延续上下文时，先
创建 Conversation：

```bash
curl -sS -X POST http://127.0.0.1:8000/v1/conversations \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user' \
  -d '{"agent_id":"iris-assistant-1","metadata":{}}'
```

记录返回的 `conversation_id`。第一轮只询问订单状态：

```bash
curl -sS -X POST \
  http://127.0.0.1:8000/v1/conversations/{conversation_id}/runs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user' \
  -H 'Idempotency-Key: conversation-message-001' \
  -d '{"input":"查询订单状态","metadata":{}}'
```

等第一轮 Run 成功并要求提供订单号后，在同一个 Conversation 中提交第二轮：

```bash
curl -sS -X POST \
  http://127.0.0.1:8000/v1/conversations/{conversation_id}/runs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user' \
  -H 'Idempotency-Key: conversation-message-002' \
  -d '{"input":"订单号是 order-001","metadata":{}}'
```

两个 Run 拥有不同 `run_id` 和递增的 `sequence`，但其 Attempt 使用同一个
`dsh_session_id`。可通过以下接口查看确定顺序：

```bash
curl -sS http://127.0.0.1:8000/v1/conversations/{conversation_id}/runs \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user'
```

Conversation 的上下文归属于持久化的 `DSH_HOME + dsh_session_id`，不归属于某个 Worker。
在 PostgreSQL Worker 模式下，替代 Worker 会使用同一组标识创建新的 Harness，并继续提交下
一个 Turn；它不会自动重放被中断的 Run。Kubernetes 多 Worker 部署必须让 Worker 挂载同一
份 DSH Home 持久卷。

## 目录

```text
src/company_agent/
├── definition.py       Agent 组装入口
├── tools.py            Python Tool
├── memory.py           只读 Memory Provider（替换为公司检索服务）
├── context.py          静态 Context
├── authorization.py    Run/Tool/Memory 权限策略
└── app.py              ControlPlane 与 FastAPI 启动

workspace/
└── .dsh/skills/
    └── order-support/
        └── SKILL.md     DSH 原生项目级 Skill
```

## 添加 Tool

在 `tools.py` 中定义：

```python
@tool(side_effect=False, permissions=("customer:read",))
def query_customer(customer_id: str, context: ToolContext) -> dict[str, str]:
    ...
```

然后把 Tool 加入 `definition.py` 的 `Agent.tools`，并在 `Agent.permissions` 和公司授权策略中
声明相同权限。租户、Principal、Run 和 Attempt 身份必须从 `ToolContext` 获取，不能信任
模型传入这些身份字段。

副作用 Tool 应使用 `SideEffect.IDEMPOTENT` 或 `SideEffect.UNSAFE`，并由自定义 Authorizer
明确授权；下游服务应使用 `ToolContext.idempotency_key` 去重。

## 添加 Skill

创建：

```text
workspace/.dsh/skills/<skill-name>/SKILL.md
```

再把名称加入 `Agent.skills`：

```python
skills=("order-support", "another-skill")
```

base-agent 会把当前 ControlPlane 的 `workspace/.dsh/skills` 显式注册为 DSH 的
`customSkillDirs`。这是必要的，因为 DSH 默认把最近的 Git 根目录作为项目根；当 workspace
位于仓库的子目录（例如本 Starter）时，仅依赖项目级自动发现会扫描错误的目录。

当前 base-agent 还不会安装远程 Skill、锁定版本或强制 allowlist；生产应用不能把
`Agent.skills` 误当成完整的供应链治理。

## 添加 Context 和 Memory

静态、非敏感、随 Agent 版本发布的规则放在 `context.py`，它们会被确定性地组合进 Agent
Prompt。

只有模型按需决定是否查询的信息，才像 `get_request_context` 一样封装成只读 Tool，通过
`ToolContext` 确定访问范围。

每个用户 Turn 都应该自动带入的个性化 Memory，使用 `memory.py` 中的
`@memory_provider`。Starter 当前返回一条演示偏好；业务只需把函数体替换成公司 Memory/RAG
服务调用，并继续使用 `MemorySearchRequest` 中由 ControlPlane 绑定的租户、Principal 和
Conversation 身份。不要增加写入方法，Memory 写入由 Kafka 下游服务负责。

Memory Provider 已纳入权限、超时、有界 Event/Audit 和 16 KiB 模型上下文总预算。详细约束
见主项目的 [`docs/memory-retrieval.md`](../docs/memory-retrieval.md)。

## 当前限制

- 单次 `/v1/runs` 仍使用独立 DSH Session；Conversation Run 串行复用持久化 Session；
- PostgreSQL 模式支持替代 Worker 使用共享 DSH Home 接收 Conversation 的下一个 Turn；
- SQLite 只适用于单进程；PostgreSQL Worker 已具备第一版 lease/heartbeat/fencing；
- Workspace 是本地目录，尚未接入 S3 materialize/commit；
- Tool JSON 大结果已自动写 LocalArtifactStore；S3、容量配额和 TTL 尚未实现；
- `resume` 受当前 DSH SDK 能力限制；
- 示例 Authorizer 和内存订单数据仅用于演示，必须由业务实现替换。

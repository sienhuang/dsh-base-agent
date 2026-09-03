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

当前多轮能力依赖同一个 ControlPlane 进程中的存活 Runtime。服务重启后会把已打开过 DSH
Session 的 Conversation 标为 `BLOCKED`，直到 DSH Resume 扩展实现。

## 目录

```text
src/company_agent/
├── definition.py       Agent 组装入口
├── tools.py            Python Tool
├── context.py          静态 Context
├── authorization.py    Run/Tool 权限策略
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

Starter 使用 DSH 的项目级 Skill 目录约定，因此本地 Skill 可以被当前 workspace 发现。但
当前 base-agent 还不会安装远程 Skill、锁定版本或强制 allowlist；生产应用不能把
`Agent.skills` 误当成完整的供应链治理。

## 添加 Context

静态、非敏感、随 Agent 版本发布的规则放在 `context.py`，它们会被确定性地组合进 Agent
Prompt。

租户数据、用户偏好和频繁变化的数据不要直接拼进静态 Prompt，应像
`get_request_context` 一样封装成只读 Tool，通过 `ToolContext` 确定访问范围。

当前核心 SDK 还没有正式的动态 `ContextProvider`，也没有把 DSH Plugin Context 纳入公司
授权和审计。Starter 不伪造这两项能力。

## 当前限制

- 单次 `/v1/runs` 仍使用独立 DSH Session；Conversation Run 在当前进程内串行复用 Session；
- 已打开的 Conversation 暂不能跨 ControlPlane 重启恢复；
- SQLite 只适用于单进程；
- Workspace 是本地目录，尚未接入 S3 materialize/commit；
- Artifact 自动收集尚未实现；
- `resume` 受当前 DSH SDK 能力限制；
- 示例 Authorizer 和内存订单数据仅用于演示，必须由业务实现替换。

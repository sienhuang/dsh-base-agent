# 业务 Agent 开发 Guideline

本文面向基于 `dsh-base-agent` 开发业务 Agent 的 Python 开发者，描述当前 `0.1.x` 实现下的
推荐工程结构、扩展方式、安全边界、运行方式和交付检查项。

`dsh-base-agent` 提供 Python SDK、稳定 HTTP API、身份和权限接缝、Run/Conversation 管理、
Tool/Memory 到 DSH 的桥接、审计与持久化。业务应用负责定义 Agent、Tool、Skill、Memory、
授权策略和业务系统调用；DSH 继续负责模型循环和 Session 历史。

## 1. 开发者需要交付什么

一个业务 Agent 应至少交付以下内容：

```text
my-business-agent/
├── pyproject.toml
├── .env.example                 非敏感配置模板
├── src/my_agent/
│   ├── definition.py            唯一的 Agent 组装入口
│   ├── tools.py                 业务 Tool
│   ├── memory.py                可选，只读 Memory Provider
│   ├── context.py               可选，版本化静态 Context
│   ├── authorization.py         Run/Tool/Memory 授权策略
│   ├── app.py                   FastAPI/ControlPlane 入口
│   └── worker.py                PostgreSQL 模式 Worker 入口
├── workspace/
│   ├── AGENTS.md                可选，DSH 工作区指导
│   └── .dsh/skills/
│       └── <skill-name>/
│           ├── SKILL.md
│           ├── references/      可选
│           ├── scripts/         可选
│           └── assets/          可选
└── tests/
```

最直接的起点是复制 [`starter/`](../starter/README.md)，然后替换其中的订单示例。不要复制
`.env`、`.venv` 或 `workspace/.dsh-base-agent/` 运行状态。

当前 SDK 要求 Python 3.12 或更高版本。业务项目只从 `dsh_base_agent` 根包导入公共 API，
不要依赖 `dsh_base_agent.adapters`、`control` 或 `store` 内部实现。

业务项目通过固定版本依赖 SDK：

```toml
[project]
requires-python = ">=3.12"
dependencies = [
    "dsh-base-agent==0.1.1",
]
```

在当前源码仓库联调时，可以像 Starter 一样临时使用本地 editable source：

```toml
[tool.uv.sources]
dsh-base-agent = { path = "..", editable = true }
```

上面的本地 path 只用于 SDK 仓库内运行 Starter。把 Starter 复制成独立业务项目后，删除该
配置并改为固定 Git Tag：

```toml
[tool.uv.sources]
dsh-base-agent = { git = "https://github.com/sienhuang/dsh-base-agent.git", tag = "v0.1.1" }
```

仓库根目录现在就是 Python 项目，因此不再需要 `subdirectory`。执行 `uv sync` 后应提交新生成
的 `uv.lock`。公司 Python 包仓库可用后，业务项目应进一步删除 `[tool.uv.sources]`，只保留
`dsh-base-agent==0.1.1`。

## 2. 请求是怎样运行的

```text
HTTP 请求
  -> RequestAuthenticator 建立 Principal
  -> Authorizer.authorize_run
  -> ControlPlane 创建 Run
  -> 本地执行或 PostgreSQL Worker 领取
  -> DSH Session / Agent Loop
       -> Memory pre-step -> authorize_memory -> Memory Provider
       -> MCP Tool call   -> authorize_tool   -> Python Tool
  -> Run / Attempt / Event / Audit / Artifact
```

需要明确区分以下对象：

- `Agent`：不可变的业务能力定义。
- `Run`：调用方看到的一次业务任务。
- `RunAttempt`：Run 的一次真实执行；Retry 会创建新 Attempt。
- `DSH Session`：模型上下文和执行历史。
- `Conversation`：一个固定 DSH Session 的业务所有者，多个 Run 在其中串行形成多轮对话。

`POST /v1/runs` 为单轮模式，每个 Attempt 使用独立 Session。多轮对话必须先创建
Conversation，再向 `/v1/conversations/{conversation_id}/runs` 提交消息。

## 3. 定义 Agent

在 `definition.py` 中集中组装 Agent：

```python
from dsh_base_agent import Agent

from my_agent.memory import search_memory
from my_agent.tools import query_order


def build_agent() -> Agent:
    return Agent(
        name="order-assistant",
        version="1.0.0",
        prompt="""你是公司订单助手。

订单状态必须通过已注册工具查询，不得猜测。
        """.strip(),
        tools=(query_order,),
        skills=("order-support",),
        memory_providers=(search_memory,),
        permissions=frozenset({"orders:read", "memory:read"}),
    )
```

规则如下：

- `name` 是 API 的 `agent_id`，必须稳定且只能包含字母、数字、点、下划线和连字符。
- `version` 表示业务定义版本；Prompt、Tool 实现、Memory 检索语义或业务行为变化时应递增。
- `tools`、`skills`、`memory_providers` 只注册该 Agent 实际需要的能力。
- `permissions` 是 Agent 能力上限，必须覆盖所有 Tool 和 Memory Provider 声明的权限。
- API 和 Worker 必须注册完全相同的 Agent 定义，否则持久化的 fingerprint 校验会失败。

Agent fingerprint 包含 Prompt、Tool Schema 和配置、Memory Provider 配置、Skill 名称、权限及
版本，但当前不会对 Python 函数体或 Skill bundle 内容做摘要。因此不能用 fingerprint 代替
发布版本管理；实现或 Skill 内容变化时必须显式递增 `version` 并保留可回滚制品。

## 4. 定义 Python Tool

Tool 是模型按需调用业务系统的入口：

```python
from typing import Any

from dsh_base_agent import SideEffect, ToolContext, tool


@tool(
    permissions=("orders:read",),
    side_effect=SideEffect.READ_ONLY,
    timeout_seconds=5.0,
)
async def query_order(order_id: str, context: ToolContext) -> dict[str, Any]:
    """查询当前租户内的一个订单。"""

    row = await order_client.get_order(
        tenant_id=context.tenant_id,
        order_id=order_id,
    )
    return {"found": row is not None, "order": row}
```

必须遵守：

- 每个模型可填写的参数都必须有类型标注；不能使用 `*args`、`**kwargs` 或位置专用参数。
- Docstring 或 `description` 会成为模型看到的 Tool 描述，应说明何时调用、输入语义和结果。
- Tenant、Principal、Run、Attempt 和幂等身份只能从 `ToolContext` 获取，不能让模型通过参数
  提供。
- Tool 内部仍要把 `tenant_id` 加入下游查询条件；Prompt 中写“不得跨租户”不构成隔离。
- 为外部 I/O 设置合理的 Tool 超时和下游客户端超时，不要在同步 Tool 中阻塞事件循环。
- 返回 JSON 可序列化数据，不返回数据库连接、生成器或任意 Python 对象。
- 优先提供过滤、分页、字段投影和专用分析 Tool，避免一次返回巨大结果。

Tool 的副作用分为：

| 类型 | 用途 | 要求 |
| --- | --- | --- |
| `READ_ONLY` | 查询 | 默认选择 |
| `IDEMPOTENT` | 可安全重试的写操作 | 下游必须使用 `context.idempotency_key` 去重 |
| `UNSAFE` | 非幂等写操作 | 需要显式授权和业务风险控制 |

当前版本尚未完成可恢复的人工 Approval 流程。`confirmation_required=True` 只是定义元数据，
不能视为已经获得用户确认。因此生产业务默认只开放只读 Tool；写 Tool 必须经过专项评审，
并由下游幂等、权限和审计共同保护。

Tool 会由 base-agent 自动转换为 loopback MCP Tool，业务开发者不需要编写 JavaScript MCP
插件。Tool 单次 JSON 结果超过 64 KiB 时会写入 ArtifactStore，模型只收到预览和引用；当前
模型不能继续按 chunk 读取完整 Artifact，所以需要模型分析完整大结果时，应另外设计分页或
聚合 Tool。

## 5. 定义权限和 Authorizer

权限有三层，缺一不可：

```text
Agent.permissions          Agent 声明的能力上限
Tool/Provider.permissions  单项能力要求
Authorizer                 当前 Principal 的运行时授权决定
```

示例：

```python
from dsh_base_agent import (
    Agent,
    AuthorizationDenied,
    MemoryAuthorization,
    Principal,
    SideEffect,
    ToolAuthorization,
)


class CompanyAuthorizer:
    async def authorize_run(self, principal: Principal, agent: Agent) -> None:
        if not await iam.can_use_agent(principal, agent.name):
            raise AuthorizationDenied("agent access denied")

    async def authorize_tool(self, request: ToolAuthorization) -> None:
        if request.tool.side_effect is not SideEffect.READ_ONLY:
            raise AuthorizationDenied("mutating tools are not enabled")
        granted = await iam.permissions(request.principal)
        missing = request.tool.permissions - granted
        if missing:
            raise AuthorizationDenied("missing tool permissions")

    async def authorize_memory(self, request: MemoryAuthorization) -> None:
        granted = await iam.permissions(request.principal)
        missing = request.provider.permissions - granted
        if missing:
            raise AuthorizationDenied("missing memory permissions")
```

`request.tool` 和 `request.provider` 来自 Host 已注册的 Agent 定义，不来自模型。生产 Authorizer
应调用公司 IAM/策略服务并采用失败闭合策略；Starter 的内存权限表只用于本地演示。

认证和授权不是一回事：Authenticator 负责建立“调用者是谁”，Authorizer 负责判断“这个调用者
能否运行 Agent 或访问能力”。禁止从请求正文、模型参数或 Prompt 推断可信身份。

## 6. 选择 Context、Memory、Tool 或 Skill

| 信息类型 | 放置位置 | 加载时机 |
| --- | --- | --- |
| Agent 身份、版本化核心规则 | `Agent.prompt` / `context.py` | 每次模型请求的系统上下文 |
| 工作区指导、非关键约定 | `workspace/AGENTS.md` | DSH Session 首次加载，变化时校对 |
| 每个 Turn 自动需要的个性化背景 | `@memory_provider` | 每个用户 Turn 的第一个 Step |
| 模型按需查询的动态事实 | `@tool` | 模型决定调用时 |
| 一类任务的操作流程和参考资料 | Skill | 模型按需加载 |

### 6.1 静态 Context 与 AGENTS.md

需要与 Agent 版本绑定的核心行为应确定性地组合进 `Agent.prompt`。它会进入 Agent fingerprint：

```python
static_context = render_static_context(static_context_sections())
prompt = f"{BASE_PROMPT}\n\n{static_context}"
```

也可以在 `workspace/AGENTS.md` 中放置仓库或工作区指导。DSH 的 `agent-instructions` 插件会自动
发现它；无需 Python 手动拼接。它以 DSH Session 为边界保存基线：独立 Run 看起来每次都会
加载，而同一 Conversation 的多个 Run 复用同一基线。未变化的内容不会重复插入。

`AGENTS.md` 当前不进入 Agent fingerprint，所以不应单独承载必须锁定、审计和回滚的核心业务
策略。文件中也不得保存密钥、Token、租户数据或用户隐私。

### 6.2 Memory Provider

每个 Turn 都要自动加入的只读背景信息使用 Memory Provider：

```python
from dsh_base_agent import MemoryItem, MemorySearchRequest, memory_provider


@memory_provider(
    name="company-memory",
    version="1.0.0",
    permissions=("memory:read",),
    timeout_seconds=3.0,
    max_results=3,
)
async def search_memory(request: MemorySearchRequest) -> list[MemoryItem]:
    rows = await memory_client.search(
        tenant_id=request.tenant_id,
        principal_id=request.principal_id,
        conversation_id=request.conversation_id,
        query=request.query,
    )
    return [
        MemoryItem(
            memory_id=row.id,
            content=row.summary,
            source="company-memory",
            score=row.score,
        )
        for row in rows
    ]
```

Memory 只做检索和注入，不提供写入 API。它是背景数据，不是高权限指令；返回内容必须按可信
Tenant/Principal 查询。Provider 失败或超时时当前采用 fail-open：记录 Event/Audit 后，Run 在
没有该 Memory 的情况下继续。

## 7. 添加 Skill

将完整 bundle 放入：

```text
workspace/.dsh/skills/<skill-name>/SKILL.md
```

最小 `SKILL.md`：

```md
---
name: order-support
description: 查询订单状态并按统一格式回答订单问题。
---

# Order support

1. 提取订单号，不明确时向用户确认。
2. 调用 `query_order`，不得猜测订单状态。
3. 返回订单号、订单状态和数据来源。
```

然后把名称加入 `Agent.skills`。`dsh-base-agent` 会将
`workspace/.dsh/skills` 注册为 DSH 的 `customSkillDirs`。

当前 Skill 能力是部分实现：`Agent.skills` 会进入 fingerprint 并控制是否启用 DSH Skill Tool，
但还没有远程安装、版本锁定、内容摘要和严格 allowlist。业务仓库必须固定 Skill bundle，审查
其中的 `SKILL.md`、脚本和资源，不得安装来源不明的 Skill。

## 8. 组装服务

本地开发可参考 Starter 的 `build_control()`：

```python
control = ControlPlane(
    workspace=workspace,
    runtime=RuntimeConfig.from_env(env_file=root / ".env"),
    store_config=ControlStoreConfig.from_env(env_file=root / ".env"),
    artifact_store_config=ArtifactStoreConfig.from_env(env_file=root / ".env"),
    authorizer=CompanyAuthorizer(),
    auto_execute=True,
)
control.register(build_agent())
api = create_app(control, authenticator=auth_config.create())
```

选择运行模式：

| 模式 | 使用场景 | 进程 |
| --- | --- | --- |
| SQLite + `auto_execute=True` | 本地开发、单进程验证 | API 内执行 |
| PostgreSQL + `auto_execute=False` | 集成及生产形态 | API 和独立 Worker |

PostgreSQL 模式下必须同时启动 API 和 Worker，否则 Run 会一直停留在 `queued`。API 与 Worker
必须使用同一数据库、同一 Agent 定义，并共享持久化 DSH Home。使用 LocalArtifactStore 时还
必须共享 Artifact 目录。

## 9. 配置和密钥

本地 `.env` 的最小模型配置为：

```dotenv
DSH_PROVIDER=deepseek-official
DSH_MODEL=<approved-model>
DSH_BASE_URL=<company-model-gateway>
DSH_API_KEY=<local-secret>
DSH_HOME=.dsh-base-agent/dsh-home
```

约束如下：

- 只提交 `.env.example`，禁止提交真实 `.env`、API Key、MOA Token 和数据库密码。
- 生产环境由容器 Secret 或公司密钥系统注入，不把密钥放入 Prompt、AGENTS.md、Skill、
  metadata、Event 或日志。
- 本地可关闭 MOA 认证并使用 `X-Tenant-ID/X-Principal-ID`；生产入口应启用可信认证。
- 本地 `DSH_HOME` 可使用项目目录；多 Worker 下必须使用所有 Worker 可见的持久卷。
- 不要提交 `workspace/.dsh-base-agent/`，其中包含本地数据库、DSH Session 和 Artifact 状态。

完整可用变量见 [`../.env.example`](../.env.example) 和
[`../starter/.env.example`](../starter/.env.example)。

## 10. 调用 API

### 10.1 独立 Run

```bash
curl -sS -X POST http://127.0.0.1:8000/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user' \
  -H 'Idempotency-Key: order-query-001' \
  -d '{"agent_id":"order-assistant","input":"查询 order-001","metadata":{}}'
```

提交成功返回 `202`。轮询：

```bash
curl -sS http://127.0.0.1:8000/v1/runs/{run_id} \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user'
```

客户端应为每次逻辑提交生成稳定、唯一的 `Idempotency-Key`。同一 Tenant 下，相同键和相同
请求返回原 Run；相同键但请求内容不同返回 `409 Conflict`。不要在不同用户动作之间复用同一个
固定键。

### 10.2 Conversation

```text
POST /v1/conversations
POST /v1/conversations/{conversation_id}/runs
GET  /v1/conversations/{conversation_id}/runs
```

同一 Conversation 固定 Tenant、Principal、Agent 版本、DSH Home 和 Session。服务端保证 Run
按 sequence 串行执行；客户端不要并发假设后一个 Turn 已看到尚未完成的前一个 Turn。

### 10.3 事件和 Artifact

```text
GET /v1/runs/{run_id}/events?after=<sequence>
GET /v1/runs/{run_id}/artifacts
GET /v1/runs/{run_id}/artifacts/{artifact_id}/content
```

业务状态以 Run API 为准；Event 是有界观测投影，DSH Session/Event 才是执行细节真相。Kafka
Notification 是允许少量丢失的实时观测流，不是可靠业务消息，也不具备事务 Outbox 语义。

## 11. 最低测试要求

每个业务 Agent 至少覆盖：

- Agent 能构造，`name`、Tool、Skill、Provider 和权限符合预期。
- Tool 参数 Schema 正确，缺失/多余/非法参数被拒绝。
- Tool 使用 `ToolContext` 中的 Tenant/Principal，不接受模型伪造身份。
- 跨租户查询返回空结果或拒绝，不发生数据泄漏。
- Authorizer 拒绝未知租户、缺少权限和未批准的副作用 Tool。
- 同步和异步 Tool 的成功、异常、超时及 readiness。
- Memory 只查询当前身份，结果数量和内容边界符合预期。
- Skill 文件存在，frontmatter 名称与 `Agent.skills` 一致。
- API 注册了正确 `agent_id`，重复幂等请求不会创建多个 Run。
- 独立 Run 使用独立 Session；Conversation 多个 Run 能保持上下文和顺序。
- 大 Tool 结果外部化后不会把完整正文写入 Event/Audit。
- API 与 Worker 构造出的 Agent fingerprint 一致。

提交前执行：

```bash
uv sync --all-groups
uv run ruff check src tests
uv run mypy src
uv run pytest
```

涉及真实模型和下游系统时，还应在隔离测试租户中完成最小 smoke test，验证 Tool 实际被调用，
而不是只验证模型生成了看起来正确的答案。

## 12. 发布检查表

- [ ] `agent_id` 唯一、稳定，调用方配置与注册名称一致。
- [ ] 行为变化已递增 Agent、Tool/Provider 或应用版本。
- [ ] API 和 Worker 使用同一构建制品。
- [ ] 所有 Tool/Provider 权限都包含在 `Agent.permissions` 中。
- [ ] Authorizer 已接公司权限源，不再使用 Starter 演示表。
- [ ] 所有数据访问都显式带可信 Tenant/Principal。
- [ ] 生产认证已开启，客户端身份 Header 不再被直接信任。
- [ ] 写 Tool 有下游幂等、审计和专项风险评审。
- [ ] Skill bundle 已固定和审查，没有未知脚本或越界路径。
- [ ] `.env`、Token、用户数据和本地运行状态未进入制品或 Git。
- [ ] PostgreSQL、DSH Home 和 Artifact 共享存储已按部署拓扑配置。
- [ ] 健康检查、Run 失败率、Worker lease、Tool 超时和 Artifact 容量已接监控。
- [ ] 已了解并接受下一节中的当前能力边界。

## 13. 当前版本不能承诺的能力

以下能力尚未完整实现，业务方案不能假设它们已经可用：

- Skill 远程安装、严格 allowlist、内容摘要和供应链版本锁定。
- S3 Workspace/Artifact 后端、Artifact 容量配额、TTL 和孤儿扫描。
- 模型按 chunk 继续读取完整大 Artifact。
- 被中断原 Turn 的可靠恢复，以及完整 WAITING/Approval 流程。
- Run/Audit/Event/Kafka 的全链路事务 Outbox；Kafka Notification 当前为 best-effort。
- 全部入口和累计数据面的统一 Payload 限额。
- 仅靠 SQLite 实现多副本或分布式并发。

上线前应再次核对 [`implementation-status.md`](implementation-status.md)；它是当前代码能力的
事实清单。生产化部署和后续缺口见 [`production-roadmap.md`](production-roadmap.md)。

## 14. 常见错误

- `Agent '<name>' is not registered`：请求中的 `agent_id` 与 `build_agent().name` 不一致，或服务
  启动入口没有调用 `control.register(...)`。
- `the starter only enables the demo-tenant tenant`：仍在使用演示 Authorizer，或 Header 中的
  Tenant 不是 `demo-tenant`。
- `address already in use`：已有服务占用了监听端口；停止旧进程或修改应用端口。
- Run 长期 `queued`：配置了 PostgreSQL，但没有启动 Worker，或 Worker 无法领取 Lease。
- `missing Tool/Memory permissions`：单项能力声明的权限不在当前 Principal grants 中。
- Agent 构造时报 undeclared permissions：Tool/Provider 权限没有加入 `Agent.permissions`。
- Skill 找不到：bundle 不在 `workspace/.dsh/skills/<name>/SKILL.md`，或名称与
  `Agent.skills` 不一致。
- 修改 `AGENTS.md` 后行为没有按预期变化：确认 DSH 实际 `cwd` 指向该 workspace，并使用新
  Session 验证；关键版本化规则应放在 `Agent.prompt`。

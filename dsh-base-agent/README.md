# dsh-base-agent

`dsh-base-agent` 是公司级 Agent 应用平台和控制面；DeepSeek Harness（DSH）是底层执行
引擎。本项目不实现自己的模型循环。

当前第一阶段提供：

- 公司统一的 Python `Agent` / `@tool` SDK；
- Python Tool 到 loopback MCP 的编译和治理边界；
- 业务 Run、RunAttempt、DSH Session 映射；
- 稳定的 FastAPI Run API；
- 追加式运行事件和独立审计记录；
- 官方 `deepseek-harness-sdk` 的窄适配层。

当前代码的逐项完成度见 [`docs/implementation-status.md`](docs/implementation-status.md)。
业务开发可从 [`starter/`](starter/README.md) 复制一个最小应用，再添加自己的 Tool、Skill、
静态 Context 和授权策略。

```python
from dsh_base_agent import Agent, ControlPlane, RuntimeConfig, tool


@tool(side_effect=False)
def query_order(order_id: str) -> dict[str, str]:
    """查询订单。"""
    return {"order_id": order_id, "status": "paid"}


agent = Agent(
    name="order-assistant",
    prompt="你是订单助手。",
    tools=(query_order,),
    skills=("order-support",),
)

control = ControlPlane(
    workspace=".",
    runtime=RuntimeConfig.from_env(),
)
control.register(agent)
```

详细架构原则见上级目录的 `docs/dsh-base-agent-positioning.md`。

Conversation 持有 DSH Session、多个 Run 串行形成多轮对话的 v0.2 方案见
[`docs/conversation-session-v0.2.md`](docs/conversation-session-v0.2.md)。该文档目前是设计草案，
现有 v0.1 运行逻辑仍然为每个 RunAttempt 创建独立 Session。

## 本地运行

```bash
uv sync --all-groups
cp .env.example .env
# 编辑 .env，填入 DSH_API_KEY 等配置
uv run python examples/order_assistant.py
```

`RuntimeConfig.from_env()` 默认读取当前目录的 `.env`。真实环境变量的优先级高于
`.env`，因此生产部署仍可由容器或 Secret 注入配置；读取过程不会修改 `os.environ`。
也可以显式指定文件或禁用文件读取：

```python
RuntimeConfig.from_env(env_file="config/local.env")
RuntimeConfig.from_env(env_file=None)
```

`ControlPlane` 默认把业务状态保存在 workspace 的
`.dsh-base-agent/control.db`，把每个不可变 Agent 定义的 DSH 数据隔离在独立目录。一次业务
Run 可以拥有多个 RunAttempt，每个 Attempt 对应一个独立的 DSH Session。

## HTTP API

应用代码注册 Agent 后，使用 `create_app(control)` 创建 FastAPI：

```python
from dsh_base_agent import create_app

api = create_app(control)
```

调用方必须携带 `X-Tenant-ID` 和 `X-Principal-ID`：

```bash
curl -X POST http://127.0.0.1:8000/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: demo-tenant' \
  -H 'X-Principal-ID: demo-user' \
  -H 'Idempotency-Key: order-request-001' \
  -d '{"agent_id":"order-assistant","input":"查询订单 order-001"}'
```

稳定业务接口包括：

```text
POST   /v1/runs
GET    /v1/runs/{run_id}
GET    /v1/runs/{run_id}/events
POST   /v1/runs/{run_id}/cancel
POST   /v1/runs/{run_id}/retry
POST   /v1/runs/{run_id}/resume
GET    /v1/runs/{run_id}/artifacts

POST   /v1/conversations
GET    /v1/conversations/{conversation_id}
POST   /v1/conversations/{conversation_id}/runs
GET    /v1/conversations/{conversation_id}/runs
```

`POST /v1/runs` 保持单次独立 Session 语义。需要多轮上下文时，先创建 Conversation，再向
Conversation 连续提交 Run。同一 Conversation 固定绑定 Tenant、Principal、Agent 版本、DSH
Home 和 Session ID，并在当前 ControlPlane 进程内严格串行执行。

## 安全默认值

- 默认 Authorizer 只允许只读 Tool；副作用 Tool 必须配置公司自己的 `Authorizer`。
- Tool 参数和结果正文不写入 Audit，只保存摘要。
- 控制面只持久化 DSH 生命周期事件的有界投影，不复制 reasoning、assistant message、Tool
  参数或完整结果。
- DSH 内置 Bash、文件、Web、Workflow/Subagent 等 Tool 默认关闭；应按公司治理策略显式
  开放。

## Skill 生命周期（待实现）

`Agent.skills` 目前只是声明式的 Skill 名称列表：名称会进入 Agent fingerprint，并决定
是否保留 DSH 原生 `tool-skill`。它尚不会安装 Skill 包、验证 Skill 是否存在，也不会
将 DSH 可见的 Skill 限制为该列表。因此：

```python
skills=("order-support",)
```

不等于 `order-support` 已经可用。在完成下述生命周期之前，这只是一个待解析的声明。

### 职责边界

- base-agent 负责 Skill 的注册、来源解析、版本锁定、内容校验、allowlist、不可变快照、
  安装和审计。
- DSH 负责通过原生 Skill Provider 发现已安装的 Skill，生成模型可见的目录，并在
  模型调用 `skill(name=...)` 时按需加载完整指令。

建议的公司 Skill 源目录：

```text
company-skills/
└── order-support/
    ├── SKILL.md
    ├── references/
    ├── scripts/
    └── assets/
```

启动 DSH Runtime 之前，base-agent 应将 Agent 选中的完整 Skill bundle 原子化到 Agent
指纹隔离的 DSH Home：

```text
<DSH_HOME>/agents/<agent-fingerprint>/skills/order-support/SKILL.md
```

建议的执行流程：

```text
Agent.skills
  -> SkillRegistry 解析名称和版本
  -> 校验 SKILL.md 与 bundle
  -> 生成选中 Skill 的不可变快照
  -> SkillMaterializer 安装到 Agent 专属 DSH Home
  -> 启动 DSH Runtime
  -> DSH 原生 Skill Provider 发现和加载
```

后续实现必须满足：

- 引入 `SkillRef` / `ResolvedSkill`，不再只依赖无版本字符串。
- 实现 `SkillRegistry` 和 `SkillMaterializer`，在 Runtime 启动前失败闭合地完成解析与安装。
- 复制完整 Skill bundle，而不是只复制 `SKILL.md`；校验路径遍历和符号链接逃逸。
- 验证 frontmatter 中的 `name` 和 `description`，并限制 kebab-case Skill 名称。
- 生产 Profile 应隔离 DSH 默认项目/用户 Skill 根目录，只向当前 Agent 暴露已声明的
  allowlist。
- Agent fingerprint 必须包含解析后的 Skill 版本和内容摘要，不能只包含名称。
- Run/Audit 应记录实际使用的 Skill 名称、版本、来源和内容摘要。
- 要有测试覆盖：缺失/非法 Skill 拒绝启动、只暴露 allowlist、资源相对路径可用、
  版本或内容变更导致新 fingerprint。

## 当前边界

这是第一条可运行的垂直切片，还不是生产完成版：

- 当前 SQLite Store 面向单控制面进程；多副本需要 PostgreSQL Store、Worker lease 和
  heartbeat。
- 当前 DSH SDK pin 没有 Host session-resume/cancel 方法；`resume` 会明确返回不支持，取消
  通过关闭本次 DSH Runtime 完成。后续采用 DSH 插件和版本化 JSON-RPC 扩展，设计见
  [`docs/dsh-resume-extension.md`](docs/dsh-resume-extension.md)。
- Conversation 已支持当前进程内的多 Run 串行和 Session 复用；已打开的 Conversation 在
  ControlPlane 重启后会失败闭合为 `BLOCKED`，不会用新 Runtime 冒充恢复原 Session。
- `skills` 已进入 Agent fingerprint，并控制是否开放 DSH 原生 Skill Tool；Skill 包的安装、
  allowlist 和版本锁定尚未自动化。
- Artifact 数据模型和查询 API 已建立，DSH Attachment 到业务 Artifact 的自动收集尚未接入。
- 服务重启时，原本 `RUNNING` 的 Attempt 会标记为 `INTERRUPTED`，业务 Run 标记为
  `FAILED`，不会盲目重放原 prompt；调用方可显式 retry 创建新 Attempt 和 Session。

## 验证

```bash
.venv/bin/ruff check src tests examples
.venv/bin/mypy src
.venv/bin/pytest
uv build
```

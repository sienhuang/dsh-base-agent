# dsh-base-agent 定位与总架构原则

## 核心定位

> `dsh-base-agent` 是公司级 Agent 应用平台和控制面；DSH 是底层执行引擎。

`dsh-base-agent` 不重新实现 Agent Runtime。

## 职责边界

| 层 | 负责内容 |
| --- | --- |
| DSH | 模型循环、Tool 调用、Session、Skill、Workflow/Subagent、上下文压缩 |
| dsh-base-agent | Python SDK、业务 API、任务管理、租户权限、审计、生产治理 |

判断一段代码应归属哪一层时，使用以下原则：

- 如果代码在决定“下一轮给模型发送什么、怎样调用 Tool、怎样压缩上下文”，它属于 DSH。
- 如果代码在决定“谁能运行、任务是否排队、是否超额、是否审批、怎样审计以及怎样满足 SLA”，它属于 `dsh-base-agent`。

## 核心数据关系

```text
Tenant
  └── AgentDefinition + Version
        └── Run（业务任务）
              ├── RunAttempt（实际执行尝试）
              │     └── DSH Session ID
              ├── Audit Log
              ├── Artifact
              └── Approval / Waiting
```

同一次等待后继续或安全的基础设施恢复复用原 RunAttempt 和 DSH Session；用户主动重试或
完整重跑创建新的 RunAttempt 和 DSH Session。Run 始终是业务调用方看到的稳定任务身份。

## 两个真相源

- DSH Session/Event 是 Agent 实际执行过程的真相。
- base-agent Run/Audit 是公司业务任务及治理状态的真相。

base-agent 保存业务 Run、RunAttempt 与 DSH Session 的映射，可以转发、索引和提取 DSH
Event 中的审计事实，但不得复制 DSH 的全部内部消息并维护第二套模型对话状态机。

## 第一阶段范围

### 1. 公司统一 Python SDK

业务开发者应只需使用 `Agent` 和 `tool` 等稳定抽象。SDK 负责：

- Python Tool 自动转换为 MCP Tool。
- Agent 定义编译成 DSH Profile/配置。
- Tool 权限、超时、重试和审计元数据。
- 保持本地运行与服务端运行语义一致。

### 2. 稳定业务 API

FastAPI 不直接暴露 DSH 内部协议，而是提供公司稳定契约：

```text
POST   /v1/runs
GET    /v1/runs/{run_id}
GET    /v1/runs/{run_id}/events
POST   /v1/runs/{run_id}/cancel
POST   /v1/runs/{run_id}/retry
POST   /v1/runs/{run_id}/resume
GET    /v1/runs/{run_id}/artifacts
```

DSH 的 Session API、事件格式或 SDK 发生变化时，业务调用方不需要同步修改。

### 3. 任务控制面

base-agent 管理业务 Run 的生命周期：

```text
QUEUED
  → RUNNING
  → WAITING
  → SUCCEEDED / FAILED / CANCELLED
```

控制面包括：

- 幂等键、队列和优先级。
- Worker lease、heartbeat 和超时。
- 重试、取消和恢复。
- WAITING 与人工审批。
- 结果和 Artifact 持久化。

这里管理的是业务 Run，不是重新实现 DSH 的模型执行循环。

### 4. 租户权限

每次调用至少携带：

```text
principal_id
tenant_id
agent_id
run_id
tool_name
```

权限检查至少覆盖：

- 用户能否运行这个 Agent。
- Agent 能否调用这个 Tool。
- Tool 能否访问当前租户资源。
- 是否允许副作用操作。
- 是否必须人工审批。
- 租户并发量和 Token 配额。

租户隔离不能只依赖 Prompt 对模型的约束。

### 5. 审计

审计日志与普通运行日志分开，采用追加写，并至少记录：

- 谁在什么时间提交了什么任务。
- 使用的 Agent、模型、配置及其版本。
- Tool 调用及参数、结果摘要。
- 权限判定和审批过程。
- 谁执行了取消、重试或恢复。
- 敏感字段脱敏记录。

## 对 `next` 项目的使用边界

`next` 只能作为生产控制面能力的零件库：

- 可以选择性迁移 Worker、lease、heartbeat、EventStore、Checkpoint、Run 状态机和 Artifact。
- 不迁移 Python Harness、模型循环或另一套 Agent Runtime。
- Flow/Workflow 和 Subagent 的实际模型执行优先交给 DSH；base-agent 只提供业务任务治理。


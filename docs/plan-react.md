# Plan-ReAct 与确定性 Workflow

当前实现提供一种受治理的两阶段执行模式：模型负责提出结构化计划，base-agent 负责保存、审批
和确定性调度，DSH 继续负责每个 Agent 的模型循环、Tool 调用和 Workflow 子 Agent。

## 开发入口

业务开发者只在 Agent 上开启策略，不手写 JavaScript：

```python
from dsh_base_agent import Agent, PlanReact

agent = Agent(
    name="order-planner",
    prompt="你是订单助手。",
    tools=(query_order, update_order),
    plan_react=PlanReact(max_steps=8),
)
```

执行状态如下：

```text
QUEUED / planning
  -> planning Attempt
  -> submit_plan(summary, steps, requested_tools)
  -> WAITING / waiting_approval
  -> approve（精确匹配 plan_id + revision + sha256）
  -> QUEUED / executing
  -> execution Attempt
  -> Host JSON-RPC 直接启动固定 DSH Workflow
  -> SUCCEEDED / completed（成功）
     或 FAILED / executing（执行阶段失败）
```

`Run.phase` 表示整个 Run 当前所处阶段，因此成功后进入 `completed`；各 Attempt 仍保留
`planning` 或 `executing`，用于判断错误发生在哪个阶段。

拒绝会把 Plan 标为 `rejected`、Run 标为 `cancelled`，且不会创建 execution Attempt。

## 谁写 Workflow Script

模型不写可执行脚本，普通业务开发者也不写。模型只能调用临时的 `submit_plan` Tool，提交：

- `summary`；
- 有唯一 `step_id` 的步骤；
- `depends_on` 形成的无环图；
- `requested_tools` Tool allowlist。

base-agent 内部维护版本化 Workflow 模板，把上述数据放进 `args`。模板采用拓扑批次执行：同一批
无依赖步骤使用 DSH `parallel()`，下一批拿到依赖步骤的输出后再调用 `agent()`。Plan 持久化：

- Workflow ID 与版本；
- 固定脚本的 SHA-256；
- Workflow args 及 SHA-256；
- 整个 Plan 的语义 SHA-256。

审批和真正执行前都会重新校验这些摘要。这里的“确定性”指控制流、步骤依赖、Tool 暴露范围和
审批对象确定；子 Agent 的自然语言输出仍然是模型生成结果，并非字节级确定。

## Tool 与权限边界

- planning Attempt 只暴露原 Agent 的只读 Tool 和临时 `submit_plan`；
- execution Attempt 只暴露获批 Plan 的 `requested_tools`；
- 每次实际 Tool 调用仍经过原有 `Authorizer`、timeout、Audit、Artifact 和租户身份边界；
- DSH 模型可见的原生 `workflow` Tool保持关闭，执行由 Host 直接触发，不依赖模型是否选择调用；
- Workflow 子 Agent 由 DSH 创建和运行，base-agent 没有实现第二套 Agent loop。

当前审批者是已认证且拥有该 Run 的 Principal。接入生产审批中心时，还需要在入口增加独立的
审批角色/策略判定，不能把“Run 所有者”自动等同于“有权批准副作用操作”。

## DSH 兼容层

固定版本 `deepseek-harness-sdk==0.1.2a3` 暂无公开的直接 Workflow Python API。当前 Host Plugin
增加 `base-agent/workflow.execute` JSON-RPC 方法，并直接调用 DSH `workflowEngine.start()`。
它依赖该固定版本中运行时可访问的 `HarnessSdkJsonRpcServer.getOrCreateSession()`，升级 DSH 前
必须执行真实 smoke test；待 DSH 提供正式扩展路由后，应替换这条兼容缝隙。

## API

```text
GET  /v1/runs/{run_id}/plan
POST /v1/runs/{run_id}/approve
POST /v1/runs/{run_id}/reject
```

审批/拒绝请求体必须包含刚读取到的 `plan_id`、`plan_revision` 和 `plan_sha256`，可附带
`comment`。陈旧 revision、错误摘要、重复决定或非等待状态均返回 `409`。

此模式当前只支持独立 Run，不支持 Conversation。它实现的是两个 Attempt 之间可跨进程持久化
的审批点，不是让 DSH 的一个 live Turn 暂停后再恢复。原 Turn 的 durable resume 仍属于
[`dsh-resume-extension.md`](dsh-resume-extension.md) 的后续能力。

Starter 的真实调用步骤和一键脚本见 [`../starter/README.md`](../starter/README.md)。

# DSH Resume 扩展方案

状态：已选定后续方向，尚未实现。

## 1. 决策

`dsh-base-agent` 后续采用公司自有 DSH 插件和版本化 JSON-RPC 扩展，将 DSH 核心能力适配
给 Python 控制面。

目标调用链：

```text
dsh-base-agent ControlPlane
  -> Python DSH Extension Client
  -> 公司自有 JSON-RPC 扩展
  -> DSH Plugin
  -> ctx.agents.resume() / approval answerer / Agent handle
```

不修改 Python SDK 的私有实现，不读取 DSH JSONL 推断状态，也不在 base-agent 中重写 Agent
模型循环。

如果官方 `dsh-sdk-jsonrpc-server` 没有第三方方法注册点，则实现公司自有 server plugin 或
companion bridge，并保留官方 `initialize`、`session/prompt` 和 `shutdown` 的既有语义；不能
通过运行时 monkey patch 偷偷改变官方协议。

## 2. 必须区分的三种语义

### 2.1 Session 多轮继续

同一个存活的 Harness 和 Session ID 再次接收 `session/prompt`，会创建下一个 Turn。这是多轮
对话，不是暂停恢复。

### 2.2 Session 生命周期恢复

DSH 核心的 `ctx.agents.resume()` 可以加载持久化 Session，重新建立一个 live Agent。随后可
通过 `followup()` 开始新的 Turn。

它恢复的是 Session 历史和 Agent 实例，不恢复之前的 JavaScript/Python 调用栈、正在运行的
Tool 或已经丢失的 Promise。

### 2.3 Turn 内审批等待

`ctx.approval.request()` 可以让当前打开的 Turn 等待 answerer 的一次性决定。第一阶段扩展可
以此把审批请求交给 ControlPlane，并在原 DSH 进程仍存活时回答。

这不是 durable out-of-turn checkpoint。若 DSH 进程在等待期间退出，不能声称新 Worker 能
从原 Tool 调用位置继续。

## 3. 扩展协议草案

所有方法放在公司命名空间中，并通过 capability handshake 判断是否可用：

```text
dba/capabilities
dba/session/resume
dba/session/status
dba/session/cancel
dba/approval/answer

dba/approval.requested       server -> client notification/request
dba/session.event            server -> client notification
dba/session.status           server -> client notification
```

`dba/capabilities` 至少返回：

```json
{
  "protocolVersion": "1",
  "features": {
    "persistedSessionResume": true,
    "liveApproval": true,
    "durableTurnResume": false,
    "cancel": true
  }
}
```

Python 适配器必须依据 capability 工作，不能根据 DSH 或插件版本号猜测功能。

## 4. `/resume` 的业务映射

不能让一个 `/resume` 动词掩盖不同的底层操作。ControlPlane 必须先读取等待原因：

```text
WAITING + LIVE_APPROVAL
  -> dba/approval/answer
  -> 原 Turn 在仍存活的 Runtime 中继续

WAITING + SESSION_RELOAD_REQUIRED
  -> dba/session/resume
  -> 恢复持久化 Session
  -> 由明确的业务策略决定是否发送新的 followup Turn

WAITING + LOST_IN_FLIGHT_TOOL
  -> 不自动恢复
  -> Conversation/Run 保持 BLOCKED，进入人工对账
```

“恢复 Session 后发送 followup”必须在事件和审计中标为新 Turn，不能记录成恢复原 Turn。

## 5. 持久化和安全要求

ControlPlane 保存：

- `run_id`、`attempt_id`、`conversation_id`；
- `dsh_home_key`、`dsh_session_id`；
- `waiting_kind`、`approval_id` 和审批状态；
- 当前 Worker lease 和 fencing token；
- 扩展协议版本与 capability 快照；
- 审批人、审批时间和脱敏后的决定摘要。

普通客户端不能提交任意 `dsh_session_id`、`approval_id` 或 DSH Home 路径。ControlPlane 根据
Tenant、Principal、Run 和 Conversation 的所有权解析这些值。重复的审批回答必须幂等，过期
lease 或 fencing token 的 Worker 不得提交结果。

## 6. 分阶段实施

1. 实现插件骨架、`dba/capabilities` 和协议兼容测试；
2. 实现空闲持久化 Session 的 `dba/session/resume`，验证进程重建后的多轮上下文；
3. 实现 Session status、event 和 cancel 的规范化适配；
4. 实现 live approval 请求、ControlPlane `WAITING` 投影和幂等回答；
5. 做进程退出、网络断开、重复回答、lease 过期和多 Worker 竞争测试；
6. 只有 DSH 提供可验证的 durable approval/checkpoint 后，才启用跨进程原 Turn 恢复。

## 7. 第一阶段验收边界

- Python 客户端能通过 capability handshake 发现扩展；
- 已持久化的空闲 Session 可在 Runtime 重建后恢复，并用新 Turn 延续上下文；
- live approval 可以进入业务 `WAITING`，回答后原 Turn 继续；
- Runtime 在审批等待期间退出时失败闭合，不重放未知 Tool；
- Session、Run、审批和 Tenant/Principal 的映射经过权限校验并写入审计；
- 未安装扩展或版本不兼容时，现有 `/resume` 继续明确返回不支持。

## 8. 非目标

- 不恢复任意语言调用栈；
- 不保证进行中的 Tool 可迁移到另一 Worker；
- 不复制 DSH 的完整 Session 状态机；
- 不把 retry、followup 和 resume 视为同一个操作；
- 不因插件存在就宣称支持 durable Turn resume。

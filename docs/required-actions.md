# RequiredAction 统一等待协议

## 定位

`RequiredAction` 是 `dsh-base-agent` 的统一业务等待协议，不是对 DSH live Turn 调用栈的跨进程
暂停。当前支持模型发起的 Conversation 用户问询，以及 ControlPlane 发起的 Plan-ReAct 审批。

```text
用户消息 -> Conversation Run / Attempt 1 -> DSH Turn 1
                                      -> ask_user_question（Host-owned MCP Tool）
                                      -> RequiredAction(PENDING)
                                      -> Run/Attempt = WAITING，Turn 1 结束

用户回答 -> POST /v1/runs/{run_id}/resume
         -> RequiredAction = ANSWERED，Run = QUEUED
         -> Conversation Run / Attempt 2 -> 同一 dsh_session_id 的 DSH Turn 2
         -> RequiredAction = CONSUMED -> 完成原任务
```

因此三个业务标识保持不变：`run_id`、`conversation_id`、`dsh_session_id`；每次继续会创建新的
`attempt_id`。等待期间 Worker 不保留 lease，也不要求原进程存活。

Plan-ReAct 使用相同协议，但审批会直接消费 Action 并更新 Plan，不恢复 planning Attempt：

```text
planning Attempt -> Plan + RequiredAction(type=plan_approval, source=policy)
                 -> Run = WAITING / waiting_approval

POST /resume(decision=approve)
  -> RequiredAction = CONSUMED + Plan = APPROVED + Run = QUEUED / executing
  -> 新 execution Attempt 直接启动固定 Workflow
```

## 开发者入口

```python
agent = Agent(
    name="order-assistant",
    prompt=(
        "缺少完成任务所需的用户信息时，必须调用 ask_user_question；"
        "工具成功后立即结束当前 Turn。"
    ),
    tools=(query_order,),
    allow_user_input=True,
)
```

`ask_user_question` 由 base-agent 自动加入当前 Agent 的 MCP Gateway，业务代码不要再定义同名
Tool。当前固定的 DSH `sdk` Profile 没有装载需要 live UI provider 的原生
`tool-ask-user`；升级 DSH 时必须在兼容性测试中确认没有同名 Tool 冲突。问题内容由模型按上下文
生成，协议只约束可治理的展示结构：一次 1～3 个问题；每个问题可以是自由文本、单选或多选。

此 Agent 必须通过 `POST /v1/conversations/{conversation_id}/runs` 使用。独立
`POST /v1/runs` 没有可继续复用的业务 Conversation，因而会返回 `409`。

## 读取和回答

当 `GET /v1/runs/{run_id}` 返回 `run.status=waiting` 时，同一响应包含当前动作：

```json
{
  "run": {
    "run_id": "run_123",
    "status": "waiting",
    "revision": 3
  },
  "attempts": [],
  "required_action": {
    "action_id": "action_123",
    "type": "user_input",
    "status": "pending",
    "revision": 1,
    "payload": {
      "questions": [
        {
          "id": "shipping_method",
          "header": "配送方式",
          "question": "请选择配送方式",
          "options": [
            {"label": "普通配送", "description": "费用较低"},
            {"label": "加急配送", "description": "更快送达"}
          ],
          "multi_select": false
        }
      ]
    }
  }
}
```

回答仍复用稳定的 Run transition 接口：

```http
POST /v1/runs/run_123/resume
Content-Type: application/json

{
  "action_id": "action_123",
  "expected_revision": 3,
  "response": {
    "answers": [
      {"id": "shipping_method", "selected": ["普通配送"]}
    ]
  }
}
```

`expected_revision` 是调用方刚读取到的 **Run revision**。它与 `action_id` 一起防止重复回答、
旧页面覆盖和并发操作。自由文本问题使用 `custom`；选择题也允许 `custom`，但 `selected` 中的值
必须来自原问题的 `options[].label`。一次响应必须回答原动作中的全部问题。

Plan 审批同样使用 `/resume`，并同时校验 Run revision 与不可变 Plan 身份：

```json
{
  "action_id": "action_plan_123",
  "expected_revision": 3,
  "response": {
    "decision": "approve",
    "plan_id": "plan_123",
    "plan_revision": 1,
    "plan_sha256": "64位十六进制摘要",
    "comment": "同意执行"
  }
}
```

`decision` 只能是 `approve` 或 `reject`。原 `/approve`、`/reject` API 仍可使用，但只是把旧请求
转换成上述响应后调用同一个 RequiredAction 状态转换，不再维护第二套审批逻辑。

## 状态、持久化和治理

- Run：`RUNNING -> WAITING -> QUEUED -> RUNNING -> SUCCEEDED/FAILED/CANCELLED`；
- 用户输入 Action：`PENDING -> ANSWERED -> CONSUMED`；Plan 审批 Action 在决策事务中直接
  `PENDING -> CONSUMED`；取消时为 `CANCELLED`；
- SQLite 和 PostgreSQL 都原子保存 Run、Attempt 和 RequiredAction 的关键状态切换；
- PostgreSQL Worker 在进入 WAITING 后释放 Conversation lease，回答后由任意 Worker 重新领取；
- Tenant、Principal、Run、Conversation 和 Action 的所有权必须全部匹配；
- 问题正文进入 Event/数据库，因此不能要求模型把密码、Token 等 Secret 当作普通回答采集；
- `expires_at` 已持久化并在回答入口检查；过期回答会原子地把 Action 标记为 `EXPIRED`，并终止
  Run。无请求时主动清扫过期等待仍是后续治理能力。

普通澄清由模型触发。Plan 审批由 ControlPlane 在 planning 成功后生成。付款、发布、删除等
更通用的 Tool 级副作用审批仍应由策略生成新的治理型 Action，不能依赖模型自行决定；这一类
细粒度审批尚未开放。

## 与 DSH 原生 ask-user 的边界

DSH 原生 blocking ask-user 适合原 Runtime 仍存活的 live interaction。本实现选择“结束当前
Turn，回答后开启同 Session 的新 Turn”，以获得跨进程、跨 Worker 的持久等待。它不会恢复旧
Tool 的 Promise 或旧语言调用栈，也不声称支持被中断 Turn 的透明迁移。

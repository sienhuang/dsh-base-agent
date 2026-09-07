# 只读 Memory pre-step

当前 Memory 边界是单向的：`dsh-base-agent` 只负责检索和注入，不提供 Memory 的新增、修改或
删除 API。Memory 写入由独立下游服务消费 Kafka 中的 DSH Event 后完成。

```text
用户提交 Run
  -> ControlPlane 绑定可信 Tenant / Principal / Conversation / Run 身份
  -> DSH turn 1 / step 1
  -> dsh-base-agent pre-step Plugin
  -> 带临时 Bearer capability 的 loopback Context Gateway
  -> Authorizer.authorize_memory(...)
  -> Python Memory Provider
  -> 公司 Memory / RAG 服务
  -> 最多若干条、有总字节上限的背景数据进入本次模型请求
```

DSH 仍然负责 Agent Loop 和 Session 历史。base-agent 只增加一个 DSH `agent/pre-step` 插件，
不会实现第二套模型循环。

## 定义 Provider

```python
from dsh_base_agent import Agent, MemoryItem, MemorySearchRequest, memory_provider


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
        limit=3,
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


agent = Agent(
    name="order-assistant",
    prompt="你是订单助手。",
    memory_providers=(search_memory,),
    permissions=frozenset({"memory:read"}),
)
```

业务开发者通常只替换 Provider 函数体。`tenant_id`、`principal_id`、`conversation_id`、
`run_id`、`attempt_id` 和 `dsh_session_id` 由 ControlPlane 绑定，不能由模型参数或 HTTP 请求
正文覆盖。Provider 的配置会进入 Agent fingerprint；检索语义发生变化时必须递增
`version`，保证 API 和 Worker 使用同一个定义。

自定义 Authorizer 还必须实现 `authorize_memory()`，并至少验证当前 Principal 是否拥有
Provider 声明的权限。Starter 中有最小示例。

## 注入和大小边界

- 只在每个 DSH Turn 的 `step == 1` 检索一次；Tool continuation 不会重复检索和追加同一批
  Memory。同一 RunAttempt 内重复请求 Context Gateway 只返回缓存结果，不会再次访问 Provider。
- 单个 Provider 默认超时 5 秒、最多返回 3 条；声明时可在安全范围内调整。
- 每个 `MemoryItem.content` 最多 16,384 个字符；所有 Provider 最终进入模型的渲染文本总计
  最多 16 KiB。放不下的条目整条省略，不会把半条内容误当完整事实。
- Memory 以 `system-reminder` 包装的 `user/message` 背景数据进入 DSH，并明确标记为不可信
  数据而非指令。内容中的字面 `<` 会转义，避免关闭外层标记。
- Memory 服务异常、超时或权限拒绝采用 fail-open：记录失败事件和审计后，本次 Run 在没有该
  Provider 内容的情况下继续。它不会把其他租户的数据降级注入。

## Event、Audit 与 Kafka 写入端

PostgreSQL 的有界事件和审计不保存 Memory 正文，只记录：

- `memory.retrieval.started`；
- `memory.retrieval.completed`；
- Provider 名称和版本；
- 查询摘要、结果摘要、数量、状态和错误类型。

真正注入 DSH 的消息属于 DSH Session 历史，其 source 是：

```json
{
  "kind": "plugin",
  "plugin": "dsh-base-agent-memory-context"
}
```

现有 Kafka Publisher 会转发原始 DSH Notification，所以外部 Memory 写入服务能够看到业务
身份和 Session Event。写入服务必须忽略上述 plugin source，只从允许的真实用户消息、模型
结果或业务 Tool Event 中提取新 Memory；否则会把检索结果重新写回 Memory，形成反馈循环。

Kafka 是当前允许少量丢失的观测流。若 Memory 写入将来要求强一致或不可丢失，应另建可靠
消费和重放边界，不能把当前 best-effort Publisher 当事务提交日志。

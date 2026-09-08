# Payload 大小边界与 ArtifactStore 决策

状态：LocalArtifactStore 和 Tool 单次结果外部化已实现；其他统一限制尚未全部实现。

## 1. 决策

ArtifactStore 是目标生产形态中的必备能力，但不是当前最高优先级。它主要解决文件、完整
Tool 大结果和长期结果正文的保存与按需读取；它不应该成为建立数据边界的前置条件。

当前 P0 先实现统一的 `PayloadLimits`：即使没有 ArtifactStore，任何数据都不能无界进入
PostgreSQL、Kafka、进程内队列或 DSH/模型上下文。超限数据必须在进入下一个边界前被拒绝
或转换为有界摘要，不能依靠数据库、Broker 或模型自行兜底。

当前 LocalArtifactStore 已实现，因此 Tool Observation 超限时优先外部化。其他尚未接入
ArtifactStore 的边界采用以下语义：

- 需要保留完整语义的输入和最终输出：返回明确的超限错误；
- Tool Observation：完整 JSON 写 ArtifactStore，DSH 只接收明确标记为不完整的预览和引用；
- 仅用于诊断的 Event、Audit 和 Notification：保存有界字段、摘要、大小和 SHA-256；
- 不允许静默截断 JSON、文件或 Tool Result 后仍把 Run 标记为成功；
- Tool 应优先提供过滤、字段投影、分页和游标，避免先产生一个巨大结果。

ArtifactStore 实现后，超限正文可以写入对象存储，其他链路仍只传递预览、`artifact_id`、
`sha256`、大小和分页信息。ArtifactStore 不会取消任何现有大小限制。

## 2. 统一计量方式

所有限制都按序列化后的 UTF-8 字节数计算，不按 Python 字符数计算：

```python
len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
```

原因是中文字符、JSON 转义、Kafka 消息和 PostgreSQL 实际写入大小都不能用 `len(str)` 准确
表示。二进制数据不得内嵌为 Base64 放进 JSON；它只能由受控文件接口或未来的 ArtifactStore
承载。

## 3. 第一版默认预算

以下值是初始安全默认值，后续根据指标和真实负载调整。它们必须可以通过配置覆盖，但生产
环境不得配置为无限制。

| 边界 | 默认上限 | 超限行为 |
| --- | ---: | --- |
| 单个 HTTP 请求体 | 1 MiB | API 返回 HTTP 413 |
| Run `input` | 64 KiB | 拒绝创建 Run |
| Run `metadata` | 16 KiB | 拒绝创建 Run |
| 单次 Tool Observation | 64 KiB | 完整结果写 ArtifactStore，DSH 接收有界预览和引用 |
| 单个 Run 的 Tool Observation 累计 | 512 KiB | 停止继续向模型投递 Tool 数据 |
| Run 最终输出 | 256 KiB | Run 失败并记录有界错误；不得静默截断 |
| 单条 Event `data` | 32 KiB | 字段投影后以摘要和哈希代替超限部分 |
| 单条 Audit `data` | 16 KiB | 脱敏、字段投影并以摘要和哈希代替超限部分 |
| 单条 Kafka 消息 | 900,000 bytes | 入队前替换为超限摘要 |
| Kafka 进程内待发送总字节数 | 64 MiB | 丢弃 Notification 并增加计数器 |

`Tool Observation 累计` 是每个 Attempt 的预算。只限制单次结果仍可能让 64 次各 64 KiB 的
调用累计把数 MiB 内容送入模型上下文。

## 4. 四道强制边界

### 4.1 API 入口

反向代理和 FastAPI 都限制请求体；应用层再分别验证 `input`、`metadata` 和以后新增的 Context
字段。错误响应包含稳定错误码、实际字节数和允许上限，但不回显完整原文。

API 校验必须发生在创建 Run、写数据库和发布任何通知之前。Content-Length 只能用于快速拒绝，
不能代替读取后的实际字节校验。

### 4.2 ToolGateway 与模型上下文

`ToolGateway` 在把 Observation 返回给 DSH 之前执行以下步骤：

```text
Tool 原始返回值
-> 类型校验与敏感字段处理
-> 单次 UTF-8 JSON 大小检查
-> Attempt 累计 Observation 预算检查
-> 超限正文写 ArtifactStore
-> 有界结果或 Artifact 预览与引用
-> DSH / 模型上下文
```

单次 Tool 结果超限时已经可以外部化；未来的 Attempt 累计预算超限仍应阻止继续投递更多正文。
DSH 的 Token 上限和上下文压缩仍然保留，但它们是第二道保护，不能替代 ToolGateway 的入口
预算。Skill、静态 Context 和 Conversation 历史也应设置独立 Token/字节预算。

### 4.3 PostgreSQL

所有 Store 写入必须经过同一个大小策略，重点覆盖：

- `Run.input`、`Run.metadata` 和 `Run.output`；
- `RunAttempt.output` 和 `error`；
- `Event.data` 和 `Audit.data`；
- 当前 payload 快照中的重复字段。

应用层负责产生稳定领域错误；PostgreSQL migration 再使用 `octet_length(...)` 和 JSONB 大小
约束作为最后防线。数据库约束只防止漏检，不能作为正常控制流，因为约束失败发生得太晚，
也无法决定应该拒绝还是摘要化。

Event 与 Audit 默认只持久化 allowlist 字段。Tool Result 正文、模型原始消息、异常堆栈和任意
第三方响应不得直接塞进 JSONB；只保存必要摘要、`sha256`、原始大小、状态和关联 ID。

### 4.4 Kafka 与进程内队列

Kafka Notification 不是业务真相源，只发送有界观察数据。大小检查必须在放入进程内队列
之前完成；只在 Producer 发送前序列化是不够的，因为数千个巨大 Python 对象仍会耗尽 Worker
内存。

队列同时限制消息数量和累计字节数。单条超限消息在入队前转换为：

```json
{
  "oversized": true,
  "original_payload_size_bytes": 1234567,
  "sha256": "..."
}
```

如果队列字节预算已满，按当前 Kafka 直发的 best-effort 语义丢弃 Notification，并记录指标；
不得阻塞 DSH Run，也不得把完整消息回退写入 PostgreSQL。

## 5. 实施顺序

1. 新增集中式 `PayloadLimits` 配置、UTF-8 JSON 计量和稳定超限错误码；
2. 在 API 入口限制 Run 输入与 metadata；
3. 在已实现的 Tool 单次外部化之上增加 Attempt 累计 Observation 预算；
4. 在 ControlPlane/ControlStore 写入前限制 output、Event 和 Audit；
5. Kafka 在入队前完成投影和大小检查，并增加队列累计字节预算；
6. 增加 PostgreSQL CHECK 约束和边界集成测试；
7. 暴露 rejected、summarized、dropped、payload bytes 和队列 bytes 指标；
8. 在 LocalArtifactStore 基础上补充容量治理，并在后续实现 S3ArtifactStore。

## 6. 验收条件

- 构造超大 input、metadata、Tool Result、模型输出、Event 和 Notification 均有确定行为；
- 超限请求不会创建半成品 Run；
- 超限 Tool Result 正文不进入 DSH/模型上下文，只进入有界预览和引用；
- PostgreSQL 中不存在完整 Tool Result 或无界原始 Notification；
- Kafka 入队前后的对象均有字节上限，压测时 Worker 内存不会随消息大小无界增长；
- Event/Audit/Kafka 中的摘要可通过 `run_id`、`attempt_id`、大小和 SHA-256 关联排查；
- ArtifactStore 写入失败时 Tool 不会产生虚假的成功结果。

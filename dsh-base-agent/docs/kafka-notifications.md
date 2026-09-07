# Kafka Notification 直发

当前实现把 DSH SDK 的全部原始 `on_notification` 通过有界内存队列直接发布到 Kafka，不使用
PostgreSQL Outbox。Kafka 是实时观测流，不是业务状态真相源；Run、Attempt 和 Audit 仍以
ControlStore 为准，Agent 执行过程仍以 DSH Session/Event 为准。

## 配置

未设置 bootstrap servers 时 Publisher 是零开销的空实现：

```dotenv
DSH_BASE_AGENT_KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092
DSH_BASE_AGENT_KAFKA_TOPIC=dsh.notifications.v1
DSH_BASE_AGENT_KAFKA_CLIENT_ID=dsh-base-agent
DSH_BASE_AGENT_KAFKA_QUEUE_CAPACITY=4096
DSH_BASE_AGENT_KAFKA_REQUEST_TIMEOUT_MS=30000
DSH_BASE_AGENT_KAFKA_CLOSE_TIMEOUT_SECONDS=10
DSH_BASE_AGENT_KAFKA_MAX_MESSAGE_BYTES=900000
DSH_BASE_AGENT_KAFKA_REQUIRED=false
```

`REQUIRED=false` 时 Kafka 启动失败只记录日志，此次进程生命周期内的 Notification 会被丢弃，
不会让 Agent Run 失败。设置为 `true` 时，Kafka Producer 启动失败会阻止应用就绪。

Producer 使用 `acks=all` 和幂等发送。Kafka key 是
`<tenant_id>:<dsh_session_id>`，保证同一 Session 的消息进入同一分区。消息包含：

```json
{
  "schema_version": 1,
  "notification_id": "notification_...",
  "tenant_id": "demo-tenant",
  "principal_id": "demo-user",
  "agent_id": "iris-assistant-1",
  "conversation_id": "conversation_...",
  "run_id": "run_...",
  "attempt_id": "attempt_...",
  "dsh_session_id": "session_...",
  "method": "session.event",
  "payload": {},
  "occurred_at": "2026-09-03T12:00:00+00:00"
}
```

超过 `MAX_MESSAGE_BYTES` 的 payload 不会原样进入 Kafka，而会替换为原始字节数和 SHA-256
摘要，避免大 Tool Result 超过 broker 限制。

## 交付语义

- `on_notification` 只做线程安全的快速入队，不等待 Kafka 网络 I/O；
- 队列满时丢弃新消息并记录 warning；
- 单条发送失败会计数和记录日志，不传播到 DSH Run；
- 正常关闭时在 `CLOSE_TIMEOUT_SECONDS` 内排空队列；
- 进程崩溃、队列溢出或 Kafka 故障可能造成消息丢失；
- 消费者仍应按 `notification_id` 幂等处理可能的重复消息。

实现入口是
[`../src/dsh_base_agent/adapters/kafka/publisher.py`](../src/dsh_base_agent/adapters/kafka/publisher.py)。

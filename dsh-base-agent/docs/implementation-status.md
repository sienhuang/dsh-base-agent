# 当前实现状态

本文描述 `dsh-base-agent` v0.1 当前代码，不包含仅存在于设计稿中的能力。

## 已实现并有测试

- 不可变 `Agent` 定义和内容 fingerprint；
- `@tool` 参数 Schema、校验、同步/异步调用和 `ToolContext` 注入；
- Python Tool 到 loopback MCP Server；
- Tool 超时、readiness、权限元数据、事件和审计；
- DSH `sdk` Profile patch 编译和官方 Python SDK 适配；
- Run/RunAttempt 状态、幂等提交、CAS 和 SQLite 持久化；
- PostgreSQL `ControlStore`、显式关系列、连接池和带 checksum 的版本化迁移；
- 可选的 DSH 原始 Notification 直发 Kafka、有界队列、业务身份信封和大消息摘要；
- PostgreSQL Pull Queue、独立 API/Worker 进程、Worker Lease、Heartbeat 和 fencing token；
- Attempt `NOT_SENT -> DISPATCHING -> ACCEPTED -> SETTLED/UNKNOWN` 投递状态；
- fencing token 跨正常 release 单调递增、Heartbeat fail-closed 和短续租超时；
- Run 总执行期限、Conversation 空闲 Lease 释放和 DSH Runtime 取消时主动关闭 Harness；
- Worker 过期恢复时 Run、Attempt、Event 和 Audit 的 PostgreSQL 原子提交；
- LocalArtifactStore 不可变正文、逻辑 location、安全路径校验和流式读取；
- Tool Result 超过 64 KiB 时自动写本地 Artifact，DSH 只接收有界预览和引用；
- FastAPI Run 查询、事件、取消、重试、resume 占位、Artifact 查询和正文下载契约；
- Conversation 创建、查询和追加 Run API；
- 按 Conversation sequence 串行执行；存活 Worker 复用 Runtime 和 MCP Tool Gateway，替代
  Worker 使用相同持久化 DSH Home 和 Session ID 重建 Runtime；
- `.env` Runtime 配置；
- 启动时重新调度 `QUEUED` Run，并保守中断旧 `RUNNING` Attempt。

## 部分实现

- Skill：名称进入 Agent fingerprint，并控制 DSH `tool-skill` 是否启用；没有安装、解析、
  版本锁定和严格 allowlist；
- Context：可由应用组合进静态 Prompt，或通过受治理 Tool 获取动态信息；核心 SDK 没有
  `ContextProvider`；
- Authorization：有协议和安全默认值，尚未接公司 IAM/策略服务；
- Artifact：本地后端已接通 Tool 大结果；S3、容量配额、TTL、孤儿扫描和模型按 chunk 读取
  尚未实现；
- Payload 边界：Tool Observation 有 64 KiB 单次限制，Kafka 有单消息摘要；Run 输入、metadata、
  最终输出、Event/Audit、Attempt 累计 Observation 和 Kafka 入队前总字节预算尚未统一实现；
- Cancel：通过关闭当前 Runtime 完成，不等于 DSH 原生可恢复取消；
- Event：PostgreSQL 只保存有界投影；可直发原始 Notification 到 Kafka，但这是允许丢失的
  观测流，没有事务 Outbox 语义；
- Conversation 换手：替代 Worker 使用共享持久化 `DSH_HOME + dsh_session_id` 处理下一个
  Turn；被中断的原 Run 会失败闭合，不会自动重放。

## 仅有设计、尚未实现

- 更完整的 Worker 调度优先级、限流和跨机房治理；
- S3 Workspace Backend、本地物化和版本提交；
- DSH Plugin/Memory 的租户隔离、allowlist 与审计；
- WAITING/Approval 的完整恢复流程；
- 分布式配额和生产 SLA。

Resume 扩展的后续方向保持不变：通过公司自有 DSH 插件和版本化 JSON-RPC 扩展，把 Session
状态、原 Turn 对账和审批 answerer 适配给 Python 控制面。详细边界和实施阶段见
[`dsh-resume-extension.md`](dsh-resume-extension.md)。普通的 Worker 换手和“恢复被中断的原
Turn”是两种能力；当前版本只支持前者。

可复制业务骨架见 [`../starter/`](../starter/README.md)。

从当前本地纵向切片走向生产的分阶段计划见
[`production-roadmap.md`](production-roadmap.md)。

ArtifactStore 的优先级和 PostgreSQL、Kafka、进程内队列、DSH/模型上下文的统一大小边界见
[`payload-limits.md`](payload-limits.md)：ArtifactStore 是目标态必备能力，但当前先实现有界
数据面。

PostgreSQL 配置、Schema 约束和集成测试说明见 [`postgresql.md`](postgresql.md)。当前已完成
持久化底座及第一版 Worker lease/fencing 和 API/Worker 分离。原始 Notification 的 Kafka
直发见 [`kafka-notifications.md`](kafka-notifications.md)，它不使用 Outbox。

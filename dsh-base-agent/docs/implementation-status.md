# 当前实现状态

本文描述 `dsh-base-agent` v0.1 当前代码，不包含仅存在于设计稿中的能力。

## 已实现并有测试

- 不可变 `Agent` 定义和内容 fingerprint；
- `@tool` 参数 Schema、校验、同步/异步调用和 `ToolContext` 注入；
- Python Tool 到 loopback MCP Server；
- Tool 超时、readiness、权限元数据、事件和审计；
- DSH `sdk` Profile patch 编译和官方 Python SDK 适配；
- Run/RunAttempt 状态、幂等提交、CAS 和 SQLite 持久化；
- FastAPI Run 查询、事件、取消、重试、resume 占位和 Artifact 查询契约；
- Conversation 创建、查询和追加 Run API；
- 同一进程内按 Conversation sequence 串行执行，并复用一个 DSH Runtime、Session 和 MCP
  Tool Gateway；
- `.env` Runtime 配置；
- 启动时重新调度 `QUEUED` Run，并保守中断旧 `RUNNING` Attempt。

## 部分实现

- Skill：名称进入 Agent fingerprint，并控制 DSH `tool-skill` 是否启用；没有安装、解析、
  版本锁定和严格 allowlist；
- Context：可由应用组合进静态 Prompt，或通过受治理 Tool 获取动态信息；核心 SDK 没有
  `ContextProvider`；
- Authorization：有协议和安全默认值，尚未接公司 IAM/策略服务；
- Artifact：有模型和查询 API，尚未自动采集 DSH 输出；
- Cancel：通过关闭当前 Runtime 完成，不等于 DSH 原生可恢复取消；
- Event：只保存有界投影，没有事务 Outbox 和外部 Sink 投递。
- Conversation 恢复：当前进程内可以多轮；ControlPlane 重启后不能重新加载已经打开的 DSH
  Session，会将 Conversation 标为 `BLOCKED`。

## 仅有设计、尚未实现

- PostgreSQL Store、Worker queue、lease、heartbeat、fencing；
- S3 Workspace Backend、本地物化和版本提交；
- DSH Plugin/Memory 的租户隔离、allowlist 与审计；
- WAITING/Approval 的完整恢复流程；
- 分布式配额和生产 SLA。

Resume 的后续实现方向已经确定：通过公司自有 DSH 插件和版本化 JSON-RPC 扩展，把
`ctx.agents.resume()`、Session 状态和审批 answerer 适配给 Python 控制面。详细边界和实施
阶段见 [`dsh-resume-extension.md`](dsh-resume-extension.md)。该决策不代表当前版本已经具备
跨进程原 Turn 恢复能力。

可复制业务骨架见 [`../starter/`](../starter/README.md)。

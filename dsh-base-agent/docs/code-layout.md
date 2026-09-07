# 代码目录与依赖边界

源码目录按职责组织，而不是按一次请求的调用顺序堆放：

```text
src/dsh_base_agent/
├── __init__.py                 对业务开发者的稳定公共入口
├── artifacts/
│   ├── config.py               ArtifactStore 后端和本地根目录配置
│   └── store.py                ArtifactStore 协议和 LocalArtifactStore
├── sdk/
│   ├── agent.py                不可变 Agent 定义
│   └── tools.py                @tool、ToolContext 和 Tool 协议
├── control/
│   ├── auth.py                 Principal、Authorizer 和权限请求
│   ├── models.py               Conversation、Run、Attempt、Event、Audit
│   └── plane.py                业务状态编排；不实现模型循环
├── adapters/
│   ├── dsh/
│   │   ├── profile.py          Agent 定义到 DSH Profile patch
│   │   └── runtime.py          官方 DSH Python SDK 窄适配
│   ├── kafka/
│   │   └── publisher.py        原始 DSH Notification 直发 Kafka
│   └── mcp/
│       └── gateway.py          Python Tool 到 loopback MCP
├── store/
│   ├── config.py               SQLite/PostgreSQL 后端选择
│   ├── sqlite.py               本地单进程 ControlStore
│   ├── postgres.py             生产 PostgreSQL ControlStore
│   └── postgres_migrations.py  版本化 Schema migration
├── worker/
│   ├── service.py              PostgreSQL claim、lease、heartbeat 和执行
│   └── server.py               独立 Worker 启动入口
└── api/
    ├── app.py                  稳定业务 HTTP 契约
    └── server.py               环境驱动的 ASGI 启动入口
```

## 依赖方向

```text
API
 └── ControlPlane
      ├── SDK definitions
      ├── Store
      ├── ArtifactStore
      └── Adapters
           ├── DSH
           └── MCP

Worker
 └── ControlPlane(auto_execute=False)
      └── PostgreSQL claim -> fenced DSH execution
```

约束如下：

- `sdk` 不依赖 API、ControlPlane、Store 或 DSH，实现纯开发者定义层；
- `control/models.py` 和 `control/auth.py` 不依赖 DSH；
- `control/plane.py` 只编排业务状态并调用 Adapter，不包含模型循环；
- `adapters` 只翻译外部协议，不成为业务状态真相源；
- `api` 不暴露 DSH JSON-RPC 或内部 Event 结构；
- `store` 持久化 ControlPlane 记录，不读取 DSH 内部 JSONL；
- `artifacts` 保存大结果正文；ControlStore 只保存 ArtifactRecord 元数据；
- 业务应用优先从 `dsh_base_agent` 根包导入公共 SDK，不依赖内部目录。

## 公共入口

业务应用统一从 `dsh_base_agent` 根包导入稳定 API。重组前的
`dsh_base_agent.agent`、`auth`、`models`、`runtime`、`profile`、`gateway`、`server` 和
`tools` 顶层模块已经删除，不再保留两套目录。需要使用内部扩展点时，使用上图中的规范路径。

`ControlPlane` 目前仍在 `control/plane.py` 中持有进程内调度逻辑。这是当前实现状态，不是
最终 Worker 架构。下一阶段 API/Worker 分离时，应将任务领取、lease、fencing 和执行生命周期
放入独立 Worker 模块，而不是把现在的进程内执行代码机械拆成多个文件。

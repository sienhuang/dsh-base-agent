# Local ArtifactStore

状态：本地文件后端和 Tool 大结果自动外部化已实现；S3 后端尚未实现。

## 1. 数据边界

Artifact 分成两部分：

```text
完整正文 -> LocalArtifactStore 文件
元数据   -> ControlStore 的 ArtifactRecord（SQLite / PostgreSQL）
```

数据库只保存 `artifact_id`、Run/Attempt 关系、逻辑 location、媒体类型、大小和 SHA-256，不保存
完整 Tool Result。`location` 使用 `local:objects/...` 逻辑键，不暴露主机绝对路径。

默认正文目录：

```text
<workspace>/.dsh-base-agent/artifacts/
```

可以通过环境变量覆盖：

```dotenv
DSH_BASE_AGENT_ARTIFACT_BACKEND=local
DSH_BASE_AGENT_ARTIFACT_LOCAL_ROOT=/shared-pvc/dsh-base-agent/artifacts
DSH_BASE_AGENT_ARTIFACT_MAX_OBJECT_BYTES=67108864
```

相对路径按 ControlPlane workspace 解析。PostgreSQL API/Worker 分离模式下，API 和所有 Worker
必须挂载同一个 Artifact 目录；本地后端不会跨节点复制文件。没有共享文件系统时应保持在开发
或单 Worker 环境，后续改用 S3ArtifactStore。

## 2. Tool 大结果

Tool Result 先规范化为 JSON 并按 UTF-8 编码。未超过 64 KiB 时行为不变；超过时：

```text
完整 JSON -> LocalArtifactStore
ArtifactRecord -> SQLite/PostgreSQL
有界预览 + artifact_id + size + sha256 -> DSH/模型
```

Observation 明确包含 `observation_complete=false`，模型不能把 JSON 前缀预览当作完整结果。
`tool.completed` Event 和 Audit 只记录大小、哈希、externalized 和 artifact_id，不保存正文。

ArtifactStore 写入失败会让 Tool 调用失败，不会伪造成功。文件已写入但数据库元数据写入失败
时，ControlPlane 会尝试删除孤儿文件。

分页和字段投影仍然是 Tool 的首选设计。ArtifactStore 防止大结果进入数据库和模型上下文，
但 Tool 在返回之前已经在进程内构造的巨大对象仍会占用内存。

## 3. 查询和下载

列出某个 Run 的 Artifact：

```text
GET /v1/runs/{run_id}/artifacts
```

流式下载正文：

```text
GET /v1/runs/{run_id}/artifacts/{artifact_id}/content
```

两个接口都先校验 Run 的 Tenant 和 Principal 所有权。下载响应包含原媒体类型、
`Content-Length`、文件名和 `X-Artifact-SHA256`。

## 4. 本地后端保证和边界

- Artifact 正文使用只创建、不覆盖的不可变文件；
- 单个本地 Artifact 默认最大 64 MiB，超限时 Tool 调用失败；
- 文件名不参与磁盘路径计算，避免路径穿越；
- 打开逻辑 location 时校验解析路径仍在配置根目录内；
- 正文以 64 KiB chunk 流式返回，不整文件加载到 API 内存；
- 删除当前只用于数据库写失败后的补偿，不提供业务删除 API；
- 尚未实现容量配额、TTL、孤儿扫描、静态加密和恶意 Tool 的进程内内存隔离；
- S3 后端应继续实现相同 `ArtifactStore` 协议，不能改变业务 API 和 ArtifactRecord。

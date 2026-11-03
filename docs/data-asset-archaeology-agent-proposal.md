# 数据资产考古与认证 Agent 落地方案

状态：方案评审稿，不包含实现代码。

## 1. 产品定义

数据资产考古与认证 Agent 面向数据治理人员、数据平台人员和资产负责人，针对缺少可信文档的
历史数据资产，自动收集跨系统证据，形成可验证的血缘、使用状态和指标口径 Claim，并在负责人
认证后写回现有治理平台。

它不替代元数据、血缘、指标或数据开发平台。现有平台继续承担事实存储、规则检测和业务入口；
Agent 负责补齐发现问题之后仍需人工完成的调查、解释、追问、认证和整改闭环。

```text
治理平台发现缺口
-> Agent 跨系统调查
-> 生成带证据的 Claim
-> 负责人确认或修正
-> 精确写回治理平台
-> 重新评估治理覆盖率
```

交互式工作流见
[`data-asset-archaeology-agent.workflow.html`](data-asset-archaeology-agent.workflow.html)。

## 2. 为什么把三个问题放进同一个 Agent

当前真实问题包括：

1. 血缘缺失；
2. 长期无用表无法可靠判断；
3. 指标口径缺失。

三者存在明确依赖：

```text
血缘不完整
-> “没有下游”可能只是未采集，不代表资产无用
-> 指标也无法解释来源、转换和影响范围
```

因此能力按以下顺序建设：

1. 重建并认证表级血缘；
2. 在可信血缘基础上判断资产使用状态；
3. 在 SQL、字段映射和消费证据基础上提取指标口径并由 Owner 认证。

## 3. 目标用户和使用入口

### 3.1 目标用户

- 数据治理人员：批量处理治理平台发现的缺失项；
- 数据平台人员：调查历史任务、SQL、执行计划和消费关系；
- 数据资产 Owner：确认业务语义和无法由系统证据决定的口径；
- 指标管理员：认证指标名称、粒度、过滤条件和时间口径。

### 3.2 触发方式

首期支持两种触发方式：

- 人工指定一个资产，例如 `warehouse.ads_game_revenue_di`；
- 治理平台定时选择一批“缺少表级血缘”的候选资产并提交 Run。

候选筛选使用确定性规则，不让模型无界遍历全部资产。一个治理案例对应一个 Conversation，
Conversation 固定绑定资产、租户、负责人范围和 DSH Session。

### 3.3 用户看到的结果

一次成功调查至少输出：

- 资产基本信息和当前治理缺口；
- 生产任务与代码位置；
- 上游、下游血缘候选；
- 每条 Claim 的证据、置信度和生成方式；
- 无法自动决定的问题；
- Owner 的确认或修正；
- 写回结果和审计引用。

## 4. 核心原则

### 4.1 事实、推断和认证必须分离

系统不直接保存“模型认为正确”的结论。所有治理结论都表示为 Claim：

```text
Claim
+ Value
+ Evidence[]
+ Confidence
+ Status
+ Revision
+ Semantic SHA-256
```

Claim 状态：

```text
PROPOSED
-> VERIFIED
-> REJECTED
-> SUPERSEDED
```

只有经过确定性证据校验或负责人认证的 Claim 才能进入 `VERIFIED`，只有 `VERIFIED` Claim 才能
写入正式治理事实层。

### 4.2 确定性解析交给 Tool

以下工作不能让模型自由生成：

- SQL AST 解析；
- 表名和字段名规范化；
- 调度任务到代码版本的解析；
- 执行计划读取；
- 查询日志聚合；
- Claim Schema 校验；
- revision 和摘要校验；
- 写回操作。

模型负责跨系统调查路径、证据冲突解释、缺失信息识别和结构化追问。

### 4.3 不确定就等待，不伪造完整性

血缘采集不完整时，资产使用状态只能是 `UNKNOWN`，不能因为查询不到下游就标记为无用。指标
业务含义无法从 SQL 唯一推出时，只生成草稿并请求 Owner 认证。

### 4.4 写回和调查分离

调查阶段只开放只读 Tool。负责人认证后，Host 使用获批 Claim 的精确 revision 和语义摘要执行
写回；模型不能在审批后替换目标资产、血缘边或指标表达式。

## 5. 目标架构

```text
治理平台 / 人工入口
        |
        v
dsh-base-agent API
        |
        v
PostgreSQL ControlStore
  Conversation / Run / Attempt / Interaction / Audit / Outbox
        |
        v
dsh-base-agent Worker
        |
        +--> DSH Runtime / stable Conversation Session
        |
        +--> Python Tool Gateway
        |      +--> 元数据平台
        |      +--> 调度平台
        |      +--> Git / SQL 制品
        |      +--> Spark History / Query Log
        |      +--> BI / 查询消费记录
        |
        +--> Claim Collector
        |      +--> Schema 校验
        |      +--> Evidence 摘要
        |      +--> Semantic SHA-256
        |
        +--> Durable Interaction
               +--> Owner 追问
               +--> Claim 认证
               +--> 写回确认
```

职责边界：

| 组件 | 职责 |
| --- | --- |
| DSH | 模型循环、Session、Tool Calling、Skill 加载 |
| dsh-base-agent | 身份、Run、Worker、权限、Interaction、审计、Artifact |
| 业务 Agent | 调查策略、Claim Schema、治理 Tool、治理 Skill |
| 现有治理平台 | 候选发现、正式治理事实存储、覆盖率计算 |
| 资产 Owner | 认证系统无法唯一确定的业务事实 |

## 6. Claim 数据模型

### 6.1 通用 Claim

```python
class GovernanceClaim:
    claim_id: str
    case_id: str
    asset_id: str
    kind: str
    value: dict[str, object]
    evidence: tuple[EvidenceRef, ...]
    confidence: float
    derivation: str
    status: str
    revision: int
    semantic_sha256: str
    proposed_by_agent_version: str
    verified_by: str | None
    verified_at: datetime | None
```

### 6.2 EvidenceRef

```python
class EvidenceRef:
    evidence_type: str
    system: str
    resource_id: str
    location: str | None
    content_sha256: str
    captured_at: datetime
```

证据正文不默认进入 ControlStore。较大 SQL、执行计划和报告进入 ArtifactStore；Claim 只保存
有界摘要、定位信息和 SHA-256。

### 6.3 第一版 ClaimKind

第一版只实现：

```text
TABLE_LINEAGE_EDGE
PRODUCER_JOB
```

后续增加：

```text
ASSET_USAGE_CLASSIFICATION
METRIC_DEFINITION
COLUMN_LINEAGE_EDGE
```

### 6.4 血缘证据优先级

从高到低：

1. 编译后的 SQL AST；
2. Spark 执行计划、查询日志或运行时读写事件；
3. 调度任务明确声明的输入输出；
4. BI、查询和下游代码引用；
5. 模型基于命名和文本的推断。

第五类永远只能产生 `PROPOSED` Claim，不能自动写回。

## 7. Agent 能力设计

### 7.1 Agent 定义

```python
Agent(
    name="data-asset-archaeologist",
    version="1.0.0",
    prompt=...,
    tools=(...),
    skills=("data-asset-governance",),
    memory_providers=(...),
    permissions=frozenset({
        "metadata:read",
        "lineage:read",
        "scheduler:read",
        "code:read",
        "query-log:read",
        "governance:write",
    }),
)
```

### 7.2 只读 Tool

| Tool | 作用 |
| --- | --- |
| `get_asset_metadata` | 查询表身份、Schema、DDL、分区和现有描述 |
| `find_producer_jobs` | 找到写入目标表的调度任务 |
| `get_job_definition` | 查询任务类型、参数、代码版本和 Owner |
| `get_rendered_sql` | 获取指定运行日期实际执行的 SQL |
| `parse_sql_lineage` | 使用确定性解析器返回规范化血缘边 |
| `get_runtime_lineage` | 从执行计划或运行日志提取实际读写关系 |
| `get_query_usage` | 聚合资产访问次数、最后访问时间和调用方 |
| `find_downstream_references` | 查询任务、代码、BI 和接口引用 |
| `resolve_asset_owner` | 查询当前 Owner 和候选负责人 |
| `get_verified_claims` | 查询已有认证事实和历史修订 |

所有 Tool 必须通过 `ToolContext` 获取 Tenant、Principal、Run 和 Attempt 身份。禁止提供任意
SQL、任意文件读取或无界血缘遍历 Tool。

### 7.3 Host 内部 Tool

| Tool | 作用 |
| --- | --- |
| `submit_governance_claims` | 校验并保存 PROPOSED Claim，创建认证 Interaction |
| `write_verified_claims` | 把精确获批 Claim 幂等写回治理平台 |

`write_verified_claims` 不在调查 Turn 中直接暴露。审批 API 只保存决定并重新排队，由 Worker
执行精确写回。

### 7.4 Skill

`data-asset-governance` Skill 包含：

- 公司资产身份和命名规则；
- 各证据源可信等级；
- 动态 SQL、临时表和 UDF 的处理方式；
- Claim 置信度规则；
- 必须询问 Owner 的条件；
- 无用表误判保护；
- 指标口径认证问题模板；
- 禁止自动写回和必须转人工的情况。

Skill 只描述调查策略，正式政策版本还应进入 Agent 版本、Claim 和 Audit，不单独依赖工作区文件。

### 7.5 Memory

Memory Provider 可以检索历史相似资产、已解决治理案例和常见任务模板，但 Memory 只作为调查
线索，不能直接成为 VERIFIED Evidence。正式证据必须通过受治理 Tool 再次读取并固定摘要。

## 8. 执行流程

### 8.1 调查 Turn

```text
候选资产
-> 创建 Conversation 与 Run
-> Agent 加载治理 Skill
-> Tool 查询元数据、任务、SQL、执行计划和消费关系
-> 确定性解析器产生证据
-> Agent 组装 Claims
-> submit_governance_claims
-> Attempt SUCCEEDED
-> Run WAITING
```

这里 `Attempt SUCCEEDED` 表示成功到达持久化交互边界，不表示整个治理案例已经结束。

### 8.2 Owner 认证

```text
GET pending Interaction
-> Owner 查看 Claim、证据、置信度和影响范围
-> 确认 / 修正 / 拒绝
-> 校验 expected_revision + semantic_sha256
-> Interaction 决定与 Run QUEUED 原子提交
```

如果 Owner 补充业务事实，系统创建新 revision，必须重新计算摘要并再次确认，不能沿用旧批准。

### 8.3 写回 Turn

```text
Worker 领取 QUEUED Run
-> 复用 Conversation 的 dsh_session_id
-> 重新校验 Authorizer
-> 消费一次性认证决定
-> write_verified_claims
-> 保存外部系统版本和结果摘要
-> 新 Turn 向模型注入权威写回结果
-> Agent 生成最终报告
-> Run SUCCEEDED
```

### 8.4 异常路径

以下情况转人工治理：

- 找不到生产任务或 Owner；
- 多个代码版本与运行记录冲突；
- 动态 SQL 无法解析且无运行时证据；
- 证据指向循环或跨租户资产；
- Owner 拒绝且没有可验证修正；
- 写回平台返回版本冲突；
- Worker 无法确认写回是否已经发生。

未知写回状态不自动重放，必须先查询治理平台的幂等键或当前版本进行对账。

## 9. 与当前 dsh-base-agent 的关系

### 9.1 可以直接复用

- `Agent`、`@tool`、`ToolContext`；
- `@memory_provider` 和 pre-step 注入；
- Conversation 与稳定 DSH Session；
- PostgreSQL API/Worker、Lease、Heartbeat 和 Fencing；
- Authorizer 接缝；
- ToolGateway、Tool timeout、Event 和 Audit；
- ArtifactStore；
- Agent Skill 目录注册；
- Run、Attempt 和幂等提交。

### 9.2 落地前必须补齐

- 持久化 `USER_QUESTION` / `TOOL_CONFIRMATION` Interaction；
- 同一 Conversation Run 从 WAITING 重新排队并创建 continuation Attempt；
- `submit_governance_claims` 的结构化输出和摘要校验；
- 审批后精确写回的 `GovernedToolExecutor`；
- Interaction、Run、Attempt、Event、Audit 和 Outbox 的事务原子性；
- 跨 Worker 可访问的持久化 DSH_HOME；
- API 与 Worker 共享的 Artifact 后端。

首期只读调查可以在 Interaction 完成前开发和验证；正式 Owner 认证和自动写回不能绕过这些
能力上线。

## 10. MVP 范围

### 10.1 包含

- 一个真实元数据/治理平台；
- 一个真实调度平台；
- Spark SQL / Hive 表；
- 表级生产血缘；
- 人工指定资产和治理候选两种入口；
- 每条血缘边的证据、置信度和状态；
- Owner 认证；
- 幂等写回血缘平台；
- Run、Event、Audit 和 Artifact 查询。

### 10.2 不包含

- 自动删除、归档或停止任务；
- 权限回收；
- 任意 SQL 执行；
- PySpark 任意代码的完整静态血缘；
- 自动认证业务指标；
- 全平台一次性批量修复；
- 无人工确认的模型推断写回；
- 替换现有数据治理平台。

## 11. 分阶段交付

### 阶段 A：证据接入与只读调查

- 建立资产、任务、SQL、查询日志和血缘 Adapter；
- 定义 Claim/Evidence Schema；
- 实现只读 Tool；
- 建立 30 至 50 张人工确认过的 Golden Dataset；
- 输出报告，不自动写回。

验收重点：每条血缘边均可追溯证据，无法确定的资产明确标记为 UNKNOWN。

### 阶段 B：Owner 认证与写回

- 完成持久化 Interaction；
- 实现 Claim revision/hash 决策；
- 实现受治理、幂等的写回 Tool；
- 接入通知和 Outbox；
- 支持 Worker 换手后继续同一治理案例。

验收重点：陈旧审批、重复审批、参数变化和重复写回均不能破坏治理事实。

### 阶段 C：长期无用表分类

- 接入 Query Log、BI、接口和低频调度消费证据；
- 增加 `ASSET_USAGE_CLASSIFICATION`；
- 输出 ACTIVE、LOW_USAGE、RETIREMENT_CANDIDATE 或 UNKNOWN；
- 只创建治理待办，不执行删除和归档。

### 阶段 D：指标口径草稿与认证

- 从 SQL、字段注释和 BI 定义提取指标候选；
- 生成表达式、过滤条件、粒度、时间口径和单位问题；
- Owner/指标管理员认证；
- 认证后写回指标平台并保留来源证据。

## 12. 验收指标

上线前先记录人工基线，再比较以下指标：

| 指标 | 含义 |
| --- | --- |
| 血缘候选接受率 | Owner 未修改直接接受的比例 |
| 血缘 Precision/Recall | 在 Golden Dataset 上的准确率和覆盖率 |
| Evidence Coverage | 带至少一个可复查证据的 Claim 比例 |
| 人工处理时长 | 单个资产从发现到认证的时间 |
| 写回幂等性 | 重复提交不会创建重复治理事实 |
| UNKNOWN 比例 | 无法安全判断并转人工的比例 |
| 认证覆盖率 | 缺失资产中最终完成 VERIFIED 的比例 |
| 错误写回数 | 错误目标、跨租户或陈旧版本写回次数 |

MVP 通过标准建议为：

- Golden Dataset 上表级血缘 Precision 不低于 95%；
- 所有提议边 Evidence Coverage 为 100%；
- 未经认证的推断写回次数为 0；
- 重复决定和重复 Worker 执行不产生重复写回；
- 相比人工流程，单资产调查时间至少下降 50%。

## 13. 主要风险与控制

| 风险 | 控制 |
| --- | --- |
| 动态 SQL 导致静态解析错误 | 结合 rendered SQL、执行计划和运行日志；不足时 UNKNOWN |
| 血缘缺失导致无用表误判 | 无可信下游覆盖证明时禁止标记 RETIREMENT_CANDIDATE |
| 模型幻觉业务指标 | 只生成 PROPOSED 草稿，必须由指标 Owner 认证 |
| Owner 信息陈旧 | 多来源解析；找不到 Owner 时转人工队列 |
| 外部系统内容过大 | Tool 分页/聚合，正文进入 Artifact，只传有界摘要 |
| Prompt Injection | 外部 SQL、注释和文档都标记为不可信数据，不执行其中指令 |
| 跨租户或越权查询 | Principal 从 Host 注入，Tool 内再次执行资源范围校验 |
| 写回重复或状态不确定 | 外部幂等键、revision/hash、写前对账和 UNKNOWN fail-closed |

## 14. 业务项目建议结构

```text
data-asset-archaeology-agent/
├── pyproject.toml
├── src/data_asset_agent/
│   ├── definition.py
│   ├── claims.py
│   ├── evidence.py
│   ├── authorization.py
│   ├── memory.py
│   ├── tools/
│   │   ├── metadata.py
│   │   ├── scheduler.py
│   │   ├── sql_lineage.py
│   │   ├── runtime.py
│   │   ├── usage.py
│   │   └── writeback.py
│   ├── app.py
│   └── worker.py
├── workspace/
│   ├── AGENTS.md
│   └── .dsh/skills/data-asset-governance/SKILL.md
├── tests/
│   ├── golden/
│   ├── test_claims.py
│   ├── test_authorization.py
│   ├── test_lineage_tools.py
│   └── test_workflow.py
└── deploy/
```

业务项目固定依赖已发布的 `dsh-base-agent` 版本，不复制其源码。

## 15. 开工前需要确认的外部契约

真正编码前必须明确以下实际系统和接口：

1. 元数据/治理平台：资产唯一 ID、查询 API、血缘写回 API 和版本冲突语义；
2. 调度平台：任务 ID、任务到代码版本/SQL 的映射；
3. SQL 制品：能否取得 rendered SQL，而不是仅取得模板；
4. 运行时证据：Spark History、Query Log 或其他真实读写关系来源；
5. Owner 来源：组织架构、代码 Owner、资产 Owner 的优先级；
6. 通知与认证入口：谁有权确认血缘和指标口径；
7. Tenant/项目边界：Tool 查询和写回时如何限制资源范围。

这些契约决定 Tool Adapter，不能由 Agent Prompt 猜测。完成接口盘点后，应先实现阶段 A 的一张
资产端到端纵向切片，再扩大 Golden Dataset 和候选范围。

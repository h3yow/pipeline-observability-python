# 数据管道运行观测：状态流转、重试与告警规则

本文说明本服务如何在**并发上报、重复上报与失败重试**下，让同一管道实例的状态与告警
结论保持一致、不回退、不重复计数，并且每个结论都有可区分、可解释的原因。

## 1. 领域模型

| 概念 | 说明 |
| --- | --- |
| `pipeline_run_id` | 一次管道运行实例的唯一标识。同一实例的所有事件共享一把锁。 |
| `event_id` | 单条事件的唯一标识，用于**幂等去重**（消息队列 at-least-once 场景）。 |
| `version` | 单调递增的事件版本号，用于判定**乱序/迟到**上报。 |
| `attempt` | 运行尝试序号，从 1 开始；每次重试 +1。 |
| `occurred_at` | 事件发生时间戳；成功事件的时间戳即“最近成功时间”。 |

代码入口：

- `pipeline_obs/models.py`：枚举与数据结构。
- `pipeline_obs/store.py`：线程安全的状态机、去重、乱序/冲突拒绝、一致快照、失新鲜巡检。
- `pipeline_obs/pipeline.py`：管道级聚合注册表（健康汇总、连续失败数、活动告警镜像）。
- `pipeline_obs/retry.py`：可重试判定、指数退避、次数上限。
- `pipeline_obs/alerting.py`：失败告警收敛、失新鲜判定与失新鲜告警收敛。
- `pipeline_obs/service.py`：门面服务。
- `main.py`：FastAPI 接口（接收事件、查询实例/管道汇总、触发巡检）。

## 2. 状态流转

状态集合：

```
PENDING  RUNNING  SUCCEEDED  FAILED_RETRYABLE  FAILED_FATAL
```

合法流转：

```
PENDING ──STARTED──▶ RUNNING
RUNNING ──SUCCEEDED──────────────▶ SUCCEEDED
RUNNING ──FAILED(RETRYABLE)──────▶ FAILED_RETRYABLE
RUNNING ──FAILED(FATAL)──────────▶ FAILED_FATAL（终态）
FAILED_RETRYABLE ──STARTED(下一 attempt)──▶ RUNNING
FAILED_RETRYABLE ──SUCCEEDED──────────────▶ SUCCEEDED
SUCCEEDED ──STARTED(新一轮)──▶ RUNNING
RUNNING ──STARTED(更高 version)──▶ RUNNING   # 重复启动/运行心跳
```

补充约束：

- **`FAILED_FATAL` 是严格终态**：之后的任何事件都被拒绝（`FATAL_ALREADY_TERMINAL`）。
- **可重试失败达到次数上限后**：状态保留在 `FAILED_RETRYABLE` 且
  `retry_exhausted=True`，后续任何事件被拒绝（`RETRY_EXHAUSTED`）。
  这一设计刻意与 `FAILED_FATAL` 区分，便于下游分辨两种终止原因：
  “错误本身不可重试” vs “错误可重试但机会已用完”。

### 2.1 事件接收的判定顺序（互斥，原因可区分）

在实例锁内按下列顺序判定，返回 `APPLIED / DUPLICATE / REJECTED`：

| 顺序 | 条件 | 结论 | 原因（`RejectReason`） |
| --- | --- | --- | --- |
| 1 | `event_id` 已处理过 | `DUPLICATE` | —（幂等忽略，状态不变、不计数） |
| 2 | `version <= applied_version` | `REJECTED` | `STALE_VERSION`（乱序/迟到） |
| 3 | 当前为 `FAILED_FATAL` | `REJECTED` | `FATAL_ALREADY_TERMINAL` |
| 4 | 已耗尽重试次数 | `REJECTED` | `RETRY_EXHAUSTED` |
| 5 | 同 attempt 已有不同结果 | `REJECTED` | `CONFLICTING_OUTCOME` |
| 6 | 状态机不允许该流转 | `REJECTED` | `ILLEGAL_TRANSITION` |
| 7 | 其余 | `APPLIED` | 推进状态/计数/告警，RETRYABLE 时同临界区写重试调度 |

要点：

- **状态绝不回退**：旧版本事件无法覆盖新版本已推进的状态。
- **计数绝不重复**：重复事件在计数之前即被幂等拦截。
- **冲突可区分**：同一 attempt 先报成功后报失败（或反之）被拒绝，避免结论二义；
  它与“乱序”“终态后上报”“重试耗尽”使用不同原因码。

## 3. 重试规则（退避 + 上限）

`RetryPolicy`（默认 `max_attempts=3, base_delay=1s, multiplier=2, max_delay=60s`）：

- 退避延迟：`delay(attempt) = min(base * multiplier^(attempt-1), max_delay)`，
  序列为 1s、2s、4s……并封顶；可选 jitter 防止重试风暴。
- **FATAL**：`should_retry=False`，立即终止，状态进入 `FAILED_FATAL`。
- **RETRYABLE 且未达上限**：`should_retry=True`，写入 `next_retry_at = now + delay`。
- **RETRYABLE 已达上限**：`should_retry=False`，`retry_exhausted=True`，
  不再调度，后续上报被 `RETRY_EXHAUSTED` 拒绝。

重试决策与状态推进在**同一把实例锁的同一临界区**内完成，因此并发查询不会看到
“已经失败但还没有重试计划”的中间态，也不会发生两次重试调度的丢失更新。

## 4. 告警收敛规则

- 进入 `FAILED_RETRYABLE / FAILED_FATAL` 且当前无活动告警：触发一次告警，
  `raised_count += 1`。
- 活动告警未恢复期间再次失败（含多次重试失败）：**收敛，不重复计数**。
- 收到 `SUCCEEDED`：异常恢复，关闭活动告警（记录恢复版本）；之后再失败算新异常，
  才允许再次计数。
- `PENDING / RUNNING` 不产生失败告警。

汇总快照 `PipelineStatus` 在锁内一次性构造，字段彼此自洽；并发读取者永远只能看到
某个已提交版本的完整结论，看不到半成品字段。

## 5. 管道级健康汇总

线上看板按管道维度看：`ObservabilityService.pipeline_health(pipeline_id)` 返回
`PipelineHealth`，回答四个问题——这个管道现在健康吗、多久没成功了、最近一次为什么挂、
当前有没有活动告警。

| 字段 | 口径 |
| --- | --- |
| `run_count` | 当前归属该管道的运行实例数。 |
| `latest_run_id` / `latest_run_state` / `latest_event_at` | 管道下“最近一次有事件”的运行及其状态/时间（同时间按 run_id 字典序决胜，保证确定性）。 |
| `last_success_at` | 管道**最近一次成功时间**（成功水位 W，随成功事件单调抬升）。 |
| `consecutive_failures` | 最近一次成功之后，当前仍停留在失败终态的**不同运行**个数（见下）。 |
| `active_failure_alert_count` / `active_staleness_alert_count` | 两类活动告警数量（性质不同，分开计数，可同时 > 0）。 |
| `has_failure_alert` / `has_staleness_alert` | 是否存在对应类别活动告警。 |
| `staleness_threshold_seconds` / `checked_at` | 最近一次巡检使用的阈值与巡检时刻。 |

活动告警的**完整明细**（哪条运行、哪一类、依据、首次产生时间、版本）通过
`service.active_alerts(pipeline_id, kind=...)` 惰性展开。健康快照主体只保留计数，
明细按需取，避免告警很多时健康查询本身变成线性扫描。

### 5.1 “连续失败数”的精确口径

- 统计对象是**运行（run）**，不是失败事件：同一运行的多次重试失败只算一个运行，
  失败索引以 `run_id` 为键、只保留该运行最近一次失败终态时间。
- 只统计失败时间**晚于管道最近成功时间**、且该运行当前仍处于
  `FAILED_RETRYABLE / FAILED_FATAL` 的运行。
- 某运行成功即恢复：从失败索引移除；若这次成功刷新了管道成功水位 W，
  则一切失败时间 `<= W` 的运行都视为“已被更新的成功覆盖”，一并剔出连续失败数。
- `FAILED_FATAL` 是严格终态，它的失败告警在该运行被人工处置前会一直挂着；
  但它是否计入“连续失败数”仍取决于其失败时间与成功水位的关系
  （可能“告警仍在、但已不计入连续失败”，两者是独立维度）。

### 5.2 为什么查询不随历史线性变慢（含保留策略口径）

聚合值在事件写入时**增量物化**，健康查询是 O(1) 的标量/计数读取：

- “最近事件时间 / 最近成功时间”是单调标量，写入时 O(1) 更新。
- “连续失败数”用「失败索引 dict + 失败时间最小堆」增量维护：每次失败终态入堆一次，
  成功抬升水位时把堆顶 `<= W` 的项批量出堆并调整计数；每个堆项至多入堆/出堆一次，
  写入摊还 O(log n)，查询 O(1)。
- 管道下多个运行的更新在「先实例锁、后管道锁」的固定加锁顺序下串行进入同一管道临界区，
  因此并发上报不丢更新；健康快照在管道锁内一次性构造，读不到半截中间态。

**保留策略（内存实现的对外口径）**：当前为**进程内、内存态**实现，保留自进程启动以来
观测到的全部运行；内存占用与“历史不同运行数 + 失败终态上报次数”同阶（成功且不再失败的
运行只留 run_id 与标量贡献，不堆积堆项）。它不做磁盘持久化，重启即清空，适合作为单实例
观测侧/sidecar。跨进程横向扩展、超长期留存或 TTL 淘汰属于存储后端的职责，接入持久化后端
时应以同样的物化聚合表 + 水位/堆（或定时聚合任务）保持查询口径与复杂度不变。

## 6. 失新鲜监控（自动告警 + 确定性巡检）

`ObservabilityService` 构造参数 `fresh_within_seconds` 是全局默认新鲜阈值（秒，可配置，
默认 300s）；单次巡检也可用 `fresh_within_seconds=` 临时覆盖。

判定基线（`StalenessVerdict` 完整返回，明细里能看到“用的哪个基线、差了多少、阈值多少、
结论是什么”）：

1. 有最近成功时间 → 基线 `LAST_SUCCESS = last_success_at`；
2. 从未成功过 → 回退基线 `FIRST_REPORT = started_at`（第一次上报时间）；
3. 连事件都没有 → 基线 `NONE`，默认视为新鲜，不产生告警。

判定式：`now - baseline > 阈值` 即失新鲜（严格大于，等于阈值仍算新鲜）。

巡检入口（**按需确定性触发，不依赖真实 sleep**）：

- `service.check_staleness(run_id, now=..., fresh_within_seconds=...)`：巡检单实例；
- `service.sweep_staleness(run_id=... | pipeline_id=... | 全量, now=..., ...)`：
  按范围巡检，返回 `{run_id: StalenessVerdict}`；
- 时刻 `now` 取自注入时钟（测试可注入可控时钟），也可在调用时显式传入。
- **时间契约**：巡检时刻 `now` 必须单调不减、且不早于已应用事件的时间——这与真实周期性
  调度一致。系统不接受“拿更早的旧时刻对已推进到更晚基线的实例再判一次”这类乱序巡检：
  那样会把一个已被更新成功覆盖的实例重新判成失新鲜。需要重放历史请用独立的服务实例。

收敛规则（`StalenessAlertState`，与失败告警完全分开）：

- 新鲜 → 失新鲜：开启**新的一段**失新鲜，`episode_id += 1`、`raised_count += 1`、
  记录 `raised_at`；
- 持续失新鲜期间反复巡检：**只告警一次、只计一次**，仅刷新判定依据；
- 恢复新鲜（重新成功，或在新基线下回到窗口内）：关闭活动告警、记录恢复时刻；
  再次失新鲜才算新段落，才允许再计一次。
- 收到成功事件时会在**同一写入临界区**立即关闭该运行的失新鲜告警（不必等下轮巡检），
  保证成功后查询不会短暂看到“已成功但仍挂着失新鲜”的中间态。

## 7. 两类告警的关系：分开、可并存、各自恢复

- 失败告警（`AlertKind.FAILURE`）由状态机失败终态驱动；失新鲜告警
  （`AlertKind.STALENESS`）由时间基线驱动。两者性质不同，分别有独立的活动标志、计数、
  首次产生时间与判定依据，因此**可能同时存在**（例如：早就失败了，且距上次成功也已超阈值）。
- 实例明细 `PipelineStatus` 新增：
  `pipeline_id`、`staleness_alert_active`、`staleness_alert_reason`、
  `staleness_raised_count`、`active_alert_kinds`（当前活动告警类别元组，如
  `("FAILURE","STALENESS")`）。旧字段 `alert_active / alert_reason` 仍只表示失败告警，
  含义不变。
- 管道汇总用两组计数/标志与 `active_alerts()` 明细区分两类；成功恢复会同时关闭该运行的
  两类活动告警，但二者也能各自独立出现/消失（如运行中先失新鲜、后失败；或只失新鲜未失败）。

## 8. 并发安全模型

- 每个 `pipeline_run_id` 一把实例锁（惰性创建，注册表另用短锁保护），同一实例的更新完全
  串行化；每个 `pipeline_id` 再一把管道锁保护聚合。
- 加锁顺序固定为「实例锁 → 管道锁」（单向，不存在反向获取），因此无死锁；不同实例/管道
  互不阻塞。
- 事件接收、计数、重试调度、失败告警、失新鲜成功恢复、管道聚合更新都在同一实例临界区及
  其嵌套的管道临界区完成；实例快照与管道快照分别在对应锁内一次性构造，均为不可变值对象。

## 9. HTTP 接口（main.py）

| 方法/路径 | 说明 |
| --- | --- |
| `POST /events` | 上报事件（新增**可选**字段 `pipeline_id`）。成功 200；重复幂等 200；冲突/拒绝 409 带 `reason`。 |
| `GET /runs/{run_id}` | 查询单个实例汇总；旧字段全部保留，末尾追加管道与失新鲜新字段；不存在 404。 |
| `GET /runs` | 列出全部实例 ID。 |
| `GET /pipelines` | 列出全部管道 ID。 |
| `GET /pipelines/{pipeline_id}/health` | 管道级健康汇总（含两类告警计数与明细）；不存在 404。 |
| `POST /staleness/sweep` | 按需触发失新鲜巡检；body 可选 `pipeline_id / run_id / fresh_within_seconds / now`。 |

### 9.1 新旧兼容范围

- **不传 `pipeline_id` 的旧上报完全不受影响**：该实例默认“自成一管道”，其管道 ID 即
  `pipeline_run_id`，旧的单实例行为、状态机、计数、重试、失败告警口径保持不变。
- 一旦显式指定 `pipeline_id`，同一运行后续事件不得改报其他管道（否则以
  `ILLEGAL_TRANSITION` 拒绝），防止归属漂移导致聚合污染。
- `PipelineEvent` 新增 `pipeline_id`（带默认值）、`PipelineStatus` 新增字段（全部带默认值、
  追加在末尾），旧的位置构造与字段读取不被打断。
- 旧方法签名保持兼容：`staleness_report(run_id, fresh_within_seconds=None)`、
  `is_stale(...)`、`status/all_status/ingest` 均不变；阈值缺省回落到服务级默认值。

## 10. 验证方法

```bash
uv sync
uv run pytest -s
```

测试运行时通过 `timeline` logger 打印**事件时间线、逐条判定依据、失新鲜判定（基线/时间差/
阈值/结论）与管道健康汇总/活动告警明细**。

- `tests/test_retry.py`：退避序列与封顶、达到上限停止、FATAL 不重试且原因可区分。
- `tests/test_state_machine.py`：乱序拒绝、重复幂等不计数、同 attempt 冲突结果、FATAL 终态后拒绝。
- `tests/test_alerting.py`：同一失败异常恢复前只告警一次、恢复后新异常重新计数、时效判定依据。
- `tests/test_concurrency.py`：并发重复上报、重试链重复投递、乱序批次、1 写 8 读快照不变量。
- `tests/test_pipeline_health.py`（新增）：
  - 40 个运行、每事件重复 4 次并发提交 + 8 个持续读取者：管道汇总不丢更新、无中间态，
    连续失败数与活动失败告警数恰为运行数；
  - 失新鲜“进入 → 持续多次巡检 → 成功恢复 → 再次失新鲜”：第一段只告警/计数一次，
    新段落才再计一次，基线在 FIRST_REPORT 与 LAST_SUCCESS 间正确切换；
  - 失败告警与失新鲜告警并存、各自独立恢复，`active_alert_kinds` 与管道两类计数正确；
  - 跨多运行混合成功/失败 + 成功水位抬升：连续失败数精确；
  - 历史从 500 增至 5000 个运行：健康查询耗时基本恒定（实测比值约 1，物化聚合非全量扫描）。

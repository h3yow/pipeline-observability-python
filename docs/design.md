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
- `pipeline_obs/pipelines.py`：管道级增量聚合、健康快照与保留策略。
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

### 4.1 失败告警（AlertKind.FAILURE）

- 进入 `FAILED_RETRYABLE / FAILED_FATAL` 且当前无活动失败告警：触发一次告警，
  `raised_count += 1`。
- 活动告警未恢复期间再次失败（含多次重试失败）：**收敛，不重复计数**。
- 收到 `SUCCEEDED`：失败恢复，关闭活动失败告警（记录恢复版本）；之后再失败算新异常，
  才允许再次计数。
- `PENDING / RUNNING` 不产生失败告警。

### 4.2 失新鲜告警（AlertKind.STALENESS）——与失败告警完全独立

两类告警性质不同：失败告警针对“运行失败”这一事实，失新鲜告警针对
“长时间没有成功”这一时效事实。它们各自维护收敛状态，**可能同时存在，
也各自独立恢复，互不开关对方**。

- **基线选择**：优先实例的 `last_success_at`（最近成功时间，`basis=LAST_SUCCESS`）；
  从未成功过的实例回退到 `started_at`（第一次上报时间，`basis=FIRST_REPORT`）。
- **判定规则**：`age = now - 基线时间`，严格 `age > fresh_within_seconds` 才判失新鲜；
  恰好等于阈值仍属新鲜。
- **收敛规则**：同一段失新鲜期间（进入失新鲜 → 恢复新鲜）只告警一次、只计一次；
  巡检重复执行只刷新“持续中”的依据描述，`raised_count` 不增长。
- **恢复**：
  - 成功事件在 ingest 时即时恢复新鲜并关闭失新鲜告警（无需等待下一次巡检）；
  - 巡检判定回到窗口内（例如阈值被调大）也只关闭失新鲜告警，不影响失败告警。
- 每次判定都产出结构化 `StalenessAssessment`：基线类型、基线时间、巡检时间、
  时间差、阈值、结论文本，汇总和明细里都能看到“用的哪个基线、差了多少、阈值多少、
  结论是什么”。

### 4.3 巡检如何确定性触发

- 阈值可配置：构造服务时给 `default_fresh_within_seconds`，或在每次
  `run_staleness_sweep(fresh_within_seconds=...)` 调用时覆盖。
- 巡检时间 `now` 由调用方注入（`run_staleness_sweep(now=...)`），缺省取注入时钟；
  **测试中直接给定点时间即可，不靠真实 sleep 等待**。
- 可巡检全部实例，也可只巡检某个 `pipeline_id` 当前保留的实例；
  返回巡检后处于失新鲜告警中的实例快照列表。

实例快照 `PipelineStatus` 在实例锁内一次性构造，管道快照在管道锁内取标量口径、
释放后逐实例取告警明细，字段彼此自洽；并发读取者永远只能看到已提交状态的完整结论，
看不到半成品字段。

## 5. 管道级健康汇总口径

事件新增可选字段 `pipeline_id`。携带该字段的事件按管道归属聚合（归属以首个携带
`pipeline_id` 的事件为准，first-wins），不携带的实例行为与旧版本完全一致。

`ObservabilityService.pipeline_health(pipeline_id)` 返回 `PipelineHealth`：

| 字段 | 口径 |
| --- | --- |
| `latest_run_id / latest_state / latest_event_at` | 最近一次运行：被应用事件中 `(occurred_at, version)` 最大者所属实例及其状态 |
| `last_success_at` | 该管道下任意实例成功事件时间的最大值（成功高水位，单调不减） |
| `consecutive_failures` | 距最近一次成功之后连续失败的**运行实例数**：对每个保留实例取其最近一次终态结果，终态为 FAILURE 且失败时间严格晚于最近成功时间即计入；同一实例的多次重试失败只算 1 个。从未成功过时按全部失败实例计 |
| `failure_alert_active` | 当前保留实例中是否存在活动失败告警 |
| `staleness_alert_active` | 当前保留实例中是否存在活动失新鲜告警 |
| `active_alerts` | 活动告警明细（`ActiveAlert`）：告警类别、所属实例、判定依据；失新鲜告警带基线/时间差/阈值，失败告警带错误分类与错误信息（“最近一次为什么挂”） |
| `run_count / retained_run_limit / evicted_runs` | 保留实例数 / 每管道保留上限 / 累计淘汰数 |

**并发自洽**：实例事件仍在各自实例锁内串行；事件应用的同一临界区内按
`实例锁 -> 管道锁` 的单向锁序增量刷新聚合（终态镜像 + 成功高水位），
因此同一管道多个运行并发上报、并发查询时：

- 不会丢更新（聚合更新与事件应用在同一临界区）；
- 不会露出半截中间态（标量口径在管道锁内原子读取，告警明细逐实例读一致快照）；
- 跨实例乱序处理不影响口径（连续失败数依据各实例终态时间与成功高水位在锁内重算，
  与事件到达顺序无关）。

锁的获取方向严格为 `实例锁 -> 管道锁 -> 注册表短锁`，管道锁内绝不反向获取实例锁，
因此不存在 AB-BA 死锁；`tests/test_pipeline_health.py` 中有混合并发压测守护。

### 5.1 保留策略（查询不随历史线性变慢）

内存态实现不可能全量保留，策略为：**每个管道只保留最近 `retained_run_limit`
个有事件的实例**（默认 1000，可在构造 `ObservabilityService(retained_run_limit=...)`
或 `PipelineStore(...)` 时配置）。

- 超出上限时，在事件应用的同一临界区内淘汰 `latest_event_at` 最小（最旧）的实例，
  同时间按 run_id 定序，淘汰过程确定；
- 健康查询只遍历当前保留成员，时间复杂度为 **O(保留量)**，与历史实例总量无关；
- 被淘汰实例的实例明细与其活动告警不再出现在管道汇总中；
  `last_success_at`（成功高水位）不回滚；`consecutive_failures` 按当前保留成员重算，
  `evicted_runs` 对外披露累计淘汰数量，口径透明可核对。
- 无 `pipeline_id` 的实例不参与任何管道的保留策略。

## 6. 时效性判定（单实例只读视图，兼容旧接口）

`ObservabilityService.staleness_report(run_id, fresh_within_seconds)` 保持原有签名与
`(是否失新鲜, 判定依据文本)` 返回值不变，内部改为复用结构化 `assess_staleness`：

1. 优先以 `last_success_at` 为基线：`now - last_success_at > 阈值` 即失新鲜。
2. 从未成功过的实例回退到 `started_at`（首次事件时间）。
3. 无任何事件的实例默认视为新鲜；未知 run_id 判定为失新鲜。

注意：该只读接口**不改变**失新鲜告警状态；告警的产生/收敛只由
`run_staleness_sweep / evaluate_staleness`（巡检）与成功事件（即时恢复）驱动。

## 7. 并发安全模型

- 每个 `pipeline_run_id` 一把 `threading.Lock`（惰性创建，注册表另用短锁保护），
  同一实例的更新完全串行化，天然消除丢失更新。
- 每个 `pipeline_id` 一把管道锁；锁序严格为
  `实例锁 -> 管道锁 -> 注册表短锁`，管道锁内不反向获取实例锁，杜绝 AB-BA 死锁。
- 不同实例、不同管道互不阻塞。
- 事件接收、计数、重试调度、两类告警评估、管道聚合刷新均在同一临界区内完成；
  快照为不可变值对象。

## 8. HTTP 接口（main.py）

| 方法/路径 | 说明 |
| --- | --- |
| `POST /events` | 上报事件。成功 200；重复上报幂等 200（`status=DUPLICATE`）；冲突/拒绝 409 并带 `reason`。 |
| `GET /runs/{run_id}` | 查询单个实例的一致汇总快照；不存在 404。原有字段全部保留，新增 `pipeline_id / last_error_class / last_error_message / staleness_alert_active / staleness_alert_reason / staleness_assessment`。 |
| `GET /runs` | 列出全部实例 ID。 |
| `GET /pipelines` | 列出全部已知管道 ID。 |
| `GET /pipelines/{pipeline_id}/health` | 管道级健康汇总；管道不存在 404。 |
| `POST /staleness-sweep` | 确定性失新鲜巡检；查询参数 `pipeline_id`（可选）、`fresh_within_seconds`（可选，缺省用服务默认值）、`at`（可选，判定时间 now）。返回失新鲜实例及其结构化判定依据。 |

事件字段：`pipeline_run_id, event_id, event_type(STARTED|SUCCEEDED|FAILED),
version, attempt, occurred_at(可选,默认当前时间), error_class(RETRYABLE|FATAL),
error_message, pipeline_id(可选)`。

### 8.1 新旧兼容范围

- `PipelineEvent` 仅在末尾新增可选字段 `pipeline_id`；不带该字段的旧上报调用方式不变。
- `PipelineStatus` 仅在末尾新增带默认值的字段；原有字段名、类型与构造方式不变，
  旧的 `status() / all_status() / staleness_report() / is_stale()` 签名与返回不变。
- 失败告警字段仍为 `alert_active / alert_reason`，语义不变；失新鲜使用独立的
  `staleness_alert_active / staleness_alert_reason`。
- 管道归属、管道汇总、失新鲜巡检均为**新增能力**，不接入就不改变任何旧行为。

## 9. 验证方法

```bash
uv sync
uv run pytest -s
```

测试覆盖（运行时通过 `timeline` logger 打印**事件时间线与逐条判定依据**）：

- `tests/test_retry.py`：退避序列与封顶、达到上限停止、FATAL 不重试且原因可区分。
- `tests/test_state_machine.py`：乱序拒绝（`STALE_VERSION`）、重复幂等不计数、
  同 attempt 冲突结果（`CONFLICTING_OUTCOME`）、FATAL 终态后拒绝。
- `tests/test_alerting.py`：同一异常恢复前只告警一次、恢复后新异常重新计数、
  基于最近成功时间的时效判定。
- `tests/test_concurrency.py`：
  - 32 线程并发重复上报同一事件：恰好 1 次 APPLIED，其余全 DUPLICATE，计数为 1；
  - 重试链路上每个事件 6 线程重复并发投递：最终失败计数恰为上限、
    `retry_exhausted=True`、第 4 次尝试被 `RETRY_EXHAUSTED` 拒绝；
  - 完全乱序并发批次：迟到事件得 `STALE_VERSION`，状态不回退；
  - 1 写者 + 8 读者持续并发：快照不变量违例数为 0（无中间态）。
- `tests/test_pipeline_health.py`（新增）：
  - 最近运行/最近成功/连续失败计数/失败告警明细的基本口径；
  - 12 个运行两阶段并发上报 + 8 读者：汇总与串行口径一致、无丢更新、无中间态；
  - 同一失败终态 24 线程重复投递：连续失败只计 1；
  - 200 个历史实例、保留上限 50：只保留最近 50、累计淘汰 150、查询不随历史变慢；
  - 20 运行上报 × 6 健康查询读者 × 2 巡检者混合并发：无死锁、无不变量违例。
- `tests/test_staleness.py`（新增）：
  - 失新鲜进入 → 持续巡检 → 成功即时恢复 → 再次失新鲜：每段恰好告警一次；
  - 从未成功时以第一次上报时间为基线；阈值边界（age==阈值仍新鲜）；
  - 失败告警与失新鲜告警并存、不同实例各自独立恢复；
  - 阈值调大只恢复失新鲜告警而失败告警保持（两类独立性）；
  - 巡检 now/阈值/管道范围均可确定性按调用指定。
- `tests/test_http_api.py`（新增）：管道健康、404、确定性巡检端点与旧端点兼容字段。

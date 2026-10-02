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

### 4.1 失败原因字段：展示范围与清空时机

失败上报携带的 `error_class`（`RETRYABLE/FATAL`）与 `error_message`（原文）会在以下
三处一致展示“最近一次失败为什么挂”：

| 位置 | 字段 |
| --- | --- |
| 运行明细 `PipelineStatus` | `last_error_class`、`last_error_message`、`failure_termination` |
| 管道汇总 `PipelineHealth` | `latest_failure_run_id/at`、`latest_failure_error_class/message`、`latest_failure_termination` |
| 活动失败告警 `ActiveAlertView` | `error_class`、`error_message`、`termination` |

`failure_termination` / `termination` 用可枚举的两类终止原因把“为什么不再重试”讲清楚：

- `FATAL`：**错误本身不可重试**（状态 `FAILED_FATAL`），立即终止、不消耗后续退避；
- `RETRY_EXHAUSTED`：**错误可重试，但重试机会已用尽**（状态仍为 `FAILED_RETRYABLE`
  且 `retry_exhausted=True`）；
- `NOT_TERMINATED`：尚未终止（运行中/等待重试），或已恢复成功。

失败告警依据文本据此区分（例如“错误本身不可重试（FATAL），立即终止”与
“错误可重试（RETRYABLE），但重试机会已用尽，停止重试”），**FATAL 不再被误导性地
写成“重试耗尽”**。

**清空时机（不残留旧错误）**：

- 从未失败：三个错误字段均为空、`failure_termination=NOT_TERMINATED`；
- 运行一旦 `SUCCEEDED` 恢复成功：该运行的错误分类/信息立即清空，
  `failure_termination` 回到 `NOT_TERMINATED`；
- 管道汇总的“最近一次失败”只统计**当前仍处于失败终态**的运行（`failure_infos`
  随运行成功即删除），因此当全部运行恢复后，管道级失败原因整体清空为 `None`；
- 运行被保留策略淘汰后，其失败原因随记录与聚合贡献一并移除。

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
| `latest_failure_run_id` / `latest_failure_at` | 当前仍失败的运行中，最近一次失败的运行与时间；全部恢复后为 `None`。 |
| `latest_failure_error_class` / `latest_failure_error_message` | 该最近一次失败的错误分类（`RETRYABLE/FATAL`）与错误信息原文。 |
| `latest_failure_termination` | 该失败的终止原因：`FATAL`（不可重试）或 `RETRY_EXHAUSTED`（机会用尽）。 |

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

**保留与淘汰策略（明确上限，内存不再无限增长）**：

- **上限**：`max_retained_runs`（`PipelineStore` / `ObservabilityService` 构造参数，
  默认 `10000`）是保留的**不同运行实例数硬上限**。
- **淘汰规则**：新运行要占名额时，若保留数已达上限，先在同一 `registry_lock` 临界区内
  淘汰一个“最近事件时间最旧”的运行（同时间按 `run_id` 字典序决胜，确定性），再插入
  新运行。淘汰候选用最小堆 + 每 run 最新标记惰性维护；因此**任何时刻保留数都不越上限**，
  读者不会看到 `cap+1`，也不会看到“记录已删、聚合仍计入”的中间态。
- **原子摘除**：淘汰在「registry_lock → 被淘汰实例锁 → 管道锁」的嵌套临界区内完成
  “标记墓碑 → 剔除管道聚合贡献（run_ids/失败索引/失败原因/两类告警镜像/最近事件重算）
  → 摘除记录与锁”。写入路径的锁序为「实例锁 → 管道锁」，不含 registry，两路径无环、
  无死锁。
- **累计淘汰量与口径披露**：`service.retention_status()` 返回
  `RetentionStatus(max_runs, retained_runs, evicted_runs, policy="drop_oldest")`；
  HTTP 暴露为 `GET /retention`。`evicted_runs` 为进程内累计淘汰的不同运行数（只增不减）。
  需要强一致地校验“保留数 == 明细条数”时，用 `service.retained_runs_view()`，
  它在同一把 registry 锁内一次性返回（保留口径, 保留运行 ID 集合）。
- **被淘汰运行的查询结果一致明确**：`service.lookup_run(run_id)` 返回 `RunLookup`：
  - `FOUND`：运行仍在保留窗口，`status` 为锁内完整快照；
  - `EVICTED`：曾被观测但已淘汰，`status=None` 且 `detail` 说明口径；
  - `UNKNOWN`：从未观测。
  三者都不抛错、不返回残缺数据。旧接口 `service.status(run_id)` 对“不存在/已淘汰”
  统一返回 `None`（语义不变）；HTTP `GET /runs/{id}` 分别返回 200 / 410(EVICTED) /
  404(UNKNOWN)。
- **被淘汰运行不再复活**：淘汰时实例锁转为**持久墓碑**（`run_id → 墓碑锁`），
  此后任何同 ID 事件（含迟到、重复、更高 version）一律 `REJECTED/RUN_EVICTED`，
  也不会再进入任何管道聚合。墓碑只保留 run_id 与一枚锁，不含事件集合、堆项与聚合，
  体量远小于完整运行历史，是幂等去重/防还魂的必要代价；完整运行状态、失败明细与
  聚合贡献仍严格受 `max_retained_runs` 约束。另有一个定长（最近 `max_runs` 个）的
  EVICTED ID 登记表，仅用于把查询结论细分为 EVICTED/UNKNOWN，更早的历史 ID 查询可
  归为 UNKNOWN，但其再上报仍被墓碑拒绝。
- **查询成本**：健康汇总为 O(1) 物化标量/计数读取；“最近一次失败原因”用失败信息
  最大堆惰性清理，堆项随保留上界定长。因此即便累计历史运行不断增长（持续淘汰），
  只要保留集合大小不变，健康查询耗时就不随累计历史线性变慢（见
  `test_health_query_cost_does_not_grow_as_history_evicts`）。

本实现为**进程内、内存态**，不做磁盘持久化、重启即清空，适合作为单实例观测侧/sidecar。
跨进程横向扩展或超长期留存属于持久化存储后端的职责；接入后端时应以同样的物化聚合表 +
水位/堆 + 显式保留窗口（或 TTL/分区淘汰）保持查询口径与复杂度不变。

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
  调度一致。若传入早于 `last_checked_at` 的旧时刻（乱序巡检），系统**原样返回上一次
  判定结论**，不刷新任何字段、不开关告警、不增减计数：既不会把已恢复实例重新判成失新鲜，
  也不会让旧的“失新鲜”结论回潮。需要重放历史请用独立的服务实例。

收敛规则（`StalenessAlertState`，与失败告警完全分开）：

- 新鲜 → 失新鲜：开启**新的一段**失新鲜，`episode_id += 1`、`raised_count += 1`、
  记录 `raised_at`；
- 持续失新鲜期间反复巡检：**只告警一次、只计一次**，仅刷新判定依据；
- 恢复新鲜（重新成功，或在新基线下回到窗口内）：关闭活动告警、记录恢复时刻；
  再次失新鲜才算新段落，才允许再计一次。

### 6.1 成功恢复后明细口径（活动状态与判定依据一起更新）

修复前只把 `staleness_alert_active` 置 false、却保留上一版“失新鲜”判定依据，导致同一份
明细自相矛盾（告警已关、结论仍停在失新鲜）。现在收到成功事件时，在**同一写入临界区**内
通过 `recover_staleness_on_success(...)` 原子完成：

1. 关闭活动失新鲜告警、清空 `raised_at`，并记录恢复时刻
   `staleness_last_recovered_at`（= 成功事件时间）；
2. **同时**把 `last_verdict` 刷新为“恢复/新鲜”结论：基线切换为
   `LAST_SUCCESS(成功时间)`、`age=0`、在窗口内，依据文本明确写出**恢复时刻**与
   “本段失新鲜告警同步关闭”。因此恢复后任何查询读到的都是新鲜结论；
3. 管道侧失新鲜告警镜像在同一临界区撤销，健康汇总不会露出已恢复的告警；
4. 同一段失新鲜结束后再巡检（含恢复同刻、更晚时刻）不重复报警、不重复计数；
   传入更早旧时刻的乱序巡检按 §6 时间契约原样返回新鲜结论，旧结论永不回潮。

## 7. 两类告警的关系：分开、可并存、各自恢复

- 失败告警（`AlertKind.FAILURE`）由状态机失败终态驱动；失新鲜告警
  （`AlertKind.STALENESS`）由时间基线驱动。两者性质不同，分别有独立的活动标志、计数、
  首次产生时间与判定依据，因此**可能同时存在**（例如：早就失败了，且距上次成功也已超阈值）。
- 实例明细 `PipelineStatus` 的扩展字段：
  `pipeline_id`、`staleness_alert_active`、`staleness_alert_reason`、
  `staleness_raised_count`、`active_alert_kinds`（当前活动告警类别元组，如
  `("FAILURE","STALENESS")`）、`staleness_last_recovered_at`（最近失新鲜恢复时刻），
  以及失败原因三件套 `last_error_class`、`last_error_message`、`failure_termination`。
  旧字段 `alert_active / alert_reason` 仍只表示失败告警，含义不变。
- 管道汇总用两组计数/标志与 `active_alerts()` 明细区分两类；失败告警明细
  （`ActiveAlertView`）额外带 `error_class / error_message / termination`；
  成功恢复会同时关闭该运行的两类活动告警，但二者也能各自独立出现/消失
  （如运行中先失新鲜、后失败；或只失新鲜未失败）。

## 8. 并发安全模型

- 每个 `pipeline_run_id` 一把实例锁（带墓碑位），同一实例的更新完全串行化；
  每个 `pipeline_id` 再一把管道锁保护聚合；另有一把 `registry_lock` 保护
  运行记录表、淘汰堆与墓碑/登记结构。
- 加锁顺序：写入路径为「实例锁 → 管道锁」；淘汰路径为
  「registry_lock → 被淘汰实例锁 → 管道锁」（淘汰发生在新运行占名额之前，
  淘汰线程尚未持有新运行的实例锁）。registry 只在淘汰/创建路径出现且位于最外层，
  两条路径不成环，因此无死锁；不同实例/管道互不阻塞。
- 事件接收、计数、重试调度、失败告警、失新鲜成功恢复、管道聚合更新都在同一实例临界区及
  其嵌套的管道临界区完成；**淘汰的“记录摘除 + 管道聚合剔除”也在同一嵌套临界区完成**，
  读者看不到“记录已删但聚合仍计入”或保留数越界的中间态。实例快照与管道快照分别在对应
  锁内一次性构造，均为不可变值对象。
- 只有真正 `APPLIED` 的写入才会登记淘汰候选；`DUPLICATE / REJECTED` 不入堆、不触发淘汰，
  避免重复/乱序事件造成多计淘汰量。

## 9. HTTP 接口（main.py）

| 方法/路径 | 说明 |
| --- | --- |
| `POST /events` | 上报事件（可选字段 `pipeline_id / error_class / error_message`）。成功 200；重复幂等 200；冲突/拒绝 409 带 `reason`（含新增 `RUN_EVICTED`）。 |
| `GET /runs/{run_id}` | 查询单个实例汇总；旧字段全部保留，末尾追加失新鲜恢复时刻与失败原因三件套；保留中 200，**已淘汰 410（body `outcome=EVICTED` 并带保留口径），从不知晓 404**。 |
| `GET /runs` | 列出全部在保留窗口内的实例 ID。 |
| `GET /pipelines` | 列出全部管道 ID。 |
| `GET /pipelines/{pipeline_id}/health` | 管道级健康汇总（含两类告警计数、明细及最近一次失败原因）；管道下无有效运行 404。 |
| `POST /staleness/sweep` | 按需触发失新鲜巡检；body 可选 `pipeline_id / run_id / fresh_within_seconds / now`。 |
| `GET /retention` | 历史保留口径：`max_runs / retained_runs / evicted_runs / policy`。 |

### 9.1 新旧兼容范围

- **不传 `pipeline_id` 的旧上报完全不受影响**：该实例默认“自成一管道”，其管道 ID 即
  `pipeline_run_id`，旧的单实例行为、状态机、计数、重试、失败告警口径保持不变。
- 一旦显式指定 `pipeline_id`，同一运行后续事件不得改报其他管道（否则以
  `ILLEGAL_TRANSITION` 拒绝），防止归属漂移导致聚合污染。
- `PipelineEvent` 新增 `pipeline_id`（带默认值）、`PipelineStatus` 新增字段（全部带默认值、
  追加在末尾），旧的位置构造与字段读取不被打断。
- 旧方法签名保持兼容：`staleness_report(run_id, fresh_within_seconds=None)`、
  `is_stale(...)`、`status/all_status/ingest` 均不变；阈值缺省回落到服务级默认值。
- **本轮新增能力均为“可选入参 + 末尾追加字段 + 新增枚举值/新接口”，不破坏旧调用**：
  - `ObservabilityService(..., max_retained_runs=None)`、`PipelineStore(max_retained_runs=...)`
    为新增可选参数；缺省取默认上限 `10000`，旧的无参构造行为不变（仅多了保留上限）。
  - `PipelineStatus / PipelineHealth / ActiveAlertView` 新增字段全部带默认值并追加在末尾，
    旧的位置构造与字段读取不被打断；旧字段含义不变。
  - 失败告警评估 `evaluate_alert(...)` 新增可选关键字 `error_class/error_message`；
    失新鲜恢复新增独立函数 `recover_staleness_on_success(...)`，旧调用不报错。
  - 新增 `RejectReason.RUN_EVICTED`、`LookupOutcome`、`RunLookup`、`RetentionStatus`、
    `FailureTermination` 与 `service.lookup_run() / retention_status() /
    retained_runs_view()`；旧代码只需把未知的拒绝原因按“已拒绝”通用处理。
  - `status(run_id)` 仍对不存在/已淘汰返回 `None`；需要区分二者时改用 `lookup_run()`。

## 10. 验证方法

```bash
uv sync
uv run pytest -s                 # 打印事件时间线与判定依据
uv run pytest -q                 # 静默模式快速回归
uv run ruff check pipeline_obs main.py tests && uv run ruff format pipeline_obs main.py tests
uv run fastapi dev main.py       # 启动后可访问 /retention、/runs/{id}、/pipelines/{id}/health
```

测试全部使用**注入时钟/显式 `now`** 确定性驱动，不依赖真实 sleep；并通过 `timeline`
logger 打印**事件时间线、逐条判定依据、失败错误分类/信息/终止原因、失新鲜判定
（基线/时间差/阈值/结论/恢复时刻）、保留口径与管道健康/活动告警明细**。

- `tests/test_retry.py`：退避序列与封顶、达到上限停止、FATAL 不重试且原因可区分。
- `tests/test_state_machine.py`：乱序拒绝、重复幂等不计数、同 attempt 冲突结果、FATAL 终态后拒绝。
- `tests/test_alerting.py`：同一失败异常恢复前只告警一次、恢复后新异常重新计数、时效判定依据。
- `tests/test_concurrency.py`：并发重复上报、重试链重复投递、乱序批次、1 写 8 读快照不变量。
- `tests/test_pipeline_health.py`：多运行并发汇总、失新鲜段落收敛、两类告警并存独立恢复、
  成功水位精确剔除、历史 500→5000 健康查询耗时恒定。
- `tests/test_recovery_consistency.py`（**本轮新增**）：失新鲜成功恢复后
  “活动标志 + 判定依据 + 恢复时刻”一致为新鲜；恢复后再巡检/乱序旧时刻巡检不重复报警、
  旧结论不回潮；失败+失新鲜并存时成功同时关闭两类。
- `tests/test_failure_reason.py`（**本轮新增**）：运行明细/管道汇总/活动告警三处可见最近
  一次失败的错误分类与信息；`FATAL` 与 `RETRY_EXHAUSTED` 依据可区分；最近失败跟随最新失败
  运行、成功恢复后清空、从未失败不残留。
- `tests/test_retention.py`（**本轮新增**）：超上限 drop-oldest 淘汰、保留/累计淘汰口径
  披露、被淘汰运行 EVICTED/UNKNOWN/FOUND 一致查询与不可复活、旧运行更新不误伤新运行、
  累计历史 1000→5000（持续淘汰、保留恒为 500）健康查询耗时仍恒定。
- `tests/test_retention_concurrency.py`（**本轮新增**）：同一管道 800 个运行、每事件重复
  3 份并发提交 + 8 个持续读取者：保留数任何时刻不越上限、累计淘汰恰为
  “观测数−上限”、管道计数/告警/失败原因始终自洽、被淘汰 ID 一律 `RUN_EVICTED`。

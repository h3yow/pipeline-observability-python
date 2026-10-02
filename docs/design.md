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

### 4.1 失败原因可见性与两类终止原因

每次 `FAILED` 事件上报的 `error_class / error_message` 都会落到该运行的“最近一次
失败原因”，并在三处一致可见：

| 位置 | 字段 |
| --- | --- |
| 运行明细 `PipelineStatus` | `last_error_class`、`last_error_message`、`last_failure_at`、`failure_terminal_reason` |
| 管道健康 `PipelineHealth` | `latest_failure_run_id`、`latest_failure_at`、`latest_error_class`、`latest_error_message`、`latest_failure_terminal_reason` |
| 活动失败告警 `ActiveAlertView` | `error_class`、`error_message`、`failure_terminal_reason`、`last_failure_at`（失新鲜告警这些字段恒为 `None`） |

`failure_terminal_reason` 明确区分两种**性质完全不同**的终止原因，避免误导处置：

- `NON_RETRYABLE`：错误本身不可重试（`error_class=FATAL`，一次即进入 `FAILED_FATAL`
  终态）。告警依据写作“错误本身不可重试（NON_RETRYABLE），立即终止”，**不会**被写成
  “重试已耗尽”。
- `RETRY_EXHAUSTED`：错误可重试（`RETRYABLE`），但重试机会已用尽（停留
  `FAILED_RETRYABLE` 且 `retry_exhausted=True`）。依据写作“可重试错误已用尽重试机会
  （RETRY_EXHAUSTED）”。
- 仍在等待/进行重试（`FAILED_RETRYABLE` 且未耗尽）时 `failure_terminal_reason=None`，
  只展示错误分类/信息，不谎称已终止。

**清空时机（关键口径）**：失败原因描述的是“最近一次**尚未恢复**的失败”。

- 运行在重试未耗尽前**成功恢复**：`last_error_class / last_error_message /
  last_failure_at / failure_terminal_reason` 立即全部清空为 `None`，同时关闭失败告警；
  历史 `failure_count` 仍保留（那是累计计数，不是“最近原因”）。
- 从未失败过的运行：这些字段恒为 `None`。
- 管道汇总的“最近一次为什么挂”是“当前仍处于失败态的运行中最近的一次失败”：某运行
  成功恢复后其贡献被移除，视图回落到其余仍失败的运行；全部恢复后为 `None`，不残留旧
  错误。进入 `FAILED_FATAL` 或重试耗尽的运行在状态机上不可再自行上报成功（分别以
  `FATAL_ALREADY_TERMINAL / RETRY_EXHAUSTED` 拒绝），其失败原因会保留到该运行被保留
  策略淘汰——这与“可重试期间恢复成功后清空”并不冲突。

### 4.2 失新鲜恢复后的明细口径（活动状态与判定依据同版）

巡检/成功都推进失新鲜结论，且**活动标志与判定依据在同一临界区一起更新**，杜绝
“活动告警已经关闭、判定依据还停留在失新鲜那一版”的自相矛盾快照：

- **巡检恢复**（`now - 基线 <= 阈值`）：`staleness_alert_active=False`，同时把
  `staleness_alert_reason` 刷新为当次“仍在新鲜窗口内”的新鲜结论（含基线/时间差/阈值），
  记录 `staleness_recovered_at=now`。
- **成功即时恢复**（收到 `SUCCEEDED`，不必等下轮巡检）：基线立即切换为
  `LAST_SUCCESS=success_at`、age=0，明细给出“成功即时恢复新鲜……在新鲜窗口内；
  失新鲜告警已关闭（此前共 N 段告警）”的依据，`staleness_recovered_at=success_at`。
  若该运行从未巡检过（无阈值口径），则只关闭告警、`staleness_alert_reason=None`。
- 查询时：活动中展示最近一次**失新鲜**依据；非活动时展示最近一次**恢复/新鲜**依据
  （`staleness_recovery_reason`），旧的失新鲜依据不会回潮。
- 同一段失新鲜结束后再巡检仍只计一次（`staleness_raised_count` 不增）；只有再次跨过
  阈值才开启**新段落**（计数 +1）。
- **时间契约**：巡检时刻 `now` 必须单调不减；早于上次巡检时刻的乱序巡检抛
  `ValueError`，防止拿旧时刻把已恢复实例重新判成失新鲜；成功事件时刻同样不得早于
  上次巡检时刻。

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

### 5.2 历史保留与淘汰策略（有界内存）

为避免“服务跑久了内存只增不减”，内存态实现对**不同运行实例数**设置明确上限：

- **上限**：`ObservabilityService(..., max_runs=N)` / `PipelineStore(max_runs=N)`，
  默认 `max_runs=1000`；传 `None` 表示不设上限（保留旧行为）。上限按“保留中的不同
  `pipeline_run_id` 个数”计，与管道数无关；跨所有管道共享。
- **淘汰规则**：每次写入应用后，若保留运行数超过上限，则淘汰**最近事件时间
  （`latest_event_at`）最旧**的运行；同一时间按 `run_id` 字典序决胜，保证确定性。
  牺牲品用一个 `(latest_event_at, run_id)` 最小堆选择，堆项惰性校验（记录已不在或该
  运行已更新到更晚时间即丢弃）；每个运行每次写入入堆一项、每项至多出堆一次，写入摊还
  O(log n)。
- **淘汰即摘除聚合**：被淘汰运行从归属集合、失败索引/连续失败数、失败原因视图、两类
  活动告警镜像中一并移除；若它恰是管道“最近一次运行”，则在剩余运行中重算该标量
  （仅淘汰命中时线性扫描一次 `latest_index`，常规健康查询仍是 O(1)）。被淘汰运行
  **不再进入任何汇总**。
- **墓碑与三态查询**：被淘汰的 `run_id` 进入一个有界墓碑集合（容量与上限同阶，挤出最
  旧墓碑）。因此单运行查询 `service.lookup_run(run_id)` 有明确一致的三种结论：
  - `PRESENT`：在保留窗口内，附带锁内构造的完整 `snapshot`；
  - `EVICTED`：曾被观测但已淘汰，`snapshot=None` 并附可读说明——**不报错、不返回残缺
    数据**；其后续事件上报被拒绝（`REJECTED / RUN_EVICTED`），不会借新事件“复活”；
  - `UNKNOWN`：从未观测到该运行。
  旧的 `service.status(run_id)` 便捷方法对 `EVICTED` 与 `UNKNOWN` 都返回 `None`，口径
  不变；需要区分二者时用 `lookup_run`。HTTP 层对应 200 / `410 Gone` / 404。
- **对外披露**：`service.retention_info()` 返回
  `RetentionInfo(max_runs, current_runs, total_evicted_runs)`；每个管道健康快照也带
  `retention`（保留口径）与该管道的 `evicted_run_count`；HTTP 另有 `GET /retention`。
- **并发与瞬时态**：淘汰在“实例锁 → 存储注册锁 → 管道锁”的固定锁序下，把“记录摘除 +
  墓碑登记 + 聚合/告警摘除”放在同一注册临界区完成。并发写入下，多个运行在淘汰执行前
  建记录，窗口可能**瞬时**超出上限（超出量不超过并发写入线程数），随后立即收敛回上限；
  终态严格满足 `current_runs <= max_runs`。健康查询 `health_snapshot()` 同时持注册锁与
  管道锁，保证 `run_count`、保留数、累计淘汰量与告警引用取自同一时刻、彼此自洽。
- 并发淘汰时若某写入运行自身就是全局最旧项，则本轮不淘汰自己（避免两个写入者互相把对方
  选为牺牲品的 AB-BA），其堆项保留到别的运行写入时再评估。

### 5.3 为什么健康查询不随历史线性变慢

聚合值在事件写入时**增量物化**，健康查询是 O(1) 的标量/计数读取：

- “最近事件时间 / 最近成功时间”是单调标量，写入时 O(1) 更新。
- “连续失败数”用「失败索引 dict + 失败时间最小堆」增量维护：每次失败终态入堆一次，
  成功抬升水位时把堆顶 `<= W` 的项批量出堆并调整计数；每个堆项至多入堆/出堆一次，
  写入摊还 O(log n)，查询 O(1)。
- “管道最近一次失败原因”用「失败原因 dict + 失败时间**最大**堆」维护：查询时惰性弹出
  失效堆顶（运行已恢复/被淘汰/重试链更新），摊还 O(1)，不随保留或累计历史增长。
- 有了保留上限，保留窗口大小恒定，淘汰堆项同样每项至多出入堆一次；因此即便累计上报的
  历史运行数无限增长，健康查询耗时也保持平稳（有界窗口 + 物化标量，非全量扫描）。
- 管道下多个运行的更新在固定加锁顺序下串行进入同一管道临界区，因此并发上报不丢更新；
  健康快照在锁内一次性构造，读不到半截中间态。

**部署形态**：当前为**进程内、内存态**实现，不做磁盘持久化、重启即清空，适合作为单实例
观测侧/sidecar。跨进程横向扩展、超长期留存属于存储后端的职责；接入持久化后端时应以同样
的物化聚合表 + 水位/堆（或定时聚合任务）+ 有界保留维持查询口径与复杂度不变。

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
  调度一致。系统不接受“拿更早的旧时刻对已推进到更晚基线的实例再判一次”这类乱序巡检
  （直接抛 `ValueError`）：那样会把一个已被更新成功覆盖的实例重新判成失新鲜。需要重放
  历史请用独立的服务实例。

收敛规则（`StalenessAlertState`，与失败告警完全分开）：

- 新鲜 → 失新鲜：开启**新的一段**失新鲜，`episode_id += 1`、`raised_count += 1`、
  记录 `raised_at`；
- 持续失新鲜期间反复巡检：**只告警一次、只计一次**，仅刷新判定依据；
- 恢复新鲜（重新成功，或在新基线下回到窗口内）：关闭活动告警、记录恢复时刻，**并把判定
  依据刷新为同版的新鲜/恢复结论**（`last_recovery_reason`、`staleness_recovered_at`）；
  再次失新鲜才算新段落，才允许再计一次。同一段失新鲜结束后的巡检不会重复报警、也不会让
  旧的失新鲜依据回潮。
- 收到成功事件时会在**同一写入临界区**立即关闭该运行的失新鲜告警并写入恢复依据（不必等
  下轮巡检），保证成功后查询不会短暂看到“已成功但仍挂着失新鲜、依据还是失新鲜版本”的
  中间态。明细口径见 4.2。

## 7. 两类告警的关系：分开、可并存、各自恢复

- 失败告警（`AlertKind.FAILURE`）由状态机失败终态驱动；失新鲜告警
  （`AlertKind.STALENESS`）由时间基线驱动。两者性质不同，分别有独立的活动标志、计数、
  首次产生时间与判定依据，因此**可能同时存在**（例如：早就失败了，且距上次成功也已超阈值）。
- 实例明细 `PipelineStatus` 含：
  `pipeline_id`、`staleness_alert_active`、`staleness_alert_reason`、
  `staleness_raised_count`、`staleness_recovered_at`、`staleness_recovery_reason`、
  `active_alert_kinds`（当前活动告警类别元组，如 `("FAILURE","STALENESS")`），以及失败
  原因字段（见 4.1）。旧字段 `alert_active / alert_reason` 仍只表示失败告警，含义不变。
- 管道汇总用两组计数/标志与 `active_alerts()` 明细区分两类；成功恢复会同时关闭该运行的
  两类活动告警并清空失败原因，但二者也能各自独立出现/消失（如运行中先失新鲜、后失败；
  或只失新鲜未失败）。

## 8. 并发安全模型

- 每个 `pipeline_run_id` 一把实例锁（惰性创建，注册表另用短锁保护），同一实例的更新完全
  串行化；每个 `pipeline_id` 再一把管道锁保护聚合；另有一把存储注册锁保护运行记录表、
  墓碑集合、顺序堆与保留计数。
- 加锁顺序固定且单向：正常写入为「实例锁 → 管道锁」；淘汰与健康一致性读取为
  「实例锁 → 存储注册锁 → 管道锁」（或纯读路径「注册锁 → 管道锁」）。所有路径中管道锁
  永远在链路最后获取，不存在反向获取，因此无等待环、无死锁；不同实例/管道互不阻塞。
- 事件接收、计数、重试调度、失败告警、失新鲜成功恢复、管道聚合更新都在同一实例临界区及
  其嵌套的管道临界区完成；实例快照与管道快照分别在对应锁内一次性构造，均为不可变值对象。
- 淘汰把“运行记录摘除 + 墓碑登记 + 管道聚合/告警摘除”放在同一注册临界区，因此查询者
  不会看到“记录已消失但仍计入汇总/仍挂告警”的中间态。

## 9. HTTP 接口（main.py）

| 方法/路径 | 说明 |
| --- | --- |
| `POST /events` | 上报事件（**可选**字段 `pipeline_id / error_class / error_message`）。成功 200；重复幂等 200；冲突/拒绝 409 带 `reason`（含 `RUN_EVICTED`）。 |
| `GET /runs/{run_id}` | 查询单个实例汇总；旧字段全部保留，末尾追加管道/失新鲜/失败原因/恢复明细新字段。在窗口内 200；**已淘汰 410 Gone（`reason=RUN_EVICTED`）**；从未见过 404。 |
| `GET /runs` | 列出全部（保留中的）实例 ID。 |
| `GET /pipelines` | 列出全部管道 ID。 |
| `GET /pipelines/{pipeline_id}/health` | 管道级健康汇总（两类告警计数与明细、最近失败原因、保留口径与该管道淘汰量）；不存在 404。 |
| `GET /retention` | 保留口径：`max_runs / current_runs / total_evicted_runs`。 |
| `POST /staleness/sweep` | 按需触发失新鲜巡检；body 可选 `pipeline_id / run_id / fresh_within_seconds / now`。 |

### 9.1 新旧兼容范围

- **不传 `pipeline_id` 的旧上报完全不受影响**：该实例默认“自成一管道”，其管道 ID 即
  `pipeline_run_id`，旧的单实例行为、状态机、计数、重试、失败告警口径保持不变。
- 一旦显式指定 `pipeline_id`，同一运行后续事件不得改报其他管道（否则以
  `ILLEGAL_TRANSITION` 拒绝），防止归属漂移导致聚合污染。
- `PipelineEvent` 的 `pipeline_id / error_class / error_message` 均为带默认值的可选字段；
  `PipelineStatus / PipelineHealth / ActiveAlertView` 的所有新字段都带默认值并**追加在
  末尾**，旧的位置构造与字段读取不被打断。
- 旧方法签名保持兼容：`staleness_report(run_id, fresh_within_seconds=None)`、
  `is_stale(...)`、`status/all_status/ingest/active_alerts/pipeline_health` 均不变；阈值
  缺省回落到服务级默认值。
- `status(run_id)` 对“已淘汰 / 从未见过”仍统一返回 `None`（旧调用无需改）；需要区分二者
  时新用 `lookup_run(run_id)`（`PRESENT / EVICTED / UNKNOWN`）。HTTP 侧旧调用方只判断
  200/404 也不受影响，`410 Gone` 是对“已淘汰”的增强口径。
- **默认行为唯一需要留意的差异**：内存历史保留默认有界（`max_runs=1000`）。若旧用法依赖
  “单进程无限累积全部运行”，构造时显式传 `max_runs=None` 即可恢复旧行为。

## 10. 验证方法

```bash
uv sync
uv run pytest -s          # 全量（时间线与判定依据打印到日志，不依赖真实 sleep）
uv run ruff check pipeline_obs main.py tests
```

所有时间相关用例都通过注入时钟/显式 `now` 确定性驱动，**不依赖真实等待**；测试运行时通过
`timeline` logger 打印**事件时间线、逐条判定依据、失败原因、失新鲜判定（基线/时间差/阈值/
结论）、恢复结论与管道健康汇总/活动告警明细**。

- `tests/test_retry.py`：退避序列与封顶、达到上限停止、FATAL 不重试且原因可区分。
- `tests/test_state_machine.py`：乱序拒绝、重复幂等不计数、同 attempt 冲突结果、FATAL 终态后拒绝。
- `tests/test_alerting.py`：同一失败异常恢复前只告警一次、恢复后新异常重新计数、时效判定依据。
- `tests/test_concurrency.py`：并发重复上报、重试链重复投递、乱序批次、1 写 8 读快照不变量。
- `tests/test_pipeline_health.py`：
  - 40 个运行并发提交 + 8 个持续读取者：管道汇总不丢更新、无中间态；
  - 失新鲜“进入 → 持续多次巡检 → 成功恢复 → 再次失新鲜”：第一段只告警/计数一次；
  - 失败告警与失新鲜告警并存、各自独立恢复，`active_alert_kinds` 与两类计数正确；
  - 跨多运行混合成功/失败 + 成功水位抬升：连续失败数精确；
  - 显式 `max_runs=None` 下历史从 500 增至 5000：健康查询耗时恒定（物化聚合非全量扫描）。
- `tests/test_recovery_consistency.py`（新增）：失新鲜成功恢复后活动状态与判定依据同版更新
  （带恢复时刻与依据）、同段结束后再巡检不重复报警且旧结论不回潮、乱序巡检时刻被拒。
- `tests/test_failure_reasons.py`（新增）：FATAL 的错误分类/信息在运行明细/管道汇总/活动告警
  三处可见且依据为 `NON_RETRYABLE`；可重试耗尽为 `RETRY_EXHAUSTED`，两类可区分；等待重试中
  不谎称终止；恢复成功/从未失败时错误字段清空；管道最近失败原因跨多运行取最近并随恢复回落。
- `tests/test_retention.py`（新增）：
  - 超上限淘汰最近事件最旧的运行、保留口径与累计淘汰量披露；
  - 被淘汰运行三态查询一致（不报错、不残缺）、再上报 `RUN_EVICTED` 拒绝且不复活；
  - 被淘汰失败运行移出连续失败数/失败原因/活动告警；淘汰按最近事件而非创建顺序；
  - 有界窗口下累计上报 5600 个运行（淘汰 5100）健康查询耗时仍平稳；
  - 同一管道 400 个运行 16 线程并发上报触发淘汰 + 8 个并发读取者：所有一致性快照不变量成立
    （运行数、连续失败数、告警数、保留数、淘汰量、告警不引用已淘汰运行），终态恰为上限。

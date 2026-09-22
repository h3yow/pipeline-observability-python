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
- `pipeline_obs/store.py`：线程安全的状态机、去重、乱序/冲突拒绝、一致快照。
- `pipeline_obs/retry.py`：可重试判定、指数退避、次数上限。
- `pipeline_obs/alerting.py`：告警收敛与时效性判定。
- `pipeline_obs/service.py`：门面服务。
- `main.py`：FastAPI 接口（接收事件、查询汇总）。

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

## 5. 时效性判定（基于最近成功时间）

`ObservabilityService.staleness_report(run_id, fresh_within_seconds)` 返回
`(是否失新鲜, 判定依据文本)`：

1. 优先以 `last_success_at`（最近成功时间）为基线：`now - last_success_at > 阈值`
   即失新鲜。
2. 从未成功过的实例回退到 `started_at`（首次事件时间）。
3. 无任何事件的实例默认视为新鲜；未知 run_id 判定为失新鲜。

判定依据文本包含基线类型、基线时间、时间差与阈值，保证结论可解释。

## 6. 并发安全模型

- 每个 `pipeline_run_id` 一把 `threading.Lock`（惰性创建，注册表另用短锁保护），
  同一实例的更新完全串行化，天然消除丢失更新。
- 不同实例互不阻塞。
- 事件接收、计数、重试调度、告警评估、快照读取均在锁内完成；快照为不可变值对象。

## 7. HTTP 接口（main.py）

| 方法/路径 | 说明 |
| --- | --- |
| `POST /events` | 上报事件。成功 200；重复上报幂等 200（`status=DUPLICATE`）；冲突/拒绝 409 并带 `reason`。 |
| `GET /runs/{run_id}` | 查询单个实例的一致汇总快照；不存在 404。 |
| `GET /runs` | 列出全部实例 ID。 |

事件字段：`pipeline_run_id, event_id, event_type(STARTED|SUCCEEDED|FAILED),
version, attempt, occurred_at(可选,默认当前时间), error_class(RETRYABLE|FATAL),
error_message`。

## 8. 验证方法

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

# 数据管道运行观测服务（Pipeline Observability）

在 FastAPI 服务内提供一套面向数据管道运行（pipeline run）的观测能力，保证同一管道在
**并发上报、重复上报、失败重试**下的状态与告警结论**一致且可解释**。

- 语言/运行时：Python 3.14、FastAPI、uv 管理依赖
- 并发模型：服务内以单把 `threading.RLock` 把「检查—判定—写入—快照」原子化
- 时间源：可注入 `Clock`（测试使用 `ManualClock`，确定性验证退避与时效）

## 目录结构

```
pipeline_obs/
  models.py         # 枚举与数据结构（事件、运行、重试、告警、汇总）
  errors.py         # EventRejectedError（携带可区分的拒绝原因）
  clock.py          # 时间源（system_clock / ManualClock）
  retry.py          # RetryPolicy：错误分类、指数退避、次数上限
  state_machine.py  # 事件驱动状态机：去重、乱序拒绝、合法流转、计数
  alerts.py         # AlertManager：告警收敛与幂等解除
  service.py        # ObservabilityService：线程安全门面（受理/调度/汇总）
main.py             # FastAPI HTTP 入口
tests/              # pytest 测试（含时间线与判定依据日志）
```

## 1. 核心概念

- `PipelineEvent`：一次上报。字段含 `event_id`（幂等键）、`pipeline_id`、`run_id`、
  `event_type`、`event_time`（UNIX 秒）、可选 `attempt_no` 与 `error`。
- 一个 `run_id` 代表管道的一次运行实例；一次运行由若干「尝试」`attempt_no=1,2,...` 组成。
- 运行状态 `RunStatus`：`RUNNING / SUCCEEDED / FAILED`。

## 2. 状态流转

```
RUN_STARTED ───────────────────────────────► RUNNING
ATTEMPT_STARTED(n)   （n 必须等于当前期望序号 = 已失败次数 + 1）
ATTEMPT_SUCCEEDED(n) ───────────────────────► SUCCEEDED（终态）
ATTEMPT_FAILED(n)
   ├─ 错误 FATAL（不可重试）───────────────► FAILED（终态，不调度）
   ├─ 错误 RETRYABLE 且未达上限 ───────────► RUNNING（退避后允许 n+1）
   └─ 错误 RETRYABLE 且 failure_count>max  ► FAILED（终态，retry_exhausted=true）
RUN_ABORTED ───────────────────────────────► FAILED（终态，按 FATAL 处理）
```

### 幂等与防回退

- **event_id 幂等**：同一 `run` 内相同 `event_id` 重复上报，返回 `deduped=true`，
  不改变任何状态、不重复计数。
- **乱序拒绝**：`event_time` 早于该 run 已观察到的最新时间 → 拒绝（`STALE_EVENT`），
  旧事件不得把状态/计数往回拉。
- **终态保护**：`SUCCEEDED/FAILED` 后再收到推进事件 → 拒绝，状态不回退。

### 可区分的拒绝原因（`RejectReason`）

| 原因 | 触发条件 |
| --- | --- |
| `UNKNOWN_PIPELINE` | 上报到未登记的管道 |
| `STALE_EVENT` | 乱序：事件时间早于最新已接受事件 |
| `INVALID_TRANSITION` | 终态后开启新尝试/上报新结果、缺少错误信息等非法流转 |
| `DUPLICATE_ATTEMPT` | 同一 `attempt_no` 重复 `ATTEMPT_STARTED` |
| `DUPLICATE_OUTCOME` | 同一 attempt 重复上报**相同**结论 |
| `CONFLICTING_OUTCOME` | 同一 attempt 先后上报**相反**结论（成功 vs 失败） |
| `UNKNOWN_ATTEMPT` | 对从未 `STARTED` 的 attempt 上报结果 |
| `ATTEMPT_OUT_OF_SEQUENCE` | 尝试序号跳跃（未按 1,2,3 顺序推进） |
| `RETRY_NOT_DUE` | 退避时间未到就试图开始下一次尝试 |
| `RETRY_EXHAUSTED` | 已达重试次数上限（汇总中以 `retry_exhausted=true` 体现） |

> 判定优先级：乱序 → attempt 语义（重复/冲突/未知/序号）→ 终态。
> 因此「终态后重复上报旧结论」会精确报 `DUPLICATE_OUTCOME/CONFLICTING_OUTCOME`，
> 而不是被笼统的终态错误掩盖。

## 3. 重试规则

- 错误分类：`RETRYABLE`（可重试）与 `FATAL`（不可重试）。
- **次数上限**：`max_retries` 表示「首次尝试之后允许的重试次数」，即最多
  `max_retries + 1` 次尝试。可重试失败累计到 `failure_count > max_retries` 时
  运行进入 `FAILED`，且 `retry_exhausted=true`，此后 `run_due_retries()` 永不再调度它。
- **指数退避**：第 `k` 次失败后的等待 `delay = base_delay * backoff_factor**(k-1)`，
  下次允许时刻 `next_retry_at = failed_at + delay`。
- 退避未到时上报下一次尝试 → `RETRY_NOT_DUE`；需由调度器周期调用 `run_due_retries()`
  （或 HTTP `POST /tick`）把到期 run 置为「可开始下一次尝试」。
- `FATAL` 立即终结，`retry_exhausted=false`，与「重试耗尽」场景区分。

## 4. 时效性与告警收敛

- 时效基线：管道 `last_success_at`；从未成功时以最早的 run 启动时间为基线。
  `now - baseline > freshness_threshold` 判定为 stale。
- 告警类型：`RUN_FAILED`（run 级）、`STALE_PIPELINE`（管道级）。
- **收敛键**：`(pipeline_id, run_id, alert_type)`；同一键处于 `ACTIVE` 时反复触发
  只返回同一条告警，不重复计数。
- **恢复解除**：run 成功会解除其 `RUN_FAILED` 与管道 `STALE_PIPELINE` 告警；
  `resolve` 幂等。解除后再次发生可作为「新的一轮异常」重新产生告警。
- 失败告警 `detail` 区分来源：`fatal: [CODE] msg` 与 `retry exhausted: [CODE] msg`。

## 5. 并发一致性

所有公共方法都在同一把 `RLock` 内完成「读取—检查—写入」：

- 并发同 `event_id`：恰有一份生效，其余 `deduped`。
- 并发同结论（不同 `event_id`）：恰有一份生效，其余 `DUPLICATE_OUTCOME`，计数不翻倍。
- 并发查询：`RunSummary/PipelineSummary` 在锁内一次性构建为不可变快照，
  读者不会看到中间态；`running + succeeded + failed == total` 恒成立，无丢失更新。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/pipelines/{id}` | 登记管道（阈值、重试参数） |
| POST | `/events` | 上报事件；接受返回 200，拒绝返回 409 与 `reason` |
| POST | `/tick` | 推进到期重试 + 刷新时效性告警 |
| GET | `/pipelines/{id}` | 管道维度汇总与活跃告警 |
| GET | `/pipelines/{id}/runs/{run_id}` | 运行实例汇总与活跃告警 |

启动：

```bash
uv run fastapi dev main.py
# 或
uv run uvicorn main:app --reload
```

上报示例：

```bash
curl -s -XPOST localhost:8000/events -H 'content-type: application/json' -d '{
  "event_id":"e1","pipeline_id":"demo","run_id":"r1",
  "event_type":"ATTEMPT_FAILED","event_time":1727000000,"attempt_no":1,
  "error":{"code":"TIMEOUT","message":"transient","category":"RETRYABLE"}
}'
```

## 7. 验证方法

```bash
uv run pytest                 # 全量测试
uv run pytest -s              # 同时打印每个用例的「事件时间线」与「判定依据」
uv run pytest tests/test_concurrency.py -s   # 只看并发场景时间线
```

测试覆盖：

- `tests/test_state_machine.py`：重复/乱序/冲突/未知/跳号等拒绝原因一一可区分，
  且被拒事件不污染状态；终态不回退。
- `tests/test_retry.py`：指数退避时刻、退避未到拒绝、达到上限终止且不再重试、
  FATAL 立即终止且与耗尽区分。
- `tests/test_concurrency.py`：16 线程同 event_id、12 线程同结论并发、
  8 读线程并发下汇总自洽、时效告警恢复前不重复并在恢复后解除。
- `tests/test_api.py`：HTTP 端到端（幂等、409 原因、汇总）。

日志样例（判定依据 + 时间线）会打印线程名、事件序号、事件时间、接受/去重/拒绝原因，
便于复盘「同一结论为何在并发下仍然一致」。

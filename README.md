# pipeline-observability-python

数据管道运行观测服务：在**并发上报、重复上报与失败重试**下保证同一管道实例的
状态与告警结论一致、可解释。

## 能力

- **事件接收与状态推进**：基于 `event_id` 幂等去重，基于 `version` 拒绝乱序/迟到；
  状态只进不退；冲突上报被拒绝并给出可区分原因
  （`STALE_VERSION / ILLEGAL_TRANSITION / FATAL_ALREADY_TERMINAL /
  RETRY_EXHAUSTED / CONFLICTING_OUTCOME`）。
- **重试调度**：指数退避 + 次数上限；区分 `RETRYABLE` 与 `FATAL`；达到上限后
  不再重试、不再接受后续尝试上报。
- **状态汇总与告警收敛**：同一异常恢复前只告警一次；逐实例加锁 + 锁内构造不可变
  快照，并发查询看不到中间态，更新不丢失。
- **管道级健康汇总**：按 `pipeline_id` 聚合其下多次运行——最近一次运行状态、最近成功
  时间、最近成功之后的连续失败运行数、当前活动告警及原因。聚合在写入时增量物化，
  健康查询 O(1)、不随历史实例数线性变慢；快照在管道锁内一次性构造。
- **失新鲜监控（两类告警分开）**：以最近成功时间为基线（从未成功则用第一次上报时间），
  超阈值即产生独立的**失新鲜告警**，与**失败告警**可并存、各自恢复；同一段失新鲜只告警/
  计数一次，重新成功即关闭。阈值可配置，巡检可用注入时钟/显式 `now` 确定性触发，无需
  真实 sleep。

## 运行

```bash
uv sync
uv run fastapi dev main.py     # HTTP 服务
uv run pytest -s               # 测试（打印事件时间线与判定依据）
```

## 快速上手

```python
from pipeline_obs import (
    ObservabilityService, RetryPolicy, PipelineEvent, EventType, ErrorClass,
)

svc = ObservabilityService(retry_policy=RetryPolicy(max_attempts=3),
                           fresh_within_seconds=300.0)  # 失新鲜阈值可配置

# 同一管道下的多个运行通过 pipeline_id 归组（不传则该实例自成一管道，兼容旧用法）
svc.ingest(PipelineEvent("run-1", "e1", EventType.STARTED, occurred_at=1.0,
                         version=1, pipeline_id="pipe-A"))
svc.ingest(PipelineEvent("run-1", "e2", EventType.FAILED, occurred_at=2.0,
                         version=2, attempt=1, error_class=ErrorClass.RETRYABLE,
                         pipeline_id="pipe-A"))

# 管道健康（O(1) 物化聚合）
print(svc.pipeline_health("pipe-A"))
# 活动告警明细（失败/失新鲜分列）：service.active_alerts("pipe-A")

# 确定性失新鲜巡检（注入时刻，不靠 sleep）
verdict = svc.check_staleness("run-1", now=400.0)
print(verdict.stale, verdict.baseline_kind, verdict.age_seconds, verdict.reason)
# 也可按管道/全量巡检：svc.sweep_staleness(pipeline_id="pipe-A", now=400.0)
```

旧的单实例用法（不传 `pipeline_id`、`service.status(run_id)`、`staleness_report(...)`
等）保持不变，详见 [`docs/design.md`](docs/design.md)。

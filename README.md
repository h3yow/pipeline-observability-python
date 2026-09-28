# pipeline-observability-python

数据管道运行观测服务：在**并发上报、重复上报与失败重试**下保证同一管道实例的
状态与告警结论一致、可解释，并提供**管道级健康汇总**与**自动失新鲜监控**。

## 能力

- **事件接收与状态推进**：基于 `event_id` 幂等去重，基于 `version` 拒绝乱序/迟到；
  状态只进不退；冲突上报被拒绝并给出可区分原因
  （`STALE_VERSION / ILLEGAL_TRANSITION / FATAL_ALREADY_TERMINAL /
  RETRY_EXHAUSTED / CONFLICTING_OUTCOME`）。事件可选携带 `pipeline_id` 归属管道。
- **重试调度**：指数退避 + 次数上限；区分 `RETRYABLE` 与 `FATAL`；达到上限后
  不再重试、不再接受后续尝试上报。
- **管道级健康汇总**：给定 `pipeline_id` 返回最近一次运行状态、最近一次成功时间、
  其后连续失败的运行次数、当前活动告警（失败/失新鲜）及判定依据；同一管道多运行
  并发上报与查询下汇总自洽、不丢更新、不露中间态。每管道按
  `retained_run_limit`（默认 1000）保留最近实例，查询复杂度不随历史线性增长。
- **两类独立告警**：失败告警（失败这一事实）与失新鲜告警（长时间未成功这一时效事实）
  分开收敛，可并存、各自恢复；同一段失新鲜期间只告警一次、只计一次。
- **失新鲜自动监控**：以最近成功时间为基线（从未成功则用第一次上报时间），
  `age > 阈值` 判失新鲜；阈值可配置，巡检支持注入 `now` **确定性触发，无需 sleep**；
  成功事件即时恢复新鲜。判定依据包含基线类型、基线时间、时间差、阈值与结论。

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

svc = ObservabilityService(
    retry_policy=RetryPolicy(max_attempts=3),
    default_fresh_within_seconds=600.0,   # 失新鲜阈值（也可每次巡检时覆盖）
    retained_run_limit=1000,
)
svc.ingest(PipelineEvent("run-1", "e1", EventType.STARTED, occurred_at=1.0,
                         version=1, pipeline_id="pipe-A"))
r = svc.ingest(PipelineEvent("run-1", "e2", EventType.FAILED, occurred_at=2.0,
                             version=2, attempt=1, error_class=ErrorClass.RETRYABLE,
                             pipeline_id="pipe-A"))
print(r.status, r.state, r.retry.should_retry, r.retry.delay_seconds)

# 管道级健康
print(svc.pipeline_health("pipe-A"))

# 确定性失新鲜巡检（注入时间，不用真实等待）
stale = svc.run_staleness_sweep(now=9999.0)
print([s.pipeline_run_id for s in stale])
```

旧的单实例上报/查询方式（不带 `pipeline_id`、`status() / staleness_report()` 等）
完全保持兼容。

详细规则见 [`docs/design.md`](docs/design.md)。

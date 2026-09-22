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
- **时效性判定**：以最近成功时间为基线（从未成功则回退首次事件时间）。

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

svc = ObservabilityService(retry_policy=RetryPolicy(max_attempts=3))
svc.ingest(PipelineEvent("run-1", "e1", EventType.STARTED, occurred_at=1.0, version=1))
r = svc.ingest(PipelineEvent("run-1", "e2", EventType.FAILED, occurred_at=2.0,
                             version=2, attempt=1, error_class=ErrorClass.RETRYABLE))
print(r.status, r.state, r.retry.should_retry, r.retry.delay_seconds)
print(svc.status("run-1"))
```

详细规则见 [`docs/design.md`](docs/design.md)。

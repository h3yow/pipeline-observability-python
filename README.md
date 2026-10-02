# pipeline-observability-python

数据管道运行观测服务：在**并发上报、重复上报与失败重试**下保证同一管道实例的
状态与告警结论一致、可解释。

## 能力

- **事件接收与状态推进**：基于 `event_id` 幂等去重，基于 `version` 拒绝乱序/迟到；
  状态只进不退；冲突上报被拒绝并给出可区分原因
  （`STALE_VERSION / ILLEGAL_TRANSITION / FATAL_ALREADY_TERMINAL /
  RETRY_EXHAUSTED / CONFLICTING_OUTCOME / RUN_EVICTED`）。
- **重试调度**：指数退避 + 次数上限；区分 `RETRYABLE` 与 `FATAL`；达到上限后
  不再重试、不再接受后续尝试上报。
- **失败原因可见、两类终止可区分**：运行明细、管道健康汇总、活动告警明细都带
  最近一次失败的 `last_error_class`（错误分类）与 `last_error_message`（错误信息原文），
  以及 `failure_termination`：`FATAL`（错误本身不可重试）与 `RETRY_EXHAUSTED`
  （可重试错误已用尽机会）——依据文本明确区分二者，FATAL 不再被写成“重试耗尽”。
  **清空时机**：从未失败时为空；一旦该运行成功恢复，失败分类/信息立即清空，
  绝不残留上一次的旧错误；管道汇总的“最近一次失败”只反映当前仍失败的运行，
  全部恢复后整体清空。
- **状态汇总与告警收敛**：同一异常恢复前只告警一次；逐实例加锁 + 锁内构造不可变
  快照，并发查询看不到中间态，更新不丢失。
- **管道级健康汇总**：按 `pipeline_id` 聚合其下多次运行——最近一次运行状态、最近成功
  时间、最近成功之后的连续失败运行数、当前活动告警及原因、**最近一次失败的错误分类/
  信息/终止原因**。聚合在写入时增量物化，健康查询 O(1)、不随历史实例数线性变慢；
  快照在管道锁内一次性构造。
- **失新鲜监控（两类告警分开，恢复后明细自洽）**：以最近成功时间为基线（从未成功则用
  第一次上报时间），超阈值即产生独立的**失新鲜告警**，与**失败告警**可并存、各自恢复；
  同一段失新鲜只告警/计数一次。**成功恢复时活动状态与判定依据在同一临界区一起刷新**：
  告警关闭的同时，明细依据立即变为“恢复/新鲜”结论并带恢复时刻
  （`staleness_last_recovered_at`），不再出现“告警已关、依据还停在失新鲜”的矛盾；
  同段失新鲜结束后再巡检不重复报警，拿更早的旧时刻做乱序巡检也不会让旧结论回潮。
  阈值可配置，巡检可用注入时钟/显式 `now` 确定性触发，无需真实 sleep。
- **历史保留与淘汰（内存有明确上限）**：`max_retained_runs`（默认 10000）为保留的
  不同运行实例数硬上限，超出后按“最近事件时间最旧、同时间 run_id 字典序最小”
  **drop-oldest** 淘汰。淘汰在**新运行占名额之前的同一临界区**完成，因此任何时刻
  保留数都不越上限，且记录摘除与管道聚合剔除原子一致。保留口径与累计淘汰量通过
  `retention_status()` / `GET /retention` 披露；被淘汰运行不再进入任何汇总，
  `lookup_run()` 给出一致明确的 `FOUND / EVICTED / UNKNOWN` 结论（不报错、不返回
  残缺数据），其再上报被 `RUN_EVICTED` 拒绝、不会“借尸还魂”。

## 保留与淘汰口径（速览）

| 项 | 口径 |
| --- | --- |
| 保留上限 | `max_retained_runs`，构造服务或 `PipelineStore` 时可配，默认 10000。 |
| 淘汰规则 | 超出上限即淘汰“最近事件时间最旧”的运行（drop-oldest），同时间按 run_id 决胜。 |
| 对外披露 | `service.retention_status()` → `max_runs / retained_runs / evicted_runs / policy`。 |
| 强一致视图 | `service.retained_runs_view()` 在同一锁内返回（保留口径, 保留运行 ID 集合）。 |
| 单运行查询 | `service.lookup_run(id)` → `FOUND`（带完整快照）/ `EVICTED` / `UNKNOWN`；旧的 `status(id)` 对淘汰者返回 `None`。 |
| 被淘汰再上报 | 拒绝 `REJECTED/RUN_EVICTED`；持久墓碑保证同 ID 永不复活。 |
| 查询成本 | 健康汇总为 O(1) 物化读取，累计历史不断增长、保留集合大小不变时不线性变慢。 |
| 墓碑说明 | 完整运行状态/失败明细/聚合贡献严格受上限约束；为保证幂等防还魂，仅为每个历史 run_id 保留一枚极小的墓碑锁（不含事件集合/堆项/聚合）。 |

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
                           fresh_within_seconds=300.0,  # 失新鲜阈值可配置
                           max_retained_runs=10_000)    # 历史运行保留上限

# 同一管道下的多个运行通过 pipeline_id 归组（不传则该实例自成一管道，兼容旧用法）
svc.ingest(PipelineEvent("run-1", "e1", EventType.STARTED, occurred_at=1.0,
                         version=1, pipeline_id="pipe-A"))
svc.ingest(PipelineEvent("run-1", "e2", EventType.FAILED, occurred_at=2.0,
                         version=2, attempt=1, error_class=ErrorClass.FATAL,
                         error_message="上游返回非法数据格式",
                         pipeline_id="pipe-A"))

# 运行明细：最近一次失败原因 + 两类终止原因之一
snap = svc.status("run-1")
print(snap.last_error_class, snap.last_error_message, snap.failure_termination)
# -> FATAL 上游返回非法数据格式 FATAL

# 管道健康（O(1) 物化聚合）：同样带最近一次失败原因
h = svc.pipeline_health("pipe-A")
print(h.latest_failure_run_id, h.latest_failure_error_class,
      h.latest_failure_error_message, h.latest_failure_termination)
# 活动告警明细（失败/失新鲜分列，失败告警带错误分类/信息/终止原因）：
# service.active_alerts("pipe-A")

# 历史保留口径与淘汰量
print(svc.retention_status())          # max_runs / retained_runs / evicted_runs
print(svc.lookup_run("old-run"))       # FOUND / EVICTED / UNKNOWN，结论一致明确

# 确定性失新鲜巡检（注入时刻，不靠 sleep）
verdict = svc.check_staleness("run-1", now=400.0)
print(verdict.stale, verdict.baseline_kind, verdict.age_seconds, verdict.reason)
# 也可按管道/全量巡检：svc.sweep_staleness(pipeline_id="pipe-A", now=400.0)
```

## 新旧调用兼容范围

- 旧上报（`PipelineEvent` 既有字段）、旧查询（`ingest / status / all_status /
  pipeline_health / active_alerts / staleness_report / is_stale /
  check_staleness / sweep_staleness`）的调用方式与返回主体字段全部保留。
- 新增能力一律通过**新增可选入参**（`max_retained_runs`、`error_message` 早已存在）
  与**在返回对象末尾追加带默认值的字段**提供，不改动旧字段顺序与含义；
  旧的位置构造、字段读取不受影响。
- `status(run_id)` 语义不变（不存在/已淘汰都返回 `None`）；需要区分
  “已淘汰”与“从不知晓”时改用新的 `lookup_run(run_id)`。
- 被淘汰运行的新上报返回新增拒绝原因 `RUN_EVICTED`（属于既有
  `IngestStatus.REJECTED` + `RejectReason` 枚举的新增值，旧调用按 REJECTED 处理即可）。

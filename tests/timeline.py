"""测试辅助：记录并打印事件时间线与每次判定依据。"""

from __future__ import annotations

import logging

from pipeline_obs import PipelineEvent
from pipeline_obs.store import IngestResult

timeline_log = logging.getLogger("timeline")


def log_event(seq: int, event: PipelineEvent) -> None:
    err = f" error={event.error_class.value if event.error_class else '-'}"
    timeline_log.info(
        "[时间线 %02d] run=%s event_id=%-4s type=%-9s v=%d attempt=%d%s @t=%.3f",
        seq,
        event.pipeline_run_id,
        event.event_id,
        event.event_type.value,
        event.version,
        event.attempt,
        err,
        event.occurred_at,
    )


def log_result(result: IngestResult) -> None:
    reason = result.reason.value if result.reason else "-"
    retry = ""
    if result.retry is not None:
        retry = (
            f" | retry(should={result.retry.should_retry}, "
            f"next_attempt={result.retry.next_attempt}, "
            f"delay={result.retry.delay_seconds:.3f})"
        )
    timeline_log.info(
        "        -> 判定=%-8s state=%-16s reason=%-24s %s%s",
        result.status.value,
        result.state.value,
        reason,
        result.detail,
        retry,
    )


def log_snapshot(snap) -> None:
    timeline_log.info(
        "        汇总: state=%s attempt=%d applied_v=%d failures=%d "
        "exhausted=%s alert=%s last_success=%s next_retry_at=%s",
        snap.state.value,
        snap.current_attempt,
        snap.applied_version,
        snap.failure_count,
        snap.retry_exhausted,
        snap.alert_active,
        snap.last_success_at,
        snap.next_retry_at,
    )
    if snap.alert_reason:
        timeline_log.info("        告警依据: %s", snap.alert_reason)


def log_health(h, alerts=()) -> None:
    """打印管道级健康汇总与活动告警明细（alerts 由 active_alerts() 显式传入）。"""
    timeline_log.info(
        "        管道[%s] runs=%d 最近运行=%s(%s) last_success=%s 连续失败=%d "
        "失败告警=%d 失新鲜告警=%d 阈值=%s 巡检@=%s",
        h.pipeline_id,
        h.run_count,
        h.latest_run_id,
        h.latest_run_state.value if h.latest_run_state else "-",
        h.last_success_at,
        h.consecutive_failures,
        h.active_failure_alert_count,
        h.active_staleness_alert_count,
        h.staleness_threshold_seconds,
        h.checked_at,
    )
    for a in alerts:
        timeline_log.info(
            "          活动告警 run=%s kind=%-9s since=%s version=%s | %s",
            a.pipeline_run_id,
            a.kind.value,
            a.since,
            a.version,
            a.reason,
        )


def log_verdict(run_id: str, v) -> None:
    """打印一次失新鲜判定结论（基线/时间差/阈值/结论）。"""
    timeline_log.info(
        "        巡检 run=%s now=%.3f 基线=%s baseline_at=%s 差值=%s 阈值=%.3f -> stale=%s",
        run_id,
        v.now,
        v.baseline_kind.value,
        v.baseline_at,
        f"{v.age_seconds:.3f}s" if v.age_seconds is not None else "-",
        v.threshold_seconds,
        v.stale,
    )
    timeline_log.info("          判定依据: %s", v.reason)

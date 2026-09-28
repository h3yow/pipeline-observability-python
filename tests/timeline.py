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
        timeline_log.info("        失败告警依据: %s", snap.alert_reason)
    if snap.staleness_assessment is not None:
        a = snap.staleness_assessment
        timeline_log.info(
            "        失新鲜判定: stale=%s 基线=%s 基线时间=%s 距今=%ss 阈值=%ss",
            a.stale, a.basis, a.basis_time, a.age_seconds, a.threshold_seconds,
        )
    if snap.staleness_alert_reason:
        timeline_log.info("        失新鲜告警依据: %s", snap.staleness_alert_reason)


def log_pipeline_health(health) -> None:
    """打印管道级健康汇总时间线。"""
    timeline_log.info(
        "        管道[%s] 健康: 最近运行=%s(%s) 最近事件@%s 最近成功=%s "
        "连续失败=%d 失败告警=%s 失新鲜告警=%s 保留实例=%d(上限%d,累计淘汰%d)",
        health.pipeline_id,
        health.latest_run_id,
        health.latest_state.value if health.latest_state else None,
        health.latest_event_at,
        health.last_success_at,
        health.consecutive_failures,
        health.failure_alert_active,
        health.staleness_alert_active,
        health.run_count,
        health.retained_run_limit,
        health.evicted_runs,
    )
    for a in health.active_alerts:
        if a.kind.value == "FAILURE":
            timeline_log.info(
                "          活动告警[失败] run=%s 错误分类=%s 原因=%s",
                a.pipeline_run_id,
                a.error_class.value if a.error_class else None,
                a.reason,
            )
        else:
            timeline_log.info(
                "          活动告警[失新鲜] run=%s 基线=%s@%s 差=%ss 阈值=%ss 原因=%s",
                a.pipeline_run_id,
                a.basis,
                a.basis_time,
                a.age_seconds,
                a.threshold_seconds,
                a.reason,
            )

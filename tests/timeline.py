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

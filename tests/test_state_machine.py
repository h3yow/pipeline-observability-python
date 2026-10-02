"""状态推进：乱序拒绝、重复幂等、冲突结果原因可区分。"""

import logging

from pipeline_obs import (
    ErrorClass,
    EventType,
    IngestStatus,
    ObservabilityService,
    PipelineEvent,
    RejectReason,
    RetryPolicy,
)
from tests.timeline import log_event, log_result, log_snapshot

logger = logging.getLogger("timeline")


def _svc():
    return ObservabilityService(
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=1.0),
        clock=lambda: 1000.0,
    )


def _ev(run, eid, etype, version, attempt=1, ec=None, at=None):
    return PipelineEvent(
        run,
        eid,
        etype,
        occurred_at=float(at if at is not None else version),
        version=version,
        attempt=attempt,
        error_class=ec,
    )


def test_out_of_order_event_is_rejected_as_stale():
    svc = _svc()
    logger.info("==== 场景：乱序上报（先应用 v=2，迟到 v=1 必须被拒）====")
    events = [
        _ev("run-ooo", "e2", EventType.FAILED, 2, 1, ErrorClass.RETRYABLE),
        _ev("run-ooo", "e1", EventType.STARTED, 1, 1),  # 迟到
    ]
    results = []
    for i, e in enumerate(events, 1):
        log_event(i, e)
        r = svc.ingest(e)
        log_result(r)
        results.append(r)
    first, second = results
    assert first.status is IngestStatus.APPLIED
    assert second.status is IngestStatus.REJECTED
    assert second.reason is RejectReason.STALE_VERSION
    # 状态不回退、计数不增加
    snap = svc.status("run-ooo")
    log_snapshot(snap)
    assert snap.state.value == "FAILED_RETRYABLE"
    assert snap.applied_version == 2
    assert snap.failure_count == 1


def test_duplicate_event_is_idempotent():
    svc = _svc()
    logger.info("==== 场景：同一事件重复上报（event_id 相同，必须幂等不计数）====")
    e1 = _ev("run-dup", "e1", EventType.STARTED, 1, 1)
    e2 = _ev("run-dup", "e2", EventType.FAILED, 2, 1, ErrorClass.RETRYABLE)
    for seq, e in enumerate([e1, e2, e2, e2], 1):  # e2 连报三次
        log_event(seq, e)
        log_result(svc.ingest(e))
    snap = svc.status("run-dup")
    log_snapshot(snap)
    assert svc.ingest(e2).status is IngestStatus.DUPLICATE
    assert snap.failure_count == 1
    assert snap.applied_version == 2


def test_conflicting_outcomes_same_attempt_are_distinguishable():
    svc = _svc()
    logger.info("==== 场景：同一 attempt 先报成功后报失败（冲突结果必须拒绝）====")
    started = _ev("run-cf", "e1", EventType.STARTED, 1, 1)
    ok = _ev("run-cf", "e2", EventType.SUCCEEDED, 2, 1)
    bad = _ev("run-cf", "e3", EventType.FAILED, 3, 1, ErrorClass.RETRYABLE)
    for i, e in enumerate([started, ok, bad], 1):
        log_event(i, e)
        r = svc.ingest(e)
        log_result(r)
        if i == 3:
            assert r.status is IngestStatus.REJECTED
            assert r.reason is RejectReason.CONFLICTING_OUTCOME
    snap = svc.status("run-cf")
    log_snapshot(snap)
    # 结论以先到的成功为准，失败不计数
    assert snap.state.value == "SUCCEEDED"
    assert snap.failure_count == 0


def test_fatal_terminal_rejects_later_events():
    svc = _svc()
    logger.info("==== 场景：FATAL 终态后任何上报均被拒绝 ====")
    started = _ev("run-fatal", "e1", EventType.STARTED, 1, 1)
    fatal = _ev("run-fatal", "e2", EventType.FAILED, 2, 1, ErrorClass.FATAL)
    later = _ev("run-fatal", "e3", EventType.STARTED, 3, 2)
    for i, e in enumerate([started, fatal, later], 1):
        log_event(i, e)
        r = svc.ingest(e)
        log_result(r)
        if i == 3:
            assert r.status is IngestStatus.REJECTED
            assert r.reason is RejectReason.FATAL_ALREADY_TERMINAL
    snap = svc.status("run-fatal")
    log_snapshot(snap)
    assert snap.state.value == "FAILED_FATAL"
    assert snap.failure_count == 1

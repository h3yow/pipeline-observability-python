"""告警收敛：同一异常恢复前只告警一次；时效性判定依据可解释。"""

import logging

from pipeline_obs import (
    ErrorClass,
    EventType,
    ObservabilityService,
    PipelineEvent,
    RetryPolicy,
)
from tests.timeline import log_event, log_result, log_snapshot

logger = logging.getLogger("timeline")


def _svc(now=1000.0):
    t = {"v": now}

    def clock():
        return t["v"]

    svc = ObservabilityService(
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=1.0),
        clock=clock,
    )
    return svc, t


def _ev(run, eid, etype, version, attempt=1, ec=None):
    return PipelineEvent(
        run, eid, etype, occurred_at=float(version),
        version=version, attempt=attempt, error_class=ec,
    )


def test_single_alert_until_recovery_then_new_alert():
    svc, _t = _svc(now=10.0)
    logger.info("==== 场景：失败→多次重试失败（不重复告警）→恢复→再次失败（新告警）====")
    script = [
        _ev("run-alert", "e1", EventType.STARTED, 1, 1),
        _ev("run-alert", "e2", EventType.FAILED, 2, 1, ErrorClass.RETRYABLE),
        _ev("run-alert", "e3", EventType.STARTED, 3, 2),
        _ev("run-alert", "e4", EventType.FAILED, 4, 2, ErrorClass.RETRYABLE),
        _ev("run-alert", "e5", EventType.STARTED, 5, 3),
        _ev("run-alert", "e6", EventType.SUCCEEDED, 6, 3),  # 恢复
        _ev("run-alert", "e7", EventType.STARTED, 7, 1),
        _ev("run-alert", "e8", EventType.FAILED, 8, 1, ErrorClass.FATAL),  # 新异常
    ]
    for i, e in enumerate(script, 1):
        log_event(i, e)
        log_result(svc.ingest(e))
        snap = svc.status("run-alert")
        log_snapshot(snap)
        if i == 4:
            assert snap.alert_active is True
            assert snap.alert_reason and "收敛" in snap.alert_reason
    snap = svc.status("run-alert")
    # 恢复时关闭，第二次失败重新计数
    assert snap.alert_active is True
    assert snap.state.value == "FAILED_FATAL"


def test_alert_count_via_direct_state():
    from pipeline_obs import AlertState, PipelineState
    from pipeline_obs.alerting import evaluate_alert

    alert = AlertState()
    evaluate_alert(alert, state=PipelineState.FAILED_RETRYABLE, retry_exhausted=False, version=1)
    evaluate_alert(alert, state=PipelineState.FAILED_RETRYABLE, retry_exhausted=False, version=2)
    evaluate_alert(alert, state=PipelineState.FAILED_RETRYABLE, retry_exhausted=True, version=3)
    logger.info("[收敛计数] 连续 3 次失败态评估 raised_count=%d（预期 1）", alert.raised_count)
    assert alert.raised_count == 1
    evaluate_alert(alert, state=PipelineState.SUCCEEDED, retry_exhausted=False, version=4)
    evaluate_alert(alert, state=PipelineState.FAILED_FATAL, retry_exhausted=True, version=5)
    logger.info("[恢复后新异常] raised_count=%d（预期 2）", alert.raised_count)
    assert alert.raised_count == 2


def test_staleness_based_on_last_success():
    svc, t = _svc(now=100.0)
    svc.ingest(PipelineEvent(
        "run-stale", "e1", EventType.STARTED, occurred_at=100.0, version=1, attempt=1))
    t["v"] = 102.0
    svc.ingest(PipelineEvent(
        "run-stale", "e2", EventType.SUCCEEDED, occurred_at=102.0, version=2, attempt=1))
    t["v"] = 106.0
    stale, why = svc.staleness_report("run-stale", fresh_within_seconds=3.0)
    logger.info("[时效判定] %s -> stale=%s", why, stale)
    assert stale is True
    t["v"] = 103.0
    stale, why = svc.staleness_report("run-stale", fresh_within_seconds=3.0)
    logger.info("[时效判定] %s -> stale=%s", why, stale)
    assert stale is False

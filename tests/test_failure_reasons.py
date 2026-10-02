"""失败原因可见：运行明细/管道汇总/活动告警都带错误分类与信息；两类终止原因可区分。"""

import logging

from pipeline_obs import (
    ErrorClass,
    EventType,
    ObservabilityService,
    PipelineEvent,
    RetryPolicy,
)

logger = logging.getLogger("timeline")


def _ev(run, eid, etype, at, version, *, attempt=1, ec=None, msg=None, pid="p"):
    return PipelineEvent(run, eid, etype, occurred_at=at, version=version, attempt=attempt,
                         error_class=ec, error_message=msg, pipeline_id=pid)


def _svc(max_attempts=3):
    return ObservabilityService(
        retry_policy=RetryPolicy(max_attempts=max_attempts, base_delay_seconds=1.0),
        max_runs=None,
    )


def test_fatal_visible_everywhere_and_non_retryable_wording():
    """FATAL：错误分类/信息在运行明细、管道汇总、活动告警三处都可见，依据不写成重试耗尽。"""
    svc = _svc()
    pid = "pipe-fatal"
    svc.ingest(_ev("rf", "s", EventType.STARTED, 1.0, 1, pid=pid))
    svc.ingest(_ev("rf", "f", EventType.FAILED, 2.0, 2,
                   ec=ErrorClass.FATAL, msg="数据格式错误: 缺少字段 id", pid=pid))

    snap = svc.status("rf")
    logger.info("[运行明细] class=%s msg=%s terminal=%s", snap.last_error_class,
                snap.last_error_message, snap.failure_terminal_reason)
    assert snap.last_error_class == "FATAL"
    assert snap.last_error_message == "数据格式错误: 缺少字段 id"
    assert snap.last_failure_at == 2.0
    assert snap.failure_terminal_reason == "NON_RETRYABLE"
    # 依据必须明确“错误本身不可重试”，不能误导成“重试次数用尽”
    assert "不可重试" in snap.alert_reason or "NON_RETRYABLE" in snap.alert_reason
    assert "用尽" not in snap.alert_reason and "耗尽" not in snap.alert_reason

    h = svc.pipeline_health(pid)
    logger.info("[管道汇总] run=%s class=%s msg=%s terminal=%s", h.latest_failure_run_id,
                h.latest_error_class, h.latest_error_message, h.latest_failure_terminal_reason)
    assert h.latest_failure_run_id == "rf"
    assert h.latest_error_class == "FATAL"
    assert h.latest_error_message == "数据格式错误: 缺少字段 id"
    assert h.latest_failure_terminal_reason == "NON_RETRYABLE"
    assert h.latest_failure_at == 2.0

    alerts = svc.active_alerts(pid)
    assert len(alerts) == 1
    a = alerts[0]
    logger.info("[活动告警] %s | %s", a.error_message, a.reason)
    assert a.kind.value == "FAILURE"
    assert a.error_class == "FATAL"
    assert a.error_message == "数据格式错误: 缺少字段 id"
    assert a.failure_terminal_reason == "NON_RETRYABLE"


def test_retry_exhausted_distinguishable_from_non_retryable():
    """可重试错误用尽机会：terminal=RETRY_EXHAUSTED，与 FATAL 的 NON_RETRYABLE 明确区分。"""
    svc = _svc(max_attempts=2)
    pid = "pipe-exh"
    svc.ingest(_ev("rg", "f1", EventType.FAILED, 1.0, 1, attempt=1,
                   ec=ErrorClass.RETRYABLE, msg="上游超时 #1", pid=pid))
    svc.ingest(_ev("rg", "s2", EventType.STARTED, 1.5, 2, attempt=2, pid=pid))
    svc.ingest(_ev("rg", "f2", EventType.FAILED, 2.0, 3, attempt=2,
                   ec=ErrorClass.RETRYABLE, msg="上游超时 #2", pid=pid))

    snap = svc.status("rg")
    logger.info("[重试耗尽] state=%s terminal=%s msg=%s", snap.state.value,
                snap.failure_terminal_reason, snap.last_error_message)
    assert snap.state.value == "FAILED_RETRYABLE"
    assert snap.retry_exhausted is True
    assert snap.failure_terminal_reason == "RETRY_EXHAUSTED"
    assert snap.last_error_class == "RETRYABLE"
    assert snap.last_error_message == "上游超时 #2"  # 最近一次失败原因
    assert "用尽" in snap.alert_reason or "RETRY_EXHAUSTED" in snap.alert_reason

    h = svc.pipeline_health(pid)
    assert h.latest_failure_terminal_reason == "RETRY_EXHAUSTED"
    assert h.latest_error_message == "上游超时 #2"
    assert h.latest_error_class == "RETRYABLE"

    alerts = svc.active_alerts(pid, kind="FAILURE")
    assert alerts[0].failure_terminal_reason == "RETRY_EXHAUSTED"
    assert alerts[0].error_message == "上游超时 #2"


def test_retry_in_progress_terminal_reason_is_none():
    """可重试但仍有机会（等待重试中）：不给出终止原因，仅显示错误。"""
    svc = _svc(max_attempts=3)
    pid = "pipe-wait"
    svc.ingest(_ev("rw", "f1", EventType.FAILED, 1.0, 1, attempt=1,
                   ec=ErrorClass.RETRYABLE, msg="网络抖动", pid=pid))
    snap = svc.status("rw")
    assert snap.state.value == "FAILED_RETRYABLE"
    assert snap.retry_exhausted is False
    assert snap.failure_terminal_reason is None
    assert snap.last_error_message == "网络抖动"
    assert svc.pipeline_health(pid).latest_failure_terminal_reason is None


def test_error_cleared_after_recovery_and_never_failed_is_clean():
    """恢复成功后旧错误不得残留（运行/管道/告警三处）；从未失败也不带错误。"""
    svc = _svc(max_attempts=3)
    pid = "pipe-clear"
    svc.ingest(_ev("rc", "f1", EventType.FAILED, 1.0, 1, attempt=1,
                   ec=ErrorClass.RETRYABLE, msg="临时故障", pid=pid))
    svc.ingest(_ev("rc", "s2", EventType.STARTED, 1.5, 2, attempt=2, pid=pid))
    svc.ingest(_ev("rc", "ok", EventType.SUCCEEDED, 2.0, 3, attempt=2, pid=pid))

    snap = svc.status("rc")
    logger.info("[恢复后] class=%s msg=%s terminal=%s failure_count=%s",
                snap.last_error_class, snap.last_error_message,
                snap.failure_terminal_reason, snap.failure_count)
    assert snap.last_error_class is None
    assert snap.last_error_message is None
    assert snap.failure_terminal_reason is None
    assert snap.last_failure_at is None
    assert snap.failure_count == 1  # 历史失败计数保留，但“最近失败原因”已清空

    h = svc.pipeline_health(pid)
    assert h.latest_error_class is None
    assert h.latest_error_message is None
    assert h.latest_failure_terminal_reason is None
    assert h.latest_failure_run_id is None
    assert svc.active_alerts(pid) == ()

    # 从未失败的运行
    svc.ingest(_ev("ro", "s", EventType.STARTED, 3.0, 1, pid=pid))
    svc.ingest(_ev("ro", "ok", EventType.SUCCEEDED, 3.1, 2, pid=pid))
    clean = svc.status("ro")
    assert clean.last_error_class is None and clean.last_error_message is None


def test_pipeline_latest_failure_tracks_most_recent_across_runs():
    """管道“最近一次为什么挂”跨多运行取最近失败；某运行恢复后不被旧失败污染。"""
    svc = _svc()
    pid = "pipe-latest"
    svc.ingest(_ev("a", "f", EventType.FAILED, 1.0, 1,
                   ec=ErrorClass.FATAL, msg="A 配置缺失", pid=pid))
    svc.ingest(_ev("b", "f", EventType.FAILED, 2.0, 1,
                   ec=ErrorClass.RETRYABLE, msg="B 网络抖动", pid=pid))
    h = svc.pipeline_health(pid)
    assert h.latest_failure_run_id == "b"
    assert h.latest_error_message == "B 网络抖动"
    # b 随后重试成功：管道最近失败原因应回落到仍处于失败态的 a
    svc.ingest(_ev("b", "s", EventType.STARTED, 2.5, 2, attempt=2, pid=pid))
    svc.ingest(_ev("b", "ok", EventType.SUCCEEDED, 3.0, 3, attempt=2, pid=pid))
    h = svc.pipeline_health(pid)
    logger.info("[回落后] run=%s msg=%s", h.latest_failure_run_id, h.latest_error_message)
    assert h.latest_failure_run_id == "a"
    assert h.latest_error_message == "A 配置缺失"
    assert h.latest_failure_terminal_reason == "NON_RETRYABLE"

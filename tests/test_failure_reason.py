"""失败原因可见性：运行明细、管道汇总、活动告警都带最近一次失败的错误分类/信息；
依据能区分“错误本身不可重试（FATAL）”与“可重试错误已用尽机会”；
成功恢复或从未失败时不残留旧错误。
"""

import logging

from pipeline_obs import (
    ErrorClass,
    EventType,
    ObservabilityService,
    PipelineEvent,
    RetryPolicy,
)
from tests.timeline import log_health

logger = logging.getLogger("timeline")


def _svc(max_attempts=3):
    return ObservabilityService(
        retry_policy=RetryPolicy(max_attempts=max_attempts, base_delay_seconds=1.0),
        clock=lambda: 1000.0,
    )


def _ev(run, pid, eid, etype, version, *, attempt=1, ec=None, msg=None):
    return PipelineEvent(
        run,
        eid,
        etype,
        occurred_at=float(version),
        version=version,
        attempt=attempt,
        error_class=ec,
        error_message=msg,
        pipeline_id=pid,
    )


def test_fatal_vs_retry_exhausted_are_distinguishable_everywhere():
    svc = _svc(max_attempts=3)
    pid = "pipe-why"

    # rA：FATAL（不可重试），第一次尝试即终止
    svc.ingest(_ev("rA", pid, "a1", EventType.STARTED, 1))
    svc.ingest(
        _ev(
            "rA",
            pid,
            "a2",
            EventType.FAILED,
            2,
            ec=ErrorClass.FATAL,
            msg="配置缺失：缺少 schema 字段",
        )
    )
    # rB：RETRYABLE 连失 3 次用尽机会
    svc.ingest(_ev("rB", pid, "b1", EventType.STARTED, 3))
    svc.ingest(
        _ev(
            "rB",
            pid,
            "b2",
            EventType.FAILED,
            4,
            attempt=1,
            ec=ErrorClass.RETRYABLE,
            msg="上游超时",
        )
    )
    svc.ingest(_ev("rB", pid, "b3", EventType.STARTED, 5, attempt=2))
    svc.ingest(
        _ev(
            "rB",
            pid,
            "b4",
            EventType.FAILED,
            6,
            attempt=2,
            ec=ErrorClass.RETRYABLE,
            msg="上游超时",
        )
    )
    svc.ingest(_ev("rB", pid, "b5", EventType.STARTED, 7, attempt=3))
    svc.ingest(
        _ev(
            "rB",
            pid,
            "b6",
            EventType.FAILED,
            8,
            attempt=3,
            ec=ErrorClass.RETRYABLE,
            msg="连接重置",
        )
    )

    snap_a = svc.status("rA")
    snap_b = svc.status("rB")

    # 运行明细：错误分类/信息可见，终止原因两类可区分
    assert snap_a.last_error_class == "FATAL"
    assert snap_a.last_error_message == "配置缺失：缺少 schema 字段"
    assert snap_a.failure_termination == "FATAL"
    assert snap_b.last_error_class == "RETRYABLE"
    assert snap_b.last_error_message == "连接重置"  # 最近一次失败信息
    assert snap_b.failure_termination == "RETRY_EXHAUSTED"

    # 依据文本必须把两类原因讲清楚，且 FATAL 不得写成“重试耗尽”
    assert "不可重试" in snap_a.alert_reason and "FATAL" in snap_a.alert_reason
    assert "耗尽" not in snap_a.alert_reason or "用尽" not in snap_a.alert_reason
    assert "机会已用尽" in snap_b.alert_reason and "RETRYABLE" in snap_b.alert_reason
    logger.info("[FATAL 依据] %s", snap_a.alert_reason)
    logger.info("[耗尽依据] %s", snap_b.alert_reason)

    # 活动告警明细同样携带错误分类/信息/终止原因
    alerts = {a.pipeline_run_id: a for a in svc.active_alerts(pid, kind="FAILURE")}
    assert alerts["rA"].error_class == "FATAL"
    assert alerts["rA"].termination == "FATAL"
    assert "schema" in alerts["rA"].error_message
    assert alerts["rB"].error_class == "RETRYABLE"
    assert alerts["rB"].termination == "RETRY_EXHAUSTED"

    # 管道汇总：最近一次失败是 rB（时间更晚），可见其错误分类/信息
    h = svc.pipeline_health(pid)
    log_health(h, svc.active_alerts(pid))
    assert h.latest_failure_run_id == "rB"
    assert h.latest_failure_error_class == "RETRYABLE"
    assert h.latest_failure_error_message == "连接重置"
    assert h.latest_failure_termination == "RETRY_EXHAUSTED"
    assert h.latest_failure_at == 8.0


def test_latest_failure_tracks_recent_run_and_clears_on_recovery():
    """最近失败原因跟随“最近失败的仍失败运行”；某运行恢复成功后清空其旧错误。"""
    svc = _svc()
    pid = "pipe-clear"

    svc.ingest(_ev("r1", pid, "s", EventType.STARTED, 1))
    svc.ingest(
        _ev("r1", pid, "f", EventType.FAILED, 2, ec=ErrorClass.FATAL, msg="旧错误")
    )
    h = svc.pipeline_health(pid)
    assert h.latest_failure_run_id == "r1"
    assert h.latest_failure_error_message == "旧错误"

    # 更新的运行失败 → 最近失败切换到 r2
    svc.ingest(_ev("r2", pid, "s2", EventType.STARTED, 3))
    svc.ingest(
        _ev("r2", pid, "f2", EventType.FAILED, 4, ec=ErrorClass.RETRYABLE, msg="新错误")
    )
    h = svc.pipeline_health(pid)
    assert h.latest_failure_run_id == "r2"
    assert h.latest_failure_error_message == "新错误"

    # r2 重试后成功恢复：它的失败原因清空；r1 仍失败，最近失败回落到 r1
    svc.ingest(_ev("r2", pid, "s3", EventType.STARTED, 5, attempt=2))
    svc.ingest(_ev("r2", pid, "ok", EventType.SUCCEEDED, 6, attempt=2))
    snap2 = svc.status("r2")
    assert snap2.last_error_class is None
    assert snap2.last_error_message is None
    assert snap2.failure_termination == "NOT_TERMINATED"
    h = svc.pipeline_health(pid)
    log_health(h, svc.active_alerts(pid))
    assert h.latest_failure_run_id == "r1"
    assert h.latest_failure_error_message == "旧错误"
    assert not any(a.pipeline_run_id == "r2" for a in svc.active_alerts(pid))


def test_never_failed_and_recovered_have_no_stale_error():
    """从未失败：错误字段为空；失败后成功：旧错误不残留。"""
    svc = _svc()
    pid = "pipe-clean"
    svc.ingest(_ev("ok1", pid, "s", EventType.STARTED, 1))
    svc.ingest(_ev("ok1", pid, "k", EventType.SUCCEEDED, 2))
    snap = svc.status("ok1")
    assert snap.last_error_class is None
    assert snap.last_error_message is None
    assert snap.failure_termination == "NOT_TERMINATED"

    svc.ingest(_ev("ok2", pid, "s2", EventType.STARTED, 3))
    svc.ingest(
        _ev(
            "ok2",
            pid,
            "f2",
            EventType.FAILED,
            4,
            ec=ErrorClass.RETRYABLE,
            msg="临时抖动",
        )
    )
    assert svc.status("ok2").last_error_message == "临时抖动"
    svc.ingest(_ev("ok2", pid, "s3", EventType.STARTED, 5, attempt=2))
    svc.ingest(_ev("ok2", pid, "k2", EventType.SUCCEEDED, 6, attempt=2))
    snap = svc.status("ok2")
    assert snap.last_error_class is None and snap.last_error_message is None
    # 管道里两个运行都成功：最近失败原因整体清空
    h = svc.pipeline_health(pid)
    assert h.latest_failure_run_id is None
    assert h.latest_failure_error_class is None
    assert h.consecutive_failures == 0

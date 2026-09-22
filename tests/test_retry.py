"""重试规则测试：指数退避、次数上限、可重试/不可重试错误区分。"""
from __future__ import annotations

import pytest

from pipeline_obs import (
    ErrorCategory,
    ErrorInfo,
    EventType,
    ObservabilityService,
    PipelineEvent,
    RejectReason,
    RetryPolicy,
    RunStatus,
)
from pipeline_obs.clock import ManualClock

from .helpers import Timeline, logger


def make_event(eid, run, et, t, attempt_no=None, error=None, pipe="p"):
    return PipelineEvent(eid, pipe, run, et, t, attempt_no, error)


RETRYABLE = lambda: ErrorInfo("TIMEOUT", "transient timeout",
                              ErrorCategory.RETRYABLE)
FATAL = lambda: ErrorInfo("SCHEMA", "bad record schema", ErrorCategory.FATAL)


def test_retry_backoff_schedule_and_exhaustion():
    clk = ManualClock(1000.0)
    svc = ObservabilityService(clock=clk)
    svc.register_pipeline("p", max_retries=2, base_delay=10.0,
                          backoff_factor=2.0)
    tl = Timeline()
    seq = 0

    def post(ev):
        nonlocal seq
        seq += 1
        res = svc.ingest(ev)
        tl.record(seq, ev, res)
        return res

    post(make_event("e1", "r", EventType.RUN_STARTED, 1000.0))
    post(make_event("e2", "r", EventType.ATTEMPT_STARTED, 1000.0, 1))
    s = svc.get_run_summary("p", "r")

    # 第 1 次可重试失败：调度 1000+10=1010
    res = post(make_event("e3", "r", EventType.ATTEMPT_FAILED, 1000.0, 1,
                          RETRYABLE()))
    assert res.run_status is RunStatus.RUNNING
    assert svc.get_run_summary("p", "r").next_retry_at == pytest.approx(1010.0)

    # 退避未到不允许开始第 2 次尝试
    clk.advance(5)
    early = post(make_event("e4", "r", EventType.ATTEMPT_STARTED, 1005.0, 2))
    assert not early.accepted
    assert early.reason is RejectReason.RETRY_NOT_DUE
    assert svc.run_due_retries() == 0

    # 到点后调度器放行
    clk.advance(5)
    assert svc.run_due_retries() == 1
    assert post(make_event("e5", "r", EventType.ATTEMPT_STARTED, 1010.0, 2)
                ).accepted
    # 第 2 次失败：退避翻倍 1010+20=1030
    post(make_event("e6", "r", EventType.ATTEMPT_FAILED, 1010.0, 2,
                    RETRYABLE()))
    assert svc.get_run_summary("p", "r").next_retry_at == pytest.approx(1030.0)

    # 最后一次（第 3 次尝试）也失败 -> 达上限，终态 FAILED
    clk.advance(20)
    svc.run_due_retries()
    post(make_event("e7", "r", EventType.ATTEMPT_STARTED, 1030.0, 3))
    final = post(make_event("e8", "r", EventType.ATTEMPT_FAILED, 1030.0, 3,
                            RETRYABLE()))
    assert final.run_status is RunStatus.FAILED

    summary = svc.get_run_summary("p", "r")
    logger.info("判定依据：max_retries=2 => 共允许 1 次初始尝试 + 2 次重试；"
                "第 3 次失败后 failure_count=3 > 2，retry_exhausted=True")
    assert summary.retry_exhausted is True
    assert summary.failure_count == 3
    assert summary.status is RunStatus.FAILED
    assert summary.last_error_category is ErrorCategory.RETRYABLE
    assert summary.next_retry_at is None

    # 达上限后 run_due_retries 永不再调度该 run
    clk.advance(1000)
    assert svc.run_due_retries() == 0
    # 终态后再上报被拒（INVALID_TRANSITION，而非继续重试）
    after = post(make_event("e9", "r", EventType.ATTEMPT_STARTED, 2030.0, 4))
    assert after.reason is RejectReason.INVALID_TRANSITION

    tl.log("重试退避与次数上限")


def test_fatal_error_does_not_retry():
    svc = ObservabilityService()
    svc.register_pipeline("p", max_retries=3, base_delay=10.0)
    tl = Timeline()
    seq = 0

    def post(ev):
        nonlocal seq
        seq += 1
        res = svc.ingest(ev)
        tl.record(seq, ev, res)
        return res

    post(make_event("e1", "r", EventType.RUN_STARTED, 1000.0))
    post(make_event("e2", "r", EventType.ATTEMPT_STARTED, 1000.0, 1))
    final = post(make_event("e3", "r", EventType.ATTEMPT_FAILED, 1000.0, 1,
                            FATAL()))

    summary = svc.get_run_summary("p", "r")
    logger.info("判定依据：FATAL 错误 category=FATAL，不可重试，"
                "立即终结；retry_exhausted=False 以与耗尽场景区分")
    assert final.run_status is RunStatus.FAILED
    assert summary.retry_exhausted is False
    assert summary.failure_count == 1
    assert summary.last_error_category is ErrorCategory.FATAL
    assert summary.next_retry_at is None
    assert summary.active_alerts[0].detail.startswith("fatal")

    tl.log("不可重试错误立即终结")


def test_policy_retryable_classification():
    policy = RetryPolicy()
    assert policy.is_retryable(RETRYABLE()) is True
    assert policy.is_retryable(FATAL()) is False
    delays = [policy.next_delay(
        __import__("pipeline_obs", fromlist=["RetryState"]).RetryState(
            base_delay=1.0, backoff_factor=2.0), n) for n in (1, 2, 3)]
    assert delays == [1.0, 2.0, 4.0]

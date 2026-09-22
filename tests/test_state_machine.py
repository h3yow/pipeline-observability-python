"""状态机测试：重复/乱序/冲突上报的拒绝原因必须可区分。"""
from __future__ import annotations

from pipeline_obs import (
    ErrorCategory,
    ErrorInfo,
    EventType,
    ObservabilityService,
    PipelineEvent,
    RejectReason,
    RunStatus,
)

from .helpers import Timeline, logger


def ev(eid, run, et, t, no=None, err=None):
    return PipelineEvent(eid, "p", run, et, t, no, err)


RETRYABLE = ErrorInfo("E", "transient", ErrorCategory.RETRYABLE)


def _service():
    svc = ObservabilityService()
    svc.register_pipeline("p", max_retries=1, base_delay=0.0)
    return svc


def test_distinct_reject_reasons():
    svc = _service()
    tl = Timeline()
    # 用例按上报顺序排列，时间戳单调不减，避免被乱序规则先行拦截而掩盖
    # 真正要验证的拒绝原因。
    cases: list[tuple[str, PipelineEvent, RejectReason, str]] = []

    svc.ingest(ev("1", "r", EventType.RUN_STARTED, 100.0))
    svc.ingest(ev("2", "r", EventType.ATTEMPT_STARTED, 101.0, 1))

    # 同一 attempt 重复 start（此时 last_event_time=101）
    cases.append(("重复ATTEMPT_STARTED",
                  ev("dup-start", "r", EventType.ATTEMPT_STARTED, 101.5, 1),
                  RejectReason.DUPLICATE_ATTEMPT, "attempt 已 started"))
    # 乱序：事件时间早于当前最新事件时间（101.5 被拒，不推进时间）
    cases.append(("乱序事件",
                  ev("stale", "r", EventType.ATTEMPT_FAILED, 99.0, 1,
                     RETRYABLE),
                  RejectReason.STALE_EVENT, "event_time 回退"))
    # 正常失败 -> 调度重试（base_delay=0，立即可重试）
    cases.append(("正常失败以推进时间线",
                  ev("fail1", "r", EventType.ATTEMPT_FAILED, 103.0, 1,
                     RETRYABLE),
                  RejectReason.DUPLICATE_OUTCOME,  # 占位，下面单独处理
                  ""))
    # 序号跳跃：直接上报 attempt 99
    cases.append(("尝试序号跳跃",
                  ev("jump", "r", EventType.ATTEMPT_STARTED, 104.0, 99),
                  RejectReason.ATTEMPT_OUT_OF_SEQUENCE, "跳号"))
    # 重复结论：再次上报 attempt1 的失败（同一结论）
    cases.append(("重复结论",
                  ev("dup-fail", "r", EventType.ATTEMPT_FAILED, 105.0, 1,
                     RETRYABLE),
                  RejectReason.DUPLICATE_OUTCOME, "同一 attempt 相同结论"))
    # 冲突结论：attempt1 改报成功（相反结论）
    cases.append(("冲突结论",
                  ev("conflict", "r", EventType.ATTEMPT_SUCCEEDED, 106.0, 1),
                  RejectReason.CONFLICTING_OUTCOME, "同一 attempt 相反结论"))
    # 未 start 的 attempt 上报结果
    cases.append(("未知attempt",
                  ev("unknown", "r", EventType.ATTEMPT_SUCCEEDED, 107.0, 50),
                  RejectReason.UNKNOWN_ATTEMPT, "attempt 从未 started"))
    # 未知管道
    cases.append(("未知管道",
                  PipelineEvent("ghost-1", "ghost", "r",
                                EventType.RUN_STARTED, 108.0),
                  RejectReason.UNKNOWN_PIPELINE, "管道未登记"))

    seq = 0
    for name, event, expected, why in cases:
        seq += 1
        result = svc.ingest(event)
        tl.record(seq, event, result)
        if name == "正常失败以推进时间线":
            # 唯一应当被接受的推进事件
            assert result.accepted, result.detail
            svc.run_due_retries()
            logger.info("判定依据[%s]：接受可重试失败并调度重试 -> %s",
                        name, result.run_status.value)
            continue
        logger.info("判定依据[%s]：%s -> accepted=%s reason=%s（期望 %s）",
                    name, why, result.accepted,
                    result.reason.value if result.reason else "-",
                    expected.value)
        assert not result.accepted, name
        assert result.reason is expected, (
            f"{name}: got {result.reason}, want {expected} "
            f"({result.detail})"
        )

    # 被拒事件不得污染状态：只有 1 个 attempt、1 次失败计数
    summary = svc.get_run_summary("p", "r")
    assert summary.failure_count == 1
    assert summary.attempt_count == 1
    tl.log("重复/乱序/冲突上报的可区分拒绝原因")


def test_no_status_rollback_after_success():
    svc = _service()
    svc.ingest(ev("1", "r", EventType.RUN_STARTED, 100.0))
    svc.ingest(ev("2", "r", EventType.ATTEMPT_STARTED, 101.0, 1))
    ok = svc.ingest(ev("3", "r", EventType.ATTEMPT_SUCCEEDED, 102.0, 1))
    assert ok.run_status is RunStatus.SUCCEEDED

    # 终态后任何推进事件都被拒绝，状态不得回退
    late_fail = svc.ingest(ev("4", "r", EventType.ATTEMPT_FAILED, 103.0, 1,
                              RETRYABLE))
    # attempt1 已成功 -> 改报失败属于“冲突结论”（精确于通用终态错误）
    assert late_fail.reason is RejectReason.CONFLICTING_OUTCOME
    # 重复成功 -> “重复结论”
    dup_ok = svc.ingest(ev("4b", "r", EventType.ATTEMPT_SUCCEEDED, 103.5, 1))
    assert dup_ok.reason is RejectReason.DUPLICATE_OUTCOME
    # 开启新尝试 -> 通用终态非法流转
    restart = svc.ingest(ev("5", "r", EventType.ATTEMPT_STARTED, 104.0, 2))
    assert restart.reason is RejectReason.INVALID_TRANSITION
    assert svc.get_run_summary("p", "r").status is RunStatus.SUCCEEDED


def test_unknown_attempt_result_rejected_when_in_order_slot():
    svc = _service()
    svc.ingest(ev("1", "r", EventType.RUN_STARTED, 100.0))
    # 直接给 attempt 1 上报结果（从未 started）
    res = svc.ingest(ev("2", "r", EventType.ATTEMPT_SUCCEEDED, 101.0, 1))
    assert res.reason is RejectReason.UNKNOWN_ATTEMPT

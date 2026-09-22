"""事件驱动的运行状态机：去重、乱序拒绝、合法流转与计数。

可区分的拒绝原因见 :class:`~pipeline_obs.models.RejectReason`：

- 终态 run 再收到推进事件        -> INVALID_TRANSITION
- event_time 早于已观察的最新时间 -> STALE_EVENT
- 同一 attempt 重复 start         -> DUPLICATE_ATTEMPT
- 同一 attempt 重复上报同一结论   -> DUPLICATE_OUTCOME
- 同一 attempt 上报相反结论       -> CONFLICTING_OUTCOME
- 未 start 的 attempt 上报结果    -> UNKNOWN_ATTEMPT
- 尝试序号跳跃（未按序推进）      -> ATTEMPT_OUT_OF_SEQUENCE
- 耗尽后/终态后再开始新的尝试     -> INVALID_TRANSITION

状态流转：

    RUN_STARTED -> RUNNING
    ATTEMPT_STARTED(n)            （要求 n 与当前尝试序号一致）
    ATTEMPT_SUCCEEDED(n)          -> RUNNING.SUCCEEDED（终态）
    ATTEMPT_FAILED(n) + 可重试     -> RUNNING（调度下一次尝试）
    ATTEMPT_FAILED(n) + 耗尽/FATAL -> FAILED（终态）
    RUN_ABORTED                   -> FAILED（终态，FATAL，不重试）
"""
from __future__ import annotations

from dataclasses import dataclass

from .errors import EventRejectedError
from .models import (
    AttemptRecord,
    ErrorCategory,
    ErrorInfo,
    EventType,
    PipelineEvent,
    PipelineRecord,
    RejectReason,
    RetryState,
    RunRecord,
    RunStatus,
)
from .retry import RetryPolicy


@dataclass(frozen=True)
class Transition:
    """apply_event 的判定结论，供 service 层驱动告警等副作用。"""

    run_terminal: bool = False
    run_succeeded: bool = False
    run_failed: bool = False
    retry_scheduled: bool = False
    outcome_error: ErrorInfo | None = None


def _reject(reason: RejectReason, detail: str) -> None:
    raise EventRejectedError(reason, detail)


def ensure_run(pipeline: PipelineRecord, event: PipelineEvent) -> RunRecord:
    """事件首次到达时惰性创建运行实例并套用管道的重试配置。"""
    run = pipeline.runs.get(event.run_id)
    if run is None:
        run = RunRecord(
            pipeline_id=event.pipeline_id,
            run_id=event.run_id,
            retry=RetryState(
                max_retries=pipeline.max_retries,
                base_delay=pipeline.base_delay,
                backoff_factor=pipeline.backoff_factor,
            ),
        )
        pipeline.runs[event.run_id] = run
    return run


def _check_order(run: RunRecord, event: PipelineEvent) -> None:
    if (run.last_event_time is not None
            and event.event_time < run.last_event_time):
        _reject(
            RejectReason.STALE_EVENT,
            f"event_time={event.event_time} < last_event_time="
            f"{run.last_event_time}",
        )


def _terminal_check(run: RunRecord, event: PipelineEvent) -> None:
    if run.status is not RunStatus.RUNNING:
        _reject(
            RejectReason.INVALID_TRANSITION,
            f"run already {run.status.value}; cannot accept "
            f"{event.event_type.value}",
        )


def _require_attempt_no(event: PipelineEvent) -> int:
    if event.attempt_no is None:
        _reject(
            RejectReason.UNKNOWN_ATTEMPT,
            f"{event.event_type.value} requires attempt_no",
        )
    assert event.attempt_no is not None
    return event.attempt_no


def apply_event(pipeline: PipelineRecord, run: RunRecord,
                event: PipelineEvent,
                retry_policy: RetryPolicy | None = None) -> Transition:
    """就地推进 run 状态；非法/重复/乱序事件抛 EventRejectedError。"""
    policy = retry_policy or RetryPolicy()
    et = event.event_type

    # 统一先做乱序检查；终态检查在各分支内、attempt 语义检查之后进行，
    # 以便对“终态后重复/冲突上报”给出更精确、可区分的原因。
    _check_order(run, event)

    if et is EventType.RUN_STARTED:
        _terminal_check(run, event)
        if run.started_at is None:
            run.started_at = event.event_time
        run.last_event_time = event.event_time
        return Transition()

    if et is EventType.RUN_ABORTED:
        _terminal_check(run, event)
        err = event.error or ErrorInfo(
            code="RUN_ABORTED",
            message="run aborted without error detail",
            category=ErrorCategory.FATAL,
        )
        if err.category is not ErrorCategory.FATAL:
            # 外部中止一律视为不可重试；若上报了 RETRYABLE 也规范化为 FATAL。
            err = ErrorInfo(code=err.code, message=err.message,
                            category=ErrorCategory.FATAL)
        run.status = RunStatus.FAILED
        run.terminal_error = err
        run.finished_at = event.event_time
        run.last_event_time = event.event_time
        return Transition(run_terminal=True, run_failed=True,
                          outcome_error=err)

    # 以下事件都必须带 attempt_no
    attempt_no = _require_attempt_no(event)

    if et is EventType.ATTEMPT_STARTED:
        existing = run.attempts.get(attempt_no)
        if existing is not None and existing.started_at is not None:
            # 重复 start 优先于终态判断，原因更具体。
            _reject(
                RejectReason.DUPLICATE_ATTEMPT,
                f"attempt {attempt_no} already started",
            )
        # 终态 run 不允许开启新尝试（含重试耗尽后的继续上报）。
        _terminal_check(run, event)
        # 新的一次尝试必须是“当前期望的序号” = 已失败次数 + 1。
        expected = (run.retry.failure_count if run.retry else 0) + 1
        if attempt_no < expected:
            _reject(
                RejectReason.DUPLICATE_ATTEMPT,
                f"attempt {attempt_no} is older than expected {expected}",
            )
        if attempt_no > expected:
            _reject(
                RejectReason.ATTEMPT_OUT_OF_SEQUENCE,
                f"attempt {attempt_no} jumps ahead of expected {expected}",
            )
        if existing is None:
            existing = AttemptRecord(attempt_no=attempt_no)
            run.attempts[attempt_no] = existing
        existing.started_at = event.event_time
        run.retry_open = False
        run.last_event_time = event.event_time
        return Transition()

    # 结果事件（成功/失败）
    existing = run.attempts.get(attempt_no)
    if existing is None or existing.started_at is None:
        _reject(
            RejectReason.UNKNOWN_ATTEMPT,
            f"attempt {attempt_no} was never started",
        )
    if existing.succeeded is not None:
        same = (
            (existing.succeeded and et is EventType.ATTEMPT_SUCCEEDED)
            or (not existing.succeeded and et is EventType.ATTEMPT_FAILED)
        )
        reason = (
            RejectReason.DUPLICATE_OUTCOME if same
            else RejectReason.CONFLICTING_OUTCOME
        )
        _reject(
            reason,
            f"attempt {attempt_no} already recorded outcome "
            f"succeeded={existing.succeeded}",
        )

    # 不是重复/冲突上报后，终态 run 再上报新结果仍属非法流转。
    _terminal_check(run, event)

    existing.finished_at = event.event_time
    run.last_event_time = event.event_time

    if et is EventType.ATTEMPT_SUCCEEDED:
        existing.succeeded = True
        run.status = RunStatus.SUCCEEDED
        run.succeeded_at = event.event_time
        run.finished_at = event.event_time
        if run.retry is not None:
            run.retry.next_retry_at = None
        return Transition(run_terminal=True, run_succeeded=True)

    # ATTEMPT_FAILED
    if event.error is None:
        _reject(
            RejectReason.INVALID_TRANSITION,
            "ATTEMPT_FAILED requires error info",
        )
    assert event.error is not None
    existing.succeeded = False
    existing.error = event.error
    scheduled = policy.register_failure(run, event.error, event.event_time)
    if scheduled:
        return Transition(retry_scheduled=True, outcome_error=event.error)
    return Transition(run_terminal=True, run_failed=True,
                      outcome_error=event.error)

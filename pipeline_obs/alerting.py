"""告警收敛：同一异常在恢复前不重复告警。"""

from __future__ import annotations

from .models import AlertState, PipelineState

_FAIL_STATES = frozenset(
    {PipelineState.FAILED_RETRYABLE, PipelineState.FAILED_FATAL}
)


def evaluate_alert(
    alert: AlertState,
    *,
    state: PipelineState,
    retry_exhausted: bool,
    version: int,
) -> AlertState:
    """根据最新状态评估告警，原地更新并返回告警状态。

    规则（同一异常恢复前不重复产生告警）：
    - 进入失败态（FAILED_RETRYABLE / FAILED_FATAL）且当前无活动告警：
      触发一次告警并计数；失败原因中体现是否已重试耗尽。
    - 已存在活动告警且仍在失败态：收敛，不重复计数，仅刷新依据描述。
    - 恢复到 SUCCEEDED：关闭活动告警，之后再次失败才算新异常。
    - PENDING / RUNNING：不改变告警（运行中的实例不产生失败告警）。
    """
    if state in _FAIL_STATES:
        reason = (
            f"管道进入 {state.value}（version={version}，"
            + ("重试已耗尽" if retry_exhausted else "等待/进行重试")
            + "）"
        )
        if not alert.active:
            alert.active = True
            alert.raised_count += 1
            alert.last_raised_version = version
            alert.reason = f"新异常触发告警：{reason}"
        else:
            # 同一异常仍未恢复：收敛，不重复计数
            alert.reason = f"活动告警收敛中（不重复告警）：{reason}"
    elif state is PipelineState.SUCCEEDED:
        if alert.active:
            alert.active = False
            alert.last_recovered_version = version
            alert.reason = f"异常已恢复（version={version}），活动告警关闭"
        else:
            alert.reason = None
    # PENDING / RUNNING：保持既有告警状态不变
    return alert


def staleness(
    *,
    last_success_at: float | None,
    started_at: float | None,
    now: float,
    fresh_within_seconds: float,
) -> tuple[bool, str]:
    """基于最近成功时间判定时效性。

    基线选择：优先使用 last_success_at（最近成功时间）；
    从未成功过的实例回退到 started_at（首次事件时间）。
    返回 (是否失新鲜, 判定依据说明)。
    """
    if fresh_within_seconds <= 0:
        raise ValueError("fresh_within_seconds 必须为正数")

    if last_success_at is not None:
        age = now - last_success_at
        stale = age > fresh_within_seconds
        basis = (
            f"基于最近成功时间 last_success_at={last_success_at:.3f}，"
            f"距今 {age:.3f}s，阈值 {fresh_within_seconds:.3f}s"
        )
        return stale, (
            basis + "，超过阈值判定失新鲜" if stale else basis + "，仍在新鲜窗口内"
        )

    if started_at is not None:
        age = now - started_at
        stale = age > fresh_within_seconds
        basis = (
            f"从未成功，回退基线 started_at={started_at:.3f}，距今 {age:.3f}s，"
            f"阈值 {fresh_within_seconds:.3f}s"
        )
        return stale, (
            basis + "，超过阈值判定失新鲜" if stale else basis + "，仍在新鲜窗口内"
        )

    return False, "尚无任何事件时间，无法判定失新鲜，默认视为新鲜"

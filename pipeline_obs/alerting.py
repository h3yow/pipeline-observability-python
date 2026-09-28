"""告警收敛：同一异常在恢复前不重复告警；失败告警与失新鲜告警分开。"""

from __future__ import annotations

from .models import (
    AlertState,
    PipelineState,
    StalenessAlertState,
    StalenessBaseline,
    StalenessVerdict,
)

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


def evaluate_staleness(
    state: StalenessAlertState,
    *,
    last_success_at: float | None,
    started_at: float | None,
    now: float,
    fresh_within_seconds: float,
) -> StalenessVerdict:
    """执行一次确定性失新鲜巡检，原地更新告警收敛状态并返回判定结论。

    判定基线（口径）：
    1. 有最近成功时间 → 以 last_success_at 为基线；
    2. 从未成功过 → 回退到 started_at（第一次上报时间）；
    3. 连任何事件都没有 → 无法判定，默认视为新鲜（不产生告警）。

    告警收敛（同一段失新鲜只告警一次、只计一次）：
    - 由新鲜跨入失新鲜：开启新段落（episode_id +1、raised_count +1、
      记录 raised_at=now）；
    - 持续失新鲜：保持活动，不重复计数，仅刷新判定依据；
    - 恢复新鲜（重新成功或时间回到窗口内）：关闭活动告警，记录恢复时刻；
      再次失新鲜才算新段落，才允许再计一次。

    结论 StalenessVerdict 中带基线类型、基线时间、时间差、阈值与结论文本，
    供汇总/明细直接展示“用的哪个基线、差了多少、阈值多少、结论是什么”。
    """
    if fresh_within_seconds <= 0:
        raise ValueError("fresh_within_seconds 必须为正数")

    if last_success_at is not None:
        age = now - last_success_at
        stale = age > fresh_within_seconds
        basis = (
            f"基线=最近成功时间 LAST_SUCCESS(last_success_at={last_success_at:.3f})，"
            f"距今 {age:.3f}s，阈值 {fresh_within_seconds:.3f}s"
        )
        verdict = StalenessVerdict(
            stale=stale,
            baseline_kind=StalenessBaseline.LAST_SUCCESS,
            baseline_at=last_success_at,
            now=now,
            threshold_seconds=fresh_within_seconds,
            age_seconds=age,
            reason=basis + ("，超过阈值判定失新鲜" if stale else "，仍在新鲜窗口内"),
        )
    elif started_at is not None:
        age = now - started_at
        stale = age > fresh_within_seconds
        basis = (
            f"基线=第一次上报时间 FIRST_REPORT(started_at={started_at:.3f}，"
            f"从未成功)，距今 {age:.3f}s，阈值 {fresh_within_seconds:.3f}s"
        )
        verdict = StalenessVerdict(
            stale=stale,
            baseline_kind=StalenessBaseline.FIRST_REPORT,
            baseline_at=started_at,
            now=now,
            threshold_seconds=fresh_within_seconds,
            age_seconds=age,
            reason=basis + ("，超过阈值判定失新鲜" if stale else "，仍在新鲜窗口内"),
        )
    else:
        verdict = StalenessVerdict(
            stale=False,
            baseline_kind=StalenessBaseline.NONE,
            baseline_at=None,
            now=now,
            threshold_seconds=fresh_within_seconds,
            age_seconds=None,
            reason="基线=NONE（尚无任何事件时间），无法判定失新鲜，默认视为新鲜",
        )

    state.last_checked_at = now
    state.last_verdict = verdict

    if verdict.stale:
        if not state.active:
            # 新鲜 → 失新鲜：开启新的一段失新鲜
            state.active = True
            state.episode_id += 1
            state.raised_count += 1
            state.raised_at = now
        # 持续失新鲜：收敛，不重复计数（依据已随 last_verdict 刷新）
    else:
        if state.active:
            # 失新鲜 → 恢复新鲜：关闭本段
            state.active = False
            state.raised_at = None
            state.last_recovered_at = now

    return verdict


def staleness(
    *,
    last_success_at: float | None,
    started_at: float | None,
    now: float,
    fresh_within_seconds: float,
) -> tuple[bool, str]:
    """基于最近成功时间判定时效性（向后兼容的无状态便捷函数）。

    基线选择：优先使用 last_success_at（最近成功时间）；
    从未成功过的实例回退到 started_at（首次事件时间）。
    返回 (是否失新鲜, 判定依据说明)。需要巡检告警收敛请用 evaluate_staleness。
    """
    verdict = evaluate_staleness(
        StalenessAlertState(),
        last_success_at=last_success_at,
        started_at=started_at,
        now=now,
        fresh_within_seconds=fresh_within_seconds,
    )
    return verdict.stale, verdict.reason

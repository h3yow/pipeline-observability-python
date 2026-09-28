"""告警收敛：同一异常在恢复前不重复告警；失新鲜判定与失新鲜告警收敛。"""

from __future__ import annotations

from .models import AlertState, PipelineState, StalenessAlertState, StalenessAssessment

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


def assess_staleness(
    *,
    last_success_at: float | None,
    started_at: float | None,
    now: float,
    fresh_within_seconds: float,
) -> StalenessAssessment:
    """结构化失新鲜判定：返回基线类型、基线时间、时间差、阈值与结论文本。

    基线选择（口径）：
    1. 优先 last_success_at（最近成功时间），basis=LAST_SUCCESS；
    2. 从未成功过回退 started_at（首次上报时间），basis=FIRST_REPORT；
    3. 两者皆无（尚未有任何事件）：无法判定，默认视为新鲜，basis=NONE。
    判定为严格大于：age > threshold 才失新鲜，等于阈值仍属新鲜。
    """
    if fresh_within_seconds <= 0:
        raise ValueError("fresh_within_seconds 必须为正数")

    if last_success_at is not None:
        age = now - last_success_at
        stale = age > fresh_within_seconds
        conclusion = "超过阈值判定失新鲜" if stale else "仍在新鲜窗口内"
        reason = (
            f"基于最近成功时间 last_success_at={last_success_at:.3f}，"
            f"距今 {age:.3f}s，阈值 {fresh_within_seconds:.3f}s，{conclusion}"
        )
        return StalenessAssessment(
            stale=stale,
            basis="LAST_SUCCESS",
            basis_time=last_success_at,
            now=now,
            age_seconds=age,
            threshold_seconds=fresh_within_seconds,
            reason=reason,
        )

    if started_at is not None:
        age = now - started_at
        stale = age > fresh_within_seconds
        conclusion = "超过阈值判定失新鲜" if stale else "仍在新鲜窗口内"
        reason = (
            f"从未成功，回退基线 started_at={started_at:.3f}，"
            f"距今 {age:.3f}s，阈值 {fresh_within_seconds:.3f}s，{conclusion}"
        )
        return StalenessAssessment(
            stale=stale,
            basis="FIRST_REPORT",
            basis_time=started_at,
            now=now,
            age_seconds=age,
            threshold_seconds=fresh_within_seconds,
            reason=reason,
        )

    return StalenessAssessment(
        stale=False,
        basis="NONE",
        basis_time=None,
        now=now,
        age_seconds=None,
        threshold_seconds=fresh_within_seconds,
        reason="尚无任何事件时间，无法判定失新鲜，默认视为新鲜",
    )


def evaluate_staleness_alert(
    alert: StalenessAlertState,
    assessment: StalenessAssessment,
) -> StalenessAlertState:
    """根据一次失新鲜判定收敛失新鲜告警，原地更新并返回。

    规则（与失败告警完全独立，同一段失新鲜期间只告警一次、只计一次）：
    - 判定失新鲜且当前无活动失新鲜告警：触发一次，raised_count += 1，
      记录触发巡检时间、基线类型/时间、时间差、阈值与结论；
    - 判定失新鲜但活动告警仍在：收敛，不重复计数，仅刷新依据描述；
    - 判定恢复新鲜（含成功事件触发的即时恢复）：关闭活动告警，
      记录恢复时间；之后再次失新鲜才算新的一段，才允许再次计数。
    """
    if assessment.stale:
        if not alert.active:
            alert.active = True
            alert.raised_count += 1
            alert.raised_at = assessment.now
            alert.basis = assessment.basis
            alert.basis_time = assessment.basis_time
            alert.threshold_seconds = assessment.threshold_seconds
            alert.age_seconds = assessment.age_seconds
            alert.reason = f"失新鲜告警触发：{assessment.reason}"
        else:
            # 同一段失新鲜期间的重复巡检：收敛，不重复告警/计数
            alert.age_seconds = assessment.age_seconds
            alert.reason = f"失新鲜告警持续中（不重复告警）：{assessment.reason}"
    else:
        if alert.active:
            alert.active = False
            alert.last_recovered_at = assessment.now
            alert.age_seconds = assessment.age_seconds
            alert.reason = f"恢复新鲜（{assessment.reason}），失新鲜告警关闭"
    return alert


def recover_staleness_alert_on_success(
    alert: StalenessAlertState,
    *,
    last_success_at: float,
) -> StalenessAlertState:
    """成功事件推进后即时恢复失新鲜告警（无需等待下一次巡检）。

    以最近成功时间为基线构造“必然新鲜”的判定（age=0），复用统一收敛逻辑，
    保证“同一段只计一次、恢复后再次失新鲜可再计一次”的口径一致。
    """
    threshold = alert.threshold_seconds if alert.threshold_seconds is not None else 0.0
    if threshold <= 0:
        # 未配置过阈值（从未巡检）：成功天然新鲜，直接关闭，保留计数字段
        if alert.active:
            alert.active = False
            alert.last_recovered_at = last_success_at
            alert.reason = (
                f"成功事件（last_success_at={last_success_at:.3f}）即时恢复新鲜，"
                "失新鲜告警关闭"
            )
        return alert
    assessment = assess_staleness(
        last_success_at=last_success_at,
        started_at=last_success_at,
        now=last_success_at,
        fresh_within_seconds=threshold,
    )
    return evaluate_staleness_alert(alert, assessment)

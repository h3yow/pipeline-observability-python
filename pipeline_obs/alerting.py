"""告警收敛：同一异常在恢复前不重复告警；失败告警与失新鲜告警分开。"""

from __future__ import annotations

from .models import (
    AlertState,
    ErrorClass,
    FailureTerminalReason,
    PipelineState,
    StalenessAlertState,
    StalenessBaseline,
    StalenessVerdict,
)

_FAIL_STATES = frozenset(
    {PipelineState.FAILED_RETRYABLE, PipelineState.FAILED_FATAL}
)


def _failure_phrase(
    state: PipelineState,
    retry_exhausted: bool,
    error_class: ErrorClass | None,
    error_message: str | None,
) -> tuple[str, FailureTerminalReason | None]:
    """构造失败态描述，并给出可区分的终止原因（仍在重试中则为 None）。

    关键区分：错误本身不可重试（FATAL，一次即终态）与
    可重试错误已用尽机会（RETRYABLE + retry_exhausted）是两回事，
    不能把 FATAL 写成“重试已耗尽”，以免误导处置。
    """
    message = f"，错误信息={error_message}" if error_message else ""
    if state is PipelineState.FAILED_FATAL:
        ec = (error_class or ErrorClass.FATAL).value
        phrase = (
            f"错误分类={ec}，错误本身不可重试（NON_RETRYABLE），立即终止{message}"
        )
        return phrase, FailureTerminalReason.NON_RETRYABLE
    # FAILED_RETRYABLE
    ec = (error_class or ErrorClass.RETRYABLE).value
    if retry_exhausted:
        phrase = (
            f"错误分类={ec}，可重试错误已用尽重试机会（RETRY_EXHAUSTED）{message}"
        )
        return phrase, FailureTerminalReason.RETRY_EXHAUSTED
    phrase = f"错误分类={ec}，等待/进行重试{message}"
    return phrase, None


def evaluate_alert(
    alert: AlertState,
    *,
    state: PipelineState,
    retry_exhausted: bool,
    version: int,
    error_class: ErrorClass | None = None,
    error_message: str | None = None,
) -> AlertState:
    """根据最新状态评估告警，原地更新并返回告警状态。

    规则（同一异常恢复前不重复产生告警）：
    - 进入失败态（FAILED_RETRYABLE / FAILED_FATAL）且当前无活动告警：
      触发一次告警并计数；失败依据中体现错误分类/错误信息，并区分
      “错误本身不可重试（NON_RETRYABLE）”与“可重试错误已用尽机会
      （RETRY_EXHAUSTED）”两种终止原因。
    - 已存在活动告警且仍在失败态：收敛，不重复计数，仅刷新依据描述。
    - 恢复到 SUCCEEDED：关闭活动告警，之后再次失败才算新异常。
    - PENDING / RUNNING：不改变告警（运行中的实例不产生失败告警）。
    """
    if state in _FAIL_STATES:
        phrase, _terminal = _failure_phrase(
            state, retry_exhausted, error_class, error_message
        )
        reason = f"管道进入 {state.value}（version={version}，{phrase}）"
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

    # 时间契约：巡检时刻必须单调不减。拿更早的旧时刻重判一个已经推进到更晚
    # 基线的实例，可能把已被更新成功覆盖的实例重新判成失新鲜（旧结论回潮），
    # 因此明确拒绝乱序巡检；需要重放历史请使用独立的服务实例。
    if state.last_checked_at is not None and now < state.last_checked_at:
        raise ValueError(
            f"巡检时刻 now={now} 早于上次巡检时刻 {state.last_checked_at}，"
            "拒绝乱序巡检（时刻必须单调不减）"
        )

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
    state.last_threshold_seconds = fresh_within_seconds

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
            # 失新鲜 → 恢复新鲜：关闭本段，并把判定依据刷新为同版的“新鲜”结论。
            # 活动标志与判定依据必须在同一临界区一起更新，杜绝“告警已关、
            # 依据仍停留在失新鲜那一版”的自相矛盾快照。
            state.active = False
            state.raised_at = None
            state.last_recovered_at = now
        # 无论本段是否刚恢复：当前为新鲜，依据与活动标志保持同一版结论，
        # 旧的失新鲜依据不允许回潮。
        state.last_recovery_reason = verdict.reason
        state.last_verdict = verdict

    return verdict


def build_success_recovery_verdict(
    state: StalenessAlertState,
    *,
    success_at: float,
) -> StalenessVerdict | None:
    """成功事件即时恢复失新鲜时，构造与恢复结论同版的新鲜判定依据。

    成功在写入临界区内立即把基线刷新为 last_success_at=success_at，
    因此此刻 age=0、必然落在新鲜窗口内（沿用最近一次巡检阈值；从未巡检过
    则无法给出阈值口径，返回 None，调用方只关闭告警即可）。

    与巡检恢复一样：活动标志关闭、恢复时刻与判定依据一起更新，
    保证恢复后明细给出的是“恢复/新鲜”结论而非旧的失新鲜依据。
    成功时刻早于上次巡检时刻属于乱序时间线（与事件 version 检查配套），拒绝。
    """
    if (
        state.last_checked_at is not None
        and success_at < state.last_checked_at
    ):
        raise ValueError(
            f"成功事件时刻 {success_at} 早于上次巡检时刻 "
            f"{state.last_checked_at}，拒绝乱序时间线"
        )
    threshold = state.last_threshold_seconds
    if threshold is None:
        # 从未巡检过：没有可复用的阈值口径，无需产出判定结论。
        return None
    age = 0.0
    reason = (
        f"成功即时恢复新鲜：基线=最近成功时间 LAST_SUCCESS"
        f"(last_success_at={success_at:.3f})，恢复时刻={success_at:.3f}，"
        f"距今 {age:.3f}s，阈值 {threshold:.3f}s，在新鲜窗口内；"
        f"失新鲜告警已关闭（此前共 {state.raised_count} 段告警）"
    )
    verdict = StalenessVerdict(
        stale=False,
        baseline_kind=StalenessBaseline.LAST_SUCCESS,
        baseline_at=success_at,
        now=success_at,
        threshold_seconds=threshold,
        age_seconds=age,
        reason=reason,
    )
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

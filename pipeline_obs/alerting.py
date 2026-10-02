"""告警收敛：同一异常在恢复前不重复告警；失败告警与失新鲜告警分开。"""

from __future__ import annotations

from .models import (
    AlertState,
    ErrorClass,
    FailureTermination,
    PipelineState,
    StalenessAlertState,
    StalenessBaseline,
    StalenessVerdict,
)

_FAIL_STATES = frozenset({PipelineState.FAILED_RETRYABLE, PipelineState.FAILED_FATAL})


def _termination_kind(
    state: PipelineState, retry_exhausted: bool
) -> FailureTermination:
    """把“最终为什么不再重试”归一为两类可区分结论。

    - FAILED_FATAL：错误本身不可重试（FATAL）；
    - FAILED_RETRYABLE + retry_exhausted：错误可重试，但机会已用尽。
    两者绝不能混写成同一个“重试耗尽”。
    """
    if state is PipelineState.FAILED_FATAL:
        return FailureTermination.FATAL
    if state is PipelineState.FAILED_RETRYABLE and retry_exhausted:
        return FailureTermination.RETRY_EXHAUSTED
    return FailureTermination.NOT_TERMINATED


def _failure_clause(
    state: PipelineState,
    retry_exhausted: bool,
    error_class: ErrorClass | str | None,
    error_message: str | None,
) -> str:
    """构造失败依据中的“状态 + 终止原因 + 错误分类/信息”片段。"""
    termination = _termination_kind(state, retry_exhausted)
    if termination is FailureTermination.FATAL:
        ending = "错误本身不可重试（FATAL），立即终止，不再调度重试"
    elif termination is FailureTermination.RETRY_EXHAUSTED:
        ending = "错误可重试（RETRYABLE），但重试机会已用尽，停止重试"
    else:
        ending = "错误可重试（RETRYABLE），等待/进行重试"

    ec_value = error_class.value if isinstance(error_class, ErrorClass) else error_class
    detail_bits = []
    if ec_value:
        detail_bits.append(f"错误分类={ec_value}")
    if error_message:
        detail_bits.append(f"错误信息={error_message}")
    extra = f"（{'，'.join(detail_bits)}）" if detail_bits else ""
    return f"管道进入 {state.value}：{ending}{extra}"


def evaluate_alert(
    alert: AlertState,
    *,
    state: PipelineState,
    retry_exhausted: bool,
    version: int,
    error_class: ErrorClass | str | None = None,
    error_message: str | None = None,
) -> AlertState:
    """根据最新状态评估告警，原地更新并返回告警状态。

    规则（同一异常恢复前不重复产生告警）：
    - 进入失败态（FAILED_RETRYABLE / FAILED_FATAL）且当前无活动告警：
      触发一次告警并计数；依据中明确区分两类终止原因——
      “错误本身不可重试（FATAL）”与“可重试错误已用尽重试机会”，
      并带上最近一次失败的错误分类与错误信息。
    - 已存在活动告警且仍在失败态：收敛，不重复计数，仅刷新依据描述。
    - 恢复到 SUCCEEDED：关闭活动告警，之后再次失败才算新异常。
    - PENDING / RUNNING：不改变告警（运行中的实例不产生失败告警）。

    error_class/error_message 为可选追加参数，旧调用方式保持兼容。
    """
    if state in _FAIL_STATES:
        clause = _failure_clause(state, retry_exhausted, error_class, error_message)
        reason = f"{clause}（version={version}）"
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

    # 时间契约：巡检时刻必须单调不减。拿更早的旧时刻再判一次属于乱序巡检，
    # 原样返回上一次结论，不刷新任何字段、不开关告警、不计数——
    # 避免已恢复的实例被旧时刻重新判成失新鲜（旧结论回潮）。
    if (
        state.last_checked_at is not None
        and now < state.last_checked_at
        and state.last_verdict is not None
    ):
        return state.last_verdict

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
    state.last_threshold_seconds = fresh_within_seconds
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


def recover_staleness_on_success(
    state: StalenessAlertState,
    *,
    recovered_at: float,
    threshold_seconds: float | None,
) -> StalenessVerdict | None:
    """成功事件即时恢复失新鲜：活动状态与判定依据必须在同一临界区一起更新。

    背景：只关闭活动标志而保留旧的“失新鲜”判定依据，会让同一份明细自相矛盾
    （告警已关、结论还停在失新鲜），看板无法判断到底恢复没有。因此这里：
    - 关闭活动告警、清空 raised_at、记录恢复时刻（仅当本段此前确实活动）；
    - 无论此前是否活动，都把 last_verdict 刷新为“恢复/新鲜”结论，
      依据中带恢复时刻；这样恢复后任何查询读到的结论都是新鲜，且
      同一段失新鲜结束后旧结论永不回潮。

    threshold_seconds 缺省时无法构造带阈值的判定依据，仅做状态关闭，
    返回 None（成功恢复不依赖阈值本身，新鲜性由成功事件保证）。
    """
    was_active = state.active
    if was_active:
        state.active = False
        state.raised_at = None
        state.last_recovered_at = recovered_at

    if threshold_seconds is None:
        return None

    verdict = StalenessVerdict(
        stale=False,
        baseline_kind=StalenessBaseline.LAST_SUCCESS,
        baseline_at=recovered_at,
        now=recovered_at,
        threshold_seconds=threshold_seconds,
        age_seconds=0.0,
        reason=(
            f"收到成功事件，运行于 t={recovered_at:.3f} 恢复新鲜"
            + ("（本段失新鲜告警同步关闭）" if was_active else "（无活动失新鲜告警）")
            + f"；基线切换为最近成功时间 LAST_SUCCESS({recovered_at:.3f})，"
            f"距今 0.000s，阈值 {threshold_seconds:.3f}s，在新鲜窗口内，判定新鲜"
        ),
    )
    state.last_checked_at = recovered_at
    state.last_verdict = verdict
    return verdict

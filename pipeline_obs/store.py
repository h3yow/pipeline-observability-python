"""管道运行状态存储：线程安全的状态机推进、计数与汇总。

设计要点：
- 每个 pipeline_run_id 独立锁，串行化同一实例的更新，杜绝丢失更新。
- 基于 version 的乱序检测：低版本事件被拒绝（STALE_VERSION）。
- 基于 event_id 的去重：重复事件幂等忽略（DUPLICATE），不重复计数。
- 非法状态流转 / 终态后上报 / 同 attempt 冲突结果均被拒绝并给出可区分原因。
- 汇总快照在锁内一次性构造，查询方不会观察到中间态。
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field

from .alerting import evaluate_alert, evaluate_staleness
from .models import (
    ActiveAlertView,
    AlertKind,
    AlertState,
    ErrorClass,
    EventType,
    IngestStatus,
    PipelineEvent,
    PipelineState,
    PipelineStatus,
    RejectReason,
    RetryDecision,
    RetryPolicy,
    StalenessAlertState,
    StalenessVerdict,
)
from .pipeline import PipelineRegistry
from .retry import decide_retry


@dataclass
class _RunRecord:
    """单个管道运行实例的内部可变状态。仅在持锁时访问。"""

    pipeline_run_id: str
    state: PipelineState = PipelineState.PENDING
    current_attempt: int = 1
    applied_version: int = -1
    started_at: float | None = None
    seen_event_ids: set[str] = field(default_factory=set)
    # 每个 attempt 已记录的结果事件类型，用于识别同一 attempt 的冲突结果
    attempt_outcomes: dict[int, EventType] = field(default_factory=dict)
    last_success_at: float | None = None
    latest_event_at: float | None = None
    failure_count: int = 0
    next_retry_at: float | None = None
    retry_exhausted: bool = False
    last_error_class: ErrorClass | None = None
    last_error_message: str | None = None
    alert: AlertState = field(default_factory=AlertState)
    # 归属管道（缺省为运行实例自身，保证旧调用方式口径不变）
    pipeline_id: str | None = None
    # 失新鲜告警收敛状态（与失败告警分开，可并存、各自恢复）
    staleness: StalenessAlertState = field(default_factory=StalenessAlertState)


# 合法状态流转表（依据事件类型）
_TRANSITIONS: dict[PipelineState, frozenset[PipelineState]] = {
    PipelineState.PENDING: frozenset(
        {
            PipelineState.RUNNING,
            PipelineState.SUCCEEDED,
            PipelineState.FAILED_RETRYABLE,
            PipelineState.FAILED_FATAL,
        }
    ),
    PipelineState.RUNNING: frozenset(
        {
            # 允许更高 version 的 STARTED（重复启动/运行心跳）；
            # 低 version 已在状态机检查之前以 STALE_VERSION 拦截。
            PipelineState.RUNNING,
            PipelineState.SUCCEEDED,
            PipelineState.FAILED_RETRYABLE,
            PipelineState.FAILED_FATAL,
        }
    ),
    # 重试等待中的新一次 STARTED 允许回到 RUNNING；也可直接收到成功/失败。
    # 注意：当 retry_exhausted=True 时，ingest 会在流转表之前单独拦截。
    PipelineState.FAILED_RETRYABLE: frozenset(
        {
            PipelineState.RUNNING,
            PipelineState.SUCCEEDED,
            PipelineState.FAILED_RETRYABLE,
            PipelineState.FAILED_FATAL,
        }
    ),
    PipelineState.SUCCEEDED: frozenset(
        {
            PipelineState.RUNNING,  # 新一轮调度（新 attempt 的 STARTED）
            PipelineState.FAILED_RETRYABLE,
            PipelineState.FAILED_FATAL,
        }
    ),
    # 不可重试终态：除更高 version 的新一轮 STARTED 外不接受其他流转，
    # STARTED 也在 ingest 中被显式按“终态后上报”策略拒绝，保持终态语义严格。
    PipelineState.FAILED_FATAL: frozenset(),
}


class IngestResult:
    """ingest_event 的返回值对象。"""

    def __init__(
        self,
        status: IngestStatus,
        *,
        state: PipelineState,
        reason: RejectReason | None = None,
        detail: str = "",
        retry: RetryDecision | None = None,
    ) -> None:
        self.status = status
        self.state = state
        self.reason = reason
        self.detail = detail
        self.retry = retry

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return (
            f"IngestResult(status={self.status.value}, state={self.state.value}, "
            f"reason={self.reason.value if self.reason else None}, detail={self.detail!r})"
        )


class PipelineStore:
    """内存态、线程安全的管道运行观测存储。"""

    def __init__(self, *, pipelines: PipelineRegistry | None = None) -> None:
        self._records: dict[str, _RunRecord] = {}
        self._locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
        self._registry_lock = threading.Lock()
        # 管道级聚合注册表：默认自建（独立使用 store 时也能按“每实例一管道”查询）
        self._pipelines = pipelines if pipelines is not None else PipelineRegistry()

    @property
    def pipelines(self) -> PipelineRegistry:
        return self._pipelines

    def _get_or_create(self, run_id: str) -> _RunRecord:
        # 字典赋值在 CPython 下原子，但显式加注册锁使语义清晰且可移植。
        record = self._records.get(run_id)
        if record is None:
            with self._registry_lock:
                record = self._records.get(run_id)
                if record is None:
                    record = _RunRecord(pipeline_run_id=run_id)
                    self._records[run_id] = record
        return record

    def _snapshot(self, rec: _RunRecord) -> PipelineStatus:
        kinds: list[str] = []
        if rec.alert.active:
            kinds.append("FAILURE")
        if rec.staleness.active:
            kinds.append("STALENESS")
        return PipelineStatus(
            pipeline_run_id=rec.pipeline_run_id,
            state=rec.state,
            current_attempt=rec.current_attempt,
            applied_version=rec.applied_version,
            last_success_at=rec.last_success_at,
            started_at=rec.started_at,
            latest_event_at=rec.latest_event_at,
            failure_count=rec.failure_count,
            next_retry_at=rec.next_retry_at,
            retry_exhausted=rec.retry_exhausted,
            alert_active=rec.alert.active,
            alert_reason=rec.alert.reason,
            pipeline_id=rec.pipeline_id,
            staleness_alert_active=rec.staleness.active,
            staleness_alert_reason=(
                rec.staleness.last_verdict.reason
                if rec.staleness.last_verdict is not None
                else None
            ),
            staleness_raised_count=rec.staleness.raised_count,
            active_alert_kinds=tuple(kinds),
        )

    def ingest_event(
        self,
        event: PipelineEvent,
        *,
        retry_policy: RetryPolicy | None = None,
    ) -> IngestResult:
        """接收并推进一个事件。

        判定顺序（持实例锁，结论互斥、原因可区分）：
        1. event_id 已处理过 → DUPLICATE（幂等，状态不变、不计数）。
        2. <= 已应用版本 → REJECTED/STALE_VERSION（乱序/迟到）。
        3. 当前为 FAILED_FATAL 终态 → REJECTED/FATAL_ALREADY_TERMINAL。
        4. 同一 attempt 已记录结果且新结果与之不同 → REJECTED/CONFLICTING_OUTCOME。
        5. 状态机不允许该流转 → REJECTED/ILLEGAL_TRANSITION。
        6. 其余 → APPLIED，推进状态、计数、评估告警收敛；
           RETRYABLE 失败在同一临界区内完成重试调度（退避/上限），
           保证查询方不会观察到“已失败但尚无重试调度”的中间态。
        """
        rec = self._get_or_create(event.pipeline_run_id)
        with self._locks[event.pipeline_run_id]:
            # 管道归属绑定：未显式指定 pipeline_id 时，默认该实例自成一管道，
            # 与旧口径完全一致；显式指定后不得跨管道漂移。
            requested_pipeline = event.pipeline_id or event.pipeline_run_id
            if rec.pipeline_id is None:
                rec.pipeline_id = requested_pipeline
            elif rec.pipeline_id != requested_pipeline:
                return IngestResult(
                    IngestStatus.REJECTED,
                    state=rec.state,
                    reason=RejectReason.ILLEGAL_TRANSITION,
                    detail=(
                        f"运行 {event.pipeline_run_id} 已归属管道 {rec.pipeline_id}，"
                        f"不得改报为管道 {requested_pipeline}"
                    ),
                )

            # 1. 重复上报：幂等忽略
            if event.event_id in rec.seen_event_ids:
                return IngestResult(
                    IngestStatus.DUPLICATE,
                    state=rec.state,
                    detail=(
                        f"event_id={event.event_id} 已处理过，幂等忽略，"
                        f"状态保持 {rec.state.value}，不重复计数"
                    ),
                )

            # 2. 乱序/迟到：版本号落后
            if event.version <= rec.applied_version:
                return IngestResult(
                    IngestStatus.REJECTED,
                    state=rec.state,
                    reason=RejectReason.STALE_VERSION,
                    detail=(
                        f"事件 version={event.version} 落后于已应用版本 "
                        f"applied_version={rec.applied_version}，判定为乱序/迟到上报"
                    ),
                )

            # 3. 不可重试终态后再上报
            if rec.state is PipelineState.FAILED_FATAL:
                return IngestResult(
                    IngestStatus.REJECTED,
                    state=rec.state,
                    reason=RejectReason.FATAL_ALREADY_TERMINAL,
                    detail=(
                        f"实例已处于不可重试终态 {rec.state.value}，"
                        f"拒绝 {event.event_type.value} 事件（version={event.version}）"
                    ),
                )

            # 3.1 可重试失败已达次数上限：不再调度任何新的尝试/结果
            if rec.retry_exhausted and rec.state is PipelineState.FAILED_RETRYABLE:
                return IngestResult(
                    IngestStatus.REJECTED,
                    state=rec.state,
                    reason=RejectReason.RETRY_EXHAUSTED,
                    detail=(
                        f"可重试失败已达次数上限（current_attempt={rec.current_attempt}），"
                        f"拒绝 {event.event_type.value} 事件（version={event.version}），不再继续重试"
                    ),
                )

            target = self._target_state(event)

            # 4. 同一 attempt 结果冲突（例如 attempt=2 既报 SUCCEEDED 又报 FAILED）
            if event.event_type in (EventType.SUCCEEDED, EventType.FAILED):
                prior = rec.attempt_outcomes.get(event.attempt)
                if prior is not None and prior is not event.event_type:
                    return IngestResult(
                        IngestStatus.REJECTED,
                        state=rec.state,
                        reason=RejectReason.CONFLICTING_OUTCOME,
                        detail=(
                            f"attempt={event.attempt} 已记录结果 {prior.value}，"
                            f"又收到冲突结果 {event.event_type.value}，拒绝以防结论二义"
                        ),
                    )

            # 5. 非法状态流转
            allowed = _TRANSITIONS[rec.state]
            if target not in allowed:
                return IngestResult(
                    IngestStatus.REJECTED,
                    state=rec.state,
                    reason=RejectReason.ILLEGAL_TRANSITION,
                    detail=(
                        f"不允许从 {rec.state.value} 经由 {event.event_type.value} "
                        f"流转到 {target.value}"
                    ),
                )

            # 6. 应用事件
            retry_decision = self._apply(rec, event, target, retry_policy=retry_policy)
            return IngestResult(
                IngestStatus.APPLIED,
                state=rec.state,
                detail=(
                    f"事件已应用：{event.event_type.value}（version={event.version}, "
                    f"attempt={event.attempt}），状态推进为 {rec.state.value}"
                ),
                retry=retry_decision,
            )

    @staticmethod
    def _target_state(event: PipelineEvent) -> PipelineState:
        if event.event_type is EventType.STARTED:
            return PipelineState.RUNNING
        if event.event_type is EventType.SUCCEEDED:
            return PipelineState.SUCCEEDED
        # FAILED：按错误分类区分可重试/不可重试
        if event.error_class is ErrorClass.FATAL:
            return PipelineState.FAILED_FATAL
        return PipelineState.FAILED_RETRYABLE

    def _apply(
        self,
        rec: _RunRecord,
        event: PipelineEvent,
        target: PipelineState,
        *,
        retry_policy: RetryPolicy | None,
    ) -> RetryDecision | None:
        rec.seen_event_ids.add(event.event_id)
        rec.applied_version = event.version
        rec.latest_event_at = event.occurred_at

        if event.event_type is EventType.STARTED:
            if rec.started_at is None:
                rec.started_at = event.occurred_at
            # 新一次尝试开始：采用上报的 attempt 序号（不得低于当前注意，乱序 STARTED 已在版本检查中拦截）
            rec.current_attempt = max(rec.current_attempt, event.attempt)
            rec.next_retry_at = None
            rec.state = target
            self._pipelines.note_started(
                rec.pipeline_id, rec.pipeline_run_id, at=event.occurred_at
            )
            return None

        if event.event_type is EventType.SUCCEEDED:
            rec.attempt_outcomes[event.attempt] = EventType.SUCCEEDED
            rec.current_attempt = max(rec.current_attempt, event.attempt)
            rec.state = PipelineState.SUCCEEDED
            rec.last_success_at = event.occurred_at
            rec.next_retry_at = None
            rec.retry_exhausted = False
            rec.last_error_class = None
            rec.last_error_message = None
            was_active = rec.alert.active
            evaluate_alert(
                rec.alert,
                state=rec.state,
                retry_exhausted=False,
                version=event.version,
            )
            self._raise_at_if_new(rec.alert, was_active, event.occurred_at)
            # 成功即恢复新鲜：关闭可能活动的失新鲜告警（同事件时间轴，立即判定）。
            self._recover_staleness_on_success(rec, at=event.occurred_at)
            self._pipelines.note_success(
                rec.pipeline_id, rec.pipeline_run_id,
                at=event.occurred_at, version=event.version,
            )
            return None

        # FAILED
        rec.attempt_outcomes[event.attempt] = EventType.FAILED
        rec.current_attempt = max(rec.current_attempt, event.attempt)
        rec.failure_count += 1
        rec.last_error_class = event.error_class
        rec.last_error_message = event.error_message
        if target is PipelineState.FAILED_FATAL:
            rec.state = PipelineState.FAILED_FATAL
            rec.next_retry_at = None
            rec.retry_exhausted = True
            was_active = rec.alert.active
            evaluate_alert(
                rec.alert,
                state=rec.state,
                retry_exhausted=True,
                version=event.version,
            )
            self._raise_at_if_new(rec.alert, was_active, event.occurred_at)
            self._note_failure_aggregate(rec, event, target)
            return None

        # RETRYABLE：在同一临界区内完成重试调度，杜绝中间态与丢失更新。
        # 调度基线使用事件发生时间 occurred_at，与 last_success_at 处于同一时间轴，
        # 便于下游比较；服务注入时钟仅用于时效性查询。
        rec.state = PipelineState.FAILED_RETRYABLE
        base_now = event.occurred_at
        if retry_policy is None:
            retry_policy = RetryPolicy()
        decision = decide_retry(
            retry_policy,
            failed_attempt=event.attempt,
            error_class=ErrorClass.RETRYABLE,
            now=base_now,
        )
        if decision.should_retry:
            rec.next_retry_at = base_now + decision.delay_seconds
            rec.retry_exhausted = False
        else:
            # 可重试错误耗尽上限：保留 FAILED_RETRYABLE 并置 exhausted，
            # 与 FATAL 的 FAILED_FATAL 明确区分。
            rec.next_retry_at = None
            rec.retry_exhausted = True
        was_active = rec.alert.active
        evaluate_alert(
            rec.alert,
            state=rec.state,
            retry_exhausted=rec.retry_exhausted,
            version=event.version,
        )
        self._raise_at_if_new(rec.alert, was_active, base_now)
        self._note_failure_aggregate(rec, event, target)
        return decision

    @staticmethod
    def _raise_at_if_new(
        alert: AlertState, was_active: bool, at: float
    ) -> None:
        """若本轮评估刚把失败告警从非活动变为活动，记录首次产生时间。"""
        if alert.active and not was_active and alert.last_raised_at is None:
            alert.last_raised_at = at

    def _note_failure_aggregate(
        self,
        rec: _RunRecord,
        event: PipelineEvent,
        target: PipelineState,
    ) -> None:
        """在同一临界区把失败终态同步进管道聚合（含失败告警镜像）。"""
        self._pipelines.note_failure(
            rec.pipeline_id,
            rec.pipeline_run_id,
            at=event.occurred_at,
            state=target,
            alert_active=rec.alert.active,
            alert_reason=rec.alert.reason,
            raised_at=rec.alert.last_raised_at,
            version=event.version,
        )

    def _recover_staleness_on_success(
        self, rec: _RunRecord, *, at: float
    ) -> None:
        """成功事件即时恢复失新鲜：关闭活动告警并记录恢复时刻（不依赖巡检）。"""
        if rec.staleness.active:
            rec.staleness.active = False
            rec.staleness.raised_at = None
            rec.staleness.last_recovered_at = at

    def check_staleness(
        self,
        run_id: str,
        *,
        fresh_within_seconds: float,
        now: float,
    ) -> StalenessVerdict | None:
        """对单个实例执行一次确定性失新鲜巡检，并同步管道失新鲜告警镜像。

        now 由调用方（service）注入，使巡检可按需确定性触发（不靠真实 sleep）。
        返回 StalenessVerdict；未知实例返回 None。
        """
        rec = self._records.get(run_id)
        if rec is None:
            return None
        with self._locks[run_id]:
            verdict = evaluate_staleness(
                rec.staleness,
                last_success_at=rec.last_success_at,
                started_at=rec.started_at,
                now=now,
                fresh_within_seconds=fresh_within_seconds,
            )
            view: ActiveAlertView | None
            if rec.staleness.active:
                view = ActiveAlertView(
                    pipeline_run_id=run_id,
                    kind=AlertKind.STALENESS,
                    reason=verdict.reason,
                    since=rec.staleness.raised_at,
                )
            else:
                view = None
            self._pipelines.set_staleness_alert(
                rec.pipeline_id,
                run_id,
                view=view,
                threshold_seconds=fresh_within_seconds,
                checked_at=verdict.now,
            )
            return verdict

    def get_status(self, run_id: str) -> PipelineStatus | None:
        """读取一致的状态汇总快照；不存在返回 None。"""
        rec = self._records.get(run_id)
        if rec is None:
            return None
        with self._locks[run_id]:
            return self._snapshot(rec)

    def list_status(self) -> list[PipelineStatus]:
        """列出全部实例的汇总快照（逐个加锁，单实例快照无中间态）。"""
        with self._registry_lock:
            run_ids = list(self._records.keys())
        snapshots: list[PipelineStatus] = []
        for run_id in run_ids:
            snap = self.get_status(run_id)
            if snap is not None:
                snapshots.append(snap)
        return snapshots

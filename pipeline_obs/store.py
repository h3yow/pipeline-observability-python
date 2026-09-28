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

from .alerting import (
    assess_staleness,
    evaluate_alert,
    evaluate_staleness_alert,
    recover_staleness_alert_on_success,
)
from .models import (
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
    StalenessAssessment,
)
from .pipelines import PipelineRegistry
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
    # 失新鲜告警（与失败告警独立维护）
    stale_alert: StalenessAlertState = field(default_factory=StalenessAlertState)
    # 最近一次失新鲜判定（巡检后填充），用于快照与管道汇总里展示判定依据
    last_staleness_assessment: "StalenessAssessment | None" = field(default=None)
    # 管道归属（首个带 pipeline_id 的事件确定后不再变更）
    pipeline_id: str | None = None


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

    def __init__(
        self,
        *,
        pipeline_registry: PipelineRegistry | None = None,
        retained_run_limit: int = 1000,
    ) -> None:
        self._records: dict[str, _RunRecord] = {}
        self._locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
        self._registry_lock = threading.Lock()
        self.pipeline_registry = pipeline_registry or PipelineRegistry(
            retained_run_limit=retained_run_limit
        )
        # 反向注入：管道快照需要读取实例的告警字段
        self.pipeline_registry._bind_store(self)

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

    # ---- 供 PipelineRegistry 摘除被淘汰实例 ----

    def _discard_record(self, run_id: str) -> None:
        """保留策略淘汰：从实例注册表摘除（调用方持有 run 锁+pipeline 锁）。

        仅操作 dict（受注册表短锁保护），不获取被淘汰实例的实例锁，
        以严格遵守 run -> pipeline 的单向锁序。
        """
        with self._registry_lock:
            self._records.pop(run_id, None)

    def _snapshot(self, rec: _RunRecord) -> PipelineStatus:
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
            last_error_class=rec.last_error_class,
            last_error_message=rec.last_error_message,
            staleness_alert_active=rec.stale_alert.active,
            staleness_alert_reason=rec.stale_alert.reason,
            staleness_assessment=rec.last_staleness_assessment,
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
            self._post_apply_pipeline(rec, event)
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
            evaluate_alert(
                rec.alert,
                state=rec.state,
                retry_exhausted=False,
                version=event.version,
            )
            # 成功即恢复新鲜：失新鲜告警即时关闭，无需等待下一次巡检
            recover_staleness_alert_on_success(
                rec.stale_alert, last_success_at=event.occurred_at
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
            evaluate_alert(
                rec.alert,
                state=rec.state,
                retry_exhausted=True,
                version=event.version,
            )
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
        evaluate_alert(
            rec.alert,
            state=rec.state,
            retry_exhausted=rec.retry_exhausted,
            version=event.version,
        )
        return decision

    def _post_apply_pipeline(
        self, rec: _RunRecord, event: PipelineEvent
    ) -> None:
        """事件在实例锁内应用后，按 run->pipeline 锁序刷新管道聚合。

        - 管道归属：以首个携带 pipeline_id 的事件为准（first-wins），
          避免同一实例被并发/重复上报拉扯到不同管道；
        - 聚合更新与淘汰在同一（run 锁 + pipeline 锁）临界区完成，
          查询方看不到“实例已更新、聚合未更新”的中间态。
        """
        pipeline_id = event.pipeline_id or rec.pipeline_id
        if pipeline_id is None:
            return
        if rec.pipeline_id is None:
            rec.pipeline_id = event.pipeline_id

        registry = self.pipeline_registry
        with registry.lock_for(pipeline_id):
            registry.on_run_applied(pipeline_id, rec.pipeline_run_id)
            registry.apply_run_update(
                pipeline_id,
                run_id=rec.pipeline_run_id,
                state=rec.state,
                latest_event_at=rec.latest_event_at or event.occurred_at,
                version=rec.applied_version,
                failed_outcome=event.event_type is EventType.FAILED,
                succeeded=event.event_type is EventType.SUCCEEDED,
            )
            victims = registry.evict_if_needed(pipeline_id)
            for rid in victims:
                if rid == rec.pipeline_run_id:
                    # 刚应用事件的实例最新，不可能被淘汰；防御性跳过
                    continue
                self._discard_record(rid)

    def sweep_staleness(
        self,
        *,
        now: float,
        fresh_within_seconds: float,
        pipeline_id: str | None = None,
    ) -> list[PipelineStatus]:
        """确定性失新鲜巡检：按给定 now/阈值评估全部（或指定管道）实例。

        - 不依赖真实 sleep：now 由调用方注入，可在测试中精确推进；
        - 逐实例加锁评估，同一段失新鲜期间只告警一次、只计一次
          （收敛逻辑见 alerting.evaluate_staleness_alert）；
        - 返回巡检后处于失新鲜告警中的实例快照列表。
        """
        if pipeline_id is None:
            with self._registry_lock:
                run_ids = list(self._records.keys())
        else:
            run_ids = self.pipeline_registry.member_ids_locked(pipeline_id)
            if run_ids is None:
                return []

        stale_snaps: list[PipelineStatus] = []
        for run_id in run_ids:
            snap = self.evaluate_run_staleness(
                run_id,
                now=now,
                fresh_within_seconds=fresh_within_seconds,
            )
            if snap is not None and snap.staleness_alert_active:
                stale_snaps.append(snap)
        return stale_snaps

    def evaluate_run_staleness(
        self,
        run_id: str,
        *,
        now: float,
        fresh_within_seconds: float,
    ) -> PipelineStatus | None:
        """对单个实例执行一次失新鲜判定并收敛其失新鲜告警；返回最新快照。"""
        with self._registry_lock:
            rec = self._records.get(run_id)
        if rec is None:
            return None
        with self._locks[run_id]:
            assessment = assess_staleness(
                last_success_at=rec.last_success_at,
                started_at=rec.started_at,
                now=now,
                fresh_within_seconds=fresh_within_seconds,
            )
            rec.last_staleness_assessment = assessment
            evaluate_staleness_alert(rec.stale_alert, assessment)
            return self._snapshot(rec)

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

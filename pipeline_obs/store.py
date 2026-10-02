"""管道运行状态存储：线程安全的状态机推进、计数与汇总。

设计要点：
- 每个 pipeline_run_id 独立锁，串行化同一实例的更新，杜绝丢失更新。
- 基于 version 的乱序检测：低版本事件被拒绝（STALE_VERSION）。
- 基于 event_id 的去重：重复事件幂等忽略（DUPLICATE），不重复计数。
- 非法状态流转 / 终态后上报 / 同 attempt 冲突结果均被拒绝并给出可区分原因。
- 汇总快照在锁内一次性构造，查询方不会观察到中间态。
"""

from __future__ import annotations

import heapq
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field

from .alerting import (
    build_success_recovery_verdict,
    evaluate_alert,
    evaluate_staleness,
)
from .models import (
    ActiveAlertView,
    AlertKind,
    AlertState,
    ErrorClass,
    EventType,
    FailureTerminalReason,
    IngestStatus,
    LookupStatus,
    PipelineEvent,
    PipelineHealth,
    PipelineState,
    PipelineStatus,
    RejectReason,
    RetentionInfo,
    RetryDecision,
    RetryPolicy,
    RunLookup,
    StalenessAlertState,
    StalenessVerdict,
)
from .pipeline import PipelineRegistry
from .retry import decide_retry

# 内存态历史保留的默认上限：最多保留最近的不同运行实例数。
# 超过后按“最近事件时间最旧（同时间按 run_id 字典序）”淘汰。
DEFAULT_MAX_RUNS = 1000


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
    last_failure_at: float | None = None
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
    """内存态、线程安全的管道运行观测存储。

    历史保留：最多保留 ``max_runs`` 个不同运行（默认 DEFAULT_MAX_RUNS），
    超过后在每次写入后淘汰“最近事件时间最旧（同时间按 run_id 字典序）”的运行；
    ``max_runs=None`` 表示不设上限（与旧行为一致）。淘汰的运行从聚合中摘除、
    不再进入任何汇总，其 run_id 在有界墓碑集合中保留，使后续查询得到一致的
    “已淘汰”结论而不是报错或残缺数据；墓碑本身也按同一上限有界，内存不无限增长。
    """

    def __init__(
        self,
        *,
        pipelines: PipelineRegistry | None = None,
        max_runs: int | None = DEFAULT_MAX_RUNS,
    ) -> None:
        if max_runs is not None and max_runs <= 0:
            raise ValueError("max_runs 必须为正整数或 None（不设上限）")
        self._records: dict[str, _RunRecord] = {}
        self._locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
        self._registry_lock = threading.Lock()
        # 管道级聚合注册表：默认自建（独立使用 store 时也能按“每实例一管道”查询）
        self._pipelines = pipelines if pipelines is not None else PipelineRegistry()
        # —— 历史保留与淘汰 ——
        self._max_runs = max_runs
        self._evicted_total = 0
        # 有界墓碑：记录已淘汰的 run_id，容量与保留上限同阶（None 时给一个保护上限）
        self._tombstones: deque[str] = deque()
        self._tombstone_set: set[str] = set()
        # (latest_event_at, run_id) 最小堆：惰性校验（堆顶时间与记录实际时间不符即丢弃），
        # 每个 run 每次写入入堆一次、出堆至多一次，写入摊还 O(log n)。
        self._order_heap: list[tuple[float, str]] = []

    @property
    def pipelines(self) -> PipelineRegistry:
        return self._pipelines

    @property
    def max_runs(self) -> int | None:
        return self._max_runs

    def retention_info(self) -> RetentionInfo:
        """对外披露保留口径、当前保留数与累计淘汰量。"""
        with self._registry_lock:
            return RetentionInfo(
                max_runs=self._max_runs,
                current_runs=len(self._records),
                total_evicted_runs=self._evicted_total,
            )

    def retention_counts(self) -> tuple[int, int, int | None]:
        """在注册锁内一次性读出 (当前保留数, 累计淘汰数, 上限)。"""
        with self._registry_lock:
            return len(self._records), self._evicted_total, self._max_runs

    def consistency_snapshot(self, pipeline_id: str) -> dict[str, object] | None:
        """在注册锁+管道锁同一临界区内读出并发自洽校验所需的全部字段。

        淘汰在同一锁序下原子提交（墓碑登记与告警/聚合摘除不可分），因此本快照
        内：活动告警引用的每个运行都必然仍在 _records 中（非墓碑、非缺失）。
        """
        agg = self._pipelines._ref(pipeline_id)
        if agg is None:
            return None
        with self._registry_lock:
            current_runs = len(self._records)
            total_evicted = self._evicted_total
            with agg.lock:
                failure_ids = tuple(agg.failure_alerts.keys())
                staleness_ids = tuple(agg.staleness_alerts.keys())
                alert_ids = failure_ids + staleness_ids
                referenced_kept = all(rid in self._records for rid in alert_ids)
                return {
                    "run_count": len(agg.run_ids),
                    "consecutive_failures": agg.consecutive_failures,
                    "failure_alert_count": len(agg.failure_alerts),
                    "staleness_alert_count": len(agg.staleness_alerts),
                    "pipeline_evicted": agg.evicted_run_count,
                    "current_runs": current_runs,
                    "total_evicted": total_evicted,
                    "alert_run_ids": alert_ids,
                    "alerts_reference_kept_runs": referenced_kept,
                }

    def active_alert_run_ids(self, pipeline_id: str) -> tuple[str, ...]:
        """原子读取“当前有活动告警的运行”，并保证它们都仍在保留窗口内。

        锁序与淘汰路径一致（注册锁 → 管道锁）：淘汰在同一临界区同时登记墓碑与
        摘除告警，因此本方法不可能返回已淘汰（墓碑）运行的 ID。供并发自洽校验
        与一致性测试使用；常规明细仍用 pipelines.active_alerts()。
        """
        agg = self._pipelines._ref(pipeline_id)
        if agg is None:
            return ()
        with self._registry_lock, agg.lock:
            ids: list[str] = []
            ids.extend(agg.failure_alerts.keys())
            ids.extend(agg.staleness_alerts.keys())
            # 注册锁保护期间不会有淘汰发生，故所有 id 必仍在 _records 中。
            return tuple(rid for rid in ids if rid in self._records)

    def health_snapshot(self, pipeline_id: str) -> PipelineHealth | None:
        """构造管道健康快照，并保证携带的保留口径与快照主体同一时刻、彼此自洽。

        加锁顺序为「存储注册锁 → 管道聚合锁」；与淘汰路径「实例锁 → 注册锁 →
        管道锁」方向一致，不构成等待环。两锁同时持有期间不会发生淘汰，
        因此 run_count / current_runs / 累计淘汰量取自同一临界区。
        """
        agg = self._pipelines._ref(pipeline_id)
        if agg is None:
            return None
        with self._registry_lock:
            current_runs = len(self._records)
            total_evicted = self._evicted_total
            retention = RetentionInfo(
                max_runs=self._max_runs,
                current_runs=current_runs,
                total_evicted_runs=total_evicted,
            )
            with agg.lock:
                return self._pipelines.health_from_aggregate(agg, retention=retention)

    def _add_tombstone_locked(self, run_id: str) -> None:
        """在注册锁内登记一个已淘汰 run_id（墓碑有界，挤出最旧墓碑）。"""
        if run_id in self._tombstone_set:
            return
        bound = self._max_runs if self._max_runs is not None else DEFAULT_MAX_RUNS
        self._tombstones.append(run_id)
        self._tombstone_set.add(run_id)
        while len(self._tombstones) > bound:
            dropped = self._tombstones.popleft()
            self._tombstone_set.discard(dropped)

    def is_evicted(self, run_id: str) -> bool:
        with self._registry_lock:
            return run_id in self._tombstone_set

    def _get_or_create(self, run_id: str) -> _RunRecord | None:
        # 字典赋值在 CPython 下原子，但显式加注册锁使语义清晰且可移植。
        record = self._records.get(run_id)
        if record is not None:
            return record
        with self._registry_lock:
            record = self._records.get(run_id)
            if record is not None:
                return record
            # 已淘汰的运行不得借新事件“复活”。
            if run_id in self._tombstone_set:
                return None
            record = _RunRecord(pipeline_run_id=run_id)
            self._records[run_id] = record
            return record

    def _snapshot(self, rec: _RunRecord) -> PipelineStatus:
        kinds: list[str] = []
        if rec.alert.active:
            kinds.append("FAILURE")
        if rec.staleness.active:
            kinds.append("STALENESS")
        # 失败终止原因（两类可区分）；非失败终止态为 None
        terminal_reason: str | None = None
        if rec.state is PipelineState.FAILED_FATAL:
            terminal_reason = FailureTerminalReason.NON_RETRYABLE.value
        elif rec.state is PipelineState.FAILED_RETRYABLE and rec.retry_exhausted:
            terminal_reason = FailureTerminalReason.RETRY_EXHAUSTED.value
        # 失新鲜判定依据必须与活动标志同一版：
        # 活动中展示最近一次（失新鲜）巡检依据；已恢复展示最近一次恢复/新鲜依据。
        if rec.staleness.active:
            staleness_reason = (
                rec.staleness.last_verdict.reason
                if rec.staleness.last_verdict is not None
                else None
            )
        else:
            staleness_reason = rec.staleness.last_recovery_reason
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
            staleness_alert_reason=staleness_reason,
            staleness_raised_count=rec.staleness.raised_count,
            active_alert_kinds=tuple(kinds),
            last_error_class=(
                rec.last_error_class.value
                if rec.last_error_class is not None
                else None
            ),
            last_error_message=rec.last_error_message,
            failure_terminal_reason=terminal_reason,
            last_failure_at=rec.last_failure_at,
            staleness_recovered_at=rec.staleness.last_recovered_at,
            staleness_recovery_reason=rec.staleness.last_recovery_reason,
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
        if rec is None:
            # 该运行已被保留策略淘汰：不再接收其事件，结论与查询口径保持一致。
            return IngestResult(
                IngestStatus.REJECTED,
                state=PipelineState.PENDING,
                reason=RejectReason.RUN_EVICTED,
                detail=(
                    f"运行 {event.pipeline_run_id} 已按历史保留策略淘汰，"
                    "不再接收事件（如需继续观测请使用新的运行实例 ID）"
                ),
            )
        with self._locks[event.pipeline_run_id]:
            # 双检：拿到实例锁后再确认一次，挡住“检查后、加锁前”被淘汰的竞态。
            with self._registry_lock:
                evicted_now = event.pipeline_run_id in self._tombstone_set
                still_kept = event.pipeline_run_id in self._records
            if evicted_now or not still_kept:
                return IngestResult(
                    IngestStatus.REJECTED,
                    state=PipelineState.PENDING,
                    reason=RejectReason.RUN_EVICTED,
                    detail=(
                        f"运行 {event.pipeline_run_id} 已按历史保留策略淘汰，"
                        "不再接收事件（如需继续观测请使用新的运行实例 ID）"
                    ),
                )
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
            # 7. 保留淘汰：超出上限后淘汰最旧运行（当前运行仍持锁，不会被选为牺牲品）。
            self._enforce_retention(current_run_id=event.pipeline_run_id)
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
        # 记录顺序堆（保留淘汰用）：每次应用都入堆一项，旧项在淘汰时惰性校验丢弃。
        heapq.heappush(self._order_heap, (event.occurred_at, event.pipeline_run_id))

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
            # 恢复成功后不得残留上一次失败的旧错误：错误分类/信息/失败时间一并清空。
            rec.last_error_class = None
            rec.last_error_message = None
            rec.last_failure_at = None
            was_active = rec.alert.active
            evaluate_alert(
                rec.alert,
                state=rec.state,
                retry_exhausted=False,
                version=event.version,
            )
            self._raise_at_if_new(rec.alert, was_active, event.occurred_at)
            # 成功即恢复新鲜：关闭可能活动的失新鲜告警（同事件时间轴，立即判定），
            # 并把判定依据刷新为同版的“恢复/新鲜”结论（带恢复时刻与依据）。
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
        rec.last_failure_at = event.occurred_at
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
                error_class=event.error_class,
                error_message=event.error_message,
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
            error_class=event.error_class,
            error_message=event.error_message,
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
        """在同一临界区把失败终态同步进管道聚合（含失败告警镜像与失败原因）。"""
        terminal: FailureTerminalReason | None
        if target is PipelineState.FAILED_FATAL:
            terminal = FailureTerminalReason.NON_RETRYABLE
        elif rec.retry_exhausted:
            terminal = FailureTerminalReason.RETRY_EXHAUSTED
        else:
            terminal = None
        self._pipelines.note_failure(
            rec.pipeline_id,
            rec.pipeline_run_id,
            at=event.occurred_at,
            state=target,
            alert_active=rec.alert.active,
            alert_reason=rec.alert.reason,
            raised_at=rec.alert.last_raised_at,
            version=event.version,
            error_class=(
                event.error_class.value if event.error_class is not None else None
            ),
            error_message=event.error_message,
            terminal_reason=terminal.value if terminal is not None else None,
            failure_at=event.occurred_at,
        )

    def _recover_staleness_on_success(
        self, rec: _RunRecord, *, at: float
    ) -> None:
        """成功事件即时恢复失新鲜：关闭活动告警，恢复时刻与判定依据一起更新。

        不依赖下轮巡检：在成功写入的同一临界区内完成。恢复依据通过
        build_success_recovery_verdict 构造（基线=本次成功、age=0、在窗口内），
        与活动标志保持同一版结论，避免“告警已关但依据仍是失新鲜”的矛盾快照。
        """
        st = rec.staleness
        recovery = build_success_recovery_verdict(st, success_at=at)
        if st.active:
            st.active = False
            st.raised_at = None
        st.last_recovered_at = at
        st.last_checked_at = at
        if recovery is not None:
            st.last_verdict = recovery
            st.last_recovery_reason = recovery.reason
        # 同步撤销管道侧的失新鲜告警镜像（阈值沿用最近一次巡检口径，未巡检则不覆盖）。
        self._pipelines.set_staleness_alert(
            rec.pipeline_id,
            rec.pipeline_run_id,
            view=None,
            threshold_seconds=st.last_threshold_seconds,
            checked_at=at,
        )

    # —— 历史保留与淘汰 ——

    def _enforce_retention(self, *, current_run_id: str) -> None:
        """超过保留上限时淘汰最旧运行。调用方已持有 current_run_id 的实例锁。

        淘汰规则：最近事件时间最旧；同时间按 run_id 字典序。
        使用惰性校验最小堆选择牺牲品。死锁规避：堆顶只在“失效”（记录已不在、
        或该运行已更新到更晚时间）时弹出；若堆顶是一条有效项但恰好属于当前
        运行（当前运行即全局最旧），本轮**不淘汰也不弹出**直接返回——否则两个
        并发写入者会互相把对方选为牺牲品形成 AB-BA。该堆项保留，等下一个别的
        运行写入时重新评估（容量至多瞬时超限 1 个并很快自愈）。
        """
        if self._max_runs is None:
            return
        while True:
            with self._registry_lock:
                if len(self._records) <= self._max_runs:
                    return
                victim_id: str | None = None
                victim_at: float | None = None
                while self._order_heap:
                    at, rid = self._order_heap[0]
                    rec = self._records.get(rid)
                    if rec is None or rec.latest_event_at != at:
                        heapq.heappop(self._order_heap)  # 失效项：丢弃
                        continue
                    if rid == current_run_id:
                        # 当前运行的有效最旧项：不淘汰自己、不弹出，本轮结束。
                        return
                    victim_id = rid
                    victim_at = at
                    break
                if victim_id is None:
                    return  # 堆被失效项耗尽，等待后续写入重建
            self._evict_run(victim_id, at=victim_at)

    def _evict_run(self, run_id: str, *, at: float | None) -> None:
        """按固定加锁顺序（实例锁 → 注册锁 → 管道锁）摘除一个被淘汰的运行。

        记录摘除与聚合摘除在同一注册锁临界区内完成，查询者不会看到“记录已消失
        但仍计入管道汇总”的中间态。管道锁永远在链路最后获取，正常写入路径为
        实例锁 → 管道锁，两者方向一致，不构成等待环。
        """
        victim_lock = self._locks[run_id]
        with victim_lock, self._registry_lock:
            rec = self._records.pop(run_id, None)
            if rec is None:
                return  # 已被其他线程淘汰
            self._evicted_total += 1
            self._add_tombstone_locked(run_id)
            pipeline_id = rec.pipeline_id
            state = rec.state
            last_failure_at = rec.last_failure_at
            # 仍在注册锁内完成聚合摘除（管道锁为链路最后一环，见上）。
            self._pipelines.evict_run(
                pipeline_id,
                run_id,
                state=state,
                failed_at=last_failure_at,
            )
            self._locks.pop(run_id, None)

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
        """读取一致的状态汇总快照；从未见过或已淘汰均返回 None。

        需要区分“已淘汰”与“从未见过”时请用 :meth:`lookup_run`。
        """
        rec = self._records.get(run_id)
        if rec is None:
            return None
        with self._locks[run_id]:
            return self._snapshot(rec)

    def lookup_run(self, run_id: str) -> RunLookup:
        """单运行三态查询：PRESENT / EVICTED / UNKNOWN，结论明确一致、不报错。

        - PRESENT：运行在保留窗口内，snapshot 为锁内构造的完整快照；
        - EVICTED：运行曾被观测但已按保留策略淘汰，不返回残缺数据，附说明；
        - UNKNOWN：从未观测到该运行。
        """
        rec = self._records.get(run_id)
        if rec is not None:
            with self._locks[run_id]:
                return RunLookup(
                    status=LookupStatus.PRESENT,
                    snapshot=self._snapshot(rec),
                )
        with self._registry_lock:
            if run_id in self._tombstone_set:
                return RunLookup(
                    status=LookupStatus.EVICTED,
                    run_id=run_id,
                    detail=(
                        f"运行 {run_id} 曾被观测，但已按历史保留策略淘汰"
                        f"（上限 max_runs={self._max_runs}）；其不再进入任何汇总，"
                        "也不返回残缺明细。累计淘汰量见 retention_info()。"
                    ),
                )
        return RunLookup(
            status=LookupStatus.UNKNOWN,
            run_id=run_id,
            detail=f"从未观测到运行 {run_id}",
        )

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

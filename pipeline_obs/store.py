"""管道运行状态存储：线程安全的状态机推进、计数与汇总。

设计要点：
- 每个 pipeline_run_id 独立锁，串行化同一实例的更新，杜绝丢失更新。
- 基于 version 的乱序检测：低版本事件被拒绝（STALE_VERSION）。
- 基于 event_id 的去重：重复事件幂等忽略（DUPLICATE），不重复计数。
- 非法状态流转 / 终态后上报 / 同 attempt 冲突结果均被拒绝并给出可区分原因。
- 汇总快照在锁内一次性构造，查询方不会观察到中间态。
"""

from __future__ import annotations

import collections
import heapq
import threading
from collections import defaultdict
from dataclasses import dataclass, field

from .alerting import (
    evaluate_alert,
    evaluate_staleness,
    recover_staleness_on_success,
)
from .models import (
    ActiveAlertView,
    AlertKind,
    AlertState,
    ErrorClass,
    EventType,
    FailureTermination,
    IngestStatus,
    LookupOutcome,
    PipelineEvent,
    PipelineState,
    PipelineStatus,
    RejectReason,
    RetentionStatus,
    RetryDecision,
    RetryPolicy,
    RunLookup,
    StalenessAlertState,
    StalenessVerdict,
)
from .pipeline import PipelineRegistry
from .retry import decide_retry


class _TombstoneLock:
    """组合 threading.Lock 并允许挂一个墓碑布尔位（实现简单、可移植）。

    run 被淘汰后该锁保留并置 tombstone=True，作为“此 ID 已淘汰”的持久判据，
    不随淘汰登记窗口滑动而失效。
    """

    __slots__ = ("_lock", "tombstone")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.tombstone = False

    def acquire(self, *args, **kwargs):
        return self._lock.acquire(*args, **kwargs)

    def release(self):
        return self._lock.release()

    def __enter__(self):
        return self._lock.__enter__()

    def __exit__(self, *exc):
        return self._lock.__exit__(*exc)


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
    # 已被保留策略淘汰：记录本身即将摘除；此标志用于在并发下杜绝“淘汰后复活”。
    evicted: bool = False


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

    历史保留：max_retained_runs 为保留的不同运行实例数硬上限（默认
    DEFAULT_MAX_RETAINED_RUNS）。超出后按“最近事件时间最旧、同时间 run_id
    字典序最小”淘汰最旧的运行（drop-oldest）。淘汰在每次写入后执行，
    因此任何时刻保留中的运行都不会超过上限；累计淘汰量通过
    retention_status() 对外披露。
    """

    DEFAULT_MAX_RETAINED_RUNS = 10_000

    def __init__(
        self,
        *,
        pipelines: PipelineRegistry | None = None,
        max_retained_runs: int | None = None,
    ) -> None:
        if max_retained_runs is None:
            max_retained_runs = self.DEFAULT_MAX_RETAINED_RUNS
        if max_retained_runs < 1:
            raise ValueError("max_retained_runs 必须为正整数")
        self._max_retained_runs = max_retained_runs
        self._records: dict[str, _RunRecord] = {}
        self._locks: dict[str, _TombstoneLock] = defaultdict(_TombstoneLock)
        self._registry_lock = threading.Lock()
        # 淘汰候选最小堆 (latest_event_at, run_id)：惰性项在淘汰时校验跳过。
        self._retention_heap: list[tuple[float, str]] = []
        # 每个 run 已入堆的最近事件时间：保证同一 (run, 时间) 只入堆一次，
        # 杜绝并发/重复路径往堆里灌重复项导致同一运行被反复当淘汰目标。
        self._heap_mark: dict[str, float] = {}
        # 已淘汰运行的“墓碑锁”（持久保留，不滑动）：run 被淘汰后记录摘除，
        # 但锁保留并打上墓碑标记；之后任何同 ID 事件拿到锁即见墓碑，一律以
        # RUN_EVICTED 拒绝。墓碑不随登记窗口滑动而失效，从根本上杜绝“旧 ID
        # 借迟到/重复事件还魂、被二次淘汰多计”。墓碑只保留 run_id 与一把锁，
        # 不含事件集合/堆项/聚合，体量远小于完整运行历史，是幂等去重的必要代价；
        # 完整运行状态、失败明细与聚合贡献仍严格受 max_retained_runs 上限约束。
        self._tombstones: dict[str, _TombstoneLock] = {}
        # 已淘汰运行 ID 登记表（定长，最多记 max_retained_runs 个）：
        # 仅用于把查询结论区分为“已淘汰 EVICTED”与“从不知晓 UNKNOWN”，
        # 不承担防还魂职责（防还魂由上面的持久墓碑保证）。
        self._evicted_order: collections.deque[str] = collections.deque()
        self._evicted: set[str] = set()
        self._evicted_count = 0
        # 管道级聚合注册表：默认自建（独立使用 store 时也能按“每实例一管道”查询）
        self._pipelines = pipelines if pipelines is not None else PipelineRegistry()

    @property
    def pipelines(self) -> PipelineRegistry:
        return self._pipelines

    @staticmethod
    def _termination(rec: _RunRecord) -> str:
        """把运行当前失败终态归因为两类可区分结论；非失败/已恢复为 NOT_TERMINATED。"""
        if rec.state is PipelineState.FAILED_FATAL:
            return FailureTermination.FATAL.value
        if rec.state is PipelineState.FAILED_RETRYABLE and rec.retry_exhausted:
            return FailureTermination.RETRY_EXHAUSTED.value
        return FailureTermination.NOT_TERMINATED.value

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
            last_error_class=(
                rec.last_error_class.value if rec.last_error_class is not None else None
            ),
            last_error_message=rec.last_error_message,
            failure_termination=self._termination(rec),
            staleness_last_recovered_at=rec.staleness.last_recovered_at,
        )

    def ingest_event(
        self,
        event: PipelineEvent,
        *,
        retry_policy: RetryPolicy | None = None,
    ) -> IngestResult:
        """接收并推进一个事件。

        判定顺序（持实例锁，结论互斥、原因可区分）：
        0. 运行已超出历史保留上限被淘汰 → REJECTED/RUN_EVICTED（不复活）。
        1. event_id 已处理过 → DUPLICATE（幂等，状态不变、不计数）。
        2. <= 已应用版本 → REJECTED/STALE_VERSION（乱序/迟到）。
        3. 当前为 FAILED_FATAL 终态 → REJECTED/FATAL_ALREADY_TERMINAL。
        4. 同一 attempt 已记录结果且新结果与之不同 → REJECTED/CONFLICTING_OUTCOME。
        5. 状态机不允许该流转 → REJECTED/ILLEGAL_TRANSITION。
        6. 其余 → APPLIED，推进状态、计数、评估告警收敛；
           RETRYABLE 失败在同一临界区内完成重试调度（退避/上限），
           保证查询方不会观察到“已失败但尚无重试调度”的中间态。
        写入释放实例锁后执行保留淘汰（可能在锁外摘除其他最旧运行）。
        """
        run_id = event.pipeline_run_id
        # 第一步（registry 短锁）：解析锁与记录、做墓碑/登记判据、必要时创建记录。
        # 不在此嵌套实例锁，保持「registry_lock → 实例锁」（淘汰）与
        # 「实例锁 → 管道锁」（写入）两条锁序互不交叉、无死锁。
        with self._registry_lock:
            if run_id in self._tombstones or run_id in self._evicted:
                evicted_detail = True
                rec = None
                run_lock = None
            else:
                evicted_detail = False
                run_lock = self._locks.get(run_id)
                rec = self._records.get(run_id)
                if rec is None:
                    # 新 run 即将占一个名额：先在同一 registry 临界区内把
                    # 最旧的一个在保运行淘汰掉，保证“插入新记录”这一刻起
                    # 保留数就不越上限（读者任何时刻都见不到 cap+1）。
                    # 新 run 的首个事件对 PENDING 必然可 APPLIED，故提前淘汰安全；
                    # 此时本线程尚未持有自身实例锁，锁序为
                    # registry → 被淘汰实例锁 → 管道锁，单向无环。
                    if len(self._records) >= self._max_retained_runs:
                        victim = self._pop_oldest_candidate()
                        if victim is not None:
                            self._evict_locked(victim)
                    # 记录与配套锁一起在 registry 短锁内创建，保证唯一。
                    rec = _RunRecord(pipeline_run_id=run_id)
                    run_lock = _TombstoneLock()
                    self._records[run_id] = rec
                    self._locks[run_id] = run_lock

        if evicted_detail:
            return IngestResult(
                IngestStatus.REJECTED,
                state=PipelineState.PENDING,
                reason=RejectReason.RUN_EVICTED,
                detail=(
                    f"运行 {run_id} 已超出历史保留上限被淘汰，"
                    f"拒绝 {event.event_type.value} 事件（version={event.version}），"
                    f"不再接收新上报；如需继续观测请使用新的 run_id"
                ),
            )

        # 第二步（实例锁）：状态机判定与推进。
        assert rec is not None and run_lock is not None
        applied_at: float | None = None
        applied = False
        with run_lock:
            if run_lock.tombstone or rec.evicted:
                # 并发窗口：拿到 registry 短锁之后、实例锁之前，该 run 刚好被淘汰。
                return IngestResult(
                    IngestStatus.REJECTED,
                    state=PipelineState.PENDING,
                    reason=RejectReason.RUN_EVICTED,
                    detail=(
                        f"运行 {run_id} 已被保留策略淘汰，"
                        f"拒绝 {event.event_type.value} 事件（version={event.version}）"
                    ),
                )
            # 管道归属绑定：未显式指定 pipeline_id 时，默认该实例自成一管道，
            # 旧口径完全一致；显式指定后不得跨管道漂移。
            requested_pipeline = event.pipeline_id or run_id
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
            applied = True
            applied_at = rec.latest_event_at
            result = IngestResult(
                IngestStatus.APPLIED,
                state=rec.state,
                detail=(
                    f"事件已应用：{event.event_type.value}（version={event.version}, "
                    f"attempt={event.attempt}），状态推进为 {rec.state.value}"
                ),
                retry=retry_decision,
            )

        # 实例锁已释放：仅当事件真正被 APPLIED（推进了最近事件时间）时，才登记
        # 淘汰候选并按需淘汰。DUPLICATE / REJECTED 不改状态、不入堆，避免同一
        # 运行被重复事件反复推上淘汰位而多计淘汰量。
        if applied and applied_at is not None:
            self._enforce_retention(event.pipeline_run_id, applied_at)
        return result

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
            # 成功即恢复新鲜：活动告警与判定依据在同一临界区一起更新为“新鲜”，
            # 不留下“告警已关、依据还停在失新鲜”的自相矛盾明细。
            recover_staleness_on_success(
                rec.staleness,
                recovered_at=event.occurred_at,
                threshold_seconds=rec.staleness.last_threshold_seconds,
            )
            self._pipelines.note_success(
                rec.pipeline_id,
                rec.pipeline_run_id,
                at=event.occurred_at,
                version=event.version,
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
    def _raise_at_if_new(alert: AlertState, was_active: bool, at: float) -> None:
        """若本轮评估刚把失败告警从非活动变为活动，记录首次产生时间。"""
        if alert.active and not was_active and alert.last_raised_at is None:
            alert.last_raised_at = at

    def _note_failure_aggregate(
        self,
        rec: _RunRecord,
        event: PipelineEvent,
        target: PipelineState,
    ) -> None:
        """在同一临界区把失败终态同步进管道聚合（含失败原因与失败告警镜像）。"""
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
            termination=self._termination(rec),
        )

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
        run_lock = self._locks.get(run_id)
        if rec is None or run_lock is None:
            return None
        with run_lock:
            if rec.evicted or run_lock.tombstone:
                return None
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
        """读取一致的状态汇总快照；不存在或已淘汰均返回 None（旧接口口径不变）。"""
        rec = self._records.get(run_id)
        run_lock = self._locks.get(run_id)
        if rec is None or run_lock is None:
            return None
        with run_lock:
            if rec.evicted or run_lock.tombstone:
                return None
            return self._snapshot(rec)

    def lookup_run(self, run_id: str) -> RunLookup:
        """单运行查询的一致结果：保留中→完整快照；已淘汰/从不知晓→明确结论。

        三者都不抛错、也不返回残缺数据：
        - FOUND：status 为锁内构造的完整快照；
        - EVICTED：该运行曾被观测但已按保留策略淘汰，不再进入任何汇总；
        - UNKNOWN：从未观测到该运行。
        """
        rec = self._records.get(run_id)
        run_lock = self._locks.get(run_id)
        if rec is not None and run_lock is not None:
            with run_lock:
                if not rec.evicted and not run_lock.tombstone:
                    return RunLookup(
                        run_id=run_id,
                        outcome=LookupOutcome.FOUND,
                        status=self._snapshot(rec),
                    )
        with self._registry_lock:
            if run_id in self._evicted or run_id in self._tombstones:
                return RunLookup(
                    run_id=run_id,
                    outcome=LookupOutcome.EVICTED,
                    detail=(
                        f"运行 {run_id} 曾被观测，但已超出历史保留上限"
                        f"（max_retained_runs={self._max_retained_runs}）按最旧优先淘汰；"
                        f"它不再进入任何管道汇总，历史明细不可再查，请使用新的 run_id"
                    ),
                )
        return RunLookup(
            run_id=run_id,
            outcome=LookupOutcome.UNKNOWN,
            detail=f"从未观测到运行 {run_id}",
        )

    def retention_status(self) -> RetentionStatus:
        """对外披露保留口径与累计淘汰量（锁内一次性读取，结论自洽）。"""
        with self._registry_lock:
            return RetentionStatus(
                max_runs=self._max_retained_runs,
                retained_runs=len(self._records),
                evicted_runs=self._evicted_count,
            )

    def retention_snapshot(self) -> tuple[RetentionStatus, tuple[str, ...]]:
        """在同一把 registry_lock 内一次性返回保留口径与当前保留运行 ID 集合。

        供并发看板做“保留数 == 明细条数”的强一致校验：两者取自同一临界区，
        中间不可能插入淘汰，因此必然严格相等（区别于两次独立读取）。
        明细快照仍逐个在各自实例锁内构造。
        """
        with self._registry_lock:
            status = RetentionStatus(
                max_runs=self._max_retained_runs,
                retained_runs=len(self._records),
                evicted_runs=self._evicted_count,
            )
            run_ids = tuple(self._records.keys())
        return status, run_ids

    def _enforce_retention(self, run_id: str, at: float) -> None:
        """写入后登记候选并把保留数压回上限（drop-oldest）。

        每次成功写入只把该运行当前的最近事件时间入堆一次；堆中同 run_id 的
        旧条目在淘汰时惰性校验跳过（与失败堆同样的“索引为准”策略）。
        淘汰整体持 registry_lock 串行（push/计数/摘除），摘除具体运行时再获取
        该运行锁；调用点已释放实例锁，故锁序为
        registry_lock → 实例锁 → 管道锁（剔除聚合），写入路径为
        实例锁 → 管道锁，不含 registry，两路径锁序无环、无死锁。
        记录摘除与管道聚合剔除在同一临界区完成，读者不会看到
        “记录已删但聚合仍计入”的不自洽窗口。
        """
        with self._registry_lock:
            # 同一 (run, 最近事件时间) 只入堆一次：并发重复/重入路径下，
            # 后续线程看到相同标记即跳过，杜绝堆里出现重复候选。
            marked = self._heap_mark.get(run_id)
            if marked != at:
                self._heap_mark[run_id] = at
                heapq.heappush(self._retention_heap, (at, run_id))
            over = len(self._records) - self._max_retained_runs
            while over > 0:
                victim = self._pop_oldest_candidate()
                if victim is None:
                    break
                if self._evict_locked(victim):
                    over -= 1

    def _pop_oldest_candidate(self) -> str | None:
        """弹出一个仍有效的最旧候选 run_id；堆耗尽返回 None（调用方持 registry_lock）。

        有效性以两处索引为准（任一不符即惰性丢弃该堆项）：
        - 该 run 仍在 records 中；
        - 堆项时间 == 该 run 已入堆的最新标记 == 其当前最近事件时间。
        """
        while self._retention_heap:
            at, rid = heapq.heappop(self._retention_heap)
            if self._heap_mark.get(rid) != at:
                continue  # 已被更新的入堆项取代（陈旧项）
            rec = self._records.get(rid)
            if rec is None:
                self._heap_mark.pop(rid, None)
                continue
            if rec.latest_event_at != at:
                continue
            return rid
        return None

    def _evict_locked(self, run_id: str) -> bool:
        """原子摘除一个被淘汰运行（调用方持 registry_lock）。

        在「registry_lock → 实例锁 → 管道锁」同一嵌套临界区内，依次：
        标记实例墓碑 → 剔除管道聚合贡献 → 摘除记录/锁/堆项/登记计数。
        因此任何查询都不会看到“记录已删但聚合仍计入”或其逆的不自洽中间态。
        返回是否真正完成一次淘汰（不存在/已淘汰返回 False）。
        """
        rec = self._records.get(run_id)
        if rec is None:
            return False
        run_lock = self._locks.get(run_id)
        if run_lock is None:
            return False
        with run_lock:
            if rec.evicted:
                return False
            pipeline_id = rec.pipeline_id
            rec.evicted = True
            run_lock.tombstone = True

            # 仍持有实例锁：先剔除管道聚合贡献。锁序为 实例锁 → 管道锁，
            # 与正常写入路径完全一致，只是外层多持一把 registry_lock。
            self._pipelines.remove_run(pipeline_id or run_id, run_id)

        del self._records[run_id]
        # 锁转为持久墓碑：从活跃锁表移入墓碑表（不滑动），窗口内外都杜绝复活。
        self._locks.pop(run_id, None)
        self._tombstones[run_id] = run_lock
        self._heap_mark.pop(run_id, None)
        if run_id not in self._evicted:
            self._evicted.add(run_id)
            self._evicted_order.append(run_id)
            # 登记表自身也定长：只记最近 max 个已淘汰 ID。
            while len(self._evicted_order) > self._max_retained_runs:
                dropped = self._evicted_order.popleft()
                self._evicted.discard(dropped)
        self._evicted_count += 1
        return True

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

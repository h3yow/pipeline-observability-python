"""管道级健康汇总：按管道聚合其下多次运行的状态与活动告警。

并发模型（与 store 的实例锁配套）：
- 每个 pipeline_id 一把锁；实例更新在“先实例锁、后管道锁”的固定加锁顺序下
  增量维护聚合，查询在管道锁内一次性构造不可变快照，不暴露中间态、不丢更新。
- 最近事件时间 / 最近成功时间均为随时间单调推进的标量，ingest 时 O(1) 更新；
  “最近一次成功后的连续失败数”用失败索引 + 最小堆增量维护（每次失败终态
  入堆一次、抬升水位时越界项出堆一次，堆条目与索引项一一对应），聚合查询
  O(1)，不随历史实例数线性扫描。

口径说明：
- “连续失败数”只统计当前仍停留在失败终态（FAILED_RETRYABLE/FAILED_FATAL）
  且其最近失败发生在管道最近一次成功之后的**不同运行**；同一运行的多次
  重试失败按一次计（失败索引以 run_id 为键，保留最近失败时间）。
- 成功是恢复信号：某运行成功后立即从失败索引移除；若它刷新了管道成功水位，
  还会顺带剔除一切不晚于新水位的失败贡献（这些失败已被更新的成功覆盖）。
"""

from __future__ import annotations

import heapq
import threading
from dataclasses import dataclass, field

from .models import (
    ActiveAlertView,
    AlertKind,
    PipelineHealth,
    PipelineState,
    RetentionInfo,
)


@dataclass
class _FailureView:
    """某运行最近一次仍未恢复失败的原因明细（管道“最近一次为什么挂”）。"""

    run_id: str
    failed_at: float
    error_class: str | None
    error_message: str | None
    terminal_reason: str | None
    state: PipelineState
    alert_reason: str | None
    raised_at: float | None
    version: int | None


@dataclass
class _PipelineAggregate:
    """单个管道的聚合可变状态。仅在持有 lock 时访问。"""

    pipeline_id: str
    lock: threading.Lock = field(default_factory=threading.Lock)
    # 归属该管道的全部（保留中的）运行实例
    run_ids: set[str] = field(default_factory=set)
    # 管道最近一次事件（标量随 ingest 单调推进，O(1) 更新）
    latest_event_at: float | None = None
    latest_run_id: str | None = None
    latest_run_state: PipelineState | None = None
    # 每个保留中运行的最近事件 (at, state)：写入 O(1) 更新；
    # 仅当被淘汰运行恰好是“最近运行”时，才扫描一次本表重算标量。
    latest_index: dict[str, tuple[float, PipelineState]] = field(default_factory=dict)
    # 管道最近一次成功时间（单调水位 W）
    last_success_at: float | None = None
    # 最近一次成功之后仍处于失败终态的运行数（连续失败数）
    consecutive_failures: int = 0
    # 各运行最近一次失败终态时间：run_id -> failed_at
    failure_index: dict[str, float] = field(default_factory=dict)
    # 失败时间最小堆 (failed_at, run_id)：水位抬升时批量剔除越界贡献
    failure_heap: list[tuple[float, str]] = field(default_factory=list)
    # 各运行最近一次失败的原因明细：run_id -> _FailureView
    failure_views: dict[str, _FailureView] = field(default_factory=dict)
    # 失败时间最大堆 (-failed_at, run_id)：惰性校验，维护“管道最近一次为什么挂”
    failure_max_heap: list[tuple[float, str]] = field(default_factory=list)
    # 活动告警镜像：run_id -> 告警视图（两类分开存放，可并存、各自恢复）
    failure_alerts: dict[str, ActiveAlertView] = field(default_factory=dict)
    staleness_alerts: dict[str, ActiveAlertView] = field(default_factory=dict)
    # 巡检信息
    staleness_threshold_seconds: float | None = None
    checked_at: float | None = None
    # 该管道累计被淘汰的运行数
    evicted_run_count: int = 0


class PipelineRegistry:
    """管道聚合注册表：维护运行归属、增量聚合与一致健康快照。"""

    def __init__(self) -> None:
        self._aggs: dict[str, _PipelineAggregate] = {}
        self._registry_lock = threading.Lock()

    def _aggregate(self, pipeline_id: str) -> _PipelineAggregate:
        """获取或创建管道聚合（注册表短锁 + 双检）。"""
        agg = self._aggs.get(pipeline_id)
        if agg is None:
            with self._registry_lock:
                agg = self._aggs.get(pipeline_id)
                if agg is None:
                    agg = _PipelineAggregate(pipeline_id=pipeline_id)
                    self._aggs[pipeline_id] = agg
        return agg

    def _ref(self, pipeline_id: str) -> _PipelineAggregate | None:
        """只读获取聚合引用，未知管道返回 None（不创建）。"""
        return self._aggs.get(pipeline_id)

    @staticmethod
    def _bump_latest(
        agg: _PipelineAggregate, run_id: str, state: PipelineState, at: float
    ) -> None:
        """推进“管道最近事件”标量并维护每运行最近事件索引（确定性决胜）。"""
        cur = agg.latest_index.get(run_id)
        cur_key = (
            agg.latest_event_at,
            agg.latest_run_id if agg.latest_run_id is not None else "",
        )
        if cur is None or at > cur[0]:
            agg.latest_index[run_id] = (at, state)
        elif at == cur[0]:
            # 同时间：保留确定性（状态以最新写入为准，时间决胜仍按 run_id 字典序）
            agg.latest_index[run_id] = (at, state)
        if agg.latest_event_at is None or (at, run_id) >= cur_key:
            agg.latest_event_at = at
            agg.latest_run_id = run_id
            agg.latest_run_state = state

    def note_started(self, pipeline_id: str, run_id: str, *, at: float) -> None:
        """STARTED 事件已应用：登记归属并推进“最近事件”标量。

        STARTED 不是终态，不影响成功水位、失败索引与连续失败数；
        失败告警在运行中本就不会被激活，这里也不动告警镜像。
        """
        agg = self._aggregate(pipeline_id)
        with agg.lock:
            agg.run_ids.add(run_id)
            self._bump_latest(agg, run_id, PipelineState.RUNNING, at)

    def note_failure(
        self,
        pipeline_id: str,
        run_id: str,
        *,
        at: float,
        state: PipelineState,
        alert_active: bool,
        alert_reason: str | None,
        raised_at: float | None,
        version: int,
        error_class: str | None = None,
        error_message: str | None = None,
        terminal_reason: str | None = None,
        failure_at: float | None = None,
    ) -> None:
        """FAILED 事件已应用：推进最近事件、维护失败索引/连续失败数与失败告警镜像。"""
        agg = self._aggregate(pipeline_id)
        with agg.lock:
            agg.run_ids.add(run_id)
            self._bump_latest(agg, run_id, state, at)

            f_at = failure_at if failure_at is not None else at
            old = agg.failure_index.get(run_id)
            if old is None:
                # 该运行此前不在失败索引：若其失败晚于当前成功水位则构成新贡献。
                if agg.last_success_at is None or f_at > agg.last_success_at:
                    agg.consecutive_failures += 1
                    heapq.heappush(agg.failure_heap, (f_at, run_id))
                    agg.failure_index[run_id] = f_at
            elif f_at > old:
                # 同一运行又失败（重试链）：只更新时间，不增加运行数。
                # 堆中旧条目在索引中已查无此项，抬升水位清理时自动跳过（惰性）。
                agg.failure_index[run_id] = f_at
                if agg.last_success_at is None or f_at > agg.last_success_at:
                    heapq.heappush(agg.failure_heap, (f_at, run_id))
            # 更早的迟到失败不会到达这里（version 检查已在实例锁内拦截）。

            # 管道“最近一次为什么挂”：保留该运行最新一次失败的完整原因明细，
            # 并以惰性最大堆维护全管道最新失败（查询时才校验堆顶，O(1) 摊还）。
            view = _FailureView(
                run_id=run_id,
                failed_at=f_at,
                error_class=error_class,
                error_message=error_message,
                terminal_reason=terminal_reason,
                state=state,
                alert_reason=alert_reason,
                raised_at=raised_at,
                version=version,
            )
            agg.failure_views[run_id] = view
            heapq.heappush(agg.failure_max_heap, (-f_at, run_id))

            if alert_active:
                existing = agg.failure_alerts.get(run_id)
                if existing is None:
                    agg.failure_alerts[run_id] = ActiveAlertView(
                        pipeline_run_id=run_id,
                        kind=AlertKind.FAILURE,
                        reason=alert_reason or "实例处于失败态",
                        since=raised_at if raised_at is not None else at,
                        version=version,
                        error_class=error_class,
                        error_message=error_message,
                        failure_terminal_reason=terminal_reason,
                        last_failure_at=f_at,
                    )
                else:
                    # 同一活动失败告警：保留首次 since，仅刷新依据、版本与失败原因。
                    agg.failure_alerts[run_id] = ActiveAlertView(
                        pipeline_run_id=run_id,
                        kind=existing.kind,
                        reason=alert_reason or existing.reason,
                        since=existing.since,
                        version=version,
                        error_class=error_class,
                        error_message=error_message,
                        failure_terminal_reason=terminal_reason,
                        last_failure_at=f_at,
                    )
            else:
                agg.failure_alerts.pop(run_id, None)

    def note_success(
        self,
        pipeline_id: str,
        run_id: str,
        *,
        at: float,
        version: int,
    ) -> None:
        """SUCCEEDED 事件已应用：抬升成功水位、清理失败贡献与两类告警镜像。"""
        agg = self._aggregate(pipeline_id)
        with agg.lock:
            agg.run_ids.add(run_id)
            self._bump_latest(agg, run_id, PipelineState.SUCCEEDED, at)

            old = agg.failure_index.pop(run_id, None)
            agg.failure_views.pop(run_id, None)
            if old is not None and (
                agg.last_success_at is None or old > agg.last_success_at
            ):
                agg.consecutive_failures -= 1

            raised = agg.last_success_at is None or at > agg.last_success_at
            if raised:
                agg.last_success_at = at
                # 水位抬升：剔除一切不晚于新水位的失败贡献。
                # 堆中惰性旧条目（failure_index 查无此项 / 时间不匹配）直接跳过。
                while agg.failure_heap:
                    ft, rid = agg.failure_heap[0]
                    if ft > at:
                        break
                    heapq.heappop(agg.failure_heap)
                    cur = agg.failure_index.get(rid)
                    if cur is not None and cur == ft:
                        del agg.failure_index[rid]
                        agg.consecutive_failures -= 1

            # 成功即恢复：失败告警与失新鲜告警都应关闭（失新鲜态由巡检方同步，
            # 这里先撤掉可能残留的镜像，保证健康汇总不露出已恢复的告警）。
            agg.failure_alerts.pop(run_id, None)
            agg.staleness_alerts.pop(run_id, None)

    def set_staleness_alert(
        self,
        pipeline_id: str,
        run_id: str,
        *,
        view: ActiveAlertView | None,
        threshold_seconds: float | None,
        checked_at: float,
    ) -> None:
        """巡检结论同步：写入（view 非空）/撤销（view=None）失新鲜活动告警镜像。

        threshold_seconds 为 None 时（如成功即时恢复且从未巡检过）不覆盖既有
        巡检阈值口径，只推进 checked_at 与告警镜像。
        """
        agg = self._aggregate(pipeline_id)
        with agg.lock:
            agg.run_ids.add(run_id)
            if threshold_seconds is not None:
                agg.staleness_threshold_seconds = threshold_seconds
            agg.checked_at = checked_at
            if view is None:
                agg.staleness_alerts.pop(run_id, None)
            else:
                existing = agg.staleness_alerts.get(run_id)
                if existing is None:
                    agg.staleness_alerts[run_id] = view
                else:
                    # 同一段失新鲜：保留首次 since，仅刷新依据。
                    agg.staleness_alerts[run_id] = ActiveAlertView(
                        pipeline_run_id=run_id,
                        kind=view.kind,
                        reason=view.reason,
                        since=existing.since,
                        version=existing.version,
                    )

    def evict_run(
        self,
        pipeline_id: str,
        run_id: str,
        *,
        state: PipelineState,
        failed_at: float | None,
    ) -> None:
        """某运行已被保留策略淘汰：从管道聚合中完整摘除（在管道锁内）。

        被淘汰运行不再进入任何汇总：归属集合、失败索引/连续失败数、失败原因视图、
        两类活动告警镜像全部移除；若它恰是“最近一次运行”，则在剩余运行中确定性
        重算（同时间按 run_id 字典序）。堆中的对应条目惰性丢弃，不做全堆扫描。
        """
        agg = self._aggregate(pipeline_id)
        with agg.lock:
            agg.run_ids.discard(run_id)
            agg.latest_index.pop(run_id, None)
            agg.failure_views.pop(run_id, None)
            agg.failure_alerts.pop(run_id, None)
            agg.staleness_alerts.pop(run_id, None)

            old = agg.failure_index.pop(run_id, None)
            if old is not None and (
                agg.last_success_at is None or old > agg.last_success_at
            ):
                agg.consecutive_failures -= 1

            if agg.latest_run_id == run_id:
                self._recompute_latest(agg)

            agg.evicted_run_count += 1

    @staticmethod
    def _recompute_latest(agg: _PipelineAggregate) -> None:
        """淘汰命中“最近运行”时，在剩余运行中重算最近事件标量。

        仅在淘汰命中时触发（淘汰本身远少于写入），按剩余运行数线性扫描一次，
        因此常规健康查询仍是 O(1)，不随历史增长。
        """
        best: tuple[float, str, PipelineState] | None = None
        for rid, (at, state) in agg.latest_index.items():
            key = (at, rid)
            if best is None or key > (best[0], best[1]):
                best = (at, rid, state)
        if best is None:
            agg.latest_event_at = None
            agg.latest_run_id = None
            agg.latest_run_state = None
        else:
            agg.latest_event_at, agg.latest_run_id, agg.latest_run_state = best

    def active_alerts(
        self, pipeline_id: str, *, kind: str | None = None
    ) -> tuple[ActiveAlertView, ...]:
        """惰性展开当前活动告警明细（按需调用，数量可能随运行数增长）。

        kind="FAILURE"/"STALENESS" 时只返回该类；缺省返回两类（先失败后失新鲜）。
        在管道锁内拷贝快照，保证与健康主体一致、不暴露迭代中的中间态。
        """
        agg = self._ref(pipeline_id)
        if agg is None:
            return ()
        with agg.lock:
            failures = tuple(agg.failure_alerts.values())
            staleness = tuple(agg.staleness_alerts.values())
        if kind == "FAILURE":
            return failures
        if kind == "STALENESS":
            return staleness
        return failures + staleness

    @staticmethod
    def _latest_failure(
        agg: _PipelineAggregate,
    ) -> tuple[str, _FailureView] | None:
        """惰性校验最大堆，返回当前仍保留的运行中“最近一次失败”的视图。

        堆顶失败时间与 failure_views 中该运行的最新失败一致才有效；
        旧条目（运行已成功恢复/被淘汰/重试链更新）直接弹出。每个堆项至多
        入堆/出堆一次，健康查询摊还 O(1)，不随历史运行数线性变慢。
        """
        while agg.failure_max_heap:
            neg_at, rid = agg.failure_max_heap[0]
            view = agg.failure_views.get(rid)
            if view is None or view.failed_at != -neg_at:
                heapq.heappop(agg.failure_max_heap)
                continue
            return rid, view
        return None

    def health(
        self,
        pipeline_id: str,
        *,
        retention: RetentionInfo | None = None,
    ) -> PipelineHealth | None:
        """在管道锁内一次性构造自洽的管道健康快照；未知管道返回 None。

        主体为 O(1) 物化标量/计数（最大堆仅惰性弹出失效堆顶），不随历史线性
        变慢；告警明细需另取 active_alerts()。需要保留口径与快照严格同一时刻时，
        由存储层 health_snapshot() 在注册锁+管道锁内调用 health_from_aggregate()。
        """
        agg = self._ref(pipeline_id)
        if agg is None:
            return None
        with agg.lock:
            return self.health_from_aggregate(agg, retention=retention)

    def health_from_aggregate(
        self,
        agg: _PipelineAggregate,
        *,
        retention: RetentionInfo | None = None,
    ) -> PipelineHealth:
        """依据已持锁的聚合对象构造健康快照（调用方必须持有 agg.lock）。"""
        latest = self._latest_failure(agg)
        if latest is not None:
            _rid, fv = latest
            latest_failure_run_id = fv.run_id
            latest_failure_at = fv.failed_at
            latest_error_class = fv.error_class
            latest_error_message = fv.error_message
            latest_terminal = fv.terminal_reason
        else:
            latest_failure_run_id = None
            latest_failure_at = None
            latest_error_class = None
            latest_error_message = None
            latest_terminal = None
        return PipelineHealth(
            pipeline_id=agg.pipeline_id,
            run_count=len(agg.run_ids),
            latest_run_id=agg.latest_run_id,
            latest_run_state=agg.latest_run_state,
            latest_event_at=agg.latest_event_at,
            last_success_at=agg.last_success_at,
            consecutive_failures=agg.consecutive_failures,
            active_failure_alert_count=len(agg.failure_alerts),
            active_staleness_alert_count=len(agg.staleness_alerts),
            has_failure_alert=bool(agg.failure_alerts),
            has_staleness_alert=bool(agg.staleness_alerts),
            staleness_threshold_seconds=agg.staleness_threshold_seconds,
            checked_at=agg.checked_at,
            latest_failure_run_id=latest_failure_run_id,
            latest_failure_at=latest_failure_at,
            latest_error_class=latest_error_class,
            latest_error_message=latest_error_message,
            latest_failure_terminal_reason=latest_terminal,
            retention=retention,
            evicted_run_count=agg.evicted_run_count,
        )

    def list_pipeline_ids(self) -> list[str]:
        """列出全部已登记管道 ID（确定性排序）。"""
        with self._registry_lock:
            return sorted(self._aggs.keys())

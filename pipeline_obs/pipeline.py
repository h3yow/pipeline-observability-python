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
)


@dataclass
class _PipelineAggregate:
    """单个管道的聚合可变状态。仅在持有 lock 时访问。"""

    pipeline_id: str
    lock: threading.Lock = field(default_factory=threading.Lock)
    # 归属该管道的全部运行实例
    run_ids: set[str] = field(default_factory=set)
    # 管道最近一次事件（标量随 ingest 单调推进，O(1) 更新）
    latest_event_at: float | None = None
    latest_run_id: str | None = None
    latest_run_state: PipelineState | None = None
    # 管道最近一次成功时间（单调水位 W）
    last_success_at: float | None = None
    # 最近一次成功之后仍处于失败终态的运行数（连续失败数）
    consecutive_failures: int = 0
    # 各运行最近一次失败终态时间：run_id -> failed_at
    failure_index: dict[str, float] = field(default_factory=dict)
    # 失败时间最小堆 (failed_at, run_id)：水位抬升时批量剔除越界贡献
    failure_heap: list[tuple[float, str]] = field(default_factory=list)
    # 活动告警镜像：run_id -> 告警视图（两类分开存放，可并存、各自恢复）
    failure_alerts: dict[str, ActiveAlertView] = field(default_factory=dict)
    staleness_alerts: dict[str, ActiveAlertView] = field(default_factory=dict)
    # 巡检信息
    staleness_threshold_seconds: float | None = None
    checked_at: float | None = None


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
        """推进“管道最近事件”标量；同时间以 run_id 字典序保证确定性。"""
        cur_key = (
            agg.latest_event_at,
            agg.latest_run_id if agg.latest_run_id is not None else "",
        )
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
    ) -> None:
        """FAILED 事件已应用：推进最近事件、维护失败索引/连续失败数与失败告警镜像。"""
        agg = self._aggregate(pipeline_id)
        with agg.lock:
            agg.run_ids.add(run_id)
            self._bump_latest(agg, run_id, state, at)

            old = agg.failure_index.get(run_id)
            if old is None:
                # 该运行此前不在失败索引：若其失败晚于当前成功水位则构成新贡献。
                if agg.last_success_at is None or at > agg.last_success_at:
                    agg.consecutive_failures += 1
                    heapq.heappush(agg.failure_heap, (at, run_id))
                    agg.failure_index[run_id] = at
            elif at > old:
                # 同一运行又失败（重试链）：只更新时间，不增加运行数。
                # 堆中旧条目在索引中已查无此项，抬升水位清理时自动跳过（惰性）。
                agg.failure_index[run_id] = at
                if agg.last_success_at is None or at > agg.last_success_at:
                    heapq.heappush(agg.failure_heap, (at, run_id))
            # 更早的迟到失败不会到达这里（version 检查已在实例锁内拦截）。

            if alert_active:
                existing = agg.failure_alerts.get(run_id)
                if existing is None:
                    agg.failure_alerts[run_id] = ActiveAlertView(
                        pipeline_run_id=run_id,
                        kind=AlertKind.FAILURE,
                        reason=alert_reason or "实例处于失败态",
                        since=raised_at if raised_at is not None else at,
                        version=version,
                    )
                else:
                    # 同一活动失败告警：保留首次 since，仅刷新依据与版本。
                    agg.failure_alerts[run_id] = ActiveAlertView(
                        pipeline_run_id=run_id,
                        kind=existing.kind,
                        reason=alert_reason or existing.reason,
                        since=existing.since,
                        version=version,
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
        threshold_seconds: float,
        checked_at: float,
    ) -> None:
        """巡检结论同步：写入（view 非空）/撤销（view=None）失新鲜活动告警镜像。"""
        agg = self._aggregate(pipeline_id)
        with agg.lock:
            agg.run_ids.add(run_id)
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

    def health(self, pipeline_id: str) -> PipelineHealth | None:
        """在管道锁内一次性构造自洽的管道健康快照；未知管道返回 None。

        主体为 O(1) 物化标量/计数，不随历史线性变慢；告警明细需另取
        active_alerts()。
        """
        agg = self._ref(pipeline_id)
        if agg is None:
            return None
        with agg.lock:
            return PipelineHealth(
                pipeline_id=pipeline_id,
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
            )

    def list_pipeline_ids(self) -> list[str]:
        """列出全部已登记管道 ID（确定性排序）。"""
        with self._registry_lock:
            return sorted(self._aggs.keys())

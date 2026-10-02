"""观测服务门面：对外统一的事件接收、查询、管道健康与失新鲜巡检接口。"""

from __future__ import annotations

import time
from collections.abc import Callable

from .alerting import staleness
from .models import (
    LookupStatus,
    PipelineEvent,
    PipelineHealth,
    RetentionInfo,
    RetryPolicy,
    RunLookup,
    StalenessVerdict,
)
from .store import DEFAULT_MAX_RUNS, IngestResult, PipelineStore


class ObservabilityService:
    """组合存储（状态机/去重/乱序/重试调度/告警收敛/管道聚合）与时效判定的应用服务。

    fresh_within_seconds：失新鲜阈值（秒），全局默认值，可在构造时配置；
    单次巡检也可显式传入阈值覆盖。巡检时刻一律取自注入时钟 clock，
    因此测试可注入可控时钟、按需确定性触发，不依赖真实 sleep。

    max_runs：内存态历史保留上限（不同运行实例数，默认 DEFAULT_MAX_RUNS）；
    超出后淘汰最近事件时间最旧的运行。传 None 表示不设上限（旧行为）。
    """

    def __init__(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        store: PipelineStore | None = None,
        clock: Callable[[], float] = time.monotonic,
        fresh_within_seconds: float = 300.0,
        max_runs: int | None = DEFAULT_MAX_RUNS,
    ) -> None:
        if fresh_within_seconds <= 0:
            raise ValueError("fresh_within_seconds 必须为正数")
        self._store = store or PipelineStore(max_runs=max_runs)
        self.retry_policy = retry_policy or RetryPolicy()
        self._clock = clock
        self.fresh_within_seconds = fresh_within_seconds

    @property
    def store(self) -> PipelineStore:
        return self._store

    def ingest(self, event: PipelineEvent) -> IngestResult:
        """接收一个上报事件并推进状态、计数、重试调度、告警与管道聚合。"""
        return self._store.ingest_event(event, retry_policy=self.retry_policy)

    def status(self, run_id: str):
        """获取单个实例的一致汇总快照。

        已按保留策略淘汰或从未见过均返回 None；需要区分二者请用 lookup_run()。
        """
        return self._store.get_status(run_id)

    def lookup_run(self, run_id: str) -> RunLookup:
        """单运行三态查询：PRESENT（含完整快照）/ EVICTED（已淘汰）/ UNKNOWN（未见）。"""
        return self._store.lookup_run(run_id)

    def retention_info(self) -> RetentionInfo:
        """披露保留口径（上限）、当前保留数与累计淘汰量。"""
        return self._store.retention_info()

    def all_status(self):
        """获取全部（保留中的）实例的一致快照列表；已淘汰运行不出现。"""
        return self._store.list_status()

    # —— 管道级健康汇总 ——

    def pipeline_health(self, pipeline_id: str) -> PipelineHealth | None:
        """获取管道级健康汇总快照；未知管道返回 None。

        口径见 docs/design.md：最近运行状态、最近成功时间、最近成功之后的
        连续失败运行数、当前活动告警（失败/失新鲜分列、可并存）及判定依据、
        最近一次失败的错误分类/信息/终止原因，以及保留口径与该管道累计淘汰量。
        """
        return self._store.health_snapshot(pipeline_id)

    def list_pipeline_ids(self) -> list[str]:
        """列出全部已登记管道 ID。"""
        return self._store.pipelines.list_pipeline_ids()

    def active_alerts(self, pipeline_id: str, *, kind: str | None = None):
        """惰性展开管道当前活动告警明细（FAILURE / STALENESS，可并存）。"""
        return self._store.pipelines.active_alerts(pipeline_id, kind=kind)

    def active_alert_run_ids(self, pipeline_id: str) -> tuple[str, ...]:
        """原子读取当前有活动告警的运行 ID（保证不含已淘汰运行）。"""
        return self._store.active_alert_run_ids(pipeline_id)

    def consistency_snapshot(self, pipeline_id: str) -> dict[str, object] | None:
        """同一临界区内读出的并发自洽校验快照（见 store 文档）。"""
        return self._store.consistency_snapshot(pipeline_id)

    # —— 失新鲜巡检（确定性触发，不依赖真实 sleep）——

    def sweep_staleness(
        self,
        *,
        run_id: str | None = None,
        pipeline_id: str | None = None,
        fresh_within_seconds: float | None = None,
        now: float | None = None,
    ) -> dict[str, StalenessVerdict]:
        """按需执行失新鲜巡检，返回 {run_id: StalenessVerdict}。

        巡检范围（互斥优先级）：
        - 指定 run_id：仅巡检该实例；
        - 否则指定 pipeline_id：巡检归属该管道的全部实例；
        - 都不指定：巡检全部已知实例。

        阈值取参：显式 fresh_within_seconds > 构造默认 fresh_within_seconds。
        巡检时刻取参：显式 now > 注入 clock()，保证测试可确定性驱动时间线。
        同一段失新鲜期间多次巡检只告警/计数一次（收敛逻辑在存储层）。
        """
        threshold = (
            fresh_within_seconds
            if fresh_within_seconds is not None
            else self.fresh_within_seconds
        )
        if threshold <= 0:
            raise ValueError("fresh_within_seconds 必须为正数")
        check_now = now if now is not None else self._clock()

        if run_id is not None:
            targets = [run_id]
        elif pipeline_id is not None:
            targets = self._run_ids_for_pipeline(pipeline_id)
        else:
            targets = [s.pipeline_run_id for s in self._store.list_status()]

        results: dict[str, StalenessVerdict] = {}
        for rid in targets:
            verdict = self._store.check_staleness(
                rid,
                fresh_within_seconds=threshold,
                now=check_now,
            )
            if verdict is not None:
                results[rid] = verdict
        return results

    def check_staleness(
        self,
        run_id: str,
        *,
        fresh_within_seconds: float | None = None,
        now: float | None = None,
    ) -> StalenessVerdict | None:
        """巡检单个实例并返回完整判定结论（含基线/时间差/阈值/结论）。"""
        threshold = (
            fresh_within_seconds
            if fresh_within_seconds is not None
            else self.fresh_within_seconds
        )
        check_now = now if now is not None else self._clock()
        return self._store.check_staleness(
            run_id, fresh_within_seconds=threshold, now=check_now
        )

    def _run_ids_for_pipeline(self, pipeline_id: str) -> list[str]:
        return [
            s.pipeline_run_id
            for s in self._store.list_status()
            if s.pipeline_id == pipeline_id
        ]

    def staleness_report(
        self, run_id: str, fresh_within_seconds: float | None = None
    ) -> tuple[bool, str]:
        """返回 (是否失新鲜, 判定依据)，依据中明确基线与时间差。

        无状态查询（不推进巡检告警收敛）；阈值缺省取构造默认值。
        """
        threshold = (
            fresh_within_seconds
            if fresh_within_seconds is not None
            else self.fresh_within_seconds
        )
        snap = self._store.get_status(run_id)
        if snap is None:
            lookup = self._store.lookup_run(run_id)
            if lookup.status is LookupStatus.EVICTED:
                return True, (
                    f"实例 {run_id} 已按历史保留策略淘汰，不再提供失新鲜明细"
                    "（结论：EVICTED，见 lookup_run）"
                )
            return True, f"实例 {run_id} 不存在，无法确认最近成功时间，判定失新鲜"
        return staleness(
            last_success_at=snap.last_success_at,
            started_at=snap.started_at,
            now=self._clock(),
            fresh_within_seconds=threshold,
        )

    def is_stale(self, run_id: str, fresh_within_seconds: float | None = None) -> bool:
        """基于最近成功时间判定某实例是否已失新鲜；未知实例视为失新鲜。"""
        stale, _ = self.staleness_report(run_id, fresh_within_seconds)
        return stale

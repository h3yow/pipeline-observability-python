"""观测服务门面：对外统一的事件接收、查询、管道健康与告警巡检接口。"""

from __future__ import annotations

import time
from collections.abc import Callable

from .alerting import assess_staleness
from .models import (
    PipelineEvent,
    PipelineHealth,
    PipelineStatus,
    RetryPolicy,
)
from .store import IngestResult, PipelineStore


class ObservabilityService:
    """组合存储（状态机/去重/乱序/重试调度/告警收敛/管道聚合）与时效判定的应用服务。

    失新鲜阈值可在构造时给默认值（default_fresh_within_seconds），
    也可在每次巡检时按调用覆盖；巡检时间 now 由注入时钟决定，
    可确定性触发，不依赖真实 sleep。
    """

    def __init__(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        store: PipelineStore | None = None,
        clock: Callable[[], float] = time.monotonic,
        default_fresh_within_seconds: float | None = None,
        retained_run_limit: int = 1000,
    ) -> None:
        self._store = store or PipelineStore(retained_run_limit=retained_run_limit)
        self.retry_policy = retry_policy or RetryPolicy()
        self._clock = clock
        self.default_fresh_within_seconds = default_fresh_within_seconds

    @property
    def store(self) -> PipelineStore:
        return self._store

    def ingest(self, event: PipelineEvent) -> IngestResult:
        """接收一个上报事件并推进状态、计数、重试调度、告警与管道聚合。"""
        return self._store.ingest_event(event, retry_policy=self.retry_policy)

    def status(self, run_id: str) -> PipelineStatus | None:
        """获取单个实例的一致汇总快照。"""
        return self._store.get_status(run_id)

    def all_status(self) -> list[PipelineStatus]:
        """获取全部实例的一致快照列表。"""
        return self._store.list_status()

    # ---- 管道级健康汇总 ----

    def pipeline_health(self, pipeline_id: str) -> PipelineHealth | None:
        """获取管道级健康汇总快照；管道不存在返回 None。"""
        return self._store.pipeline_registry.get_health(pipeline_id)

    def list_pipelines(self) -> list[str]:
        """列出当前已知的全部管道 ID。"""
        return self._store.pipeline_registry.list_pipeline_ids()

    # ---- 失新鲜：旧接口保持不变（签名/返回类型兼容）----

    def staleness_report(
        self, run_id: str, fresh_within_seconds: float
    ) -> tuple[bool, str]:
        """返回 (是否失新鲜, 判定依据)，依据中明确基线、时间差与阈值。"""
        snap = self._store.get_status(run_id)
        if snap is None:
            return True, f"实例 {run_id} 不存在，无法确认最近成功时间，判定失新鲜"
        assessment = assess_staleness(
            last_success_at=snap.last_success_at,
            started_at=snap.started_at,
            now=self._clock(),
            fresh_within_seconds=fresh_within_seconds,
        )
        return assessment.stale, assessment.reason

    def is_stale(self, run_id: str, fresh_within_seconds: float) -> bool:
        """基于最近成功时间判定某实例是否已失新鲜；未知实例视为失新鲜。"""
        stale, _ = self.staleness_report(run_id, fresh_within_seconds)
        return stale

    # ---- 失新鲜：自动监控巡检（确定性触发）----

    def _resolve_threshold(self, fresh_within_seconds: float | None) -> float:
        threshold = fresh_within_seconds or self.default_fresh_within_seconds
        if threshold is None or threshold <= 0:
            raise ValueError(
                "必须配置正数的 fresh_within_seconds（构造参数或巡检入参）"
            )
        return threshold

    def run_staleness_sweep(
        self,
        *,
        now: float | None = None,
        fresh_within_seconds: float | None = None,
        pipeline_id: str | None = None,
    ) -> list[PipelineStatus]:
        """执行一次失新鲜巡检并收敛失新鲜告警，返回失新鲜实例快照。

        - now 缺省取注入时钟，测试中可显式传入以确定性推进时间线；
        - 同一段失新鲜期间多次巡检只告警一次、只计一次；
        - 成功事件会在 ingest 时即时关闭失新鲜告警；
        - pipeline_id 缺省巡检全部实例，给定后只巡检该管道保留实例。
        """
        threshold = self._resolve_threshold(fresh_within_seconds)
        return self._store.sweep_staleness(
            now=self._clock() if now is None else now,
            fresh_within_seconds=threshold,
            pipeline_id=pipeline_id,
        )

    def evaluate_staleness(
        self,
        run_id: str,
        *,
        now: float | None = None,
        fresh_within_seconds: float | None = None,
    ) -> PipelineStatus | None:
        """对单个实例执行失新鲜判定并收敛告警；未知实例返回 None。"""
        threshold = self._resolve_threshold(fresh_within_seconds)
        return self._store.evaluate_run_staleness(
            run_id,
            now=self._clock() if now is None else now,
            fresh_within_seconds=threshold,
        )

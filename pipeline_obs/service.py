"""观测服务门面：对外统一的事件接收、查询与告警收敛接口。"""

from __future__ import annotations

import time
from collections.abc import Callable

from .alerting import staleness
from .models import (
    PipelineEvent,
    PipelineStatus,
    RetryPolicy,
)
from .store import IngestResult, PipelineStore


class ObservabilityService:
    """组合存储（状态机/去重/乱序/重试调度/告警收敛）与时效判定的应用服务。"""

    def __init__(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        store: PipelineStore | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._store = store or PipelineStore()
        self.retry_policy = retry_policy or RetryPolicy()
        self._clock = clock

    @property
    def store(self) -> PipelineStore:
        return self._store

    def ingest(self, event: PipelineEvent) -> IngestResult:
        """接收一个上报事件并推进状态、计数、重试调度与告警。"""
        return self._store.ingest_event(event, retry_policy=self.retry_policy)

    def status(self, run_id: str) -> PipelineStatus | None:
        """获取单个实例的一致汇总快照。"""
        return self._store.get_status(run_id)

    def all_status(self) -> list[PipelineStatus]:
        """获取全部实例的一致快照列表。"""
        return self._store.list_status()

    def staleness_report(
        self, run_id: str, fresh_within_seconds: float
    ) -> tuple[bool, str]:
        """返回 (是否失新鲜, 判定依据)，依据中明确基线与时间差。"""
        snap = self._store.get_status(run_id)
        if snap is None:
            return True, f"实例 {run_id} 不存在，无法确认最近成功时间，判定失新鲜"
        return staleness(
            last_success_at=snap.last_success_at,
            started_at=snap.started_at,
            now=self._clock(),
            fresh_within_seconds=fresh_within_seconds,
        )

    def is_stale(self, run_id: str, fresh_within_seconds: float) -> bool:
        """基于最近成功时间判定某实例是否已失新鲜；未知实例视为失新鲜。"""
        stale, _ = self.staleness_report(run_id, fresh_within_seconds)
        return stale

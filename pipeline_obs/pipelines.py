"""管道级健康汇总：并发安全的增量聚合、快照与保留策略。

口径说明（对外契约）：

- 最近一次运行：按“最新被应用事件”确定。实例事件在其实例锁内串行应用，
  每次 APPLIED 事件后（在同一临界区、按 run->pipeline 锁序）刷新聚合，
  因此并发上报不会出现丢失更新，查询也不会看到半截中间态。
- 最近一次成功时间：该管道下任意实例成功事件的最大 occurred_at。
- 连续失败次数：最近一次成功之后，产生过 FAILED 结果事件的**不同运行实例**
  数量；同一实例的多次重试失败只算一个失败运行。从未成功过则按全部失败
  实例数计。
- 活动告警：取当前保留实例的告警状态（失败告警 / 失新鲜告警），
  两类独立、可能并存。
- 保留策略：内存态实现按每管道保留最近 retained_run_limit 个有事件的实例，
  超出时淘汰 latest_event_at 最小（最旧）的实例；被淘汰实例的活动告警
  不再计入管道汇总。淘汰发生在事件应用的同一临界区内。

锁序约定（严格单向，杜绝 AB-BA 死锁）：
    实例锁(run) -> 管道锁(pipeline) -> 注册表短锁(registry)
- 管道锁内只读写本模块的内存缓存，绝不反向获取实例锁；
- 管道健康快照：先在管道锁内拷贝标量口径与成员集合，释放管道锁后再逐实例
  加锁读取告警快照。两部分各自对应某个已提交状态，绝无半成品字段。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .models import (
    ActiveAlert,
    AlertKind,
    PipelineHealth,
    PipelineState,
)

if TYPE_CHECKING:
    from .models import PipelineStatus
    from .store import PipelineStore


@dataclass
class _RunMeta:
    """管道锁内维护的实例镜像（仅供连续失败口径/淘汰排序，无需实例锁）。

    latest_terminal 为该实例最近一次终态结果（SUCCESS/FAILURE）及其时间；
    同一实例先失败后成功时以最新终态为准，因此同一实例只计一次。
    """

    latest_event_at: float
    version: int
    state: PipelineState
    terminal_kind: str | None = None      # "SUCCESS" / "FAILURE" / None
    terminal_at: float | None = None


@dataclass
class _PipelineAggregate:
    """单个管道的增量聚合状态。仅在持有该管道锁时访问。"""

    pipeline_id: str
    latest_run_id: str | None = None
    latest_state: PipelineState | None = None
    latest_event_at: float | None = None
    latest_version: int | None = None
    last_success_at: float | None = None
    consecutive_failures: int = 0
    # 管道成功高水位：任意实例成功时间的最大值（单调不减）
    success_high_water: float | None = None
    # 当前保留实例的终态镜像：run_id -> ("SUCCESS"/"FAILURE", 终态时间)
    terminal_by_run: dict[str, tuple[str, float]] = field(default_factory=dict)
    # 派生集合：最近终态为失败且晚于成功高水位的实例（即连续失败口径）
    failed_run_ids: set[str] = field(default_factory=set)
    # 当前保留的实例集合
    run_ids: set[str] = field(default_factory=set)
    # 实例镜像：run_id -> 最近事件时间/版本/状态/终态（供淘汰排序与最近运行重算）
    run_meta: dict[str, _RunMeta] = field(default_factory=dict)
    evicted_runs: int = 0


class PipelineRegistry:
    """管道聚合注册表：维护 pipeline_id -> 增量聚合状态。"""

    def __init__(
        self,
        *,
        retained_run_limit: int = 1000,
        store: "PipelineStore | None" = None,
    ) -> None:
        if retained_run_limit < 1:
            raise ValueError("retained_run_limit 必须 >= 1")
        self._aggs: dict[str, _PipelineAggregate] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._registry_lock = threading.Lock()
        self.retained_run_limit = retained_run_limit
        # store 由 PipelineStore 在构造时反向注入（仅用于快照读取实例告警）
        self._store: "PipelineStore | None" = store

    def _bind_store(self, store: "PipelineStore") -> None:
        self._store = store

    def lock_for(self, pipeline_id: str) -> threading.Lock:
        """获取（惰性创建）管道锁；注册表用短锁保护。"""
        lock = self._locks.get(pipeline_id)
        if lock is None:
            with self._registry_lock:
                lock = self._locks.get(pipeline_id)
                if lock is None:
                    lock = threading.Lock()
                    self._locks[pipeline_id] = lock
        return lock

    def get_or_create_agg(self, pipeline_id: str) -> _PipelineAggregate:
        """获取/创建聚合对象。调用方须已持有管道锁。"""
        agg = self._aggs.get(pipeline_id)
        if agg is None:
            agg = _PipelineAggregate(pipeline_id=pipeline_id)
            self._aggs[pipeline_id] = agg
        return agg

    def on_run_applied(self, pipeline_id: str, run_id: str) -> None:
        """登记实例归属（须在管道锁内调用）。"""
        agg = self.get_or_create_agg(pipeline_id)
        agg.run_ids.add(run_id)

    def apply_run_update(
        self,
        pipeline_id: str,
        *,
        run_id: str,
        state: PipelineState,
        latest_event_at: float,
        version: int,
        failed_outcome: bool,
        succeeded: bool,
    ) -> None:
        """根据刚应用完事件的实例状态刷新管道聚合（须在管道锁内）。

        由 PipelineStore 在“实例锁 + 管道锁”双锁临界区末尾调用，
        传入的均为本次事件应用后的最终值，保证聚合与实例状态自洽。
        """
        agg = self.get_or_create_agg(pipeline_id)
        agg.run_ids.add(run_id)
        meta = agg.run_meta.get(run_id)
        if meta is None:
            meta = _RunMeta(
                latest_event_at=latest_event_at, version=version, state=state
            )
            agg.run_meta[run_id] = meta
        else:
            meta.latest_event_at = latest_event_at
            meta.version = version
            meta.state = state

        # 最近一次运行：按 (latest_event_at, version) 取最新；
        # 同一事件时间下以更高 version 为准，同 run 的事件天然 version 递增。
        if (
            agg.latest_event_at is None
            or latest_event_at > agg.latest_event_at
            or (
                latest_event_at == agg.latest_event_at
                and (agg.latest_version is None or version >= agg.latest_version)
            )
        ):
            agg.latest_event_at = latest_event_at
            agg.latest_version = version
            agg.latest_run_id = run_id
            agg.latest_state = state

        # 更新该实例的最近终态结果（STARTED 等非终态不影响连续失败口径）
        terminal_kind: str | None = None
        if succeeded:
            terminal_kind = "SUCCESS"
        elif failed_outcome:
            terminal_kind = "FAILURE"
        if terminal_kind is not None:
            prev = agg.terminal_by_run.get(run_id)
            # 实例内部事件已按 version 串行，此处只可能收到更新的终态；
            # 若收到同时间更早的终态（不会发生于同实例，防御性保留较新者）
            if prev is None or latest_event_at >= prev[1]:
                agg.terminal_by_run[run_id] = (terminal_kind, latest_event_at)
            meta.terminal_kind, meta.terminal_at = agg.terminal_by_run[run_id]
            self._refresh_consecutive_failures(agg)

    @staticmethod
    def _refresh_consecutive_failures(agg: _PipelineAggregate) -> None:
        """依据成功高水位与各实例最近终态重算连续失败集合。

        口径：最近一次成功时间 = 所有 SUCCESS 终态的最大时间；
        连续失败数 = 最近终态为 FAILURE 且失败时间严格晚于该成功时间的实例数。
        从未成功时高水位为 None，全部 FAILURE 终态实例计入。
        每次终态更新后在管道锁内全量重算当前保留成员（O(保留量)），
        因而跨实例并发导致的处理顺序任意都不会产生错误口径。
        """
        high = agg.success_high_water
        for _rid, (kind, at) in agg.terminal_by_run.items():
            if kind == "SUCCESS" and (high is None or at > high):
                high = at
        agg.success_high_water = high
        agg.last_success_at = high
        failed = {
            rid
            for rid, (kind, at) in agg.terminal_by_run.items()
            if kind == "FAILURE" and (high is None or at > high)
        }
        agg.failed_run_ids = failed
        agg.consecutive_failures = len(failed)

    def evict_if_needed(self, pipeline_id: str) -> list[str]:
        """按保留策略淘汰最旧实例（须在管道锁内，仅操作缓存，不获取实例锁）。

        返回被淘汰的 run_id 列表；调用方（store）随后按锁序摘除实例记录。
        增量计数器的历史贡献处理：被淘汰实例从“当前连续失败段”集合移除；
        最近成功时间等历史口径不回滚（口径见模块文档）。
        """
        agg = self._aggs.get(pipeline_id)
        if agg is None or len(agg.run_ids) <= self.retained_run_limit:
            return []

        # 选出 latest_event_at 最小（最旧）的若干实例；同时间按 run_id 定序保证确定性
        members = sorted(
            agg.run_ids,
            key=lambda rid: (agg.run_meta[rid].latest_event_at, rid),
        )
        excess = len(agg.run_ids) - self.retained_run_limit
        victims = members[:excess]
        for rid in victims:
            agg.run_ids.discard(rid)
            agg.failed_run_ids.discard(rid)
            agg.run_meta.pop(rid, None)
            agg.terminal_by_run.pop(rid, None)
            agg.evicted_runs += 1
        # 终态镜像变更后按统一口径重算（成功高水位可能来自被淘汰的旧成功实例，
        # 其值仍作为全局最近成功时间保留；这里只重算失败集合）
        failed = {
            r
            for r, (kind, at) in agg.terminal_by_run.items()
            if kind == "FAILURE"
            and (agg.success_high_water is None or at > agg.success_high_water)
        }
        agg.failed_run_ids = failed
        agg.consecutive_failures = len(failed)
        if agg.latest_run_id in victims:
            self._recompute_latest(agg)
        return victims

    def _recompute_latest(self, agg: _PipelineAggregate) -> None:
        """从缓存镜像重算“最近一次运行”（仅管道锁内使用）。"""
        agg.latest_run_id = None
        agg.latest_state = None
        agg.latest_event_at = None
        agg.latest_version = None
        for rid, meta in agg.run_meta.items():
            if (
                agg.latest_event_at is None
                or meta.latest_event_at > agg.latest_event_at
                or (
                    meta.latest_event_at == agg.latest_event_at
                    and meta.version >= (agg.latest_version or -1)
                )
            ):
                agg.latest_event_at = meta.latest_event_at
                agg.latest_version = meta.version
                agg.latest_run_id = rid
                agg.latest_state = meta.state

    def member_ids_locked(self, pipeline_id: str) -> list[str] | None:
        """管道锁内返回成员列表的有序拷贝；管道不存在返回 None。"""
        agg = self._aggs.get(pipeline_id)
        if agg is None:
            return None
        return sorted(agg.run_ids)

    def _collect_alerts(
        self, run_ids: list[str]
    ) -> tuple[list[ActiveAlert], bool, bool]:
        """逐实例加锁读取快照并汇总活动告警（调用时不得持有管道锁）。"""
        assert self._store is not None
        active: list[ActiveAlert] = []
        failure_active = False
        staleness_active = False
        for rid in run_ids:
            snap: "PipelineStatus | None" = self._store.get_status(rid)
            if snap is None:
                continue
            if snap.alert_active:
                failure_active = True
                active.append(
                    ActiveAlert(
                        kind=AlertKind.FAILURE,
                        pipeline_run_id=rid,
                        reason=snap.alert_reason or "实例处于失败态",
                        error_class=snap.last_error_class,
                        error_message=snap.last_error_message,
                    )
                )
            if snap.staleness_alert_active:
                staleness_active = True
                assessment = snap.staleness_assessment
                active.append(
                    ActiveAlert(
                        kind=AlertKind.STALENESS,
                        pipeline_run_id=rid,
                        reason=snap.staleness_alert_reason or "实例已失新鲜",
                        basis=assessment.basis if assessment else None,
                        basis_time=assessment.basis_time if assessment else None,
                        age_seconds=assessment.age_seconds if assessment else None,
                        threshold_seconds=(
                            assessment.threshold_seconds if assessment else None
                        ),
                    )
                )
        return active, failure_active, staleness_active

    def get_health(self, pipeline_id: str) -> PipelineHealth | None:
        """读取管道健康汇总快照；管道不存在返回 None。

        先在管道锁内原子拷贝标量口径与成员集合，释放管道锁后再逐实例加锁
        读取告警，杜绝 run<->pipeline 反向嵌套死锁；标量口径来自某一次已
        提交的聚合状态，告警明细来自各自一致快照，均无半截中间态。
        """
        with self.lock_for(pipeline_id):
            agg = self._aggs.get(pipeline_id)
            if agg is None:
                return None
            scalar = (
                agg.pipeline_id,
                agg.latest_run_id,
                agg.latest_state,
                agg.latest_event_at,
                agg.last_success_at,
                agg.consecutive_failures,
                len(agg.run_ids),
                agg.evicted_runs,
            )
            member_ids = sorted(agg.run_ids)

        active_alerts: tuple[ActiveAlert, ...] = ()
        failure_active = False
        staleness_active = False
        if self._store is not None:
            alerts, failure_active, staleness_active = self._collect_alerts(member_ids)
            active_alerts = tuple(alerts)

        return PipelineHealth(
            pipeline_id=scalar[0],
            latest_run_id=scalar[1],
            latest_state=scalar[2],
            latest_event_at=scalar[3],
            last_success_at=scalar[4],
            consecutive_failures=scalar[5],
            failure_alert_active=failure_active,
            staleness_alert_active=staleness_active,
            active_alerts=active_alerts,
            run_count=scalar[6],
            retained_run_limit=self.retained_run_limit,
            evicted_runs=scalar[7],
        )

    def list_pipeline_ids(self) -> list[str]:
        with self._registry_lock:
            return sorted(self._aggs.keys())

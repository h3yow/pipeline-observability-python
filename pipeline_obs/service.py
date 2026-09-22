"""观测服务门面：并发安全地受理事件、驱动重试与输出汇总。

并发一致性：所有读取-检查-写入都在同一把可重入锁内完成（check-then-act
原子化），因此并发重复上报只会有一份生效，汇总快照不会观察到中间态，
计数器也不会丢失更新。
"""
from __future__ import annotations

import threading

from .alerts import AlertManager
from .clock import Clock, system_clock
from .errors import EventRejectedError
from .models import (
    AlertType,
    EventType,
    IngestResult,
    PipelineEvent,
    PipelineRecord,
    PipelineSummary,
    RejectReason,
    RunRecord,
    RunStatus,
    RunSummary,
)
from .retry import RetryPolicy
from .state_machine import apply_event, ensure_run


class ObservabilityService:
    def __init__(self, clock: Clock | None = None,
                 retry_policy: RetryPolicy | None = None) -> None:
        self._clock: Clock = clock or system_clock
        self._pipelines: dict[str, PipelineRecord] = {}
        self._alerts = AlertManager()
        self._retry = retry_policy or RetryPolicy()
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 配置
    # ------------------------------------------------------------------
    def register_pipeline(self, pipeline_id: str, *,
                          freshness_threshold: float = 300.0,
                          max_retries: int = 3,
                          base_delay: float = 1.0,
                          backoff_factor: float = 2.0) -> None:
        with self._lock:
            self._pipelines[pipeline_id] = PipelineRecord(
                pipeline_id=pipeline_id,
                freshness_threshold=freshness_threshold,
                max_retries=max_retries,
                base_delay=base_delay,
                backoff_factor=backoff_factor,
            )

    def _get_pipeline(self, pipeline_id: str) -> PipelineRecord:
        pipeline = self._pipelines.get(pipeline_id)
        if pipeline is None:
            raise EventRejectedError(
                RejectReason.UNKNOWN_PIPELINE,
                f"pipeline {pipeline_id!r} not registered",
            )
        return pipeline

    # ------------------------------------------------------------------
    # 事件受理
    # ------------------------------------------------------------------
    def ingest(self, event: PipelineEvent) -> IngestResult:
        """受理单个事件（幂等、线程安全）。

        - 相同 event_id 重复上报：返回 deduped=True（不产生任何状态变化）。
        - 非法/乱序/冲突上报：返回 accepted=False 与可区分原因。
        """
        with self._lock:
            try:
                pipeline = self._get_pipeline(event.pipeline_id)
            except EventRejectedError as exc:
                return IngestResult(
                    accepted=False, reason=exc.reason,
                    run_status=RunStatus.RUNNING, detail=exc.detail,
                )

            run = ensure_run(pipeline, event)

            # 1) 事件 ID 幂等：同一实例的重复上报不计数、不回退。
            if event.event_id in run.seen_event_ids:
                return IngestResult(
                    accepted=True, reason=None, run_status=run.status,
                    detail=f"duplicate event_id={event.event_id}; "
                           f"idempotently ignored",
                    deduped=True,
                )

            # 2) 未到重试时刻的下一次尝试：拒绝（防止提前/并发冲闯）。
            if (event.event_type is EventType.ATTEMPT_STARTED
                    and run.status is RunStatus.RUNNING
                    and run.retry is not None
                    and run.retry.next_retry_at is not None
                    and not run.retry_open
                    and event.event_time < run.retry.next_retry_at):
                return IngestResult(
                    accepted=False,
                    reason=RejectReason.RETRY_NOT_DUE,
                    run_status=run.status,
                    detail=f"retry not due: event_time={event.event_time} "
                           f"< next_retry_at={run.retry.next_retry_at}",
                )

            try:
                transition = apply_event(pipeline, run, event, self._retry)
            except EventRejectedError as exc:
                return IngestResult(
                    accepted=False, reason=exc.reason,
                    run_status=run.status, detail=exc.detail,
                )

            # 3) 事件已生效，登记幂等键并处理副作用。
            run.seen_event_ids.add(event.event_id)

            if transition.run_succeeded:
                pipeline.last_success_at = event.event_time
                # 恢复：解除该 run 的失败告警与管道时效告警。
                self._alerts.resolve_all_for_run(
                    pipeline.pipeline_id, run.run_id, event.event_time
                )
                self._alerts.resolve(
                    pipeline.pipeline_id, None,
                    AlertType.STALE_PIPELINE, event.event_time,
                )
            elif transition.run_failed:
                detail = ""
                if transition.outcome_error is not None:
                    e = transition.outcome_error
                    exhausted = run.retry is not None and run.retry.exhausted
                    kind = "retry exhausted" if exhausted else "fatal"
                    detail = f"{kind}: [{e.code}] {e.message}"
                self._alerts.raise_alert(
                    pipeline, run, AlertType.RUN_FAILED,
                    event.event_time, detail,
                )

            return IngestResult(
                accepted=True, reason=None, run_status=run.status,
                detail="",
            )

    # ------------------------------------------------------------------
    # 重试调度推进
    # ------------------------------------------------------------------
    def run_due_retries(self) -> int:
        """将所有已到退避时刻的重试置为“可开始下一次尝试”。

        返回状态被推进的 run 数量。达到上限的 run 已经是 FAILED 终态，
        不会出现在这里，因此永不继续重试。
        """
        now = self._clock()
        triggered = 0
        with self._lock:
            for pipeline in self._pipelines.values():
                for run in pipeline.runs.values():
                    retry = run.retry
                    if (run.status is RunStatus.RUNNING
                            and retry is not None
                            and retry.next_retry_at is not None
                            and not retry.exhausted
                            and not run.retry_open
                            and now >= retry.next_retry_at):
                        run.retry_open = True
                        triggered += 1
        return triggered

    # ------------------------------------------------------------------
    # 时效性评估
    # ------------------------------------------------------------------
    def _is_pipeline_stale(self, pipeline: PipelineRecord,
                           now: float) -> bool:
        baseline = pipeline.last_success_at
        if baseline is None:
            # 从未成功：只要存在已启动的 run 且超过阈值即视为 stale。
            starts = [r.started_at for r in pipeline.runs.values()
                      if r.started_at is not None]
            if not starts:
                return False
            baseline = min(starts)
        return (now - baseline) > pipeline.freshness_threshold

    def evaluate_freshness(self) -> int:
        """评估所有管道时效性，维护 STALE_PIPELINE 告警。返回变更条数。"""
        now = self._clock()
        changes = 0
        with self._lock:
            for pipeline in self._pipelines.values():
                if self._is_pipeline_stale(pipeline, now):
                    before = len(self._alerts.active_alerts(
                        pipeline.pipeline_id))
                    self._alerts.raise_alert(
                        pipeline, None, AlertType.STALE_PIPELINE, now,
                        f"no success within "
                        f"{pipeline.freshness_threshold}s",
                    )
                    after = len(self._alerts.active_alerts(
                        pipeline.pipeline_id))
                    changes += after - before
                else:
                    if self._alerts.resolve(
                        pipeline.pipeline_id, None,
                        AlertType.STALE_PIPELINE, now,
                    ):
                        changes += 1
        return changes

    # ------------------------------------------------------------------
    # 下游查询（快照在锁内一次性构建）
    # ------------------------------------------------------------------
    def get_run_summary(self, pipeline_id: str, run_id: str) -> RunSummary:
        with self._lock:
            pipeline = self._get_pipeline(pipeline_id)
            run = pipeline.runs.get(run_id)
            if run is None:
                raise KeyError(f"run {run_id!r} not found")
            alerts = tuple(self._alerts.active_alerts(
                pipeline_id, run_id))
            retry = run.retry
            return RunSummary(
                pipeline_id=pipeline_id,
                run_id=run_id,
                status=run.status,
                started_at=run.started_at,
                finished_at=run.finished_at,
                attempt_count=len(run.attempts),
                failure_count=retry.failure_count if retry else 0,
                retry_exhausted=bool(retry and retry.exhausted),
                next_retry_at=(retry.next_retry_at if retry else None),
                last_error_code=(retry.last_error.code
                                 if retry and retry.last_error else None),
                last_error_category=(retry.last_error.category
                                     if retry and retry.last_error else None),
                active_alerts=alerts,
            )

    def get_pipeline_summary(self, pipeline_id: str) -> PipelineSummary:
        with self._lock:
            pipeline = self._get_pipeline(pipeline_id)
            now = self._clock()
            runs = list(pipeline.runs.values())
            active = tuple(self._alerts.active_alerts(pipeline_id))
            return PipelineSummary(
                pipeline_id=pipeline_id,
                last_success_at=pipeline.last_success_at,
                freshness_threshold=pipeline.freshness_threshold,
                is_stale=self._is_pipeline_stale(pipeline, now),
                total_runs=len(runs),
                running_runs=sum(
                    r.status is RunStatus.RUNNING for r in runs),
                succeeded_runs=sum(
                    r.status is RunStatus.SUCCEEDED for r in runs),
                failed_runs=sum(
                    r.status is RunStatus.FAILED for r in runs),
                active_alerts=active,
            )

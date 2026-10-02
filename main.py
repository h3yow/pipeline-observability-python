"""FastAPI 入口：暴露管道观测的事件接收、状态查询、管道健康与失新鲜巡检接口。"""

from __future__ import annotations

import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from pipeline_obs import (
    ErrorClass,
    EventType,
    IngestStatus,
    LookupStatus,
    ObservabilityService,
    PipelineEvent,
)

app = FastAPI(title="pipeline-observability")
service = ObservabilityService()


class EventIn(BaseModel):
    pipeline_run_id: str
    event_id: str
    event_type: EventType
    occurred_at: float | None = None
    version: int
    attempt: int = 1
    error_class: ErrorClass | None = None
    error_message: str | None = None
    pipeline_id: str | None = None  # 可选：归属管道；缺省按“每实例自成一管道”


@app.get("/")
def read_root():
    return {"service": "pipeline-observability"}


@app.post("/events")
def post_event(payload: EventIn):
    event = PipelineEvent(
        pipeline_run_id=payload.pipeline_run_id,
        event_id=payload.event_id,
        event_type=payload.event_type,
        occurred_at=payload.occurred_at if payload.occurred_at is not None else time.time(),
        version=payload.version,
        attempt=payload.attempt,
        error_class=payload.error_class,
        error_message=payload.error_message,
        pipeline_id=payload.pipeline_id,
    )
    result = service.ingest(event)
    body = {
        "status": result.status.value,
        "state": result.state.value,
    }
    if result.reason is not None:
        body["reason"] = result.reason.value
    if result.detail:
        body["detail"] = result.detail
    if result.retry is not None:
        body["retry"] = {
            "should_retry": result.retry.should_retry,
            "next_attempt": result.retry.next_attempt,
            "delay_seconds": result.retry.delay_seconds,
            "reason": result.retry.reason,
        }
    # 冲突/拒绝返回 409，重复上报幂等返回 200
    status_code = 409 if result.status is IngestStatus.REJECTED else 200
    return JSONResponse(body, status_code=status_code)


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    # 三态查询：在窗口内返回完整快照；已淘汰返回 410 Gone（结论明确、不报错、不残缺）；
    # 从未见过返回 404。旧调用方仍可据 200/404 使用；410 是新增强口径。
    lookup = service.lookup_run(run_id)
    if lookup.status is LookupStatus.EVICTED:
        return JSONResponse(
            {"detail": "run evicted by retention policy", "reason": "RUN_EVICTED",
             "message": lookup.detail},
            status_code=410,
        )
    snap = lookup.snapshot
    if snap is None:
        return JSONResponse({"detail": "run not found"}, status_code=404)
    # 旧字段全部保留，仅在末尾追加新字段（向后兼容）
    return {
        "pipeline_run_id": snap.pipeline_run_id,
        "state": snap.state.value,
        "current_attempt": snap.current_attempt,
        "applied_version": snap.applied_version,
        "last_success_at": snap.last_success_at,
        "started_at": snap.started_at,
        "latest_event_at": snap.latest_event_at,
        "failure_count": snap.failure_count,
        "next_retry_at": snap.next_retry_at,
        "retry_exhausted": snap.retry_exhausted,
        "alert_active": snap.alert_active,
        "alert_reason": snap.alert_reason,
        # —— 新增：管道归属与失新鲜告警明细 ——
        "pipeline_id": snap.pipeline_id,
        "active_alert_kinds": list(snap.active_alert_kinds),
        "staleness_alert_active": snap.staleness_alert_active,
        "staleness_alert_reason": snap.staleness_alert_reason,
        "staleness_raised_count": snap.staleness_raised_count,
        # —— 新增：最近一次失败原因（从未失败/已恢复成功后为 null）——
        "last_error_class": snap.last_error_class,
        "last_error_message": snap.last_error_message,
        "failure_terminal_reason": snap.failure_terminal_reason,
        "last_failure_at": snap.last_failure_at,
        # —— 新增：失新鲜恢复时刻与同版恢复/新鲜依据 ——
        "staleness_recovered_at": snap.staleness_recovered_at,
        "staleness_recovery_reason": snap.staleness_recovery_reason,
    }


@app.get("/runs")
def list_runs():
    return [s.pipeline_run_id for s in service.all_status()]


@app.get("/pipelines")
def list_pipelines():
    """列出全部管道 ID。"""
    return service.list_pipeline_ids()


@app.get("/pipelines/{pipeline_id}/health")
def get_pipeline_health(pipeline_id: str):
    """管道级健康汇总：最近运行、最近成功、连续失败数、活动告警（两类分列）。"""
    h = service.pipeline_health(pipeline_id)
    if h is None:
        return JSONResponse({"detail": "pipeline not found"}, status_code=404)
    return {
        "pipeline_id": h.pipeline_id,
        "run_count": h.run_count,
        "latest_run_id": h.latest_run_id,
        "latest_run_state": h.latest_run_state.value
        if h.latest_run_state is not None
        else None,
        "latest_event_at": h.latest_event_at,
        "last_success_at": h.last_success_at,
        "consecutive_failures": h.consecutive_failures,
        "active_failure_alert_count": h.active_failure_alert_count,
        "active_staleness_alert_count": h.active_staleness_alert_count,
        "has_failure_alert": h.has_failure_alert,
        "has_staleness_alert": h.has_staleness_alert,
        "staleness_threshold_seconds": h.staleness_threshold_seconds,
        "checked_at": h.checked_at,
        # —— 新增：管道最近一次失败原因（从未失败/全部恢复后为 null）——
        "latest_failure_run_id": h.latest_failure_run_id,
        "latest_failure_at": h.latest_failure_at,
        "latest_error_class": h.latest_error_class,
        "latest_error_message": h.latest_error_message,
        "latest_failure_terminal_reason": h.latest_failure_terminal_reason,
        # —— 新增：保留口径与该管道累计淘汰量 ——
        "evicted_run_count": h.evicted_run_count,
        "retention": (
            None
            if h.retention is None
            else {
                "max_runs": h.retention.max_runs,
                "current_runs": h.retention.current_runs,
                "total_evicted_runs": h.retention.total_evicted_runs,
            }
        ),
        "active_alerts": [
            {
                "pipeline_run_id": a.pipeline_run_id,
                "kind": a.kind.value,
                "reason": a.reason,
                "since": a.since,
                "version": a.version,
                # 失败告警附带最近一次失败原因（失新鲜告警恒为 null）
                "error_class": a.error_class,
                "error_message": a.error_message,
                "failure_terminal_reason": a.failure_terminal_reason,
                "last_failure_at": a.last_failure_at,
            }
            for a in service.active_alerts(pipeline_id)
        ],
    }


@app.get("/retention")
def get_retention():
    """对外披露历史保留口径、当前保留数与累计淘汰量。"""
    info = service.retention_info()
    return {
        "max_runs": info.max_runs,
        "current_runs": info.current_runs,
        "total_evicted_runs": info.total_evicted_runs,
    }


class SweepIn(BaseModel):
    pipeline_id: str | None = None
    run_id: str | None = None
    fresh_within_seconds: float | None = None
    now: float | None = None  # 可选：注入巡检时刻，便于确定性触发/测试


@app.post("/staleness/sweep")
def sweep_staleness(payload: SweepIn):
    """按需触发失新鲜巡检（不依赖真实 sleep；now 可显式注入）。"""
    verdicts = service.sweep_staleness(
        run_id=payload.run_id,
        pipeline_id=payload.pipeline_id,
        fresh_within_seconds=payload.fresh_within_seconds,
        now=payload.now,
    )
    return {
        rid: {
            "stale": v.stale,
            "baseline_kind": v.baseline_kind.value,
            "baseline_at": v.baseline_at,
            "now": v.now,
            "threshold_seconds": v.threshold_seconds,
            "age_seconds": v.age_seconds,
            "reason": v.reason,
        }
        for rid, v in verdicts.items()
    }

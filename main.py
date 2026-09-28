"""FastAPI 入口：暴露管道观测的事件接收与状态查询接口。"""

from __future__ import annotations

import time

from fastapi import FastAPI
from pydantic import BaseModel

from pipeline_obs import (
    ErrorClass,
    EventType,
    IngestStatus,
    ObservabilityService,
    PipelineEvent,
)

app = FastAPI(title="pipeline-observability")
service = ObservabilityService(default_fresh_within_seconds=300.0)


class EventIn(BaseModel):
    pipeline_run_id: str
    event_id: str
    event_type: EventType
    occurred_at: float | None = None
    version: int
    attempt: int = 1
    error_class: ErrorClass | None = None
    error_message: str | None = None
    pipeline_id: str | None = None


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
    from fastapi.responses import JSONResponse

    return JSONResponse(body, status_code=status_code)


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    snap = service.status(run_id)
    if snap is None:
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": "run not found"}, status_code=404)
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
        "pipeline_id": snap.pipeline_id,
        "last_error_class": (
            snap.last_error_class.value if snap.last_error_class else None
        ),
        "last_error_message": snap.last_error_message,
        "staleness_alert_active": snap.staleness_alert_active,
        "staleness_alert_reason": snap.staleness_alert_reason,
        "staleness_assessment": (
            {
                "stale": snap.staleness_assessment.stale,
                "basis": snap.staleness_assessment.basis,
                "basis_time": snap.staleness_assessment.basis_time,
                "now": snap.staleness_assessment.now,
                "age_seconds": snap.staleness_assessment.age_seconds,
                "threshold_seconds": snap.staleness_assessment.threshold_seconds,
                "reason": snap.staleness_assessment.reason,
            }
            if snap.staleness_assessment
            else None
        ),
    }


@app.get("/runs")
def list_runs():
    return [s.pipeline_run_id for s in service.all_status()]


def _alert_dict(a):
    return {
        "kind": a.kind.value,
        "pipeline_run_id": a.pipeline_run_id,
        "reason": a.reason,
        "since_version": a.since_version,
        "since_time": a.since_time,
        "basis": a.basis,
        "basis_time": a.basis_time,
        "age_seconds": a.age_seconds,
        "threshold_seconds": a.threshold_seconds,
        "error_class": a.error_class.value if a.error_class else None,
        "error_message": a.error_message,
    }


@app.get("/pipelines/{pipeline_id}/health")
def get_pipeline_health(pipeline_id: str):
    """管道级健康汇总：最近运行、最近成功、连续失败数、活动告警及判定依据。"""
    health = service.pipeline_health(pipeline_id)
    if health is None:
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": "pipeline not found"}, status_code=404)
    return {
        "pipeline_id": health.pipeline_id,
        "latest_run_id": health.latest_run_id,
        "latest_state": health.latest_state.value if health.latest_state else None,
        "latest_event_at": health.latest_event_at,
        "last_success_at": health.last_success_at,
        "consecutive_failures": health.consecutive_failures,
        "failure_alert_active": health.failure_alert_active,
        "staleness_alert_active": health.staleness_alert_active,
        "active_alerts": [_alert_dict(a) for a in health.active_alerts],
        "run_count": health.run_count,
        "retained_run_limit": health.retained_run_limit,
        "evicted_runs": health.evicted_runs,
    }


@app.get("/pipelines")
def list_pipelines():
    return service.list_pipelines()


@app.post("/staleness-sweep")
def staleness_sweep(
    pipeline_id: str | None = None,
    fresh_within_seconds: float | None = None,
    at: float | None = None,
):
    """确定性失新鲜巡检：可显式指定判定时间 at 与阈值，不依赖真实等待。"""
    stale = service.run_staleness_sweep(
        now=at,
        fresh_within_seconds=fresh_within_seconds,
        pipeline_id=pipeline_id,
    )
    return {
        "evaluated_stale": [
            {
                "pipeline_run_id": s.pipeline_run_id,
                "pipeline_id": s.pipeline_id,
                "state": s.state.value,
                "staleness_alert_active": s.staleness_alert_active,
                "staleness_alert_reason": s.staleness_alert_reason,
                "assessment": (
                    {
                        "stale": s.staleness_assessment.stale,
                        "basis": s.staleness_assessment.basis,
                        "basis_time": s.staleness_assessment.basis_time,
                        "now": s.staleness_assessment.now,
                        "age_seconds": s.staleness_assessment.age_seconds,
                        "threshold_seconds": s.staleness_assessment.threshold_seconds,
                        "reason": s.staleness_assessment.reason,
                    }
                    if s.staleness_assessment
                    else None
                ),
            }
            for s in stale
        ]
    }

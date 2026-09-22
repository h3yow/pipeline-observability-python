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
    }


@app.get("/runs")
def list_runs():
    return [s.pipeline_run_id for s in service.all_status()]

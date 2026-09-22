"""FastAPI 入口：暴露管道观测能力的 HTTP 接口。"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from pipeline_obs.models import (
    ErrorCategory,
    ErrorInfo,
    EventType,
    PipelineEvent,
)
from pipeline_obs.service import ObservabilityService

app = FastAPI(title="Pipeline Observability", version="0.1.0")

# 单进程内共享一个线程安全服务实例。
svc = ObservabilityService()
svc.register_pipeline("demo", freshness_threshold=300.0)


class ErrorBody(BaseModel):
    code: str
    message: str = ""
    category: ErrorCategory = ErrorCategory.RETRYABLE


class EventBody(BaseModel):
    event_id: str
    pipeline_id: str
    run_id: str
    event_type: EventType
    event_time: float = Field(
        ..., description="UNIX 秒；乱序（更旧）事件将被拒绝"
    )
    attempt_no: int | None = None
    error: ErrorBody | None = None


class PipelineBody(BaseModel):
    freshness_threshold: float = 300.0
    max_retries: int = 3
    base_delay: float = 1.0
    backoff_factor: float = 2.0


@app.post("/pipelines/{pipeline_id}")
def register_pipeline(pipeline_id: str, body: PipelineBody):
    svc.register_pipeline(
        pipeline_id,
        freshness_threshold=body.freshness_threshold,
        max_retries=body.max_retries,
        base_delay=body.base_delay,
        backoff_factor=body.backoff_factor,
    )
    return {"registered": pipeline_id}


@app.post("/events")
def ingest_event(body: EventBody):
    error = None
    if body.error is not None:
        error = ErrorInfo(
            code=body.error.code,
            message=body.error.message,
            category=body.error.category,
        )
    event = PipelineEvent(
        event_id=body.event_id,
        pipeline_id=body.pipeline_id,
        run_id=body.run_id,
        event_type=body.event_type,
        event_time=body.event_time,
        attempt_no=body.attempt_no,
        error=error,
    )
    result = svc.ingest(event)
    status_code = 200 if result.accepted else 409
    return JSONResponse(
        status_code=status_code,
        content={
            "accepted": result.accepted,
            "deduped": result.deduped,
            "reason": result.reason.value if result.reason else None,
            "run_status": result.run_status.value,
            "detail": result.detail,
        },
    )


@app.post("/tick")
def tick():
    """推进到期重试并刷新时效性（由调度器周期性调用）。"""
    due = svc.run_due_retries()
    freshness_changes = svc.evaluate_freshness()
    return {"retries_triggered": due, "freshness_changes": freshness_changes}


@app.get("/pipelines/{pipeline_id}/runs/{run_id}")
def run_summary(pipeline_id: str, run_id: str):
    summary = svc.get_run_summary(pipeline_id, run_id)
    return _dump_run_summary(summary)


@app.get("/pipelines/{pipeline_id}")
def pipeline_summary(pipeline_id: str):
    summary = svc.get_pipeline_summary(pipeline_id)
    return {
        "pipeline_id": summary.pipeline_id,
        "last_success_at": summary.last_success_at,
        "freshness_threshold": summary.freshness_threshold,
        "is_stale": summary.is_stale,
        "total_runs": summary.total_runs,
        "running_runs": summary.running_runs,
        "succeeded_runs": summary.succeeded_runs,
        "failed_runs": summary.failed_runs,
        "active_alerts": [_dump_alert(a) for a in summary.active_alerts],
    }


def _dump_alert(a) -> dict:
    return {
        "alert_id": a.alert_id,
        "pipeline_id": a.pipeline_id,
        "run_id": a.run_id,
        "alert_type": a.alert_type.value,
        "status": a.status.value,
        "created_at": a.created_at,
        "updated_at": a.updated_at,
        "detail": a.detail,
    }


def _dump_run_summary(s) -> dict:
    return {
        "pipeline_id": s.pipeline_id,
        "run_id": s.run_id,
        "status": s.status.value,
        "started_at": s.started_at,
        "finished_at": s.finished_at,
        "attempt_count": s.attempt_count,
        "failure_count": s.failure_count,
        "retry_exhausted": s.retry_exhausted,
        "next_retry_at": s.next_retry_at,
        "last_error_code": s.last_error_code,
        "last_error_category": (
            s.last_error_category.value if s.last_error_category else None
        ),
        "active_alerts": [_dump_alert(a) for a in s.active_alerts],
    }

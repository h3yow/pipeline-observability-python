"""HTTP API 端到端冒烟测试。"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def client():
    # 每个测试重新导入 app，得到干净的服务状态。
    main = importlib.import_module("main")
    from fastapi.testclient import TestClient
    return TestClient(main.app)


def test_event_lifecycle_and_reject_reason_over_http(client):
    # demo 管道在应用启动时已注册；注册一个自定义管道
    r = client.post("/pipelines/pl", json={
        "freshness_threshold": 100.0, "max_retries": 1,
        "base_delay": 0.0, "backoff_factor": 2.0,
    })
    assert r.status_code == 200

    def post(payload):
        return client.post("/events", json=payload).json()

    post({"event_id": "s", "pipeline_id": "pl", "run_id": "r",
          "event_type": "RUN_STARTED", "event_time": 1000.0})
    post({"event_id": "a1", "pipeline_id": "pl", "run_id": "r",
          "event_type": "ATTEMPT_STARTED", "event_time": 1000.0,
          "attempt_no": 1})

    # 重复事件（同 event_id）幂等
    dup = post({"event_id": "a1", "pipeline_id": "pl", "run_id": "r",
                "event_type": "ATTEMPT_STARTED", "event_time": 1000.0,
                "attempt_no": 1})
    assert dup["deduped"] is True

    # 乱序事件被拒，409 且原因可区分
    stale = client.post("/events", json={
        "event_id": "x", "pipeline_id": "pl", "run_id": "r",
        "event_type": "ATTEMPT_FAILED", "event_time": 900.0,
        "attempt_no": 1,
        "error": {"code": "T", "message": "t", "category": "RETRYABLE"},
    })
    assert stale.status_code == 409
    assert stale.json()["reason"] == "STALE_EVENT"

    # 未知管道
    ghost = client.post("/events", json={
        "event_id": "g", "pipeline_id": "nope", "run_id": "r",
        "event_type": "RUN_STARTED", "event_time": 1001.0,
    })
    assert ghost.status_code == 409
    assert ghost.json()["reason"] == "UNKNOWN_PIPELINE"

    # 查询汇总
    summary = client.get("/pipelines/pl/runs/r").json()
    assert summary["attempt_count"] == 1
    assert summary["failure_count"] == 0

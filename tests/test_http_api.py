"""HTTP 端到端冒烟：管道健康查询与确定性失新鲜巡检端点。"""

import logging

from fastapi.testclient import TestClient

import main as main_module
from tests.timeline import log_pipeline_health  # noqa: F401  (确保日志工具可用)

logger = logging.getLogger("timeline")


def _client():
    # 每个用例使用独立的服务实例，避免全局单例串数据
    from pipeline_obs import ObservabilityService
    main_module.service = ObservabilityService(default_fresh_within_seconds=10.0)
    return TestClient(main_module.app)


def test_pipeline_health_and_sweep_endpoints():
    client = _client()
    # 上报两个属于同一管道的运行
    for run, etype, ec, ts in [
        ("r1", "STARTED", None, 0.0),
        ("r1", "SUCCEEDED", None, 0.0),
        ("r2", "STARTED", None, 1.0),
        ("r2", "FAILED", "FATAL", 1.0),
    ]:
        payload = {
            "pipeline_run_id": run, "event_id": f"{run}-{etype}-{ts}",
            "event_type": etype, "occurred_at": ts, "version": 1,
            "pipeline_id": "P1",
        }
        if etype == "STARTED":
            payload["version"] = 1
        else:
            payload["version"] = 2
        if ec:
            payload["error_class"] = ec
        r = client.post("/events", json=payload)
        assert r.status_code == 200, r.text

    r = client.get("/pipelines/P1/health")
    assert r.status_code == 200
    body = r.json()
    logger.info("[HTTP] 管道健康响应：%s", body)
    assert body["latest_run_id"] == "r2"
    assert body["last_success_at"] == 0.0
    assert body["consecutive_failures"] == 1
    assert body["failure_alert_active"] is True
    assert body["staleness_alert_active"] is False
    kinds = {a["kind"] for a in body["active_alerts"]}
    assert kinds == {"FAILURE"}

    # 不存在的管道 404
    assert client.get("/pipelines/nope/health").status_code == 404

    # 确定性巡检：at=12（基线0，age=12>10）
    r = client.post("/staleness-sweep?pipeline_id=P1&at=12")
    assert r.status_code == 200
    stale = r.json()["evaluated_stale"]
    logger.info("[HTTP] 巡检失新鲜实例数=%d", len(stale))
    assert {x["pipeline_run_id"] for x in stale} == {"r1", "r2"}
    for x in stale:
        ass = x["assessment"]
        assert ass["threshold_seconds"] == 10.0
        assert ass["stale"] is True

    body = client.get("/pipelines/P1/health").json()
    assert body["staleness_alert_active"] is True
    assert {a["kind"] for a in body["active_alerts"]} == {"FAILURE", "STALENESS"}

    # 旧端点仍可用且包含原有字段（兼容口径）
    run_body = client.get("/runs/r2").json()
    for key in ("state", "failure_count", "alert_active", "alert_reason",
                "retry_exhausted", "next_retry_at"):
        assert key in run_body, f"旧字段缺失：{key}"
    assert run_body["pipeline_id"] == "P1"
    assert client.get("/runs").status_code == 200

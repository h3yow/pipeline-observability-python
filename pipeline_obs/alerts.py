"""
告警收敛规则：

- 收敛键：``(pipeline_id, run_id, alert_type)``。run_id=None 表示管道级时效告警。
- 同一收敛键处于 ACTIVE 时再次触发 -> 返回既有告警，不新建、不重复计数。
- resolve 幂等：不存在或已解除均为空操作。
- 运行成功解除其 RUN_FAILED 告警（失败终态不会“恢复”，由 STALE 恢复体现）。
  STALE_PIPELINE 在管道重新出现成功后解除。
"""
from __future__ import annotations

from .models import (
    Alert,
    AlertStatus,
    AlertType,
    PipelineRecord,
    RunRecord,
)


class AlertManager:
    def __init__(self) -> None:
        # key: (pipeline_id, run_id|None, alert_type) -> Alert
        self._alerts: dict[tuple[str, str | None, AlertType], Alert] = {}

    def _key(self, pipeline_id: str, run_id: str | None,
             alert_type: AlertType) -> tuple[str, str | None, AlertType]:
        return (pipeline_id, run_id, alert_type)

    def raise_alert(self, pipeline: PipelineRecord, run: RunRecord | None,
                    alert_type: AlertType, now: float,
                    detail: str = "") -> Alert:
        """若同 key 已存在 ACTIVE 告警则收敛返回旧告警，否则新建。"""
        key = self._key(pipeline.pipeline_id,
                        run.run_id if run else None, alert_type)
        existing = self._alerts.get(key)
        if existing is not None and existing.status is AlertStatus.ACTIVE:
            return existing
        alert_id = "alert:" + ":".join(
            [pipeline.pipeline_id, run.run_id if run else "-",
             alert_type.value]
        )
        alert = Alert(
            alert_id=alert_id,
            pipeline_id=pipeline.pipeline_id,
            run_id=run.run_id if run else None,
            alert_type=alert_type,
            status=AlertStatus.ACTIVE,
            created_at=now,
            updated_at=now,
            detail=detail,
        )
        self._alerts[key] = active = alert
        return active

    def resolve(self, pipeline_id: str, run_id: str | None,
                alert_type: AlertType, now: float) -> bool:
        """解除告警；返回是否真正发生 ACTIVE->RESOLVED 的状态变更。"""
        key = self._key(pipeline_id, run_id, alert_type)
        existing = self._alerts.get(key)
        if existing is None or existing.status is not AlertStatus.ACTIVE:
            return False
        # Alert 为 frozen dataclass，用 dataclasses.replace 更新时间戳。
        from dataclasses import replace
        self._alerts[key] = replace(
            existing, status=AlertStatus.RESOLVED, updated_at=now
        )
        return True

    def resolve_all_for_run(self, pipeline_id: str, run_id: str,
                            now: float) -> None:
        for alert_type in (AlertType.RUN_FAILED,):
            self.resolve(pipeline_id, run_id, alert_type, now)

    def active_alerts(self, pipeline_id: str,
                      run_id: str | None = None) -> list[Alert]:
        result = []
        for (pid, rid, _at), alert in self._alerts.items():
            if pid != pipeline_id or alert.status is not AlertStatus.ACTIVE:
                continue
            if run_id is not None and rid != run_id:
                continue
            result.append(alert)
        return result

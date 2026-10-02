"""失新鲜成功恢复后：活动状态与判定依据同版更新，不再自相矛盾、不重复报警、旧结论不回潮。"""

import logging

import pytest

from pipeline_obs import EventType, ObservabilityService, PipelineEvent, RetryPolicy
from tests.timeline import log_event, log_result, log_verdict

logger = logging.getLogger("timeline")


def _svc(threshold=3.0):
    t = {"v": 100.0}
    svc = ObservabilityService(
        retry_policy=RetryPolicy(max_attempts=5, base_delay_seconds=1.0),
        clock=lambda: t["v"],
        fresh_within_seconds=threshold,
        max_runs=None,
    )
    return svc, t


def _ev(run, eid, etype, at, version, *, attempt=1, pid="p"):
    return PipelineEvent(run, eid, etype, occurred_at=at, version=version,
                         attempt=attempt, pipeline_id=pid)


def test_status_and_basis_update_together_after_recovery():
    """恢复后明细给的是恢复/新鲜结论（带恢复时刻与依据），不再停留失新鲜版本。"""
    svc, _t = _svc()
    run = "run-rec"
    svc.ingest(_ev(run, "e1", EventType.STARTED, 100.0, 1))

    v = svc.check_staleness(run, now=104.0)
    log_verdict(run, v)
    snap = svc.status(run)
    assert snap.staleness_alert_active is True
    assert "判定失新鲜" in snap.staleness_alert_reason
    assert snap.active_alert_kinds == ("STALENESS",)

    # 成功恢复：活动标志关闭，同时依据必须刷新为同版“新鲜/恢复”结论。
    log_event(2, _ev(run, "e2", EventType.SUCCEEDED, 105.0, 2))
    log_result(svc.ingest(_ev(run, "e2", EventType.SUCCEEDED, 105.0, 2)))
    snap = svc.status(run)
    logger.info("[恢复后明细] active=%s recovered_at=%s", snap.staleness_alert_active,
                snap.staleness_recovered_at)
    logger.info("[恢复后依据] %s", snap.staleness_alert_reason)
    assert snap.staleness_alert_active is False
    assert snap.staleness_recovered_at == 105.0
    assert snap.staleness_alert_reason is not None
    assert "在新鲜窗口内" in snap.staleness_alert_reason
    assert "判定失新鲜" not in snap.staleness_alert_reason
    assert snap.staleness_recovery_reason == snap.staleness_alert_reason
    assert snap.active_alert_kinds == ()
    # 管道侧镜像同样撤销
    assert not svc.pipeline_health("p").has_staleness_alert
    assert svc.active_alerts("p") == ()


def test_no_repeat_alert_and_no_stale_basis_relapse_after_recovery():
    """同一段失新鲜结束后再巡检：不重复报警、计数不增、旧失新鲜结论不回潮。"""
    svc, _t = _svc()
    run = "run-relapse"
    svc.ingest(_ev(run, "e1", EventType.STARTED, 100.0, 1))
    svc.check_staleness(run, now=104.0)  # 进入失新鲜（第 1 段）
    svc.ingest(_ev(run, "e2", EventType.SUCCEEDED, 105.0, 2))

    # 恢复后连续巡检（时刻推进、但仍在成功后的新鲜窗口内）
    for now in (105.0, 106.0, 107.0):
        v = svc.check_staleness(run, now=now)
        log_verdict(run, v)
        assert v.stale is False
        snap = svc.status(run)
        assert snap.staleness_alert_active is False
        assert "判定失新鲜" not in snap.staleness_alert_reason

    snap = svc.status(run)
    assert snap.staleness_raised_count == 1  # 整段只计一次
    assert snap.staleness_recovered_at == 105.0
    logger.info("[不回潮] raised_count=%s recovered_at=%s 依据=%s",
                snap.staleness_raised_count, snap.staleness_recovered_at,
                snap.staleness_alert_reason)

    # 跨过阈值再次失新鲜：才算新段落（计数 +1），证明“旧段落”确实已干净收尾
    v = svc.check_staleness(run, now=109.0)
    assert v.stale is True
    assert svc.status(run).staleness_raised_count == 2


def test_out_of_order_sweep_time_is_rejected():
    """巡检时刻必须单调不减，拒绝拿旧时刻重判导致旧结论回潮。"""
    svc, _t = _svc()
    run = "run-ooo-sweep"
    svc.ingest(_ev(run, "e1", EventType.STARTED, 100.0, 1))
    svc.check_staleness(run, now=110.0)  # 已失新鲜
    svc.ingest(_ev(run, "e2", EventType.SUCCEEDED, 111.0, 2))  # 恢复
    with pytest.raises(ValueError):
        svc.check_staleness(run, now=105.0)  # 旧时刻不得把恢复后实例判回失新鲜
    snap = svc.status(run)
    assert snap.staleness_alert_active is False

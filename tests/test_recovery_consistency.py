"""失新鲜成功恢复后，活动状态与判定依据一起更新，明细不再自相矛盾。

覆盖：
- 恢复后 staleness_alert_active=False 且 staleness_alert_reason 为“新鲜/恢复”结论，
  依据中带恢复时刻；staleness_last_recovered_at 已记录；
- 恢复后再巡检（含同刻/更晚）不重复报警、旧失新鲜结论不回潮；
- 拿更早的旧时刻巡检属于乱序巡检，不改变任何结论与计数；
- 管道聚合侧的失新鲜告警镜像同步撤销。
"""

import logging

from pipeline_obs import (
    ErrorClass,
    EventType,
    ObservabilityService,
    PipelineEvent,
    RetryPolicy,
)
from tests.timeline import log_event, log_health, log_result, log_verdict

logger = logging.getLogger("timeline")


def _svc(threshold=3.0):
    t = {"v": 100.0}

    def clock():
        return t["v"]

    return (
        ObservabilityService(
            retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=1.0),
            clock=clock,
            fresh_within_seconds=threshold,
        ),
        t,
    )


def _ev(run, pid, eid, etype, version, at, *, attempt=1, ec=None):
    return PipelineEvent(
        run,
        eid,
        etype,
        occurred_at=at,
        version=version,
        attempt=attempt,
        error_class=ec,
        pipeline_id=pid,
    )


def test_stale_reason_refreshed_to_fresh_together_with_active_flag():
    """恢复后：活动标志与判定依据必须同时变成“恢复/新鲜”，且带恢复时刻。"""
    svc, _t = _svc(threshold=3.0)
    pid, run = "pipe-rec", "run-rec-1"

    svc.ingest(_ev(run, pid, "e1", EventType.STARTED, 1, 100.0))
    v = svc.check_staleness(run, now=104.0)
    log_verdict(run, v)
    assert v.stale
    snap = svc.status(run)
    assert snap.staleness_alert_active
    assert "失新鲜" in snap.staleness_alert_reason
    stale_reason = snap.staleness_alert_reason

    # 成功恢复（不依赖巡检，同一写入临界区完成）
    log_event(2, _ev(run, pid, "e2", EventType.SUCCEEDED, 2, 105.0))
    log_result(svc.ingest(_ev(run, pid, "e2", EventType.SUCCEEDED, 2, 105.0)))
    snap = svc.status(run)

    # 核心断言：活动已关，依据不能再是失新鲜那版，必须给出新鲜/恢复结论
    assert snap.staleness_alert_active is False
    assert snap.active_alert_kinds == ()
    assert snap.staleness_alert_reason is not None
    assert snap.staleness_alert_reason != stale_reason
    assert "新鲜" in snap.staleness_alert_reason
    assert "105.000" in snap.staleness_alert_reason  # 恢复时刻进入依据
    assert snap.staleness_last_recovered_at == 105.0  # 恢复时刻字段
    logger.info("[恢复后明细依据] %s", snap.staleness_alert_reason)

    h = svc.pipeline_health(pid)
    log_health(h, svc.active_alerts(pid))
    assert not h.has_staleness_alert
    assert svc.active_alerts(pid, kind="STALENESS") == ()


def test_recovered_episode_does_not_re_alert_or_resurrect_old_verdict():
    """同一段失新鲜结束后再巡检：不重复计数，旧结论永不回潮。"""
    svc, t = _svc(threshold=3.0)
    pid, run = "pipe-rec2", "run-rec-2"

    svc.ingest(_ev(run, pid, "e1", EventType.STARTED, 1, 100.0))
    svc.check_staleness(run, now=104.0)
    assert svc.status(run).staleness_raised_count == 1
    svc.ingest(_ev(run, pid, "e2", EventType.SUCCEEDED, 2, 105.0))

    # 恢复后立刻用“看似会失新鲜”的旧基线思路巡检也无效：基线已是成功时间
    for now in (105.0, 106.0, 107.5):
        v = svc.check_staleness(run, now=now)
        log_verdict(run, v)
        assert not v.stale
        assert not svc.status(run).staleness_alert_active
    assert svc.status(run).staleness_raised_count == 1  # 没有重复报警

    # 乱序巡检：拿更早的旧时刻，结论/计数/活动状态一律不变（旧结论不回潮）
    t["v"] = 200.0
    v_old = svc.check_staleness(run, now=90.0)
    assert v_old is not None and not v_old.stale
    snap = svc.status(run)
    assert not snap.staleness_alert_active
    assert snap.staleness_raised_count == 1
    assert "新鲜" in snap.staleness_alert_reason
    logger.info(
        "[乱序巡检后] raised=%d active=%s",
        snap.staleness_raised_count,
        snap.staleness_alert_active,
    )

    # 真正跨过新基线阈值才算新段落，才允许再告一次（累计 2）
    v = svc.check_staleness(run, now=109.0)
    assert v.stale and svc.status(run).staleness_raised_count == 2


def test_failure_then_success_clears_both_alerts_and_staleness_reason_fresh():
    """失败+失新鲜并存时成功：两类告警同关，失新鲜依据变新鲜，失败依据不残留。"""
    svc, _t = _svc(threshold=3.0)
    pid, run = "pipe-rec3", "run-rec-3"
    svc.ingest(_ev(run, pid, "e1", EventType.STARTED, 1, 100.0))
    svc.ingest(_ev(run, pid, "e2", EventType.FAILED, 2, 100.5, ec=ErrorClass.RETRYABLE))
    svc.check_staleness(run, now=106.0)
    snap = svc.status(run)
    assert snap.active_alert_kinds == ("FAILURE", "STALENESS")

    svc.ingest(_ev(run, pid, "e3", EventType.SUCCEEDED, 3, 107.0, attempt=2))
    snap = svc.status(run)
    assert snap.active_alert_kinds == ()
    assert not snap.alert_active and not snap.staleness_alert_active
    assert "新鲜" in (snap.staleness_alert_reason or "")
    log_health(svc.pipeline_health(pid), svc.active_alerts(pid))

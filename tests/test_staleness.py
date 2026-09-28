"""失新鲜自动监控：确定性巡检、单段只告警一次、与失败告警并存且各自恢复。"""

import logging

from pipeline_obs import (
    AlertKind,
    ErrorClass,
    EventType,
    ObservabilityService,
    PipelineEvent,
    RetryPolicy,
)
from tests.timeline import (
    log_pipeline_health,
    log_result,
    log_snapshot,
)

logger = logging.getLogger("timeline")

THRESHOLD = 10.0


def _svc(now=100.0, **kw):
    t = {"v": now}

    def clock():
        return t["v"]

    svc = ObservabilityService(
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=1.0),
        clock=clock,
        default_fresh_within_seconds=THRESHOLD,
        **kw,
    )
    return svc, t


def _ev(run, eid, etype, ts, version, attempt=1, ec=None, pid=None):
    return PipelineEvent(
        run, eid, etype, occurred_at=ts, version=version,
        attempt=attempt, error_class=ec, pipeline_id=pid,
    )


def test_staleness_enter_persist_recover_alerted_once():
    """失新鲜：新鲜 -> 进入（告警1次）-> 持续巡检（不重复）-> 成功恢复 -> 再次失新鲜（新一段1次）。"""
    svc, t = _svc(now=100.0)
    run = "run-stale-cycle"
    pid = "pipe-stale"
    logger.info("==== 时间线：成功@100；阈值 %ss；巡检时钟确定性推进 ====", THRESHOLD)

    svc.ingest(_ev(run, "e1", EventType.STARTED, 100.0, 1, pid=pid))
    svc.ingest(_ev(run, "e2", EventType.SUCCEEDED, 100.0, 2, pid=pid))

    snap = svc.evaluate_staleness(run, now=105.0)
    log_snapshot(snap)
    assert snap.staleness_alert_active is False
    assert snap.staleness_assessment.basis == "LAST_SUCCESS"
    assert snap.staleness_assessment.age_seconds == 5.0

    logger.info("---- 巡检 @111：age=11 > 10，应触发失新鲜告警 ----")
    snap = svc.evaluate_staleness(run, now=111.0)
    log_snapshot(snap)
    assert snap.staleness_alert_active is True
    assert snap.staleness_assessment.stale is True
    assert "失新鲜告警触发" in snap.staleness_alert_reason
    rec = svc.store._records[run]
    assert rec.stale_alert.raised_count == 1
    assert rec.stale_alert.raised_at == 111.0
    assert rec.stale_alert.basis == "LAST_SUCCESS"
    assert rec.stale_alert.age_seconds == 11.0
    assert rec.stale_alert.threshold_seconds == THRESHOLD

    for now in (112.0, 115.0, 130.0):
        snap = svc.evaluate_staleness(run, now=now)
        assert snap.staleness_alert_active is True
        assert "持续" in snap.staleness_alert_reason
        assert svc.store._records[run].stale_alert.raised_count == 1
    logger.info("---- 连续巡检 @112/@115/@130：raised_count 仍为 1（只计一次）----")

    h = svc.pipeline_health(pid)
    log_pipeline_health(h)
    assert h.staleness_alert_active is True
    stale_alerts = [a for a in h.active_alerts if a.kind is AlertKind.STALENESS]
    assert len(stale_alerts) == 1
    assert stale_alerts[0].basis == "LAST_SUCCESS"
    assert stale_alerts[0].threshold_seconds == THRESHOLD

    logger.info("---- 新一轮 STARTED+SUCCEEDED@131：失新鲜告警应即时关闭（无需巡检）----")
    log_result(svc.ingest(_ev(run, "e3", EventType.STARTED, 131.0, 3, attempt=2, pid=pid)))
    log_result(svc.ingest(_ev(run, "e4", EventType.SUCCEEDED, 131.0, 4, attempt=2, pid=pid)))
    snap = svc.status(run)
    log_snapshot(snap)
    assert snap.staleness_alert_active is False
    assert svc.store._records[run].stale_alert.raised_count == 1
    assert svc.store._records[run].stale_alert.last_recovered_at == 131.0
    assert svc.pipeline_health(pid).staleness_alert_active is False

    snap = svc.evaluate_staleness(run, now=142.0)
    assert snap.staleness_alert_active is True
    assert svc.store._records[run].stale_alert.raised_count == 2
    assert snap.staleness_assessment.age_seconds == 11.0
    logger.info("---- 再次失新鲜@142：新一段 raised_count=2 ----")


def test_never_succeeded_uses_first_report_as_baseline():
    """从未成功：以第一次上报时间为基线判定失新鲜。"""
    svc, t = _svc(now=200.0)
    run = "run-never-ok"
    svc.ingest(_ev(run, "e1", EventType.STARTED, 200.0, 1))
    snap = svc.evaluate_staleness(run, now=205.0)
    assert snap.staleness_assessment.basis == "FIRST_REPORT"
    assert snap.staleness_alert_active is False
    snap = svc.evaluate_staleness(run, now=210.1)
    log_snapshot(snap)
    assert snap.staleness_assessment.basis == "FIRST_REPORT"
    assert abs(snap.staleness_assessment.age_seconds - 10.1) < 1e-9
    assert snap.staleness_alert_active is True
    assert svc.store._records[run].stale_alert.raised_count == 1


def test_failure_and_staleness_coexist_and_recover_independently():
    """失败告警与失新鲜告警并存，可各自独立恢复（互不影响对方实例）。

    时间线（管道 pipe-coexist 下两个运行）：
    - run-bad：成功@0 后 @5 RETRYABLE 失败（失败告警开，仍新鲜）；
      @20 巡检（失新鲜告警也开）-> 两类并存；@31 重试成功 -> 两类同时恢复。
    - run-x：@18 成功后 @19 FATAL 失败（失败告警开、新鲜）；@30 巡检失新鲜 -> 并存；
      run-bad 恢复后 run-x 的两类告警必须原样保持。
    - 最后验证：阈值调大可只让失新鲜恢复新鲜，而失败告警纹丝不动。
    """
    svc, t = _svc(now=0.0)
    pid = "pipe-coexist"

    bad = "run-bad"
    svc.ingest(_ev(bad, "b1", EventType.STARTED, 0.0, 1, pid=pid))
    svc.ingest(_ev(bad, "b2", EventType.SUCCEEDED, 0.0, 2, pid=pid))
    svc.ingest(_ev(bad, "b3", EventType.STARTED, 5.0, 3, attempt=2, pid=pid))
    log_result(svc.ingest(_ev(bad, "b4", EventType.FAILED, 5.0, 4, attempt=2,
                             ec=ErrorClass.RETRYABLE, pid=pid)))
    h = svc.pipeline_health(pid)
    log_pipeline_health(h)
    assert h.failure_alert_active is True
    assert h.staleness_alert_active is False

    logger.info("---- 巡检@20：失败告警已在，失新鲜告警应叠加 ----")
    svc.run_staleness_sweep(now=20.0, pipeline_id=pid)
    h = svc.pipeline_health(pid)
    log_pipeline_health(h)
    assert h.failure_alert_active is True
    assert h.staleness_alert_active is True
    kinds = {(a.kind, a.pipeline_run_id) for a in h.active_alerts}
    assert (AlertKind.FAILURE, bad) in kinds
    assert (AlertKind.STALENESS, bad) in kinds
    snap = svc.status(bad)
    assert snap.alert_active is True and snap.staleness_alert_active is True
    a = snap.staleness_assessment
    assert (a.basis, a.basis_time, a.age_seconds, a.threshold_seconds) == (
        "LAST_SUCCESS", 0.0, 20.0, THRESHOLD)

    x = "run-x"
    svc.ingest(_ev(x, "x1", EventType.STARTED, 18.0, 1, pid=pid))
    svc.ingest(_ev(x, "x2", EventType.SUCCEEDED, 18.0, 2, pid=pid))
    svc.ingest(_ev(x, "x3", EventType.STARTED, 19.0, 3, attempt=2, pid=pid))
    svc.ingest(_ev(x, "x4", EventType.FAILED, 19.0, 4, attempt=2,
                   ec=ErrorClass.FATAL, pid=pid))
    svc.run_staleness_sweep(now=19.5, pipeline_id=pid)
    h = svc.pipeline_health(pid)
    kinds = {(a.kind, a.pipeline_run_id) for a in h.active_alerts}
    assert (AlertKind.FAILURE, x) in kinds
    assert (AlertKind.STALENESS, x) not in kinds
    logger.info("---- run-x 失败但新鲜：仅有失败告警 ----")

    svc.run_staleness_sweep(now=30.0, pipeline_id=pid)
    h = svc.pipeline_health(pid)
    kinds = {(a.kind, a.pipeline_run_id) for a in h.active_alerts}
    assert (AlertKind.STALENESS, x) in kinds
    assert (AlertKind.FAILURE, x) in kinds
    logger.info("---- @30：run-x 失败+失新鲜并存 ----")
    log_pipeline_health(h)

    # run-bad @31 重试成功：run-bad 上两类告警同时恢复
    svc.ingest(_ev(bad, "b5", EventType.STARTED, 31.0, 5, attempt=3, pid=pid))
    svc.ingest(_ev(bad, "b6", EventType.SUCCEEDED, 31.0, 6, attempt=3, pid=pid))
    h = svc.pipeline_health(pid)
    kinds = {(a.kind, a.pipeline_run_id) for a in h.active_alerts}
    assert (AlertKind.FAILURE, bad) not in kinds
    assert (AlertKind.STALENESS, bad) not in kinds
    assert (AlertKind.FAILURE, x) in kinds
    assert (AlertKind.STALENESS, x) in kinds
    logger.info("---- run-bad@31 成功恢复：run-bad 两类关闭，run-x 不受影响 ----")
    log_pipeline_health(h)

    # 连续失败口径：最近成功为 run-bad@31；run-x 失败@19 早于它，不计入
    assert h.last_success_at == 31.0
    assert h.consecutive_failures == 0

    # 失新鲜可独立恢复：用更大阈值巡检 run-x，失新鲜关闭而失败告警保持
    snap = svc.evaluate_staleness(x, now=30.0, fresh_within_seconds=100.0)
    log_snapshot(snap)
    assert snap.staleness_alert_active is False
    assert snap.alert_active is True
    h = svc.pipeline_health(pid)
    log_pipeline_health(h)
    assert h.staleness_alert_active is False
    assert h.failure_alert_active is True
    logger.info("---- run-x 阈值调大恢复新鲜：失新鲜告警关闭，失败告警保持 ----")


def test_threshold_boundary_equal_is_still_fresh():
    """age 恰好等于阈值仍属新鲜（严格大于才失新鲜）。"""
    svc, t = _svc(now=0.0)
    run = "run-boundary"
    svc.ingest(_ev(run, "e1", EventType.STARTED, 0.0, 1))
    svc.ingest(_ev(run, "e2", EventType.SUCCEEDED, 0.0, 2))
    snap = svc.evaluate_staleness(run, now=10.0)
    assert snap.staleness_assessment.stale is False
    assert snap.staleness_alert_active is False
    snap = svc.evaluate_staleness(run, now=10.000001)
    assert snap.staleness_assessment.stale is True
    assert snap.staleness_alert_active is True


def test_sweep_is_deterministic_and_threshold_configurable():
    """巡检可确定性触发（注入 now，不 sleep）；阈值可服务级/调用级配置，可限定管道范围。"""
    svc, t = _svc(now=0.0)
    pid = "pipe-sweep"
    for rid in ["s1", "s2", "s3"]:
        svc.ingest(_ev(rid, f"{rid}s", EventType.STARTED, 0.0, 1, pid=pid))

    stale = svc.run_staleness_sweep(now=11.0)
    assert {s.pipeline_run_id for s in stale} == {"s1", "s2", "s3"}
    svc.run_staleness_sweep(now=12.0)
    svc.run_staleness_sweep(now=13.0)
    for rid in ["s1", "s2", "s3"]:
        assert svc.store._records[rid].stale_alert.raised_count == 1

    stale = svc.run_staleness_sweep(now=13.0, fresh_within_seconds=20.0)
    assert stale == []
    for rid in ["s1", "s2", "s3"]:
        assert svc.status(rid).staleness_alert_active is False

    svc.ingest(_ev("other", "o1", EventType.STARTED, 0.0, 1, pid="OTHER"))
    stale = svc.run_staleness_sweep(
        now=99.0, pipeline_id=pid, fresh_within_seconds=10.0)
    assert {s.pipeline_run_id for s in stale} == {"s1", "s2", "s3"}
    assert svc.status("other").staleness_alert_active is False
    logger.info("---- 确定性巡检：now/阈值/管道范围均可按调用指定 ----")

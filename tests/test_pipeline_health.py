"""管道级健康汇总 + 失新鲜告警：并发一致性、单段单次告警、两类告警并存与独立恢复。"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from pipeline_obs import (
    ErrorClass,
    EventType,
    ObservabilityService,
    PipelineEvent,
    RetryPolicy,
)
from tests.timeline import log_event, log_health, log_result, log_verdict

logger = logging.getLogger("timeline")


def _svc(threshold=3.0, max_attempts=3):
    t = {"v": 100.0}

    def clock():
        return t["v"]

    return (
        ObservabilityService(
            retry_policy=RetryPolicy(max_attempts=max_attempts, base_delay_seconds=1.0),
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


def test_concurrent_reports_keep_pipeline_summary_consistent():
    """同一管道下多个运行并发上报：聚合不丢更新、不露中间态。"""
    svc, _t = _svc()
    pid = "pipe-conc"
    n_runs = 40
    stop = threading.Event()
    violations: list[str] = []

    def reader():
        while not stop.is_set():
            h = svc.pipeline_health(pid)
            if h is None:
                continue
            if h.consecutive_failures < 0 or h.consecutive_failures > h.run_count:
                violations.append("bad consecutive_failures")
            total_alerts = h.active_failure_alert_count + h.active_staleness_alert_count
            if total_alerts > h.run_count * 2:
                violations.append("active alerts exceed 2x runs")
            if h.has_staleness_alert and h.staleness_threshold_seconds is None:
                violations.append("stale alert without threshold")
            kinds = [a.kind.value for a in svc.active_alerts(pid)]
            if any(k not in ("FAILURE", "STALENESS") for k in kinds):
                violations.append("unknown alert kind")

    logger.info(
        "==== 管道并发汇总：%d 个运行、每事件重复 4 次并发提交 + 8 持续读取者 ====",
        n_runs,
    )
    readers = [threading.Thread(target=reader) for _ in range(8)]
    for th in readers:
        th.start()

    events = []
    for i in range(1, n_runs + 1):
        run = f"run-{i:02d}"
        events.append(_ev(run, pid, f"r{i}-s", EventType.STARTED, 1, float(i)))
        events.append(
            _ev(
                run,
                pid,
                f"r{i}-f",
                EventType.FAILED,
                2,
                float(i) + 0.5,
                ec=ErrorClass.RETRYABLE,
            )
        )

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda e: svc.ingest(e), events * 4))

    stop.set()
    for th in readers:
        th.join()

    h = svc.pipeline_health(pid)
    alerts = svc.active_alerts(pid)
    log_health(h, alerts)
    logger.info(
        "[并发结论] runs=%d 连续失败=%d 活动告警=%d 读不变量违例=%d（预期 0）",
        h.run_count,
        h.consecutive_failures,
        len(alerts),
        len(violations),
    )
    assert violations == []
    assert h.run_count == n_runs
    assert h.consecutive_failures == n_runs
    assert len(alerts) == n_runs
    assert all(a.kind.value == "FAILURE" for a in alerts)


def test_staleness_enter_persist_recover_alerted_once():
    """失新鲜：进入→持续→恢复只告警一次、只计一次；再次失新鲜才算新段落。"""
    svc, t = _svc(threshold=3.0)
    pid = "pipe-stale"
    run = "run-stale-1"
    logger.info("==== 失新鲜时间线：阈值=3s，注入时钟确定性推进 ====")

    def sweep(now, label):
        t["v"] = now
        v = svc.check_staleness(run, now=now)
        logger.info("[巡检@%.1f] %s", now, label)
        log_verdict(run, v)
        log_health(svc.pipeline_health(pid), svc.active_alerts(pid))
        return v

    log_event(1, _ev(run, pid, "e1", EventType.STARTED, 1, 100.0))
    log_result(svc.ingest(_ev(run, pid, "e1", EventType.STARTED, 1, 100.0)))

    v = sweep(102.0, "窗口内应新鲜")
    assert not v.stale
    assert svc.status(run).staleness_raised_count == 0

    v = sweep(103.1, "跨过阈值进入失新鲜，应告第一次")
    assert v.stale and v.baseline_kind.value == "FIRST_REPORT"
    snap = svc.status(run)
    assert snap.staleness_alert_active and snap.staleness_raised_count == 1
    assert snap.active_alert_kinds == ("STALENESS",)

    for now in (104.0, 105.0, 110.0):
        v = sweep(now, "持续失新鲜，收敛不重复")
        assert v.stale
    assert svc.status(run).staleness_raised_count == 1
    assert svc.pipeline_health(pid).has_staleness_alert

    t["v"] = 111.0
    log_event(2, _ev(run, pid, "e2", EventType.SUCCEEDED, 2, 111.0))
    log_result(svc.ingest(_ev(run, pid, "e2", EventType.SUCCEEDED, 2, 111.0)))
    v = sweep(111.0, "成功恢复，基线切换 LAST_SUCCESS")
    assert not v.stale and v.baseline_kind.value == "LAST_SUCCESS"
    assert not svc.status(run).staleness_alert_active
    assert not svc.pipeline_health(pid).has_staleness_alert
    assert svc.status(run).staleness_raised_count == 1

    v = sweep(115.0, "再次跨过阈值，新段落再告一次（累计 2）")
    assert v.stale
    assert svc.status(run).staleness_raised_count == 2


def test_failure_and_staleness_alerts_coexist_and_recover_independently():
    """失败告警与失新鲜告警可并存，并各自独立恢复。"""
    svc, t = _svc(threshold=3.0, max_attempts=5)
    pid = "pipe-both"
    run = "run-both-1"
    logger.info("==== 两类告警并存与独立恢复 ====")

    svc.ingest(_ev(run, pid, "e1", EventType.STARTED, 1, 100.0))
    t["v"] = 102.0
    log_result(
        svc.ingest(
            _ev(run, pid, "e2", EventType.FAILED, 2, 102.0, ec=ErrorClass.RETRYABLE)
        )
    )
    snap = svc.status(run)
    assert snap.alert_active and not snap.staleness_alert_active
    assert snap.active_alert_kinds == ("FAILURE",)
    logger.info("[失败后] kinds=%s", snap.active_alert_kinds)

    v = svc.check_staleness(run, now=106.0)
    log_verdict(run, v)
    assert v.stale
    snap = svc.status(run)
    assert snap.alert_active and snap.staleness_alert_active
    assert snap.active_alert_kinds == ("FAILURE", "STALENESS")
    h = svc.pipeline_health(pid)
    log_health(h, svc.active_alerts(pid))
    assert h.has_failure_alert and h.has_staleness_alert
    assert sorted(a.kind.value for a in svc.active_alerts(pid)) == [
        "FAILURE",
        "STALENESS",
    ]

    # 成功同时恢复两类（RETRYABLE 允许重试后成功）
    svc.ingest(_ev(run, pid, "e3", EventType.SUCCEEDED, 3, 107.0, attempt=2))
    snap = svc.status(run)
    assert not snap.alert_active and not snap.staleness_alert_active

    # 新一轮：先只出现失新鲜（运行中、无失败告警）
    svc.ingest(_ev(run, pid, "e4", EventType.STARTED, 4, 107.0, attempt=3))
    assert not svc.check_staleness(run, now=107.0).stale
    v = svc.check_staleness(run, now=111.0)  # 成功基线 107，超 3s
    assert v.stale
    snap = svc.status(run)
    assert snap.staleness_alert_active and not snap.alert_active
    assert snap.active_alert_kinds == ("STALENESS",)
    logger.info(
        "[仅失新鲜] kinds=%s 依据=%s",
        snap.active_alert_kinds,
        snap.staleness_alert_reason,
    )

    # 再失败：两类并存；同段失新鲜不因失败而重复计数
    svc.ingest(
        _ev(
            run,
            pid,
            "e5",
            EventType.FAILED,
            5,
            111.5,
            attempt=3,
            ec=ErrorClass.RETRYABLE,
        )
    )
    svc.check_staleness(run, now=112.0)
    snap = svc.status(run)
    assert snap.active_alert_kinds == ("FAILURE", "STALENESS")
    raised = snap.staleness_raised_count
    svc.check_staleness(run, now=113.0)
    svc.check_staleness(run, now=114.0)
    assert svc.status(run).staleness_raised_count == raised

    # 再次成功：两类各自关闭
    svc.ingest(_ev(run, pid, "e6", EventType.SUCCEEDED, 6, 115.0, attempt=4))
    svc.check_staleness(run, now=115.0)
    snap = svc.status(run)
    assert snap.active_alert_kinds == ()
    log_health(svc.pipeline_health(pid), svc.active_alerts(pid))
    logger.info("[独立恢复后] kinds=%s", snap.active_alert_kinds)


def test_pipeline_summary_exact_across_many_runs_with_watermark():
    """跨多运行混合成功/失败：连续失败数与成功水位精确（覆盖水位抬升批量剔除）。"""
    svc, _t = _svc()
    pid = "pipe-mix"
    # r1 失败@1、r2 失败@2、r3 成功@3（水位=3，前两个失败被覆盖）→ 连续失败 0
    svc.ingest(_ev("r1", pid, "s", EventType.STARTED, 1, 0.0))
    svc.ingest(_ev("r1", pid, "f", EventType.FAILED, 2, 1.0, ec=ErrorClass.RETRYABLE))
    svc.ingest(_ev("r2", pid, "s", EventType.STARTED, 1, 1.5))
    svc.ingest(_ev("r2", pid, "f", EventType.FAILED, 2, 2.0, ec=ErrorClass.RETRYABLE))
    svc.ingest(_ev("r3", pid, "s", EventType.STARTED, 1, 2.5))
    svc.ingest(_ev("r3", pid, "ok", EventType.SUCCEEDED, 2, 3.0))
    h = svc.pipeline_health(pid)
    log_health(h, svc.active_alerts(pid))
    assert h.last_success_at == 3.0 and h.consecutive_failures == 0

    # 水位之后的新失败才计数；同一运行重试失败不重复计
    svc.ingest(_ev("r4", pid, "s", EventType.STARTED, 1, 4.0))
    svc.ingest(
        _ev(
            "r4",
            pid,
            "f1",
            EventType.FAILED,
            2,
            5.0,
            attempt=1,
            ec=ErrorClass.RETRYABLE,
        )
    )
    svc.ingest(_ev("r4", pid, "s2", EventType.STARTED, 3, 5.5, attempt=2))
    svc.ingest(
        _ev(
            "r4",
            pid,
            "f2",
            EventType.FAILED,
            4,
            6.0,
            attempt=2,
            ec=ErrorClass.RETRYABLE,
        )
    )
    svc.ingest(_ev("r5", pid, "s", EventType.STARTED, 1, 6.5))
    svc.ingest(_ev("r5", pid, "f", EventType.FAILED, 2, 7.0, ec=ErrorClass.FATAL))
    h = svc.pipeline_health(pid)
    log_health(h, svc.active_alerts(pid))
    assert h.consecutive_failures == 2  # r4、r5 各算一个不同运行
    assert h.latest_run_id == "r5" and h.latest_run_state.value == "FAILED_FATAL"

    # r4 成功：水位抬到 8，r5(7) 被覆盖 → 归零
    svc.ingest(_ev("r4", pid, "ok", EventType.SUCCEEDED, 5, 8.0, attempt=3))
    h = svc.pipeline_health(pid)
    log_health(h, svc.active_alerts(pid))
    assert h.last_success_at == 8.0 and h.consecutive_failures == 0
    # r5 的失败告警仍在（它没恢复），但已不计入连续失败
    assert h.has_failure_alert


def test_pipeline_health_query_cost_does_not_grow_with_history():
    """历史实例线性增长时，管道健康查询耗时基本恒定（物化聚合，非全量扫描）。"""
    import time as _time

    svc, _t = _svc()
    pid = "pipe-scale"

    def seed(n, offset=0):
        for i in range(offset + 1, offset + n + 1):
            run = f"run-{i:05d}"
            svc.ingest(_ev(run, pid, "s", EventType.STARTED, 1, float(i)))
            svc.ingest(
                _ev(
                    run,
                    pid,
                    "f",
                    EventType.FAILED,
                    2,
                    float(i) + 0.5,
                    ec=ErrorClass.RETRYABLE,
                )
            )

    def query_burst(repeat=2000):
        start = _time.perf_counter()
        for _ in range(repeat):
            svc.pipeline_health(pid)
        return _time.perf_counter() - start

    seed(500)
    t_small = query_burst()
    seed(4500, offset=500)  # 再补 4500 个新运行，累计 5000 个不同历史运行
    t_large = query_burst()
    logger.info(
        "[查询成本] 500 运行: %.4fs/2000次；5000 运行: %.4fs/2000次（比值 %.2f，应接近 1）",
        t_small,
        t_large,
        t_large / t_small,
    )
    # 物化聚合：历史增长 10 倍，查询耗时不应同比例增长（放宽到 4 倍容纳调度噪声）
    assert t_large < t_small * 4.0
    h = svc.pipeline_health(pid)
    assert h.run_count == 5000 and h.consecutive_failures == 5000

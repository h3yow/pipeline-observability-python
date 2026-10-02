"""历史保留上限与淘汰：淘汰最旧、三态查询一致、被淘汰不进汇总、查询成本不随历史增长。"""

import logging
import threading
import time as _time
from concurrent.futures import ThreadPoolExecutor

from pipeline_obs import (
    ErrorClass,
    EventType,
    IngestStatus,
    LookupStatus,
    ObservabilityService,
    PipelineEvent,
    RejectReason,
    RetentionInfo,
    RetryPolicy,
)

logger = logging.getLogger("timeline")


def _ev(run, eid, etype, at, version, *, attempt=1, ec=None, msg=None, pid="p"):
    return PipelineEvent(run, eid, etype, occurred_at=at, version=version, attempt=attempt,
                         error_class=ec, error_message=msg, pipeline_id=pid)


def _svc(max_runs):
    return ObservabilityService(
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=1.0),
        fresh_within_seconds=3.0,
        max_runs=max_runs,
    )


def test_eviction_evicts_oldest_and_discloses_counts():
    """超过上限后淘汰最近事件时间最旧的运行；保留口径与累计淘汰量对外披露。"""
    svc = _svc(max_runs=3)
    for i in range(1, 6):
        svc.ingest(_ev(f"run{i}", "e", EventType.STARTED, float(i), 1, pid="pp"))

    info = svc.retention_info()
    logger.info("[保留口径] %s", info)
    assert isinstance(info, RetentionInfo)
    assert info.max_runs == 3
    assert info.current_runs == 3
    assert info.total_evicted_runs == 2

    kept = {s.pipeline_run_id for s in svc.all_status()}
    assert kept == {"run3", "run4", "run5"}

    h = svc.pipeline_health("pp")
    logger.info("[管道汇总] run_count=%s evicted=%s", h.run_count, h.evicted_run_count)
    assert h.run_count == 3
    assert h.evicted_run_count == 2
    assert h.retention.total_evicted_runs == 2


def test_evicted_run_lookup_is_consistent_and_reports_are_rejected():
    """被淘汰运行：单独查询给明确 EVICTED 结论（不报错、不残缺），再上报被拒且不复活。"""
    svc = _svc(max_runs=2)
    for i in range(1, 4):
        svc.ingest(_ev(f"r{i}", "e", EventType.STARTED, float(i), 1, pid="pq"))

    lookup = svc.lookup_run("r1")
    logger.info("[三态查询-EVICTED] %s | %s", lookup.status, lookup.detail)
    assert lookup.status is LookupStatus.EVICTED
    assert lookup.snapshot is None and lookup.run_id == "r1"
    # 旧的 status() 便捷查询对“淘汰/未知”都给 None（不报错、不返回残缺数据）
    assert svc.status("r1") is None

    assert svc.lookup_run("never-seen").status is LookupStatus.UNKNOWN
    assert svc.lookup_run("r3").status is LookupStatus.PRESENT
    assert svc.lookup_run("r3").snapshot.pipeline_run_id == "r3"

    # 被淘汰运行再上报：明确拒绝（新原因码），不会借新事件复活
    res = svc.ingest(_ev("r1", "late", EventType.STARTED, 99.0, 2, pid="pq"))
    logger.info("[淘汰后上报] %s %s", res.status, res.reason)
    assert res.status is IngestStatus.REJECTED
    assert res.reason is RejectReason.RUN_EVICTED
    assert svc.lookup_run("r1").status is LookupStatus.EVICTED
    # 拒绝不会额外增加淘汰计数，也不会挤占保留窗口
    assert svc.retention_info().current_runs == 2
    assert svc.retention_info().total_evicted_runs == 1


def test_evicted_run_excluded_from_failure_aggregate_and_alerts():
    """被淘汰的失败运行不再计入连续失败数、失败原因视图与活动告警。"""
    svc = _svc(max_runs=2)
    svc.ingest(_ev("a", "f", EventType.FAILED, 1.0, 1,
                   ec=ErrorClass.FATAL, msg="A fatal", pid="pr"))
    svc.ingest(_ev("b", "f", EventType.FAILED, 2.0, 1,
                   ec=ErrorClass.RETRYABLE, msg="B retry", pid="pr"))
    h = svc.pipeline_health("pr")
    assert h.run_count == 2 and h.consecutive_failures == 2
    assert h.latest_failure_run_id == "b"
    assert len(svc.active_alerts("pr")) == 2

    # 加入 c、d：a、b 依次被最旧淘汰
    svc.ingest(_ev("c", "f", EventType.FAILED, 3.0, 1,
                   ec=ErrorClass.FATAL, msg="C fatal", pid="pr"))
    svc.ingest(_ev("d", "f", EventType.FAILED, 4.0, 1,
                   ec=ErrorClass.FATAL, msg="D fatal", pid="pr"))
    h = svc.pipeline_health("pr")
    logger.info("[淘汰后汇总] runs=%s 连续失败=%s latest=%s 告警=%s 淘汰=%s",
                h.run_count, h.consecutive_failures, h.latest_failure_run_id,
                h.active_failure_alert_count, h.evicted_run_count)
    assert h.run_count == 2
    assert {s.pipeline_run_id for s in svc.all_status()} == {"c", "d"}
    assert h.consecutive_failures == 2          # 只剩 c、d 两个失败运行
    assert h.latest_failure_run_id == "d"
    assert h.latest_error_message == "D fatal"
    assert h.active_failure_alert_count == 2
    assert h.evicted_run_count == 2
    assert {a.pipeline_run_id for a in svc.active_alerts("pr")} == {"c", "d"}


def test_eviction_order_reflects_latest_event_not_creation():
    """淘汰按“最近事件时间”而非创建顺序：旧 run 收到更新事件后应让更旧的 run 先出局。"""
    svc = _svc(max_runs=2)
    svc.ingest(_ev("a", "e1", EventType.STARTED, 1.0, 1, pid="po"))
    svc.ingest(_ev("b", "e1", EventType.STARTED, 2.0, 1, pid="po"))
    # a 收到更晚事件（3.0），变成比 b 新；再插入 c 时应淘汰 b 而非 a
    svc.ingest(_ev("a", "e2", EventType.STARTED, 3.0, 2, pid="po"))
    svc.ingest(_ev("c", "e1", EventType.STARTED, 4.0, 1, pid="po"))
    kept = {s.pipeline_run_id for s in svc.all_status()}
    logger.info("[最近事件淘汰] kept=%s", kept)
    assert kept == {"a", "c"}
    assert svc.lookup_run("b").status is LookupStatus.EVICTED


def test_health_query_cost_stays_flat_under_eviction():
    """有界保留下历史不断上报（远多过上限），管道健康查询仍 O(1)、不随累计历史变慢。"""
    svc = _svc(max_runs=500)
    pid = "ps"

    def seed(n, offset=0):
        for i in range(offset + 1, offset + n + 1):
            run = f"run-{i:06d}"
            svc.ingest(_ev(run, "s", EventType.STARTED, float(i), 1, pid=pid))
            svc.ingest(_ev(run, "f", EventType.FAILED, float(i) + 0.5, 2,
                           ec=ErrorClass.RETRYABLE, msg=f"err-{i}", pid=pid))

    def burst(repeat=2000):
        start = _time.perf_counter()
        for _ in range(repeat):
            svc.pipeline_health(pid)
        return _time.perf_counter() - start

    seed(600)            # 已淘汰 100
    t_small = burst()
    seed(5000, offset=600)   # 再上报 5000 个新运行，累计淘汰 5100，窗口仍为 500
    info = svc.retention_info()
    t_large = burst()
    logger.info(
        "[淘汰下查询成本] current=%s total_evicted=%s；%.4fs vs %.4fs/2000次（比值 %.2f）",
        info.current_runs, info.total_evicted_runs, t_small, t_large, t_large / t_small,
    )
    assert info.current_runs == 500
    assert info.total_evicted_runs == 5100
    assert t_large < t_small * 4.0   # 不随累计历史线性变慢
    h = svc.pipeline_health(pid)
    assert h.run_count == 500
    assert h.consecutive_failures == 500


def test_concurrent_reports_queries_and_eviction_stay_consistent():
    """同一管道多运行并发上报（触发淘汰）+ 并发查询：汇总始终自洽。"""
    svc = _svc(max_runs=100)
    pid = "pconc"
    n_runs = 400
    stop = threading.Event()
    violations: list[str] = []

    def reader():
        while not stop.is_set():
            # 全部字段取自同一临界区，避免用多个独立快照做跨调用比较。
            c = svc.consistency_snapshot(pid)
            if c is None:
                continue
            if not (0 <= c["consecutive_failures"] <= c["run_count"]):
                violations.append("bad consecutive_failures")
            # 并发写入下允许窗口瞬时超出，但不超过 max_runs + 并发写入线程数(16)。
            if c["run_count"] > 100 + 16 or c["current_runs"] > 100 + 16:
                violations.append("run count exceeds cap beyond concurrency slack")
            if c["failure_alert_count"] > c["run_count"]:
                violations.append("failure alerts exceed runs")
            if c["staleness_alert_count"] > c["run_count"]:
                violations.append("staleness alerts exceed runs")
            if c["pipeline_evicted"] < 0 or c["total_evicted"] < 0:
                violations.append("negative evicted")
            # 该管道只有本测试在写入：管道 run_count 不可能多于全局保留数；
            # 摘除顺序为“先全局记录、后管道聚合”（同一注册临界区内），
            # 因此只可能瞬时 run_count 比 current_runs 落后，终态收敛相等。
            if not (c["run_count"] <= c["current_runs"]):
                violations.append("pipeline run_count exceeds global retained runs")
            if not c["alerts_reference_kept_runs"]:
                violations.append("active alert references an evicted/missing run")

    readers = [threading.Thread(target=reader) for _ in range(8)]
    for th in readers:
        th.start()

    events = []
    for i in range(1, n_runs + 1):
        run = f"run-{i:04d}"
        events.append(_ev(run, f"s{i}", EventType.STARTED, float(i), 1, pid=pid))
        events.append(_ev(run, f"f{i}", EventType.FAILED, float(i) + 0.5, 2,
                          ec=ErrorClass.RETRYABLE, msg=f"err-{i}", pid=pid))
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda e: svc.ingest(e), events))

    stop.set()
    for th in readers:
        th.join()
    from collections import Counter
    logger.info("[违例分布] %s", Counter(violations))

    h = svc.pipeline_health(pid)
    info = svc.retention_info()
    logger.info("[并发淘汰结论] runs=%s(<=100) 连续失败=%s 淘汰=%s 违例=%s",
                h.run_count, h.consecutive_failures, info.total_evicted_runs,
                len(violations))
    assert violations == []
    assert h.run_count == 100
    assert h.consecutive_failures == 100
    assert h.active_failure_alert_count == 100
    assert info.total_evicted_runs == n_runs - 100
    assert info.current_runs == 100

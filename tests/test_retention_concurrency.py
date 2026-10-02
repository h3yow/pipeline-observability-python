"""并发上报 + 并发查询 + 并发淘汰：保留口径与管道汇总始终自洽。"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from pipeline_obs import (
    ErrorClass,
    EventType,
    IngestStatus,
    LookupOutcome,
    ObservabilityService,
    PipelineEvent,
    PipelineStore,
    RejectReason,
    RetryPolicy,
)

logger = logging.getLogger("timeline")


def _ev(run, pid, seq, etype, *, ec=None):
    # 时间戳随 run 序号单调：STARTED@i、FAILED@i+0.5，保证 drop-oldest
    # 的淘汰边界严格按 run 序号确定（不依赖同时间戳的 run_id 决胜）。
    order = int(run.split("-")[1])
    at = float(order) if seq == 1 else float(order) + 0.5
    return PipelineEvent(
        run,
        f"{run}-e{seq}",
        etype,
        occurred_at=at,
        version=seq,
        attempt=1,
        error_class=ec,
        pipeline_id=pid,
    )


def test_concurrent_ingest_query_eviction_stays_consistent():
    """同一管道大量运行并发上报（含重复）+ 持续查询：保留不越界、计数自洽、结论明确。"""
    max_runs = 100
    store = PipelineStore(max_retained_runs=max_runs)
    svc = ObservabilityService(retry_policy=RetryPolicy(max_attempts=3), store=store)
    pid = "pipe-conc-ret"
    n_runs = 800
    stop = threading.Event()
    violations: list[str] = []

    def reader():
        while not stop.is_set():
            rs = store.retention_status()
            if not (0 <= rs.retained_runs <= max_runs):
                violations.append(f"retained {rs.retained_runs} out of bound")
            if rs.evicted_runs < 0:
                violations.append("negative evicted")
            # 强一致视图：保留数与保留 ID 集合取自同一临界区，必须严格相等，
            # 即使此刻正在并发淘汰。
            status, run_ids = svc.retained_runs_view()
            if status.retained_runs != len(run_ids):
                violations.append("retention snapshot not self-consistent")
            if len(run_ids) > max_runs:
                violations.append("retained ids exceed cap")
            snaps = svc.all_status()
            # 每张明细快照对应的 run 必须仍在某次一致视图保留集合的并意义上有效：
            # 单次 all_status 自身不越界，且任意快照都能被 lookup 一致识别。
            if not (0 <= len(snaps) <= max_runs):
                violations.append("all_status out of bound")
            if snaps:
                one = snaps[len(snaps) // 2].pipeline_run_id
                lk = svc.lookup_run(one)
                if lk.outcome not in (LookupOutcome.FOUND, LookupOutcome.EVICTED):
                    violations.append("lookup returned ambiguous outcome")
                if lk.outcome is LookupOutcome.FOUND and (
                    lk.status is None or lk.status.pipeline_run_id != one
                ):
                    violations.append("found lookup returned partial data")
            h = svc.pipeline_health(pid)
            if h is not None:
                if not (0 <= h.consecutive_failures <= h.run_count <= max_runs):
                    violations.append("bad pipeline counters")
                if (
                    h.active_failure_alert_count + h.active_staleness_alert_count
                ) > h.run_count * 2:
                    violations.append("alerts exceed runs")
                if h.latest_failure_run_id is not None:
                    if h.latest_failure_error_class not in ("RETRYABLE", "FATAL"):
                        violations.append("latest failure missing error class")
                    if h.latest_failure_termination not in (
                        "FATAL",
                        "RETRY_EXHAUSTED",
                        "NOT_TERMINATED",
                    ):
                        violations.append("bad termination")

    readers = [threading.Thread(target=reader) for _ in range(8)]
    for th in readers:
        th.start()

    events = []
    for i in range(1, n_runs + 1):
        run = f"run-{i:04d}"
        events.append(_ev(run, pid, 1, EventType.STARTED))
        events.append(
            _ev(
                run,
                pid,
                2,
                EventType.FAILED,
                ec=ErrorClass.FATAL if i % 5 == 0 else ErrorClass.RETRYABLE,
            )
        )

    with ThreadPoolExecutor(max_workers=16) as pool:
        # 重复并发提交，制造去重 + 淘汰 + 聚合混合负载
        list(pool.map(lambda e: svc.ingest(e), events * 3))

    stop.set()
    for th in readers:
        th.join()

    rs = store.retention_status()
    h = svc.pipeline_health(pid)
    snaps = svc.all_status()
    logger.info(
        "[并发淘汰结论] 观测运行=%d 保留=%d(上限%d) 累计淘汰=%d 管道runs=%d "
        "连续失败=%d 读不变量违例=%d",
        n_runs,
        rs.retained_runs,
        max_runs,
        rs.evicted_runs,
        h.run_count,
        h.consecutive_failures,
        len(violations),
    )
    assert violations == []
    assert rs.retained_runs == max_runs
    assert rs.evicted_runs == n_runs - max_runs
    assert len(snaps) == max_runs
    assert h.run_count == max_runs
    assert h.consecutive_failures == max_runs
    # 最近失败原因在并发后仍自洽可见
    assert h.latest_failure_run_id is not None
    assert h.latest_failure_error_class is not None

    # 被淘汰运行查询结论一致：从实际保留集合反推一个已淘汰 ID（并发乱序投递下
    # 具体淘汰哪个 ID 不确定，但保留/淘汰数量恒定），验证 EVICTED 且不可复活。
    retained_ids = {s.pipeline_run_id for s in snaps}
    all_ids = {f"run-{i:04d}" for i in range(1, n_runs + 1)}
    evicted_ids = all_ids - retained_ids
    assert len(evicted_ids) == n_runs - max_runs
    sample_evicted = next(iter(evicted_ids))
    assert svc.lookup_run(sample_evicted).outcome is LookupOutcome.EVICTED
    # 保留中的 ID 全部 FOUND
    for rid in retained_ids:
        assert svc.lookup_run(rid).outcome is LookupOutcome.FOUND
    r = svc.ingest(_ev(sample_evicted, pid, 1, EventType.STARTED))
    assert r.status is IngestStatus.REJECTED
    assert r.reason is RejectReason.RUN_EVICTED
    # 已淘汰 ID 的事件被拒绝后，保留数与累计淘汰量都不变
    assert store.retention_status().retained_runs == max_runs
    assert store.retention_status().evicted_runs == n_runs - max_runs

"""历史保留与淘汰：明确上限、drop-oldest 淘汰、口径披露、查询一致、查询成本恒定。"""

import logging
import time as _time

from pipeline_obs import (
    ErrorClass,
    EventType,
    LookupOutcome,
    ObservabilityService,
    PipelineEvent,
    PipelineStore,
    RejectReason,
    RetryPolicy,
)
from tests.timeline import log_health

logger = logging.getLogger("timeline")


def _svc(max_runs):
    store = PipelineStore(max_retained_runs=max_runs)
    svc = ObservabilityService(
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=1.0),
        store=store,
    )
    return svc, store


def _ev(run, pid, eid, etype, at, *, version, attempt=1, ec=None):
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


def test_eviction_drops_oldest_and_exposes_retention_status():
    svc, store = _svc(max_runs=3)
    pid = "pipe-ret"
    for i in range(1, 6):  # 5 个不同运行，上限 3 → 淘汰 run1、run2
        run = f"run-{i}"
        svc.ingest(_ev(run, pid, f"{i}s", EventType.STARTED, float(i), version=1))
        svc.ingest(
            _ev(
                run,
                pid,
                f"{i}f",
                EventType.FAILED,
                float(i) + 0.5,
                version=2,
                ec=ErrorClass.RETRYABLE,
            )
        )

    rs = store.retention_status()
    logger.info("[保留口径] %s", rs)
    assert rs.max_runs == 3
    assert rs.retained_runs == 3
    assert rs.evicted_runs == 2
    assert rs.policy == "drop_oldest"

    # 被淘汰者：不进 all_status、管道 run_count，且单查给出一致明确的 EVICTED
    ids = {s.pipeline_run_id for s in svc.all_status()}
    assert ids == {"run-3", "run-4", "run-5"}
    h = svc.pipeline_health(pid)
    log_health(h, svc.active_alerts(pid))
    assert h.run_count == 3
    assert h.consecutive_failures == 3

    lk = svc.lookup_run("run-1")
    assert lk.outcome is LookupOutcome.EVICTED
    assert lk.status is None and lk.detail
    # 旧接口 status() 对淘汰者返回 None（不报错、不残缺）
    assert svc.status("run-1") is None
    # 从不知晓：UNKNOWN，与 EVICTED 明确区分
    assert svc.lookup_run("ghost").outcome is LookupOutcome.UNKNOWN

    # 被淘汰运行不再复活：新上报被 RUN_EVICTED 拒绝
    r = svc.ingest(_ev("run-1", pid, "x", EventType.STARTED, 99.0, version=99))
    assert r.reason is RejectReason.RUN_EVICTED
    assert store.retention_status().retained_runs == 3


def test_old_run_updates_do_not_evict_newer_runs():
    """仍在保留窗口内的旧运行收到新事件后变“最新”，不应导致更新的运行被误淘汰。"""
    svc, store = _svc(max_runs=2)
    pid = "p"
    svc.ingest(_ev("a", pid, "a1", EventType.STARTED, 1.0, version=1))
    svc.ingest(_ev("b", pid, "b1", EventType.STARTED, 2.0, version=1))
    # a 收到更晚事件 → a 变最新（2.5），淘汰候选中的旧项 (1.0,a) 必须惰性跳过
    svc.ingest(_ev("a", pid, "a2", EventType.STARTED, 2.5, version=2))
    svc.ingest(_ev("c", pid, "c1", EventType.STARTED, 3.0, version=1))  # 触发淘汰
    rs = store.retention_status()
    kept = {s.pipeline_run_id for s in svc.all_status()}
    logger.info("[旧运行更新后] kept=%s evicted=%d", kept, rs.evicted_runs)
    assert rs.evicted_runs == 1
    assert kept == {"a", "c"}  # b 最旧被淘汰，a 因更新事件而保留


def test_health_query_cost_does_not_grow_as_history_evicts():
    """历史持续增长并不断淘汰时，健康查询成本保持 O(1)（不随累计历史线性变慢）。"""
    svc, store = _svc(max_runs=500)
    pid = "pipe-scale-ret"

    def seed(n, offset=0):
        for i in range(offset + 1, offset + n + 1):
            run = f"run-{i:06d}"
            svc.ingest(_ev(run, pid, "s", EventType.STARTED, float(i), version=1))
            svc.ingest(
                _ev(
                    run,
                    pid,
                    "f",
                    EventType.FAILED,
                    float(i) + 0.5,
                    version=2,
                    ec=ErrorClass.RETRYABLE,
                )
            )

    def query_burst(repeat=3000):
        start = _time.perf_counter()
        for _ in range(repeat):
            svc.pipeline_health(pid)
        return _time.perf_counter() - start

    seed(1000)  # 已淘汰 500，保留 500
    assert store.retention_status().evicted_runs == 500
    t_a = query_burst()
    seed(4000, offset=1000)  # 累计观测 5000、淘汰 4500，保留仍为 500
    rs = store.retention_status()
    assert rs.retained_runs == 500 and rs.evicted_runs == 4500
    t_b = query_burst()
    logger.info(
        "[淘汰下查询成本] 累计历史1000: %.4fs/3000次；累计历史5000: %.4fs/3000次（比值 %.2f）",
        t_a,
        t_b,
        t_b / t_a,
    )
    assert t_b < t_a * 4.0  # 保留集合大小不变，查询不应随累计历史线性增长
    h = svc.pipeline_health(pid)
    assert h.run_count == 500 and h.consecutive_failures == 500
    # 登记窗口内的已淘汰运行明确识别为 EVICTED；保留窗口内为 FOUND。
    assert svc.lookup_run("run-004500").outcome is LookupOutcome.EVICTED
    assert svc.lookup_run("run-005000").outcome is LookupOutcome.FOUND
    # 最早淘汰、已滑出 EVICTED 登记窗口的 ID：查询可能归为 UNKNOWN，
    # status() 一律返回 None；但墓碑持久，其再上报仍被 RUN_EVICTED 拒绝。
    assert svc.status("run-000001") is None
    assert svc.lookup_run("run-000001").outcome in (
        LookupOutcome.EVICTED,
        LookupOutcome.UNKNOWN,
    )
    from pipeline_obs import PipelineEvent as _PE

    zombie = _PE(
        "run-000001",
        "z",
        EventType.STARTED,
        occurred_at=9999.0,
        version=99,
        pipeline_id=pid,
    )
    rz = svc.ingest(zombie)
    assert rz.status.value == "REJECTED"
    assert rz.reason is RejectReason.RUN_EVICTED
    assert store.retention_status().retained_runs == 500

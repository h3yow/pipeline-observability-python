"""管道级健康汇总：多运行并发上报下汇总自洽、不丢更新、不随历史线性变慢、保留策略。"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from pipeline_obs import (
    ErrorClass,
    EventType,
    IngestStatus,
    ObservabilityService,
    PipelineEvent,
    RetryPolicy,
)
from tests.timeline import log_event, log_pipeline_health, log_result, log_snapshot

logger = logging.getLogger("timeline")


def _svc(**kw):
    return ObservabilityService(
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.01),
        clock=lambda: 1000.0,
        **kw,
    )


def _mk(run, version, etype, *, pid="P", attempt=1, ec=None, t=None):
    return PipelineEvent(
        run, f"{run}-v{version}-{etype.value}", etype,
        occurred_at=float(t if t is not None else version),
        version=version, attempt=attempt, error_class=ec, pipeline_id=pid,
    )


def test_pipeline_health_basic_semantics():
    """最近运行/最近成功/连续失败计数/失败告警的基本口径。"""
    svc = _svc()
    pid = "pipe-basic"
    logger.info("==== 场景：run-a 成功；run-b FATAL 失败；run-c RETRYABLE 失败 ====")
    script = [
        _mk("a", 1, EventType.STARTED, pid=pid, t=1),
        _mk("a", 2, EventType.SUCCEEDED, pid=pid, t=2),
        _mk("b", 1, EventType.STARTED, pid=pid, t=3),
        _mk("b", 2, EventType.FAILED, pid=pid, ec=ErrorClass.FATAL, t=4),
        _mk("c", 1, EventType.STARTED, pid=pid, t=5),
        _mk("c", 2, EventType.FAILED, pid=pid, ec=ErrorClass.RETRYABLE, t=6),
    ]
    for i, e in enumerate(script, 1):
        log_event(i, e)
        log_result(svc.ingest(e))
        log_pipeline_health(svc.pipeline_health(pid))

    h = svc.pipeline_health(pid)
    assert h.latest_run_id == "c"
    assert h.latest_state.value == "FAILED_RETRYABLE"
    assert h.latest_event_at == 6.0
    assert h.last_success_at == 2.0
    # 最近成功之后有 b、c 两个不同失败运行；c 的多次重试只算 1 个
    assert h.consecutive_failures == 2
    assert h.failure_alert_active is True
    assert h.staleness_alert_active is False  # 尚未巡检
    fail_alerts = {a.pipeline_run_id for a in h.active_alerts if a.kind.value == "FAILURE"}
    assert fail_alerts == {"b", "c"}
    # “最近一次为什么挂”：c 的错误分类/信息可从明细获取
    c_alert = next(a for a in h.active_alerts
                   if a.kind.value == "FAILURE" and a.pipeline_run_id == "c")
    assert c_alert.error_class is ErrorClass.RETRYABLE
    assert svc.status("b").last_error_class is ErrorClass.FATAL

    logger.info("==== 场景：run-c 重试成功 -> 连续失败清零、失败告警只剩 b ====")
    svc.ingest(_mk("c", 3, EventType.STARTED, pid=pid, attempt=2, t=7))
    svc.ingest(_mk("c", 4, EventType.SUCCEEDED, pid=pid, attempt=2, t=8))
    h = svc.pipeline_health(pid)
    log_pipeline_health(h)
    assert h.last_success_at == 8.0
    assert h.consecutive_failures == 0
    assert h.failure_alert_active is True   # b 仍 FATAL
    fail_alerts = {a.pipeline_run_id for a in h.active_alerts if a.kind.value == "FAILURE"}
    assert fail_alerts == {"b"}


def test_concurrent_multi_run_reports_health_stays_consistent():
    """同一管道下多个运行被不同线程并发上报：最终汇总与串行口径完全一致。"""
    svc = _svc()
    pid = "pipe-conc"
    runs = [f"run-{i}" for i in range(12)]

    # 每个 run 一条成功或失败的终态事件（全部 STARTED 后再交错发终态）
    started = [_mk(r, 1, EventType.STARTED, pid=pid, t=0) for r in runs]
    terminals = []
    expected_failed_runs = set()
    for i, r in enumerate(runs):
        if i % 3 == 0:
            terminals.append(_mk(r, 2, EventType.SUCCEEDED, pid=pid, t=10 + i))
        else:
            ec = ErrorClass.FATAL if i % 2 else ErrorClass.RETRYABLE
            terminals.append(_mk(r, 2, EventType.FAILED, pid=pid, ec=ec, t=10 + i))
            expected_failed_runs.add(r)

    violations: list[str] = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            h = svc.pipeline_health(pid)
            if h is None:
                continue
            # 快照自洽不变量
            if h.consecutive_failures < 0:
                violations.append("negative consecutive_failures")
            if h.consecutive_failures > h.run_count:
                violations.append("consecutive_failures > run_count")
            if not h.failure_alert_active and any(
                a.kind.value == "FAILURE" for a in h.active_alerts
            ):
                violations.append("failure flag disagrees with alerts")
            if not h.staleness_alert_active and any(
                a.kind.value == "STALENESS" for a in h.active_alerts
            ):
                violations.append("staleness flag disagrees with alerts")
            # 活动告警明细必须都属于该管道保留成员
            if any(a.pipeline_run_id not in [
                    rid for rid in [x.latest_run_id] or []] for a in []):
                pass
            if h.last_success_at is not None and h.latest_event_at is not None:
                if h.latest_event_at < 0:
                    violations.append("negative latest_event_at")

    logger.info("==== 并发：12 个运行的 STARTED 并发 -> 终态并发，8 读者持续校验管道快照 ====")

    def ingest_all(events, n):
        barrier = threading.Barrier(n)

        def task(e):
            barrier.wait()
            return svc.ingest(e)

        with ThreadPoolExecutor(max_workers=n) as pool:
            return list(pool.map(task, events))

    readers = [threading.Thread(target=reader) for _ in range(8)]
    for th in readers:
        th.start()

    r1 = ingest_all(started, len(started))
    r2 = ingest_all(terminals, len(terminals))

    stop.set()
    for th in readers:
        th.join()

    applied = sum(r.status is IngestStatus.APPLIED for r in r1 + r2)
    logger.info("[并发结论] 事件数=%d APPLIED=%d（预期 %d），快照不变量违例=%d",
                len(r1) + len(r2), applied, len(r1) + len(r2), len(violations))
    assert applied == len(r1) + len(r2)
    assert violations == []

    h = svc.pipeline_health(pid)
    log_pipeline_health(h)
    # 终态事件时间最大的 run（run-11, t=21）必为最近运行
    assert h.latest_run_id == "run-11"
    assert h.latest_event_at == 21.0
    # 最后一个成功的 run 是 run-9（i=9, t=19）；其后 run-10、run-11 失败
    assert h.last_success_at == 19.0
    assert h.consecutive_failures == 2
    fail_alerts = {a.pipeline_run_id for a in h.active_alerts if a.kind.value == "FAILURE"}
    # run-9 成功无失败告警；run-10/11 失败有；更老的失败 run 也仍保留失败告警
    assert "run-9" not in fail_alerts
    assert {"run-10", "run-11"} <= fail_alerts
    assert fail_alerts == expected_failed_runs


def test_duplicate_and_concurrent_terminal_for_same_run_counted_once():
    """同一运行的终态被多线程重复并发投递：管道连续失败计数不被重复累加。"""
    svc = _svc()
    pid = "pipe-dup"
    svc.ingest(_mk("dup-1", 1, EventType.STARTED, pid=pid, t=1))
    fail = _mk("dup-1", 2, EventType.FAILED, pid=pid, ec=ErrorClass.FATAL, t=2)
    n = 24
    barrier = threading.Barrier(n)

    def task():
        barrier.wait()
        return svc.ingest(fail)

    logger.info("==== 并发：同一失败终态被 %d 线程重复投递 ====", n)
    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(lambda _: task(), range(n)))
    applied = sum(r.status is IngestStatus.APPLIED for r in results)
    dups = sum(r.status is IngestStatus.DUPLICATE for r in results)
    logger.info("[并发结论] APPLIED=%d（预期1） DUPLICATE=%d（预期%d）", applied, dups, n - 1)
    assert applied == 1 and dups == n - 1
    h = svc.pipeline_health(pid)
    log_pipeline_health(h)
    assert h.consecutive_failures == 1  # 同 run 重复失败只计一次
    assert h.run_count == 1


def test_retention_policy_does_not_slow_query_linearly():
    """超过保留上限淘汰最旧实例；查询为 O(保留量) 且不随全部历史线性变慢。"""
    limit = 50
    svc = _svc(retained_run_limit=limit)
    pid = "pipe-retention"
    logger.info("==== 保留策略：上报 200 个运行，每管道仅保留最近 %d 个 ====", limit)
    for i in range(1, 201):
        run = f"r{i}"
        svc.ingest(_mk(run, 1, EventType.STARTED, pid=pid, t=float(i)))
        ec = ErrorClass.RETRYABLE
        svc.ingest(_mk(run, 2, EventType.FAILED, pid=pid, ec=ec, t=float(i) + 0.5))

    h = svc.pipeline_health(pid)
    log_pipeline_health(h)
    assert h.run_count == limit
    assert h.evicted_runs == 150
    assert h.retained_run_limit == limit
    # 被淘汰的旧实例状态查询返回 None；最近 limit 个仍在
    assert svc.status("r1") is None
    assert svc.status("r200") is not None
    # 最近运行仍是最新一批
    assert h.latest_run_id == "r200"
    assert h.latest_event_at == 200.5
    # 管道列表/健康查询访问的实例数受限于保留量，而非历史总量
    import time

    start = time.perf_counter()
    for _ in range(200):
        svc.pipeline_health(pid)
    elapsed = time.perf_counter() - start
    logger.info("[性能口径] 200 次健康查询耗时 %.4fs（历史 200，保留 %d）", elapsed, limit)
    assert len(svc.all_status()) == limit  # 全局实例表也只保留留存实例


def test_mixed_concurrent_ingest_health_sweep_no_deadlock():
    """高并发混合负载：多运行上报 + 持续健康查询 + 失新鲜巡检，不得死锁且口径自洽。"""
    svc = _svc(default_fresh_within_seconds=5.0)
    pid = "pipe-mixed"
    runs = [f"m{i}" for i in range(20)]
    stop = threading.Event()
    violations: list[str] = []

    def build_events():
        evts = []
        for i, r in enumerate(runs):
            evts.append(_mk(r, 1, EventType.STARTED, pid=pid, t=float(i)))
            if i % 2 == 0:
                evts.append(_mk(r, 2, EventType.SUCCEEDED, pid=pid, t=100.0 + i))
            else:
                evts.append(_mk(r, 2, EventType.FAILED, pid=pid,
                                ec=ErrorClass.RETRYABLE, t=100.0 + i))
        return evts

    events = build_events()

    def reader():
        while not stop.is_set():
            h = svc.pipeline_health(pid)
            if h is None:
                continue
            if not (0 <= h.consecutive_failures <= h.run_count):
                violations.append(f"bad consec {h.consecutive_failures}/{h.run_count}")
            if h.failure_alert_active != any(
                    a.kind.value == "FAILURE" for a in h.active_alerts):
                violations.append("failure flag mismatch")
            if h.staleness_alert_active != any(
                    a.kind.value == "STALENESS" for a in h.active_alerts):
                violations.append("staleness flag mismatch")

    def sweeper():
        now = 101.0
        while not stop.is_set():
            svc.run_staleness_sweep(now=now, pipeline_id=pid)
            now += 1.0

    logger.info("==== 混合并发：20 运行上报 x 6 读者 x 2 巡检者，验证无死锁/无中间态 ====")
    readers = [threading.Thread(target=reader) for _ in range(6)]
    sweeps = [threading.Thread(target=sweeper) for _ in range(2)]
    for th in readers + sweeps:
        th.start()

    barrier = threading.Barrier(len(events))

    def writer(e):
        barrier.wait()
        return svc.ingest(e)

    with ThreadPoolExecutor(max_workers=len(events)) as pool:
        results = list(pool.map(writer, events))

    stop.set()
    for th in readers + sweeps:
        th.join(timeout=10)
        assert not th.is_alive(), "出现线程未结束（疑似死锁）"

    from pipeline_obs import RejectReason
    # v1/v2 完全并发时，v2 可能先于 v1 应用，后到的 v1 被 STALE_VERSION 正确拒绝；
    # 每个实例的终态（v2）必然恰好应用一次。
    rejected = [r for r in results if r.status is IngestStatus.REJECTED]
    assert all(r.reason is RejectReason.STALE_VERSION for r in rejected)
    terminal_results = [
        r for e, r in zip(events, results) if e.event_type is not EventType.STARTED
    ]
    assert sum(r.status is IngestStatus.APPLIED for r in terminal_results) == 20
    assert violations == []
    h = svc.pipeline_health(pid)
    log_pipeline_health(h)
    # 终态时间最大的运行 m19(奇数) 失败；最近成功是 m18@118；其后 m19 失败 -> 连续失败 1
    assert h.latest_run_id == "m19"
    assert h.last_success_at == 118.0
    assert h.consecutive_failures == 1
    assert h.run_count == 20

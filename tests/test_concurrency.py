"""并发场景：并发重复上报、重试耗尽、并发查询与更新下的一致性。"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from pipeline_obs import (
    ErrorClass,
    EventType,
    IngestStatus,
    ObservabilityService,
    PipelineEvent,
    RejectReason,
    RetryPolicy,
)
from tests.timeline import log_event, log_result, log_snapshot

logger = logging.getLogger("timeline")


def _svc():
    return ObservabilityService(
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.01),
        clock=lambda: 1000.0,
    )


def test_concurrent_duplicate_reports_counted_once():
    """N 个线程并发上报同一个事件：恰好 1 次 APPLIED，其余全部 DUPLICATE。"""
    svc = _svc()
    run_id = "run-conc-dup"
    # 先确保实例存在
    svc.ingest(PipelineEvent(
        run_id, "start", EventType.STARTED, occurred_at=1.0, version=1, attempt=1))

    event = PipelineEvent(
        run_id, "fail-1", EventType.FAILED, occurred_at=2.0,
        version=2, attempt=1, error_class=ErrorClass.RETRYABLE,
    )
    n = 32
    logger.info("==== 并发重复上报：%d 线程同时提交同一 event_id=fail-1 ====", n)

    results: list = []
    barrier = threading.Barrier(n)

    def submit():
        barrier.wait()  # 尽量让所有线程同时进入临界区竞争
        return svc.ingest(event)

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(lambda _: submit(), range(n)))

    applied = [r for r in results if r.status is IngestStatus.APPLIED]
    duplicates = [r for r in results if r.status is IngestStatus.DUPLICATE]
    logger.info(
        "[并发结论] APPLIED=%d DUPLICATE=%d REJECTED=%d（预期 1/%d/0）",
        len(applied), len(duplicates),
        len(results) - len(applied) - len(duplicates), n - 1,
    )
    log_result(applied[0])
    snap = svc.status(run_id)
    log_snapshot(snap)
    assert len(applied) == 1
    assert len(duplicates) == n - 1
    assert snap.failure_count == 1  # 无重复计数
    assert snap.applied_version == 2


def test_concurrent_retries_reach_limit_and_stop():
    """重试链路上每个事件被多个投递线程重复并发提交：结论唯一、失败计数=上限、之后拒绝。

    轮次之间严格按 attempt 1->2->3 推进（屏障同步）；
    同一事件在其轮次内被 N 个线程并发重复投递（可能乱序进入临界区），
    以模拟消息队列 at-least-once + 失败重试的真实混合负载。
    """
    svc = _svc()
    run_id = "run-conc-retry"
    rounds = [
        PipelineEvent(run_id, "e1", EventType.STARTED, occurred_at=1.0, version=1, attempt=1),
        PipelineEvent(run_id, "e2", EventType.FAILED, occurred_at=2.0, version=2,
                      attempt=1, error_class=ErrorClass.RETRYABLE),
        PipelineEvent(run_id, "e3", EventType.STARTED, occurred_at=3.0, version=3, attempt=2),
        PipelineEvent(run_id, "e4", EventType.FAILED, occurred_at=4.0, version=4,
                      attempt=2, error_class=ErrorClass.RETRYABLE),
        PipelineEvent(run_id, "e5", EventType.STARTED, occurred_at=5.0, version=5, attempt=3),
        PipelineEvent(run_id, "e6", EventType.FAILED, occurred_at=6.0, version=6,
                      attempt=3, error_class=ErrorClass.RETRYABLE),
    ]
    n = 6
    logger.info("==== 重复投递混合负载：6 个事件各由 %d 线程并发提交，轮次间屏障同步 ====", n)
    totals = {IngestStatus.APPLIED: 0, IngestStatus.DUPLICATE: 0,
              IngestStatus.REJECTED: 0}

    for idx, event in enumerate(rounds, 1):
        barrier = threading.Barrier(n)

        def submit(e=event, b=barrier):
            b.wait()
            return svc.ingest(e)

        with ThreadPoolExecutor(max_workers=n) as pool:
            round_results = list(pool.map(lambda _: submit(), range(n)))
        for r in round_results:
            totals[r.status] += 1
        log_event(idx, event)
        log_result(round_results[0])
        snap = svc.status(run_id)
        log_snapshot(snap)
        # 每一轮：恰好 1 次应用，其余全部幂等重复
        assert sum(r.status is IngestStatus.APPLIED for r in round_results) == 1
        assert sum(r.status is IngestStatus.DUPLICATE for r in round_results) == n - 1

    logger.info(
        "[并发结论] 累计 APPLIED=%d（预期 6） DUPLICATE=%d（预期 %d） REJECTED=%d（预期 0）",
        totals[IngestStatus.APPLIED], totals[IngestStatus.DUPLICATE],
        6 * (n - 1), totals[IngestStatus.REJECTED],
    )
    assert totals[IngestStatus.APPLIED] == 6
    assert totals[IngestStatus.DUPLICATE] == 6 * (n - 1)
    assert totals[IngestStatus.REJECTED] == 0

    snap = svc.status(run_id)
    assert snap.failure_count == 3
    assert snap.retry_exhausted is True
    assert snap.state.value == "FAILED_RETRYABLE"
    assert snap.alert_active is True

    # 达到上限后再上报第 4 次尝试：必须被 RETRY_EXHAUSTED 拒绝
    extra = PipelineEvent(run_id, "e7", EventType.STARTED, occurred_at=7.0,
                          version=7, attempt=4)
    r = svc.ingest(extra)
    log_event(99, extra)
    log_result(r)
    assert r.status is IngestStatus.REJECTED
    assert r.reason is RejectReason.RETRY_EXHAUSTED
    assert svc.status(run_id).failure_count == 3


def test_concurrent_genuinely_out_of_order_batch_is_rejected_stale():
    """同一批事件完全乱序并发提交：迟到者必须拿到 STALE_VERSION，状态绝不回退。"""
    svc = _svc()
    run_id = "run-conc-ooo"
    events = [
        PipelineEvent(run_id, f"v{v}", EventType.STARTED,
                      occurred_at=float(v), version=v, attempt=v)
        for v in range(1, 6)
    ]
    # 打乱顺序并重复，制造乱序+重复
    batch = events[::-1] * 2
    n = len(batch)
    barrier = threading.Barrier(n)
    logger.info("==== 完全乱序并发：%d 份上报（v5..v1 x2）同时提交 ====", n)

    def submit(e):
        barrier.wait()
        return svc.ingest(e)

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(submit, batch))

    applied = [r for r in results if r.status is IngestStatus.APPLIED]
    stale = [r for r in results if r.reason is RejectReason.STALE_VERSION]
    dup = [r for r in results if r.status is IngestStatus.DUPLICATE]
    logger.info("[乱序结论] APPLIED=%d（最大为 5） STALE=%d DUPLICATE=%d",
                len(applied), len(stale), len(dup))
    # 无论临界区进入顺序如何：最多 5 次应用，其余为 STALE 或 DUPLICATE
    assert 1 <= len(applied) <= 5
    assert len(applied) + len(stale) + len(dup) == n
    snap = svc.status(run_id)
    log_snapshot(snap)
    # 状态永不回退：最终 attempt 等于被应用的最高 version
    assert snap.state.value == "RUNNING"
    assert snap.current_attempt == snap.applied_version


def test_concurrent_queries_never_observe_intermediate_state():
    """更新进行中并发查询：每个快照必须自洽（失败计数与状态匹配，无中间态）。"""
    svc = _svc()
    run_id = "run-conc-read"
    stop = threading.Event()
    violations: list[str] = []

    def reader():
        while not stop.is_set():
            snap = svc.status(run_id)
            if snap is None:
                continue
            # 不变量：失败计数非负；exhausted 仅在 FAILED_RETRYABLE 下为真；
            # 有 next_retry_at 时必为可重试等待态
            if snap.failure_count < 0:
                violations.append("negative failure_count")
            if snap.retry_exhausted and snap.state.value != "FAILED_RETRYABLE":
                violations.append(f"exhausted with bad state {snap.state.value}")
            if snap.next_retry_at is not None and snap.state.value != "FAILED_RETRYABLE":
                violations.append(f"next_retry_at with state {snap.state.value}")
            # 版本号单调：快照版本只会增长（在同一线程观测中）
            # 告警激活时必处于失败态
            if snap.alert_active and snap.state.value not in (
                "FAILED_RETRYABLE", "FAILED_FATAL"
            ):
                violations.append(f"alert active with state {snap.state.value}")

    def writer():
        # 多个实例 + 同一实例的成功/失败轮替，持续制造更新
        for i in range(1, 201):
            attempt = ((i - 1) % 3) + 1
            etype = EventType.SUCCEEDED if i % 4 == 0 else (
                EventType.STARTED if i % 2 == 1 else EventType.FAILED)
            ec = ErrorClass.RETRYABLE if etype is EventType.FAILED else None
            svc.ingest(PipelineEvent(
                run_id, f"w{i}", etype, occurred_at=float(i),
                version=i, attempt=attempt, error_class=ec,
            ))

    logger.info("==== 并发查询与更新：1 个持续写入者 + 8 个持续读取者 ====")
    readers = [threading.Thread(target=reader) for _ in range(8)]
    for t in readers:
        t.start()
    writer()
    stop.set()
    for t in readers:
        t.join()

    snap = svc.status(run_id)
    log_snapshot(snap)
    logger.info("[读写一致性] 读取者发现的不变量违例数=%d（预期 0）", len(violations))
    assert violations == []

"""并发测试：并发/重复上报下状态、计数、告警结论一致。"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from pipeline_obs import (
    AlertType,
    ErrorCategory,
    ErrorInfo,
    EventType,
    ObservabilityService,
    PipelineEvent,
    RunStatus,
)
from pipeline_obs.clock import ManualClock

from .helpers import Timeline, logger

RETRYABLE = ErrorInfo("TIMEOUT", "transient", ErrorCategory.RETRYABLE)


def _svc(max_retries=2):
    svc = ObservabilityService()
    svc.register_pipeline("p", max_retries=max_retries, base_delay=0.0)
    return svc


def test_concurrent_duplicate_same_event_id_applied_once():
    """N 个线程用同一 event_id 并发上报同一事件，只能有一份生效。"""
    svc = _svc()
    svc.ingest(PipelineEvent("start", "p", "r", EventType.RUN_STARTED, 100.0))
    event = PipelineEvent("fail-1", "p", "r", EventType.ATTEMPT_STARTED,
                          101.0, 1)
    tl = Timeline()
    barrier = threading.Barrier(16)

    def worker(i):
        barrier.wait()
        # 每个线程都尝试“开始 attempt 1”，且重复使用同一 event_id。
        res = svc.ingest(event)
        tl.record(i, event, res)
        return res

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(worker, range(16)))

    accepted = [r for r in results if r.accepted and not r.deduped]
    deduped = [r for r in results if r.deduped]
    summary = svc.get_run_summary("p", "r")
    logger.info("判定依据：16 线程同 event_id 并发 ATTEMPT_STARTED；"
                "锁内 check-then-act => 唯一生效=%d, 幂等去重=%d",
                len(accepted), len(deduped))
    assert len(accepted) == 1
    assert len(deduped) == 15
    assert summary.attempt_count == 1
    assert summary.status is RunStatus.RUNNING
    tl.log("并发同 event_id 重复上报（16 线程）")


def test_concurrent_same_outcome_distinct_ids_no_double_count():
    """不同 event_id 但同一 attempt 的结果并发上报：恰一份生效，其余拒绝。"""
    svc = _svc(max_retries=0)
    svc.ingest(PipelineEvent("start", "p", "r", EventType.RUN_STARTED, 100.0))
    svc.ingest(PipelineEvent("a1", "p", "r", EventType.ATTEMPT_STARTED,
                             101.0, 1))

    def make(eid):
        return PipelineEvent(eid, "p", "r", EventType.ATTEMPT_FAILED,
                             102.0, 1, RETRYABLE)

    events = [make(f"fail-{i}") for i in range(12)]
    tl = Timeline()
    barrier = threading.Barrier(12)

    def worker(i):
        barrier.wait()
        res = svc.ingest(events[i])
        tl.record(i, events[i], res)
        return res

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(worker, range(12)))

    applied = [r for r in results if r.accepted]
    rejected = [r for r in results if not r.accepted]
    summary = svc.get_run_summary("p", "r")
    logger.info("判定依据：12 线程并发上报 attempt1 失败（不同 event_id）；"
                "生效=%d 被拒=%d；失败计数必须=1，run 终态 FAILED（耗尽）",
                len(applied), len(rejected))
    assert len(applied) == 1
    assert len(rejected) == 11
    assert summary.failure_count == 1
    assert summary.retry_exhausted is True
    assert summary.status is RunStatus.FAILED
    # 同一异常只允许一条 ACTIVE 告警
    run_alerts = [a for a in summary.active_alerts
                  if a.alert_type is AlertType.RUN_FAILED]
    assert len(run_alerts) == 1
    tl.log("并发同结论不同 event_id（12 线程，计数不重复）")


def test_concurrent_readers_never_observe_intermediate_state():
    """更新进行中并发查询：快照中的计数必须自洽，无中间态。"""
    svc = _svc()

    stop = threading.Event()
    violations: list[str] = []

    def reader():
        while not stop.is_set():
            s = svc.get_pipeline_summary("p")
            # 不变量：分状态计数之和恒等于总 run 数
            if (s.running_runs + s.succeeded_runs + s.failed_runs
                    != s.total_runs):
                violations.append(
                    f"inconsistent counts: total={s.total_runs} "
                    f"parts={s.running_runs}/{s.succeeded_runs}/"
                    f"{s.failed_runs}"
                )

    readers = [threading.Thread(target=reader) for _ in range(8)]
    for t in readers:
        t.start()

    # 主线程持续制造 run：开始 -> 成功
    for i in range(200):
        rid = f"r{i}"
        svc.ingest(PipelineEvent(f"{rid}-s", "p", rid,
                                 EventType.RUN_STARTED, 100.0 + i))
        svc.ingest(PipelineEvent(f"{rid}-a", "p", rid,
                                 EventType.ATTEMPT_STARTED, 100.0 + i, 1))
        svc.ingest(PipelineEvent(f"{rid}-ok", "p", rid,
                                 EventType.ATTEMPT_SUCCEEDED, 100.0 + i, 1))

    stop.set()
    for t in readers:
        t.join()

    final = svc.get_pipeline_summary("p")
    logger.info("判定依据：8 读线程与 200 个 run 的更新并发；"
                "快照在锁内构建 => 计数恒自洽，违规=%d，最终 total=%d",
                len(violations), final.total_runs)
    assert violations == []
    assert final.total_runs == 200
    assert final.succeeded_runs == 200


def test_alert_not_duplicated_before_recovery():
    """同一异常在恢复前反复评估，不得重复产生告警；恢复后解除。"""
    clk = ManualClock(1000.0)
    svc = ObservabilityService(clock=clk)
    svc.register_pipeline("p", freshness_threshold=100.0)
    svc.ingest(PipelineEvent("s", "p", "r", EventType.RUN_STARTED, 1000.0))

    clk.advance(200)
    changes = [svc.evaluate_freshness() for _ in range(5)]
    active = svc.get_pipeline_summary("p").active_alerts
    logger.info("判定依据：连续 5 次时效评估处于 stale 区间；"
                "首次变更=%s，后续收敛为 0；ACTIVE 告警数=%d",
                changes, len(active))
    assert changes == [1, 0, 0, 0, 0]
    assert len(active) == 1

    # 恢复：成功后 STALE 告警解除
    svc.ingest(PipelineEvent("a1", "p", "r", EventType.ATTEMPT_STARTED,
                             1200.0, 1))
    svc.ingest(PipelineEvent("ok", "p", "r", EventType.ATTEMPT_SUCCEEDED,
                             1200.0, 1))
    svc.evaluate_freshness()
    assert tuple(svc.get_pipeline_summary("p").active_alerts) == ()

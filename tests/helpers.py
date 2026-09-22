"""测试辅助：记录并打印「事件时间线」与「判定依据」。"""
from __future__ import annotations

import logging
import threading

from pipeline_obs.models import PipelineEvent

logger = logging.getLogger("pipeline_obs.tests")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


class Timeline:
    """线程安全的事件受理时间线。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.records: list[dict] = []

    def record(self, seq: int, event: PipelineEvent, result) -> None:
        with self._lock:
            self.records.append({
                "seq": seq,
                "thread": threading.current_thread().name,
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "attempt_no": event.attempt_no,
                "event_time": event.event_time,
                "accepted": result.accepted,
                "deduped": result.deduped,
                "reason": result.reason.value if result.reason else "-",
                "status": result.run_status.value,
            })

    def log(self, title: str) -> None:
        logger.info("=" * 72)
        logger.info("时间线：%s", title)
        logger.info(
            "%-4s %-12s %-8s %-18s %-5s t=%-8s %-7s %-7s -> %-9s %s",
            "seq", "thread", "event_id", "event_type", "att", "time",
            "accept", "dedup", "status", "reason",
        )
        for r in sorted(self.records, key=lambda x: x["seq"]):
            logger.info(
                "%-4d %-12s %-8s %-18s %-5s t=%-8.1f %-7s %-7s -> %-9s %s",
                r["seq"], r["thread"], r["event_id"], r["event_type"],
                str(r["attempt_no"]), r["event_time"],
                str(r["accepted"]), str(r["deduped"]), r["status"],
                r["reason"],
            )
        logger.info("=" * 72)

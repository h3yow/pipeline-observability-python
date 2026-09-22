"""重试调度：退避序列、次数上限、可重试/不可重试区分。"""

import logging

from pipeline_obs import ErrorClass, RetryPolicy
from pipeline_obs.retry import compute_backoff_delay, decide_retry

logger = logging.getLogger("timeline")


def test_backoff_is_exponential_and_capped():
    policy = RetryPolicy(
        max_attempts=5, base_delay_seconds=1.0, multiplier=2.0, max_delay_seconds=10.0
    )
    delays = [compute_backoff_delay(policy, a) for a in range(1, 6)]
    logger.info("[退避序列] attempts=1..5 delays=%s（预期 1,2,4,8,10 封顶）", delays)
    assert delays == [1.0, 2.0, 4.0, 8.0, 10.0]


def test_retryable_schedules_with_backoff_until_limit():
    policy = RetryPolicy(max_attempts=3, base_delay_seconds=1.0, multiplier=2.0)
    d1 = decide_retry(policy, failed_attempt=1, error_class=ErrorClass.RETRYABLE, now=0.0)
    d2 = decide_retry(policy, failed_attempt=2, error_class=ErrorClass.RETRYABLE, now=0.0)
    d3 = decide_retry(policy, failed_attempt=3, error_class=ErrorClass.RETRYABLE, now=0.0)
    logger.info("[重试判定] a1=%s", d1.reason)
    logger.info("[重试判定] a2=%s", d2.reason)
    logger.info("[重试判定] a3=%s", d3.reason)
    assert (d1.should_retry, d1.next_attempt, d1.delay_seconds) == (True, 2, 1.0)
    assert (d2.should_retry, d2.next_attempt, d2.delay_seconds) == (True, 3, 2.0)
    assert d3.should_retry is False
    assert "次数上限" in d3.reason


def test_fatal_error_is_never_retried_and_reason_distinct():
    policy = RetryPolicy(max_attempts=5)
    d = decide_retry(policy, failed_attempt=1, error_class=ErrorClass.FATAL, now=0.0)
    logger.info("[不可重试] %s", d.reason)
    assert d.should_retry is False
    assert "FATAL" in d.reason

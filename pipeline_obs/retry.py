"""失败重试调度：可重试判定、指数退避、次数上限。"""

from __future__ import annotations

import random

from .models import ErrorClass, RetryDecision, RetryPolicy


def compute_backoff_delay(
    policy: RetryPolicy, attempt: int, *, jitter: bool = False
) -> float:
    """根据失败时的 attempt 序号计算退避延迟（秒）。

    attempt 从 1 开始；采用 base * multiplier**(attempt-1) 并以 max_delay 封顶。
    jitter=True 时叠加 [0, 步进值) 的随机抖动，避免重试风暴（默认关闭，保证可测试）。
    """
    if attempt < 1:
        raise ValueError(f"attempt 必须从 1 开始，实际为 {attempt}")
    raw = policy.base_delay_seconds * policy.multiplier ** (attempt - 1)
    delay = min(raw, policy.max_delay_seconds)
    if jitter and delay > 0:
        delay = min(
            policy.max_delay_seconds,
            delay + random.random() * policy.base_delay_seconds,
        )
    return delay


def decide_retry(
    policy: RetryPolicy,
    *,
    failed_attempt: int,
    error_class: ErrorClass,
    now: float,
) -> RetryDecision:
    """对一次失败给出是否继续重试的调度结论。

    判定顺序（依据同时写入 reason，保证结论可解释）：
    1. FATAL 错误：不可重试，立即终止，不消耗后续退避。
    2. RETRYABLE 且 failed_attempt 已达 max_attempts：次数耗尽，不再重试。
    3. 其余 RETRYABLE：退避后调度第 failed_attempt+1 次尝试。
    """
    if failed_attempt < 1:
        raise ValueError(f"failed_attempt 必须从 1 开始，实际为 {failed_attempt}")

    if error_class is ErrorClass.FATAL:
        return RetryDecision(
            should_retry=False,
            next_attempt=failed_attempt,
            delay_seconds=0.0,
            reason="FATAL 错误不可重试，立即终止",
        )

    if failed_attempt >= policy.max_attempts:
        return RetryDecision(
            should_retry=False,
            next_attempt=failed_attempt,
            delay_seconds=0.0,
            reason=(
                f"可重试失败已达次数上限 max_attempts={policy.max_attempts}，停止重试"
            ),
        )

    delay = compute_backoff_delay(policy, failed_attempt)
    return RetryDecision(
        should_retry=True,
        next_attempt=failed_attempt + 1,
        delay_seconds=delay,
        reason=(
            f"第 {failed_attempt} 次尝试失败（RETRYABLE），"
            f"退避 {delay:.3f}s 后进行第 {failed_attempt + 1} 次尝试"
        ),
    )

"""重试策略：错误可重试判定、退避计算、次数上限。

规则：
- FATAL（不可重试）错误立即终结运行，不调度重试。
- RETRYABLE 错误累计失败次数；当 failure_count > max_retries 时终止（耗尽）。
  即运行最多经历 ``max_retries + 1`` 次尝试（首次 + max_retries 次重试）。
- 未耗尽时按指数退避调度：delay = base_delay * factor**(failure_count-1)。
"""
from __future__ import annotations

from .models import ErrorCategory, ErrorInfo, RetryState, RunRecord, RunStatus


class RetryPolicy:
    """根据错误分类与重试状态决定是否继续重试及下次退避时间。"""

    def __init__(self, default_max_retries: int = 3,
                 default_base_delay: float = 1.0,
                 backoff_factor: float = 2.0) -> None:
        self.default_max_retries = default_max_retries
        self.default_base_delay = default_base_delay
        self.backoff_factor = backoff_factor

    def is_retryable(self, error: ErrorInfo) -> bool:
        return error.category is ErrorCategory.RETRYABLE

    def next_delay(self, retry: RetryState, failure_count: int) -> float:
        """指数退避：第 1 次失败后等待 base_delay，之后每次乘 backoff_factor。"""
        if failure_count < 1:
            raise ValueError("failure_count must be >= 1")
        return retry.base_delay * (retry.backoff_factor ** (failure_count - 1))

    def register_failure(self, run: RunRecord, error: ErrorInfo,
                         failed_at: float) -> bool:
        """登记一次失败并更新 run/retry 状态。

        返回 True 表示已调度下一次重试；False 表示运行进入终态 FAILED
        （原因可能是 FATAL 错误或重试次数耗尽，由 terminal_error 与
        retry.exhausted 区分）。
        """
        if run.retry is None:
            run.retry = RetryState()
        retry = run.retry
        retry.last_error = error
        retry.failure_count += 1
        retry.next_retry_at = None

        if error.category is ErrorCategory.FATAL:
            # 不可重试错误：立即终结。
            run.status = RunStatus.FAILED
            run.terminal_error = error
            run.finished_at = failed_at
            return False

        if retry.failure_count > retry.max_retries:
            # 可重试错误但次数已达上限：终结并标记耗尽。
            retry.exhausted = True
            run.status = RunStatus.FAILED
            run.terminal_error = error
            run.finished_at = failed_at
            return False

        # 仍可重试：调度下一次尝试。
        retry.next_retry_at = failed_at + self.next_delay(
            retry, retry.failure_count
        )
        return True

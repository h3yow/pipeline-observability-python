"""数据管道运行观测：核心领域模型与枚举定义。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class PipelineState(str, Enum):
    """管道运行状态机。"""

    PENDING = "PENDING"          # 已接收启动事件，尚未开始运行
    RUNNING = "RUNNING"          # 运行中
    SUCCEEDED = "SUCCEEDED"      # 最近一次运行成功
    FAILED_RETRYABLE = "FAILED_RETRYABLE"  # 失败，可重试（退避等待中）
    FAILED_FATAL = "FAILED_FATAL"          # 失败，不可重试（终态）


class EventType(str, Enum):
    """上报事件类型。"""

    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class ErrorClass(str, Enum):
    """失败错误分类。"""

    RETRYABLE = "RETRYABLE"      # 可重试错误（如瞬时网络抖动、上游超时）
    FATAL = "FATAL"              # 不可重试错误（如数据格式错误、配置缺失）


class RejectReason(str, Enum):
    """事件被拒绝（冲突）的可区分原因。"""

    STALE_VERSION = "STALE_VERSION"              # 乱序：事件版本号落后于已应用版本
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"    # 状态机不允许该流转
    FATAL_ALREADY_TERMINAL = "FATAL_ALREADY_TERMINAL"  # 实例已进入不可重试终态
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"          # 可重试失败已达次数上限，不得再调度
    CONFLICTING_OUTCOME = "CONFLICTING_OUTCOME"  # 同一 attempt 收到互相冲突的结果上报


class IngestStatus(str, Enum):
    """事件接收结论。"""

    APPLIED = "APPLIED"        # 事件被接受并推进了状态
    DUPLICATE = "DUPLICATE"    # 重复上报：幂等忽略，状态不变、不重复计数
    REJECTED = "REJECTED"      # 冲突上报：被拒绝，附带 RejectReason


@dataclass(frozen=True)
class PipelineEvent:
    """一次管道运行事件上报。

    attempt 为运行尝试序号（从 1 开始）；version 为单调递增的事件序号，
    用于判定乱序上报；同一 (event_id) 的重复上报必须幂等。
    """

    pipeline_run_id: str
    event_id: str
    event_type: EventType
    occurred_at: float
    version: int
    attempt: int = 1
    error_class: ErrorClass | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class RetryPolicy:
    """失败重试策略：指数退避 + 次数上限。"""

    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    multiplier: float = 2.0


@dataclass(frozen=True)
class RetryDecision:
    """对一次失败的重试调度结论。"""

    should_retry: bool
    next_attempt: int
    delay_seconds: float
    reason: str


@dataclass
class AlertState:
    """单个管道实例的告警收敛状态。"""

    active: bool = False          # 当前是否存在未恢复的活动告警
    raised_count: int = 0         # 累计告警次数（同一异常恢复前不重复计数）
    last_raised_version: int | None = None
    last_recovered_version: int | None = None
    reason: str | None = None     # 当前活动告警的判定依据


@dataclass
class PipelineStatus:
    """面向下游查询的状态汇总快照。"""

    pipeline_run_id: str
    state: PipelineState
    current_attempt: int
    applied_version: int
    last_success_at: float | None
    started_at: float | None
    latest_event_at: float | None
    failure_count: int
    next_retry_at: float | None
    retry_exhausted: bool
    alert_active: bool
    alert_reason: str | None

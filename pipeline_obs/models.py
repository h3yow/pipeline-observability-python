"""领域类型定义：事件、状态、错误、告警与汇总快照。

本模块只包含枚举与不可变/可变数据结构，不含业务逻辑。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class EventType(str, Enum):
    """上报事件类型。"""

    RUN_STARTED = "RUN_STARTED"
    ATTEMPT_STARTED = "ATTEMPT_STARTED"
    ATTEMPT_SUCCEEDED = "ATTEMPT_SUCCEEDED"
    ATTEMPT_FAILED = "ATTEMPT_FAILED"
    RUN_ABORTED = "RUN_ABORTED"


class RunStatus(str, Enum):
    """管道运行实例状态。"""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class ErrorCategory(str, Enum):
    """错误分类：决定是否可重试。"""

    RETRYABLE = "RETRYABLE"
    FATAL = "FATAL"
    NONE = "NONE"


class RejectReason(str, Enum):
    """事件被拒绝（或去重）时的可区分原因。"""

    UNKNOWN_PIPELINE = "UNKNOWN_PIPELINE"
    INVALID_TRANSITION = "INVALID_TRANSITION"          # 状态回退/终态后上报
    STALE_EVENT = "STALE_EVENT"                        # 乱序：事件时间过旧
    DUPLICATE_ATTEMPT = "DUPLICATE_ATTEMPT"            # 同一 attempt 重复上报
    DUPLICATE_OUTCOME = "DUPLICATE_OUTCOME"            # 成功/失败结论重复
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"                # 已达重试上限
    ATTEMPT_OUT_OF_SEQUENCE = "ATTEMPT_OUT_OF_SEQUENCE"  # 尝试序号跳跃
    RETRY_NOT_DUE = "RETRY_NOT_DUE"                    # 退避时间未到
    CONFLICTING_OUTCOME = "CONFLICTING_OUTCOME"        # 同一 attempt 结论互相冲突
    UNKNOWN_ATTEMPT = "UNKNOWN_ATTEMPT"                # 未 start 的 attempt 上报结果


class AlertType(str, Enum):
    """告警类型。"""

    RUN_FAILED = "RUN_FAILED"
    STALE_PIPELINE = "STALE_PIPELINE"


class AlertStatus(str, Enum):
    ACTIVE = "ACTIVE"
    RESOLVED = "RESOLVED"


@dataclass(frozen=True)
class ErrorInfo:
    """一次失败尝试的错误信息。"""

    code: str
    message: str
    category: ErrorCategory


@dataclass(frozen=True)
class PipelineEvent:
    """外部上报的管道运行事件。"""

    event_id: str
    pipeline_id: str
    run_id: str
    event_type: EventType
    event_time: float
    attempt_no: int | None = None
    error: ErrorInfo | None = None


@dataclass
class AttemptRecord:
    """单次尝试的记录。"""

    attempt_no: int
    started_at: float | None = None
    finished_at: float | None = None
    succeeded: bool | None = None
    error: ErrorInfo | None = None


@dataclass
class RetryState:
    """重试调度状态。"""

    max_retries: int = 3
    base_delay: float = 1.0
    backoff_factor: float = 2.0
    failure_count: int = 0
    next_retry_at: float | None = None
    exhausted: bool = False
    last_error: ErrorInfo | None = None


@dataclass
class RunRecord:
    """一个管道运行实例（run）的完整状态。"""

    pipeline_id: str
    run_id: str
    status: RunStatus = RunStatus.RUNNING
    started_at: float | None = None
    last_event_time: float | None = None
    succeeded_at: float | None = None
    finished_at: float | None = None
    terminal_error: ErrorInfo | None = None
    attempts: dict[int, AttemptRecord] = field(default_factory=dict)
    retry: RetryState | None = None
    seen_event_ids: set[str] = field(default_factory=set)
    retry_open: bool = False  # 到期后是否允许上报下一次尝试


@dataclass
class PipelineRecord:
    """管道维度登记信息与最近成功时间。"""

    pipeline_id: str
    freshness_threshold: float = 300.0
    max_retries: int = 3
    base_delay: float = 1.0
    backoff_factor: float = 2.0
    last_success_at: float | None = None
    runs: dict[str, RunRecord] = field(default_factory=dict)


@dataclass(frozen=True)
class Alert:
    """告警。同一异常在恢复前只产生一条。"""

    alert_id: str
    pipeline_id: str
    run_id: str | None
    alert_type: AlertType
    status: AlertStatus
    created_at: float
    updated_at: float
    detail: str = ""


@dataclass(frozen=True)
class IngestResult:
    """事件受理结果。"""

    accepted: bool
    reason: RejectReason | None
    run_status: RunStatus
    detail: str = ""
    deduped: bool = False


@dataclass(frozen=True)
class RunSummary:
    """运行实例维度汇总快照。"""

    pipeline_id: str
    run_id: str
    status: RunStatus
    started_at: float | None
    finished_at: float | None
    attempt_count: int
    failure_count: int
    retry_exhausted: bool
    next_retry_at: float | None
    last_error_code: str | None
    last_error_category: ErrorCategory | None
    active_alerts: tuple[Alert, ...]


@dataclass(frozen=True)
class PipelineSummary:
    """管道维度汇总快照。"""

    pipeline_id: str
    last_success_at: float | None
    freshness_threshold: float
    is_stale: bool
    total_runs: int
    running_runs: int
    succeeded_runs: int
    failed_runs: int
    active_alerts: tuple[Alert, ...]

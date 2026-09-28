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


class AlertKind(str, Enum):
    """告警类别：失败告警与失新鲜告警互相独立，可并存、各自恢复。"""

    FAILURE = "FAILURE"        # 运行失败（可重试失败/不可重试终态/重试耗尽）
    STALENESS = "STALENESS"    # 超过新鲜窗口未成功（失新鲜）


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

    pipeline_id 为可选的管道归属标识，用于管道级健康汇总；缺省时该实例
    不归属任何管道（不影响原有单实例上报/查询口径）。
    """

    pipeline_run_id: str
    event_id: str
    event_type: EventType
    occurred_at: float
    version: int
    attempt: int = 1
    error_class: ErrorClass | None = None
    error_message: str | None = None
    pipeline_id: str | None = None


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
    """单个管道实例的失败告警收敛状态。"""

    active: bool = False          # 当前是否存在未恢复的活动失败告警
    raised_count: int = 0         # 累计失败告警次数（同一异常恢复前不重复计数）
    last_raised_version: int | None = None
    last_recovered_version: int | None = None
    reason: str | None = None     # 当前活动失败告警的判定依据


@dataclass
class StalenessAlertState:
    """单个管道实例的失新鲜告警收敛状态（与失败告警独立）。

    同一段失新鲜期间（进入失新鲜 -> 重新成功恢复新鲜）只告警一次、只计一次；
    巡检重复执行不会反复刷告警。
    """

    active: bool = False          # 当前是否处于失新鲜告警中
    raised_count: int = 0         # 累计失新鲜告警次数（每段失新鲜期间最多 1 次）
    raised_at: float | None = None           # 当前/最近一次失新鲜告警触发时间（巡检时钟）
    last_recovered_at: float | None = None  # 最近一次恢复新鲜的时间
    basis: str | None = None      # 基线类型：LAST_SUCCESS / FIRST_REPORT
    basis_time: float | None = None         # 判定所用基线时间
    threshold_seconds: float | None = None  # 判定所用新鲜阈值
    age_seconds: float | None = None        # 触发时距基线的时间差
    reason: str | None = None     # 当前失新鲜告警的判定依据文本


@dataclass(frozen=True)
class StalenessAssessment:
    """一次失新鲜判定的结构化结论（判定依据可解释）。"""

    stale: bool
    basis: str                    # LAST_SUCCESS / FIRST_REPORT / NONE
    basis_time: float | None
    now: float
    age_seconds: float | None
    threshold_seconds: float
    reason: str


@dataclass(frozen=True)
class ActiveAlert:
    """管道健康汇总中的一条活动告警明细。"""

    kind: AlertKind
    pipeline_run_id: str
    reason: str
    since_version: int | None = None   # 失败告警：触发版本
    since_time: float | None = None    # 失新鲜告警：触发巡检时间
    basis: str | None = None           # 失新鲜告警：基线类型
    basis_time: float | None = None
    age_seconds: float | None = None
    threshold_seconds: float | None = None
    error_class: ErrorClass | None = None  # 失败告警：错误分类
    error_message: str | None = None


@dataclass(frozen=True)
class PipelineHealth:
    """管道级健康汇总快照（锁内一次性构造，字段彼此自洽）。"""

    pipeline_id: str
    latest_run_id: str | None                  # 最近一次运行的实例 ID
    latest_state: PipelineState | None         # 最近一次运行状态
    latest_event_at: float | None              # 最近一次事件时间
    last_success_at: float | None              # 管道最近一次成功时间（任意实例）
    consecutive_failures: int                  # 距最近一次成功之后连续失败的运行次数
    failure_alert_active: bool                 # 当前是否存在活动失败告警
    staleness_alert_active: bool               # 当前是否存在活动失新鲜告警
    active_alerts: tuple[ActiveAlert, ...]     # 活动告警明细（含判定依据）
    run_count: int                             # 当前保留的运行实例数
    retained_run_limit: int                    # 每管道保留实例上限（保留策略口径）
    evicted_runs: int                          # 累计因保留策略被淘汰的实例数


@dataclass
class PipelineStatus:
    """面向下游查询的状态汇总快照。

    新增的失新鲜相关字段全部带默认值，原有构造与调用方式保持兼容。
    """

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
    # ---- 新增：管道归属（默认 None，保持旧口径）----
    pipeline_id: str | None = None
    # ---- 新增：失败错误明细（供管道汇总展示“最近一次为什么挂”）----
    last_error_class: ErrorClass | None = None
    last_error_message: str | None = None
    # ---- 新增：失新鲜告警（与失败告警独立，默认未巡检）----
    staleness_alert_active: bool = False
    staleness_alert_reason: str | None = None
    staleness_assessment: StalenessAssessment | None = None

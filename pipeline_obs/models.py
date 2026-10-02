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
    RUN_EVICTED = "RUN_EVICTED"                  # 运行已被保留策略淘汰，不再接收事件


class FailureTerminalReason(str, Enum):
    """失败终止的两类可区分原因。"""

    NON_RETRYABLE = "NON_RETRYABLE"      # 错误本身不可重试（FATAL），立即终止
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"  # 错误可重试，但重试机会已用尽


class LookupStatus(str, Enum):
    """单运行查询的归属结论。"""

    PRESENT = "PRESENT"    # 运行在保留窗口内，snapshot 有效
    EVICTED = "EVICTED"    # 运行曾被观测到，但已按保留策略淘汰
    UNKNOWN = "UNKNOWN"    # 从未观测到该运行


class AlertKind(str, Enum):
    """告警类别：失败告警与失新鲜告警性质不同，可并存、各自独立恢复。"""

    FAILURE = "FAILURE"        # 失败告警：实例进入失败态（含重试耗尽/不可重试）
    STALENESS = "STALENESS"    # 失新鲜告警：距基线时间超过新鲜阈值


class StalenessBaseline(str, Enum):
    """失新鲜判定使用的基线类型。"""

    LAST_SUCCESS = "LAST_SUCCESS"    # 最近一次成功时间
    FIRST_REPORT = "FIRST_REPORT"    # 从未成功：第一次上报时间
    NONE = "NONE"                    # 尚无任何事件，无法判定


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
    # 可选：归属管道 ID。缺省时按“每实例自成一管道”处理（向后兼容）。
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
    """单个管道实例的告警收敛状态。"""

    active: bool = False          # 当前是否存在未恢复的活动告警
    raised_count: int = 0         # 累计告警次数（同一异常恢复前不重复计数）
    last_raised_version: int | None = None
    last_recovered_version: int | None = None
    reason: str | None = None     # 当前活动告警的判定依据
    last_raised_at: float | None = None   # 当前活动失败告警首次产生时间


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
    # —— 新增字段（均带默认值，旧的位置构造与旧字段含义不变）——
    pipeline_id: str | None = None              # 所属管道 ID
    staleness_alert_active: bool = False        # 是否存在活动的失新鲜告警
    staleness_alert_reason: str | None = None   # 失新鲜告警的当前判定依据
    staleness_raised_count: int = 0             # 失新鲜告警累计次数（每段失新鲜只计一次）
    active_alert_kinds: tuple[str, ...] = ()    # 当前活动告警类别（FAILURE / STALENESS）
    # —— 失败原因可见性（同样追加在末尾、带默认值，旧构造不受影响）——
    last_error_class: str | None = None         # 最近一次失败的错误分类（从未失败/已恢复成功后为 None）
    last_error_message: str | None = None       # 最近一次失败的错误信息（清空时机同上）
    failure_terminal_reason: str | None = None  # 终止原因：NON_RETRYABLE / RETRY_EXHAUSTED（非终止态为 None）
    last_failure_at: float | None = None        # 最近一次失败事件时间（清空时机同上）
    # —— 失新鲜恢复明细（活动状态与判定依据保持同一版结论）——
    staleness_recovered_at: float | None = None  # 最近一次从失新鲜恢复新鲜的时刻
    staleness_recovery_reason: str | None = None  # 恢复/新鲜结论的判定依据（与活动标志同版）


@dataclass(frozen=True)
class StalenessVerdict:
    """一次失新鲜判定的完整结论与判定依据。"""

    stale: bool                              # 结论：是否失新鲜
    baseline_kind: StalenessBaseline         # 使用的基线类型
    baseline_at: float | None                # 基线时间戳
    now: float                               # 判定时刻
    threshold_seconds: float                 # 新鲜阈值
    age_seconds: float | None                # 距今时间差（无基线时为 None）
    reason: str                              # 人类可读判定依据


@dataclass
class StalenessAlertState:
    """单个实例的失新鲜告警收敛状态：同一段失新鲜期间只告警一次。"""

    active: bool = False                     # 当前是否处于活动失新鲜告警
    raised_count: int = 0                    # 累计告警次数（每段失新鲜只计一次）
    episode_id: int = 0                      # 失新鲜段落序号，每次新发 +1
    raised_at: float | None = None           # 当前段落首次判定失新鲜的时刻
    last_checked_at: float | None = None     # 最近一次巡检时刻
    last_verdict: StalenessVerdict | None = None  # 最近一次判定结论（含依据）
    last_recovered_at: float | None = None   # 最近一次恢复新鲜的时刻
    last_recovery_reason: str | None = None  # 最近一次恢复/新鲜结论的判定依据（与活动标志同版）
    last_threshold_seconds: float | None = None  # 最近一次巡检所用阈值（供成功即时恢复复用）


@dataclass(frozen=True)
class ActiveAlertView:
    """管道下某条活动告警的明细视图。"""

    pipeline_run_id: str
    kind: AlertKind
    reason: str
    since: float | None          # 告警首次产生的时间
    version: int | None = None   # 触发告警的事件版本（失败告警）
    # —— 失败告警携带的最近一次失败原因（失新鲜告警恒为 None）——
    error_class: str | None = None       # 错误分类（RETRYABLE / FATAL）
    error_message: str | None = None     # 错误信息
    failure_terminal_reason: str | None = None  # NON_RETRYABLE / RETRY_EXHAUSTED
    last_failure_at: float | None = None        # 最近一次失败事件时间


@dataclass(frozen=True)
class PipelineHealth:
    """管道级健康汇总快照（锁内一次性构造，字段彼此自洽）。

    主体字段均为物化标量/计数，构造为 O(1)，不随历史运行数增长；
    完整活动告警明细可能很多，故只在快照里保留两类告警的数量，
    需要明细时用 PipelineRegistry.active_alerts(pipeline_id) 惰性展开。
    """

    pipeline_id: str
    run_count: int                                  # 汇总覆盖的运行实例数
    latest_run_id: str | None                       # 最近一次有事件的运行
    latest_run_state: PipelineState | None          # 最近一次运行的状态
    latest_event_at: float | None                   # 管道最近事件时间
    last_success_at: float | None                   # 管道最近一次成功时间
    consecutive_failures: int                       # 最近一次成功之后连续失败的运行数
    active_failure_alert_count: int                 # 当前活动失败告警数
    active_staleness_alert_count: int               # 当前活动失新鲜告警数
    has_failure_alert: bool                         # 当前是否有失败告警
    has_staleness_alert: bool                       # 当前是否有失新鲜告警
    staleness_threshold_seconds: float | None       # 巡检所用新鲜阈值（未巡检为 None）
    checked_at: float | None                        # 最近一次失新鲜巡检时刻
    # —— 管道维度“最近一次为什么挂”（追加字段，带默认值，旧构造兼容）——
    latest_failure_run_id: str | None = None        # 管道最近一次仍未恢复的失败运行
    latest_failure_at: float | None = None          # 该失败的事件时间
    latest_error_class: str | None = None           # 该失败的错误分类
    latest_error_message: str | None = None         # 该失败的错误信息
    latest_failure_terminal_reason: str | None = None  # NON_RETRYABLE / RETRY_EXHAUSTED
    # —— 保留与淘汰披露（追加字段，带默认值）——
    retention: RetentionInfo | None = None        # 保留口径（上限；None 表示不设上限）
    evicted_run_count: int = 0                      # 该管道累计被淘汰的运行数


@dataclass(frozen=True)
class RetentionInfo:
    """历史保留口径对外披露。"""

    max_runs: int | None            # 每个服务实例最多保留的不同运行数；None 表示不设上限
    current_runs: int               # 当前保留中的运行数
    total_evicted_runs: int         # 服务启动以来累计淘汰的运行数


@dataclass(frozen=True)
class RunLookup:
    """单运行查询的一致结果：在窗口内 / 已淘汰 / 从未见过，三态明确、不报错。"""

    status: LookupStatus
    snapshot: PipelineStatus | None = None   # status=PRESENT 时有效
    run_id: str | None = None                # 被淘汰/未知的运行 ID
    detail: str | None = None                # 淘汰说明等可读信息

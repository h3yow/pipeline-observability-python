"""领域异常。"""
from __future__ import annotations

from .models import RejectReason


class EventRejectedError(Exception):
    """事件无法被接受，携带可区分的拒绝原因。"""

    def __init__(self, reason: RejectReason, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")

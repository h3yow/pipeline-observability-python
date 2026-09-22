"""时间源：便于测试中注入固定/可控时钟。"""
from __future__ import annotations

import time
from typing import Callable, Protocol


class Clock(Protocol):
    def __call__(self) -> float: ...


def system_clock() -> float:
    return time.time()


class ManualClock:
    """测试用可控时钟。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += seconds
        return self.now

"""可替换的时钟抽象。

调度器只依赖 Clock 协议，不直接调用 time/ datetime，
这样测试可以使用完全确定的虚拟时钟，无需真实等待即可证明
最短间隔、Retry-After、租约恢复等时间相关行为。
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Protocol


class Clock(Protocol):
    def monotonic(self) -> float:
        """单调时间，用于间隔计算。"""

    def wall(self) -> datetime:
        """墙钟时间（UTC），用于记录与 HTTP 日期解析。"""

    def sleep(self, seconds: float) -> None:
        """等待指定秒数。虚拟时钟会直接推进时间而不是真等。"""

    def parse_http_date(self, value: str) -> float | None:
        """把 HTTP-date（RFC 7231）转成 monotonic 时间；无法解析返回 None。"""


class RealClock:
    """真实时钟：monotonic 与 wall 都来自系统。"""

    def monotonic(self) -> float:
        return time.monotonic()

    def wall(self) -> datetime:
        return datetime.now(timezone.utc)

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)

    def parse_http_date(self, value: str) -> float:
        parsed = parsedate_to_datetime(value)
        if parsed is None:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        target = parsed.timestamp()
        return self.monotonic() + (target - self.wall().timestamp())


class FakeClock:
    """确定性虚拟时钟。

    ``start_wall`` 是虚拟墙钟起点；虚拟 monotonic 从 0 开始，
    两者保持固定偏移。sleep 只推进虚拟时间，测试不产生真实等待。
    状态可序列化到检查点，崩溃后的新进程可以从同一虚拟时刻继续。
    """

    def __init__(self, start_wall: datetime, offset: float = 0.0):
        if start_wall.tzinfo is None:
            start_wall = start_wall.replace(tzinfo=timezone.utc)
        self._wall_origin = start_wall.timestamp()
        self._now = float(offset)
        # worker 线程与主线程都会访问虚拟时钟，用锁保护。
        self._lock = threading.Lock()
        # 在途并行工作的令牌集合。monotonic() 只返回基础时间 _now，
        # 不被在途工作提前；并行延迟的完成时刻在 begin_parallel 时
        # 一次性算出并交给调度器的 flight，因此两个并行 3 秒请求
        # 合计只流逝 3 秒而不是 6 秒。
        self._pending: set[object] = set()
        self._counter = 0

    def monotonic(self) -> float:
        with self._lock:
            return self._now

    def wall(self) -> datetime:
        with self._lock:
            return datetime.fromtimestamp(
                self._wall_origin + self._now, timezone.utc)

    def sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("不能回到过去")
        with self._lock:
            self._now += seconds

    def begin_parallel(self, seconds: float) -> tuple[object, float]:
        """登记一段并行延迟，返回 (令牌, 虚拟完成时刻)。"""
        if seconds < 0:
            raise ValueError("不能回到过去")
        with self._lock:
            self._counter += 1
            token = self._counter
            self._pending.add(token)
            return token, self._now + seconds

    def end_parallel(self, token: object) -> None:
        """并行工作结束：移出在途登记，不修改基础时间。"""
        with self._lock:
            self._pending.discard(token)

    def advance(self, seconds: float) -> None:
        self.sleep(seconds)

    def parse_http_date(self, value: str) -> float | None:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if parsed is None:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp() - self._wall_origin

    def to_state(self) -> dict:
        with self._lock:
            return {"wall_origin": self._wall_origin, "offset": self._now}

    @classmethod
    def from_state(cls, state: dict) -> "FakeClock":
        return cls(datetime.fromtimestamp(state["wall_origin"], timezone.utc),
                   state["offset"])

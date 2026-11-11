"""决策记录。

调度器每做出一次"允许 / 延迟 / 拒绝"决定都追加一条 JSON Lines 记录，
含虚拟或真实时间、origin、URL、决定类型与原因。
记录只追加不改写，崩溃前已落盘的原因不会丢失。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

# 决定类型
ALLOW = "allow"
DELAY = "delay"
REJECT = "reject"


@dataclass(frozen=True)
class Decision:
    seq: int
    monotonic: float
    wall: str
    round_id: str
    origin: str
    url: str
    verdict: str
    reason: str
    until: float | None = None
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = {
            "seq": self.seq,
            "monotonic": round(self.monotonic, 6),
            "wall": self.wall,
            "round": self.round_id,
            "origin": self.origin,
            "url": self.url,
            "verdict": self.verdict,
            "reason": self.reason,
        }
        if self.until is not None:
            data["until"] = round(self.until, 6)
        if self.detail is not None:
            data["detail"] = self.detail
        return data


class DecisionLog:
    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._seq = 0
        if self.path.exists():
            # 恢复时延续序号，保证跨进程全局递增。
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    self._seq = max(self._seq, json.loads(line)["seq"])

    def record(self, round_id: str, origin: str, url: str, verdict: str,
               reason: str, monotonic: float, wall: datetime,
               until: float | None = None, detail: str | None = None) -> Decision:
        self._seq += 1
        decision = Decision(self._seq, monotonic,
                            wall.astimezone().isoformat() if wall.tzinfo else wall.isoformat(),
                            round_id, origin, url, verdict, reason, until, detail)
        # 行缓冲追加；每条记录独立成行，进程被 kill 也至多丢最后一条。
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(decision.to_dict(), ensure_ascii=False) + "\n")
        return decision

    def read_all(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        return [json.loads(line) for line in
                self.path.read_text(encoding="utf-8").splitlines() if line.strip()]

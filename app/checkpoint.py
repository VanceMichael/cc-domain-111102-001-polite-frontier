"""检查点与跨进程协调。

- checkpoint.json 原子写入（临时文件 + os.replace），崩溃不会产生半截文件；
- lock 文件用 fcntl 排他锁，同一 runtime 目录只允许一个调度进程；
- 任务领取时状态立即变为 claimed 并落盘；恢复时把残留 claimed 任务
  统一退回 pending，保证"进程退出后任务不会永久卡在执行中"。
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
from pathlib import Path
from typing import Any

CHECKPOINT_NAME = "checkpoint.json"
LOCK_NAME = "scheduler.lock"

# 任务状态
PENDING = "pending"
CLAIMED = "claimed"
DONE = "done"
FAILED = "failed"
REJECTED = "rejected"
TERMINAL_STATES = frozenset({DONE, FAILED, REJECTED})


class LockBusy(RuntimeError):
    pass


class SchedulerLock:
    """跨独立进程的排他锁（仅 POSIX，本项目运行环境为 Linux）。"""

    def __init__(self, runtime_dir: str | os.PathLike):
        self.path = Path(runtime_dir) / LOCK_NAME
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh: object = None

    def acquire(self) -> None:
        self._fh = self.path.open("a+")
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._fh.close()
            self._fh = None
            raise LockBusy(f"另一个调度进程正占用 {self.path}") from exc
        self._fh.write(f"pid={os.getpid()}\n")
        self._fh.flush()

    def release(self) -> None:
        if self._fh is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "SchedulerLock":
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


class CheckpointStore:
    SCHEMA_VERSION = 1

    def __init__(self, runtime_dir: str | os.PathLike):
        self.dir = Path(runtime_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / CHECKPOINT_NAME

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> dict[str, Any]:
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if data.get("version") != self.SCHEMA_VERSION:
            raise ValueError("检查点版本不受支持")
        return data

    def save(self, state: dict[str, Any]) -> None:
        payload = {"version": self.SCHEMA_VERSION, **state}
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        os.replace(tmp, self.path)  # 同目录原子改名

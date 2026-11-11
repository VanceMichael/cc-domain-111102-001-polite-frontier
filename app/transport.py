"""传输层抽象。

调度器只依赖 Transport 协议，测试时用 ReplayTransport 重放 fixtures，
生产环境可替换成 HttpTransport。传输层不做任何调度、重试或跳转跟随，
这些策略全部由 scheduler 决定，保证每条请求都有对应的决策记录。
"""

from __future__ import annotations

import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Protocol

from app.clock import Clock
from app.sources import origin_of
from app.wire import Replay


class TransportError(RuntimeError):
    """传输层失败（连接被拒、DNS 失败等），区别于收到了 HTTP 响应。"""


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict[str, str]
    body: bytes
    url: str  # 实际响应的 URL（传输层不跟随跳转，等于请求 URL）
    completes_at: float | None = None  # 虚拟时钟上的完成时刻（重放延迟用）


class Transport(Protocol):
    def get(self, url: str, headers: dict[str, str]) -> Response: ...


class ReplayTransport:
    """基于 app.wire.Replay 的重放传输。

    ``latency`` 是每次请求消耗的虚拟秒数（由注入的时钟 sleep 推进），
    为 0 时立即返回，便于确定性测试；并发上限行为通过 latency > 0 演示。

    重放键兼容两种写法：fixtures 的原始 URL，以及调度器内部规范化
    （补全默认端口）后的 URL；查询参数始终保留，不参与折叠。
    """

    def __init__(self, replay: Replay, clock: Clock, latency: float = 0.0):
        self._replay = replay
        self._clock = clock
        self._latency = latency
        self._aliases = self._build_aliases(replay.routes)
        # Replay 的响应游标（positions）不是线程安全的；多个 worker
        # 线程可能同时读取，用锁把"取游标 + 推进"保护成原子操作。
        self._lock = threading.Lock()

    @staticmethod
    def _build_aliases(routes: dict) -> dict[str, str]:
        from urllib.parse import urlsplit, urlunsplit
        aliases: dict[str, str] = {}
        for key in routes:
            aliases[key] = key
            parts = urlsplit(key)
            try:
                hostport = origin_of(key).split("://", 1)[1]
            except ValueError:
                continue
            canonical = urlunsplit((parts.scheme, hostport, parts.path,
                                    parts.query, parts.fragment))
            aliases.setdefault(canonical, key)
        return aliases

    @property
    def requests(self) -> list[dict]:
        return self._replay.requests

    def get(self, url: str, headers: dict[str, str]) -> Response:
        # 延迟的虚拟时钟记账由调度器在派发时统一完成（并行窗口），
        # 这里只保留 latency 字段供调度器读取，传输本身保持无策略。
        key = self._aliases.get(url, url)
        with self._lock:
            reply = self._replay.get(key, headers)
        return Response(reply.status, dict(reply.headers), reply.body, url)


class HttpTransport:
    """真实 HTTP 传输（网络适配时使用；示例全程不访问真实网络）。

    不跟随重定向、不重试：3xx 原样返回给调度器做跨域策略判断。
    """

    def __init__(self, clock: Clock, timeout: float = 20.0):
        self._clock = clock
        self._timeout = timeout

    def get(self, url: str, headers: dict[str, str]) -> Response:
        request = urllib.request.Request(url, headers=headers, method="GET")

        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, hdrs, newurl):
                return None

        opener = urllib.request.build_opener(_NoRedirect)
        try:
            with opener.open(request, timeout=self._timeout) as resp:
                return Response(resp.status, dict(resp.headers), resp.read(), url)
        except urllib.error.HTTPError as exc:
            # HTTPError 也是一次合法响应（含 429/503/3xx 的头）。
            return Response(exc.code, dict(exc.headers or {}), exc.read() or b"", url)
        except urllib.error.URLError as exc:
            raise TransportError(str(exc.reason)) from exc

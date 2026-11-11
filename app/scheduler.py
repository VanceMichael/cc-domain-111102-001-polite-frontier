"""多站点采集调度器。

设计要点（对应需求逐条落地）：

* 每个 origin 独立维护并发计数、最短请求间隔、退避截止时间，
  限流退避（429/503 的 Retry-After）只影响对应 origin；
* 每个 origin 拥有独立线程池，大小等于该 origin 的并发上限，
  站点之间的并发额度互不抢占；
* robots.txt 按 User-Agent 解释 Allow/Disallow/Crawl-delay，
  显式拒绝的 URL 直接 reject；robots 获取失败时 origin 暂停（fail-closed），
  绝不默认放行；
* Retry-After 同时支持 delta-seconds 和 HTTP-date（解析委托给时钟）；
* 跨域重定向把目标 URL 重新入队，由目标 origin 的 robots/配额重新裁决，
  原站点的配额不能"带过去"；
* 同一轮内按完整 URL 去重（查询参数不同视为不同 URL，不折叠）；
* 公平性：每个调度节拍每个 origin 最多放行一个请求，并轮换 origin 顺序，
  持续有任务的站点无法饿死其他站点；
* 领取（claim）立即写检查点；恢复时残留 claimed 任务退回 pending，
  不会永久卡在执行中；
* 时钟与传输全部通过构造函数注入，可完全替换为虚拟实现。

线程模型：主线程独占所有状态变更与决策日志；worker 线程只执行传输 I/O，
通过 future 把 Response 或异常交回主线程处理，因此不需要给业务状态加锁。
"""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urljoin

from app.checkpoint import (
    CLAIMED, DONE, FAILED, PENDING, REJECTED, TERMINAL_STATES, CheckpointStore,
)
from app.clock import Clock
from app.journal import ALLOW, DELAY, REJECT, DecisionLog
from app.robots import RobotsRules, parse_robots
from app.sources import Source, origin_of
from app.transport import Response, Transport, TransportError

USER_AGENT = "DisclosureBot"

# origin 级 robots 状态
ROBOTS_PENDING = "pending"      # 尚未成功获取
ROBOTS_READY = "ready"          # 已获取并解释
ROBOTS_PAUSED = "paused"        # 获取暂时失败，等待退避后重试
ROBOTS_DISABLED = "disabled"    # 多次失败，放弃本 origin（fail-closed）

# 决策原因（全部稳定字符串，供测试与 README 观察）
R_ROBOTS_DISALLOW = "robots-disallow"
R_ROBOTS_UNAVAILABLE = "robots-unavailable"
R_ORIGIN_DISABLED = "origin-disabled"
R_CONCURRENCY_FULL = "concurrency-limit"
R_MIN_INTERVAL = "min-interval"
R_CRAWL_DELAY = "crawl-delay"
R_BACKOFF = "origin-backoff"
R_DUPLICATE = "duplicate-url"
R_DISPATCH = "dispatch"
R_ROBOTS_FETCH = "robots-fetch"
R_ROBOTS_EMPTY = "robots-empty-allows-all"
R_RETRY_AFTER = "retry-after"
R_ATTEMPTS_EXHAUSTED = "attempts-exhausted"
R_REDIRECT = "redirect"
R_HTTP_ERROR = "http-error"
R_TRANSPORT_ERROR = "transport-error"
R_LEASE_RECOVERED = "lease-recovered"
R_CANCELLED = "cancelled"

REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
RATE_LIMIT_STATUSES = frozenset({429, 503})
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_ROBOTS_ATTEMPTS = 5
DEFAULT_BACKOFF = 5.0

# worker 返回的崩溃信号：crash_hook 触发时不在线程内退出进程。
class _CrashSignal:
    def __init__(self, url: str):
        self.url = url


@dataclass
class Task:
    url: str
    origin: str
    state: str = PENDING
    attempts: int = 0
    redirected_from: str | None = None

    def to_state(self) -> dict[str, Any]:
        data = {"url": self.url, "state": self.state, "attempts": self.attempts}
        if self.redirected_from is not None:
            data["redirected_from"] = self.redirected_from
        return data

    @classmethod
    def from_state(cls, data: dict[str, Any]) -> "Task":
        return cls(data["url"], origin_of(data["url"]), data["state"],
                   data.get("attempts", 0), data.get("redirected_from"))


@dataclass
class _Flight:
    """一个已派发、等待 worker 返回的请求。"""
    kind: str               # "robots" 或 "task"
    origin: str
    task: Task | None
    future: object
    started_at: float
    completes_at: float     # 虚拟时钟上的预计完成时刻（无延迟则等于 started_at）


@dataclass
class OriginRuntime:
    config: Source
    robots_state: str = ROBOTS_PENDING
    robots_attempts: int = 0
    robots_until: float | None = None
    rules: RobotsRules | None = None
    in_flight: int = 0
    last_request_at: float | None = None
    backoff_until: float | None = None
    tasks: list[Task] = field(default_factory=list)
    pool: ThreadPoolExecutor | None = None
    flights: list[_Flight] = field(default_factory=list)

    def effective_interval(self) -> float:
        crawl = self.rules.crawl_delay if self.rules is not None else None
        if crawl is None:
            return self.config.min_interval
        return max(self.config.min_interval, crawl)

    def shutdown(self) -> None:
        if self.pool is not None:
            self.pool.shutdown(wait=False, cancel_futures=True)
            self.pool = None


class Scheduler:
    def __init__(self, sources: list[Source], transport: Transport, clock: Clock,
                 store: CheckpointStore, journal: DecisionLog,
                 max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 robots_attempts: int = DEFAULT_ROBOTS_ATTEMPTS,
                 default_backoff: float = DEFAULT_BACKOFF,
                 crash_hook: Callable[[str], None] | None = None,
                 round_id: str | None = None):
        self.sources = sources
        self.transport = transport
        self.clock = clock
        self.store = store
        self.journal = journal
        self.max_attempts = max_attempts
        self.robots_attempts_limit = robots_attempts
        self.default_backoff = default_backoff
        # crash_hook(url)：在 claim 落盘之后、真正发请求之前调用，
        # 测试用它模拟"任务被领取后进程退出"。
        self.crash_hook = crash_hook
        self.round_id = round_id or f"round-{uuid.uuid4().hex[:12]}"
        self.origins: dict[str, OriginRuntime] = {
            s.origin: OriginRuntime(config=s) for s in sources
        }
        self.seen: set[str] = set()
        self._delay_notes: set[tuple] = set()
        self.cancelled = False
        self._recover_or_init()

    # ------------------------------------------------------------------ #
    # 初始化与恢复
    # ------------------------------------------------------------------ #
    def _recover_or_init(self) -> None:
        if not self.store.exists():
            for source in self.sources:
                rt = self.origins[source.origin]
                for url in source.urls:
                    rt.tasks.append(Task(url, source.origin))
                    self.seen.add(url)
            self._persist()
            return

        data = self.store.load()
        if data.get("round_id"):
            # 恢复沿用原轮次：同 URL 在整轮内只采一次的语义跨进程保持。
            self.round_id = data["round_id"]
        saved = {o["origin"]: o for o in data["origins"]}
        now = self.clock.monotonic()
        for origin, rt in self.origins.items():
            snap = saved.get(origin)
            if snap is None:
                for url in rt.config.urls:
                    rt.tasks.append(Task(url, origin))
                    self.seen.add(url)
                continue
            rt.robots_state = snap["robots_state"]
            rt.robots_attempts = snap.get("robots_attempts", 0)
            rt.robots_until = snap.get("robots_until")
            rt.last_request_at = snap.get("last_request_at")
            rt.backoff_until = snap.get("backoff_until")
            if snap.get("robots_text") is not None:
                rt.rules = parse_robots(snap["robots_text"], USER_AGENT)
            for task_data in snap["tasks"]:
                task = Task.from_state(task_data)
                if task.state == CLAIMED:
                    # 关键恢复路径：上个进程领取后退出，退回队列重新调度，
                    # 并留下原因记录，任务不会永久卡在 claimed。
                    task.state = PENDING
                    task.attempts = task_data.get("attempts", 0)
                    self.journal.record(
                        self.round_id, origin, task.url, ALLOW,
                        R_LEASE_RECOVERED, now, self.clock.wall(),
                        detail="残留 claimed 状态已退回 pending")
                rt.tasks.append(task)
                self.seen.add(task.url)
        self._persist()

    def _persist(self) -> None:
        self.store.save({
            "round_id": self.round_id,
            "clock": self.clock_state(),
            "origins": [self._origin_snapshot(rt) for rt in self.origins.values()],
        })

    def clock_state(self) -> dict[str, Any] | None:
        to_state = getattr(self.clock, "to_state", None)
        return to_state() if callable(to_state) else None

    def _origin_snapshot(self, rt: OriginRuntime) -> dict[str, Any]:
        robots_text = None
        if rt.robots_state == ROBOTS_READY and rt.rules is not None:
            # 重新序列化规则文本：保留原始模式即可重建等价规则集。
            lines = [f"User-agent: {USER_AGENT}"]
            if rt.rules.crawl_delay is not None:
                lines.append(f"Crawl-delay: {rt.rules.crawl_delay}")
            for rule in rt.rules.rules:
                lines.append(f"{'Allow' if rule.allowed else 'Disallow'}: {rule.pattern}")
            robots_text = "\n".join(lines) + "\n"
        return {
            "origin": rt.config.origin,
            "robots_state": rt.robots_state,
            "robots_attempts": rt.robots_attempts,
            "robots_until": rt.robots_until,
            "robots_text": robots_text,
            "in_flight": rt.in_flight,
            "last_request_at": rt.last_request_at,
            "backoff_until": rt.backoff_until,
            "tasks": [t.to_state() for t in rt.tasks],
        }

    def _pool(self, rt: OriginRuntime) -> ThreadPoolExecutor:
        if rt.pool is None:
            # 每个 origin 独立池：本 origin 占满也不会借走别的 origin 的线程。
            label = rt.config.origin.split("://", 1)[-1].split(":", 1)[0]
            rt.pool = ThreadPoolExecutor(
                max_workers=rt.config.concurrency,
                thread_name_prefix=f"crawl-{label}")
        return rt.pool

    # ------------------------------------------------------------------ #
    # 主循环
    # ------------------------------------------------------------------ #
    def run(self, is_cancelled: Callable[[], bool] | None = None,
            idle_callback: Callable[[float], None] | None = None) -> None:
        """运行到本轮全部任务到达终态（或被取消）。"""
        try:
            order = list(self.origins.values())
            while True:
                if is_cancelled is not None and is_cancelled():
                    self.cancelled = True
                    self._note_cancellations()
                    self._persist()
                    return
                # 先回收上一轮 sleep 后已完成的 worker（首次为空操作）。
                self._drain_finished_flights()
                now = self.clock.monotonic()
                for rt in order:
                    self._step_origin(rt, now)
                # 轮换起点，保证轮询公平，没有 origin 能长期占据第一。
                order.append(order.pop(0))

                if self._all_terminal():
                    self._persist()
                    return
                # flight 在派发时已记录 completes_at，即使 worker 瞬时
                # 完成也据此算出正等待量，并行延迟不会塌缩为 0。
                wait = self._wait_seconds()
                if wait is None:
                    self._persist()
                    return
                if idle_callback is not None:
                    idle_callback(wait)
                self.clock.sleep(wait)
        finally:
            for rt in self.origins.values():
                rt.shutdown()

    def _drain_finished_flights(self) -> None:
        """在主线程统一处理已完成的 worker 输出（独占状态变更）。

        一个 flight 只有同时满足"虚拟完成时刻已到"和"worker 已返回"
        才会被回收：重放模式下 worker 瞬时返回，但虚拟延迟未到点时
        它仍占用 origin 的并发槽，从而让并发上限与并行时序真实生效。
        """
        now = self.clock.monotonic()
        for rt in self.origins.values():
            finished = [f for f in rt.flights
                        if f.future.done() and now >= f.completes_at]
            for flight in finished:
                rt.flights.remove(flight)
                rt.in_flight = max(0, rt.in_flight - 1)
                result = flight.future.result()
                if isinstance(result, _CrashSignal):
                    # claim 已落盘；在主线程退出，模拟进程被杀死。
                    raise SystemExit(0)
                if flight.kind == "robots":
                    if isinstance(result, TransportError):
                        self._finish_robots_error(rt, now, str(result))
                    else:
                        self._handle_robots_response(rt, result, now)
                else:
                    if isinstance(result, TransportError):
                        self._handle_transport_error(rt, flight.task, now,
                                                     str(result))
                    else:
                        self._handle_response(rt, flight.task, result, now)

    def _all_terminal(self) -> bool:
        for rt in self.origins.values():
            if rt.flights or rt.in_flight:
                return False
            if any(t.state not in TERMINAL_STATES for t in rt.tasks):
                return False
            if rt.robots_state in {ROBOTS_PENDING, ROBOTS_PAUSED}:
                return False
        return True

    def _wait_seconds(self) -> float | None:
        """计算下一次唤醒需要的虚拟/真实秒数。

        等待依据只来自主循环自己持有的 flight 记录（派发时即确定
        completes_at），不依赖 worker 是否已结束：worker 瞬时完成也
        不会让等待量塌缩为 0。有在途请求时睡到最早完成时刻与最近
        计时器的较小值；无在途请求时直接睡到最早闸门开放时刻。
        """
        now = self.clock.monotonic()
        timer = self._next_timer(now)
        deadlines = [f.completes_at for rt in self.origins.values()
                     for f in rt.flights]
        if not deadlines:
            if timer is None:
                return None
            return max(0.0, timer - now)
        # 有在途 flight 时，无论业务计时器算到什么时刻，都必须先等过
        # 最早一个 flight 的虚拟完成时刻，否则会零睡眠空转并重复派发。
        earliest_flight = min(deadlines)
        lower_bound = max(now, earliest_flight)
        if timer is None:
            return lower_bound - now
        return max(lower_bound, timer) - now

    def _next_timer(self, now: float) -> float | None:
        # 对每个仍有未决工作的 origin 求"最早可能放行时刻"，
        # 同时覆盖 robots 暂停、限流退避和最短间隔三类闸门，
        # 否则只剩 backoff 拦截时会算出 now，造成零睡眠空转。
        candidates: list[float] = []
        for rt in self.origins.values():
            origin_ready_at = now
            if rt.robots_state in {ROBOTS_PENDING, ROBOTS_PAUSED}:
                origin_ready_at = max(
                    origin_ready_at,
                    rt.robots_until if rt.robots_until is not None else now)
            if rt.backoff_until is not None:
                origin_ready_at = max(origin_ready_at, rt.backoff_until)
            if rt.robots_state in {ROBOTS_PENDING, ROBOTS_PAUSED}:
                candidates.append(origin_ready_at)
            elif any(t.state == PENDING for t in rt.tasks):
                if rt.in_flight >= rt.config.concurrency:
                    candidates.append(origin_ready_at)
                elif rt.last_request_at is not None:
                    deadline = rt.last_request_at + rt.effective_interval()
                    candidates.append(max(origin_ready_at, deadline))
                else:
                    candidates.append(origin_ready_at)
        return min(candidates) if candidates else None

    # ------------------------------------------------------------------ #
    # 单个 origin 的一个调度节拍（最多放行一个请求）
    # ------------------------------------------------------------------ #
    def _step_origin(self, rt: OriginRuntime, tick_now: float) -> None:
        # 一个节拍内持续派发，直到该 origin 被某个闸门挡住或没有待办：
        # 这样并发额度可以被真实填满（在途请求在 worker 线程中执行）。
        # 跨 origin 的公平性由主循环的 origin 轮换保证，不依赖单拍限制。
        # 闸门判断固定使用节拍开始时刻，不被本节拍登记的在途窗口提前。
        while True:
            now = tick_now
            # 1) robots 闸门
            if rt.robots_state == ROBOTS_DISABLED:
                for task in rt.tasks:
                    if task.state == PENDING:
                        task.state = REJECTED
                        self._decide(rt, task.url, REJECT,
                                     R_ORIGIN_DISABLED, now=now)
                        self._persist()
                return
            if rt.robots_state in {ROBOTS_PENDING, ROBOTS_PAUSED}:
                if rt.robots_until is not None and now < rt.robots_until:
                    return
                self._maybe_dispatch_robots(rt, now)
                # robots 请求本身占一个在途槽，等它返回后再继续业务派发。
                return
            # robots READY
            task = next((t for t in rt.tasks if t.state == PENDING), None)
            if task is None:
                return

            # 2) robots 显式拒绝：拒绝当前任务后继续尝试后续任务。
            assert rt.rules is not None
            if not rt.rules.allows(task.url):
                task.state = REJECTED
                self._decide(rt, task.url, REJECT, R_ROBOTS_DISALLOW, now=now)
                self._persist()
                continue

            # 3) 并发闸门（只影响本 origin）：槽位满，本节拍结束。
            if rt.in_flight >= rt.config.concurrency:
                self._decide(rt, task.url, DELAY, R_CONCURRENCY_FULL, now=now)
                return

            # 4) 时间闸门：限流退避 / 最短间隔 / Crawl-delay，各自保留原因。
            if rt.backoff_until is not None and now < rt.backoff_until:
                self._decide(rt, task.url, DELAY, R_BACKOFF, now=now,
                             until=rt.backoff_until)
                return
            if rt.last_request_at is not None:
                interval_deadline = rt.last_request_at + rt.effective_interval()
                if now < interval_deadline:
                    reason = (R_CRAWL_DELAY if rt.rules.crawl_delay is not None
                              and rt.effective_interval() == rt.rules.crawl_delay
                              else R_MIN_INTERVAL)
                    self._decide(rt, task.url, DELAY, reason, now=now,
                                 until=interval_deadline)
                    return
            self._dispatch_task(rt, task, now)

    # ------------------------------------------------------------------ #
    # 请求派发：claim + 落盘 + 提交 worker（worker 不触碰共享状态）
    # ------------------------------------------------------------------ #
    def _submit(self, rt: OriginRuntime, url: str, kind: str,
                task: Task | None, now: float) -> None:
        # 若传输层声明了重放延迟，先在虚拟时钟登记并行窗口，
        # 完成时刻在派发时即可确定，供主循环精确等待。
        latency = getattr(self.transport, "_latency", 0.0)
        if latency:
            token, completes_at = self.clock.begin_parallel(latency)
        else:
            token, completes_at = None, now

        def work() -> Any:
            if self.crash_hook is not None:
                try:
                    self.crash_hook(url)
                except BaseException as exc:  # 包括测试里的 SystemExit
                    if isinstance(exc, SystemExit):
                        return _CrashSignal(url)
                    raise
            try:
                return self.transport.get(url, {"User-Agent": USER_AGENT})
            except TransportError as exc:
                return exc
            finally:
                if token is not None:
                    self.clock.end_parallel(token)

        flight = _Flight(kind, rt.config.origin, task,
                         self._pool(rt).submit(work), now, completes_at)
        rt.flights.append(flight)
        rt.in_flight += 1
        rt.last_request_at = now

    def _maybe_dispatch_robots(self, rt: OriginRuntime, now: float) -> None:
        # 已有 robots 请求在途（虚拟延迟未到点）时不得重复派发。
        if any(f.kind == "robots" for f in rt.flights):
            return
        if rt.in_flight >= rt.config.concurrency:
            self._decide(rt, rt.config.origin + "/robots.txt", DELAY,
                         R_CONCURRENCY_FULL, now=now)
            return
        if rt.backoff_until is not None and now < rt.backoff_until:
            self._decide(rt, rt.config.origin + "/robots.txt", DELAY,
                         R_BACKOFF, now=now, until=rt.backoff_until)
            return
        if rt.last_request_at is not None:
            deadline = rt.last_request_at + rt.config.min_interval
            if now < deadline:
                self._decide(rt, rt.config.origin + "/robots.txt", DELAY,
                             R_MIN_INTERVAL, now=now, until=deadline)
                return
        if rt.robots_attempts >= self.robots_attempts_limit:
            rt.robots_state = ROBOTS_DISABLED
            self._decide(rt, rt.config.origin + "/robots.txt", REJECT,
                         R_ORIGIN_DISABLED, now=now,
                         detail="robots 反复获取失败，origin 关闭（fail-closed）")
            self._persist()
            return

        url = rt.config.origin + "/robots.txt"
        rt.robots_attempts += 1
        self._decide(rt, url, ALLOW, R_ROBOTS_FETCH, now=now)
        self._persist()  # claim 即落盘
        self._submit(rt, url, "robots", None, now)

    def _dispatch_task(self, rt: OriginRuntime, task: Task, now: float) -> None:
        task.state = CLAIMED
        task.attempts += 1
        self._decide(rt, task.url, ALLOW, R_DISPATCH, now=now,
                     detail=f"第 {task.attempts} 次尝试"
                            + (f"，重定向自 {task.redirected_from}"
                               if task.redirected_from else ""))
        self._persist()  # claim 立即落盘，崩溃后可恢复
        self._submit(rt, task.url, "task", task, now)

    # ------------------------------------------------------------------ #
    # worker 返回后的响应处理（全部在主线程）
    # ------------------------------------------------------------------ #
    def _handle_robots_response(self, rt: OriginRuntime, response: Response,
                                now: float) -> None:
        if response.status == 200:
            text = response.body.decode("utf-8", errors="replace")
            rt.rules = parse_robots(text, USER_AGENT)
            rt.robots_state = ROBOTS_READY
            rt.robots_until = None
            reason = R_ROBOTS_EMPTY if not rt.rules.rules else "robots-ready"
            self._decide(rt, response.url, ALLOW, reason, now=now,
                         detail=(f"Crawl-delay={rt.rules.crawl_delay}"
                                 f"，规则 {len(rt.rules.rules)} 条"))
            self._persist()
            return
        if response.status in RATE_LIMIT_STATUSES:
            until = self._retry_after(response, now)
            rt.robots_state = ROBOTS_PAUSED
            rt.robots_until = until
            rt.backoff_until = until
            self._decide(rt, response.url, DELAY, R_RETRY_AFTER, now=now,
                         until=until,
                         detail=f"robots 收到 {response.status}，origin 暂停")
        else:
            # 404 按 RFC 视为无 robots（全允许）；其余状态按暂时失败暂停。
            if response.status == 404:
                rt.rules = RobotsRules(USER_AGENT)
                rt.robots_state = ROBOTS_READY
                rt.robots_until = None
                self._decide(rt, response.url, ALLOW, R_ROBOTS_EMPTY, now=now,
                              detail="robots 404，按无规则处理")
            else:
                rt.robots_state = ROBOTS_PAUSED
                until = now + self.default_backoff
                rt.robots_until = until
                rt.backoff_until = until
                self._decide(rt, response.url, DELAY, R_ROBOTS_UNAVAILABLE,
                             now=now, until=until,
                             detail=f"robots 状态 {response.status}，暂停而非放行")
        self._persist()

    def _finish_robots_error(self, rt: OriginRuntime, now: float, detail: str) -> None:
        until = now + self.default_backoff
        rt.robots_state = ROBOTS_PAUSED
        rt.robots_until = until
        rt.backoff_until = until
        self._decide(rt, rt.config.origin + "/robots.txt", DELAY,
                     R_TRANSPORT_ERROR, now=now, until=until, detail=detail)
        self._persist()

    def _handle_transport_error(self, rt: OriginRuntime, task: Task,
                                now: float, detail: str) -> None:
        if task.attempts >= self.max_attempts:
            task.state = FAILED
            self._decide(rt, task.url, REJECT, R_ATTEMPTS_EXHAUSTED, now=now,
                         detail=f"传输错误：{detail}")
        else:
            task.state = PENDING
            until = now + self.default_backoff
            rt.backoff_until = until
            self._decide(rt, task.url, DELAY, R_TRANSPORT_ERROR, now=now,
                         until=until, detail=detail)
        self._persist()

    def _handle_response(self, rt: OriginRuntime, task: Task,
                         response: Response, now: float) -> None:
        status = response.status
        if 200 <= status < 300:
            task.state = DONE
            self._decide(rt, task.url, ALLOW, f"http-{status}", now=now,
                         detail=f"{len(response.body)} 字节")
            self._persist()
            return

        if status in REDIRECT_STATUSES:
            self._handle_redirect(rt, task, response, now)
            return

        if status in RATE_LIMIT_STATUSES:
            until = self._retry_after(response, now)
            rt.backoff_until = until  # 退避只影响对应 origin
            if task.attempts >= self.max_attempts:
                task.state = FAILED
                self._decide(rt, task.url, REJECT, R_ATTEMPTS_EXHAUSTED, now=now,
                             until=until, detail=f"{status} 重试次数耗尽")
            else:
                task.state = PENDING
                self._decide(rt, task.url, DELAY, R_RETRY_AFTER, now=now,
                             until=until,
                             detail=f"{status} 要求等待至 {until:.0f}")
            self._persist()
            return

        # 其余 HTTP 状态（4xx/5xx）：不做静默重试，记录后失败。
        task.state = FAILED
        self._decide(rt, task.url, REJECT, R_HTTP_ERROR, now=now,
                     detail=f"HTTP {status}")
        self._persist()

    def _handle_redirect(self, rt: OriginRuntime, task: Task,
                         response: Response, now: float) -> None:
        location = response.headers.get("Location") or response.headers.get("location")
        if not location:
            task.state = FAILED
            self._decide(rt, task.url, REJECT, R_HTTP_ERROR, now=now,
                         detail=f"{response.status} 缺少 Location")
            self._persist()
            return
        target = urljoin(task.url, location)
        try:
            target_origin = origin_of(target)
        except ValueError as exc:
            task.state = FAILED
            self._decide(rt, task.url, REJECT, R_HTTP_ERROR, now=now,
                         detail=f"重定向地址非法：{exc}")
            self._persist()
            return

        if target_origin not in self.origins:
            # 跨域到未登记站点：没有配额与 robots 上下文，拒绝而不是借道发出。
            task.state = REJECTED
            self._decide(rt, task.url, REJECT, "redirect-unknown-origin",
                         now=now, detail=f"目标 {target_origin} 不在来源列表")
            self._persist()
            return

        # 原任务结束；目标作为新任务重新入队，接受目标 origin 的完整策略约束。
        task.state = DONE
        self._decide(rt, task.url, ALLOW, R_REDIRECT, now=now,
                     detail=f"{response.status} -> {target}")
        target_rt = self.origins[target_origin]
        if target in self.seen:
            # 本轮已采过同一完整 URL（含查询串）：直接拒绝且不重复入队。
            self._decide(target_rt, target, REJECT, R_DUPLICATE, now=now,
                         detail="重定向目标在本轮已采集，不重复请求")
            self._persist()
            return
        self.seen.add(target)
        # 不在这里做 robots 预判：目标任务进入目标 origin 的正常调度闸门，
        # 由其 robots / 并发 / 间隔 / 退避策略重新裁决，原站点配额不会带过来。
        target_rt.tasks.append(Task(target, target_origin, PENDING, 0,
                                    redirected_from=task.url))
        self._persist()

    # ------------------------------------------------------------------ #
    # 工具方法
    # ------------------------------------------------------------------ #
    def _retry_after(self, response: Response, now: float) -> float:
        header = response.headers.get("Retry-After") or \
            response.headers.get("retry-after")
        if header is None:
            return now + self.default_backoff
        value = header.strip()
        if value.isdigit():
            return now + int(value)  # delta-seconds
        dated = self.clock.parse_http_date(value)  # HTTP-date
        if dated is None or dated < now:
            return now + self.default_backoff
        return dated

    def _decide(self, rt: OriginRuntime, url: str, verdict: str, reason: str,
                now: float | None = None, until: float | None = None,
                detail: str | None = None) -> None:
        moment = self.clock.monotonic() if now is None else now
        # 同一 URL 的重复 delay 原因每个节拍只记一次，避免空转刷屏；
        # 原因或截止时间变化（例如新的 Retry-After）会产生新记录。
        key = (url, reason, round(until or 0.0, 6))
        if verdict == DELAY and key in self._delay_notes:
            return
        if verdict == DELAY:
            self._delay_notes.add(key)
        else:
            self._delay_notes.discard(key)
        self.journal.record(self.round_id, rt.config.origin, url, verdict,
                            reason, moment, self.clock.wall(), until, detail)

    def _note_cancellations(self) -> None:
        now = self.clock.monotonic()
        for rt in self.origins.values():
            for task in rt.tasks:
                if task.state == PENDING:
                    self._decide(rt, task.url, REJECT, R_CANCELLED, now=now,
                                 detail="调度器已取消，不再发出请求")

    def pending_urls(self) -> list[str]:
        return [t.url for rt in self.origins.values() for t in rt.tasks
                if t.state == PENDING]

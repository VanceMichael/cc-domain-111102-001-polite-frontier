"""命令行入口：运行、恢复与查看决策记录。

run     启动一轮采集（虚拟时钟重放，可被 --cancel-after 取消）
resume  从 runtime 目录的检查点恢复，继续同一轮
show    打印决策 JSONL，支持按 verdict / origin 过滤
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# -I 隔离模式下 cwd 不在 sys.path；显式加入受信任的项目根。
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.checkpoint import CheckpointStore, SchedulerLock, LockBusy
from app.clock import FakeClock
from app.journal import DecisionLog
from app.scheduler import Scheduler
from app.sources import read_sources
from app.transport import ReplayTransport
from app.wire import Replay

CLOCK_START = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)


def build_scheduler(sources_path: str, responses_path: str, runtime_dir: str,
                    crash_after_claim: str | None = None,
                    latency: float = 0.0) -> Scheduler:
    """从检查点恢复或新建一个完整调度器（虚拟时钟 + 重放传输）。"""
    store = CheckpointStore(runtime_dir)
    journal = DecisionLog(Path(runtime_dir) / "decisions.jsonl")
    sources = read_sources(sources_path)

    if store.exists():
        state = store.load()
        clock = FakeClock.from_state(state["clock"])
    else:
        clock = FakeClock(CLOCK_START)

    replay = Replay(responses_path)
    transport = ReplayTransport(replay, clock, latency=latency)

    hook = None
    if crash_after_claim:
        def hook(url: str) -> None:
            if url == crash_after_claim:
                # claim 已落盘、请求尚未发出：模拟进程被 kill -9。
                raise SystemExit(0)

    return Scheduler(sources, transport, clock, store, journal,
                     crash_hook=hook)


def cmd_run(args: argparse.Namespace) -> int:
    return _execute(args, fresh=True)


def cmd_resume(args: argparse.Namespace) -> int:
    return _execute(args, fresh=False)


def _execute(args: argparse.Namespace, fresh: bool) -> int:
    store = CheckpointStore(args.runtime)
    if fresh and store.exists() and not args.force:
        print(f"检查点已存在：{store.path}，如需覆盖请加 --force，"
              f"恢复请使用 resume", file=sys.stderr)
        return 2
    try:
        with SchedulerLock(args.runtime):
            scheduler = build_scheduler(
                args.sources, args.responses, args.runtime,
                crash_after_claim=args.crash_after_claim,
                latency=args.latency)
            cancelled = {"flag": False, "after": args.cancel_after}

            def is_cancelled() -> bool:
                if cancelled["after"] is None:
                    return False
                # 以决策记录中真实发出的派发数为准；取消在循环顶部生效，
                # 因此被取消后不会再有新的 claim，更不会发出新请求。
                dispatched = sum(
                    1 for d in scheduler.journal.read_all()
                    if d["verdict"] == "allow"
                    and d["reason"] in {"dispatch", "robots-fetch"})
                if dispatched >= cancelled["after"]:
                    cancelled["flag"] = True
                    return True
                return False

            scheduler.run(is_cancelled=is_cancelled)
            _print_summary(scheduler, cancelled["flag"])
            return 130 if cancelled["flag"] else 0
    except LockBusy as exc:
        print(str(exc), file=sys.stderr)
        return 3


def _print_summary(scheduler: Scheduler, cancelled: bool) -> None:
    print(f"轮次 {scheduler.round_id} 虚拟时间 "
          f"{scheduler.clock.wall().isoformat()}")
    for rt in scheduler.origins.values():
        counts: dict[str, int] = {}
        for task in rt.tasks:
            counts[task.state] = counts.get(task.state, 0) + 1
        print(f"  {rt.config.origin} robots={rt.robots_state} "
              f"在途={rt.in_flight} 任务={counts}")
    print("取消生效，未再发出新请求" if cancelled else "本轮结束")


def cmd_show(args: argparse.Namespace) -> int:
    path = Path(args.runtime) / "decisions.jsonl"
    if not path.exists():
        print(f"没有决策记录：{path}", file=sys.stderr)
        return 1
    for entry in DecisionLog(path).read_all():
        if args.verdict and entry["verdict"] != args.verdict:
            continue
        if args.origin and entry["origin"] != args.origin:
            continue
        print(json.dumps(entry, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="多站点披露采集调度器（重放模式）")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--sources", default="fixtures/sources.json")
        p.add_argument("--responses", default="fixtures/responses.json")
        p.add_argument("--runtime", default="runtime/demo")
        p.add_argument("--latency", type=float, default=0.0,
                       help="每次请求消耗的虚拟秒数（演示并发上限用）")
        p.add_argument("--cancel-after", type=int, default=None,
                       help="发出 N 个请求后取消")
        p.add_argument("--crash-after-claim", default=None,
                       help="领取指定 URL 后立即退出（模拟崩溃）")

    p_run = sub.add_parser("run", help="新建一轮采集")
    p_run.add_argument("--force", action="store_true")
    add_common(p_run)
    p_run.set_defaults(func=cmd_run)

    p_resume = sub.add_parser("resume", help="从检查点恢复")
    add_common(p_resume)
    p_resume.set_defaults(func=cmd_resume)

    p_show = sub.add_parser("show", help="查看决策记录")
    p_show.add_argument("--verdict", choices=["allow", "delay", "reject"])
    p_show.add_argument("--origin")
    p_show.add_argument("--runtime", default="runtime/demo")
    p_show.set_defaults(func=cmd_show)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

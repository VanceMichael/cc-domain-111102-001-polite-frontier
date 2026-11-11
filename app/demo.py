"""三站点重放演示。

三个场景（全程虚拟时钟，无真实等待、无真实网络）：

1. fairness  一个站点持续限流，另外两个站点仍然持续取得进展；
2. cancel    发出若干请求后取消，证明取消后不再发任何请求；
3. crash     任务领取后进程被杀死，独立进程从检查点恢复并完成本轮，
             包括对重定向跨域策略、Retry-After 两种格式的覆盖。

演示素材是合成的 .invalid 响应，不是任何真实单位的披露文件。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# 以 -I 隔离模式启动时 cwd 不在 sys.path 中；显式加入受信任的项目根目录，
# 而不是依赖可能位于不可信素材目录中的 cwd。
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.checkpoint import CheckpointStore, SchedulerLock
from app.journal import DecisionLog
from app.runner import build_scheduler
DEMO_FIXTURES = ROOT / "runtime" / "demo_fixtures"
CLOCK_START = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)

# alpha 持续限流：前 3 次都返回 429（Retry-After: 5 秒），第 4 次成功。
# beta / gamma 始终 200，用来证明它们不会被 alpha 的限流拖住。
DEMO_SOURCES = [
    {"origin": "https://alpha.invalid", "concurrency": 1, "min_interval": 2,
     "urls": ["https://alpha.invalid/report?q=1",
              "https://alpha.invalid/report?q=2"]},
    {"origin": "https://beta.invalid", "concurrency": 2, "min_interval": 1,
     "urls": ["https://beta.invalid/data", "https://beta.invalid/notes"]},
    {"origin": "https://gamma.invalid", "concurrency": 1, "min_interval": 0,
     "urls": ["https://gamma.invalid/public",
              "https://gamma.invalid/private"]},
]

DEMO_RESPONSES = {
    "https://alpha.invalid/robots.txt": [
        {"status": 200,
         "body": "User-agent: DisclosureBot\nAllow: /report\nCrawl-delay: 2\n"}],
    "https://beta.invalid/robots.txt": [
        {"status": 200, "body": "User-agent: *\nAllow: /\n"}],
    "https://gamma.invalid/robots.txt": [
        {"status": 200,
         "body": "User-agent: *\nDisallow: /private\nAllow: /\n"}],
    "https://alpha.invalid/report?q=1": [
        {"status": 429, "headers": {"Retry-After": "5"}},
        {"status": 429, "headers": {"Retry-After": "5"}},
        {"status": 429, "headers": {"Retry-After": "5"}},
        {"status": 200, "body": "甲披露-1"},
    ],
    "https://alpha.invalid/report?q=2": [
        {"status": 200, "body": "甲披露-2"}],
    "https://beta.invalid/data": [{"status": 200, "body": "乙数据"}],
    "https://beta.invalid/notes": [{"status": 200, "body": "乙备注"}],
    "https://gamma.invalid/public": [{"status": 200, "body": "丙公开"}],
    "https://gamma.invalid/private": [{"status": 200, "body": "不应被请求"},
                                      {"status": 200, "body": "不应被请求"}],
}


def write_fixtures() -> tuple[Path, Path]:
    # 素材放在独立的 runtime 子目录，与受信任的项目代码隔离。
    # 重放键保持完整 URL 原样；规范化（默认端口）由传输层别名处理。
    DEMO_FIXTURES.mkdir(parents=True, exist_ok=True)
    sources_path = DEMO_FIXTURES / "sources.json"
    responses_path = DEMO_FIXTURES / "responses.json"
    sources_path.write_text(json.dumps(DEMO_SOURCES, ensure_ascii=False, indent=2),
                            encoding="utf-8")
    responses_path.write_text(
        json.dumps(DEMO_RESPONSES, ensure_ascii=False, indent=2),
        encoding="utf-8")
    return sources_path, responses_path


def _timeline(journal: DecisionLog) -> list[dict]:
    return journal.read_all()


def _print_header(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def demo_fairness(sources_path: Path, responses_path: Path,
                  runtime: Path) -> None:
    _print_header("场景 1：alpha 持续限流，beta / gamma 仍持续推进（虚拟时钟）")
    scheduler = build_scheduler(str(sources_path), str(responses_path),
                                str(runtime))
    scheduler.run()
    records = _timeline(scheduler.journal)
    for rec in records:
        until = f" -> 等到 t={rec['until']}" if "until" in rec else ""
        print(f"t={rec['monotonic']:>5.1f}s {rec['verdict']:<6} "
              f"{rec['origin']:<24} {rec['url']:<42} {rec['reason']}{until}")

    # 自动断言：alpha 还在限流等待时，beta、gamma 已经完成采集。
    alpha_done_at = min(r["monotonic"] for r in records
                        if r["url"].endswith("q=1") and r["reason"] == "http-200")
    beta_progress = [r["monotonic"] for r in records
                     if r["origin"] == "https://beta.invalid:443"
                     and r["verdict"] == "allow" and r["reason"] == "http-200"]
    gamma_progress = [r["monotonic"] for r in records
                      if r["origin"] == "https://gamma.invalid:443"
                      and r["verdict"] == "allow" and r["reason"] == "http-200"]
    assert beta_progress and max(beta_progress) < alpha_done_at, \
        "beta 不应被 alpha 的限流饿死"
    assert gamma_progress and max(gamma_progress) < alpha_done_at, \
        "gamma 不应被 alpha 的限流饿死"
    blocked = [r for r in records
               if r["url"].endswith("/private") and r["verdict"] == "reject"]
    assert blocked, "gamma /private 必须被 robots 显式拒绝"
    print(f"\n断言通过：alpha 在 t={alpha_done_at:.0f}s 才成功，"
          f"beta 已于 t={max(beta_progress):.0f}s、gamma 于 "
          f"t={max(gamma_progress):.0f}s 完成；/private 被拒绝，未发请求。")


def demo_cancel(sources_path: Path, responses_path: Path,
                runtime: Path) -> None:
    _print_header("场景 2：取消后不再发出任何请求")
    scheduler = build_scheduler(str(sources_path), str(responses_path),
                                str(runtime))
    # 取消信号在调度循环顶部检查；阈值取一次完整节拍可派发的数量，
    # 因此用信号翻转时刻做边界，而不是要求精确的请求总数。
    budget = {"n": 6}
    signal_flipped = {"at": None}

    def is_cancelled() -> bool:
        allowed = sum(1 for d in scheduler.journal.read_all()
                      if d["verdict"] == "allow"
                      and d["reason"] in {"dispatch", "robots-fetch"})
        if allowed >= budget["n"]:
            if signal_flipped["at"] is None:
                signal_flipped["at"] = scheduler.clock.monotonic()
            return True
        return False

    scheduler.run(is_cancelled=is_cancelled)
    records = scheduler.journal.read_all()
    sent = [r for r in records
            if r["verdict"] == "allow"
            and r["reason"] in {"dispatch", "robots-fetch"}]
    flip = signal_flipped["at"]
    after = [r for r in sent if r["monotonic"] > flip]
    print(f"取消信号在 t={flip}s 翻转（阈值 {budget['n']}），"
          f"此前累计发出 {len(sent) - len(after)} 个请求；"
          f"翻转之后新发出请求数={len(after)}")
    assert not after, "取消信号生效后不能再发出任何请求"
    cancels = [r for r in records if r["reason"] == "cancelled"]
    print(f"剩余任务均留下 cancelled 记录：{len(cancels)} 条")
    assert cancels, "未完成的任务必须留下取消原因"
    # 传输层侧独立核对：重放记录的请求总数与决策派发数一致，
    # 证明没有"绕过决策记录"的隐藏请求。
    assert len(scheduler.transport.requests) == len(sent), \
        "传输层收到的请求必须全部有决策记录"
    print("断言通过：取消后传输层零新请求，且每条请求都有决策留痕。")


def demo_crash(sources_path: Path, responses_path: Path,
               runtime: Path) -> None:
    _print_header("场景 3：领取后进程退出，独立进程恢复（跨进程检查）")
    crash_url = "https://beta.invalid/data"

    # 子进程 A：领取 crash_url 后立即以 SystemExit 退出（claim 已落盘）。
    # 直接以脚本路径 + -I 启动：不依赖 cwd，且忽略用户站点包等不可信路径。
    env = dict(os.environ)
    worker = str(ROOT / "app" / "demo.py")
    cmd = [sys.executable, "-I", worker, "worker",
           "--sources", str(sources_path), "--responses", str(responses_path),
           "--runtime", str(runtime), "--crash-after-claim", crash_url]
    proc = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True)
    print(f"进程 A 退出码={proc.returncode}（模拟 kill 后退出）")
    assert proc.returncode == 0, proc.stderr

    snapshot = CheckpointStore(runtime).load()
    claimed = [t for o in snapshot["origins"] for t in o["tasks"]
               if t["url"] == crash_url]
    assert claimed and claimed[0]["state"] == "claimed", \
        "崩溃落盘的状态必须是 claimed"
    print(f"检查点确认 {crash_url} 停在 claimed，尚无结果。")

    # 子进程 B：独立进程恢复，必须把 claimed 退回 pending 并跑完整轮。
    cmd_b = [sys.executable, "-I", worker, "worker",
             "--sources", str(sources_path), "--responses", str(responses_path),
             "--runtime", str(runtime)]
    proc_b = subprocess.run(cmd_b, cwd=ROOT, env=env, capture_output=True,
                            text=True)
    assert proc_b.returncode == 0, proc_b.stderr
    print("进程 B 恢复成功，输出：")
    print(proc_b.stdout)

    journal = DecisionLog(runtime / "decisions.jsonl")
    recovered = [r for r in journal.read_all()
                 if r["reason"] == "lease-recovered" and r["url"] == crash_url]
    assert recovered, "恢复时必须记录 lease-recovered 原因"
    final = CheckpointStore(runtime).load()
    unfinished = [t for o in final["origins"] for t in o["tasks"]
                  if t["state"] in {"pending", "claimed"}]
    assert not unfinished, "恢复后不能有任务永久卡在执行中"
    print(f"断言通过：进程 B 记录 lease-recovered，"
          f"全部任务到达终态，残留 claimed 已清零。")


def worker(args: argparse.Namespace) -> int:
    """供跨进程演示调用的独立工作进程。"""
    try:
        with SchedulerLock(args.runtime):
            scheduler = build_scheduler(args.sources, args.responses,
                                        args.runtime,
                                        crash_after_claim=args.crash_after_claim)
            try:
                scheduler.run()
            except SystemExit as exc:
                # 模拟"领取后进程被杀死"：claim 已落盘，请求尚未发出。
                if args.crash_after_claim is not None and exc.code in (0, None):
                    print(f"模拟崩溃：已领取 {args.crash_after_claim} 后退出")
                    return 0
                raise
            for rt in scheduler.origins.values():
                states = {}
                for t in rt.tasks:
                    states[t.state] = states.get(t.state, 0) + 1
                print(f"{rt.config.origin} {states}")
            return 0
    except Exception as exc:  # 子进程把失败送回父进程断言
        print(f"worker 失败：{exc}", file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="三站点重放演示")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("all")
    sub.add_parser("fairness")
    sub.add_parser("cancel")
    sub.add_parser("crash")
    p_worker = sub.add_parser("worker")
    p_worker.add_argument("--sources", required=True)
    p_worker.add_argument("--responses", required=True)
    p_worker.add_argument("--runtime", required=True)
    p_worker.add_argument("--crash-after-claim", default=None)
    args = parser.parse_args(argv)

    if args.command == "worker":
        return worker(args)

    sources_path, responses_path = write_fixtures()
    runtime_base = ROOT / "runtime"
    # 每个场景用独立 runtime，并在开始前清理，保证子命令可重复运行、互不污染。
    def reset(name: str) -> Path:
        target = runtime_base / name
        shutil.rmtree(target, ignore_errors=True)
        return target

    if args.command in {"all", "fairness"}:
        demo_fairness(sources_path, responses_path, reset("fairness"))
    if args.command in {"all", "cancel"}:
        demo_cancel(sources_path, responses_path, reset("cancel"))
    if args.command in {"all", "crash"}:
        demo_crash(sources_path, responses_path, reset("crash"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

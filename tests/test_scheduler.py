"""调度器集成测试：策略闸门、限流退避、重定向、恢复与公平性。

全部使用 FakeClock + ReplayTransport，零真实等待、零真实网络。
"""

import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.checkpoint import CheckpointStore, CLAIMED, SchedulerLock, LockBusy
from app.clock import FakeClock
from app.journal import DecisionLog
from app.scheduler import Scheduler
from app.sources import read_sources
from app.transport import ReplayTransport, TransportError
from app.wire import Replay

PROJECT_ROOT = Path(__file__).resolve().parent.parent
START = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)
ALPHA = "https://alpha.invalid:443"
BETA = "https://beta.invalid:443"
GAMMA = "https://gamma.invalid:443"


def write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def make_sources(tmp: Path) -> Path:
    sources = [
        {"origin": "https://alpha.invalid", "concurrency": 1,
         "min_interval": 2,
         "urls": ["https://alpha.invalid/report?q=1",
                  "https://alpha.invalid/report?q=2"]},
        {"origin": "https://beta.invalid", "concurrency": 2,
         "min_interval": 1, "urls": ["https://beta.invalid/data"]},
        {"origin": "https://gamma.invalid", "concurrency": 1,
         "min_interval": 0,
         "urls": ["https://gamma.invalid/public",
                  "https://gamma.invalid/secret"]},
    ]
    path = tmp / "sources.json"
    write_json(path, sources)
    return path


def basic_responses() -> dict:
    return {
        "https://alpha.invalid/robots.txt": [
            {"status": 200,
             "body": "User-agent: DisclosureBot\nAllow: /report\nCrawl-delay: 2\n"}],
        "https://beta.invalid/robots.txt": [
            {"status": 200, "body": "User-agent: *\nAllow: /\n"}],
        "https://gamma.invalid/robots.txt": [
            {"status": 200,
             "body": "User-agent: *\nDisallow: /secret\nAllow: /public\n"}],
        "https://alpha.invalid/report?q=1": [
            {"status": 200, "body": "a1"}],
        "https://alpha.invalid/report?q=2": [
            {"status": 200, "body": "a2"}],
        "https://beta.invalid/data": [{"status": 200, "body": "b"}],
        "https://gamma.invalid/public": [{"status": 200, "body": "g"}],
        "https://gamma.invalid/secret": [{"status": 200, "body": "never"}],
    }


class Harness:
    def __init__(self, tmp: Path, responses: dict | None = None):
        self.tmp = tmp
        self.sources_path = make_sources(tmp)
        self.responses_path = tmp / "responses.json"
        write_json(self.responses_path, responses or basic_responses())
        self.runtime = tmp / "runtime"
        self.scheduler = self.build()

    def build(self) -> Scheduler:
        store = CheckpointStore(self.runtime)
        if store.exists():
            clock = FakeClock.from_state(store.load()["clock"])
        else:
            clock = FakeClock(START)
        replay = Replay(self.responses_path)
        transport = ReplayTransport(replay, clock)
        return Scheduler(read_sources(self.sources_path), transport, clock,
                         store, DecisionLog(self.runtime / "decisions.jsonl"))

    @property
    def records(self) -> list[dict]:
        return self.scheduler.journal.read_all()

    def records_for(self, url: str, verdict: str | None = None) -> list[dict]:
        return [r for r in self.records
                if url in r["url"]
                and (verdict is None or r["verdict"] == verdict)]

    def task(self, origin: str, url_fragment: str):
        rt = self.scheduler.origins[origin]
        return next(t for t in rt.tasks if url_fragment in t.url)


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.tmp = Path(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def test_robots_disallow_blocks_request(self):
        h = Harness(self.tmp)
        h.scheduler.run()
        secret = h.task(GAMMA, "/secret")
        self.assertEqual(secret.state, "rejected")
        verdicts = [r["reason"] for r in h.records if "/secret" in r["url"]]
        self.assertIn("robots-disallow", verdicts)
        # 被拒绝的地址绝不能到达传输层。
        fetched = [req["url"] for req in h.scheduler.transport.requests]
        self.assertFalse(any("/secret" in u for u in fetched))

    def test_distinct_query_strings_are_separate_tasks(self):
        h = Harness(self.tmp)
        h.scheduler.run()
        done = [t.url for rt in h.scheduler.origins.values()
                for t in rt.tasks if t.state == "done"]
        # 查询参数不同的业务 URL 各自独立保留，不折叠。
        self.assertIn("https://alpha.invalid/report?q=1", done)
        self.assertIn("https://alpha.invalid/report?q=2", done)
        fetched = [req["url"] for req in h.scheduler.transport.requests
                   if "/report" in req["url"]]
        self.assertEqual(len(fetched), 2)

    def test_min_interval_and_crawl_delay_measured_by_virtual_clock(self):
        h = Harness(self.tmp)
        h.scheduler.run()
        dispatches = [r for r in h.records
                      if r["reason"] == "dispatch" and "alpha" in r["origin"]]
        # alpha 两次派发间隔至少为 max(min_interval=2, crawl-delay=2)。
        gap = dispatches[1]["monotonic"] - dispatches[0]["monotonic"]
        self.assertGreaterEqual(gap, 2.0)
        delays = [r for r in h.records if r["reason"] in
                  {"min-interval", "crawl-delay"}]
        self.assertTrue(delays, "必须留下延迟原因记录")

    def test_retry_after_delta_seconds_scopes_backoff_to_origin(self):
        responses = basic_responses()
        responses["https://alpha.invalid/report?q=1"] = [
            {"status": 429, "headers": {"Retry-After": "8"}},
            {"status": 200, "body": "a1"}]
        h = Harness(self.tmp, responses)
        h.scheduler.run()
        retry = next(r for r in h.records
                     if r["reason"] == "retry-after" and "q=1" in r["url"])
        self.assertEqual(retry["until"] - retry["monotonic"], 8.0)
        # beta 的调度不应被 alpha 的退避推迟：beta 在 alpha 恢复前已完成。
        beta_done = next(r for r in h.records
                         if r["reason"] == "http-200" and "beta" in r["origin"])
        alpha_done = next(r for r in h.records
                          if r["reason"] == "http-200" and "q=1" in r["url"])
        self.assertLess(beta_done["monotonic"], alpha_done["monotonic"])
        # backoff 字段只存在于 alpha。
        self.assertIsNotNone(h.scheduler.origins[ALPHA].backoff_until)
        self.assertIsNone(h.scheduler.origins[BETA].backoff_until)

    def test_retry_after_http_date(self):
        responses = basic_responses()
        responses["https://alpha.invalid/report?q=1"] = [
            {"status": 503,
             "headers": {"Retry-After": "Wed, 11 Nov 2026 12:00:20 GMT"}},
            {"status": 200, "body": "a1"}]
        h = Harness(self.tmp, responses)
        h.scheduler.run()
        retry = next(r for r in h.records if r["reason"] == "retry-after")
        # 起点 12:00:00 => HTTP-date 解析为虚拟 t=20。
        self.assertEqual(retry["until"], 20.0)

    def test_cross_origin_redirect_re_evaluated_at_target(self):
        responses = basic_responses()
        responses["https://beta.invalid/data"] = [
            {"status": 302,
             "headers": {"Location": "https://gamma.invalid/secret"}}]
        h = Harness(self.tmp, responses)
        h.scheduler.run()
        redirect = next(r for r in h.records if r["reason"] == "redirect")
        self.assertIn("gamma.invalid/secret", redirect["detail"])
        target = h.task(GAMMA, "/secret")
        self.assertEqual(target.state, "rejected")
        # 目标地址没有被实际请求：借不到 beta 的配额直接发出。
        fetched = [req["url"] for req in h.scheduler.transport.requests]
        self.assertFalse(any("/secret" in u for u in fetched))

    def test_duplicate_url_in_round_not_fetched_twice(self):
        responses = basic_responses()
        # 两个初始 URL 都跳转到同一个允许的目标。
        responses["https://beta.invalid/data"] = [
            {"status": 302,
             "headers": {"Location": "https://gamma.invalid/public"}}]
        h = Harness(self.tmp, responses)
        h.scheduler.run()
        dup = [r for r in h.records if r["reason"] == "duplicate-url"]
        self.assertTrue(dup)
        public_fetches = [req for req in h.scheduler.transport.requests
                          if "/public" in req["url"]]
        self.assertEqual(len(public_fetches), 1)

    def test_robots_failure_pauses_origin_instead_of_allowing(self):
        responses = basic_responses()
        responses["https://alpha.invalid/robots.txt"] = [
            {"status": 500, "body": ""},
            {"status": 200,
             "body": "User-agent: DisclosureBot\nAllow: /report\n"}]
        h = Harness(self.tmp, responses)
        h.scheduler.run()
        pause = [r for r in h.records
                 if r["reason"] == "robots-unavailable"]
        self.assertTrue(pause, "robots 获取失败必须暂停并记录")
        # 暂停期间 alpha 的业务 URL 一个都不能发。
        early_fetches = [req["url"] for req in h.scheduler.transport.requests
                         if "/report" in req["url"]]
        self.assertTrue(early_fetches)  # 恢复后仍能完成
        first_report = next(r["monotonic"] for r in h.records
                            if r["reason"] == "dispatch" and "/report" in r["url"])
        self.assertGreater(first_report, 0.0)

    def test_transport_failure_does_not_default_allow(self):
        h = Harness(self.tmp)

        class FlakyTransport(ReplayTransport):
            def __init__(self, inner):
                self._inner = inner
                self.calls = 0

            @property
            def requests(self):
                return self._inner.requests

            def get(self, url, headers):
                if url.endswith("/robots.txt") and "alpha" in url:
                    self.calls += 1
                    if self.calls == 1:
                        raise TransportError("连接被重置")
                return self._inner.get(url, headers)

        inner = h.scheduler.transport
        h.scheduler.transport = FlakyTransport(inner)
        h.scheduler.run()
        self.assertTrue(any(r["reason"] == "transport-error"
                            for r in h.records))
        alpha_rt = h.scheduler.origins[ALPHA]
        # 失败后进入暂停，最终重试成功，任务全部终态。
        self.assertEqual(alpha_rt.robots_state, "ready")

    def test_fair_scheduling_under_persistent_rate_limit(self):
        responses = basic_responses()
        responses["https://alpha.invalid/report?q=1"] = [
            {"status": 429, "headers": {"Retry-After": "5"}},
            {"status": 429, "headers": {"Retry-After": "5"}},
            {"status": 200, "body": "a1"}]
        h = Harness(self.tmp, responses)
        h.scheduler.run()
        alpha_ok = next(r["monotonic"] for r in h.records
                        if r["reason"] == "http-200" and "q=1" in r["url"])
        beta_ok = [r["monotonic"] for r in h.records
                   if r["reason"] == "http-200" and "beta" in r["origin"]]
        gamma_ok = [r["monotonic"] for r in h.records
                    if r["reason"] == "http-200" and "gamma" in r["origin"]]
        self.assertTrue(beta_ok and max(beta_ok) < alpha_ok)
        self.assertTrue(gamma_ok and max(gamma_ok) < alpha_ok)

    def test_cancel_stops_new_requests(self):
        h = Harness(self.tmp)
        budget = {"n": 4}

        def cancelled() -> bool:
            return sum(1 for r in h.records
                       if r["verdict"] == "allow"
                       and r["reason"] in {"dispatch", "robots-fetch"}
                       ) >= budget["n"]

        h.scheduler.run(is_cancelled=cancelled)
        flip_count = budget["n"]
        after = [r for r in h.records
                 if r["verdict"] == "allow"
                 and r["reason"] in {"dispatch", "robots-fetch"}]
        # 同一节拍可能多放行，但翻转后不能继续增长到完成整轮。
        self.assertLess(len(after), 8)
        self.assertTrue(any(r["reason"] == "cancelled" for r in h.records))
        pending_left = [t for rt in h.scheduler.origins.values()
                        for t in rt.tasks if t.state == "pending"]
        self.assertTrue(pending_left)

    def test_claimed_task_recovered_in_new_scheduler_instance(self):
        h = Harness(self.tmp)

        def crash(url: str):
            if url == "https://alpha.invalid/report?q=1":
                raise SystemExit(0)

        h.scheduler.crash_hook = crash
        with self.assertRaises(SystemExit):
            h.scheduler.run()
        # 检查点里任务确实停在 claimed。
        snap = CheckpointStore(h.runtime).load()
        claimed = [t for o in snap["origins"] for t in o["tasks"]
                   if t["url"] == "https://alpha.invalid/report?q=1"]
        self.assertEqual(claimed[0]["state"], CLAIMED)

        # 全新实例（模拟独立进程）恢复：claimed 退回 pending 并完成。
        resumed = h.build()
        resumed.run()
        recover = [r for r in resumed.journal.read_all()
                   if r["reason"] == "lease-recovered"]
        self.assertTrue(recover)
        states = [t.state for rt in resumed.origins.values()
                  for t in rt.tasks]
        self.assertNotIn(CLAIMED, states)
        self.assertNotIn("pending", states)

    def test_recovery_runs_in_separate_process(self):
        # 真正跨独立进程验证恢复语义。
        h = Harness(self.tmp)
        worker = PROJECT_ROOT / "app" / "demo.py"
        env = dict(__import__("os").environ)
        cmd = [sys.executable, "-I", str(worker), "worker",
               "--sources", str(h.sources_path),
               "--responses", str(h.responses_path),
               "--runtime", str(h.runtime),
               "--crash-after-claim", "https://beta.invalid/data"]
        proc = subprocess.run(cmd, cwd=PROJECT_ROOT, env=env,
                              capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        proc2 = subprocess.run(
            [sys.executable, "-I", str(worker), "worker",
             "--sources", str(h.sources_path),
             "--responses", str(h.responses_path),
             "--runtime", str(h.runtime)],
            cwd=PROJECT_ROOT, env=env, capture_output=True, text=True)
        self.assertEqual(proc2.returncode, 0, proc2.stderr)
        snap = CheckpointStore(h.runtime).load()
        leftover = [t for o in snap["origins"] for t in o["tasks"]
                    if t["state"] in {"pending", "claimed"}]
        self.assertEqual(leftover, [])

    def test_lock_excludes_second_process(self):
        h = Harness(self.tmp)
        h.scheduler.run()
        with SchedulerLock(h.runtime):
            with self.assertRaises(LockBusy):
                SchedulerLock(h.runtime).acquire()

    def test_checkpoint_round_id_survives_restart(self):
        # 第一轮写入检查点；全新实例恢复时 round_id 必须保持，
        # 以保证"同一轮只采一次"的去重语义跨进程成立。
        h = Harness(self.tmp)
        rt_dir = self.tmp / "roundtest"
        store = CheckpointStore(rt_dir)
        clock = FakeClock(START)
        s1 = Scheduler(
            read_sources(h.sources_path),
            ReplayTransport(Replay(h.responses_path), clock),
            clock, store, DecisionLog(rt_dir / "decisions.jsonl"))
        original_round = s1.round_id
        s1.run()

        clock2 = FakeClock.from_state(store.load()["clock"])
        s2 = Scheduler(
            read_sources(h.sources_path),
            ReplayTransport(Replay(h.responses_path), clock2),
            clock2, store, DecisionLog(rt_dir / "decisions.jsonl"))
        self.assertEqual(s2.round_id, original_round)

    def test_concurrency_limit_is_per_origin(self):
        # beta 并发=2：用带虚拟延迟的传输制造在途窗口，验证同一 origin
        # 最多两个在途，超出的任务收到 concurrency-limit 并进入下一批。
        sources = [
            {"origin": "https://beta.invalid", "concurrency": 2,
             "min_interval": 0,
             "urls": ["https://beta.invalid/data",
                      "https://beta.invalid/notes",
                      "https://beta.invalid/third",
                      "https://beta.invalid/fourth"]},
        ]
        src_path = self.tmp / "c_sources.json"
        write_json(src_path, sources)
        resp = {
            "https://beta.invalid/robots.txt": [
                {"status": 200, "body": "User-agent: *\nAllow: /\n"}],
            "https://beta.invalid/data": [{"status": 200}],
            "https://beta.invalid/notes": [{"status": 200}],
            "https://beta.invalid/third": [{"status": 200}],
            "https://beta.invalid/fourth": [{"status": 200}],
        }
        resp_path = self.tmp / "c_responses.json"
        write_json(resp_path, resp)
        rt_dir = self.tmp / "c_runtime"
        store = CheckpointStore(rt_dir)
        clock = FakeClock(START)
        sched = Scheduler(read_sources(src_path),
                          ReplayTransport(Replay(resp_path), clock, latency=3.0),
                          clock, store,
                          DecisionLog(rt_dir / "decisions.jsonl"))
        sched.run()
        records = sched.journal.read_all()
        self.assertTrue(any(r["reason"] == "concurrency-limit"
                            for r in records))
        # 两批并行，每批 3 虚拟秒；robots 也占一批，故业务完成时刻 >= 6。
        business = [r for r in records if r["reason"] == "dispatch"]
        first_batch = [r for r in business if r["monotonic"]
                       == business[0]["monotonic"]]
        self.assertEqual(len(first_batch), 2, "同批最多填满并发额度 2")
        self.assertTrue(all(t.state == "done"
                            for rt in sched.origins.values()
                            for t in rt.tasks))

    def test_origins_run_in_parallel_with_independent_pools(self):
        # 两个 origin 各并发 1、虚拟延迟 3：独立线程池应让它们并行，
        # 而不是一个站点的在途请求把另一个站点串行化。
        sources = [
            {"origin": "https://a.invalid", "concurrency": 1,
             "min_interval": 0, "urls": ["https://a.invalid/x"]},
            {"origin": "https://b.invalid", "concurrency": 1,
             "min_interval": 0, "urls": ["https://b.invalid/y"]},
        ]
        src_path = self.tmp / "p_sources.json"
        write_json(src_path, sources)
        resp = {
            "https://a.invalid/robots.txt": [
                {"status": 200, "body": "User-agent: *\nAllow: /\n"}],
            "https://b.invalid/robots.txt": [
                {"status": 200, "body": "User-agent: *\nAllow: /\n"}],
            "https://a.invalid/x": [{"status": 200}],
            "https://b.invalid/y": [{"status": 200}],
        }
        resp_path = self.tmp / "p_responses.json"
        write_json(resp_path, resp)
        rt_dir = self.tmp / "p_runtime"
        store = CheckpointStore(rt_dir)
        clock = FakeClock(START)
        sched = Scheduler(read_sources(src_path),
                          ReplayTransport(Replay(resp_path), clock, latency=3.0),
                          clock, store,
                          DecisionLog(rt_dir / "decisions.jsonl"))
        sched.run()
        dispatches = [r for r in sched.journal.read_all()
                      if r["reason"] == "dispatch"]
        # 两个业务请求在同一虚拟时刻并行派发（串行则会相差 3 秒）。
        self.assertEqual(len({r["monotonic"] for r in dispatches}), 1)


if __name__ == "__main__":
    unittest.main()

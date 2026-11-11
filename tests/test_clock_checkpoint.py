"""时钟与检查点测试。"""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.checkpoint import CheckpointStore, LockBusy, SchedulerLock
from app.clock import FakeClock, RealClock

START = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)


class FakeClockTests(unittest.TestCase):
    def test_sleep_advances_only_virtual_time(self):
        clock = FakeClock(START)
        clock.sleep(10)
        clock.advance(5)
        self.assertEqual(clock.monotonic(), 15.0)
        self.assertEqual(clock.wall(),
                         datetime(2026, 11, 11, 12, 0, 15, tzinfo=timezone.utc))

    def test_http_date_parsed_against_virtual_origin(self):
        clock = FakeClock(START)
        # 虚拟起点 12:00:00，日期 12:00:20 => 单调时间 20。
        self.assertEqual(
            clock.parse_http_date("Wed, 11 Nov 2026 12:00:20 GMT"), 20.0)
        self.assertIsNone(clock.parse_http_date("not-a-date"))

    def test_state_roundtrip(self):
        clock = FakeClock(START)
        clock.advance(42)
        restored = FakeClock.from_state(clock.to_state())
        self.assertEqual(restored.monotonic(), 42.0)
        self.assertEqual(restored.wall(), clock.wall())

    def test_real_clock_http_date(self):
        clock = RealClock()
        target = clock.wall().timestamp() + 30
        http_value = datetime.fromtimestamp(
            target, timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        parsed = clock.parse_http_date(http_value)
        self.assertAlmostEqual(parsed, clock.monotonic() + 30, delta=2)


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = Path(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def test_save_and_load_roundtrip(self):
        store = CheckpointStore(self.dir)
        store.save({"round_id": "r1", "origins": []})
        loaded = store.load()
        self.assertEqual(loaded["version"], CheckpointStore.SCHEMA_VERSION)
        self.assertEqual(loaded["round_id"], "r1")

    def test_no_tmp_file_left_after_save(self):
        store = CheckpointStore(self.dir)
        store.save({"x": 1})
        self.assertFalse(list(self.dir.glob("*.tmp")))

    def test_bad_version_rejected(self):
        store = CheckpointStore(self.dir)
        (self.dir / "checkpoint.json").write_text(
            json.dumps({"version": 999}), encoding="utf-8")
        with self.assertRaises(ValueError):
            store.load()

    def test_lock_mutual_exclusion(self):
        with SchedulerLock(self.dir):
            with self.assertRaises(LockBusy):
                SchedulerLock(self.dir).acquire()
        # 释放后可再次获取。
        lock = SchedulerLock(self.dir)
        lock.acquire()
        lock.release()


if __name__ == "__main__":
    unittest.main()

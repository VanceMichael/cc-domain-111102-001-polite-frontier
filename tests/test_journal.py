"""决策日志测试。"""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.journal import ALLOW, DELAY, REJECT, DecisionLog


class JournalTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.path = Path(self._dir.name) / "decisions.jsonl"

    def tearDown(self):
        self._dir.cleanup()

    def test_records_are_append_only_jsonl(self):
        log = DecisionLog(self.path)
        now = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)
        log.record("r1", "o1", "u1", ALLOW, "dispatch", 0.0, now)
        log.record("r1", "o1", "u1", DELAY, "min-interval", 0.0, now, until=2.0)
        log.record("r1", "o1", "u2", REJECT, "robots-disallow", 0.0, now)
        lines = self.path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 3)
        for line in lines:  # 每行都是独立合法 JSON
            self.assertTrue(json.loads(line))

    def test_seq_continues_across_instances(self):
        log1 = DecisionLog(self.path)
        now = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)
        log1.record("r1", "o", "u", ALLOW, "dispatch", 0.0, now)
        log2 = DecisionLog(self.path)  # 模拟进程重启
        decision = log2.record("r1", "o", "u", ALLOW, "dispatch", 1.0, now)
        self.assertEqual(decision.seq, 2)
        self.assertEqual(log2.read_all()[-1]["seq"], 2)

    def test_filterable_by_verdict(self):
        log = DecisionLog(self.path)
        now = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)
        log.record("r1", "o", "u1", ALLOW, "dispatch", 0.0, now)
        log.record("r1", "o", "u2", REJECT, "robots-disallow", 0.0, now)
        entries = log.read_all()
        self.assertEqual({e["verdict"] for e in entries}, {ALLOW, REJECT})


if __name__ == "__main__":
    unittest.main()

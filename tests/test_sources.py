import unittest
import json
from app.wire import Replay

from app.sources import origin_of, read_sources
class DomainTests(unittest.TestCase):
    def test_default_port(self):
        self.assertEqual(origin_of("https://A.invalid/x"), "https://a.invalid:443")
    def test_credentials_rejected(self):
        with self.assertRaises(ValueError): origin_of("https://u:p@a.invalid/x")
    def test_sources(self):
        self.assertEqual(len(read_sources("fixtures/sources.json")), 3)


class WireTests(unittest.TestCase):
    def test_unknown_route(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        with TemporaryDirectory() as d:
            p=Path(d)/"routes.json";p.write_text("{}")
            self.assertEqual(Replay(p).get("https://missing.invalid").status,404)

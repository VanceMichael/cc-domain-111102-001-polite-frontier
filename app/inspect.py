from pathlib import Path
from app.wire import Replay
from app.sources import read_sources
print([s.origin for s in read_sources("fixtures/sources.json")])

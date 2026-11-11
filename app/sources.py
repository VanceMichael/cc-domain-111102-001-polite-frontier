from dataclasses import dataclass
import json
from pathlib import Path
from urllib.parse import urlsplit

@dataclass(frozen=True)
class Source:
    origin: str
    concurrency: int
    min_interval: float
    urls: tuple

def origin_of(url):
    p = urlsplit(url)
    if p.scheme not in {"http", "https"} or not p.hostname or p.username or p.password:
        raise ValueError("来源需要无凭据的 HTTP 地址")
    port = p.port or (443 if p.scheme == "https" else 80)
    return f"{p.scheme}://{p.hostname.lower()}:{port}"

def read_sources(path):
    values = json.loads(Path(path).read_text(encoding="utf-8"))
    result = []
    for v in values:
        if v["concurrency"] < 1 or v["min_interval"] < 0:
            raise ValueError("并发和间隔配置无效")
        expected = origin_of(v["origin"])
        if any(origin_of(u) != expected for u in v["urls"]):
            raise ValueError("初始地址与来源不一致")
        result.append(Source(expected, v["concurrency"], v["min_interval"], tuple(v["urls"])))
    return result

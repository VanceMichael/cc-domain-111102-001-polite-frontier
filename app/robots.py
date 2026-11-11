"""robots.txt 解释器（按指定 User-Agent 选择规则组）。

实现 RFC 9309 的核心语义：
- User-agent 大小写不敏感；显式命名的组优先于通配组 ``*``，只选一组，不合并；
- Allow 与 Disallow 按路径前缀/通配匹配，最长模式胜出，长度相同 Allow 胜出；
- Crawl-delay 来自选中组，非法值忽略；
- 空规则集（例如 404 得到的空文档）表示全部允许。

本模块只负责解释，"暂时获取失败则暂停" 的策略放在 scheduler 中。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Rule:
    allowed: bool
    pattern: str


@dataclass
class RobotsRules:
    user_agent: str
    rules: list[Rule] = field(default_factory=list)
    crawl_delay: float | None = None

    def allows(self, url: str) -> bool:
        path = urlsplit(url).path or "/"
        # 只匹配路径部分；查询参数不参与 robots 判定，业务 URL 不会被折叠。
        best: Rule | None = None
        for rule in self.rules:
            if _pattern_matches(rule.pattern, path):
                if best is None or len(rule.pattern) > len(best.pattern):
                    best = rule
        if best is None:
            return True
        return best.allowed


def _pattern_matches(pattern: str, path: str) -> bool:
    """支持 ``*`` 通配与结尾 ``$`` 的 robots 路径匹配。"""
    if "$" in pattern:
        end_anchor = pattern.endswith("$")
        pieces = pattern[:-1].split("*") if end_anchor else pattern.split("*")
        return _wildcard_match(pieces, path, end_anchor)
    if "*" not in pattern:
        return path.startswith(pattern)
    return _wildcard_match(pattern.split("*"), path, False)


def _wildcard_match(pieces: list[str], path: str, end_anchor: bool) -> bool:
    if not pieces:
        return True
    if not path.startswith(pieces[0]):
        return False
    cursor = len(pieces[0])
    for piece in pieces[1:-1] + ([] if end_anchor else pieces[-1:]):
        if end_anchor and piece == pieces[-1]:
            return path.endswith(piece) and cursor <= len(path)
        idx = path.find(piece, cursor)
        if idx < 0:
            return False
        cursor = idx + len(piece)
    if end_anchor:
        return path.endswith(pieces[-1])
    return True


def parse_robots(text: str, user_agent: str) -> RobotsRules:
    """从 robots 文本中为指定 UA 选出规则组。"""
    # 每次调用使用独立容器，避免可变默认结构跨调用污染。
    groups: dict[str, tuple[list[Rule], float | None]] = {}
    pending_agents: list[str] = []
    last_was_agent = False

    def touch(name: str) -> None:
        if name not in groups:
            groups[name] = ([], None)

    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            last_was_agent = False
            continue
        field, _, value = line.partition(":")
        field = field.strip().lower()
        value = value.strip()
        if field == "user-agent":
            name = value.lower()
            touch(name)
            if not last_was_agent:
                pending_agents = []
            pending_agents.append(name)
            last_was_agent = True
            continue
        last_was_agent = False
        if not pending_agents:
            continue
        if field == "allow":
            if value:
                for name in pending_agents:
                    rules, delay = groups[name]
                    rules.append(Rule(True, value))
        elif field == "disallow":
            # 空 Disallow 表示允许全部，等价于无规则，直接忽略。
            if value:
                for name in pending_agents:
                    rules, delay = groups[name]
                    rules.append(Rule(False, value))
        elif field == "crawl-delay":
            try:
                delay = float(value)
            except ValueError:
                continue
            if delay >= 0:
                for name in pending_agents:
                    groups[name] = (groups[name][0], delay)

    wanted = user_agent.lower()
    chosen: str | None = None
    if wanted in groups and wanted != "*":
        # 命名 UA 组存在则优先，哪怕规则为空。
        chosen = wanted
    elif "*" in groups:
        chosen = "*"
    else:
        # 没有任何适用组：全部允许。
        return RobotsRules(user_agent)

    rules, delay = groups[chosen]
    return RobotsRules(user_agent, list(rules), delay)

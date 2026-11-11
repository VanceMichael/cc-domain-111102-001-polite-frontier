"""robots 解释器测试：UA 选择、最长匹配、Crawl-delay、通配。"""

import unittest

from app.robots import parse_robots

UA = "DisclosureBot"


class RobotsSelectionTests(unittest.TestCase):
    def test_named_agent_preferred_over_wildcard(self):
        text = (
            "User-agent: *\nDisallow: /\n"
            "User-agent: DisclosureBot\nAllow: /public\n"
        )
        rules = parse_robots(text, UA)
        self.assertTrue(rules.allows("https://x.invalid/public/a"))
        # 命名组只有 Allow /public，其余路径无匹配 => 默认允许。
        self.assertTrue(rules.allows("https://x.invalid/other"))

    def test_wildcard_group_used_when_named_absent(self):
        rules = parse_robots("User-agent: *\nDisallow: /secret\n", UA)
        self.assertFalse(rules.allows("https://x.invalid/secret/file"))
        self.assertTrue(rules.allows("https://x.invalid/open"))

    def test_longest_match_wins_allow_on_tie(self):
        text = (
            "User-agent: *\n"
            "Disallow: /folder/\nAllow: /folder/file\n"
        )
        rules = parse_robots(text, UA)
        self.assertTrue(rules.allows("https://x.invalid/folder/file"))
        self.assertFalse(rules.allows("https://x.invalid/folder/other"))

    def test_case_insensitive_user_agent(self):
        rules = parse_robots(
            "User-agent: disclosurebot\nDisallow: /a\n", "DISCLOSUREBOT")
        self.assertFalse(rules.allows("https://x.invalid/a"))

    def test_consecutive_user_agent_lines_share_block(self):
        text = (
            "User-agent: Other\nUser-agent: DisclosureBot\n"
            "Disallow: /shared\n"
        )
        rules = parse_robots(text, UA)
        self.assertFalse(rules.allows("https://x.invalid/shared"))

    def test_query_string_not_part_of_path_match(self):
        rules = parse_robots("User-agent: *\nAllow: /report\n", UA)
        # 不同查询参数是不同业务 URL，但 robots 只按路径裁决，二者都允许。
        self.assertTrue(rules.allows("https://x.invalid/report?q=1"))
        self.assertTrue(rules.allows("https://x.invalid/report?q=2"))

    def test_wildcard_and_end_anchor(self):
        rules = parse_robots(
            "User-agent: *\nDisallow: /*.tmp$\n", UA)
        self.assertFalse(rules.allows("https://x.invalid/a.tmp"))
        self.assertTrue(rules.allows("https://x.invalid/a.tmp/x"))

    def test_crawl_delay_parsed_and_bad_value_ignored(self):
        rules = parse_robots(
            "User-agent: *\nCrawl-delay: 3\nAllow: /\n", UA)
        self.assertEqual(rules.crawl_delay, 3.0)
        rules2 = parse_robots(
            "User-agent: *\nCrawl-delay: soon\nAllow: /\n", UA)
        self.assertIsNone(rules2.crawl_delay)

    def test_empty_document_allows_all(self):
        rules = parse_robots("", UA)
        self.assertEqual(rules.rules, [])
        self.assertTrue(rules.allows("https://x.invalid/anything"))

    def test_comments_and_blank_lines(self):
        text = "# 注释\nUser-agent: *  # 行尾注释\n\nDisallow: /no\n"
        rules = parse_robots(text, UA)
        self.assertFalse(rules.allows("https://x.invalid/no"))

    def test_parser_has_no_shared_mutable_state(self):
        # 回归：连续解析不同文档时规则不能跨调用污染。
        first = parse_robots("User-agent: *\nDisallow: /a\n", UA)
        second = parse_robots("User-agent: *\nAllow: /b\n", UA)
        self.assertEqual(len(first.rules), 1)
        self.assertEqual(len(second.rules), 1)
        self.assertTrue(second.allows("https://x.invalid/a"))


if __name__ == "__main__":
    unittest.main()

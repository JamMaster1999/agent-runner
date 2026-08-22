#!/usr/bin/env python3
"""Committed stream captures replayed through the real parsers: typed
usage lands on the usage events and nowhere else, and no message carries
a usage number (the typed fields are the only consumer API)."""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

import os as _os

REPO = Path(__file__).resolve().parents[1]
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner.harness.claude_stream import ClaudeStreamParser  # noqa: E402
from agent_runner.harness.codex_stream import CodexStreamParser  # noqa: E402
from agent_runner.harness.stream import StreamEvent  # noqa: E402
from agent_runner.runtime import Usage  # noqa: E402

FIXTURES = REPO / "tests" / "fixtures" / "stream"
USAGE_EVENT_KINDS = {"turn_completed", "result_success", "result_error"}
USAGE_NUMBER = re.compile(r"\b(input|cached|cache read|cache write|output) \d+|cost \$")


def replay(name: str, parser) -> list[StreamEvent]:
    events: list[StreamEvent] = []
    for line in (FIXTURES / name).read_text(encoding="utf-8", errors="replace").splitlines():
        events.extend(parser.parse_line(line))
    return events


class FixtureReplayTest(unittest.TestCase):
    def assert_usage_only_on_usage_events(self, events: list[StreamEvent]) -> None:
        for event in events:
            self.assertNotRegex(event.message, USAGE_NUMBER)
            if event.event not in USAGE_EVENT_KINDS:
                for name in Usage.names():
                    self.assertIsNone(getattr(event, name), f"{event.event} must not set {name}")

    def test_codex_fixture(self) -> None:
        events = replay("codex_stdout_usage.jsonl", CodexStreamParser())
        self.assert_usage_only_on_usage_events(events)
        completed = [e for e in events if e.event == "turn_completed"]
        # Full usage, no usage key, a missing cached key, an empty usage dict.
        self.assertEqual(len(completed), 4)
        self.assertEqual(completed[0].tok_input, 2234830)
        self.assertEqual(completed[0].tok_cache_read, 2113536)
        self.assertEqual(completed[0].tok_cache_write, 0)
        self.assertEqual(completed[0].tok_output, 11021)
        self.assertIsNone(completed[2].tok_cache_read)
        self.assertTrue(all(e.cost_usd is None for e in completed))

    def test_claude_fixture(self) -> None:
        events = replay("claude_stdout_usage.jsonl", ClaudeStreamParser())
        self.assert_usage_only_on_usage_events(events)
        success = next(e for e in events if e.event == "result_success")
        error = next(e for e in events if e.event == "result_error")
        self.assertEqual(success.cost_usd, 0.326606)
        self.assertEqual(
            (success.tok_input, success.tok_cache_write, success.tok_cache_read, success.tok_output),
            (3, 16075, 35932, 5915),
        )
        self.assertEqual(error.cost_usd, 1.3731999999999998)
        self.assertEqual(error.tok_output, 2486)


if __name__ == "__main__":
    unittest.main()

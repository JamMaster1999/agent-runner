#!/usr/bin/env python3
"""Stream captures replayed through the real parsers: typed usage lands on
the usage events and nowhere else, and no message carries a usage number
(the typed fields are the only consumer API). The committed fixtures run
everywhere; the newest local production captures run when this machine
has them."""

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
# Pointed at a GTM tree, the sweep replays real production captures; in
# this repo (and CI) the directory is absent and the sweep skips.
LOCAL_RUNS = Path(_os.environ["AGENT_RUNNER_PROJECT_ROOT"]) / ".local" / "runs"
LIVE_CAPTURE_LIMIT = 25
USAGE_EVENT_KINDS = {"turn_completed", "result_success", "result_error"}
TOKEN_FIELDS = ("tok_input", "tok_cache_write", "tok_cache_read", "tok_output")
USAGE_NUMBER = re.compile(r"\b(input|cached|cache read|cache write|output) \d+|cost \$")


def replay(path: Path, parser) -> list[StreamEvent]:
    events: list[StreamEvent] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        events.extend(parser.parse_line(line))
    return events


class UsageContractMixin:
    def assert_usage_only_on_usage_events(self, events: list[StreamEvent], context: str = "") -> None:
        for event in events:
            self.assertNotRegex(event.message, USAGE_NUMBER, context)
            if event.event not in USAGE_EVENT_KINDS:
                for name in Usage.names():
                    self.assertIsNone(getattr(event, name), f"{context}: {event.event} must not set {name}")


class FixtureReplayTest(UsageContractMixin, unittest.TestCase):
    def test_codex_fixture(self) -> None:
        events = replay(FIXTURES / "codex_stdout_usage.jsonl", CodexStreamParser())
        self.assert_usage_only_on_usage_events(events)
        completed = [e for e in events if e.event == "turn_completed"]
        # Full usage, no usage key, a missing cached key, an empty usage dict.
        self.assertEqual(len(completed), 4)
        self.assertEqual(completed[0].tok_input, 2234830)
        self.assertEqual(completed[0].tok_cache_read, 2113536)
        self.assertEqual(completed[0].tok_cache_write, 0)
        self.assertEqual(completed[0].tok_output, 11021)
        # Each event is the delta since the last running total, so an
        # attempt that adds them ends at the last total the process reported.
        self.assertEqual((completed[2].tok_input, completed[2].tok_output), (48213, 902))
        self.assertIsNone(completed[2].tok_cache_read)
        summed = Usage()
        for event in events:
            summed.add_event(event)
        self.assertEqual((summed.tok_input, summed.tok_output, summed.tok_cache_read), (2283043, 11923, 2113536))
        self.assertTrue(all(e.cost_usd is None for e in completed))

    def test_claude_fixture(self) -> None:
        events = replay(FIXTURES / "claude_stdout_usage.jsonl", ClaudeStreamParser())
        self.assert_usage_only_on_usage_events(events)
        success = next(e for e in events if e.event == "result_success")
        error, capped = [e for e in events if e.event == "result_error"]
        self.assertEqual(success.cost_usd, 0.326606)
        # Summed over the per-model table (opus + the haiku subagent), not
        # the main-loop usage block.
        self.assertEqual(
            (success.tok_input, success.tok_cache_write, success.tok_cache_read, success.tok_output),
            (123, 18175, 43932, 6325),
        )
        self.assertEqual(error.cost_usd, 1.3731999999999998)
        self.assertEqual(error.tok_output, 2486)
        # A result without the per-model table (error_max_turns here) still
        # types its tokens, from the main-loop usage block.
        self.assertEqual(capped.cost_usd, 0.21)
        self.assertEqual((capped.tok_input, capped.tok_cache_write, capped.tok_cache_read, capped.tok_output), (12, 3100, 52000, 1900))


class LiveCaptureTest(UsageContractMixin, unittest.TestCase):
    """The newest local raw captures, when this machine has them: every
    usage event the real CLIs emitted carries typed tokens, and no message
    carries a usage number. Any payload shape the fixtures missed lands
    here first."""

    def newest_captures(self, filename: str) -> list[Path]:
        if not LOCAL_RUNS.is_dir():
            self.skipTest(".local/runs not present (CI)")
        captures = sorted(
            LOCAL_RUNS.glob(f"*/*/attempt-*/{filename}"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not captures:
            self.skipTest(f"no {filename} captures under .local/runs")
        return captures[:LIVE_CAPTURE_LIMIT]

    def sweep(self, filename: str, parser_factory, with_cost: bool) -> None:
        usage_events = 0
        for path in self.newest_captures(filename):
            events = replay(path, parser_factory())
            self.assert_usage_only_on_usage_events(events, str(path))
            for event in events:
                if event.event not in USAGE_EVENT_KINDS:
                    continue
                usage_events += 1
                for name in TOKEN_FIELDS:
                    self.assertIsNotNone(getattr(event, name), f"{path}: {event.event} left {name} untyped")
                if with_cost:
                    self.assertIsNotNone(event.cost_usd, f"{path}: {event.event} left cost_usd untyped")
        self.assertGreater(usage_events, 0, f"live sweep of {filename} saw no usage events")

    def test_codex_live_captures(self) -> None:
        self.sweep("codex.stdout.jsonl", CodexStreamParser, with_cost=False)

    def test_claude_live_captures(self) -> None:
        self.sweep("claude.stdout.log", ClaudeStreamParser, with_cost=True)


if __name__ == "__main__":
    unittest.main()

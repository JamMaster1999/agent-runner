#!/usr/bin/env python3
"""Typed usage == message-regex parity (extraction plan §4 step 2).

Migration 031 adds typed token/cost columns to pipeline_events; the dashboard
reads them with the old message regexes as fallback for pre-031 rows. The
whole scheme only works if, for every event the dashboard filters on, the
typed fields equal EXACTLY what the regexes would scrape back out of the
message text — parity by construction. This harness replays committed
fixtures (and, when present, the newest local live captures) through the real
parsers and asserts that equality per event, per field, and column-wise.
"""

from __future__ import annotations

import json
import re
import sys
import unittest
from pathlib import Path

import os as _os

REPO = Path(__file__).resolve().parents[1]
# Runner-repo test header: point the runner's path constants at this repo,
# then put src/ on sys.path when agent_runner is not already importable (the
# no-pip stdlib run — the same path the GTM bootstrap shim relies on).
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
_os.environ.setdefault("RUNNER_PROJECT_ID", "testproj")
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner.harness.claude_stream import ClaudeStreamParser  # noqa: E402
from agent_runner.harness.codex_stream import CodexStreamParser  # noqa: E402
from agent_runner.harness.stream import StreamEvent  # noqa: E402

FIXTURES = REPO / "tests" / "fixtures" / "stream"
# Live sweeps ride the configured project root: pointed at a GTM tree
# this replays real production captures; in this repo (and CI) the
# directory is absent and the live tests skip.
LOCAL_RUNS = Path(_os.environ["AGENT_RUNNER_PROJECT_ROOT"]) / ".local" / "runs"
LIVE_CAPTURE_LIMIT = 25

# The dashboard's message-scraping regexes, replicated verbatim from
# pipeline_dashboard/server.js:361-366 (POSIX substring() returns the first
# match's capture group, same as re.search().group(1)). tok_cache_read is a
# COALESCE: 'cache read (\d+)' (claude) else 'cached (\d+)' (codex).
RE_INPUT = re.compile(r"input (\d+)")
RE_CACHE_WRITE = re.compile(r"cache write (\d+)")
RE_CACHE_READ = re.compile(r"cache read (\d+)")
RE_CACHED = re.compile(r"cached (\d+)")
RE_OUTPUT = re.compile(r"output (\d+)")
RE_COST = re.compile(r"cost \$([0-9.]+)")

# The dashboard only scrapes these kinds (server.js:370); typed fields must
# be NULL on every other kind.
USAGE_EVENT_KINDS = {"turn_completed", "result_success", "result_error"}

TOKEN_FIELDS = ("tok_input", "tok_cache_write", "tok_cache_read", "tok_output")
# The message renders cost with %.4f, so regex-vs-typed can differ by up to
# half a final digit plus float noise.
COST_TOLERANCE = 5.1e-5


def regex_int(pattern: re.Pattern[str], message: str) -> int | None:
    match = pattern.search(message)
    return int(match.group(1)) if match else None


def regex_usage(message: str) -> dict[str, int | float | None]:
    cache_read = regex_int(RE_CACHE_READ, message)
    if cache_read is None:
        cache_read = regex_int(RE_CACHED, message)
    cost_match = RE_COST.search(message)
    return {
        "tok_input": regex_int(RE_INPUT, message),
        "tok_cache_write": regex_int(RE_CACHE_WRITE, message),
        "tok_cache_read": cache_read,
        "tok_output": regex_int(RE_OUTPUT, message),
        "cost_usd": float(cost_match.group(1)) if cost_match else None,
    }


def replay(path: Path, parser) -> list[StreamEvent]:
    events: list[StreamEvent] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        events.extend(parser.parse_line(line))
    return events


class TypedUsageParityMixin:
    """Per-event and column-sum parity assertions shared by fixture and
    live-capture tests."""

    def assert_event_parity(self, event: StreamEvent, context: str) -> None:
        if event.event not in USAGE_EVENT_KINDS:
            # Negative guard: no other event kind may carry typed usage.
            for field in (*TOKEN_FIELDS, "cost_usd"):
                self.assertIsNone(
                    getattr(event, field),
                    f"{context}: {event.event} must not set {field}",
                )
            return
        scraped = regex_usage(event.message)
        for field in TOKEN_FIELDS:
            # Typed fields are the consumer contract; the message is display
            # only. A regex scrape of the message, WHERE IT FINDS a value,
            # must agree with typed — but typed may carry values the message
            # never renders (codex tok_cache_write).
            if scraped[field] is None:
                continue
            self.assertEqual(
                getattr(event, field),
                scraped[field],
                f"{context}: {field} typed != regex on {event.message!r}",
            )
        typed_cost, regex_cost = event.cost_usd, scraped["cost_usd"]
        if typed_cost is None or regex_cost is None:
            self.assertEqual(
                typed_cost,
                regex_cost,
                f"{context}: cost_usd typed/regex null mismatch on {event.message!r}",
            )
        else:
            self.assertLessEqual(
                abs(typed_cost - regex_cost),
                COST_TOLERANCE,
                f"{context}: cost_usd typed {typed_cost} != regex {regex_cost}",
            )

    def assert_sum_parity(self, events: list[StreamEvent], context: str) -> None:
        usage_events = [e for e in events if e.event in USAGE_EVENT_KINDS]
        scraped = [regex_usage(e.message) for e in usage_events]
        for field in TOKEN_FIELDS:
            # Column-wise form of the same contract: sum only where the
            # scrape found a value, so typed-only fields never trip it.
            typed_sum = sum(
                getattr(e, field) or 0
                for e, s in zip(usage_events, scraped)
                if s[field] is not None
            )
            regex_sum = sum(s[field] or 0 for s in scraped)
            self.assertEqual(typed_sum, regex_sum, f"{context}: SUM({field}) mismatch")
        typed_cost = sum(e.cost_usd or 0.0 for e in usage_events)
        regex_cost = sum(s["cost_usd"] or 0.0 for s in scraped)
        n_cost = sum(1 for e in usage_events if e.cost_usd is not None)
        self.assertLessEqual(
            abs(typed_cost - regex_cost),
            COST_TOLERANCE * max(n_cost, 1),
            f"{context}: SUM(cost_usd) mismatch",
        )


class FixtureParityTest(TypedUsageParityMixin, unittest.TestCase):
    """Committed sanitized captures: full usage, absent usage, missing keys,
    error results, interleaved noise."""

    def replay_fixture(self, name: str, parser) -> list[StreamEvent]:
        events = replay(FIXTURES / name, parser)
        self.assertTrue(events, f"{name} produced no events — fixture broken?")
        return events

    def test_codex_fixture_parity(self) -> None:
        events = self.replay_fixture("codex_stdout_usage.jsonl", CodexStreamParser())
        completed = [e for e in events if e.event == "turn_completed"]
        # The fixture covers: full usage, no usage key, a missing usage key
        # (no cached_input_tokens), and an empty usage dict.
        self.assertEqual(len(completed), 4)
        for event in events:
            self.assert_event_parity(event, "codex fixture")
        self.assert_sum_parity(events, "codex fixture")
        # Codex types cache write when the raw payload carries
        # cache_write_input_tokens (typed is authoritative; the message
        # renders neither cache write nor cost). Cost stays None: the codex
        # stream carries no dollars.
        self.assertTrue(
            any(event.tok_cache_write is not None for event in completed),
            "fixture carries cache_write_input_tokens; typed field must land",
        )
        for event in completed:
            self.assertIsNone(event.cost_usd)

    def test_claude_fixture_parity(self) -> None:
        events = self.replay_fixture("claude_stdout_usage.jsonl", ClaudeStreamParser())
        kinds = [e.event for e in events]
        self.assertIn("result_success", kinds)
        self.assertIn("result_error", kinds)
        for event in events:
            self.assert_event_parity(event, "claude fixture")
        self.assert_sum_parity(events, "claude fixture")


class LiveCaptureParityTest(TypedUsageParityMixin, unittest.TestCase):
    """Replay the newest local raw captures, when this machine has them.

    CI-safe: skips when .local/runs is absent. On the dev machine this sweeps
    real production streams, so any payload shape the fixtures missed still
    gets the per-event parity check.
    """

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

    def sweep(self, filename: str, parser_factory) -> None:
        checked = 0
        for path in self.newest_captures(filename):
            events = replay(path, parser_factory())
            for event in events:
                self.assert_event_parity(event, str(path))
            self.assert_sum_parity(events, str(path))
            checked += len(events)
        self.assertGreater(checked, 0, f"live sweep of {filename} parsed no events")

    def test_codex_live_captures(self) -> None:
        self.sweep("codex.stdout.jsonl", CodexStreamParser)

    def test_claude_live_captures(self) -> None:
        self.sweep("claude.stdout.log", ClaudeStreamParser)


if __name__ == "__main__":
    unittest.main()

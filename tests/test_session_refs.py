#!/usr/bin/env python3
"""Characterization tests for session-ref extraction.

Pins session-ref extraction from captured attempt stdout files (the inputs
to `codex exec resume` / `claude --resume`), now living in the harness
adapters. Session refs are opaque text pulled from each dialect's stream.
"""

from __future__ import annotations

import json
import sys
import tempfile
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

from agent_runner.harness.claude_code import ClaudeCodeAdapter  # noqa: E402
from agent_runner.harness.codex import CodexAdapter  # noqa: E402


def codex_thread_id(path):
    return CodexAdapter().session_ref_from_log(path)


def claude_session_id(path):
    return ClaudeCodeAdapter().session_ref_from_log(path)


def write_jsonl(path: Path, payloads: list) -> Path:
    path.write_text("".join(json.dumps(payload) + "\n" for payload in payloads))
    return path


class CodexThreadIdTest(unittest.TestCase):
    def test_extracts_thread_id_from_thread_started(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = write_jsonl(
                Path(tmp) / "codex.stdout.jsonl",
                [
                    {"type": "turn.started"},
                    {"type": "thread.started", "thread_id": "th_abc123"},
                    {"type": "turn.completed"},
                ],
            )
            self.assertEqual(codex_thread_id(path), "th_abc123")

    def test_first_thread_started_wins(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = write_jsonl(
                Path(tmp) / "codex.stdout.jsonl",
                [
                    {"type": "thread.started", "thread_id": "th_first"},
                    {"type": "thread.started", "thread_id": "th_second"},
                ],
            )
            self.assertEqual(codex_thread_id(path), "th_first")

    def test_invalid_json_lines_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "codex.stdout.jsonl"
            path.write_text(
                "not json\n" + json.dumps({"type": "thread.started", "thread_id": "th_ok"}) + "\n"
            )
            self.assertEqual(codex_thread_id(path), "th_ok")

    def test_empty_thread_id_and_absent_event_yield_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            no_id = write_jsonl(
                Path(tmp) / "no_id.jsonl", [{"type": "thread.started", "thread_id": ""}]
            )
            self.assertIsNone(codex_thread_id(no_id))
            no_event = write_jsonl(Path(tmp) / "no_event.jsonl", [{"type": "turn.started"}])
            self.assertIsNone(codex_thread_id(no_event))

    def test_missing_file_yields_none(self) -> None:
        self.assertIsNone(codex_thread_id(Path("/nonexistent/codex.stdout.jsonl")))


class ClaudeSessionIdTest(unittest.TestCase):
    def test_extracts_first_session_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = write_jsonl(
                Path(tmp) / "claude.stdout.log",
                [
                    {"type": "system", "subtype": "init", "session_id": "sess-123"},
                    {"type": "assistant", "session_id": "sess-123"},
                ],
            )
            self.assertEqual(claude_session_id(path), "sess-123")

    def test_session_id_is_coerced_to_str(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = write_jsonl(Path(tmp) / "claude.stdout.log", [{"session_id": 123}])
            self.assertEqual(claude_session_id(path), "123")

    def test_invalid_json_lines_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "claude.stdout.log"
            path.write_text("garbage\n" + json.dumps({"session_id": "sess-9"}) + "\n")
            self.assertEqual(claude_session_id(path), "sess-9")

    def test_no_session_id_yields_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = write_jsonl(Path(tmp) / "claude.stdout.log", [{"type": "result"}])
            self.assertIsNone(claude_session_id(path))

    def test_missing_file_yields_none(self) -> None:
        self.assertIsNone(claude_session_id(Path("/nonexistent/claude.stdout.log")))


if __name__ == "__main__":
    unittest.main()

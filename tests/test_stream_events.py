#!/usr/bin/env python3
"""Characterization tests for the stream-dialect parser modules.

Pins the two stream-dialect parsers (Codex `codex exec --json` JSONL, Claude
`--print --output-format stream-json` JSONL) on fixture lines. The typed
usage fields are the consumer contract; messages are display only and carry
no usage numbers.
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

from agent_runner.harness.claude_stream import ClaudeStreamParser  # noqa: E402
from agent_runner.harness.codex_stream import CodexStreamParser  # noqa: E402
from agent_runner.harness.stream import (  # noqa: E402
    JsonlTail,
    StreamEvent,
    redact_db_urls,
)


class CodexStreamParserTest(unittest.TestCase):
    def parse(self, payload: dict) -> list[StreamEvent]:
        return CodexStreamParser().parse_line(json.dumps(payload))

    def test_thread_started_carries_session_id(self) -> None:
        events = self.parse({"type": "thread.started", "thread_id": "th_abc123"})
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "session_started")
        self.assertEqual(events[0].message, "Codex thread started: th_abc123")

    def test_turn_completed_usage_is_typed(self) -> None:
        events = self.parse(
            {
                "type": "turn.completed",
                "usage": {"input_tokens": 1200, "cached_input_tokens": 300, "output_tokens": 450},
            }
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "turn_completed")
        self.assertEqual(events[0].message, "Codex turn completed")
        self.assertEqual(events[0].tok_input, 1200)
        self.assertEqual(events[0].tok_cache_read, 300)
        self.assertEqual(events[0].tok_output, 450)
        # Absent from the payload -> untyped; the codex stream carries no dollars.
        self.assertIsNone(events[0].tok_cache_write)
        self.assertIsNone(events[0].cost_usd)

    def test_turn_completed_cache_write_is_typed(self) -> None:
        events = self.parse(
            {
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 10,
                    "cache_write_input_tokens": 999,
                    "cached_input_tokens": 5,
                    "output_tokens": 3,
                },
            }
        )
        self.assertEqual(events[0].tok_cache_write, 999)
        self.assertEqual(events[0].tok_input, 10)

    def test_turn_completed_missing_usage_key_stays_untyped(self) -> None:
        events = self.parse(
            {"type": "turn.completed", "usage": {"input_tokens": 7, "output_tokens": 2}}
        )
        self.assertIsNone(events[0].tok_cache_read)
        self.assertEqual(events[0].tok_input, 7)
        self.assertEqual(events[0].tok_output, 2)

    def test_turn_completed_without_usage(self) -> None:
        events = self.parse({"type": "turn.completed"})
        self.assertEqual(events[0].message, "Codex turn completed")
        for field in ("tok_input", "tok_cache_write", "tok_cache_read", "tok_output", "cost_usd"):
            self.assertIsNone(getattr(events[0], field))

    def test_turn_failed_event(self) -> None:
        events = self.parse({"type": "turn.failed", "error": {"message": "stream disconnected"}})
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "turn_failed")
        self.assertEqual(events[0].message, "Codex turn failed: stream disconnected")

    def test_turn_failed_without_error_message(self) -> None:
        events = self.parse({"type": "turn.failed"})
        self.assertEqual(events[0].message, "Codex turn failed: unknown error")

    def test_stream_error_event(self) -> None:
        events = self.parse({"type": "error", "message": "unexpected end of stream"})
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "stream_error")
        self.assertEqual(events[0].message, "Codex stream error: unexpected end of stream")

    def test_agent_message_lifts_progress_lines(self) -> None:
        parser = CodexStreamParser()
        events = parser.parse_line(
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "id": "item_1",
                        "type": "agent_message",
                        "text": "PROGRESS: 2/5 scraping subjects\nAll good",
                    },
                }
            )
        )
        self.assertEqual([e.event for e in events], ["agent_progress", "agent_message"])
        self.assertEqual(events[0].message, "[codex] scraping subjects")
        self.assertEqual((events[0].current, events[0].total), (2, 5))
        self.assertEqual(events[1].message, "Codex: All good")

    def test_item_events_dedupe_on_id_and_status(self) -> None:
        parser = CodexStreamParser()
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "item_1", "type": "agent_message", "text": "done"},
            }
        )
        first = parser.parse_line(line)
        second = parser.parse_line(line)
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])

    def test_invalid_json_and_non_dict_lines_yield_nothing(self) -> None:
        parser = CodexStreamParser()
        self.assertEqual(parser.parse_line("not json"), [])
        self.assertEqual(parser.parse_line("[1, 2, 3]"), [])
        self.assertEqual(parser.parse_line(""), [])


class ClaudeStreamParserTest(unittest.TestCase):
    def parse(self, payload: dict) -> list[StreamEvent]:
        return ClaudeStreamParser().parse_line(json.dumps(payload))

    def test_system_init_carries_model_and_session_id(self) -> None:
        events = self.parse(
            {"type": "system", "subtype": "init", "model": "claude-opus-4-6", "session_id": "sess-123"}
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "session_started")
        self.assertEqual(events[0].message, "Claude session started: claude-opus-4-6 (sess-123)")

    def test_result_success_usage_and_cost_are_typed(self) -> None:
        events = self.parse(
            {
                "type": "result",
                "subtype": "success",
                "duration_ms": 1000,
                "num_turns": 4,
                "usage": {
                    "input_tokens": 10,
                    "cache_creation_input_tokens": 20,
                    "cache_read_input_tokens": 30,
                    "output_tokens": 40,
                },
                "total_cost_usd": 1.2345,
            }
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "result_success")
        self.assertEqual(events[0].message, "Claude result: success (1000 ms, 4 turns)")
        self.assertEqual(events[0].tok_input, 10)
        self.assertEqual(events[0].tok_cache_write, 20)
        self.assertEqual(events[0].tok_cache_read, 30)
        self.assertEqual(events[0].tok_output, 40)
        self.assertEqual(events[0].cost_usd, 1.2345)

    def test_result_cost_typed_without_usage_block(self) -> None:
        events = self.parse(
            {"type": "result", "subtype": "success", "total_cost_usd": 0.123456789}
        )
        self.assertEqual(events[0].cost_usd, 0.123456789)
        # No usage block -> token fields stay untyped even though cost is set.
        self.assertIsNone(events[0].tok_input)

    def test_result_error_appends_detail(self) -> None:
        events = self.parse(
            {
                "type": "result",
                "subtype": "error_during_execution",
                "result": "something broke",
            }
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "result_error")
        self.assertEqual(events[0].message, "Claude result: error_during_execution — something broke")
        for field in ("tok_input", "tok_cache_write", "tok_cache_read", "tok_output", "cost_usd"):
            self.assertIsNone(getattr(events[0], field))

    def test_assistant_text_lifts_progress_lines(self) -> None:
        events = self.parse(
            {
                "type": "assistant",
                "message": {
                    "content": [
                        {"type": "text", "text": "PROGRESS: 1/4 directory located\nStarting sweep"}
                    ]
                },
            }
        )
        self.assertEqual([e.event for e in events], ["agent_progress", "agent_message"])
        self.assertEqual(events[0].message, "[claude] directory located")
        self.assertEqual((events[0].current, events[0].total), (1, 4))
        self.assertEqual(events[1].message, "Claude: Starting sweep")

    def test_tool_use_then_result_labels_completion_by_name(self) -> None:
        parser = ClaudeStreamParser()
        started = parser.parse_line(
            json.dumps(
                {
                    "type": "assistant",
                    "message": {
                        "content": [
                            {
                                "type": "tool_use",
                                "id": "tu_1",
                                "name": "WebSearch",
                                "input": {"query": "fixture university registrar"},
                            }
                        ]
                    },
                }
            )
        )
        self.assertEqual(started[0].event, "tool_started")
        self.assertEqual(started[0].message, "Tool call: WebSearch fixture university registrar")
        finished = parser.parse_line(
            json.dumps(
                {
                    "type": "user",
                    "message": {
                        "content": [
                            {"type": "tool_result", "tool_use_id": "tu_1", "content": "10 results"}
                        ]
                    },
                }
            )
        )
        self.assertEqual(finished[-1].event, "tool_completed")
        self.assertEqual(finished[-1].message, "Tool finished: WebSearch")

    def test_tool_result_error_becomes_tool_failed(self) -> None:
        parser = ClaudeStreamParser()
        parser.parse_line(
            json.dumps(
                {
                    "type": "assistant",
                    "message": {
                        "content": [
                            {"type": "tool_use", "id": "tu_2", "name": "Bash", "input": {"command": "false"}}
                        ]
                    },
                }
            )
        )
        events = parser.parse_line(
            json.dumps(
                {
                    "type": "user",
                    "message": {
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": "tu_2",
                                "is_error": True,
                                "content": [{"type": "text", "text": "boom"}],
                            }
                        ]
                    },
                }
            )
        )
        self.assertEqual(events[-1].event, "tool_failed")
        self.assertEqual(events[-1].message, "Tool failed: Bash — boom")

    def test_parent_tool_use_id_marks_subagent(self) -> None:
        events = self.parse(
            {
                "type": "assistant",
                "parent_tool_use_id": "tu_parent",
                "message": {"content": [{"type": "text", "text": "researching"}]},
            }
        )
        self.assertEqual(events[0].message, "[subagent] Claude: researching")


class RedactionTest(unittest.TestCase):
    def test_redact_db_urls(self) -> None:
        self.assertEqual(
            redact_db_urls("dsn postgresql://user:pw@host:5432/db failed"),
            "dsn postgres://[redacted] failed",
        )

    def test_stream_event_messages_are_redacted_on_construction(self) -> None:
        event = StreamEvent("stream_error", "cannot reach postgres://user:pw@host/db")
        self.assertEqual(event.message, "cannot reach postgres://[redacted]")


class JsonlTailTest(unittest.TestCase):
    def test_returns_only_complete_lines_and_advances(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "stream.jsonl"
            path.write_text('{"a": 1}\n{"b": ')
            tail = JsonlTail(path)
            self.assertEqual(tail.read_new_lines(), ['{"a": 1}'])
            # The partial line stays buffered until its newline arrives.
            self.assertEqual(tail.read_new_lines(), [])
            with path.open("a") as fh:
                fh.write('2}\n')
            self.assertEqual(tail.read_new_lines(), ['{"b": 2}'])

    def test_unicode_line_separator_does_not_shear_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "stream.jsonl"
            record = '{"text": "before\u2028after"}'.replace("\\u2028", "\u2028")
            path.write_text(record + "\n")
            lines = JsonlTail(path).read_new_lines()
            self.assertEqual(lines, [record])
            self.assertEqual(json.loads(lines[0])["text"], "before\u2028after")

    def test_missing_file_yields_nothing(self) -> None:
        tail = JsonlTail(Path("/nonexistent/stream.jsonl"))
        self.assertEqual(tail.read_new_lines(), [])


if __name__ == "__main__":
    unittest.main()

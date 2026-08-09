"""Shared stream-telemetry machinery for the harness adapters.

The pieces here are dialect-neutral: the PROGRESS line convention agents
print on every harness, the ``StreamEvent`` record the attempt loop hands
to ``on_event``, and ``JsonlTail`` for incrementally reading a live CLI
stdout capture.

Redaction rides with ``StreamEvent`` on purpose: callers forward event
messages to logs and dashboards, so a database URL captured from an
executed command line or echoed agent text must never reach an event
message.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROGRESS_LINE = re.compile(r"PROGRESS:\s*(\d+)\s*/\s*(\d+)\s*[-—:]?\s*(.*)")

MESSAGE_LIMIT = 300

# Defense in depth: events rows are served to the public dashboard, so
# a database URL captured from an executed command line or echoed agent text
# must never reach an event message.
DB_URL_RE = re.compile(r"postgres(?:ql)?://\S+", re.IGNORECASE)


def redact_db_urls(text: str) -> str:
    return DB_URL_RE.sub("postgres://[redacted]", str(text))


@dataclass
class StreamEvent:
    event: str
    message: str
    current: int | None = None
    total: int | None = None
    # Typed usage (migration 031): set ONLY on turn_completed/result_* events,
    # mirroring EXACTLY the numbers the message text renders — the dashboard's
    # message regexes stay the fallback for pre-031 rows, so typed and regex
    # must agree by construction (None where the regex would find nothing).
    tok_input: int | None = None
    tok_cache_write: int | None = None
    tok_cache_read: int | None = None
    tok_output: int | None = None
    cost_usd: float | None = None

    def __post_init__(self) -> None:
        # Every stream-derived message passes through here regardless of
        # which parser branch built it.
        self.message = redact_db_urls(self.message)


def typed_token(value: Any) -> int | None:
    """A usage value, typed — only genuine ints count.

    A missing/None/non-int usage key renders as e.g. ``input None`` in the
    message, where the ``input (\\d+)`` regex finds nothing; the typed column
    must be NULL there too (parity by construction). bools are ints in Python
    but never valid token counts.
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def clip(text: str, limit: int = MESSAGE_LIMIT) -> str:
    text = " ".join(str(text).split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def progress_events(text: str, source: str) -> list[StreamEvent]:
    events: list[StreamEvent] = []
    for match in PROGRESS_LINE.finditer(text or ""):
        current, total, message = int(match.group(1)), int(match.group(2)), match.group(3).strip()
        events.append(
            StreamEvent(
                event="agent_progress",
                message=clip(f"[{source}] {message or 'progress'}"),
                current=current,
                total=total,
            )
        )
    return events


class JsonlTail:
    """Incrementally read complete lines appended to a file since last call."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.offset = 0

    def read_new_lines(self) -> list[str]:
        try:
            with self.path.open("rb") as fh:
                fh.seek(self.offset)
                data = fh.read()
        except FileNotFoundError:
            return []
        if not data:
            return []
        end = data.rfind(b"\n")
        if end == -1:
            return []
        chunk = data[: end + 1]
        self.offset += len(chunk)
        # Split on newline bytes only: str.splitlines() also breaks on
        # U+2028/U+2029/U+0085, which appear unescaped inside JSON strings
        # of web-scraped text and would shear one JSONL record into two
        # unparsable (silently dropped) fragments.
        return [seg.decode("utf-8", errors="replace") for seg in chunk.split(b"\n") if seg]

"""Shared hook-capture machinery for the provider hook scripts.

Both capture scripts (``claude_hook_event``, ``codex_hook_event``) run
inside provider hook processes and share everything but their event fields:
stdin payload parsing, the RUNNER_* attribution stamp, and the atomic
append to the harness event log. The per-provider scripts supply only the
field mapping.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# The project tree these events belong to comes from the configured runner
# root (__file__-derivation is meaningless post-move): the client bridge
# shim (or the engine's inherited environment) supplies
# AGENT_RUNNER_PROJECT_ROOT / RUNNER_STATE_DIR before capture runs.
from agent_runner import util


def event_log_path(provider: str) -> Path:
    return util.state_dir() / f"{provider}_hooks" / "events.jsonl"


def env_value(*names: str) -> str | None:
    """First set value across the given attribution names."""
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def env_int(*names: str) -> int | None:
    value = env_value(*names)
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def read_payload() -> dict[str, Any]:
    """The hook's stdin as a dict; undecodable stdin is preserved raw."""
    raw = sys.stdin.read()
    try:
        return json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        return {"raw_stdin": raw}


def attribution() -> dict[str, Any] | None:
    """The RUNNER_* stamp identifying the orchestrated attempt this hook
    fired inside; None for interactive sessions in the repo (not
    orchestrator-launched — the capture script ignores them)."""
    run_id = env_value("RUNNER_RUN_ID")
    key = env_value("RUNNER_JOB_KEY")
    if not run_id or not key:
        return None
    return {
        "run_id": run_id,
        "key": key,
        "attempt": env_int("RUNNER_ATTEMPT"),
        "task_type": env_value("RUNNER_TASK_TYPE"),
        "backend": env_value("RUNNER_BACKEND"),
        "output_path": env_value("RUNNER_OUTPUT_PATH"),
    }


def append_event(provider: str, event: dict[str, Any]) -> None:
    """Stamp and append one captured event to the provider's event log.

    One O_APPEND write() per record: hook processes from parallel fan-out
    agents share this file, and POSIX O_APPEND makes a single write atomic
    with respect to offset, so records never interleave mid-line (a
    buffered text-mode append can split one large record across syscalls).
    Mode 0600: events carry assistant message content."""
    log_path = event_log_path(provider)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    record = {"at": datetime.now(timezone.utc).isoformat(), **event}
    line = json.dumps(record, separators=(",", ":")) + "\n"
    fd = os.open(log_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        os.write(fd, line.encode("utf-8"))
    finally:
        os.close(fd)

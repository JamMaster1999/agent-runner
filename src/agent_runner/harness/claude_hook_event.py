#!/usr/bin/env python3
"""Capture Claude Code hook events for the attempt loop to drain.

Invoked by committed hook configs as ``agent-runner hook claude`` (or
``python3 -m agent_runner hook claude``); appends one JSONL record per
event to the harness's local event log."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# The project tree these events belong to comes from the configured
# runner root (__file__-derivation is meaningless post-move): the client
# bridge shim (or the engine's inherited environment) supplies
# AGENT_RUNNER_PROJECT_ROOT / RUNNER_STATE_DIR before main() runs.
from agent_runner import util


def event_log_path() -> Path:
    return util.state_dir() / "claude_hooks" / "events.jsonl"


def env_int(*names: str) -> int | None:
    value = env_value(*names)
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def env_value(*names: str) -> str | None:
    """First set value across the given attribution names."""
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def main() -> None:
    run_id = env_value("RUNNER_RUN_ID")
    job_stable_id = env_value("RUNNER_JOB_KEY")
    if not run_id or not job_stable_id:
        return

    raw = sys.stdin.read()
    try:
        payload: dict[str, Any] = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        payload = {"raw_stdin": raw}

    event = {
        "at": datetime.now(timezone.utc).isoformat(),
        "hook_event_name": payload.get("hook_event_name"),
        "provider": "claude",
        "agent_type": payload.get("agent_type")
        or env_value("RUNNER_AGENT_NAME"),
        "agent_id": payload.get("agent_id"),
        "session_id": payload.get("session_id"),
        "transcript_path": payload.get("transcript_path"),
        "cwd": payload.get("cwd"),
        "permission_mode": payload.get("permission_mode"),
        "model": payload.get("model"),
        "source": payload.get("source"),
        "stop_hook_active": payload.get("stop_hook_active"),
        "last_assistant_message": payload.get("last_assistant_message"),
        "error": payload.get("error"),
        "error_details": payload.get("error_details"),
        "reason": payload.get("reason"),
        "run_id": run_id,
        "job_stable_id": job_stable_id,
        "phase": env_value("RUNNER_PHASE"),
        "backend": env_value("RUNNER_BACKEND"),
        "attempt": env_int("RUNNER_ATTEMPT"),
        "output_path": env_value("RUNNER_OUTPUT_PATH"),
    }

    event_log = event_log_path()
    event_log.parent.mkdir(parents=True, exist_ok=True)
    # One O_APPEND write() per record: hook processes from parallel fan-out
    # agents share this file, and POSIX O_APPEND makes a single write atomic
    # with respect to offset, so records never interleave mid-line (a
    # buffered text-mode append can split one large record across syscalls).
    line = json.dumps(event, separators=(",", ":")) + "\n"
    fd = os.open(event_log, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, line.encode("utf-8"))
    finally:
        os.close(fd)


if __name__ == "__main__":
    main()

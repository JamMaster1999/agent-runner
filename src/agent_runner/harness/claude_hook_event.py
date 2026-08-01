#!/usr/bin/env python3
"""Capture Claude Code hook events for the runner's engine poll loop.

Moved verbatim from GTM core/claude_hook_event.py at extraction step 6;
the GTM path keeps a bridge shim (delete at step 7) because
.claude/settings.json invokes it by repo-relative path. Consolidating
the claude/codex twins into one parameterized script is step-7 work."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# The project tree these events belong to comes from the configured
# runner root (__file__-derivation is meaningless post-move): the GTM
# bridge shim (or the engine's inherited environment) supplies
# AGENT_RUNNER_PROJECT_ROOT before this module imports.
from agent_runner.util import ROOT
EVENT_LOG = ROOT / ".local" / "claude_hooks" / "events.jsonl"


def env_int(name: str) -> int | None:
    value = os.environ.get(name)
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def main() -> None:
    run_id = os.environ.get("UFLO_RUN_ID")
    job_stable_id = os.environ.get("UFLO_JOB_STABLE_ID")
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
        "agent_type": payload.get("agent_type") or os.environ.get("UFLO_AGENT_NAME"),
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
        "phase": os.environ.get("UFLO_PHASE"),
        "backend": os.environ.get("UFLO_BACKEND"),
        "attempt": env_int("UFLO_ATTEMPT"),
        "output_path": os.environ.get("UFLO_OUTPUT_PATH"),
    }

    EVENT_LOG.parent.mkdir(parents=True, exist_ok=True)
    # One O_APPEND write() per record: hook processes from parallel fan-out
    # agents share this file, and POSIX O_APPEND makes a single write atomic
    # with respect to offset, so records never interleave mid-line (a
    # buffered text-mode append can split one large record across syscalls).
    line = json.dumps(event, separators=(",", ":")) + "\n"
    fd = os.open(EVENT_LOG, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, line.encode("utf-8"))
    finally:
        os.close(fd)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Capture Codex hook events for the attempt loop to drain.

Invoked by committed hook configs as ``agent-runner hook codex`` (or
``python3 -m agent_runner hook codex``); appends one JSONL record per
event to the harness's local event log.

Attribution comes from the RUNNER_* environment stamped by the attempt
loop's ``agent_env()`` — Codex propagates the exec process environment into every
hook process, including hooks fired for subagent activity (verified
2026-07-05). Events without a run id come from interactive Codex sessions
in the repo and are ignored.

Wired hook events (.codex/config.toml):
- PreToolUse (Read-blocker, separate script)
- PostToolUse on the collab spawn/wait tools — the reliable subagent
  lifecycle signal under ``codex exec`` with stable multi_agent (v1)
- SubagentStart/SubagentStop — do NOT fire under exec v1; kept as
  best-effort for the GUI app and future CLI versions
"""

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
    return util.state_dir() / "codex_hooks" / "events.jsonl"


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


def main() -> None:
    run_id = env_value("RUNNER_RUN_ID")
    job_stable_id = env_value("RUNNER_JOB_KEY")
    if not run_id or not job_stable_id:
        # Not an orchestrator-launched session; stay silent but keep the
        # contract that Subagent hooks expect JSON stdout.
        print(json.dumps({"continue": True}))
        return

    raw = sys.stdin.read()
    try:
        payload: dict[str, Any] = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        payload = {"raw_stdin": raw}

    tool_input = payload.get("tool_input") or {}
    spawned_agent_type = None
    if isinstance(tool_input, dict):
        spawned_agent_type = tool_input.get("agent_type") or tool_input.get("task_name")

    event = {
        "at": datetime.now(timezone.utc).isoformat(),
        "hook_event_name": payload.get("hook_event_name"),
        "agent_type": payload.get("agent_type"),
        "agent_id": payload.get("agent_id"),
        "session_id": payload.get("session_id"),
        "turn_id": payload.get("turn_id"),
        "model": payload.get("model"),
        "cwd": payload.get("cwd"),
        "transcript_path": payload.get("transcript_path"),
        "agent_transcript_path": payload.get("agent_transcript_path"),
        "tool_name": payload.get("tool_name"),
        "tool_use_id": payload.get("tool_use_id"),
        "spawned_agent_type": spawned_agent_type,
        "duration_ms": payload.get("duration_ms"),
        "last_assistant_message": payload.get("last_assistant_message"),
        "run_id": run_id,
        "job_stable_id": job_stable_id,
        "attempt": env_int("RUNNER_ATTEMPT"),
        "phase": env_value("RUNNER_PHASE"),
        "backend": env_value("RUNNER_BACKEND"),
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

    # SubagentStart/SubagentStop hooks expect JSON stdout on success.
    print(json.dumps({"continue": True}))


if __name__ == "__main__":
    main()

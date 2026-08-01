#!/usr/bin/env python3
"""Capture Codex hook events for the runner's engine poll loop.

Moved verbatim from GTM core/codex_hook_event.py at extraction step 6;
the GTM path keeps a bridge shim (delete at step 7) because
.codex/config.toml invokes it by repo-relative path. Consolidating the
claude/codex twins into one parameterized script is step-7 work.

Attribution comes exclusively from the UFLO_* environment stamped by the
orchestrator's ``agent_env()`` — Codex propagates the exec process
environment into every hook process, including hooks fired for subagent
activity (verified 2026-07-05). Events without UFLO_RUN_ID come from
interactive Codex sessions in this repo and are ignored.

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
# runner root (__file__-derivation is meaningless post-move): the GTM
# bridge shim (or the engine's inherited environment) supplies
# AGENT_RUNNER_PROJECT_ROOT before this module imports.
from agent_runner.util import ROOT
EVENT_LOG = ROOT / ".local" / "codex_hooks" / "events.jsonl"


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
        "attempt": env_int("UFLO_ATTEMPT"),
        "phase": os.environ.get("UFLO_PHASE"),
        "backend": os.environ.get("UFLO_BACKEND"),
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

    # SubagentStart/SubagentStop hooks expect JSON stdout on success.
    print(json.dumps({"continue": True}))


if __name__ == "__main__":
    main()

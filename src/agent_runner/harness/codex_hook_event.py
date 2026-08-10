#!/usr/bin/env python3
"""Capture Codex hook events for the attempt loop to drain.

Invoked by committed hook configs as ``agent-runner hook codex`` (or
``python3 -m agent_runner hook codex``); appends one JSONL record per
event to the harness's local event log. The shared capture machinery lives
in ``hook_capture``; this script is the Codex field mapping plus the JSON
stdout reply the Subagent hooks expect.

Attribution comes from the RUNNER_* environment stamped by the attempt
loop's ``agent_env()`` — Codex propagates the exec process environment into
every hook process, including hooks fired for subagent activity (verified
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

from agent_runner.harness import hook_capture

CONTINUE_STDOUT = json.dumps({"continue": True})


def main() -> None:
    attribution = hook_capture.attribution()
    if attribution is None:
        # Not an orchestrator-launched session; stay silent but keep the
        # contract that Subagent hooks expect JSON stdout.
        print(CONTINUE_STDOUT)
        return

    payload = hook_capture.read_payload()
    tool_input = payload.get("tool_input") or {}
    spawned_agent_type = None
    if isinstance(tool_input, dict):
        spawned_agent_type = tool_input.get("agent_type") or tool_input.get("task_name")

    hook_capture.append_event(
        "codex",
        {
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
            **attribution,
        },
    )

    # SubagentStart/SubagentStop hooks expect JSON stdout on success.
    print(CONTINUE_STDOUT)


if __name__ == "__main__":
    main()

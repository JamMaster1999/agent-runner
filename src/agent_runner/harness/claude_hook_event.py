#!/usr/bin/env python3
"""Capture Claude Code hook events for the attempt loop to drain.

Invoked by committed hook configs as ``agent-runner hook claude`` (or
``python3 -m agent_runner hook claude``); appends one JSONL record per
event to the harness's local event log. The shared capture machinery lives
in ``hook_capture``; this script is the Claude field mapping."""

from __future__ import annotations

from agent_runner.harness import hook_capture


def main() -> None:
    attribution = hook_capture.attribution()
    if attribution is None:
        return

    payload = hook_capture.read_payload()
    hook_capture.append_event(
        "claude",
        {
            "hook_event_name": payload.get("hook_event_name"),
            "provider": "claude",
            "agent_type": payload.get("agent_type")
            or hook_capture.env_value("RUNNER_AGENT_NAME"),
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
            **attribution,
        },
    )


if __name__ == "__main__":
    main()

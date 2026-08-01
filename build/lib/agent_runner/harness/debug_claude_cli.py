#!/usr/bin/env python3
"""Small Claude CLI smoke test for the Uflo orchestrator environment.

This intentionally does not touch pipeline_jobs or production results. It only
checks whether the local Claude CLI can make a simple request and whether the
same agent invocation style used by the orchestrator can write a tiny JSON file.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path


# Post-move the target project tree comes from the configured runner root
# (AGENT_RUNNER_PROJECT_ROOT), not __file__.
from agent_runner.util import PROJECT_ROOT, ROOT

DEBUG_ROOT = ROOT / ".local" / "debug_claude_cli"


def run(label: str, command: list[str], *, input_text: str | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    print(f"\n== {label} ==")
    print("$ " + " ".join(shellish(part) for part in command))
    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=env,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    print(f"exit={result.returncode}")
    if result.stdout:
        print("-- stdout --")
        print(result.stdout.rstrip())
    if result.stderr:
        print("-- stderr --")
        print(result.stderr.rstrip())
    return result


def shellish(value: str) -> str:
    if not value:
        return "''"
    if all(ch.isalnum() or ch in "-_./:=@" for ch in value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"


def safe_env_snapshot(env: dict[str, str]) -> dict[str, str]:
    prefixes = ("ANTHROPIC", "CLAUDE", "CODEX", "AWS", "BEDROCK", "VERTEX", "GOOGLE")
    snapshot: dict[str, str] = {}
    for key in sorted(env):
        if not key.startswith(prefixes):
            continue
        value = env.get(key, "")
        if any(secret in key.upper() for secret in ("KEY", "TOKEN", "SECRET", "PASSWORD")):
            snapshot[key] = "<set>" if value else ""
        else:
            snapshot[key] = value
    return snapshot


def main() -> int:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = DEBUG_ROOT / now
    out_dir.mkdir(parents=True, exist_ok=True)
    probe_path = out_dir / "agent_probe.json"

    claude_path = shutil.which("claude")
    print(f"project_root={PROJECT_ROOT}")
    print(f"debug_dir={out_dir}")
    print(f"which_claude={claude_path or '<missing>'}")
    print("env_relevant=" + json.dumps(safe_env_snapshot(os.environ), indent=2, sort_keys=True))

    if not claude_path:
        return 127

    failures = 0

    for label, command in [
        ("version", ["claude", "--version"]),
        ("auth status", ["claude", "auth", "status"]),
        (
            "simple api call",
            [
                "claude",
                "--print",
                "--output-format",
                "json",
                "--tools",
                "",
                "--max-budget-usd",
                "0.10",
                "--model",
                "sonnet",
                "Healthcheck only. Reply with OK.",
            ],
        ),
    ]:
        if run(label, command).returncode != 0:
            failures += 1

    agent_prompt = f"""Claude CLI debug probe.

Do not browse, search, inspect files, or run shell commands.
Write exactly this JSON object to this output path:

{probe_path}

{{"ok": true, "source": "debug_claude_cli"}}

After writing the file, print only OK.
"""

    env = os.environ.copy()
    env.update(
        {
            "UFLO_RUN_ID": f"debug_claude_cli__{now}",
            "UFLO_JOB_STABLE_ID": "debug_claude_cli__phase1__claude",
            "UFLO_ATTEMPT": "1",
            "UFLO_OUTPUT_PATH": str(probe_path),
            "UFLO_AGENT_NAME": "prod-phase1-institution",
            "UFLO_PHASE": "phase1",
            "UFLO_BACKEND": "claude",
            "CLAUDE_CODE_SESSIONEND_HOOKS_TIMEOUT_MS": "10000",
        }
    )
    agent_result = run(
        "agent write probe",
        [
            "claude",
            "--agent",
            "prod-phase1-institution",
            "--permission-mode",
            "bypassPermissions",
            "--disallowedTools=Read,Bash",
            "--print",
            "--verbose",
            "--output-format",
            "stream-json",
            "--include-hook-events",
        ],
        input_text=agent_prompt,
        env=env,
    )
    if agent_result.returncode != 0:
        failures += 1

    print("\n== written file check ==")
    if probe_path.exists():
        print(f"exists=true path={probe_path}")
        print(probe_path.read_text().rstrip())
    else:
        print(f"exists=false path={probe_path}")
        failures += 1

    if failures:
        print(f"\nFAILED checks={failures}")
        return 1
    print("\nOK all Claude CLI smoke checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

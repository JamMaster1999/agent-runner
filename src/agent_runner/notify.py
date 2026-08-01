#!/usr/bin/env python3
"""macOS operator notifier: the runner's pluggable alert implementation.

Copied from GTM core/notify_operator.py at extraction step 6 (plan §1:
the module SPLITS — phase-level notices stay GTM on the original file,
which the orchestrator still subprocesses; this copy is the runner's
alert impl). The two copies diverge from here on."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


# The notification log lands in the client project tree, resolved through
# the configured runner root (AGENT_RUNNER_PROJECT_ROOT), not __file__.
from agent_runner.util import ROOT

DEFAULT_LOG = ROOT / ".local" / "notifications.jsonl"


def append_log(path: Path, title: str, message: str, severity: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "message": message,
        "severity": severity,
    }
    with path.open("a") as fh:
        fh.write(json.dumps(event, separators=(",", ":")) + "\n")


def applescript_literal(text: str) -> str:
    """AppleScript string literal via json.dumps with ensure_ascii=False:
    osascript accepts raw UTF-8 but rejects \\uXXXX escapes, so non-ASCII
    (e.g. the '…' in validation failure messages) must pass through raw.
    JSON's \\" and \\\\ escapes are AppleScript-compatible. Control characters
    other than newline/tab are stripped first."""
    cleaned = "".join(ch for ch in text if ch in "\n\t" or ord(ch) >= 32)
    return json.dumps(cleaned, ensure_ascii=False)


def macos_notify(title: str, message: str) -> bool:
    if sys.platform != "darwin":
        return False
    script = (
        "display notification "
        + applescript_literal(message)
        + " with title "
        + applescript_literal(title)
    )
    try:
        result = subprocess.run(["osascript", "-e", script], check=False)
    except FileNotFoundError:
        return False
    return result.returncode == 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("message")
    parser.add_argument("--title", default="Uflo production pipeline")
    parser.add_argument("--severity", choices=("info", "warning", "error"), default="warning")
    parser.add_argument("--method", choices=("macos", "stdout", "none"), default="macos")
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    append_log(args.log, args.title, args.message, args.severity)

    if args.method == "stdout":
        print(f"[{args.severity}] {args.title}: {args.message}")
        return
    if args.method == "none":
        return

    if not macos_notify(args.title, args.message):
        print(f"[{args.severity}] {args.title}: {args.message}")


if __name__ == "__main__":
    main()

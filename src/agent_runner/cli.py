#!/usr/bin/env python3
"""The `agent-runner` CLI: the hook-capture shim and the sandbox keeper.

Two process boundaries: committed hook configs run ``agent-runner hook
<provider>`` (or ``python3 -m agent_runner hook <provider>``), which
captures one provider hook event from stdin into the harness's local
event log for the attempt loop to drain; and a sandbox's entrypoint runs
``python3 -m agent_runner keeper``, which keeps the workspace in S3 for
as long as the sandbox lives (``agent_runner.workspace``).

``hook`` is advisory at the process boundary: an internal failure (capture
crash, unwritable state dir) logs to stderr and exits 0, because it runs
inside provider hook processes that must never fail over telemetry.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from agent_runner.workspace import DEFAULT_CHECKPOINT_SECONDS, keeper_main


# The JSON hook-output contract: providers that parse hook stdout (the
# Subagent hooks) must still receive a well-formed "keep going" reply when
# capture fails — printed on every hook failure path, harmless for
# providers that ignore hook stdout.
CONTINUE_STDOUT = json.dumps({"continue": True})


def cmd_hook(args: argparse.Namespace) -> int:
    try:
        from importlib import import_module

        # Providers by convention, so this module stays provider-neutral:
        # each harness owns its capture script and its stdin/stdout contract.
        capture = import_module(f"agent_runner.harness.{args.provider}_hook_event")
        capture.main()
        return 0
    except (Exception, SystemExit) as exc:
        import sys

        # Advisory: hook capture must never fail the provider's hook
        # invocation. The capture main prints its own success stdout; on
        # failure, supply the JSON continue reply here and exit 0.
        print(CONTINUE_STDOUT)
        print(
            f"WARNING: agent-runner hook capture failed for {args.provider}: {exc!r}",
            file=sys.stderr,
        )
        return 0


def cmd_keeper(args: argparse.Namespace) -> int:
    return keeper_main(args.root, args.every)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-runner",
        description="Runner CLI shim for provider hook processes.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    hook = subparsers.add_parser(
        "hook",
        help="Capture one provider hook event from stdin (advisory: exits 0 on failure).",
    )
    hook.add_argument(
        "provider",
        help="Harness provider name; dispatches to agent_runner.harness.<provider>_hook_event",
    )
    hook.set_defaults(handler=cmd_hook)

    keeper = subparsers.add_parser(
        "keeper",
        help="The sandbox entrypoint: prepare the workspace, checkpoint it to S3 until released.",
    )
    keeper.add_argument("--root", type=Path, default=None, help="the workspace root (default: AGENT_RUNNER_WORKSPACE)")
    keeper.add_argument("--every", type=float, default=DEFAULT_CHECKPOINT_SECONDS, help="seconds between checkpoints")
    keeper.set_defaults(handler=cmd_keeper)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())

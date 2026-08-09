#!/usr/bin/env python3
"""The `agent-runner` CLI: the hook-capture shim.

The platform verbs (emit / requeue / migrate) died with the platform half
at the stage-3 carve-out — there is no job store to serve. What remains is
the one process boundary the CLIs themselves call: committed hook configs
run ``agent-runner hook <provider>`` (or ``python3 -m agent_runner hook
<provider>``), which captures one provider hook event from stdin into the
harness's local event log for the attempt loop to drain.

``hook`` is advisory at the process boundary: an internal failure (capture
crash, unwritable state dir) logs to stderr and exits 0, because it runs
inside provider hook processes that must never fail over telemetry.
"""

from __future__ import annotations

import argparse
import json


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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())

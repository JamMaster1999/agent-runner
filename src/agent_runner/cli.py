#!/usr/bin/env python3
"""The `agent-runner` CLI: emit / requeue / hook / migrate subcommands.

Step 7 of the extraction plan: the runner writes its events SQL directly
(``agent_runner.events``), so the client-repo job_event script and its
subprocess hop are gone. Agent prompts embed
``python3 -m agent_runner emit progress ...``; committed hook configs run
``... hook <provider>``; ``requeue`` is the operator recovery command.

Attribution falls back per value: explicit flag > RUNNER_* environment
(stamped by the engine's ``agent_env``) > the legacy UFLO_* names
(co-honored for one release, then removed). The DSN comes from
RUNNER_EMIT_DSN, else DATABASE_URL — never from argv, so it stays out of
process listings and the publicly served command_* stream events (a bridge
until the step-8 restricted emitter role exists).

``emit`` and ``hook`` are advisory at the process boundary: an internal
failure (DB hiccup, capture crash) logs to stderr and exits 0, because
these run inside agent shell commands and hook processes that must never
fail over telemetry. ``requeue`` and ``migrate`` are operator commands and
keep loud failures — a schema change that half-worked must never exit 0.

``migrate`` (extraction step 8) applies the runner database's own
migrations through ``agent_runner.migrations``; db/apply_migrations.py is
the same call from a repo checkout. Its DSN comes from a named environment
variable (RUNNER_DSN by default) or a private one-value file, never a raw
argv value and NEVER from DATABASE_URL — that variable names the client's
database, and the schema this command writes uses generic table names. The
applier also refuses a target that carries client tables or a foreign
migration ledger.

Import-light on purpose: no driver (and no agent_runner submodule) at
parse time — the stdlib suite imports this module cleanly; handlers import
lazily.

Agent shells resolve ``python3`` to the system interpreter, which may lack
psycopg — the deleted client-repo job_event script re-exec'd onto a venv
for exactly this. ``reexec_with_driver`` (called from the ``__main__``
process entries only, never from ``main()``) replays the invocation under
RUNNER_PYTHON — the engine's own interpreter, stamped by ``agent_env`` —
so in-shell emits write real rows instead of warning and losing them.
"""

from __future__ import annotations

import argparse
import json
import os
import sys


LIFECYCLES = ("start", "progress", "finish", "fail", "heartbeat")

# Per-value attribution fallback chain: explicit flag > RUNNER_* > legacy
# UFLO_* (the pre-extraction names, co-honored for one release).
ENV_FALLBACKS = {
    "job_key": ("RUNNER_JOB_KEY", "UFLO_JOB_STABLE_ID"),
    "run_id": ("RUNNER_RUN_ID", "UFLO_RUN_ID"),
    "attempt": ("RUNNER_ATTEMPT", "UFLO_ATTEMPT"),
    "phase": ("RUNNER_PHASE", "UFLO_PHASE"),
    "backend": ("RUNNER_BACKEND", "UFLO_BACKEND"),
}

# The JSON hook-output contract: providers that parse hook stdout (the
# Subagent hooks) must still receive a well-formed "keep going" reply when
# capture fails — printed on every hook failure path, harmless for
# providers that ignore hook stdout.
CONTINUE_STDOUT = json.dumps({"continue": True})

# Breaks the re-exec loop when RUNNER_PYTHON itself lacks psycopg (same
# guard shape as the GTM entry-point shim this CLI replaced).
_REEXEC_GUARD = "RUNNER_PSYCOPG_REEXEC"


def reexec_with_driver() -> None:
    """Process-entry shim for ``python3 -m agent_runner ...``: when psycopg
    is missing from the current interpreter, re-exec the same invocation
    (same argv, cwd, environment, open stdin) under RUNNER_PYTHON — the
    interpreter running the engine, stamped into agent shells by
    ``agent_env`` — before any handler takes the advisory failure path with
    the event row silently lost.

    Call from the ``__main__`` process entries ONLY, never from ``main()``:
    the suites call ``main()`` in-process and must not be exec'd away. No-op
    (deferring to the handlers' advisory/lazy-import failure story) when
    psycopg imports, when RUNNER_PYTHON is unset or gone, or when the guard
    shows one hop already happened.
    """
    try:
        import psycopg  # noqa: F401
    except ImportError:
        pass
    else:
        # Child processes started from here may hop themselves.
        os.environ.pop(_REEXEC_GUARD, None)
        return
    python = os.environ.get("RUNNER_PYTHON")
    if not python or os.environ.get(_REEXEC_GUARD) or not os.path.exists(python):
        return
    os.environ[_REEXEC_GUARD] = "1"
    os.execv(python, [python, "-m", "agent_runner", *sys.argv[1:]])


def resolved(explicit: str | None, name: str) -> str | None:
    """One attribution value through the fallback chain."""
    if explicit is not None:
        return explicit
    for env_name in ENV_FALLBACKS[name]:
        value = os.environ.get(env_name)
        if value:
            return value
    return None


def emit_dsn() -> str:
    url = os.environ.get("RUNNER_EMIT_DSN") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "RUNNER_EMIT_DSN (or DATABASE_URL) must be set in the "
            "environment; the DSN is never passed on argv."
        )
    return url


def load_batch(batch_json: str | None) -> list[dict]:
    if not batch_json:
        return []
    raw = sys.stdin.read() if batch_json == "-" else open(batch_json).read()
    batch = json.loads(raw)
    if not isinstance(batch, list):
        raise ValueError("--batch-json must contain a JSON array.")
    return batch


def cmd_emit(args: argparse.Namespace) -> int:
    try:
        from agent_runner import events  # lazy: needs the configured root

        job_key = resolved(args.job_key, "job_key")
        if not job_key:
            raise RuntimeError(
                "emit needs a job key: pass it on argv or set RUNNER_JOB_KEY."
            )
        attempt = args.attempt if args.attempt is not None else resolved(None, "attempt")
        stable_id, status = events.emit_event(
            emit_dsn(),
            args.lifecycle,
            job_key,
            run_id=resolved(args.run_id, "run_id"),
            phase=resolved(args.phase, "phase"),
            backend=resolved(args.backend, "backend"),
            attempt=None if attempt is None else int(attempt),
            event_name=args.event_name,
            message=args.message,
            current=args.current,
            total=args.total,
            batch=load_batch(args.batch_json),
        )
    except (Exception, SystemExit) as exc:
        # Advisory: agent progress must never fail the agent's shell command.
        print(
            f"WARNING: agent-runner emit {args.lifecycle} failed: {exc}",
            file=sys.stderr,
        )
        return 0
    if args.lifecycle == "heartbeat":
        # Keep the historical `status=` line for anything still reading the
        # cancel poll off CLI stdout.
        print(f"heartbeat: {stable_id} status={status}")
    else:
        print(f"{args.lifecycle}: {stable_id}")
    return 0


def cmd_requeue(args: argparse.Namespace) -> int:
    from agent_runner import jobstore  # lazy: needs the configured root

    url = os.environ.get("RUNNER_DSN") or os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("RUNNER_DSN (or DATABASE_URL) must be set to requeue a job.")
    jobstore.requeue_job(url, args.job_key)  # prints what was requeued
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    from agent_runner import migrations  # lazy: keeps parse time driver-free
    from agent_runner.secret_input import secret_value

    # apply_pending prints what it applied (or the dry-run list) and raises
    # SystemExit on any failure — operator command, no advisory swallow. It
    # also refuses a target that looks like a client database, which is why
    # the override rides through here rather than being decided locally.
    # Dry-run is intentionally offline: apply_pending lists schema + role
    # files before it ever resolves a DSN or imports psycopg.
    url = None
    if not args.dry_run:
        url = secret_value(
            label="runner database URL",
            env_name=args.database_url_env,
            file_path=args.database_url_file,
            default_env="RUNNER_DSN",
        )
    migrations.apply_pending(
        url,
        dry_run=args.dry_run,
        with_roles=not args.skip_roles,
        roles_only=args.roles_only,
        allow_foreign=args.i_know_this_is_the_runner_db,
    )
    return 0


def cmd_hook(args: argparse.Namespace) -> int:
    try:
        from importlib import import_module

        # Providers by convention, so this module stays provider-neutral:
        # each harness owns its capture script and its stdin/stdout contract.
        capture = import_module(f"agent_runner.harness.{args.provider}_hook_event")
        capture.main()
        return 0
    except (Exception, SystemExit) as exc:
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
        description="Runner CLI shims for agent shells, hook processes, and operators.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    emit = subparsers.add_parser(
        "emit",
        help="Append pipeline event(s) and run the guarded job update (advisory: exits 0 on failure).",
    )
    emit.add_argument("lifecycle", choices=LIFECYCLES)
    emit.add_argument(
        "job_key",
        nargs="?",
        help="pipeline_jobs stable id; falls back to RUNNER_JOB_KEY / UFLO_JOB_STABLE_ID",
    )
    emit.add_argument("--message")
    emit.add_argument("--current", type=int)
    emit.add_argument("--total", type=int)
    emit.add_argument("--run-id")
    emit.add_argument("--phase")
    emit.add_argument("--backend")
    emit.add_argument("--attempt", type=int)
    emit.add_argument(
        "--event-name", help="Event kind override; defaults to the lifecycle"
    )
    emit.add_argument(
        "--batch-json",
        help="Path (or '-' for stdin) to a JSON array of {event, message, current, total} to append in one update",
    )
    emit.set_defaults(handler=cmd_emit)

    requeue = subparsers.add_parser(
        "requeue",
        help="Put a blocked/failed/cancelled job back in the queue (attempt history preserved).",
    )
    requeue.add_argument("job_key")
    requeue.set_defaults(handler=cmd_requeue)

    hook = subparsers.add_parser(
        "hook",
        help="Capture one provider hook event from stdin (advisory: exits 0 on failure).",
    )
    hook.add_argument(
        "provider",
        help="Harness provider name; dispatches to agent_runner.harness.<provider>_hook_event",
    )
    hook.set_defaults(handler=cmd_hook)

    migrate = subparsers.add_parser(
        "migrate",
        help="Apply pending runner-database migrations (operator command: loud failures).",
    )
    database = migrate.add_mutually_exclusive_group()
    database.add_argument(
        "--database-url-env",
        metavar="NAME",
        help="environment variable holding the runner DSN (default: RUNNER_DSN)",
    )
    database.add_argument(
        "--database-url-file",
        metavar="PATH",
        help="private (mode 0600) file containing only the runner DSN",
    )
    migrate.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the migration filenames without connecting",
    )
    migrate.add_argument(
        "--skip-roles",
        action="store_true",
        help="Schema chain only; skip db/roles (emitter role + grants)",
    )
    migrate.add_argument(
        "--roles-only",
        action="store_true",
        help="Re-apply db/roles only — the repair path for revoked grants",
    )
    migrate.add_argument(
        "--i-know-this-is-the-runner-db",
        action="store_true",
        help=(
            "Override the client-database guard (target carries client "
            "tables or a foreign migration ledger)"
        ),
    )
    migrate.set_defaults(handler=cmd_migrate)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    reexec_with_driver()
    raise SystemExit(main())

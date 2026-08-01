#!/usr/bin/env python3
"""Operator entry point for the runner database's migrations.

Deliberate parity with GTM's db/apply_migrations.py — same path, same
flags, same output — so the muscle memory carries across repos. All of the
logic lives in ``agent_runner.migrations`` (importable, unit-testable, and
what ``agent-runner migrate`` calls); this file is only the argv surface.

  python3 db/apply_migrations.py --dry-run
  python3 db/apply_migrations.py --database-url postgres://…
  python3 db/apply_migrations.py --roles-only     # repair revoked grants

Without --database-url the DSN comes from RUNNER_DSN — and from NOWHERE
else. DATABASE_URL is deliberately not consulted: it is the client's
variable on every machine that has both, and this chain writes generically
named tables plus a cluster-global role. The applier additionally refuses a
target carrying client tables or a foreign migration ledger; that refusal
is overridable only with --i-know-this-is-the-runner-db.

db/roles (the emitter role and its grants) is not ledgered and re-applies
on every run — re-running is the repair path for a revoked grant. It is
also the only part that needs CREATEROLE; --skip-roles leaves it out.

Works with no pip install: src/ goes on sys.path when agent_runner is not
already importable, the same path the test headers take.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(ROOT / "src"))

from agent_runner.migrations import apply_pending  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-roles", action="store_true")
    parser.add_argument("--roles-only", action="store_true")
    parser.add_argument("--i-know-this-is-the-runner-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    apply_pending(
        args.database_url,
        dry_run=args.dry_run,
        with_roles=not args.skip_roles,
        roles_only=args.roles_only,
        allow_foreign=args.i_know_this_is_the_runner_db,
    )


if __name__ == "__main__":
    main()

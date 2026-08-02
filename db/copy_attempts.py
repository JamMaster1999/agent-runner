#!/usr/bin/env python3
"""Operator entry point for the step-9 attempts copy (one-time cutover).

  python3 db/copy_attempts.py \
      --source-url-env ATTEMPTS_COPY_SOURCE_DSN \
      --target-url-env ATTEMPTS_COPY_TARGET_DSN [--dry-run]

Copies GTM's pipeline_attempts into the runner database's attempts table,
handling the hazard on the attempts table comment (db/migrations/003, as
amended by 007): two-pass chain re-link and the resume_depth backfill, plus
a per-job renumber that is now cosmetic rather than forced by a unique key.
Logic lives in ``agent_runner.attempts_copy``; this file is only the argv
surface.

The URL values never ride argv.  Each side is read from a named environment
variable or a mode-0600 one-value file; the dedicated environment defaults
are ATTEMPTS_COPY_SOURCE_DSN and ATTEMPTS_COPY_TARGET_DSN.  The tool never
falls back to DATABASE_URL or RUNNER_DSN, because pointing either end at the
wrong database is the whole failure class. The target must pass the applier's
assert_runner_target (a GTM/client database is refused) and must already have
the migrations applied; the source must actually hold pipeline_attempts.
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

from agent_runner.attempts_copy import copy_attempts  # noqa: E402
from agent_runner.migrations import (  # noqa: E402
    _psycopg,
    _safe_exception_kind,
    assert_runner_target,
)
from agent_runner.secret_input import secret_value  # noqa: E402


SOURCE_ENV = "ATTEMPTS_COPY_SOURCE_DSN"
TARGET_ENV = "ATTEMPTS_COPY_TARGET_DSN"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--source-url-env",
        metavar="NAME",
        help=f"environment variable holding the source DSN (default: {SOURCE_ENV})",
    )
    source.add_argument(
        "--source-url-file",
        metavar="PATH",
        help="private (mode 0600) file containing only the source DSN",
    )
    target = parser.add_mutually_exclusive_group()
    target.add_argument(
        "--target-url-env",
        metavar="NAME",
        help=f"environment variable holding the target DSN (default: {TARGET_ENV})",
    )
    target.add_argument(
        "--target-url-file",
        metavar="PATH",
        help="private (mode 0600) file containing only the target DSN",
    )
    parser.add_argument("--project", default="gtm")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_url = secret_value(
        label="source database URL",
        env_name=args.source_url_env,
        file_path=args.source_url_file,
        default_env=SOURCE_ENV,
    )
    target_url = secret_value(
        label="target database URL",
        env_name=args.target_url_env,
        file_path=args.target_url_file,
        default_env=TARGET_ENV,
    )
    if source_url == target_url:
        raise SystemExit("Source and target resolve to the same DSN; the copy crosses databases.")
    try:
        psycopg = _psycopg()
        with (
            psycopg.connect(source_url) as source,
            psycopg.connect(target_url) as target,
        ):
            target.autocommit = False
            assert_runner_target(target)
            with source.cursor() as cur:
                cur.execute("SELECT to_regclass('public.pipeline_attempts') IS NOT NULL")
                if not cur.fetchone()[0]:
                    raise SystemExit(
                        "Source has no pipeline_attempts table — is the source"
                        " selector the GTM database?"
                    )
            with target.cursor() as cur:
                cur.execute("SELECT to_regclass('public.attempts') IS NOT NULL")
                if not cur.fetchone()[0]:
                    raise SystemExit(
                        "Target has no attempts table — apply db/migrations first."
                    )
            copy_attempts(
                source, target, project=args.project, dry_run=args.dry_run
            )
    except SystemExit:
        raise
    except Exception as exc:
        # Driver diagnostics are not a stable secrecy boundary.  Never render
        # an arbitrary exception that may have incorporated a connection URI.
        # The class + optional SQLSTATE remain enough to distinguish network,
        # authentication, and SQL failures without exposing either selector.
        raise SystemExit(
            f"Attempts copy failed ({_safe_exception_kind(exc)});"
            " database URL values were not logged."
        ) from None


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Operator entry point for the step-9 attempts copy (one-time cutover).

  python3 db/copy_attempts.py --source-url postgres://…gtm… \
                              --target-url postgres://…runner… [--dry-run]

Copies GTM's pipeline_attempts into the runner database's attempts table,
implementing the three-part hazard on the attempts table comment
(db/migrations/003): per-job renumber, two-pass chain re-link, and the
resume_depth backfill. Logic lives in ``agent_runner.attempts_copy``; this
file is only the argv surface.

Both URLs are explicit and required — this tool never reads DATABASE_URL or
RUNNER_DSN, because pointing either end at the wrong database is the whole
failure class. The target must pass the applier's assert_runner_target (a
GTM/client database is refused) and must already have the migrations
applied; the source must actually hold pipeline_attempts.
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
from agent_runner.migrations import _psycopg, assert_runner_target  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--target-url", required=True)
    parser.add_argument("--project", default="gtm")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.source_url == args.target_url:
        raise SystemExit("--source-url and --target-url are the same DSN; the copy crosses databases.")
    psycopg = _psycopg()
    with psycopg.connect(args.source_url) as source, psycopg.connect(args.target_url) as target:
        target.autocommit = False
        assert_runner_target(target)
        with source.cursor() as cur:
            cur.execute("SELECT to_regclass('public.pipeline_attempts') IS NOT NULL")
            if not cur.fetchone()[0]:
                raise SystemExit("source has no pipeline_attempts table — is --source-url the GTM database?")
        with target.cursor() as cur:
            cur.execute("SELECT to_regclass('public.attempts') IS NOT NULL")
            if not cur.fetchone()[0]:
                raise SystemExit("target has no attempts table — apply db/migrations first.")
        copy_attempts(source, target, project=args.project, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""The step-9 attempts copy, run for real across two scratch databases.

Exercises agent_runner.attempts_copy end to end on the exact hazard the
attempts table comment (003) documents: a force-rerun history whose attempt
numbers repeat across runs, a cross-run resume chain, and an exhausted
session that must STAY exhausted after the resume_depth backfill. The source
scratch database is built from a verbatim copy of GTM migration 026's DDL;
the target is built by the real applier over db/migrations.

The plan tests at the bottom (renumber, link resolution, malformed pairs)
are pure Python and run everywhere; the DB tests carry the same environment
contract as test_runner_migrations.py (psycopg + reachable Postgres, skip
cleanly otherwise).
"""

from __future__ import annotations

import getpass
import os
import subprocess
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock
from urllib.parse import urlsplit, urlunsplit

import os as _os

REPO = Path(__file__).resolve().parents[1]
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import migrations  # noqa: E402
from agent_runner.attempts import RESUME_BUDGET  # noqa: E402
from agent_runner.attempts_copy import build_plan, copy_attempts  # noqa: E402

import db.copy_attempts as copy_cli  # noqa: E402

TEST_URL = os.environ.get(
    "GTM_TEST_DATABASE_URL",
    f"postgres://{getpass.getuser()}@127.0.0.1:55432/uflo_gtm_production",
)

SOURCE_DB = "attempts_copy_source_scratch"
TARGET_DB = "attempts_copy_target_scratch"

# Verbatim from GTM db/migrations/026_create_pipeline_attempts.sql (minus
# indexes/comments — only the shape feeds the copy).
SOURCE_DDL = """
CREATE TABLE pipeline_attempts (
  id                  bigserial PRIMARY KEY,
  job_stable_id       text NOT NULL,
  run_id              text NOT NULL,
  attempt             integer NOT NULL,
  phase               text NOT NULL,
  backend             text NOT NULL,
  prompt_fingerprint  text NOT NULL,
  session_id          text,
  attempt_dir         text NOT NULL,
  outcome             text,
  failure_category    text,
  consumed_by_run_id  text,
  consumed_by_attempt integer,
  consumed_at         timestamptz,
  created_at          timestamptz NOT NULL DEFAULT now(),
  finished_at         timestamptz,

  UNIQUE (run_id, job_stable_id, attempt),
  CHECK (outcome IS NULL OR outcome IN ('succeeded', 'failed'))
);
"""


def _with_dbname(url: str, dbname: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path=f"/{dbname}"))


SOURCE_URL = _with_dbname(TEST_URL, SOURCE_DB)
TARGET_URL = _with_dbname(TEST_URL, TARGET_DB)


def _live_db_available() -> bool:
    try:
        import psycopg
    except ImportError:
        return False
    try:
        with psycopg.connect(TEST_URL, connect_timeout=3):
            return True
    except psycopg.Error:
        return False


def _ts(second: int) -> datetime:
    return datetime(2026, 7, 1, 0, 0, second, tzinfo=timezone.utc)


def _admin_exec(sql: str) -> None:
    import psycopg

    with psycopg.connect(TEST_URL, autocommit=True) as conn:
        conn.execute(sql)


# The force-rerun hazard, literally: job J ran twice (R1 then R2, attempt
# numbers restarting at 1), with a resume chain crossing the run boundary:
# (R1,1) <- (R1,2) <- (R2,1) <- (R2,2), head open with a session. Job K is a
# single fresh attempt; job M carries the half-cleared consumer pair the old
# two-column unconsume could leave behind.
SEED_ROWS = [
    # (job, run, attempt, session, consumed_by_run, consumed_by_attempt, created_at)
    ("J", "R1", 1, "sess-a", "R1", 2, _ts(1)),
    ("J", "R1", 2, "sess-b", "R2", 1, _ts(2)),
    ("J", "R2", 1, "sess-c", "R2", 2, _ts(3)),
    ("J", "R2", 2, "sess-d", None, None, _ts(4)),
    ("K", "R2", 1, "sess-k", None, None, _ts(5)),
    ("M", "R1", 1, "sess-m", "R9", None, _ts(6)),
]


class BuildPlanTest(unittest.TestCase):
    """Pure-Python plan logic; no database anywhere."""

    def source_rows(self):
        return [
            (job, run, attempt, "codex", f"fp-{job}", session, f"/tmp/{job}-{run}-{attempt}",
             None, None, c_run, c_att, created, None)
            for (job, run, attempt, session, c_run, c_att, created) in SEED_ROWS
        ]

    def test_renumber_is_per_job_chronological(self) -> None:
        plan = build_plan(self.source_rows())
        by_triple = {r.old_triple: r.new_attempt for r in plan.rows}
        self.assertEqual(by_triple[("R1", "J", 1)], 1)
        self.assertEqual(by_triple[("R1", "J", 2)], 2)
        self.assertEqual(by_triple[("R2", "J", 1)], 3)
        self.assertEqual(by_triple[("R2", "J", 2)], 4)
        self.assertEqual(by_triple[("R2", "K", 1)], 1)

    def test_links_and_malformed_pairs_are_counted(self) -> None:
        plan = build_plan(self.source_rows())
        self.assertEqual(plan.source_count, 6)
        self.assertEqual(plan.linked_count, 3)
        self.assertEqual(plan.malformed_count, 1)
        malformed = [r for r in plan.rows if r.malformed_consumer]
        self.assertEqual(len(malformed), 1)
        self.assertIsNone(malformed[0].consumed_by)


class CopyCliGuardTest(unittest.TestCase):
    """The argv surface carries selector names, never database secrets."""

    def test_same_url_refused_without_url_in_argv_or_logs(self) -> None:
        sentinel = "postgres://copy:SENTINEL_COPY_PASSWORD@same.invalid/db"
        command = [
            sys.executable,
            str(REPO / "db" / "copy_attempts.py"),
            "--source-url-env",
            "COPY_TEST_SOURCE",
            "--target-url-env",
            "COPY_TEST_TARGET",
        ]
        self.assertNotIn(sentinel, command)
        environment = dict(os.environ)
        environment.update(COPY_TEST_SOURCE=sentinel, COPY_TEST_TARGET=sentinel)
        proc = subprocess.run(
            command,
            env=environment,
            capture_output=True, text=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("same DSN", proc.stderr)
        self.assertNotIn(sentinel, proc.stdout + proc.stderr)
        self.assertNotIn("SENTINEL_COPY_PASSWORD", proc.stdout + proc.stderr)

    def test_no_generic_database_fallback(self) -> None:
        environment = dict(os.environ)
        environment.pop("ATTEMPTS_COPY_SOURCE_DSN", None)
        environment.pop("ATTEMPTS_COPY_TARGET_DSN", None)
        environment["DATABASE_URL"] = (
            "postgres://wrong:GENERIC_PASSWORD@client.invalid/db"
        )
        environment["RUNNER_DSN"] = (
            "postgres://wrong:RUNNER_PASSWORD@runner.invalid/db"
        )
        proc = subprocess.run(
            [sys.executable, str(REPO / "db" / "copy_attempts.py")],
            env=environment,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("ATTEMPTS_COPY_SOURCE_DSN", proc.stderr)
        self.assertNotIn("GENERIC_PASSWORD", proc.stdout + proc.stderr)
        self.assertNotIn("RUNNER_PASSWORD", proc.stdout + proc.stderr)

    def test_driver_exception_cannot_render_either_url(self) -> None:
        source = "postgres://source:SENTINEL_SOURCE_PASSWORD@source.invalid/db"
        target = "postgres://target:SENTINEL_TARGET_PASSWORD@target.invalid/db"

        class DriverFailure(Exception):
            sqlstate = "28P01"

        fake_driver = types.SimpleNamespace(
            connect=mock.Mock(side_effect=DriverFailure(source + " " + target))
        )
        with (
            mock.patch.dict(
                os.environ,
                {"COPY_TEST_SOURCE": source, "COPY_TEST_TARGET": target},
            ),
            mock.patch.object(copy_cli, "_psycopg", return_value=fake_driver),
            mock.patch.object(
                sys,
                "argv",
                [
                    "copy_attempts.py",
                    "--source-url-env",
                    "COPY_TEST_SOURCE",
                    "--target-url-env",
                    "COPY_TEST_TARGET",
                ],
            ),
            self.assertRaises(SystemExit) as caught,
        ):
            copy_cli.main()
        rendered = str(caught.exception)
        self.assertIn("DriverFailure, SQLSTATE 28P01", rendered)
        self.assertNotIn(source, rendered)
        self.assertNotIn(target, rendered)
        self.assertNotIn("SENTINEL_SOURCE_PASSWORD", rendered)
        self.assertNotIn("SENTINEL_TARGET_PASSWORD", rendered)


@unittest.skipUnless(_live_db_available(), "psycopg + local Postgres required")
class CopyAttemptsLiveTest(unittest.TestCase):
    """The copy against real scratch databases, ordered phases."""

    @classmethod
    def setUpClass(cls) -> None:
        import psycopg

        for db in (SOURCE_DB, TARGET_DB):
            _admin_exec(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
            _admin_exec(f"CREATE DATABASE {db}")
        with psycopg.connect(SOURCE_URL, autocommit=True) as conn:
            conn.execute(SOURCE_DDL)
            for (job, run, attempt, session, c_run, c_att, created) in SEED_ROWS:
                conn.execute(
                    "INSERT INTO pipeline_attempts (job_stable_id, run_id, attempt,"
                    " phase, backend, prompt_fingerprint, session_id, attempt_dir,"
                    " consumed_by_run_id, consumed_by_attempt, created_at)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (job, run, attempt, "phase5", "codex", f"fp-{job}", session,
                     f"/tmp/{job}-{run}-{attempt}", c_run, c_att, created),
                )
        # Roles are cluster-global and not what this test is about.
        migrations.apply_pending(TARGET_URL, with_roles=False)

    @classmethod
    def tearDownClass(cls) -> None:
        for db in (SOURCE_DB, TARGET_DB):
            _admin_exec(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")

    def connections(self):
        import psycopg

        source = psycopg.connect(SOURCE_URL)
        target = psycopg.connect(TARGET_URL)
        target.autocommit = False
        self.addCleanup(source.close)
        self.addCleanup(target.close)
        return source, target

    def target_rows(self, sql: str, params=None) -> list[tuple]:
        import psycopg

        with psycopg.connect(TARGET_URL, autocommit=True) as conn:
            return conn.execute(sql, params).fetchall()

    def test_01_dry_run_writes_nothing(self) -> None:
        source, target = self.connections()
        summary = copy_attempts(source, target, dry_run=True)
        self.assertEqual(summary["source_rows"], 6)
        self.assertEqual(self.target_rows("SELECT count(*) FROM attempts")[0][0], 0)

    def test_02_copy_renumbers_relinks_and_backfills(self) -> None:
        source, target = self.connections()
        summary = copy_attempts(source, target)
        self.assertEqual(summary["target_rows"], 6)
        self.assertEqual(summary["chain_links_present"], 3)
        self.assertEqual(summary["unresolved_links"], 0)
        self.assertEqual(summary["malformed_pairs"], 1)

        rows = self.target_rows(
            "SELECT attempt, session_ref, consumed_by_attempt_id, resume_depth"
            " FROM attempts WHERE job_key = 'J' ORDER BY attempt"
        )
        self.assertEqual([r[0] for r in rows], [1, 2, 3, 4])
        self.assertEqual([r[1] for r in rows], ["sess-a", "sess-b", "sess-c", "sess-d"])
        # The chain re-linked through the OLD coordinates: each row's consumer
        # is the next renumbered row; the head is unconsumed.
        ids = self.target_rows(
            "SELECT attempt, id FROM attempts WHERE job_key = 'J' ORDER BY attempt"
        )
        id_of = {attempt: row_id for attempt, row_id in ids}
        self.assertEqual([r[2] for r in rows],
                         [id_of[2], id_of[3], id_of[4], None])
        # Depth counts the chain BEHIND each row (the nomination semantics):
        # fresh session 0, head of the four-row chain 3.
        self.assertEqual([r[3] for r in rows], [0, 1, 2, 3])

    def test_03_exhausted_chain_stays_exhausted(self) -> None:
        # The whole point of the backfill direction: J's head has been
        # resumed RESUME_BUDGET times, so `resume_depth < budget` must FAIL
        # for it — an inverted backfill would give it depth 0 and re-arm it.
        (head_depth,) = self.target_rows(
            "SELECT resume_depth FROM attempts WHERE job_key = 'J'"
            " AND consumed_by_attempt_id IS NULL"
        )[0]
        self.assertGreaterEqual(head_depth, RESUME_BUDGET)
        (fresh_depth,) = self.target_rows(
            "SELECT resume_depth FROM attempts WHERE job_key = 'K'"
        )[0]
        self.assertLess(fresh_depth, RESUME_BUDGET)

    def test_04_rerun_is_a_clean_noop(self) -> None:
        source, target = self.connections()
        summary = copy_attempts(source, target)
        self.assertIn("already copied", summary["note"])
        self.assertEqual(self.target_rows("SELECT count(*) FROM attempts")[0][0], 6)

    def test_05_mismatched_target_refuses(self) -> None:
        import psycopg

        with psycopg.connect(TARGET_URL, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO attempts (project_id, job_key, attempt, harness,"
                " lease_ref, prompt_fingerprint, workspace_ref)"
                " VALUES ('gtm', 'EXTRA', 99, 'codex', 'RX', 'fp-x', '/tmp/x')"
            )
        try:
            source, target = self.connections()
            with self.assertRaises(SystemExit) as ctx:
                copy_attempts(source, target)
            self.assertIn("refusing to guess", str(ctx.exception))
        finally:
            with psycopg.connect(TARGET_URL, autocommit=True) as conn:
                conn.execute("DELETE FROM attempts WHERE job_key = 'EXTRA'")


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Resume-claim SQL against a scratch database (design-doc step-0 gate item).

Runs the REAL pipeline_attempts statements in agent_runner/attempts.py —
record_attempt_start / record_attempt_session / record_attempt_outcome /
claim_resumable_attempt / unconsume_attempt — against a live Postgres, in a
scratch database this class creates from the migration-026 snapshot
(tests/fixtures/pipeline_attempts.sql) and drops again.
GTM's tests/test_attempt_paths.py covers the same claim logic with
`db_rows` mocked; this file is the one place the recursive-CTE nomination, the
verify-before-consume UPDATE, and the budget arithmetic execute for real.

Same environment contract as GTM's tests/test_db_transport.py: needs psycopg plus
a reachable Postgres (the .local instance on 55432, or GTM_TEST_DATABASE_URL)
and skips cleanly otherwise, so the stdlib-only suite stays green. The
connecting role must be able to CREATE DATABASE (true for the local dev
instance and the CI service container).
"""

from __future__ import annotations

import argparse
import getpass
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from urllib.parse import urlsplit, urlunsplit

import os as _os

REPO = Path(__file__).resolve().parents[1]
# Runner-repo test header: point the runner's path constants at this repo,
# then put src/ on sys.path when agent_runner is not already importable (the
# no-pip stdlib run — the same path the GTM bootstrap shim relies on).
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import attempts  # noqa: E402
from agent_runner.runtime import RunnerJob  # noqa: E402

TEST_URL = os.environ.get(
    "GTM_TEST_DATABASE_URL",
    f"postgres://{getpass.getuser()}@127.0.0.1:55432/uflo_gtm_production",
)

SCRATCH_DB = "gtm_test_resume_claim_scratch"

# Byte-identical snapshot of GTM migration 026 — the compat-bridge schema
# (runner-owned migrations supersede it at extraction step 8).
MIGRATION_026 = REPO / "tests" / "fixtures" / "pipeline_attempts.sql"

FP = "a" * 64
OTHER_FP = "b" * 64


def _with_dbname(url: str, dbname: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path=f"/{dbname}"))


SCRATCH_URL = _with_dbname(TEST_URL, SCRATCH_DB)


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


def _admin_exec(sql: str) -> None:
    import psycopg

    with psycopg.connect(TEST_URL, autocommit=True) as conn:
        conn.execute(sql)


def _scratch_rows(sql: str, params=None) -> list[tuple]:
    import psycopg

    with psycopg.connect(SCRATCH_URL, autocommit=True) as conn:
        cursor = conn.execute(sql, params)
        return cursor.fetchall() if cursor.description is not None else []


def make_job(backend: str = "claude", key: str | None = None) -> RunnerJob:
    """The step-5 store-half fixture: the generic RunnerJob (key/task_type/
    harness are the columns the attempts statements bind)."""
    return RunnerJob(
        key=key or f"999_scratch-university__phase5_batch_001__{backend}",
        group_key="999_scratch-university",
        task_type="phase5",
        harness=backend,
        agent_ref="prod-phase5-instructor",
        attempt_dir_name="phase5_batch_001",
        output_filename="phase5_batch_001.json",
        canonical_relpath="results/999_scratch-university/claude/phase5_batch_001.json",
    )


@unittest.skipUnless(_live_db_available(), "psycopg + local Postgres required")
class ResumeClaimSqlTest(unittest.TestCase):
    """The pipeline_attempts statements, executed for real."""

    @classmethod
    def setUpClass(cls) -> None:
        _admin_exec(f"DROP DATABASE IF EXISTS {SCRATCH_DB} WITH (FORCE)")
        _admin_exec(f"CREATE DATABASE {SCRATCH_DB}")
        # The real migration DDL, not a hand-copied schema: drift between the
        # migration and the claim SQL fails here first.
        _scratch_rows(MIGRATION_026.read_text())

    @classmethod
    def tearDownClass(cls) -> None:
        _admin_exec(f"DROP DATABASE IF EXISTS {SCRATCH_DB} WITH (FORCE)")

    def setUp(self) -> None:
        _scratch_rows("TRUNCATE pipeline_attempts RESTART IDENTITY")
        self.args = argparse.Namespace(database_url=SCRATCH_URL)
        self.job = make_job()
        self.tmp = Path(tempfile.mkdtemp(prefix="gtm_resume_claim_")).resolve()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    # -- helpers ----------------------------------------------------------

    def start_attempt(
        self,
        run_id: str,
        attempt: int,
        *,
        fingerprint: str = FP,
        session_id: str | None = None,
        job: RunnerJob | None = None,
    ) -> Path:
        job = job or self.job
        directory = self.tmp / run_id / f"attempt-{attempt:02d}"
        attempts.record_attempt_start(
            self.args, job, run_id, attempt, fingerprint, directory
        )
        if session_id is not None:
            attempts.record_attempt_session(self.args, job, run_id, attempt, session_id)
        return directory

    def claim(self, run_id: str, attempt: int, *, fingerprint: str = FP):
        return attempts.claim_resumable_attempt(
            self.args, self.job, run_id, attempt, fingerprint
        )

    def rows(self) -> list[tuple]:
        return _scratch_rows(
            "SELECT run_id, attempt, session_id, consumed_by_run_id,"
            " consumed_by_attempt, consumed_at"
            " FROM pipeline_attempts ORDER BY id"
        )

    # -- record statements ------------------------------------------------

    def test_record_start_then_claim_round_trip(self) -> None:
        directory = self.start_attempt("run-a", 1, session_id="sess-a")
        claimed = self.claim("run-b", 1)
        self.assertIsNotNone(claimed)
        session_id, resumed_dir, candidate_id = claimed
        self.assertEqual(session_id, "sess-a")
        self.assertEqual(resumed_dir, directory)
        (row,) = self.rows()
        self.assertEqual(row[:5], ("run-a", 1, "sess-a", "run-b", 1))
        self.assertIsNotNone(row[5])  # consumed_at stamped
        self.assertEqual(
            _scratch_rows("SELECT id FROM pipeline_attempts")[0][0], candidate_id
        )
        # Filesystem mirror of the DB consumption.
        self.assertTrue((directory / "resume_consumed.json").exists())

    def test_record_start_upserts_the_same_attempt_row(self) -> None:
        directory = self.start_attempt("run-a", 1)
        attempts.record_attempt_start(
            self.args, self.job, "run-a", 1, OTHER_FP, directory
        )
        rows = _scratch_rows("SELECT prompt_fingerprint FROM pipeline_attempts")
        self.assertEqual(rows, [(OTHER_FP,)])

    def test_session_ref_first_write_wins(self) -> None:
        self.start_attempt("run-a", 1, session_id="sess-first")
        attempts.record_attempt_session(self.args, self.job, "run-a", 1, "sess-late")
        (row,) = self.rows()
        self.assertEqual(row[2], "sess-first")

    def test_failed_attempt_is_still_resumable(self) -> None:
        # 2026-07-28 resume policy: failed-in-any-category resumes too; only
        # a changed fingerprint or a spent budget blocks the claim.
        self.start_attempt("run-a", 1, session_id="sess-a")
        attempts.record_attempt_outcome(
            self.args, self.job, "run-a", 1, "failed", "agent_error"
        )
        outcome = _scratch_rows(
            "SELECT outcome, failure_category, finished_at IS NOT NULL"
            " FROM pipeline_attempts"
        )
        self.assertEqual(outcome, [("failed", "agent_error", True)])
        claimed = self.claim("run-b", 1)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed[0], "sess-a")

    # -- nomination filters -----------------------------------------------

    def test_claim_skips_wrong_fingerprint_missing_session_and_other_backend(self) -> None:
        self.start_attempt("run-a", 1, fingerprint=OTHER_FP, session_id="sess-other-fp")
        self.start_attempt("run-a", 2)  # session never recorded
        codex_job = make_job(backend="codex", key=self.job.key)  # same job, other backend
        self.start_attempt("run-a", 3, session_id="sess-codex", job=codex_job)
        self.assertIsNone(self.claim("run-b", 1))
        for row in self.rows():
            self.assertIsNone(row[3], f"claim consumed a non-candidate row: {row}")

    def test_claim_takes_the_newest_candidate(self) -> None:
        self.start_attempt("run-a", 1, session_id="sess-old")
        self.start_attempt("run-a", 2, session_id="sess-new")
        claimed = self.claim("run-b", 1)
        self.assertEqual(claimed[0], "sess-new")
        old_row, new_row = self.rows()
        self.assertIsNone(old_row[3])  # older candidate left for a later claim
        self.assertEqual(new_row[3], "run-b")

    def test_consumed_row_is_never_reclaimed(self) -> None:
        self.start_attempt("run-a", 1, session_id="sess-a")
        self.assertIsNotNone(self.claim("run-b", 1))
        self.assertIsNone(self.claim("run-c", 1))

    # -- budget (recursive chain) -----------------------------------------

    def test_budget_counts_the_session_chain_not_the_job(self) -> None:
        # s0 resumed three times through the consumption chain; the fourth
        # resume of that lineage is refused, but a brand-new session on the
        # same job claims fine (R7: a job is never permanently unresumable).
        self.start_attempt("run-0", 1, session_id="sess-0")
        for n in (1, 2, 3):
            claimed = self.claim(f"run-{n}", 1)
            self.assertIsNotNone(claimed, f"resume {n} of the chain should be allowed")
            self.assertEqual(claimed[0], f"sess-{n - 1}")
            self.start_attempt(f"run-{n}", 1, session_id=f"sess-{n}")
        self.assertIsNone(
            self.claim("run-4", 1), "chain of RESUME_BUDGET consumptions must stop"
        )
        self.start_attempt("run-fresh", 1, session_id="sess-fresh")
        claimed = self.claim("run-5", 1)
        self.assertIsNotNone(claimed, "a fresh session starts with a full budget")
        self.assertEqual(claimed[0], "sess-fresh")

    # -- verify-before-consume --------------------------------------------

    def test_lost_race_consumes_nothing_for_the_loser(self) -> None:
        # A rival consumes the nominated row between the two statements: the
        # loser's guarded UPDATE matches zero rows, returns None, writes no
        # marker — the rival's claim stands untouched.
        directory = self.start_attempt("run-a", 1, session_id="sess-a")
        real_db_rows = attempts.db_rows

        def racing_db_rows(url, sql, params=None, **kwargs):
            rows = real_db_rows(url, sql, params, **kwargs)
            if "WITH RECURSIVE" in sql and rows:
                real_db_rows(
                    url,
                    "UPDATE pipeline_attempts SET consumed_by_run_id = %s,"
                    " consumed_by_attempt = %s, consumed_at = now()"
                    " WHERE id = %s AND consumed_by_run_id IS NULL",
                    ["run-rival", 7, rows[0][0]],
                )
            return rows

        with mock.patch.object(attempts, "db_rows", racing_db_rows):
            self.assertIsNone(self.claim("run-loser", 1))
        (row,) = self.rows()
        self.assertEqual(row[3:5], ("run-rival", 7))
        self.assertFalse((directory / "resume_consumed.json").exists())

    def test_locality_guard_leaves_the_row_for_a_machine_with_the_files(self) -> None:
        # Sandbox mode (GTM_DATA_ROOT set): a missing attempt dir means the
        # transcript is elsewhere — nominate, verify, walk away unconsumed.
        with mock.patch.dict(os.environ, {"GTM_DATA_ROOT": str(self.tmp)}):
            directory = self.start_attempt("run-a", 1, session_id="sess-a")
            stored = _scratch_rows("SELECT attempt_dir FROM pipeline_attempts")[0][0]
            self.assertFalse(
                stored.startswith("/"), "attempt_dir must be data-root relative"
            )
            shutil.rmtree(directory)
            self.assertIsNone(self.claim("run-b", 1))
            (row,) = self.rows()
            self.assertIsNone(row[3], "locality guard must not burn the claim")
            directory.mkdir(parents=True)
            claimed = self.claim("run-b", 2)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed[1], directory)

    # -- unconsume ---------------------------------------------------------

    def test_unconsume_is_owner_guarded_and_reopens_the_claim(self) -> None:
        directory = self.start_attempt("run-a", 1, session_id="sess-a")
        _, resumed_dir, candidate_id = self.claim("run-b", 2)
        attempts.unconsume_attempt(
            self.args, self.job, candidate_id, "run-imposter", 1, resumed_dir
        )
        (row,) = self.rows()
        self.assertEqual(row[3:5], ("run-b", 2), "only the consuming owner may release")
        attempts.unconsume_attempt(
            self.args, self.job, candidate_id, "run-b", 2, resumed_dir
        )
        (row,) = self.rows()
        self.assertEqual(row[3:6], (None, None, None))
        self.assertFalse((directory / "resume_consumed.json").exists())
        claimed = self.claim("run-c", 1)
        self.assertIsNotNone(claimed, "a released session is claimable again")
        self.assertEqual(claimed[0], "sess-a")


if __name__ == "__main__":
    unittest.main()

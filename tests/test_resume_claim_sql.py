#!/usr/bin/env python3
"""Resume-claim SQL against a scratch database (design-doc step-0 gate item).

Runs the REAL attempts statements in agent_runner/attempts.py —
record_attempt_start / record_attempt_session / record_attempt_outcome /
claim_resumable_attempt / unconsume_attempt — against a live Postgres, in a
scratch database this class builds from the REAL db/migrations DDL and drops
again. GTM's tests/test_attempt_paths.py covers the same claim logic with
`db_rows` mocked; this file is the one place the nomination SELECT, the
verify-before-consume UPDATE, and the budget arithmetic execute for real.

Step-9 retype: the target is the runner's own `attempts` table, not the
compat-bridge pipeline_attempts. Two things changed shape and both are
pinned below — the resume chain is one self-FK (consumed_by_attempt_id)
instead of the (consumed_by_run_id, consumed_by_attempt) pair, and an
attempt is identified by its ROW ID, so the engine records the attempt
before it claims (migration 007). The attempt ORDINAL repeats across runs
of one job and keys nothing; test_a_force_rerun_ordinal_collision_keeps_both
_rows is the regression for the schema that assumed otherwise.

The migration files are executed directly rather than through
migrations.apply_pending: this file needs no ledger and no runner_emitter
role, so its environment contract stays psycopg + a Postgres the connecting
role can CREATE DATABASE on (the .local instance on 55432, or
GTM_TEST_DATABASE_URL), skipping cleanly otherwise so the stdlib-only suite
stays green.
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
_os.environ.setdefault("RUNNER_PROJECT_ID", "testproj")
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import attempts, migrations  # noqa: E402
from agent_runner.runtime import RunnerJob  # noqa: E402

TEST_URL = os.environ.get(
    "GTM_TEST_DATABASE_URL",
    f"postgres://{getpass.getuser()}@127.0.0.1:55432/uflo_gtm_production",
)

SCRATCH_DB = "gtm_test_resume_claim_scratch"

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
    """The attempts statements, executed for real."""

    @classmethod
    def setUpClass(cls) -> None:
        _admin_exec(f"DROP DATABASE IF EXISTS {SCRATCH_DB} WITH (FORCE)")
        _admin_exec(f"CREATE DATABASE {SCRATCH_DB}")
        # The real migration DDL, not a hand-copied schema: drift between the
        # migrations and the claim SQL fails here first.
        for path in migrations.migration_paths():
            _scratch_rows(path.read_text())
        # 001 seeds no tenant (the runner refuses to guess one); register the
        # test tenant the way a run start does (jobstore.ensure_project).
        _scratch_rows(
            "INSERT INTO projects (project_id, name)"
            " VALUES ('testproj', 'testproj') ON CONFLICT DO NOTHING"
        )

    @classmethod
    def tearDownClass(cls) -> None:
        _admin_exec(f"DROP DATABASE IF EXISTS {SCRATCH_DB} WITH (FORCE)")

    def setUp(self) -> None:
        _scratch_rows("TRUNCATE attempts RESTART IDENTITY")
        self.args = argparse.Namespace(database_url=SCRATCH_URL)
        self.job = make_job()
        self.tmp = Path(tempfile.mkdtemp(prefix="gtm_resume_claim_")).resolve()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    # -- helpers ----------------------------------------------------------

    def start(
        self,
        run_id: str,
        attempt: int,
        *,
        fingerprint: str = FP,
        session_id: str | None = None,
        job: RunnerJob | None = None,
    ) -> tuple[Path, int]:
        """Record one attempt launch; returns its workspace and its row id."""
        job = job or self.job
        directory = self.tmp / run_id / f"attempt-{attempt:02d}"
        attempt_id = attempts.record_attempt_start(
            self.args, job, run_id, attempt, fingerprint, directory
        )
        self.assertIsNotNone(attempt_id, "record_attempt_start must return a row id")
        if session_id is not None:
            attempts.record_attempt_session(self.args, job, attempt_id, session_id)
        return directory, attempt_id

    def claim(
        self, attempt_id: int | None, run_id: str, attempt: int, *, fingerprint: str = FP
    ):
        return attempts.claim_resumable_attempt(
            self.args, self.job, attempt_id, fingerprint, run_id, attempt
        )

    def resume(self, run_id: str, attempt: int, *, fingerprint: str = FP):
        """The engine's order: record THIS attempt, then claim a candidate.
        Returns (claim result, this attempt's row id)."""
        _, attempt_id = self.start(run_id, attempt, fingerprint=fingerprint)
        return (
            self.claim(attempt_id, run_id, attempt, fingerprint=fingerprint),
            attempt_id,
        )

    def rows(self) -> list[tuple]:
        return _scratch_rows(
            "SELECT id, lease_ref, attempt, session_ref, consumed_by_attempt_id,"
            " resume_depth FROM attempts ORDER BY id"
        )

    def row(self, attempt_id: int) -> tuple:
        return _scratch_rows(
            "SELECT id, lease_ref, attempt, session_ref, consumed_by_attempt_id,"
            " resume_depth FROM attempts WHERE id = %s",
            (attempt_id,),
        )[0]

    # -- record statements ------------------------------------------------

    def test_record_start_then_claim_round_trip(self) -> None:
        directory, candidate = self.start("run-a", 1, session_id="sess-a")
        claimed, claimer = self.resume("run-b", 1)
        self.assertIsNotNone(claimed)
        session_id, resumed_dir, candidate_id = claimed
        self.assertEqual(session_id, "sess-a")
        self.assertEqual(resumed_dir, directory)
        self.assertEqual(candidate_id, candidate)
        # The chain is one self-FK, and the consumer's depth is precomputed.
        self.assertEqual(self.row(candidate)[4], claimer)
        self.assertEqual(self.row(claimer)[5], 1)
        # Filesystem mirror of the DB consumption.
        self.assertTrue((directory / "resume_consumed.json").exists())

    def test_record_start_never_upserts_a_second_launch(self) -> None:
        # Two launches are two rows even with one ordinal: an attempt IS its
        # row (007). The pre-007 upsert overwrote the earlier row here.
        directory, first = self.start("run-a", 1, session_id="sess-a")
        attempts.record_attempt_start(self.args, self.job, "run-a", 1, OTHER_FP, directory)
        fingerprints = _scratch_rows(
            "SELECT prompt_fingerprint FROM attempts ORDER BY id"
        )
        self.assertEqual(fingerprints, [(FP,), (OTHER_FP,)])
        self.assertEqual(self.row(first)[3], "sess-a", "the earlier session survives")

    def test_a_force_rerun_ordinal_collision_keeps_both_rows(self) -> None:
        # The regression for the schema flaw 007 fixes. --force-rerun resets
        # jobs.attempt_count, so the next run launches attempt 1 again. Under
        # UNIQUE (project_id, job_key, attempt) that second launch clobbered
        # the first row — including the session ref resume exists to find.
        _, first = self.start("run-a", 1, session_id="sess-a")
        attempts.record_attempt_outcome(self.args, self.job, first, "failed", "timeout")
        claimed, second = self.resume("run-b", 1)  # same ordinal, new run
        self.assertNotEqual(first, second, "two launches, two rows")
        self.assertIsNotNone(claimed, "the earlier run's session is still resumable")
        self.assertEqual(claimed[0], "sess-a")
        self.assertEqual(
            [(row[1], row[2]) for row in self.rows()],
            [("run-a", 1), ("run-b", 1)],
            "history keeps both launches of ordinal 1",
        )

    def test_session_ref_first_write_wins(self) -> None:
        _, attempt_id = self.start("run-a", 1, session_id="sess-first")
        attempts.record_attempt_session(self.args, self.job, attempt_id, "sess-late")
        self.assertEqual(self.row(attempt_id)[3], "sess-first")

    def test_failed_attempt_is_still_resumable(self) -> None:
        # 2026-07-28 resume policy: failed-in-any-category resumes too; only
        # a changed fingerprint or a spent budget blocks the claim.
        _, attempt_id = self.start("run-a", 1, session_id="sess-a")
        attempts.record_attempt_outcome(
            self.args, self.job, attempt_id, "failed", "agent_error"
        )
        self.assertEqual(
            _scratch_rows(
                "SELECT outcome, error_code, finished_at IS NOT NULL"
                " FROM attempts WHERE id = %s",
                (attempt_id,),
            ),
            [("failed", "agent_error", True)],
        )
        claimed, _ = self.resume("run-b", 1)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed[0], "sess-a")

    def test_outcome_closes_only_its_own_row(self) -> None:
        _, first = self.start("run-a", 1, session_id="sess-a")
        _, second = self.start("run-b", 1, session_id="sess-b")
        attempts.record_attempt_outcome(self.args, self.job, second, "succeeded")
        self.assertEqual(
            _scratch_rows("SELECT id, outcome FROM attempts ORDER BY id"),
            [(first, None), (second, "succeeded")],
        )

    # -- nomination filters -----------------------------------------------

    def test_claim_skips_wrong_fingerprint_missing_session_and_other_backend(self) -> None:
        self.start("run-a", 1, fingerprint=OTHER_FP, session_id="sess-other-fp")
        self.start("run-a", 2)  # session never recorded
        codex_job = make_job(backend="codex", key=self.job.key)  # same job, other backend
        self.start("run-a", 3, session_id="sess-codex", job=codex_job)
        claimed, _ = self.resume("run-b", 1)
        self.assertIsNone(claimed)
        for row in self.rows():
            self.assertIsNone(row[4], f"claim consumed a non-candidate row: {row}")

    def test_claim_takes_the_newest_candidate(self) -> None:
        _, old = self.start("run-a", 1, session_id="sess-old")
        _, new = self.start("run-a", 2, session_id="sess-new")
        claimed, claimer = self.resume("run-b", 1)
        self.assertEqual(claimed[0], "sess-new")
        self.assertIsNone(self.row(old)[4], "older candidate left for a later claim")
        self.assertEqual(self.row(new)[4], claimer)

    def test_claim_never_nominates_the_claiming_attempt_itself(self) -> None:
        # The claimer's own row is inserted first now, so the nomination must
        # exclude it. (It carries no session ref yet either — belt and braces.)
        _, claimer = self.start("run-a", 1)
        attempts.record_attempt_session(self.args, self.job, claimer, "sess-self")
        self.assertIsNone(self.claim(claimer, "run-a", 1))
        self.assertIsNone(self.row(claimer)[4])

    def test_an_untracked_attempt_cannot_claim(self) -> None:
        # record_attempt_start's insert failed: there is no row for the chain
        # to point at, so the attempt runs fresh rather than ending a lineage.
        self.start("run-a", 1, session_id="sess-a")
        self.assertIsNone(self.claim(None, "run-b", 1))
        self.assertIsNone(self.rows()[0][4], "nothing was consumed")

    def test_consumed_row_is_never_reclaimed(self) -> None:
        self.start("run-a", 1, session_id="sess-a")
        self.assertIsNotNone(self.resume("run-b", 1)[0])
        self.assertIsNone(self.resume("run-c", 1)[0])

    # -- budget (the precomputed chain depth) -------------------------------

    def test_budget_counts_the_session_chain_not_the_job(self) -> None:
        # s0 resumed three times through the consumption chain; the fourth
        # resume of that lineage is refused, but a brand-new session on the
        # same job claims fine (R7: a job is never permanently unresumable).
        self.start("run-0", 1, session_id="sess-0")
        for n in (1, 2, 3):
            claimed, claimer = self.resume(f"run-{n}", 1)
            self.assertIsNotNone(claimed, f"resume {n} of the chain should be allowed")
            self.assertEqual(claimed[0], f"sess-{n - 1}")
            self.assertEqual(self.row(claimer)[5], n, "depth is precomputed at claim")
            attempts.record_attempt_session(self.args, self.job, claimer, f"sess-{n}")
        self.assertIsNone(
            self.resume("run-4", 1)[0], "chain of RESUME_BUDGET consumptions must stop"
        )
        self.start("run-fresh", 1, session_id="sess-fresh")
        claimed, _ = self.resume("run-5", 1)
        self.assertIsNotNone(claimed, "a fresh session starts with a full budget")
        self.assertEqual(claimed[0], "sess-fresh")

    # -- verify-before-consume --------------------------------------------

    def test_lost_race_consumes_nothing_for_the_loser(self) -> None:
        # A rival consumes the nominated row between the two statements: the
        # loser's guarded UPDATE matches zero rows, returns None, writes no
        # marker — the rival's claim stands untouched.
        directory, candidate = self.start("run-a", 1, session_id="sess-a")
        _, rival = self.start("run-rival", 7)
        real_db_rows = attempts.db_rows

        def racing_db_rows(url, sql, params=None, **kwargs):
            rows = real_db_rows(url, sql, params, **kwargs)
            if "ORDER BY id DESC LIMIT 1" in sql and rows:
                real_db_rows(
                    url,
                    "UPDATE attempts SET consumed_by_attempt_id = %s"
                    " WHERE id = %s AND consumed_by_attempt_id IS NULL",
                    [rival, rows[0][0]],
                )
            return rows

        with mock.patch.object(attempts, "db_rows", racing_db_rows):
            claimed, loser = self.resume("run-loser", 1)
        self.assertIsNone(claimed)
        self.assertEqual(self.row(candidate)[4], rival)
        self.assertEqual(self.row(loser)[5], 0, "the loser's depth is never stamped")
        self.assertFalse((directory / "resume_consumed.json").exists())

    def test_locality_guard_leaves_the_row_for_a_machine_with_the_files(self) -> None:
        # Sandbox mode (GTM_DATA_ROOT set): a missing attempt dir means the
        # transcript is elsewhere — nominate, verify, walk away unconsumed.
        with mock.patch.dict(os.environ, {"GTM_DATA_ROOT": str(self.tmp)}):
            directory, candidate = self.start("run-a", 1, session_id="sess-a")
            stored = _scratch_rows(
                "SELECT workspace_ref FROM attempts WHERE id = %s", (candidate,)
            )[0][0]
            self.assertFalse(
                stored.startswith("/"), "workspace_ref must be data-root relative"
            )
            shutil.rmtree(directory)
            self.assertIsNone(self.resume("run-b", 1)[0])
            self.assertIsNone(
                self.row(candidate)[4], "locality guard must not burn the claim"
            )
            directory.mkdir(parents=True)
            claimed, _ = self.resume("run-b", 2)
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed[1], directory)

    # -- unconsume ---------------------------------------------------------

    def test_unconsume_is_owner_guarded_and_reopens_the_claim(self) -> None:
        directory, candidate = self.start("run-a", 1, session_id="sess-a")
        claimed, owner = self.resume("run-b", 2)
        _, resumed_dir, candidate_id = claimed
        self.assertEqual(candidate_id, candidate)
        _, imposter = self.start("run-imposter", 1)

        attempts.unconsume_attempt(self.args, self.job, candidate_id, imposter, resumed_dir)
        self.assertEqual(
            self.row(candidate)[4], owner, "only the consuming owner may release"
        )

        attempts.unconsume_attempt(self.args, self.job, candidate_id, owner, resumed_dir)
        self.assertIsNone(self.row(candidate)[4])
        self.assertEqual(self.row(owner)[5], 0, "release rolls the depth stamp back")
        self.assertFalse((directory / "resume_consumed.json").exists())

        claimed, _ = self.resume("run-c", 1)
        self.assertIsNotNone(claimed, "a released session is claimable again")
        self.assertEqual(claimed[0], "sess-a")


if __name__ == "__main__":
    unittest.main()

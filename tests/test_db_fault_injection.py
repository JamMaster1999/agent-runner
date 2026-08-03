#!/usr/bin/env python3
"""Bookkeeping writes under a genuinely stalled database (2026-08-03 incident).

The production failure these tests pin: the first day on an undersized
PlanetScale instance, statements stalled past their 60s budget and every
bookkeeping path treated its own write failure as evidence about the JOB —
healthy work went terminally 'blocked' on attempt 1 (the event append's
``retryable=False`` wrap), claim timeouts stranded rows 'queued' forever, and
success-record stalls were alerted as batch failures.

Nothing here mocks the failure. The stall is real: a second connection holds
``ACCESS EXCLUSIVE`` on the table while the write under test runs with a
1-second server-side statement_timeout (the same ``timeout_conninfo`` dial
production uses, just shorter), so the code under test sees the same
QueryCanceled-wrapped OperationalError the incident produced. Scratch
database from the REAL migrations, same environment contract as
tests/test_resume_claim_sql.py: psycopg + a local Postgres (the .local
instance on 55432, or GTM_TEST_DATABASE_URL), skipping cleanly otherwise.

The engine-loop ordering properties (claim-stall retry bounds, fail-event
stall not aborting the state update) are pinned DB-free at the bottom — the
error OBJECTS those tests inject are the exact shapes the live classes above
prove the transport emits.
"""

from __future__ import annotations

import argparse
import contextlib
import getpass
import os
import sys
import threading
import time
import unittest
from pathlib import Path
from typing import Any
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

from agent_runner import engine, events, jobstore, migrations, outcomes  # noqa: E402
from agent_runner.jobstore import claim_job, ensure_job  # noqa: E402
from agent_runner.runtime import RunnerError, RunnerJob  # noqa: E402
from agent_runner.util import db_rows as real_db_rows  # noqa: E402

TEST_URL = os.environ.get(
    "GTM_TEST_DATABASE_URL",
    f"postgres://{getpass.getuser()}@127.0.0.1:55432/uflo_gtm_production",
)

SCRATCH_DB = "gtm_test_db_fault_injection_scratch"


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


def make_job(key: str = "999_stall-university__phase5_batch_001__claude") -> RunnerJob:
    return RunnerJob(
        key=key,
        group_key="999_stall-university",
        task_type="phase5",
        harness="claude",
        agent_ref="prod-phase5-instructor",
        attempt_dir_name="phase5_batch_001",
        output_filename="phase5_batch_001.json",
        canonical_relpath="results/999_stall-university/claude/phase5_batch_001.json",
    )


@contextlib.contextmanager
def hold_exclusive_lock(table: str, release_after: float | None = None):
    """A second connection holding ACCESS EXCLUSIVE on ``table`` — the real
    stall. With ``release_after`` a timer commits the blocker mid-flight,
    modeling the incident's recovery window."""
    import psycopg

    blocker = psycopg.connect(SCRATCH_URL)
    blocker.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    released = threading.Event()

    def release() -> None:
        blocker.commit()
        released.set()

    timer: threading.Timer | None = None
    if release_after is not None:
        timer = threading.Timer(release_after, release)
        timer.start()
    try:
        yield released
    finally:
        if timer is not None:
            timer.cancel()
        if not released.is_set():
            blocker.rollback()
        blocker.close()


def fast_db_rows(url: str, sql: str, params=None, *, timeout: int = 60, retry: bool = True):
    """The REAL transport with a 1s statement budget: same connection dial,
    same server-side statement_timeout mechanism, same error classification —
    only the number is smaller so a stalled test fails in seconds."""
    return real_db_rows(url, sql, params, timeout=1, retry=False)


@unittest.skipUnless(_live_db_available(), "psycopg + local Postgres required")
class DbFaultInjectionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _admin_exec(f"DROP DATABASE IF EXISTS {SCRATCH_DB} WITH (FORCE)")
        _admin_exec(f"CREATE DATABASE {SCRATCH_DB}")
        for path in migrations.migration_paths():
            _scratch_rows(path.read_text())

    @classmethod
    def tearDownClass(cls) -> None:
        _admin_exec(f"DROP DATABASE IF EXISTS {SCRATCH_DB} WITH (FORCE)")

    def setUp(self) -> None:
        _scratch_rows("TRUNCATE events, attempts, jobs RESTART IDENTITY CASCADE")
        self.job = make_job()
        ensure_job(SCRATCH_URL, self.job, max_attempts=5, force=False)

    def job_row(self) -> tuple:
        rows = _scratch_rows(
            "SELECT status, attempt_count FROM jobs WHERE job_key = %s",
            [self.job.key],
        )
        return rows[0]

    # -- the event append (the batches-021/025 'blocked' path) ---------------

    def test_stalled_lifecycle_write_raises_transient_not_terminal(self) -> None:
        with mock.patch.object(events, "DB_TIMEOUT_SECONDS", 1):
            with hold_exclusive_lock("jobs"):
                with self.assertRaises(RunnerError) as caught:
                    events.run_job_event(
                        SCRATCH_URL, "finish", self.job, "done", fatal=True
                    )
        failure = caught.exception
        # The incident shape was code='job_event_failed', retryable=False —
        # which policy_signal routed to CHAIN_TERMINAL (terminal 'blocked'
        # on attempt 1). The stall must stay transient end to end.
        self.assertEqual(failure.code, "job_event_transient")
        self.assertTrue(failure.retryable)
        self.assertEqual(
            engine.policy_signal(failure), outcomes.INFRASTRUCTURE_FAILURE
        )
        decision = engine.POLICY[engine.policy_signal(failure)]
        self.assertEqual(decision.action, "retry")
        self.assertFalse(decision.consumes_attempt)
        # And the stall changed nothing about the job.
        self.assertEqual(self.job_row(), ("queued", 0))

    def test_lifecycle_write_succeeds_once_the_stall_clears(self) -> None:
        # The retry the policy grants must actually work: same call, lock
        # gone, row flips.
        events.run_job_event(SCRATCH_URL, "finish", self.job, "done", fatal=True)
        self.assertEqual(self.job_row()[0], "succeeded")

    def test_missing_job_row_stays_terminal(self) -> None:
        # The transient reclassification must not soften the real contract
        # failure: no jobs row is proof, not weather.
        ghost = make_job(key="999_stall-university__no_such_job__claude")
        with self.assertRaises(RunnerError) as caught:
            events.run_job_event(SCRATCH_URL, "finish", ghost, "done", fatal=True)
        self.assertEqual(caught.exception.code, "job_event_failed")
        self.assertFalse(caught.exception.retryable)

    # -- the claim write (the stranded-'queued' path) ------------------------

    def test_stalled_claim_raises_db_timeout_and_strands_nothing_yet(self) -> None:
        with mock.patch.object(jobstore, "db_rows", fast_db_rows):
            with hold_exclusive_lock("jobs"):
                with self.assertRaises(RunnerError) as caught:
                    claim_job(SCRATCH_URL, self.job, "run-fault-injection")
        self.assertEqual(caught.exception.code, "db_timeout")
        self.assertTrue(caught.exception.retryable)
        # The row is untouched — still claimable. Before the engine's
        # claim-stall retry, this exception aborted the batch while the row
        # sat here 'queued' with nothing ever coming back for it.
        self.assertEqual(self.job_row(), ("queued", 0))

    def test_engine_retries_a_stalled_claim_until_the_db_recovers(self) -> None:
        """The full incident scenario, end to end: claim stalls on a real
        lock, the engine's bounded retry keeps trying, the DB recovers
        mid-run, and the job is claimed and completed instead of stranded."""
        args = argparse.Namespace(
            database_url=SCRATCH_URL,
            no_sleep=True,
            retry_backoff_seconds=[0],
        )
        attempt_result = object()

        def fake_attempt(adapter, args_, job, run_id, run_dir, attempt, **kwargs):
            return attempt_result

        with (
            mock.patch.object(jobstore, "db_rows", fast_db_rows),
            mock.patch.object(engine, "claim_job", jobstore.claim_job),
            mock.patch.object(engine, "get_adapter", lambda name: object()),
            mock.patch.object(engine, "run_agent_job_once", fake_attempt),
            # released after ~1.5s: the first 1s-budget claim try genuinely
            # times out, a later bounded retry lands.
            hold_exclusive_lock("jobs", release_after=1.5),
        ):
            result = engine.run_with_retries(
                args, self.job, "run-fault-injection", Path("/tmp/unused")
            )
        self.assertIs(result, attempt_result)
        status, attempts = self.job_row()
        self.assertEqual(status, "running")  # claimed by us; finish is facade-side
        self.assertEqual(attempts, 1)


class ClaimStallLoopTest(unittest.TestCase):
    """DB-free bounds on the claim-stall retry: the injected error object is
    the exact shape DbFaultInjectionTest proves the live transport emits."""

    def setUp(self) -> None:
        self.args = argparse.Namespace(
            database_url="postgresql://claim-stall.invalid/unused",
            no_sleep=True,
            retry_backoff_seconds=[0],
        )
        self.job = make_job()

    def stall(self) -> RunnerError:
        return RunnerError(
            "database call timed out after 1s (1 try).",
            code="db_timeout",
            retryable=True,
            alert=False,
        )

    def test_claim_stalls_past_the_cap_surface_the_stranding(self) -> None:
        calls = {"n": 0}

        def always_stalling_claim(url, job, run_id):
            calls["n"] += 1
            raise self.stall()

        with mock.patch.object(engine, "claim_job", always_stalling_claim):
            with self.assertRaises(RunnerError) as caught:
                engine.run_with_retries(
                    self.args, self.job, "run-x", Path("/tmp/unused")
                )
        self.assertEqual(calls["n"], engine.CLAIM_STALL_RETRIES + 1)
        self.assertEqual(caught.exception.code, "db_timeout")
        # The message must tell the operator the truth about the row's fate.
        self.assertIn("queued", str(caught.exception))
        self.assertIn("requeue", str(caught.exception))

    def test_non_timeout_claim_failures_do_not_burn_stall_retries(self) -> None:
        def broken_claim(url, job, run_id):
            raise RunnerError("boom", code="db_error", retryable=False, alert=True)

        with mock.patch.object(engine, "claim_job", broken_claim):
            with self.assertRaises(RunnerError) as caught:
                engine.run_with_retries(
                    self.args, self.job, "run-x", Path("/tmp/unused")
                )
        self.assertEqual(caught.exception.code, "db_error")


class FailEventStallTest(unittest.TestCase):
    """A transient stall on the FAIL audit append must not abort the state
    update that follows it (mark_retry/mark_blocked own the row's fate)."""

    def test_fail_event_stall_still_reaches_mark_retry(self) -> None:
        args = argparse.Namespace(
            database_url="postgresql://fail-stall.invalid/unused",
            no_sleep=True,
            retry_backoff_seconds=[0],
        )
        job = make_job()
        state: dict[str, Any] = {"attempt": 0, "retried": False}

        def fake_claim(url, j, run_id):
            state["attempt"] += 1
            return {"claimed": True, "attempt": state["attempt"], "max_attempts": 5}

        def failing_then_ok_attempt(adapter, a, j, run_id, run_dir, attempt, **kw):
            if attempt == 1:
                raise RunnerError("cli died", code="unknown", retryable=True)
            return "ok"

        def stalling_fail_event(url, command, j, message, **kw):
            if command == "fail":
                raise RunnerError(
                    "jobs event update failed.",
                    code="job_event_transient",
                    retryable=True,
                    alert=False,
                )

        def record_retry(url, j, message, delay, consume_attempt=True):
            state["retried"] = True

        with (
            mock.patch.object(engine, "claim_job", fake_claim),
            mock.patch.object(engine, "get_adapter", lambda name: object()),
            mock.patch.object(engine, "run_agent_job_once", failing_then_ok_attempt),
            mock.patch.object(engine, "record_attempt_outcome", lambda *a, **k: None),
            mock.patch.object(engine, "run_job_event", stalling_fail_event),
            mock.patch.object(engine, "mark_retry", record_retry),
            mock.patch.object(engine.sys, "stderr"),
        ):
            result = engine.run_with_retries(args, job, "run-x", Path("/tmp/unused"))
        self.assertEqual(result, "ok")
        # The stalled audit write was downgraded to a warning and the retry
        # state update still ran — before the guard, the stall raised out of
        # the loop and left the row 'running' for the reaper to find.
        self.assertTrue(state["retried"])


if __name__ == "__main__":
    unittest.main()

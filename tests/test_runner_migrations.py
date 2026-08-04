#!/usr/bin/env python3
"""The runner schema, applied for real onto a scratch database (step 8 gate).

Runs the REAL applier — agent_runner.migrations.apply_pending — over the
REAL db/migrations chain plus db/roles, in a scratch database this class
creates and drops, then asserts the shape that lands: the seven tables,
their exact column sets, the constraints and indexes the store code depends
on, the runner_events NOTIFY trigger end to end, and the runner_emitter
role's INSERT-only grants. Nothing here is a hand-copied schema, so drift
between the migrations and the step-9 store code fails in this file first.

Several assertions are regression tests, not shape checks:
  - No caller vocabulary in any CHECK. GTM's pipeline_jobs phase CHECK cost
    five migrations (012, 013, 019, 024, 025) whose whole content was editing
    a list of caller strings.
  - No GTM column names anywhere. The old names (stable_id, institution_id,
    run_id, session_id, attempt_dir, …) are successors, not survivors.
  - The lease acquire statement is executed for real, on the two shapes a
    second uniqueness on leases would abort.
  - Re-running db/roles repairs a revoked grant, which no ledger can see.
  - The emitter can INSERT but cannot read back its own id.

Same environment contract as tests/test_resume_claim_sql.py: needs psycopg
plus a reachable Postgres (the .local instance on 55432, or
GTM_TEST_DATABASE_URL) and skips cleanly otherwise, so the stdlib-only suite
stays green. The connecting role must be able to CREATE DATABASE and CREATE
ROLE (true for the local dev instance and the CI service container).

MigrationsTargetGuardTest at the bottom needs no database at all and runs
everywhere: it pins the two guards standing between this applier and the
client's database.
"""

from __future__ import annotations

import contextlib
import getpass
import io
import json
import os
import sys
import threading
import unittest
from pathlib import Path
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

from agent_runner import migrations  # noqa: E402

TEST_URL = os.environ.get(
    "GTM_TEST_DATABASE_URL",
    f"postgres://{getpass.getuser()}@127.0.0.1:55432/uflo_gtm_production",
)

SCRATCH_DB = "runner_schema_scratch"

# The step-8 chain, spelled out: a new migration adds a line here on purpose,
# so "the applier ran everything, in order" stays a real assertion.
EXPECTED_MIGRATIONS = [
    "001_create_projects.sql",
    "002_create_jobs.sql",
    "003_create_attempts.sql",
    "004_create_events.sql",
    "005_create_leases.sql",
    "006_create_accounts.sql",
    "007_attempts_row_identity.sql",
]

# db/roles is NOT part of the chain and never reaches the ledger: CREATE ROLE
# is cluster-global, grants drift invisibly, and only this part needs
# CREATEROLE. It re-runs on every apply, which is the repair path.
EXPECTED_ROLE_FILES = [
    "010_create_runner_emitter_role.sql",
    "020_grant_runner_emitter.sql",
]

RUNNER_TABLES = [
    "projects",
    "jobs",
    "attempts",
    "events",
    "leases",
    "accounts",
    "account_usage",
]

# The runner database carries no GTM tables — neither business tables nor the
# pipeline_* names the compat bridge wrote until step 8.
FORBIDDEN_TABLES = [
    "pipeline_jobs",
    "pipeline_events",
    "pipeline_runs",
    "pipeline_attempts",
    "run_requests",
    "institutions",
    "instructors",
    "departments",
]

# Exact column sets (equality, not subset): a stray column fails just as loudly
# as a missing one.
EXPECTED_COLUMNS = {
    "jobs": {
        "id", "project_id", "job_key", "group_key",
        "task_type", "harness", "agent_ref", "labels",
        "prompt_ref", "request_identity", "artifact_contract", "probe_spec",
        "resource_specs", "required_env", "policy",
        "status", "attempt_count", "max_attempts", "next_retry_at",
        "progress_current", "progress_total", "progress_message",
        "progress_updated_at",
        "claimed_by", "claimed_at", "lease_ref", "account_id",
        "error_code", "outcome_code", "error_message", "error_details",
        "started_at", "finished_at", "heartbeat_at", "created_at", "updated_at",
    },
    "attempts": {
        "id", "project_id", "job_key", "attempt",
        "harness", "account_id", "lease_ref",
        "request_identity", "prompt_fingerprint", "session_ref", "workspace_ref",
        "outcome", "error_code", "outcome_code",
        "consumed_by_attempt_id", "resume_depth",
        "tok_input", "tok_cache_write", "tok_cache_read", "tok_output",
        "cost_usd", "created_at", "finished_at",
    },
    "events": {
        "id", "project_id", "job_key", "group_key", "lease_ref", "attempt",
        "harness", "task_type", "account_id",
        "kind", "message", "progress_current", "progress_total",
        "tok_input", "tok_cache_write", "tok_cache_read", "tok_output",
        "cost_usd", "data", "at",
    },
    "leases": {
        "id", "project_id", "lease_ref", "lease_key", "holder", "status",
        "kind", "labels", "outcome", "error_code", "error_message",
        "started_at", "heartbeat_at", "finished_at",
    },
    "accounts": {
        "id", "project_id", "harness", "label", "secret_ref",
        "status", "disabled_reason", "concurrent_cap", "cooldown_until",
        "last_used_at", "created_at",
    },
    # The rollup mirrors its two sources' token columns exactly — attempts
    # and events both carry all four, and cache reads are most of the volume.
    "account_usage": {
        "account_id", "project_id", "window_start", "requests",
        "tok_input", "tok_cache_write", "tok_cache_read", "tok_output",
        "cost_usd",
    },
}

# GTM column names that must not have survived the rename. Each has a
# successor: run_id -> lease_ref, session_id -> session_ref, attempt_dir ->
# workspace_ref, failure_category -> error_code, phase -> task_type, backend
# -> harness, output_path -> artifact_contract, stable_id -> job_key.
FORBIDDEN_COLUMNS = [
    "stable_id", "institution_id", "school_id", "department_id", "term_id",
    "phase", "backend", "unit_type", "unit_key", "input_path", "output_path",
    "agent_name", "session_id", "attempt_dir", "failure_category",
    "consumed_by_run_id", "run_id",
]

# Caller vocabulary that must never reach a CHECK constraint.
FORBIDDEN_CHECK_WORDS = ["phase1", "phase5", "institution", "school", "department", "term"]

# Cross-reference columns that stay opaque text. The self-FK on the resume
# chain is the one sanctioned foreign key among them.
REF_COLUMNS = {
    "job_key", "group_key", "lease_ref", "session_ref", "workspace_ref",
    "consumed_by_attempt_id",
}

# Tables the emitter role must not touch at all.
OFF_LIMITS_TABLES = ["jobs", "attempts", "leases", "projects", "accounts"]


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


def _rows(url: str, sql: str, params=None) -> list[tuple]:
    import psycopg

    with psycopg.connect(url, autocommit=True) as conn:
        cursor = conn.execute(sql, params)
        return cursor.fetchall() if cursor.description is not None else []


def _admin_exec(sql: str) -> None:
    _rows(TEST_URL, sql)


def _scratch_rows(sql: str, params=None) -> list[tuple]:
    return _rows(SCRATCH_URL, sql, params)


def _scratch_value(sql: str, params=None):
    return _scratch_rows(sql, params)[0][0]


@unittest.skipUnless(_live_db_available(), "psycopg + local Postgres required")
class RunnerMigrationsTest(unittest.TestCase):
    """db/migrations applied by the real applier, then inspected."""

    # runner_emitter is a CLUSTER-GLOBAL object, so a dev machine may already
    # have a real one. Recorded before the chain runs; the teardown only drops
    # a role this class created.
    emitter_pre_existed = True

    @classmethod
    def setUpClass(cls) -> None:
        cls.emitter_pre_existed = bool(
            _rows(TEST_URL, "SELECT 1 FROM pg_roles WHERE rolname = 'runner_emitter'")
        )
        _admin_exec(f"DROP DATABASE IF EXISTS {SCRATCH_DB} WITH (FORCE)")
        _admin_exec(f"CREATE DATABASE {SCRATCH_DB}")
        cls.applied = migrations.apply_pending(SCRATCH_URL)

    @classmethod
    def tearDownClass(cls) -> None:
        # Database first: dropping it takes the per-database grants with it,
        # which is what lets the role drop cleanly.
        _admin_exec(f"DROP DATABASE IF EXISTS {SCRATCH_DB} WITH (FORCE)")
        if not cls.emitter_pre_existed:
            _admin_exec("DROP ROLE IF EXISTS runner_emitter")

    # -- helpers ----------------------------------------------------------

    def public_tables(self) -> set[str]:
        return {
            row[0]
            for row in _scratch_rows(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = 'public'"
            )
        }

    def columns_of(self, table: str) -> set[str]:
        return {
            row[0]
            for row in _scratch_rows(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            )
        }

    def foreign_keys(self) -> list[tuple[str, tuple[str, ...], str]]:
        """(table, columns, referenced table) for every public-schema FK."""
        rows = _scratch_rows(
            "SELECT c.conrelid::regclass::text,"
            "       (SELECT array_agg(a.attname ORDER BY k.ord)"
            "          FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)"
            "          JOIN pg_attribute a"
            "            ON a.attrelid = c.conrelid AND a.attnum = k.attnum),"
            "       c.confrelid::regclass::text"
            " FROM pg_constraint c"
            " WHERE c.contype = 'f' AND c.connamespace = 'public'::regnamespace"
        )
        return [(row[0], tuple(row[1]), row[2]) for row in rows]

    def index_defs(self) -> dict[str, str]:
        return {
            row[0]: row[1]
            for row in _scratch_rows(
                "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'"
            )
        }

    # -- the applier -------------------------------------------------------

    def test_chain_applies_in_order_and_is_idempotent(self) -> None:
        self.assertEqual(self.applied, EXPECTED_MIGRATIONS)
        self.assertEqual(
            self.applied,
            [path.name for path in migrations.migration_paths()],
            "the applier must run every file in db/migrations, in filename order",
        )
        # The ledger, not the schema, is what makes a second run a no-op.
        self.assertEqual(migrations.apply_pending(SCRATCH_URL), [])
        # …and the files themselves are re-runnable, ledger or no ledger.
        for path in migrations.migration_paths():
            _scratch_rows(path.read_text())
        self.assertEqual(migrations.apply_pending(SCRATCH_URL), [])

    def test_ledger_rows_carry_the_runner_prefix(self) -> None:
        keys = [
            row[0]
            for row in _scratch_rows(
                "SELECT filename FROM schema_migrations ORDER BY filename"
            )
        ]
        self.assertEqual(
            keys, ["runner-migrations/" + name for name in EXPECTED_MIGRATIONS]
        )

    def test_role_files_run_but_never_reach_the_ledger(self) -> None:
        # Role provisioning is cluster-global and grant drift is invisible to
        # a ledger, so db/roles is applied every run and recorded nowhere.
        self.assertEqual(
            [path.name for path in migrations.role_paths()], EXPECTED_ROLE_FILES
        )
        ledger = {
            row[0]
            for row in _scratch_rows("SELECT filename FROM schema_migrations")
        }
        for name in EXPECTED_ROLE_FILES:
            self.assertNotIn("runner-migrations/" + name, ledger)
            self.assertNotIn(name, ledger)

    def test_reapplying_roles_repairs_a_revoked_grant(self) -> None:
        # The drift a ledger cannot see: one REVOKE and every emit fails,
        # while `migrate` keeps saying "No pending migrations." Re-running
        # db/roles IS the repair, so it must not need the ledger's blessing.
        _scratch_rows("REVOKE INSERT ON events FROM runner_emitter")
        self.assertFalse(
            _scratch_value(
                "SELECT has_table_privilege('runner_emitter', 'events', 'INSERT')"
            )
        )
        self.assertEqual(migrations.apply_pending(SCRATCH_URL), [])  # nothing pending
        self.assertTrue(
            _scratch_value(
                "SELECT has_table_privilege('runner_emitter', 'events', 'INSERT')"
            ),
            "a plain migrate must re-apply db/roles and restore the grant",
        )
        # …and the narrow repair invocation does the same on its own.
        _scratch_rows("REVOKE INSERT ON events FROM runner_emitter")
        migrations.apply_pending(SCRATCH_URL, roles_only=True)
        self.assertTrue(
            _scratch_value(
                "SELECT has_table_privilege('runner_emitter', 'events', 'INSERT')"
            ),
            "--roles-only must restore the grant without touching the chain",
        )

    # -- tables and columns ------------------------------------------------

    def test_runner_tables_present_and_gtm_tables_absent(self) -> None:
        tables = self.public_tables()
        for name in RUNNER_TABLES:
            self.assertIn(name, tables)
        for name in FORBIDDEN_TABLES:
            self.assertNotIn(
                name, tables, f"{name} is a GTM table; the runner DB carries none"
            )

    def test_column_sets_are_exact(self) -> None:
        for table, expected in EXPECTED_COLUMNS.items():
            with self.subTest(table=table):
                self.assertEqual(self.columns_of(table), expected)

    def test_no_gtm_column_names_survive_anywhere(self) -> None:
        present = {
            (row[0], row[1])
            for row in _scratch_rows(
                "SELECT table_name, column_name FROM information_schema.columns"
                " WHERE table_schema = 'public' AND column_name = ANY(%s)",
                (FORBIDDEN_COLUMNS,),
            )
        }
        self.assertEqual(present, set(), f"GTM column names survived: {present}")

    # -- constraints and indexes -------------------------------------------

    def test_identity_uniqueness(self) -> None:
        unique = {
            (row[0], row[1])
            for row in _scratch_rows(
                "SELECT conrelid::regclass::text, pg_get_constraintdef(oid)"
                " FROM pg_constraint"
                " WHERE contype = 'u' AND connamespace = 'public'::regnamespace"
            )
        }
        self.assertIn(("jobs", "UNIQUE (project_id, job_key)"), unique)
        # attempts is the exception: 007 dropped the (project, job, attempt)
        # uniqueness because attempt numbers repeat across runs of one job
        # (requeue and --force-rerun both reset the counter). The row id is
        # the identity; the ordinal keeps only a lookup index.
        self.assertNotIn(("attempts", "UNIQUE (project_id, job_key, attempt)"), unique)
        self.assertEqual(
            [table for table, _ in unique if table == "attempts"],
            [],
            "an attempt is identified by its row id, never by its ordinal",
        )
        self.assertTrue(
            any(
                " ON public.attempts " in definition
                and definition.endswith("(project_id, job_key, attempt)")
                and not definition.startswith("CREATE UNIQUE INDEX")
                for definition in self.index_defs().values()
            ),
            "the attempts-of-this-job lookup keeps a non-unique index",
        )

    def test_lock_and_scan_indexes(self) -> None:
        defs = self.index_defs()
        held = [
            name
            for name, definition in defs.items()
            if " ON public.leases " in definition
            and definition.startswith("CREATE UNIQUE INDEX")
            and "WHERE (status = 'held'::text)" in definition
        ]
        self.assertEqual(
            len(held), 1, f"exactly one partial unique index is THE lease lock: {defs}"
        )
        for table in ("jobs", "leases"):
            with self.subTest(table=table):
                self.assertTrue(
                    any(
                        f" ON public.{table} " in definition
                        and "(status, heartbeat_at)" in definition
                        for definition in defs.values()
                    ),
                    f"the stale-holder predicate scans {table} (status, heartbeat_at)",
                )
        self.assertTrue(
            any(
                " ON public.events " in definition and definition.endswith("(at)")
                for definition in defs.values()
            ),
            "the retention DELETE scans events (at)",
        )

    def test_leases_carries_no_second_uniqueness(self) -> None:
        # THE lock is the partial unique index; any other unique constraint or
        # index on leases is a different constraint, which ON CONFLICT
        # (project_id, lease_key) cannot absorb — see the acquire test below.
        constraints = _scratch_rows(
            "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint"
            " WHERE contype IN ('u', 'p') AND conrelid = 'leases'::regclass"
        )
        self.assertEqual(
            [name for name, definition in constraints if not definition.startswith("PRIMARY KEY")],
            [],
            f"leases has a unique CONSTRAINT beside its primary key: {constraints}",
        )
        unique_indexes = [
            name
            for name, definition in self.index_defs().items()
            if " ON public.leases " in definition
            and definition.startswith("CREATE UNIQUE INDEX")
        ]
        self.assertEqual(
            sorted(unique_indexes), ["leases_one_held_per_key", "leases_pkey"]
        )

    def test_lease_acquire_statement_runs_on_both_repeat_shapes(self) -> None:
        # The real acquire statement, not a paraphrase. A global UNIQUE on
        # lease_ref makes both of these raise and abort the caller's
        # transaction instead of returning no row.
        acquire = (
            "INSERT INTO leases (project_id, lease_ref, lease_key, holder, kind)"
            " VALUES ('gtm', %s, %s, %s, %s)"
            " ON CONFLICT (project_id, lease_key) WHERE status = 'held'"
            " DO NOTHING RETURNING id"
        )
        held = _scratch_rows(acquire, ("run-1", "acq-A", "host:1", "exclusive"))
        self.assertEqual(len(held), 1)

        # (a) the same holder taking a SECOND lease_key under one handle —
        # what kind='tracked_task' does many times per run.
        second = _scratch_rows(acquire, ("run-1", "acq-B", "host:1", "tracked_task"))
        self.assertEqual(len(second), 1, "one holder must be able to hold two keys")

        # A genuine collision still reports itself as no-row, not an error.
        self.assertEqual(
            _scratch_rows(acquire, ("run-2", "acq-A", "host:2", "exclusive")),
            [],
            "a held lease_key must yield DO NOTHING, not a raised constraint",
        )

        # (b) re-acquiring the same key with the same handle after release.
        _scratch_rows(
            "UPDATE leases SET status = 'released', finished_at = now()"
            " WHERE lease_key = 'acq-A'"
        )
        again = _scratch_rows(acquire, ("run-1", "acq-A", "host:1", "exclusive"))
        self.assertEqual(len(again), 1, "a released key must be re-acquirable")

    def test_a_tracked_task_can_record_its_terminal_result(self) -> None:
        # D9(a) is claim-dedupe + heartbeat + TERMINAL RECORD; the third one
        # needs columns, and status is not it (release is release).
        _scratch_rows(
            "INSERT INTO leases (project_id, lease_ref, lease_key, holder, kind)"
            " VALUES ('gtm', 'run-term', 'term-key', 'host:9', 'tracked_task')"
        )
        _scratch_rows(
            "UPDATE leases SET status = 'released', finished_at = now(),"
            " outcome = 'failed', error_code = 'invalid_invocation',"
            " error_message = 'import rejected the packet'"
            " WHERE lease_key = 'term-key'"
        )
        self.assertEqual(
            _scratch_rows(
                "SELECT status, outcome, error_code FROM leases WHERE lease_key = 'term-key'"
            ),
            [("released", "failed", "invalid_invocation")],
        )
        with self.assertRaises(Exception):
            # Caller verdicts stay opaque and out of this CHECK.
            _scratch_rows(
                "UPDATE leases SET outcome = 'succeeded_with_failures'"
                " WHERE lease_key = 'term-key'"
            )

    def test_submit_columns_the_design_requires_have_no_defaults(self) -> None:
        # NOT NULL with a DEFAULT '{}' would accept a job with no output
        # contract and no retry policy while satisfying every constraint.
        defaults = dict(
            _scratch_rows(
                "SELECT column_name, column_default FROM information_schema.columns"
                " WHERE table_schema = 'public' AND table_name = 'jobs'"
                " AND column_name IN ('artifact_contract', 'policy')"
            )
        )
        self.assertEqual(defaults, {"artifact_contract": None, "policy": None})
        with self.assertRaises(Exception):
            _scratch_rows(
                "INSERT INTO jobs (project_id, job_key, group_key, task_type,"
                " harness, agent_ref)"
                " VALUES ('gtm', 'no-contract', 'g', 't', 'h', '{}'::jsonb)"
            )

    def test_events_project_id_has_the_single_tenant_default(self) -> None:
        # The emitter role holds no SELECT, so an emit can never derive the
        # one NOT NULL routing column. Without a default every insert fails.
        (event_id,) = _scratch_rows(
            "INSERT INTO events (job_key, kind) VALUES ('defaulted', 'job.start')"
            " RETURNING id"
        )[0]
        self.assertEqual(
            _scratch_rows("SELECT project_id FROM events WHERE id = %s", (event_id,)),
            [("gtm",)],
        )

    def test_no_caller_vocabulary_in_check_constraints(self) -> None:
        # The regression test for GTM's five phase-CHECK churn migrations.
        checks = _scratch_rows(
            "SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid)"
            " FROM pg_constraint"
            " WHERE contype = 'c' AND connamespace = 'public'::regnamespace"
        )
        for table, name, definition in checks:
            for word in FORBIDDEN_CHECK_WORDS:
                self.assertNotIn(
                    word,
                    definition.lower(),
                    f"caller vocabulary '{word}' in CHECK {table}.{name}: {definition}",
                )

    def test_the_resume_chain_is_the_only_foreign_key_between_refs(self) -> None:
        keys = self.foreign_keys()
        self.assertEqual(
            [key for key in keys if key[0] == "events"],
            [],
            "events takes no foreign key: the INSERT-only emitter cannot read a parent row",
        )
        ref_keys = {
            (table, column)
            for table, columns, _ in keys
            for column in columns
            if column in REF_COLUMNS
        }
        self.assertEqual(ref_keys, {("attempts", "consumed_by_attempt_id")})
        for table, column in (("jobs", "lease_ref"), ("attempts", "job_key")):
            self.assertNotIn((table, column), ref_keys)

    # -- the NOTIFY trigger ------------------------------------------------

    def test_event_insert_notifies_runner_events(self) -> None:
        import psycopg

        listener = psycopg.connect(SCRATCH_URL, autocommit=True)
        self.addCleanup(listener.close)
        listener.execute("LISTEN runner_events")

        caught: list = []

        def collect() -> None:
            # A daemon thread rather than notifies(timeout=...): the keyword
            # arrived in psycopg 3.2 and the package floor is 3.1. On the
            # timeout path the cleanup closes the connection under this
            # generator, which is the only thing that ends it.
            try:
                for note in listener.notifies():
                    caught.append(note)
                    break
            except Exception:
                pass

        # Started BEFORE the insert so nothing else on this connection can
        # consume the notification first.
        worker = threading.Thread(target=collect, daemon=True)
        worker.start()

        (event_id,) = _scratch_rows(
            "INSERT INTO events (project_id, job_key, group_key, kind, message)"
            " VALUES ('gtm', 'scratch__job', 'scratch-group', 'job.start', 'hello')"
            " RETURNING id"
        )[0]
        worker.join(timeout=10)

        self.assertTrue(caught, "no runner_events notification within 10s")
        note = caught[0]
        self.assertEqual(note.channel, "runner_events")
        payload = json.loads(note.payload)
        self.assertEqual(
            set(payload), {"id", "project_id", "job_key", "group_key", "kind"}
        )
        self.assertEqual(payload["id"], event_id)
        self.assertEqual(payload["project_id"], "gtm")
        self.assertEqual(payload["group_key"], "scratch-group")
        self.assertEqual(payload["kind"], "job.start")
        # job_key is what lets a listener tell whether the frame belongs to
        # the job whose event pane is open; group_key answers only routing.
        self.assertEqual(payload["job_key"], "scratch__job")

    # -- the emitter role --------------------------------------------------

    def test_runner_emitter_can_only_append_events(self) -> None:
        # Privilege lookups, not a login: the role is created with no password
        # on purpose (provisioned out of band at step 9).
        self.assertTrue(
            _scratch_value("SELECT has_table_privilege('runner_emitter', 'events', 'INSERT')")
        )
        for privilege in ("SELECT", "UPDATE", "DELETE"):
            with self.subTest(table="events", privilege=privilege):
                self.assertFalse(
                    _scratch_value(
                        "SELECT has_table_privilege('runner_emitter', 'events', %s)",
                        (privilege,),
                    )
                )
        for table in OFF_LIMITS_TABLES:
            for privilege in ("INSERT", "SELECT", "UPDATE", "DELETE"):
                with self.subTest(table=table, privilege=privilege):
                    self.assertFalse(
                        _scratch_value(
                            "SELECT has_table_privilege('runner_emitter', %s, %s)",
                            (table, privilege),
                        ),
                        f"the emitter must not hold {privilege} on {table}",
                    )
        self.assertTrue(
            _scratch_value(
                "SELECT has_sequence_privilege('runner_emitter', 'events_id_seq', 'USAGE')"
            ),
            "INSERT without the sequence would fail on the bigserial default",
        )
        self.assertTrue(
            _scratch_value("SELECT has_schema_privilege('runner_emitter', 'public', 'USAGE')")
        )
        self.assertFalse(
            _scratch_value("SELECT has_schema_privilege('runner_emitter', 'public', 'CREATE')")
        )

    def test_emitter_appends_but_cannot_read_back_its_own_id(self) -> None:
        # The consequence 020's comment (c) spells out, proven as the role
        # itself. SET ROLE, not a login: the role has no password by design,
        # and the class already had to be able to create it.
        import psycopg

        conn = psycopg.connect(SCRATCH_URL, autocommit=True)
        self.addCleanup(conn.close)
        conn.execute("SET ROLE runner_emitter")

        # A constant needs no column read, so the append itself works.
        self.assertEqual(
            conn.execute(
                "INSERT INTO events (project_id, job_key, kind)"
                " VALUES ('gtm', 'as-emitter', 'job.start') RETURNING 1"
            ).fetchall(),
            [(1,)],
        )
        # RETURNING a real column needs SELECT, which INSERT-only excludes —
        # so events.id, the cursor contract, is invisible to its own writer.
        with self.assertRaises(psycopg.errors.InsufficientPrivilege):
            conn.execute(
                "INSERT INTO events (project_id, job_key, kind)"
                " VALUES ('gtm', 'as-emitter', 'job.start') RETURNING id"
            )


class _FakeCursor:
    """Just enough psycopg cursor for the two driver-free classes below."""

    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class MigrationsTargetGuardTest(unittest.TestCase):
    """The two guards between the applier and the CLIENT's database.

    No database and no driver: both guards are pure enough to test on the
    stdlib interpreter, which is where an operator's first mistake gets
    caught anyway.
    """

    def setUp(self) -> None:
        self._saved = {
            name: os.environ.get(name) for name in ("RUNNER_DSN", "DATABASE_URL")
        }
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        for name, value in self._saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def test_database_url_is_never_the_runner_dsn(self) -> None:
        # DATABASE_URL is the CLIENT's variable — GTM's core/db.py reads
        # exactly it, this repo's own CI `full` job sets it to the GTM
        # database, and so does the Modal Secret. A fallback onto it made a
        # no-flag `agent-runner migrate` write the runner schema into the
        # shared GTM/CRM database.
        os.environ.pop("RUNNER_DSN", None)
        os.environ["DATABASE_URL"] = "postgres://localhost:55432/uflo_gtm_production"
        with self.assertRaises(SystemExit) as caught:
            migrations.resolve_url()
        message = str(caught.exception)
        self.assertIn("RUNNER_DSN", message)
        self.assertIn("DATABASE_URL", message)  # named, so the mistake is obvious

        os.environ["RUNNER_DSN"] = "postgres://localhost:55432/agent_runner"
        self.assertEqual(
            migrations.resolve_url(), "postgres://localhost:55432/agent_runner"
        )
        self.assertEqual(migrations.resolve_url("postgres://explicit/db"),
                         "postgres://explicit/db")

    def test_client_tables_and_foreign_ledger_rows_are_refused(self) -> None:
        class FakeConn:
            """Answers the three read-only questions the guard asks."""

            def __init__(self, tables, ledger_rows, ledger_exists=True):
                self.tables, self.ledger_rows = tables, ledger_rows
                self.ledger_exists = ledger_exists

            def execute(self, sql, params=None):
                if "information_schema.columns" in sql:
                    return _FakeCursor([("filename",)] if self.ledger_exists else [])
                if "information_schema.tables" in sql:
                    wanted = set(params[0])
                    return _FakeCursor(
                        [(name,) for name in sorted(self.tables) if name in wanted]
                    )
                if "FROM schema_migrations" in sql:
                    return _FakeCursor([(row,) for row in self.ledger_rows])
                if "current_database" in sql:
                    return _FakeCursor([("uflo_gtm_production",)])
                raise AssertionError(f"unexpected guard query: {sql}")

        clean = FakeConn([], ["runner-migrations/001_create_projects.sql"])
        migrations.assert_runner_target(clean)  # the correct target: silent

        for tables, ledger in (
            (["pipeline_jobs", "institutions"], []),
            ([], ["graph-migrations/031_group_key.sql"]),
            ([], ["crm-migrations/004_contacts.sql"]),
        ):
            with self.subTest(tables=tables, ledger=ledger):
                target = FakeConn(tables, ledger)
                with self.assertRaises(SystemExit) as caught:
                    migrations.assert_runner_target(target)
                self.assertIn("uflo_gtm_production", str(caught.exception))
                # …and the documented override is the ONLY way through. It
                # warns rather than going quiet, so swallow that line here.
                noise = io.StringIO()
                with contextlib.redirect_stdout(noise):
                    migrations.assert_runner_target(target, allow_foreign=True)
                self.assertIn("WARNING", noise.getvalue())


class RoleProvisioningPrivilegeTest(unittest.TestCase):
    """db/roles against a connection that cannot CREATE ROLE.

    The step-9 target (Railway) and the step-12 one (PlanetScale) both hand
    the applier a non-superuser app role, and neither has ever run this
    chain. Faking the privilege answers is the only way to exercise that
    here — a real unprivileged login needs a password and an hba rule the
    suite cannot assume.
    """

    class FakeConn:
        def __init__(self, *, privileged: bool, role_present: bool):
            self.privileged, self.role_present = privileged, role_present
            self.executed: list[str] = []
            self.commits = 0

        def execute(self, sql, params=None):
            if "rolcreaterole" in sql:
                return _FakeCursor([(self.privileged,)])
            if "FROM pg_roles WHERE rolname = %s" in sql:
                return _FakeCursor([(1,)] if self.role_present else [])
            self.executed.append(sql)
            return _FakeCursor([])

        def commit(self):
            self.commits += 1

        def rollback(self):  # pragma: no cover - only on a failing file
            pass

    def test_privileged_connection_runs_every_role_file(self) -> None:
        conn = self.FakeConn(privileged=True, role_present=False)
        with contextlib.redirect_stdout(io.StringIO()):
            ran = migrations._provision_roles(conn)
        self.assertEqual(ran, EXPECTED_ROLE_FILES)
        self.assertEqual(conn.commits, len(EXPECTED_ROLE_FILES))

    def test_unprivileged_with_the_role_present_still_grants(self) -> None:
        # A managed provider's app role: it owns the tables, so the GRANTs
        # apply; only the cluster-global file is skipped.
        conn = self.FakeConn(privileged=False, role_present=True)
        with contextlib.redirect_stdout(io.StringIO()) as out:
            ran = migrations._provision_roles(conn)
        self.assertEqual(ran, ["020_grant_runner_emitter.sql"])
        self.assertIn("Skipping 010", out.getvalue())

    def test_unprivileged_with_the_role_missing_stops_and_says_how(self) -> None:
        conn = self.FakeConn(privileged=False, role_present=False)
        with self.assertRaises(SystemExit) as caught:
            migrations._provision_roles(conn)
        message = str(caught.exception)
        self.assertIn("CREATEROLE", message)
        self.assertIn(migrations.CLUSTER_ROLE_FILE, message)
        # The whole point of the split: the tables are already safe.
        self.assertIn("fully applied and ledgered", message)
        self.assertEqual(conn.executed, [], "nothing was granted half-way")


if __name__ == "__main__":
    unittest.main()

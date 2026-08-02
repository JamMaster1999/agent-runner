#!/usr/bin/env python3
"""The `agent-runner` CLI shims (extraction step 7): emit / requeue / hook.

The client-repo job_event script died at step 7; its SQL lives in
agent_runner.events as direct functions and the CLI is what agent shells,
hook processes, and operators invoke. Pinned here:

- argparse dispatch for all three subcommands.
- DSN precedence: RUNNER_EMIT_DSN wins over DATABASE_URL (never argv).
- Attribution precedence per value: explicit flag > RUNNER_* env > legacy
  UFLO_* env (co-honored for one release).
- The ported SQL preserves the load-bearing guards: status <> 'cancelled'
  on every pipeline_jobs update, the attempt_count fence, finish/fail
  terminal-row rules, and the unconditional event insert (patched at the
  psycopg seam with the fake driver from test_transport.py).
- hook prints the JSON continue reply and exits 0 even when the harness
  capture main raises; emit exits 0 on internal failure (advisory).
- requeue calls jobstore.requeue_job with the resolved DSN.
- emit_event is single-try at the transport (retry=False): the events CTE
  has no idempotency key, so a timeout replay would double-insert.
- the __main__ process entries re-exec onto RUNNER_PYTHON when psycopg is
  missing (agent shells resolve python3 to a driverless interpreter, which
  would otherwise warn-and-exit-0 with the row lost); main() itself never
  re-execs, so in-process suite calls are safe.
"""

from __future__ import annotations

import contextlib
import io
import json
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

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

from agent_runner import cli, events, jobstore  # noqa: E402
from agent_runner.runtime import RunnerError  # noqa: E402

URL = "postgresql://unused"


class FakeCursor:
    description = object()  # not None -> fetchall path

    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, rows):
        self._rows = rows
        self.executed: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return FakeCursor(self._rows)


def fake_psycopg(connect) -> types.ModuleType:
    """A stand-in psycopg module: the exception hierarchy, conninfo, and a
    caller-supplied connect."""
    module = types.ModuleType("psycopg")

    class Error(Exception):
        pass

    class OperationalError(Error):
        pass

    conninfo = types.ModuleType("psycopg.conninfo")
    conninfo.make_conninfo = lambda url, **kwargs: url
    module.Error = Error
    module.OperationalError = OperationalError
    module.conninfo = conninfo
    module.connect = connect
    return module


class ParserDispatchTest(unittest.TestCase):
    def test_subcommands_bind_their_handlers(self) -> None:
        parser = cli.build_parser()
        for argv, handler in (
            (["emit", "progress", "job-1"], cli.cmd_emit),
            (["requeue", "job-1"], cli.cmd_requeue),
            (["hook", "someprovider"], cli.cmd_hook),
        ):
            with self.subTest(argv=argv):
                self.assertIs(parser.parse_args(argv).handler, handler)

    def test_emit_rejects_unknown_lifecycles(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                cli.build_parser().parse_args(["emit", "explode", "job-1"])


class MigrateSecretTransportTest(unittest.TestCase):
    def test_migrate_argv_contains_only_environment_variable_name(self) -> None:
        sentinel = "postgresql://runner:SENTINEL_MIGRATE_PASSWORD@db.invalid/runner"
        argv = ["migrate", "--database-url-env", "CUTOVER_RUNNER_DSN"]
        self.assertNotIn(sentinel, argv)
        with (
            mock.patch.dict(_os.environ, {"CUTOVER_RUNNER_DSN": sentinel}),
            mock.patch("agent_runner.migrations.apply_pending") as apply_pending,
        ):
            code = cli.main(argv)
        self.assertEqual(code, 0)
        apply_pending.assert_called_once_with(
            sentinel,
            dry_run=False,
            with_roles=True,
            roles_only=False,
            allow_foreign=False,
        )

    def test_dry_run_needs_no_dsn_and_lists_schema_and_roles(self) -> None:
        with mock.patch.dict(_os.environ, {}, clear=False):
            _os.environ.pop("RUNNER_DSN", None)
            _os.environ.pop("DATABASE_URL", None)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = cli.main(["migrate", "--dry-run"])
        self.assertEqual(code, 0)
        rendered = output.getvalue()
        self.assertIn("001_create_projects.sql", rendered)
        self.assertIn("010_create_runner_emitter_role.sql (roles", rendered)

    def test_dry_run_rejects_literal_uri_and_empty_selectors_without_echo(self) -> None:
        sentinel = "postgresql://runner:SENTINEL_CLI_DRY_RUN@db.invalid/runner"
        cases = (
            ("--database-url-env", sentinel),
            ("--database-url-file", sentinel),
            ("--database-url-env", ""),
            ("--database-url-file", ""),
        )
        for selector, value in cases:
            with (
                self.subTest(selector=selector, empty=not value),
                self.assertRaises(SystemExit) as caught,
            ):
                cli.main(["migrate", "--dry-run", selector, value])
            rendered = str(caught.exception)
            self.assertIn("not a value", rendered)
            self.assertNotIn(sentinel, rendered)
            self.assertNotIn("SENTINEL_CLI_DRY_RUN", rendered)

    def test_valid_named_selector_dry_run_is_dsn_and_driver_free(self) -> None:
        selector = "UNSET_BUT_VALID_RUNNER_DSN"
        with (
            mock.patch.dict(_os.environ, {}, clear=False),
            mock.patch(
                "agent_runner.migrations._psycopg",
                side_effect=AssertionError("dry-run imported the driver"),
            ),
        ):
            _os.environ.pop(selector, None)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = cli.main(
                    ["migrate", "--dry-run", "--database-url-env", selector]
                )
        self.assertEqual(code, 0)
        self.assertIn("001_create_projects.sql", output.getvalue())

    def test_help_exposes_selectors_but_no_raw_database_url_value(self) -> None:
        parser = cli.build_parser()
        subparsers = parser._subparsers._group_actions[0]
        help_text = subparsers.choices["migrate"].format_help()
        self.assertIn("--database-url-env NAME", help_text)
        self.assertIn("--database-url-file PATH", help_text)
        self.assertNotIn("--database-url URL", help_text)


class EmitSqlContractTest(unittest.TestCase):
    """The ported update SQL, executed through the fake driver seam."""

    def setUp(self) -> None:
        self._saved = sys.modules.get("psycopg")
        self._had = "psycopg" in sys.modules
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        if self._had:
            sys.modules["psycopg"] = self._saved
        else:
            sys.modules.pop("psycopg", None)

    def run_emit(self, command, rows=None, **kwargs):
        conn = FakeConnection([("job-1", "running")] if rows is None else rows)
        sys.modules["psycopg"] = fake_psycopg(mock.Mock(return_value=conn))
        result = events.emit_event(URL, command, "job-1", **kwargs)
        return result, conn

    def update_arm(self, sql: str) -> str:
        # Everything from the upd CTE on: the guarded pipeline_jobs UPDATE.
        return sql.split("upd AS")[1]

    def test_progress_keeps_cancelled_guard_and_attempt_fence(self) -> None:
        result, conn = self.run_emit(
            "progress", attempt=3, message="halfway", current=5, total=10
        )
        self.assertEqual(result, ("job-1", "running"))
        [(sql, params)] = conn.executed
        update = self.update_arm(sql)
        self.assertIn("status <> 'cancelled'", update)
        self.assertIn("attempt_count = %s", update)
        # The fence binds the attempt value at the guard position (the last
        # parameter after the assignment params).
        self.assertEqual(params[-1], 3)
        self.assertEqual(params[-2], "job-1")

    def test_progress_without_attempt_has_no_fence(self) -> None:
        _, conn = self.run_emit("progress", message="tick")
        [(sql, _)] = conn.executed
        self.assertNotIn("attempt_count = %s", self.update_arm(sql))
        self.assertIn("status <> 'cancelled'", self.update_arm(sql))

    def test_finish_never_resurrects_blocked_or_failed_rows(self) -> None:
        _, conn = self.run_emit("finish", attempt=2)
        [(sql, _)] = conn.executed
        update = self.update_arm(sql)
        self.assertIn("status IN ('queued', 'running', 'succeeded')", update)
        self.assertIn("status = 'succeeded'", update)
        self.assertIn("status <> 'cancelled'", update)

    def test_fail_never_overwrites_a_terminal_row(self) -> None:
        _, conn = self.run_emit("fail", message="boom", attempt=1)
        [(sql, _)] = conn.executed
        update = self.update_arm(sql)
        self.assertIn("status IN ('queued', 'running')", update)
        self.assertIn("status = 'failed'", update)

    def test_event_insert_lands_outside_the_guard(self) -> None:
        # The audit-trail insert is unconditional: the guard fences only the
        # jobs UPDATE arm, never the events INSERT.
        _, conn = self.run_emit("fail", message="boom", attempt=1)
        [(sql, _)] = conn.executed
        insert_arm = sql.split("upd AS")[0]
        self.assertIn("INSERT INTO events", insert_arm)
        self.assertNotIn("status <> 'cancelled'", insert_arm)
        self.assertNotIn("attempt_count = %s", insert_arm)

    def test_batch_progress_is_one_statement(self) -> None:
        batch = [
            {"event": "turn_completed", "message": "turn 1", "tok_input": 10},
            {"event": "progress", "message": "25/50", "current": 25, "total": 50},
        ]
        _, conn = self.run_emit("progress", attempt=1, batch=batch)
        self.assertEqual(len(conn.executed), 1)
        [(sql, params)] = conn.executed
        self.assertEqual(sql.count("(%s"), len(batch))
        self.assertIn("turn 1", params)
        self.assertIn("25/50", params)

    def test_heartbeat_bumps_the_run_row_when_run_id_is_given(self) -> None:
        # Step 9: the run row is a lease now; the bump lands on the held
        # leases row for this lease_ref.
        _, conn = self.run_emit("heartbeat", run_id="run-1")
        [(sql, params)] = conn.executed
        self.assertIn("UPDATE leases", sql)
        self.assertIn("status = 'held'", sql)
        self.assertIn("run-1", params)
        self.assertIn("status <> 'cancelled'", sql)
        # Returns the job status for the cancel poll.
        self.assertEqual(self.run_emit("heartbeat")[0], ("job-1", "running"))

    def test_missing_job_row_raises_job_missing(self) -> None:
        with self.assertRaises(RunnerError) as ctx:
            self.run_emit("progress", rows=[])
        self.assertEqual(ctx.exception.code, "job_missing")

    def test_emit_is_single_try_never_replaying_the_event_insert(self) -> None:
        # The CTE appends audit rows with no idempotency key: a transport
        # replay after a commit-with-lost-reply would double-insert them (and
        # re-run the guarded update), so emit_event opts out of db_rows' one
        # timeout retry — exactly one connect attempt.
        connect = mock.Mock()
        module = fake_psycopg(connect)
        sys.modules["psycopg"] = module
        connect.side_effect = module.OperationalError("connection dropped")
        with self.assertRaises(RunnerError) as ctx:
            events.emit_event(URL, "progress", "job-1", message="tick")
        self.assertEqual(ctx.exception.code, "db_timeout")
        self.assertEqual(connect.call_count, 1)


class EmitCliTest(unittest.TestCase):
    """Dispatch, DSN precedence, attribution precedence, advisory exit."""

    def call_emit(self, argv, env):
        captured: dict = {}

        def fake_emit_event(url, command, job_key, **kwargs):
            captured.update(url=url, command=command, job_key=job_key, **kwargs)
            return ("job-1", "running")

        with (
            mock.patch.dict(_os.environ, env, clear=False),
            mock.patch.object(events, "emit_event", fake_emit_event),
            contextlib.redirect_stdout(io.StringIO()) as out,
        ):
            for name in cli.ENV_FALLBACKS.values():
                for var in name:
                    if var not in env:
                        _os.environ.pop(var, None)
            for var in ("RUNNER_EMIT_DSN", "DATABASE_URL"):
                if var not in env:
                    _os.environ.pop(var, None)
            code = cli.main(argv)
        return code, captured, out.getvalue()

    def test_runner_emit_dsn_wins_over_database_url(self) -> None:
        code, captured, _ = self.call_emit(
            ["emit", "progress", "job-1"],
            {"RUNNER_EMIT_DSN": "postgresql://emit", "DATABASE_URL": "postgresql://default"},
        )
        self.assertEqual(code, 0)
        self.assertEqual(captured["url"], "postgresql://emit")

    def test_database_url_is_the_dsn_fallback(self) -> None:
        _, captured, _ = self.call_emit(
            ["emit", "progress", "job-1"], {"DATABASE_URL": "postgresql://default"}
        )
        self.assertEqual(captured["url"], "postgresql://default")

    def test_explicit_flags_beat_runner_env(self) -> None:
        _, captured, _ = self.call_emit(
            [
                "emit", "progress", "job-1",
                "--run-id", "flag-run",
                "--phase", "flag-phase",
                "--backend", "flag-backend",
                "--attempt", "7",
            ],
            {
                "DATABASE_URL": "postgresql://default",
                "RUNNER_RUN_ID": "env-run",
                "RUNNER_PHASE": "env-phase",
                "RUNNER_BACKEND": "env-backend",
                "RUNNER_ATTEMPT": "1",
            },
        )
        self.assertEqual(captured["run_id"], "flag-run")
        self.assertEqual(captured["phase"], "flag-phase")
        self.assertEqual(captured["backend"], "flag-backend")
        self.assertEqual(captured["attempt"], 7)

    def test_runner_env_beats_legacy_uflo_env(self) -> None:
        _, captured, _ = self.call_emit(
            ["emit", "progress"],
            {
                "DATABASE_URL": "postgresql://default",
                "RUNNER_JOB_KEY": "runner-key",
                "UFLO_JOB_STABLE_ID": "uflo-key",
                "RUNNER_RUN_ID": "runner-run",
                "UFLO_RUN_ID": "uflo-run",
                "RUNNER_ATTEMPT": "2",
                "UFLO_ATTEMPT": "9",
            },
        )
        self.assertEqual(captured["job_key"], "runner-key")
        self.assertEqual(captured["run_id"], "runner-run")
        self.assertEqual(captured["attempt"], 2)

    def test_legacy_uflo_env_is_still_honored(self) -> None:
        _, captured, _ = self.call_emit(
            ["emit", "heartbeat"],
            {
                "DATABASE_URL": "postgresql://default",
                "UFLO_JOB_STABLE_ID": "uflo-key",
                "UFLO_RUN_ID": "uflo-run",
                "UFLO_PHASE": "phase5",
                "UFLO_BACKEND": "someharness",
            },
        )
        self.assertEqual(captured["job_key"], "uflo-key")
        self.assertEqual(captured["run_id"], "uflo-run")
        self.assertEqual(captured["phase"], "phase5")
        self.assertEqual(captured["backend"], "someharness")

    def test_heartbeat_prints_the_status_line(self) -> None:
        _, _, stdout = self.call_emit(
            ["emit", "heartbeat", "job-1"], {"DATABASE_URL": "postgresql://default"}
        )
        self.assertEqual(stdout.strip(), "heartbeat: job-1 status=running")

    def test_internal_failure_still_exits_zero(self) -> None:
        # Advisory contract: a DB hiccup (or any internal failure) must never
        # fail the agent's shell command.
        with (
            mock.patch.dict(_os.environ, {"DATABASE_URL": "postgresql://default"}),
            mock.patch.object(events, "emit_event", side_effect=RunnerError("db down", code="db_error")),
            contextlib.redirect_stderr(io.StringIO()) as err,
        ):
            code = cli.main(["emit", "progress", "job-1"])
        self.assertEqual(code, 0)
        self.assertIn("WARNING: agent-runner emit progress failed", err.getvalue())

    def test_missing_job_key_and_dsn_exit_zero_with_a_warning(self) -> None:
        for argv, env in (
            (["emit", "progress"], {"DATABASE_URL": "postgresql://default"}),
            (["emit", "progress", "job-1"], {}),
        ):
            with self.subTest(argv=argv, env=env):
                with contextlib.redirect_stderr(io.StringIO()):
                    code, _, _ = self.call_emit(argv, env)
                self.assertEqual(code, 0)


class HookCliTest(unittest.TestCase):
    def test_capture_failure_prints_continue_and_exits_zero(self) -> None:
        from agent_runner.harness import codex_hook_event

        with (
            mock.patch.object(codex_hook_event, "main", side_effect=RuntimeError("boom")),
            contextlib.redirect_stdout(io.StringIO()) as out,
            contextlib.redirect_stderr(io.StringIO()) as err,
        ):
            code = cli.main(["hook", "codex"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out.getvalue()), {"continue": True})
        self.assertIn("WARNING: agent-runner hook capture failed", err.getvalue())

    def test_unknown_provider_is_also_advisory(self) -> None:
        with (
            contextlib.redirect_stdout(io.StringIO()) as out,
            contextlib.redirect_stderr(io.StringIO()),
        ):
            code = cli.main(["hook", "no-such-provider"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out.getvalue()), {"continue": True})

    def test_success_defers_stdout_to_the_capture_main(self) -> None:
        from agent_runner.harness import claude_hook_event

        with (
            mock.patch.object(claude_hook_event, "main") as capture,
            contextlib.redirect_stdout(io.StringIO()) as out,
        ):
            code = cli.main(["hook", "claude"])
        capture.assert_called_once_with()
        self.assertEqual(code, 0)
        self.assertEqual(out.getvalue(), "")


class ReexecTest(unittest.TestCase):
    """The process-entry driver re-exec: agent shells resolve ``python3`` to
    an interpreter that may lack psycopg, so the ``__main__`` entries hop
    the whole invocation onto RUNNER_PYTHON (the engine's interpreter,
    stamped by agent_env) — otherwise every in-shell emit would take the
    advisory exit-0 path and silently lose its event row."""

    ARGV = ["/somewhere/agent_runner/__main__.py", "emit", "progress", "job-1"]

    def run_entry(self, env, psycopg_module):
        with (
            mock.patch.dict(_os.environ, env, clear=False),
            mock.patch.object(sys, "argv", list(self.ARGV)),
            mock.patch.dict(sys.modules, {"psycopg": psycopg_module}),
            mock.patch.object(cli.os, "execv") as execv,
        ):
            for var in ("RUNNER_PYTHON", cli._REEXEC_GUARD):
                if var not in env:
                    _os.environ.pop(var, None)
            cli.reexec_with_driver()
            guard = _os.environ.get(cli._REEXEC_GUARD)
        return execv, guard

    def test_missing_driver_reexecs_onto_runner_python(self) -> None:
        # sys.modules['psycopg'] = None forces ImportError on import.
        execv, guard = self.run_entry({"RUNNER_PYTHON": sys.executable}, None)
        execv.assert_called_once_with(
            sys.executable,
            [sys.executable, "-m", "agent_runner", "emit", "progress", "job-1"],
        )
        # The guard rides the environment into the exec'd process so a
        # RUNNER_PYTHON also missing psycopg cannot loop.
        self.assertEqual(guard, "1")

    def test_driver_present_is_a_noop_and_clears_the_guard(self) -> None:
        execv, guard = self.run_entry(
            {"RUNNER_PYTHON": sys.executable, cli._REEXEC_GUARD: "1"},
            types.ModuleType("psycopg"),
        )
        execv.assert_not_called()
        # Popped so child processes started from here may hop themselves.
        self.assertIsNone(guard)

    def test_guard_breaks_the_exec_loop(self) -> None:
        execv, guard = self.run_entry(
            {"RUNNER_PYTHON": sys.executable, cli._REEXEC_GUARD: "1"}, None
        )
        execv.assert_not_called()
        self.assertEqual(guard, "1")

    def test_without_runner_python_the_advisory_path_owns_the_failure(self) -> None:
        execv, _ = self.run_entry({}, None)
        execv.assert_not_called()

    def test_a_vanished_runner_python_never_execs(self) -> None:
        execv, _ = self.run_entry({"RUNNER_PYTHON": "/no/such/interpreter"}, None)
        execv.assert_not_called()


class RequeueCliTest(unittest.TestCase):
    def test_requeue_calls_jobstore_with_the_resolved_dsn(self) -> None:
        with (
            mock.patch.dict(_os.environ, {"RUNNER_DSN": "postgresql://runner"}),
            mock.patch.object(jobstore, "requeue_job") as requeue,
        ):
            code = cli.main(["requeue", "job-9"])
        self.assertEqual(code, 0)
        requeue.assert_called_once_with("postgresql://runner", "job-9")

    def test_database_url_is_the_requeue_fallback(self) -> None:
        with (
            mock.patch.dict(_os.environ, {"DATABASE_URL": "postgresql://default"}),
            mock.patch.object(jobstore, "requeue_job") as requeue,
        ):
            _os.environ.pop("RUNNER_DSN", None)
            cli.main(["requeue", "job-9"])
        requeue.assert_called_once_with("postgresql://default", "job-9")

    def test_requeue_failures_stay_loud(self) -> None:
        # Operator command: unlike emit/hook, errors surface as they are.
        with (
            mock.patch.dict(_os.environ, {"DATABASE_URL": "postgresql://default"}),
            mock.patch.object(jobstore, "requeue_job", side_effect=SystemExit("nope")),
            self.assertRaises(SystemExit),
        ):
            cli.main(["requeue", "job-9"])


if __name__ == "__main__":
    unittest.main()

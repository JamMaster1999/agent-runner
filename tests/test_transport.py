#!/usr/bin/env python3
"""The runner's own psycopg transport: the RunnerError contract at the seam.

Extraction step 6 gave agent_runner.util its own psycopg layer (previously a
delegation to the GTM transport). This file pins the error contract the
runner modules rely on, patched at the psycopg seam with a fake driver
module so it runs in the stdlib suite too:

- OperationalError (statement_timeout / connect_timeout) retries ONCE, then
  surfaces as RunnerError(code='db_timeout', retryable=True, alert=False).
- Any other psycopg.Error is terminal:
  RunnerError(code='db_error', retryable=False, alert=True).
- db_tx has the same contract, with the transaction-script label as details.
- With no psycopg importable at all, first DB use raises the loud SystemExit
  install guidance (import agent_runner stays driver-free).
"""

from __future__ import annotations

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
_os.environ.setdefault("RUNNER_PROJECT_ID", "testproj")
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import util as runner_util  # noqa: E402
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


class TransportContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = sys.modules.get("psycopg")
        self._had = "psycopg" in sys.modules
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        if self._had:
            sys.modules["psycopg"] = self._saved
        else:
            sys.modules.pop("psycopg", None)

    def install(self, connect) -> types.ModuleType:
        module = fake_psycopg(connect)
        sys.modules["psycopg"] = module
        return module

    def test_operational_error_retries_once_then_raises_retryable_db_timeout(self) -> None:
        connect = mock.Mock()
        module = self.install(connect)
        connect.side_effect = module.OperationalError("statement timeout")
        sql = "SELECT * FROM pipeline_jobs;"
        with self.assertRaises(RunnerError) as ctx:
            runner_util.db_rows(URL, sql, timeout=60)
        failure = ctx.exception
        self.assertEqual(failure.code, "db_timeout")
        self.assertTrue(failure.retryable)
        self.assertFalse(failure.alert)
        self.assertEqual(str(failure), "database call timed out after 60s (2 tries).")
        self.assertEqual(failure.details, sql)
        self.assertIsInstance(failure.__cause__, module.OperationalError)
        # Retried exactly once: two connect attempts total.
        self.assertEqual(connect.call_count, 2)

    def test_retry_false_is_single_try_for_non_idempotent_statements(self) -> None:
        # The events-append CTE has no idempotency key: under autocommit a
        # commit whose reply was lost looks like a failed try, so a replay
        # would double-insert. retry=False opts such statements out of the
        # one timeout retry — exactly one connect attempt.
        connect = mock.Mock()
        module = self.install(connect)
        connect.side_effect = module.OperationalError("connection dropped")
        with self.assertRaises(RunnerError) as ctx:
            runner_util.db_rows(URL, "SELECT 1;", retry=False)
        failure = ctx.exception
        self.assertEqual(failure.code, "db_timeout")
        self.assertTrue(failure.retryable)
        self.assertEqual(str(failure), "database call timed out after 60s (1 try).")
        self.assertEqual(connect.call_count, 1)

    def test_transient_operational_error_recovers_on_the_retry(self) -> None:
        connect = mock.Mock()
        module = self.install(connect)
        connect.side_effect = [
            module.OperationalError("dial timeout"),
            FakeConnection([(1,)]),
        ]
        self.assertEqual(runner_util.db_rows(URL, "SELECT 1;"), [(1,)])
        self.assertEqual(connect.call_count, 2)

    def test_other_db_errors_are_terminal_db_error_with_alert(self) -> None:
        connect = mock.Mock()
        module = self.install(connect)
        connect.side_effect = module.Error('relation "pipeline_jobs" does not exist')
        with self.assertRaises(RunnerError) as ctx:
            runner_util.db_rows(URL, "SELECT 1;")
        failure = ctx.exception
        self.assertEqual(failure.code, "db_error")
        self.assertFalse(failure.retryable)
        self.assertTrue(failure.alert)
        self.assertEqual(str(failure), "database command failed")
        self.assertEqual(failure.details, 'relation "pipeline_jobs" does not exist')
        # Terminal on the first try: no blind retry of a broken statement.
        self.assertEqual(connect.call_count, 1)

    def test_db_rows_binds_cleaned_params(self) -> None:
        conn = FakeConnection([("row",)])
        self.install(mock.Mock(return_value=conn))
        rows = runner_util.db_rows(URL, "SELECT %s;", ["with\x00nul"])
        self.assertEqual(rows, [("row",)])
        self.assertEqual(conn.executed, [("SELECT %s;", ["withnul"])])

    def test_db_tx_same_contract_with_script_label_details(self) -> None:
        connect = mock.Mock()
        module = self.install(connect)
        connect.side_effect = module.OperationalError("lock wait")

        def lifecycle_script(conn):  # pragma: no cover - never reached
            return conn

        with self.assertRaises(RunnerError) as ctx:
            runner_util.db_tx(URL, lifecycle_script, timeout=30)
        failure = ctx.exception
        self.assertEqual(failure.code, "db_timeout")
        self.assertTrue(failure.retryable)
        self.assertIn("transaction script", failure.details)
        self.assertIn("lifecycle_script", failure.details)
        self.assertEqual(connect.call_count, 2)

    def test_db_tx_runs_the_script_inside_the_connection(self) -> None:
        conn = FakeConnection([])
        self.install(mock.Mock(return_value=conn))
        result = runner_util.db_tx(URL, lambda c: c.execute("UPDATE x;") and "done")
        self.assertEqual(result, "done")
        self.assertEqual(conn.executed, [("UPDATE x;", None)])

    def test_missing_psycopg_raises_the_loud_install_guidance(self) -> None:
        sys.modules["psycopg"] = None  # forces ImportError on import
        with self.assertRaises(SystemExit) as ctx:
            runner_util.db_rows(URL, "SELECT 1;")
        self.assertIn("psycopg", str(ctx.exception))
        self.assertIn("pip install", str(ctx.exception))


class CleanParamsTest(unittest.TestCase):
    def test_none_sequence_and_mapping(self) -> None:
        self.assertIsNone(runner_util.clean_params(None))
        self.assertEqual(runner_util.clean_params(["a\x00b", 3]), ["ab", 3])
        self.assertEqual(
            runner_util.clean_params({"k": "a\x00b", "n": 3}), {"k": "ab", "n": 3}
        )


if __name__ == "__main__":
    unittest.main()

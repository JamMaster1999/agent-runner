#!/usr/bin/env python3
"""Migration driver failures never render DSNs or arbitrary exception text."""

from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


REPO = Path(__file__).resolve().parents[1]
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import migrations  # noqa: E402


SENTINEL_DSN = "postgresql://runner:SENTINEL_DRIVER_SECRET@private.invalid/db"
SENTINEL = "SENTINEL_DRIVER_SECRET"


class SecretDriverError(Exception):
    sqlstate = "28P01"


class MaliciousStateError(Exception):
    sqlstate = "28P01 " + SENTINEL


class FailingConn:
    def __init__(self, error, *, rollback_error=None):
        self.error = error
        self.rollback_error = rollback_error
        self.rollback_calls = 0

    def execute(self, *_args, **_kwargs):
        raise self.error

    def commit(self):
        raise AssertionError("commit must not run after execute failure")

    def rollback(self):
        self.rollback_calls += 1
        if self.rollback_error is not None:
            raise self.rollback_error


class MigrationSecrecyTest(unittest.TestCase):
    def assert_sanitized(self, caught, *, sqlstate=True):
        rendered = str(caught.exception)
        self.assertIn("SecretDriverError", rendered)
        if sqlstate:
            self.assertIn("SQLSTATE 28P01", rendered)
        self.assertNotIn(SENTINEL_DSN, rendered)
        self.assertNotIn(SENTINEL, rendered)

    def test_connect_failure_is_bounded(self) -> None:
        driver = types.SimpleNamespace(
            connect=mock.Mock(side_effect=SecretDriverError(SENTINEL_DSN))
        )
        with (
            mock.patch.object(migrations, "_psycopg", return_value=driver),
            self.assertRaises(SystemExit) as caught,
        ):
            migrations.apply_pending(SENTINEL_DSN, with_roles=False)
        self.assert_sanitized(caught)

    def test_migration_failure_rolls_back_without_rendering_message(self) -> None:
        conn = FailingConn(SecretDriverError(SENTINEL_DSN))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "999_sentinel.sql"
            path.write_text("SELECT 1;", encoding="utf-8")
            with self.assertRaises(SystemExit) as caught:
                migrations.apply_migration(conn, path)
        self.assertEqual(conn.rollback_calls, 1)
        self.assert_sanitized(caught)

    def test_role_failure_and_rollback_failure_are_both_bounded(self) -> None:
        conn = FailingConn(
            SecretDriverError(SENTINEL_DSN),
            rollback_error=SecretDriverError(SENTINEL_DSN),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "999_sentinel_role.sql"
            path.write_text("SELECT 1;", encoding="utf-8")
            with self.assertRaises(SystemExit) as caught:
                migrations.run_role_file(conn, path)
        self.assertEqual(conn.rollback_calls, 1)
        self.assert_sanitized(caught)
        self.assertIn("rollback failed", str(caught.exception))

    def test_unvalidated_sqlstate_is_omitted(self) -> None:
        conn = FailingConn(MaliciousStateError(SENTINEL_DSN))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "999_bad_state.sql"
            path.write_text("SELECT 1;", encoding="utf-8")
            with self.assertRaises(SystemExit) as caught:
                migrations.apply_migration(conn, path)
        rendered = str(caught.exception)
        self.assertIn("MaliciousStateError", rendered)
        self.assertNotIn("SQLSTATE", rendered)
        self.assertNotIn(SENTINEL, rendered)


if __name__ == "__main__":
    unittest.main()

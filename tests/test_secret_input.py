#!/usr/bin/env python3
"""Secret selectors keep database credentials out of operator argv/logs."""

from __future__ import annotations

import contextlib
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO = Path(__file__).resolve().parents[1]
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner.secret_input import secret_value  # noqa: E402
import db.apply_migrations as migration_script  # noqa: E402


SENTINEL_URL = "postgresql://cutover:NEVER_PRINT_THIS@sentinel.invalid/runner"
SENTINEL_PASSWORD = "NEVER_PRINT_THIS"


class SecretInputTest(unittest.TestCase):
    def test_named_environment_variable_returns_value_without_printing_it(self) -> None:
        with mock.patch.dict(os.environ, {"CUTOVER_RUNNER_DSN": SENTINEL_URL}):
            output = io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                value = secret_value(
                    label="runner database URL", env_name="CUTOVER_RUNNER_DSN"
                )
        self.assertEqual(value, SENTINEL_URL)
        self.assertNotIn(SENTINEL_URL, output.getvalue())
        self.assertNotIn(SENTINEL_PASSWORD, output.getvalue())

    def test_selector_rejects_a_literal_url_without_echoing_it(self) -> None:
        with self.assertRaises(SystemExit) as caught:
            secret_value(label="runner database URL", env_name=SENTINEL_URL)
        message = str(caught.exception)
        self.assertNotIn(SENTINEL_URL, message)
        self.assertNotIn(SENTINEL_PASSWORD, message)
        self.assertIn("variable name", message)

    def test_private_one_value_file_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runner.dsn"
            path.write_text(SENTINEL_URL + "\n", encoding="utf-8")
            path.chmod(0o600)
            self.assertEqual(
                secret_value(label="runner database URL", file_path=path),
                SENTINEL_URL,
            )

    def test_extra_trailing_blank_line_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runner.dsn"
            path.write_text(SENTINEL_URL + "\n\n", encoding="utf-8")
            path.chmod(0o600)
            with self.assertRaises(SystemExit) as caught:
                secret_value(label="runner database URL", file_path=path)
        message = str(caught.exception)
        self.assertIn("exactly one value", message)
        self.assertNotIn(SENTINEL_URL, message)
        self.assertNotIn(SENTINEL_PASSWORD, message)

    @unittest.skipUnless(os.name == "posix", "POSIX permission contract")
    def test_group_readable_file_is_refused_without_reading_or_echoing_value(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runner.dsn"
            path.write_text(SENTINEL_URL, encoding="utf-8")
            path.chmod(0o640)
            with self.assertRaises(SystemExit) as caught:
                secret_value(label="runner database URL", file_path=path)
        message = str(caught.exception)
        self.assertNotIn(SENTINEL_URL, message)
        self.assertNotIn(SENTINEL_PASSWORD, message)
        self.assertIn("chmod 600", message)


class MigrationScriptDryRunTest(unittest.TestCase):
    def test_repo_entrypoint_dry_run_needs_no_dsn_and_lists_roles(self) -> None:
        with (
            mock.patch.dict(os.environ, {}, clear=False),
            mock.patch.object(
                sys, "argv", ["apply_migrations.py", "--dry-run"]
            ),
        ):
            os.environ.pop("RUNNER_DSN", None)
            os.environ.pop("DATABASE_URL", None)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                migration_script.main()
        rendered = output.getvalue()
        self.assertIn("001_create_projects.sql", rendered)
        self.assertIn("010_create_runner_emitter_role.sql (roles", rendered)


if __name__ == "__main__":
    unittest.main()

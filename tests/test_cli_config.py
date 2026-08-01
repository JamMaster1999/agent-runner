#!/usr/bin/env python3
"""Characterization tests for the Codex agent-config plumbing.

Pins toml_cli_value / flattened_codex_config — the `-c dotted.key=value`
flattening for `codex exec`, living in the codex harness adapter. (The
sync_agents render constraint, parse_backoff, and the psycopg re-exec shim
cases stayed in the GTM suite with the GTM modules they exercise; the DB
transport's RunnerError contract is pinned in tests/test_transport.py.)
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

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

from agent_runner.harness import codex as codex_harness  # noqa: E402


class TomlCliValueTest(unittest.TestCase):
    def test_bool_renders_bare_toml_literals(self) -> None:
        self.assertEqual(codex_harness.toml_cli_value(True), "true")
        self.assertEqual(codex_harness.toml_cli_value(False), "false")

    def test_numbers_render_bare(self) -> None:
        self.assertEqual(codex_harness.toml_cli_value(3), "3")
        self.assertEqual(codex_harness.toml_cli_value(2.5), "2.5")

    def test_strings_render_json_quoted(self) -> None:
        self.assertEqual(codex_harness.toml_cli_value("gpt-5.5"), '"gpt-5.5"')
        self.assertEqual(codex_harness.toml_cli_value('say "hi"'), '"say \\"hi\\""')

    def test_lists_render_recursively(self) -> None:
        self.assertEqual(codex_harness.toml_cli_value(["firecrawl", 1, True]), '["firecrawl", 1, true]')

    def test_unsupported_values_raise_terminal_runner_error(self) -> None:
        from agent_runner.runtime import RunnerError

        for value in ({"nested": 1}, None):
            with self.subTest(value=value):
                with self.assertRaises(RunnerError) as ctx:
                    codex_harness.toml_cli_value(value)
                self.assertEqual(ctx.exception.code, "invalid_codex_agent_config")
                self.assertFalse(ctx.exception.retryable)
                self.assertTrue(ctx.exception.alert)


class FlattenedCodexConfigTest(unittest.TestCase):
    def test_scalar_passes_through(self) -> None:
        self.assertEqual(codex_harness.flattened_codex_config("model", "gpt-5.5"), [("model", "gpt-5.5")])

    def test_nested_tables_flatten_to_dotted_keys(self) -> None:
        config = {"shell_tool": False, "web": {"enabled": True}}
        self.assertEqual(
            codex_harness.flattened_codex_config("features", config),
            [("features.shell_tool", False), ("features.web.enabled", True)],
        )

    def test_composed_dash_c_arguments(self) -> None:
        # The exact composition codex_agent_config_args performs per key.
        config = {"model": "gpt-5.5", "features": {"shell_tool": False}}
        args: list[str] = []
        for key, value in config.items():
            for dotted_key, dotted_value in codex_harness.flattened_codex_config(key, value):
                args.extend(["-c", f"{dotted_key}={codex_harness.toml_cli_value(dotted_value)}"])
        self.assertEqual(
            args,
            ["-c", 'model="gpt-5.5"', "-c", "features.shell_tool=false"],
        )


if __name__ == "__main__":
    unittest.main()

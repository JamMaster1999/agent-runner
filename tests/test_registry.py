#!/usr/bin/env python3
"""Harness registry + adapter-contract tests (design doc §2, §12 gate 2).

Proves both built-in adapters register with their documented Capabilities,
and that a new harness plugs in with zero edits outside its own class plus
one register() call — the pluggability acceptance gate.
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
_os.environ.setdefault("RUNNER_PROJECT_ID", "testproj")
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner.harness import (  # noqa: E402
    Capabilities,
    HarnessAdapter,
    get_adapter,
    register,
    registered_adapters,
)
from agent_runner.runtime import RunnerError  # noqa: E402


class BuiltinRegistrationTest(unittest.TestCase):
    def test_codex_registers_with_documented_capabilities(self) -> None:
        adapter = get_adapter("codex")
        self.assertIsInstance(adapter, HarnessAdapter)
        self.assertEqual(adapter.name, "codex")
        self.assertEqual(
            adapter.capabilities,
            Capabilities(
                resume=True,
                followup=True,
                hooks=True,
                doctor=True,
                final_message_artifact=True,
            ),
        )

    def test_claude_registers_with_documented_capabilities(self) -> None:
        adapter = get_adapter("claude")
        self.assertIsInstance(adapter, HarnessAdapter)
        self.assertEqual(adapter.name, "claude")
        self.assertEqual(
            adapter.capabilities,
            Capabilities(
                resume=True,
                followup=False,
                hooks=True,
                doctor=False,
                final_message_artifact=False,
            ),
        )

    def test_registration_order_feeds_the_reaper_hint(self) -> None:
        # The reaper's orphan hint joins names and pgrep patterns in
        # registration order; the built-ins must reproduce the pre-adapter
        # text verbatim. Prefix assertion: later registrations (the stub
        # below) may follow.
        adapters = registered_adapters()[:2]
        self.assertEqual([adapter.name for adapter in adapters], ["codex", "claude"])
        self.assertEqual("/".join(adapter.name for adapter in adapters), "codex/claude")
        self.assertEqual(
            "|".join(pattern for adapter in adapters for pattern in adapter.orphan_patterns()),
            "codex exec|claude",
        )

    def test_unknown_backend_raises_terminal_runner_error(self) -> None:
        with self.assertRaises(RunnerError) as ctx:
            get_adapter("no-such-harness")
        self.assertEqual(ctx.exception.code, "unknown_backend")
        self.assertFalse(ctx.exception.retryable)


class StubAdapter(HarnessAdapter):
    """Gemini-style stub (§12 gate 2): a new harness is exactly one adapter
    class raising NotImplementedError plus one register() call — zero edits
    anywhere else."""

    name = "stub-harness"
    display_name = "Stub"
    start_label = "Stub"
    session_noun = "session"
    capabilities = Capabilities()

    def resolve_binary(self):
        raise NotImplementedError

    def health_checks(self, args):
        raise NotImplementedError

    def build_spawn(self, job, directory):
        raise NotImplementedError

    def build_resume(self, job, directory, session_ref):
        raise NotImplementedError

    def materialize_agent(self, agent, header):
        raise NotImplementedError

    def session_ref_from_log(self, stdout_path):
        raise NotImplementedError

    def stream_parser(self):
        raise NotImplementedError

    def hook_event_log(self):
        raise NotImplementedError

    def normalize_hook_event(self, event, agent_name):
        raise NotImplementedError

    def stream_error_line(self, payload):
        raise NotImplementedError


class StubPluggabilityTest(unittest.TestCase):
    def test_stub_registers_with_one_call_and_inherits_defaults(self) -> None:
        register(StubAdapter())
        adapter = get_adapter("stub-harness")
        self.assertIsInstance(adapter, StubAdapter)
        self.assertIn(adapter, registered_adapters())
        # Central degradation defaults, no per-harness code required:
        # no marker data -> no terminal proof, everything retries 'unknown';
        # no followup -> the engine falls back to a plain retry;
        # 'local-login' -> no credential env.
        self.assertIsNone(adapter.classify("token expired?? who knows"))
        failure = adapter.classify_failure("mystery output")
        self.assertEqual(failure.code, "unknown")
        self.assertTrue(failure.retryable)
        self.assertEqual(str(failure), "stub-harness attempt failed")
        self.assertIsNone(adapter.build_followup(None, Path("/nonexistent"), "ref"))
        self.assertEqual(adapter.bind_credentials(), {})
        self.assertEqual(adapter.env_overrides(), {})
        self.assertEqual(adapter.orphan_patterns(), ["stub-harness"])


if __name__ == "__main__":
    unittest.main()

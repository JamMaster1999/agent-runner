#!/usr/bin/env python3
"""Characterization tests for the harness adapters' failure classification.

Pins the 2026-07-28 policy now that the marker data lives per adapter:
terminal only when the CLI itself reports auth expiry, billing or quota
exhaustion, an invalid invocation, or the health probe's budget cap;
everything else — including ambiguous or empty text — retries as 'unknown'.
Both adapters deliberately carry identical tables today: the pre-adapter
code matched one shared list for both CLIs, and splitting it per dialect
would be a behavior change, not a move.
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

from agent_runner.harness import get_adapter  # noqa: E402

ADAPTERS = (get_adapter("codex"), get_adapter("claude"))


class TerminalMarkerTableTest(unittest.TestCase):
    def test_category_order_is_pinned(self) -> None:
        # Order matters: classification returns the FIRST matching category.
        for adapter in ADAPTERS:
            with self.subTest(adapter=adapter.name):
                self.assertEqual(
                    [category for category, _ in adapter.terminal_markers],
                    ["health_budget_too_low", "auth", "billing_or_credits", "invalid_invocation"],
                )

    def test_every_marker_is_lowercase(self) -> None:
        # Matching lower-cases the input text; a mixed-case marker would
        # silently never match.
        for adapter in ADAPTERS:
            for _, markers in adapter.terminal_markers:
                for marker in markers:
                    self.assertEqual(marker, marker.lower())

    def test_tables_are_identical_across_adapters(self) -> None:
        # Deliberate: the pre-adapter code matched one shared list for both
        # CLIs; splitting it per dialect would change behavior. Prune per
        # dialect on purpose, not as a side effect of the adapter move.
        codex, claude = ADAPTERS
        self.assertEqual(codex.terminal_markers, claude.terminal_markers)


class ClassifyFailureTest(unittest.TestCase):
    def test_each_terminal_marker_maps_to_its_code(self) -> None:
        # Terminal markers carry alert=True (the operator-worthy fact flag,
        # step-5 retype of notify_now); the unknown default stays
        # retryable=True, alert=False.
        for adapter in ADAPTERS:
            for code, markers in adapter.terminal_markers:
                for marker in markers:
                    text = f"CLI reported: {marker.upper()} — see logs"
                    with self.subTest(adapter=adapter.name, marker=marker):
                        error = adapter.classify_failure(text)
                        self.assertEqual(error.code, code)
                        self.assertFalse(error.retryable)
                        self.assertTrue(error.alert)
                        self.assertEqual(error.details, text)
                        self.assertEqual(
                            str(error), f"{adapter.name} terminal failure: {code}"
                        )

    def test_first_code_wins_on_multi_category_text(self) -> None:
        error = get_adapter("codex").classify_failure(
            "reached maximum budget after retry; also not logged in"
        )
        self.assertEqual(error.code, "health_budget_too_low")

    def test_ambiguous_text_is_retryable_unknown(self) -> None:
        text = "network flake: connection reset by peer (HTTP 403 from registrar page)"
        adapter = get_adapter("codex")
        # The evidence slot reports no terminal proof; the default supplies
        # the retryable 'unknown' judgment.
        self.assertIsNone(adapter.classify(text))
        error = adapter.classify_failure(text)
        self.assertEqual(error.code, "unknown")
        self.assertTrue(error.retryable)
        self.assertFalse(error.alert)
        self.assertEqual(error.details, text)
        self.assertEqual(str(error), "codex attempt failed")

    def test_incidental_transcript_lookalikes_stay_retryable(self) -> None:
        # 'api key' appears in web-research content; only the CLI's own
        # 'invalid api key' phrasing is terminal.
        error = get_adapter("claude").classify_failure("page mentions an api key signup form")
        self.assertEqual(error.code, "unknown")
        self.assertTrue(error.retryable)

    def test_empty_and_none_like_text_is_retryable_unknown(self) -> None:
        for text in ("", "   "):
            error = get_adapter("claude").classify_failure(text)
            self.assertEqual(error.code, "unknown")
            self.assertTrue(error.retryable)


if __name__ == "__main__":
    unittest.main()

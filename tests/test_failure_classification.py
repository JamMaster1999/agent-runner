#!/usr/bin/env python3
"""Characterization tests for the harness adapters' failure classification.

Stage-3 vocabulary: marker codes ARE outcome words. Markers are matched
only against CLI-owned error text; terminal proof exists only for ``auth``
(auth expiry / billing / quota — fails fast and alerts); ``rate_limited``
and invalid-invocation ``spawn_failure`` stay retryable evidence; anything
unmatched — including ambiguous or empty text — classifies ``infra``.
Both adapters deliberately carry identical tables: the pre-adapter code
matched one shared list for both CLIs, and splitting per dialect would be
a behavior change, not a move.
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

import os as _os

REPO = Path(__file__).resolve().parents[1]
# Runner-repo test header: point the runner's path constants at this repo,
# then put src/ on sys.path when agent_runner is not already importable.
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import outcomes  # noqa: E402
from agent_runner.harness import get_adapter  # noqa: E402

ADAPTERS = (get_adapter("codex"), get_adapter("claude"))


class TerminalMarkerTableTest(unittest.TestCase):
    def test_category_order_is_pinned(self) -> None:
        # Order matters: classification returns the FIRST matching category,
        # and a subscription CLI's "usage limit" text must classify
        # rate_limited before the auth/billing sweep sees it.
        for adapter in ADAPTERS:
            with self.subTest(adapter=adapter.name):
                self.assertEqual(
                    [category for category, _ in adapter.terminal_markers],
                    [outcomes.RATE_LIMITED, outcomes.AUTH, outcomes.SPAWN_FAILURE],
                )

    def test_marker_codes_are_outcome_words(self) -> None:
        for adapter in ADAPTERS:
            for code, _ in adapter.terminal_markers:
                self.assertIn(code, outcomes.OUTCOMES)

    def test_every_marker_is_lowercase(self) -> None:
        # Matching lower-cases the input text; a mixed-case marker would
        # silently never match.
        for adapter in ADAPTERS:
            for _, markers in adapter.terminal_markers:
                for marker in markers:
                    self.assertEqual(marker, marker.lower())

    def test_tables_are_identical_across_adapters(self) -> None:
        codex, claude = ADAPTERS
        self.assertEqual(codex.terminal_markers, claude.terminal_markers)


class ClassifyFailureTest(unittest.TestCase):
    def test_each_marker_maps_to_its_outcome_code(self) -> None:
        for adapter in ADAPTERS:
            for code, markers in adapter.terminal_markers:
                for marker in markers:
                    text = f"CLI reported: {marker.upper()} — see logs"
                    with self.subTest(adapter=adapter.name, marker=marker):
                        error = adapter.classify_failure(text)
                        self.assertEqual(error.code, code)
                        self.assertEqual(error.details, text)

    def test_auth_is_the_only_terminal_class(self) -> None:
        adapter = get_adapter("codex")
        auth = adapter.classify_failure("oauth token has expired")
        self.assertEqual(auth.code, outcomes.AUTH)
        self.assertFalse(auth.retryable)
        self.assertTrue(auth.alert)
        limited = adapter.classify_failure("429: too many requests")
        self.assertEqual(limited.code, outcomes.RATE_LIMITED)
        self.assertTrue(limited.retryable)
        self.assertFalse(limited.alert)
        invocation = adapter.classify_failure("error: unknown option '--frobnicate'")
        self.assertEqual(invocation.code, outcomes.SPAWN_FAILURE)
        self.assertTrue(invocation.retryable)

    def test_billing_text_classifies_auth(self) -> None:
        # The ruled vocabulary folds billing/quota into auth: both mean the
        # account cannot pay for retries, so both fail fast.
        for text in (
            "billing_error: payment required",
            "your credit balance is too low",
            "insufficient_quota for this request",
        ):
            error = get_adapter("claude").classify_failure(text)
            self.assertEqual(error.code, outcomes.AUTH)
            self.assertFalse(error.retryable)

    def test_usage_limit_wins_over_login_text(self) -> None:
        # First category wins: a subscription window message mentioning
        # /login must back off, not fail fast.
        error = get_adapter("codex").classify_failure(
            "usage limit reached — please run /login or wait for the window"
        )
        self.assertEqual(error.code, outcomes.RATE_LIMITED)

    def test_session_limit_text_classifies_rate_limited(self) -> None:
        # The Claude subscription cap's synthetic turn (live tier,
        # 2026-08-21): a window message, so it backs off — never infra.
        for text in (
            "claude result success: You've hit your session limit · resets 8:50pm (UTC)",
            "You've hit your limit · resets 5pm",
        ):
            error = get_adapter("claude").classify_failure(text)
            self.assertEqual(error.code, outcomes.RATE_LIMITED)
            self.assertTrue(error.retryable)

    def test_claude_session_limit_text_names_the_reset(self) -> None:
        # The CLI renders the reset as a bare clock time inside a day (the
        # next such moment), a date beyond it, minutes dropped on the hour.
        adapter = get_adapter("claude")
        now = datetime(2026, 9, 1, 21, 4, tzinfo=timezone.utc)
        for text, expected in (
            ("You've hit your session limit · resets 11:20pm (UTC)", datetime(2026, 9, 1, 23, 20, tzinfo=timezone.utc)),
            ("You've hit your session limit · resets 2:20pm (UTC)", datetime(2026, 9, 2, 14, 20, tzinfo=timezone.utc)),
            ("You've hit your limit · resets 3pm (UTC)", datetime(2026, 9, 2, 15, 0, tzinfo=timezone.utc)),
            ("You've hit your weekly limit · resets Sep 5, 6:07pm (UTC)", datetime(2026, 9, 5, 18, 7, tzinfo=timezone.utc)),
            ("You've hit your weekly limit · resets Jan 2, 2027, 6pm (UTC)", datetime(2027, 1, 2, 18, 0, tzinfo=timezone.utc)),
        ):
            self.assertEqual(adapter.reset_time_in(text, now=now), expected, text)
        error = adapter.classify("claude result success: You've hit your session limit · resets 8:50pm (UTC)")
        self.assertEqual(error.code, outcomes.RATE_LIMITED)
        self.assertIsNotNone(error.resets_at)
        self.assertGreater(error.resets_at, datetime.now(timezone.utc))
        self.assertEqual((error.resets_at.hour, error.resets_at.minute), (20, 50))

    def test_ambiguous_text_is_retryable_infra(self) -> None:
        text = "network flake: connection reset by peer (HTTP 403 from registrar page)"
        adapter = get_adapter("codex")
        # The evidence slot reports no proof; the default supplies the
        # retryable 'infra' judgment.
        self.assertIsNone(adapter.classify(text))
        error = adapter.classify_failure(text)
        self.assertEqual(error.code, outcomes.INFRA)
        self.assertTrue(error.retryable)
        self.assertFalse(error.alert)
        self.assertEqual(error.details, text)
        self.assertEqual(str(error), "codex attempt failed")

    def test_incidental_transcript_lookalikes_stay_retryable(self) -> None:
        # 'api key' appears in web-research content; only the CLI's own
        # 'invalid api key' phrasing is terminal.
        error = get_adapter("claude").classify_failure("page mentions an api key signup form")
        self.assertEqual(error.code, outcomes.INFRA)
        self.assertTrue(error.retryable)

    def test_empty_and_none_like_text_is_retryable_infra(self) -> None:
        for text in ("", "   "):
            error = get_adapter("claude").classify_failure(text)
            self.assertEqual(error.code, outcomes.INFRA)
            self.assertTrue(error.retryable)


if __name__ == "__main__":
    unittest.main()


class ResetTimeParsing(unittest.TestCase):
    """The reset moment named in a subscription CLI's limit text reaches
    RunnerError.resets_at; text naming none leaves it None."""

    def test_codex_usage_limit_text_names_the_reset(self) -> None:
        adapter = get_adapter("codex")
        error = adapter.classify(
            "You've hit your usage limit. Visit https://chatgpt.com/codex/settings/usage "
            "to purchase more credits or try again at Sep 5th, 2026 6:07 PM."
        )
        self.assertIsNotNone(error)
        self.assertEqual(error.code, outcomes.RATE_LIMITED)
        self.assertIsNotNone(error.resets_at)
        local = error.resets_at.astimezone()
        self.assertEqual(
            (local.year, local.month, local.day, local.hour, local.minute),
            (2026, 9, 5, 18, 7),
        )

    def test_limit_text_without_a_reset_leaves_none(self) -> None:
        adapter = get_adapter("codex")
        error = adapter.classify("Rate limit reached, please slow down")
        self.assertIsNotNone(error)
        self.assertEqual(error.code, outcomes.RATE_LIMITED)
        self.assertIsNone(error.resets_at)

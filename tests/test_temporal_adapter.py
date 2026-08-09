#!/usr/bin/env python3
"""The Temporal activity wrapper (matrix rows C1, C2, C4, C5): heartbeat
details carry session_ref + progress, the resume budget falls back to a
fresh session, and outcomes map to the ruled retry decisions.

Needs the ``temporalio`` extra; the whole module skips cleanly without it
(the core CI job runs Temporal-less by design).
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest import mock

import os as _os

REPO = Path(__file__).resolve().parents[1]
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

try:
    import temporalio  # noqa: F401

    HAVE_TEMPORALIO = True
except ImportError:
    HAVE_TEMPORALIO = False

if HAVE_TEMPORALIO:
    from temporalio.exceptions import ApplicationError
    from temporalio.testing import ActivityEnvironment

    from agent_runner import outcomes
    from agent_runner.runtime import AttemptReport, RunSpec, Verdict
    from agent_runner.temporal import (
        CheckpointSpec,
        TemporalRunConfig,
        application_error_for,
        recommended_retry_policy,
        run_agent_attempt,
    )
    from agent_runner.temporal import activity as activity_module

FAKE_CLI = REPO / "tests" / "fake_cli" / "fake-cli"


@unittest.skipUnless(HAVE_TEMPORALIO, "temporalio not installed (core CI is Temporal-less)")
class RetryMappingTest(unittest.TestCase):
    """C1 / C5: the outcome-to-retry table."""

    def test_rate_limited_backs_off_long_and_free(self) -> None:
        report = AttemptReport(outcome=outcomes.RATE_LIMITED, error="throttled")
        error = application_error_for(report, rate_limit_backoff=timedelta(minutes=45))
        self.assertEqual(error.type, outcomes.RATE_LIMITED)
        self.assertFalse(error.non_retryable)
        self.assertEqual(error.next_retry_delay, timedelta(minutes=45))

    def test_auth_fails_fast(self) -> None:
        report = AttemptReport(outcome=outcomes.AUTH, error="not logged in")
        error = application_error_for(report, rate_limit_backoff=timedelta(minutes=1))
        self.assertEqual(error.type, outcomes.AUTH)
        self.assertTrue(error.non_retryable)

    def test_infra_and_friends_retry_ordinarily(self) -> None:
        for outcome in (outcomes.INFRA, outcomes.SPAWN_FAILURE, outcomes.TIMEOUT,
                        outcomes.INVALID_SCHEMA):
            error = application_error_for(
                AttemptReport(outcome=outcome), rate_limit_backoff=timedelta(minutes=1)
            )
            self.assertEqual(error.type, outcome)
            self.assertFalse(error.non_retryable)
            self.assertIsNone(error.next_retry_delay)

    def test_recommended_policy_blocks_terminal_outcomes(self) -> None:
        policy = recommended_retry_policy()
        self.assertEqual(policy.non_retryable_error_types, [outcomes.AUTH])


@unittest.skipUnless(HAVE_TEMPORALIO, "temporalio not installed (core CI is Temporal-less)")
class ResumeDecisionTest(unittest.TestCase):
    """C2 / C4: session_ref rides heartbeat details; the budget caps
    resumes of one session and then falls back fresh."""

    def decide(self, prior, budget=3):
        return activity_module.resume_decision(prior, budget)

    def test_first_attempt_starts_fresh(self) -> None:
        self.assertEqual(self.decide(None), (None, 0, False))

    def test_prior_session_is_resumed_and_counted(self) -> None:
        session, count, fallback = self.decide(
            {"session_ref": "th_9", "resume_count": 1}
        )
        self.assertEqual((session, count, fallback), ("th_9", 2, False))

    def test_budget_exhausted_falls_back_fresh(self) -> None:
        session, count, fallback = self.decide(
            {"session_ref": "th_9", "resume_count": 3}
        )
        self.assertEqual((session, count, fallback), (None, 0, True))

    def test_prior_without_session_is_fresh(self) -> None:
        self.assertEqual(
            self.decide({"session_ref": None, "resume_count": 2}), (None, 0, False)
        )


@unittest.skipUnless(HAVE_TEMPORALIO, "temporalio not installed (core CI is Temporal-less)")
class ActivityRunTest(unittest.TestCase):
    """The wrapper end to end inside an ActivityEnvironment, CLI stubbed by
    the fake-CLI rig."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.calls = self.tmp / "calls"
        self.scenario_path = self.tmp / "scenario.json"
        self.workdir = self.tmp / "work"
        patcher = mock.patch.dict(
            _os.environ,
            {
                "AGENT_RUNNER_PROJECT_ROOT": str(self.tmp),
                "RUNNER_CODEX_CLI": str(FAKE_CLI),
                "FAKE_CLI_SCENARIO": str(self.scenario_path),
                "FAKE_CLI_CALLS": str(self.calls),
            },
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def spec(self) -> RunSpec:
        return RunSpec(
            key="fixture__research__codex",
            harness="codex",
            agent_ref="fixture-agent",
            agent_config={"model": "fixture-model"},
            required_env=("FAKE_CLI_SCENARIO", "FAKE_CLI_CALLS"),
        )

    def validator(self):
        def validate(workdir: Path) -> Verdict:
            try:
                data = json.loads((Path(workdir) / "out.json").read_text())
            except (OSError, ValueError):
                return Verdict(valid=False, message="missing out.json")
            return Verdict(valid=True, data=data)

        return validate

    def run_activity(self, coro_fn, *args, **kwargs):
        env = ActivityEnvironment()
        heartbeats: list = []
        env.on_heartbeat = lambda *details: heartbeats.append(details)
        result = asyncio.run(env.run(coro_fn, *args, **kwargs))
        return result, heartbeats

    def test_valid_run_heartbeats_session_ref(self) -> None:
        self.scenario_path.write_text(
            json.dumps(
                [
                    {
                        "emit": [{"type": "thread.started", "thread_id": "th_1"}],
                        "write": [
                            {"path": str(self.workdir / "out.json"), "text": '{"ok": true}'}
                        ],
                        "exit": 0,
                    }
                ]
            )
        )

        async def act():
            return await run_agent_attempt(
                self.spec(),
                "task",
                self.workdir,
                validate=self.validator(),
                config=TemporalRunConfig(heartbeat_seconds=0.05),
            )

        report, heartbeats = self.run_activity(act)
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertEqual(report.session_ref, "th_1")
        # The final heartbeat carries the session_ref for the next attempt.
        final = heartbeats[-1][0]
        self.assertEqual(final["session_ref"], "th_1")
        self.assertEqual(final["resume_count"], 0)

    def test_rate_limited_raises_typed_with_long_free_backoff(self) -> None:
        # Matrix C1, whole row: classified rate_limited, then the retry
        # mapping backs off long via next_retry_delay.
        self.scenario_path.write_text(
            json.dumps(
                [
                    {
                        "emit": [{"type": "error", "message": "Rate limit reached"}],
                        "exit": 1,
                    }
                ]
            )
        )

        async def act():
            return await run_agent_attempt(
                self.spec(),
                "task",
                self.workdir,
                validate=self.validator(),
                config=TemporalRunConfig(
                    heartbeat_seconds=0.05, rate_limit_backoff=timedelta(minutes=30)
                ),
            )

        with self.assertRaises(ApplicationError) as caught:
            self.run_activity(act)
        self.assertEqual(caught.exception.type, outcomes.RATE_LIMITED)
        self.assertEqual(caught.exception.next_retry_delay, timedelta(minutes=30))

    def test_prior_heartbeat_details_resume_the_session(self) -> None:
        # Matrix C2: session_ref from heartbeat details -> the next attempt
        # resumes the session (here: on the same rig, asserting the resume
        # command shape and the incremented count in details).
        self.scenario_path.write_text(
            json.dumps(
                [
                    {
                        "write": [
                            {"path": str(self.workdir / "out.json"), "text": '{"ok": true}'}
                        ],
                        "exit": 0,
                    }
                ]
            )
        )

        async def act():
            return await run_agent_attempt(
                self.spec(),
                "task",
                self.workdir,
                validate=self.validator(),
                config=TemporalRunConfig(heartbeat_seconds=0.05),
            )

        with mock.patch.object(
            activity_module,
            "prior_heartbeat_details",
            return_value={"session_ref": "th_9", "resume_count": 0},
        ):
            report, heartbeats = self.run_activity(act)
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertTrue(report.resumed)
        call = json.loads((self.calls / "call-00.json").read_text())
        self.assertIn("resume", call["argv"])
        self.assertIn("th_9", call["argv"])
        self.assertEqual(heartbeats[-1][0]["resume_count"], 1)

    def test_resume_budget_exhausted_falls_back_fresh(self) -> None:
        # Matrix C4: budget spent -> fresh session, recorded.
        self.scenario_path.write_text(
            json.dumps(
                [
                    {
                        "emit": [{"type": "thread.started", "thread_id": "th_new"}],
                        "write": [
                            {"path": str(self.workdir / "out.json"), "text": '{"ok": true}'}
                        ],
                        "exit": 0,
                    }
                ]
            )
        )

        async def act():
            return await run_agent_attempt(
                self.spec(),
                "task",
                self.workdir,
                validate=self.validator(),
                config=TemporalRunConfig(heartbeat_seconds=0.05, resume_budget=3),
            )

        with mock.patch.object(
            activity_module,
            "prior_heartbeat_details",
            return_value={"session_ref": "th_old", "resume_count": 3},
        ):
            report, heartbeats = self.run_activity(act)
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertFalse(report.resumed)
        call = json.loads((self.calls / "call-00.json").read_text())
        self.assertNotIn("resume", call["argv"])
        # The fresh session's ref rides out with a reset count.
        final = heartbeats[-1][0]
        self.assertEqual(final["session_ref"], "th_new")
        self.assertEqual(final["resume_count"], 0)

    def test_checkpoint_folder_prepared_and_cross_term_discarded(self) -> None:
        # Matrix B3 (adapter half): the folder is prepared before spawn and
        # a stale term's checkpoint is discarded loudly before any resume.
        from agent_runner import workdirs

        stale_dir = workdirs.checkpoint_dir(self.tmp / "vol", "scrape", "2027SPRING")
        (stale_dir / "progress.json").write_text(
            json.dumps({"term": "2026FALL", "pages": 9})
        )
        self.scenario_path.write_text(
            json.dumps(
                [
                    {
                        "write": [
                            {"path": str(self.workdir / "out.json"), "text": '{"ok": true}'}
                        ],
                        "exit": 0,
                    }
                ]
            )
        )

        async def act():
            return await run_agent_attempt(
                self.spec(),
                "task",
                self.workdir,
                validate=self.validator(),
                checkpoint=CheckpointSpec(
                    root=self.tmp / "vol", child="scrape", term="2027SPRING"
                ),
                config=TemporalRunConfig(heartbeat_seconds=0.05),
            )

        report, _ = self.run_activity(act)
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertFalse((stale_dir / "progress.json").exists())


if __name__ == "__main__":
    unittest.main()

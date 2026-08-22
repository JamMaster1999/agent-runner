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
from datetime import datetime, timedelta, timezone
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
    from agent_runner.runtime import AttemptReport, RunSpec, Usage, Verdict
    from agent_runner.temporal import (
        CheckpointSpec,
        TemporalRunConfig,
        application_error_for,
        recommended_retry_policy,
        run_agent_attempt,
    )
    from agent_runner.temporal import activity as activity_module
    from agent_runner.temporal import retry as retry_module

FAKE_CLI = REPO / "tests" / "fake_cli" / "fake-cli"
CAP = timedelta(hours=6)


@unittest.skipUnless(HAVE_TEMPORALIO, "temporalio not installed (core CI is Temporal-less)")
class RetryMappingTest(unittest.TestCase):
    """C1 / C5: the outcome-to-retry table."""

    def test_rate_limited_backs_off_long_and_free(self) -> None:
        report = AttemptReport(outcome=outcomes.RATE_LIMITED, error="throttled")
        error = application_error_for(report, rate_limit_backoff=timedelta(minutes=45), reset_cap=CAP)
        self.assertEqual(error.type, outcomes.RATE_LIMITED)
        self.assertFalse(error.non_retryable)
        self.assertEqual(error.next_retry_delay, timedelta(minutes=45))

    def test_failure_details_carry_the_whole_report(self) -> None:
        # The error that reaches history is the report, not its first line:
        # this attempt's full record (outcome, the CLI-owned text, the
        # session to resume, timing, spend) and the attempts before it (the
        # b51 lesson, 2026-08-21 — the session limit text never left the
        # worker). Two explicit keys, nothing stated twice.
        own = {"attempt": 2, "outcome": "infra", "detail": "You've hit your session limit", "session_ref": "sess-1"}
        report = AttemptReport(
            outcome=outcomes.INFRA,
            error="k: claude attempt failed",
            attempts=({"attempt": 1, "outcome": "stalled"}, own),
        )
        error = application_error_for(report, rate_limit_backoff=timedelta(minutes=1), reset_cap=CAP)
        self.assertEqual(
            error.details[0],
            {"attempt": own, "attempts": [{"attempt": 1, "outcome": "stalled"}]},
        )
        # A report the core runner built carries no record yet.
        bare = application_error_for(AttemptReport(outcome=outcomes.INFRA), rate_limit_backoff=timedelta(minutes=1), reset_cap=CAP)
        self.assertEqual(bare.details[0], {"attempt": None, "attempts": []})

    def test_rate_limited_waits_for_the_reset_when_the_cli_named_one(self) -> None:
        # A known reset time beats the flat backoff: the retry lands just
        # after the limit lifts instead of probing every quarter hour.
        now = datetime(2026, 8, 21, 20, 0, tzinfo=timezone.utc)
        delay = retry_module.rate_limit_delay
        default, cap = timedelta(minutes=15), timedelta(hours=6)
        self.assertEqual(delay(None, default, cap, now), default)
        self.assertEqual(delay(now + timedelta(minutes=50), default, cap, now), timedelta(minutes=50))
        # Already past (or a few seconds out): retry promptly, not instantly.
        self.assertEqual(delay(now - timedelta(hours=1), default, cap, now), retry_module.RESET_DELAY_FLOOR)
        # The cap is the caller's: a waiting retry holds its slot.
        self.assertEqual(delay(now + timedelta(days=3), default, cap, now), cap)
        self.assertEqual(delay(now + timedelta(hours=2), default, timedelta(hours=1), now), timedelta(hours=1))

        report = AttemptReport(
            outcome=outcomes.RATE_LIMITED,
            error="capped",
            resets_at=datetime.now(timezone.utc) + timedelta(hours=2),
        )
        error = application_error_for(report, rate_limit_backoff=default, reset_cap=cap)
        self.assertFalse(error.non_retryable)
        self.assertGreater(error.next_retry_delay, timedelta(hours=1, minutes=59))
        self.assertLessEqual(error.next_retry_delay, timedelta(hours=2))

    def test_a_reset_past_the_retry_window_fails_at_once(self) -> None:
        # The retry could never start and finish before schedule-to-close
        # fires, so idling until then only holds the slot: fail now,
        # non-retryable, naming both times.
        now = datetime(2026, 8, 22, 8, 0, tzinfo=timezone.utc)
        resets_at = datetime(2026, 8, 22, 9, 0, tzinfo=timezone.utc)
        report = AttemptReport(outcome=outcomes.RATE_LIMITED, error="k: capped", resets_at=resets_at)
        error = application_error_for(
            report, rate_limit_backoff=timedelta(minutes=15), reset_cap=CAP,
            retry_by=resets_at - timedelta(minutes=1), now=now,
        )
        self.assertEqual(error.type, outcomes.RATE_LIMITED)
        self.assertTrue(error.non_retryable)
        self.assertIsNone(error.next_retry_delay)
        self.assertIn("2026-08-22T09:00:00+00:00", error.message)
        self.assertIn("(2026-08-22T08:59:00+00:00)", error.message)
        # A reset inside the window waits as usual.
        error = application_error_for(
            report, rate_limit_backoff=timedelta(minutes=15), reset_cap=CAP,
            retry_by=resets_at + timedelta(hours=1), now=now,
        )
        self.assertFalse(error.non_retryable)
        self.assertEqual(error.next_retry_delay, timedelta(hours=1))
        # A window already closed is the server's timeout to report: a bare
        # ActivityEnvironment (epoch scheduled_time, 1s schedule-to-close)
        # must never turn a rate limit non-retryable.
        error = application_error_for(
            report, rate_limit_backoff=timedelta(minutes=15), reset_cap=CAP,
            retry_by=now - timedelta(days=1), now=now,
        )
        self.assertFalse(error.non_retryable)

    def test_activity_deadline_is_scheduled_time_plus_schedule_to_close(self) -> None:
        import dataclasses

        info = ActivityEnvironment().info
        scheduled = datetime(2026, 8, 22, 8, 0, tzinfo=timezone.utc)
        info = dataclasses.replace(info, scheduled_time=scheduled, schedule_to_close_timeout=timedelta(hours=8))
        self.assertEqual(activity_module.activity_deadline(info), scheduled + timedelta(hours=8))
        for absent in (None, timedelta(0)):
            info = dataclasses.replace(info, schedule_to_close_timeout=absent)
            self.assertIsNone(activity_module.activity_deadline(info))

    def test_heartbeat_with_ten_maximal_records_stays_small(self) -> None:
        # Temporal caps heartbeat details at 2 MB; the record bounds its own
        # texts by bytes, so ten of the worst stay far under it.
        loud = AttemptReport(
            outcome=outcomes.INFRA,
            error="é" * 10_000,
            detail="x" * 100_000,
            session_ref="s" * 4000,
            resets_at=datetime.now(timezone.utc),
        )
        state = activity_module._HeartbeatState(session_ref="s" * 4000, progress={"message": "m" * 300})
        for number in range(1, 30):
            state.record(activity_module.attempt_record(number, loud, "2026-08-22T08:00:00+00:00", "2026-08-22T09:00:00+00:00"))
        payload = state.payload()
        self.assertEqual(len(payload["attempts"]), activity_module.ATTEMPTS_KEPT)
        record = payload["attempts"][-1]
        # error keeps its head (the key and the cause), detail its tail (the
        # CLI's last words), the ref is bounded too.
        self.assertLessEqual(len(record["error"].encode()), activity_module.RECORD_TEXT_LIMIT)
        self.assertTrue(record["error"].startswith("é"))
        self.assertLessEqual(len(record["detail"].encode()), activity_module.RECORD_TEXT_LIMIT)
        self.assertTrue(record["detail"].endswith("x"))
        self.assertLessEqual(len(record["session_ref"].encode()), activity_module.RECORD_REF_LIMIT)
        self.assertLess(len(json.dumps(payload).encode()), 32_000)
        # A worker that died at attempt 1000 names only the newest gap.
        self.assertEqual(len(activity_module.vanished_attempts(1000, [], None)), activity_module.ATTEMPTS_KEPT)

    def test_auth_fails_fast(self) -> None:
        report = AttemptReport(outcome=outcomes.AUTH, error="not logged in")
        error = application_error_for(report, rate_limit_backoff=timedelta(minutes=1), reset_cap=CAP)
        self.assertEqual(error.type, outcomes.AUTH)
        self.assertTrue(error.non_retryable)

    def test_infra_and_friends_retry_ordinarily(self) -> None:
        for outcome in (outcomes.INFRA, outcomes.SPAWN_FAILURE, outcomes.TIMEOUT,
                        outcomes.INVALID_SCHEMA):
            error = application_error_for(
                AttemptReport(outcome=outcome), rate_limit_backoff=timedelta(minutes=1), reset_cap=CAP
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

    def test_fingerprint_names_the_prompt_not_the_run(self) -> None:
        """Same prompt -> same fingerprint; any body or config change -> a
        different one (a minor bump kills resume, ruled 2026-08-16)."""
        from agent_runner.harness.base import AgentDef

        a = AgentDef(name="x", description="d", config={"model": "m"}, body="prompt")
        same = AgentDef(name="y", description="other", config={"model": "m"}, body="prompt")
        touched = AgentDef(name="x", description="d", config={"model": "m"}, body="prompt v2")
        reconfigured = AgentDef(name="x", description="d", config={"model": "m2"}, body="prompt")
        fp = activity_module.agent_fingerprint
        self.assertEqual(fp(a), fp(same))
        self.assertNotEqual(fp(a), fp(touched))
        self.assertNotEqual(fp(a), fp(reconfigured))
        self.assertIsNone(fp(None))


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
                "RUNNER_CLAUDE_CLI": str(FAKE_CLI),
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

    def run_activity(self, coro_fn, *args, heartbeats: list | None = None, info=None, **kwargs):
        env = ActivityEnvironment()
        if info is not None:
            env.info = info
        heartbeats = [] if heartbeats is None else heartbeats
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

    def test_failed_attempt_is_recorded_for_the_next_one(self) -> None:
        # A failure lands in the final heartbeat's ``attempts`` — the next
        # attempt inherits it, and the error's details name it too.
        self.scenario_path.write_text(
            json.dumps([{"emit": [{"type": "error", "message": "boom"}], "exit": 1}])
        )

        async def act():
            return await run_agent_attempt(
                self.spec(),
                "task",
                self.workdir,
                validate=self.validator(),
                config=TemporalRunConfig(heartbeat_seconds=0.05),
            )

        prior = {
            "session_ref": "th_1",
            "resume_count": 0,
            "attempts": [{"attempt": 1, "outcome": "stalled", "error": "silent"}],
        }
        heartbeats: list = []
        with mock.patch.object(activity_module, "prior_heartbeat_details", return_value=prior):
            with self.assertRaises(ApplicationError) as caught:
                self.run_activity(act, heartbeats=heartbeats)
        details = caught.exception.details[0]
        # This attempt's full record under one key, the ones before it
        # under the other; the heartbeat carries them all, this one last.
        self.assertEqual(details["attempts"], prior["attempts"])
        own = details["attempt"]
        self.assertEqual(own["outcome"], outcomes.INFRA)
        self.assertTrue(own["resumed"])
        self.assertEqual(own["session_ref"], "th_1")
        self.assertEqual(heartbeats[-1][0]["attempts"][-1], own)

    def test_vanished_attempts_are_named_from_the_attempt_number(self) -> None:
        # A worker killed mid-attempt reports nothing; attempt 4 arriving
        # with attempts 1 and 3 on record means 2 died with its worker.
        recorded = [{"attempt": 1, "outcome": "stalled"}, {"attempt": 3, "outcome": "infra"}]
        (gap,) = activity_module.vanished_attempts(4, recorded, None)
        self.assertEqual(gap["attempt"], 2)
        self.assertEqual(gap["outcome"], outcomes.INFRA)
        self.assertIn("worker died", gap["error"])
        self.assertEqual(set(gap), set(activity_module.RECORD_KEYS))
        self.assertEqual(set(activity_module.attempt_record(1, AttemptReport(outcome="infra"), "t", "t")), set(activity_module.RECORD_KEYS))
        # One shape: an unknown spend is zeros, the same dict as a reported one.
        self.assertEqual(gap["usage"], Usage().as_dict())
        self.assertIsNone(gap["session_ref"])
        self.assertEqual(activity_module.vanished_attempts(1, [], None), [])
        self.assertEqual(activity_module.vanished_attempts(3, recorded[:1] + [{"attempt": 2}], None), [])

    def test_a_vanished_attempt_keeps_what_its_last_heartbeat_knew(self) -> None:
        # The dead worker's heartbeat carried the session it was in and the
        # running usage: attempt 2's record takes them, so attempt 3's
        # baseline on the same thread is 2500, not attempt 1's 1000 — the
        # dead attempt's 1500 is charged to it, not to the next one.
        one = {"attempt": 1, "outcome": "stalled", "session_ref": "th_1",
               "usage": Usage(1000).as_dict(), "session_usage": Usage(1000).as_dict()}
        prior = {
            "attempt": 2,
            "session_ref": "th_1",
            "usage": Usage(1500).as_dict(),
            "session_usage": Usage(2500).as_dict(),
            "attempts": [one],
        }
        (gap,) = activity_module.vanished_attempts(3, [one], prior)
        self.assertEqual(gap["session_ref"], "th_1")
        self.assertEqual(gap["usage"], Usage(1500).as_dict())
        self.assertEqual(gap["session_usage"], Usage(2500).as_dict())
        self.assertEqual(activity_module.session_usage_before("th_1", [one, gap]), Usage(2500))
        # A heartbeat from an earlier attempt (the dead one never beat) says
        # nothing about the gap.
        (gap,) = activity_module.vanished_attempts(3, [one], {**prior, "attempt": 1})
        self.assertIsNone(gap["session_ref"])
        self.assertEqual(activity_module.session_usage_before("th_1", [one, gap]), Usage(1000))

    def test_a_fresh_session_fallback_starts_its_usage_from_nothing(self) -> None:
        # Attempt 2 asked to resume th_old (baseline 1000) but the CLI
        # opened th_new: the session total is th_new's alone, in the
        # heartbeat and in the record — so a worker that dies after this
        # point leaves attempt 3 a baseline of th_new's own spend, never
        # th_old's.
        self.scenario_path.write_text(
            json.dumps(
                [
                    {
                        "emit": [
                            {"type": "thread.started", "thread_id": "th_new"},
                            {"type": "turn.completed", "usage": {"input_tokens": 50, "output_tokens": 5}},
                        ],
                        "write": [{"path": str(self.workdir / "out.json"), "text": '{"ok": true}'}],
                        "exit": 0,
                    }
                ]
            )
        )

        async def act():
            return await run_agent_attempt(
                self.spec(), "task", self.workdir,
                validate=self.validator(), config=TemporalRunConfig(heartbeat_seconds=0.05),
            )

        old = {"attempt": 1, "outcome": "stalled", "session_ref": "th_old",
               "usage": Usage(1000).as_dict(), "session_usage": Usage(1000).as_dict()}
        with mock.patch.object(
            activity_module, "prior_heartbeat_details",
            return_value={"session_ref": "th_old", "resume_count": 0, "attempts": [old]},
        ):
            report, heartbeats = self.run_activity(act)
        own = report.attempts[-1]
        self.assertEqual(own["session_ref"], "th_new")
        self.assertEqual(own["usage"], Usage(50, 0, 0, 5).as_dict())
        self.assertEqual(own["session_usage"], Usage(50, 0, 0, 5).as_dict())
        final = heartbeats[-1][0]
        self.assertEqual(final["session_ref"], "th_new")
        self.assertEqual(final["session_usage"], Usage(50, 0, 0, 5).as_dict())
        # Had the worker died right after thread.started, the heartbeat
        # would already have said th_new stood at nothing.
        self.assertEqual(
            activity_module.session_usage_before("th_new", [old] + activity_module.vanished_attempts(3, [old], {"attempt": 2, "session_ref": "th_new", "usage": Usage().as_dict(), "session_usage": Usage().as_dict()})),
            Usage(),
        )

    def test_rate_limited_with_a_reset_time_end_to_end(self) -> None:
        # The whole path under the activity: the typed rate_limit_event
        # decides the outcome, its resetsAt reaches the record and the
        # retry delay, and the activity's own window (scheduled_time +
        # schedule_to_close − margin) decides whether waiting is worth it.
        import dataclasses

        resets_at = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(hours=3)
        self.scenario_path.write_text(
            json.dumps(
                [
                    {
                        "emit": [
                            {"type": "system", "subtype": "init", "session_id": "sess-cap", "model": "m"},
                            {
                                "type": "rate_limit_event",
                                "rate_limit_info": {"status": "rejected", "resetsAt": int(resets_at.timestamp()), "rateLimitType": "five_hour"},
                                "session_id": "sess-cap",
                            },
                        ],
                        "sleep": 30,
                        "exit": 0,
                    }
                ]
            )
        )
        from agent_runner.harness.base import AgentDef

        agent = AgentDef(name="fixture-claude-agent", description="f", config={}, body="Fixture body.\n")
        spec = RunSpec(key="fixture__claude", harness="claude", required_env=("FAKE_CLI_SCENARIO", "FAKE_CLI_CALLS"))

        async def act():
            return await run_agent_attempt(
                spec, "task", self.workdir, agent=agent, timeout_minutes=0.5,
                config=TemporalRunConfig(heartbeat_seconds=0.05, rate_limit_reset_margin=timedelta(minutes=15)),
            )

        def info(window: timedelta):
            return dataclasses.replace(
                ActivityEnvironment().info,
                scheduled_time=datetime.now(timezone.utc),
                schedule_to_close_timeout=window,
            )

        heartbeats: list = []
        with self.assertRaises(ApplicationError) as caught:
            self.run_activity(act, heartbeats=heartbeats, info=info(timedelta(hours=8)))
        error = caught.exception
        self.assertEqual(error.type, outcomes.RATE_LIMITED)
        self.assertFalse(error.non_retryable)
        self.assertGreater(error.next_retry_delay, timedelta(hours=2, minutes=59))
        self.assertLessEqual(error.next_retry_delay, timedelta(hours=3))
        self.assertEqual(error.details[0]["attempt"]["resets_at"], resets_at.isoformat(timespec="seconds"))
        self.assertEqual(heartbeats[-1][0]["attempts"][-1]["outcome"], outcomes.RATE_LIMITED)

        (self.calls / "count").unlink()
        with self.assertRaises(ApplicationError) as caught:
            self.run_activity(act, info=info(timedelta(hours=3)))
        self.assertEqual(caught.exception.type, outcomes.RATE_LIMITED)
        self.assertTrue(caught.exception.non_retryable)
        self.assertIn("past the last retry window", caught.exception.message)

    def test_session_usage_baseline_survives_a_newer_record_shape(self) -> None:
        # A mid-run redeploy may replay a record written by a newer Usage:
        # unknown keys are ignored instead of crashing every later attempt.
        entry = {"attempt": 1, "session_ref": "th_1", "session_usage": {"tok_input": 5, "tok_images": 2}}
        self.assertEqual(activity_module.session_usage_before("th_1", [entry]), Usage(tok_input=5))

    def test_every_attempt_leaves_a_typed_record(self) -> None:
        # Success included: the record names what the attempt spent, when,
        # and where that left the session's total.
        self.scenario_path.write_text(
            json.dumps(
                [
                    {
                        "emit": [
                            {"type": "thread.started", "thread_id": "th_1"},
                            {
                                "type": "turn.completed",
                                "usage": {"input_tokens": 500, "cached_input_tokens": 100, "output_tokens": 50},
                            },
                        ],
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

        attempts = [{"attempt": 1, "outcome": "infra", "error": "died", "session_ref": None}]
        with mock.patch.object(
            activity_module,
            "prior_heartbeat_details",
            return_value={"session_ref": None, "resume_count": 0, "attempts": attempts},
        ):
            report, heartbeats = self.run_activity(act)
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertEqual(list(report.attempts)[0], attempts[0])
        own = report.attempts[-1]
        self.assertEqual(own["attempt"], 1)
        self.assertEqual(own["outcome"], outcomes.VALID)
        self.assertEqual(own["session_ref"], "th_1")
        self.assertFalse(own["resumed"])
        self.assertIsNone(own["resets_at"])
        self.assertLessEqual(own["started_at"], own["ended_at"])
        datetime.fromisoformat(own["ended_at"])
        self.assertEqual(own["usage"]["tok_input"], 500)
        self.assertEqual(own["usage"]["tok_output"], 50)
        self.assertEqual(own["usage"]["cost_usd"], 0.0)
        self.assertEqual(own["session_usage"], own["usage"])
        # The heartbeat carried the running attempt's usage, typed, before
        # the record existed; the final one carries both.
        final = heartbeats[-1][0]
        self.assertEqual(final["attempt"], 1)
        self.assertEqual(final["usage"]["tok_input"], 500)
        self.assertEqual(final["session_usage"]["tok_input"], 500)
        self.assertEqual(final["attempts"], list(report.attempts))

    def test_resumed_attempt_records_its_own_spend_and_the_session_total(self) -> None:
        # Attempt 2's record carries what it spent, and the session's total
        # continues from where the prior record on the same thread left it.
        self.scenario_path.write_text(
            json.dumps(
                [
                    {
                        "emit": [
                            {"type": "thread.started", "thread_id": "th_1"},
                            {
                                "type": "turn.completed",
                                "usage": {"input_tokens": 800, "cached_input_tokens": 300, "output_tokens": 80},
                            },
                        ],
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

        first = {
            "attempt": 1,
            "outcome": "stalled",
            "session_ref": "th_1",
            "usage": Usage(500, 0, 100, 50).as_dict(),
            "session_usage": Usage(500, 0, 100, 50).as_dict(),
        }
        with mock.patch.object(
            activity_module,
            "prior_heartbeat_details",
            return_value={"session_ref": "th_1", "resume_count": 0, "attempts": [first]},
        ):
            report, _ = self.run_activity(act)
        own = report.attempts[-1]
        self.assertTrue(own["resumed"])
        self.assertEqual(own["usage"], Usage(800, 0, 300, 80).as_dict())
        self.assertEqual(own["session_usage"], Usage(1300, 0, 400, 130).as_dict())
        self.assertEqual(report.usage, Usage(800, 0, 300, 80))

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

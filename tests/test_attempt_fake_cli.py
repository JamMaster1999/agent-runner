#!/usr/bin/env python3
"""A-tier attempt tests: spawn / stream / classify / repair through the
real adapters against the fake-CLI rig — zero tokens (test_matrix.md rows
C1, C3, C5, C7 plus the valid, timeout, resume, cancel, and isolation
scenarios).

The rig is ``tests/fake_cli/fake-cli``; the adapters reach it through
their binary overrides (RUNNER_CODEX_CLI / RUNNER_CLAUDE_CLI), so every
test exercises the production spawn path end to end: command build, stdin
prompt delivery, stream parsing, session-ref extraction, outcome
classification, repair follow-ups, and child reaping.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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
from agent_runner.attempt import AttemptCancelled, run_attempt  # noqa: E402
from agent_runner.harness.base import AgentDef  # noqa: E402
from agent_runner.runtime import RunSpec, Verdict  # noqa: E402

FAKE_CLI = REPO / "tests" / "fake_cli" / "fake-cli"


def codex_spec(**overrides) -> RunSpec:
    values = dict(
        key="fixture__research__codex",
        harness="codex",
        agent_ref="fixture-agent",
        agent_config={"model": "fixture-model"},
        task_type="research",
        required_env=("FAKE_CLI_SCENARIO", "FAKE_CLI_CALLS"),
    )
    values.update(overrides)
    return RunSpec(**values)


class FakeCliCase(unittest.TestCase):
    """Shared rig plumbing: a scratch project root, the stub as the codex
    binary, and per-test scenario/call files."""

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

    def scenario(self, calls: list[dict]) -> None:
        self.scenario_path.write_text(json.dumps(calls))

    def recorded_call(self, index: int) -> dict:
        return json.loads((self.calls / f"call-{index:02d}.json").read_text())

    def out_path(self) -> Path:
        return self.workdir / "out.json"

    def json_validator(self):
        """A project-shaped contract closure: out.json must parse and carry
        {"ok": true}; anything else is invalid with a repair message."""

        def validate(workdir: Path) -> Verdict:
            path = Path(workdir) / "out.json"
            try:
                data = json.loads(path.read_text())
            except (OSError, ValueError):
                return Verdict(
                    valid=False,
                    message="out.json missing or unparsable",
                    repair_message="REPAIR: write valid out.json",
                )
            if data.get("ok") is True:
                return Verdict(valid=True, data=data)
            return Verdict(
                valid=False,
                message="ok flag missing",
                repair_message="REPAIR: set ok=true in out.json",
            )

        return validate


class ValidRunTest(FakeCliCase):
    def test_valid_run_streams_usage_session_and_substituted_prompt(self) -> None:
        self.scenario(
            [
                {
                    "emit": [
                        {"type": "thread.started", "thread_id": "th_1"},
                        {
                            "type": "turn.completed",
                            "usage": {
                                "input_tokens": 100,
                                "cached_input_tokens": 40,
                                "output_tokens": 7,
                            },
                        },
                        {
                            "type": "item.completed",
                            "item": {
                                "id": "i1",
                                "type": "agent_message",
                                "text": "PROGRESS: 2/5 — halfway",
                            },
                        },
                    ],
                    "write": [
                        {"path": str(self.out_path()), "text": '{"ok": true}'}
                    ],
                    "exit": 0,
                }
            ]
        )
        events = []
        sessions = []
        report = run_attempt(
            codex_spec(),
            "Do the work. Write to {{RUNNER_OUTPUT_PATH}}/out.json (attempt {{RUNNER_ATTEMPT}}).",
            self.workdir,
            validate=self.json_validator(),
            on_event=events.append,
            on_session=sessions.append,
            run_id="run-1",
            attempt=1,
            poll_seconds=0.05,
        )
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertEqual(report.session_ref, "th_1")
        self.assertEqual(sessions, ["th_1"])
        self.assertEqual(report.data, {"ok": True})
        self.assertEqual(report.usage.tok_input, 100)
        self.assertEqual(report.usage.tok_cache_read, 40)
        self.assertEqual(report.usage.tok_output, 7)
        progress = [e for e in events if e.event == "agent_progress"]
        self.assertEqual((progress[0].current, progress[0].total), (2, 5))
        # The delivered prompt is the task with the closed variable set
        # substituted — and it is what the CLI actually received on stdin.
        prompt = (self.workdir / "prompt.md").read_text()
        self.assertIn(f"Write to {self.workdir}/out.json (attempt 1).", prompt)
        self.assertEqual(self.recorded_call(0)["stdin"], prompt)
        # Fresh session: a plain exec, no resume.
        self.assertNotIn("resume", self.recorded_call(0)["argv"])

    def test_valid_output_beats_nonzero_exit(self) -> None:
        self.scenario(
            [
                {
                    "write": [{"path": str(self.out_path()), "text": '{"ok": true}'}],
                    "stderr": "segfault during shutdown",
                    "exit": 139,
                }
            ]
        )
        report = run_attempt(
            codex_spec(), "task", self.workdir,
            validate=self.json_validator(), poll_seconds=0.05,
        )
        self.assertEqual(report.outcome, outcomes.VALID)


class ClassificationTest(FakeCliCase):
    def run_failing(self, call: dict) -> object:
        self.scenario([call])
        return run_attempt(
            codex_spec(), "task", self.workdir,
            validate=self.json_validator(), poll_seconds=0.05,
        )

    def test_rate_limited_stream_error_classifies_rate_limited(self) -> None:
        # Matrix C1 (classification half; the long free backoff is the
        # temporal layer's, tested there).
        report = self.run_failing(
            {
                "emit": [{"type": "error", "message": "Rate limit reached for requests"}],
                "exit": 1,
            }
        )
        self.assertEqual(report.outcome, outcomes.RATE_LIMITED)
        self.assertIn("rate_limited", report.error)

    def test_auth_failure_classifies_auth(self) -> None:
        # Matrix C5: the CLI's own auth report is terminal proof.
        report = self.run_failing(
            {"stderr": "Not logged in. Please run /login.", "exit": 1}
        )
        self.assertEqual(report.outcome, outcomes.AUTH)

    def test_billing_failure_is_auth_class(self) -> None:
        report = self.run_failing(
            {"stderr": "billing_error: credit balance is too low", "exit": 1}
        )
        self.assertEqual(report.outcome, outcomes.AUTH)

    def test_unproven_failure_classifies_infra(self) -> None:
        report = self.run_failing(
            {"stderr": "connection reset by peer", "exit": 1}
        )
        self.assertEqual(report.outcome, outcomes.INFRA)

    def test_transcript_lookalike_text_is_not_terminal(self) -> None:
        # 'api key' inside researched page content must never classify auth;
        # only CLI-owned error text is consulted, and this arrives as an
        # agent message, not an error event.
        report = self.run_failing(
            {
                "emit": [
                    {
                        "type": "item.completed",
                        "item": {
                            "id": "i1",
                            "type": "agent_message",
                            "text": "the page mentions an invalid api key signup form",
                        },
                    }
                ],
                "stderr": "exited badly",
                "exit": 1,
            }
        )
        self.assertEqual(report.outcome, outcomes.INFRA)


class SpawnFailureTest(FakeCliCase):
    def test_missing_binary_is_spawn_failure(self) -> None:
        # Matrix C7: the claude adapter resolves via override/PATH only, so
        # clearing both proves the missing-CLI path with no fallback risk.
        with mock.patch.dict(
            _os.environ, {"RUNNER_CLAUDE_CLI": "", "PATH": str(self.tmp)}
        ):
            report = run_attempt(
                RunSpec(key="k", harness="claude", agent_ref="a"),
                "task",
                self.workdir,
                poll_seconds=0.05,
            )
        self.assertEqual(report.outcome, outcomes.SPAWN_FAILURE)

    def test_vanished_binary_at_exec_is_spawn_failure(self) -> None:
        # The override passes the existence check, then exec fails: the
        # OSError path, not the missing_command path.
        ghost = self.tmp / "ghost-cli"
        ghost.write_text("#!/bin/sh\n")  # not executable
        with mock.patch.dict(_os.environ, {"RUNNER_CODEX_CLI": str(ghost)}):
            report = run_attempt(
                codex_spec(), "task", self.workdir, poll_seconds=0.05
            )
        self.assertEqual(report.outcome, outcomes.SPAWN_FAILURE)


class TimeoutTest(FakeCliCase):
    def test_overrunning_attempt_times_out_and_reaps(self) -> None:
        self.scenario([{"sleep": 30, "exit": 0}])
        report = run_attempt(
            codex_spec(), "task", self.workdir,
            validate=self.json_validator(),
            timeout_minutes=0.01,
            poll_seconds=0.05,
        )
        self.assertEqual(report.outcome, outcomes.TIMEOUT)
        self.assertIn("timed out", report.error)


class RepairTest(FakeCliCase):
    def test_invalid_schema_repairs_into_the_open_session(self) -> None:
        # Matrix C3: the project's repair message goes into the still-open
        # session (codex followup = `exec resume <thread>`), and the
        # repaired output is accepted without a fresh attempt.
        self.scenario(
            [
                {
                    "emit": [{"type": "thread.started", "thread_id": "th_1"}],
                    "write": [{"path": str(self.out_path()), "text": '{"ok": false}'}],
                    "exit": 0,
                },
                {
                    "write": [{"path": str(self.out_path()), "text": '{"ok": true}'}],
                    "exit": 0,
                },
            ]
        )
        report = run_attempt(
            codex_spec(repair_rounds=2), "task", self.workdir,
            validate=self.json_validator(), poll_seconds=0.05,
        )
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertEqual(report.repair_rounds_used, 1)
        followup = self.recorded_call(1)
        self.assertIn("resume", followup["argv"])
        self.assertIn("th_1", followup["argv"])
        self.assertEqual(followup["stdin"], "REPAIR: set ok=true in out.json")

    def test_no_repair_budget_ends_invalid_schema(self) -> None:
        self.scenario(
            [
                {
                    "emit": [{"type": "thread.started", "thread_id": "th_1"}],
                    "write": [{"path": str(self.out_path()), "text": '{"ok": false}'}],
                    "exit": 0,
                }
            ]
        )
        report = run_attempt(
            codex_spec(repair_rounds=0), "task", self.workdir,
            validate=self.json_validator(), poll_seconds=0.05,
        )
        self.assertEqual(report.outcome, outcomes.INVALID_SCHEMA)
        self.assertFalse((self.calls / "call-01.json").exists())

    def test_hopeless_repair_gives_up_after_budget(self) -> None:
        self.scenario(
            [
                {
                    "emit": [{"type": "thread.started", "thread_id": "th_1"}],
                    "write": [{"path": str(self.out_path()), "text": '{"ok": false}'}],
                    "exit": 0,
                },
                {"exit": 0},
            ]
        )
        report = run_attempt(
            codex_spec(repair_rounds=2), "task", self.workdir,
            validate=self.json_validator(), poll_seconds=0.05,
        )
        self.assertEqual(report.outcome, outcomes.INVALID_SCHEMA)
        # Identical verdict after the first repair ends the loop early.
        self.assertEqual(report.repair_rounds_used, 1)


class ResumeTest(FakeCliCase):
    def test_session_ref_resumes_with_preamble(self) -> None:
        self.scenario(
            [
                {
                    "write": [{"path": str(self.out_path()), "text": '{"ok": true}'}],
                    "exit": 0,
                }
            ]
        )
        report = run_attempt(
            codex_spec(), "task body", self.workdir,
            validate=self.json_validator(),
            session_ref="th_9",
            poll_seconds=0.05,
        )
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertTrue(report.resumed)
        call = self.recorded_call(0)
        self.assertIn("resume", call["argv"])
        self.assertIn("th_9", call["argv"])
        self.assertTrue(call["stdin"].startswith("RESUME:"))
        self.assertIn("task body", call["stdin"])


class CancelTest(FakeCliCase):
    def test_should_stop_terminates_and_raises_cancelled(self) -> None:
        self.scenario([{"sleep": 30, "exit": 0}])
        with self.assertRaises(AttemptCancelled):
            run_attempt(
                codex_spec(), "task", self.workdir,
                poll_seconds=0.05,
                should_stop=lambda: True,
            )


class IsolationTest(FakeCliCase):
    def test_agent_env_is_filtered_and_stamped(self) -> None:
        self.scenario(
            [
                {
                    "write": [{"path": str(self.out_path()), "text": '{"ok": true}'}],
                    "exit": 0,
                }
            ]
        )
        with mock.patch.dict(
            _os.environ, {"OPERATOR_SECRET_DSN": "postgres://secret"}
        ):
            run_attempt(
                codex_spec(), "task", self.workdir,
                validate=self.json_validator(),
                run_id="run-7",
                poll_seconds=0.05,
            )
        env = self.recorded_call(0)["env"]
        # Operator secrets never reach the agent process.
        self.assertNotIn("OPERATOR_SECRET_DSN", env)
        # Declared required_env and adapter passthrough do.
        self.assertEqual(env["FAKE_CLI_SCENARIO"], str(self.scenario_path))
        self.assertEqual(env["RUNNER_CODEX_CLI"], str(FAKE_CLI))
        # The RUNNER_* attribution set is stamped.
        self.assertEqual(env["RUNNER_JOB_KEY"], "fixture__research__codex")
        self.assertEqual(env["RUNNER_RUN_ID"], "run-7")
        self.assertEqual(env["RUNNER_ATTEMPT"], "1")


class ClaudeAgentDefTest(FakeCliCase):
    def test_agent_definition_materializes_and_spawns(self) -> None:
        # Spawn takes an agent definition and a task message: the claude
        # dialect writes its discovery file from the definition, nothing is
        # authored on disk by the caller.
        self.scenario(
            [
                {
                    "emit": [
                        {"type": "system", "subtype": "init", "session_id": "sess-1", "model": "m"},
                    ],
                    "write": [{"path": str(self.out_path()), "text": '{"ok": true}'}],
                    "exit": 0,
                }
            ]
        )
        agent = AgentDef(
            name="fixture-claude-agent",
            description="fixture",
            config={"model": "fixture-model"},
            body="Fixture body.\n",
        )
        report = run_attempt(
            RunSpec(
                key="fixture__claude",
                harness="claude",
                required_env=("FAKE_CLI_SCENARIO", "FAKE_CLI_CALLS"),
            ),
            "task",
            self.workdir,
            agent=agent,
            validate=self.json_validator(),
            poll_seconds=0.05,
        )
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertEqual(report.session_ref, "sess-1")
        discovery = self.tmp / ".claude" / "agents" / "fixture-claude-agent.md"
        self.assertTrue(discovery.is_file())
        self.assertIn("Fixture body.", discovery.read_text())
        argv = self.recorded_call(0)["argv"]
        self.assertIn("--agent", argv)
        self.assertIn("fixture-claude-agent", argv)


if __name__ == "__main__":
    unittest.main()

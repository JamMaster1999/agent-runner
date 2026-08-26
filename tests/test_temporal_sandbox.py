#!/usr/bin/env python3
"""The sandboxed activity wrapper, end to end on the local executor: a
keeper subprocess, an attempt exec'd inside it through the test project's
entrypoint, the fake CLI at the bottom, and ``ActivityEnvironment`` on top.

Needs the ``temporalio`` extra; the module skips cleanly without it.
"""

from __future__ import annotations

import asyncio
import json
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
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

    from agent_runner import outcomes, state
    from agent_runner.executor import SANDBOX_GONE, LocalExecutor, SandboxSpec
    from agent_runner.workspace import READY_MARKER, attempt_workdir, marker, pid_file
    from agent_runner.runtime import RunSpec
    from agent_runner.temporal.sandbox import run_sandboxed_attempt

FAKE_CLI = REPO / "tests" / "fake_cli" / "fake-cli"
ENTRY = (sys.executable, str(REPO / "tests" / "fake_cli" / "serve-attempt"))
KEEPER = (sys.executable, "-m", "agent_runner", "keeper", "--every", "1")
KEY = "fixture__research__codex"


def codex_spec(**overrides) -> RunSpec:
    values = dict(
        key=KEY,
        harness="codex",
        agent_ref="fixture-agent",
        agent_config={"model": "fixture-model"},
        task_type="research",
        required_env=("FAKE_CLI_SCENARIO", "FAKE_CLI_CALLS"),
    )
    values.update(overrides)
    return RunSpec(**values)


@unittest.skipUnless(HAVE_TEMPORALIO, "temporalio not installed (core CI is Temporal-less)")
class SandboxedAttemptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.scenario_path = self.tmp / "scenario.json"
        env = mock.patch.dict(
            _os.environ,
            {
                "AGENT_RUNNER_PROJECT_ROOT": str(self.tmp),
                "PYTHONPATH": str(REPO / "src"),
                "RUNNER_CODEX_CLI": str(FAKE_CLI),
                "FAKE_CLI_SCENARIO": str(self.scenario_path),
                "FAKE_CLI_CALLS": str(self.tmp / "calls"),
            },
        )
        env.start()
        self.addCleanup(env.stop)
        _os.environ.pop(state.STATE_S3_ENV, None)
        self.executor = LocalExecutor(self.tmp / "sandboxes")
        self.sandbox = self.executor.create(SandboxSpec(
            name="run-1-research", command=KEEPER, ttl_seconds=120,
            env={"AGENT_RUNNER_WORKSPACE_GROUP": "mit/run-1/research"},
        ))
        self.addCleanup(self.sandbox.terminate)
        deadline = time.monotonic() + 15
        while not marker(self.sandbox.workspace, READY_MARKER).exists():
            self.assertLess(time.monotonic(), deadline, "keeper never said ready")
            time.sleep(0.05)

    def scenario(self, calls: list[dict]) -> None:
        self.scenario_path.write_text(json.dumps(calls))

    def out_path(self) -> Path:
        return attempt_workdir(self.sandbox.workspace, KEY, 1) / "out.json"

    def valid_scenario(self) -> None:
        self.scenario([{
            "emit": [
                {"type": "thread.started", "thread_id": "th_1"},
                {"type": "item.completed", "item": {"type": "command_execution", "command": "ls"}},
                {"type": "turn.completed", "usage": {"input_tokens": 10, "cached_input_tokens": 4, "output_tokens": 3}},
            ],
            "write": [{"path": str(self.out_path()), "text": '{"ok": true}'}],
        }])

    def attempt(self, **kwargs):
        env = ActivityEnvironment()
        beats: list[dict] = []
        env.on_heartbeat = beats.append
        report = asyncio.run(env.run(
            run_sandboxed_attempt, self.sandbox, ENTRY, codex_spec(), "task",
            validator={"child": "research"}, **kwargs,
        ))
        return report, beats

    def test_the_exec_env_reaches_the_attempt(self) -> None:
        """A credential riding the exec lands in the CLI home of that attempt."""
        self.valid_scenario()
        report, _ = self.attempt(env={"CODEX_AUTH_JSON": '{"token": "attempt-2"}'})
        self.assertEqual(report.outcome, outcomes.VALID)
        auth = next(Path(self.sandbox.workspace).rglob("codex-home/auth.json"))
        self.assertEqual(auth.read_text(), '{"token": "attempt-2"}')

    def test_a_valid_report_with_the_sandbox_in_every_heartbeat(self) -> None:
        self.valid_scenario()
        report, beats = self.attempt()
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertEqual(report.session_ref, "th_1")
        self.assertEqual(report.data, {"ok": True, "validator": {"child": "research"}})
        self.assertEqual(report.usage.tok_input, 10)
        self.assertEqual(report.attempts[-1]["outcome"], outcomes.VALID)
        self.assertTrue(beats)
        self.assertTrue(all(beat["sandbox"] == self.sandbox.id for beat in beats))
        self.assertEqual(beats[-1]["session_ref"], "th_1")
        self.assertEqual(beats[-1]["usage"]["tok_input"], 10)
        self.assertTrue(any(beat["progress"].get("message") for beat in beats), "progress never rode a heartbeat")

    def test_a_failed_attempt_raises_the_outcome_word_with_the_record(self) -> None:
        self.scenario([{"stderr": "something broke", "exit": 1}])
        with self.assertRaises(ApplicationError) as caught:
            self.attempt()
        self.assertEqual(caught.exception.type, outcomes.INFRA)
        self.assertFalse(caught.exception.non_retryable)
        self.assertEqual(caught.exception.details[0]["attempt"]["outcome"], outcomes.INFRA)

    def wait_for_pidfile(self) -> Path:
        pidfile = pid_file(self.sandbox.workspace, KEY)
        deadline = time.monotonic() + 15
        while not pidfile.exists():
            self.assertLess(time.monotonic(), deadline, "the attempt never started")
            time.sleep(0.05)
        return pidfile

    def test_a_sandbox_that_ends_under_the_attempt_is_sandbox_gone(self) -> None:
        self.scenario([{"sleep": 30}])

        def end_it() -> None:
            self.wait_for_pidfile()
            self.sandbox.terminate()

        threading.Thread(target=end_it, daemon=True).start()
        with self.assertRaises(ApplicationError) as caught:
            self.attempt()
        self.assertEqual(caught.exception.type, SANDBOX_GONE)
        self.assertTrue(caught.exception.non_retryable)
        self.assertEqual(caught.exception.details[0]["attempt"]["outcome"], outcomes.INFRA)

    def test_exec_on_a_dead_sandbox_is_sandbox_gone(self) -> None:
        self.valid_scenario()
        self.sandbox.terminate()
        with self.assertRaises(ApplicationError) as caught:
            self.attempt()
        self.assertEqual(caught.exception.type, SANDBOX_GONE)
        self.assertTrue(caught.exception.non_retryable)

    def test_a_stale_attempt_process_is_killed_before_the_next(self) -> None:
        stale = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        self.addCleanup(stale.kill)
        pidfile = pid_file(self.sandbox.workspace, KEY)
        pidfile.parent.mkdir(parents=True, exist_ok=True)
        pidfile.write_text(str(stale.pid))
        self.valid_scenario()
        report, _ = self.attempt()
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertEqual(stale.wait(5), -signal.SIGTERM)
        self.assertNotEqual(int(pidfile.read_text()), stale.pid)

    def test_cancel_ends_the_attempt_process_before_propagating(self) -> None:
        self.scenario([{"sleep": 30}])
        env = ActivityEnvironment()
        env.on_heartbeat = lambda *_: None
        pidfile = pid_file(self.sandbox.workspace, KEY)

        async def scenario() -> int:
            task = asyncio.create_task(env.run(
                run_sandboxed_attempt, self.sandbox, ENTRY, codex_spec(), "task", validator={},
            ))
            while not pidfile.exists():
                await asyncio.sleep(0.05)
            pid = int(pidfile.read_text())
            env.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            return pid

        pid = asyncio.run(scenario())
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                _os.kill(pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.05)
        self.fail("the attempt process outlived the cancel")


if __name__ == "__main__":
    unittest.main()

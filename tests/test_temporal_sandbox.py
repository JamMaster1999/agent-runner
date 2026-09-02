#!/usr/bin/env python3
"""The sandboxed activity wrapper, end to end on the local executor: a
keeper subprocess, an attempt exec'd inside it through the test project's
entrypoint, the fake CLI at the bottom, and ``ActivityEnvironment`` on top.

Needs the ``temporalio`` extra; the module skips cleanly without it.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import signal
import sys
import tempfile
import time
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

    from agent_runner import outcomes, state
    from agent_runner.executor import SANDBOX_GONE, LocalExecutor, Sandbox, SandboxSpec
    from agent_runner.workspace import READY_MARKER, attempt_workdir, marker, pid_file
    from agent_runner.runtime import RunSpec
    from agent_runner.temporal.sandbox import run_sandboxed_attempt
    from agent_runner.pool import Pool
    from agent_runner.temporal.activity import TemporalRunConfig
    from agent_runner.temporal.retry import RESET_DELAY_FLOOR

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


async def wait_for(test: unittest.TestCase, predicate, what: str, seconds: float = 15.0) -> None:
    deadline = time.monotonic() + seconds
    while not predicate():
        test.assertLess(time.monotonic(), deadline, what)
        await asyncio.sleep(0.05)


def alive(pid: int) -> bool:
    try:
        _os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@unittest.skipUnless(HAVE_TEMPORALIO, "temporalio not installed (core CI is Temporal-less)")
class SandboxedAttemptTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
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
        self.sandbox = await self.executor.create(SandboxSpec(
            name="run-1-research", command=KEEPER, ttl_seconds=120,
            env={"AGENT_RUNNER_WORKSPACE_GROUP": "mit/run-1/research"},
        ))
        self.addAsyncCleanup(self.sandbox.terminate)
        await wait_for(self, marker(self.sandbox.workspace, READY_MARKER).exists, "keeper never said ready")

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

    async def attempt(self, sandbox: Sandbox | None = None, env_: ActivityEnvironment | None = None, **kwargs):
        env_ = env_ or ActivityEnvironment()
        beats: list[dict] = []
        env_.on_heartbeat = beats.append
        report = await env_.run(
            run_sandboxed_attempt, sandbox or self.sandbox, ENTRY, codex_spec(), "task",
            validator={"child": "research"}, **kwargs,
        )
        return report, beats

    async def test_the_exec_env_reaches_the_attempt(self) -> None:
        """A credential riding the exec lands in the CLI home of that attempt."""
        self.valid_scenario()
        report, _ = await self.attempt(env={"CODEX_AUTH_JSON": '{"token": "attempt-2"}'})
        self.assertEqual(report.outcome, outcomes.VALID)
        auth = next(Path(self.sandbox.workspace).rglob("codex-home/auth.json"))
        self.assertEqual(auth.read_text(), '{"token": "attempt-2"}')

    async def test_a_pool_credential_reaches_the_attempt(self) -> None:
        self.valid_scenario()
        pool = Pool("CODEX_AUTH_JSON", ('{"token": "slot-0"}', '{"token": "slot-1"}'))
        report, _ = await self.attempt(pool=pool)
        self.assertEqual(report.outcome, outcomes.VALID)
        auth = next(Path(self.sandbox.workspace).rglob("codex-home/auth.json"))
        self.assertEqual(auth.read_text(), '{"token": "slot-0"}')

    async def test_a_rate_limited_attempt_holds_its_pool_slot_and_retries_on_the_next(self) -> None:
        pool = Pool("CODEX_AUTH_JSON", ('{"token": "slot-0"}', '{"token": "slot-1"}'))
        self.scenario([{"stderr": "usage limit reached — please run /login or wait for the window", "exit": 1}])
        with self.assertRaises(ApplicationError) as caught:
            await self.attempt(pool=pool, config=TemporalRunConfig(rate_limit_backoff=timedelta(minutes=20)))
        self.assertEqual(caught.exception.type, outcomes.RATE_LIMITED)
        self.assertAlmostEqual(
            pool.held[0], datetime.now(timezone.utc) + timedelta(minutes=20), delta=timedelta(seconds=10)
        )
        self.assertEqual(caught.exception.next_retry_delay, RESET_DELAY_FLOOR, "slot 1 is free: retry promptly")

    async def test_a_named_reset_holds_its_slot_and_the_retry_rotates_to_a_free_one(self) -> None:
        pool = Pool("CODEX_AUTH_JSON", ('{"token": "slot-0"}', '{"token": "slot-1"}'))
        reset = (datetime.now() + timedelta(hours=2)).replace(second=0, microsecond=0)
        self.scenario([{
            "stderr": f"You've hit your usage limit. try again at {reset.strftime('%b %d, %Y %I:%M %p')}.",
            "exit": 1,
        }])
        with self.assertRaises(ApplicationError) as caught:
            await self.attempt(pool=pool)
        self.assertEqual(caught.exception.type, outcomes.RATE_LIMITED)
        self.assertAlmostEqual(pool.held[0], reset.astimezone(timezone.utc), delta=timedelta(seconds=1))
        self.assertEqual(
            caught.exception.next_retry_delay, RESET_DELAY_FLOOR,
            "slot 0 is held until its reset; slot 1 is free, so the retry runs there now",
        )

    async def test_a_valid_report_with_the_sandbox_in_every_heartbeat(self) -> None:
        self.valid_scenario()
        report, beats = await self.attempt()
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

    async def test_the_heartbeat_pumps_while_the_exec_is_still_starting(self) -> None:
        """A platform slow to start the attempt (200 execs at once) must not
        read as a dead worker: the pump beats through the stale kill and the
        exec, before the attempt's first tick."""
        self.valid_scenario()
        beats_before_start: list[float] = []
        started: list[float] = []

        class SlowStart(Sandbox):
            def __init__(self, inner: Sandbox) -> None:
                self.__dict__.update(inner.__dict__)
                self._inner = inner

            async def exec(self, *command, **kwargs):
                await asyncio.sleep(0.7)
                proc = await self._inner.exec(*command, **kwargs)
                started.append(time.monotonic())
                return proc

            async def poll(self):
                return await self._inner.poll()

            async def terminate(self):
                await self._inner.terminate()

        env = ActivityEnvironment()
        env.on_heartbeat = lambda *_: beats_before_start.append(time.monotonic()) if not started else None
        await env.run(
            run_sandboxed_attempt, SlowStart(self.sandbox), ENTRY, codex_spec(), "task",
            validator={"child": "research"}, config=TemporalRunConfig(heartbeat_seconds=0.2),
        )
        self.assertGreaterEqual(len(beats_before_start), 2, "no heartbeat while the exec was starting")

    async def test_a_silent_attempt_process_is_killed_and_the_attempt_ends_infra(self) -> None:
        """The attempt stream ticks every heartbeat_seconds; one silent for
        the activity's heartbeat timeout is wedged — ended by pid, ended
        ``infra`` here (a retry resumes the session in the same sandbox),
        never a slot held until the exec's own deadline."""
        self.scenario([{"sleep": 30}])
        env = ActivityEnvironment()
        env.info = dataclasses.replace(env.info, heartbeat_timeout=timedelta(seconds=1))
        pidfile = pid_file(self.sandbox.workspace, KEY)

        async def freeze_the_attempt() -> int:
            await wait_for(self, pidfile.exists, "the attempt never started")
            pid = int(pidfile.read_text())
            _os.kill(pid, signal.SIGSTOP)  # ticks stop; the process is still there
            return pid

        freezer = asyncio.create_task(freeze_the_attempt())
        with self.assertRaises(ApplicationError) as caught:
            await self.attempt(env_=env, config=TemporalRunConfig(heartbeat_seconds=0.2))
        pid = await freezer
        self.assertEqual(caught.exception.type, outcomes.INFRA)
        self.assertIn("went silent for 1s", str(caught.exception))
        self.assertEqual(caught.exception.details[0]["attempt"]["outcome"], outcomes.INFRA)
        await wait_for(self, lambda: not alive(pid), "the silent attempt process outlived the attempt", seconds=10)

    async def test_a_failed_attempt_raises_the_outcome_word_with_the_record(self) -> None:
        self.scenario([{"stderr": "something broke", "exit": 1}])
        with self.assertRaises(ApplicationError) as caught:
            await self.attempt()
        self.assertEqual(caught.exception.type, outcomes.INFRA)
        self.assertFalse(caught.exception.non_retryable)
        self.assertEqual(caught.exception.details[0]["attempt"]["outcome"], outcomes.INFRA)

    async def test_a_sandbox_that_ends_under_the_attempt_is_sandbox_gone(self) -> None:
        self.scenario([{"sleep": 30}])
        pidfile = pid_file(self.sandbox.workspace, KEY)

        async def end_it() -> None:
            await wait_for(self, pidfile.exists, "the attempt never started")
            await self.sandbox.terminate()

        ender = asyncio.create_task(end_it())
        with self.assertRaises(ApplicationError) as caught:
            await self.attempt()
        await ender
        self.assertEqual(caught.exception.type, SANDBOX_GONE)
        self.assertTrue(caught.exception.non_retryable)
        self.assertEqual(caught.exception.details[0]["attempt"]["outcome"], outcomes.INFRA)

    async def test_exec_on_a_dead_sandbox_is_sandbox_gone(self) -> None:
        self.valid_scenario()
        await self.sandbox.terminate()
        with self.assertRaises(ApplicationError) as caught:
            await self.attempt()
        self.assertEqual(caught.exception.type, SANDBOX_GONE)
        self.assertTrue(caught.exception.non_retryable)

    async def test_a_stale_attempt_process_is_killed_before_the_next(self) -> None:
        stale = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(60)")
        self.addCleanup(lambda: stale.returncode is None and stale.kill())
        pidfile = pid_file(self.sandbox.workspace, KEY)
        pidfile.parent.mkdir(parents=True, exist_ok=True)
        pidfile.write_text(str(stale.pid))
        self.valid_scenario()
        report, _ = await self.attempt()
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertEqual(await asyncio.wait_for(stale.wait(), 5), -signal.SIGTERM)
        self.assertNotEqual(int(pidfile.read_text()), stale.pid)

    async def test_cancel_ends_the_attempt_process_before_propagating(self) -> None:
        self.scenario([{"sleep": 30}])
        env = ActivityEnvironment()
        env.on_heartbeat = lambda *_: None
        pidfile = pid_file(self.sandbox.workspace, KEY)
        task = asyncio.create_task(env.run(
            run_sandboxed_attempt, self.sandbox, ENTRY, codex_spec(), "task", validator={},
        ))
        await wait_for(self, pidfile.exists, "the attempt never started")
        pid = int(pidfile.read_text())
        env.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await wait_for(self, lambda: not alive(pid), "the attempt process outlived the cancel", seconds=10)


if __name__ == "__main__":
    unittest.main()

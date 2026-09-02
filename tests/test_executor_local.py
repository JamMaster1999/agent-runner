#!/usr/bin/env python3
"""The executor adaptor on its local backend: the same lifecycle a Modal
sandbox has — create runs the keeper, exec runs a command in the
workspace's environment, terminate ends everything, and ``ExecutorGone``
is what a dead sandbox answers with.
"""

from __future__ import annotations

import asyncio
import inspect
import sys
import tempfile
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

from agent_runner import state  # noqa: E402
from agent_runner.executor import ExecutorGone, LocalExecutor, SandboxSpec  # noqa: E402
from agent_runner.workspace import READY_MARKER, RELEASE_MARKER, marker  # noqa: E402

GROUP = "mit/run-7/scrape"
KEEPER = (sys.executable, "-m", "agent_runner", "keeper", "--every", "0.2")


async def wait_for(test: unittest.TestCase, predicate, seconds: float = 15.0) -> None:
    """Until ``predicate()`` (plain or awaitable) is true."""
    deadline = time.monotonic() + seconds
    while True:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        test.assertLess(time.monotonic(), deadline, "timed out waiting")
        await asyncio.sleep(0.05)


async def ended(sandbox) -> bool:
    return await sandbox.poll() is not None


class LocalExecutorTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        env = mock.patch.dict(_os.environ, {"PYTHONPATH": str(REPO / "src")})
        env.start()
        self.addCleanup(env.stop)
        _os.environ.pop(state.STATE_S3_ENV, None)
        self.executor = LocalExecutor(self.tmp / "sandboxes")

    def spec(self, name: str = "run-7-scrape", ttl: int = 60) -> SandboxSpec:
        return SandboxSpec(
            name=name,
            command=KEEPER,
            ttl_seconds=ttl,
            env={"AGENT_RUNNER_WORKSPACE_GROUP": GROUP},
            secrets={"FAKE_SECRET": "s3cret"},
            tags={"gtm": "1", "child": "scrape"},
        )

    async def create(self, **kwargs):
        sandbox = await self.executor.create(self.spec(**kwargs))
        self.addAsyncCleanup(sandbox.terminate)
        await wait_for(self, marker(sandbox.workspace, READY_MARKER).exists)
        return sandbox

    def keeper_log(self, sandbox) -> str:
        return (self.tmp / "sandboxes" / sandbox.name / "keeper.log").read_text()

    async def test_create_runs_the_keeper_in_a_workspace_and_says_ready(self) -> None:
        sandbox = await self.create()
        self.assertEqual(sandbox.workspace, str(self.tmp / "sandboxes" / "run-7-scrape" / "work"))
        self.assertEqual(marker(sandbox.workspace, READY_MARKER).read_text(), "fresh")
        self.assertIn("ready fresh", self.keeper_log(sandbox))
        self.assertIsNone(await sandbox.poll())
        self.assertEqual(sandbox.tags, {"gtm": "1", "child": "scrape"})

    async def test_exec_sees_the_workspace_env_and_secrets_and_reads_stdin(self) -> None:
        sandbox = await self.create()
        proc = await sandbox.exec(
            sys.executable, "-c",
            "import os, sys; print(os.environ['AGENT_RUNNER_WORKSPACE']); "
            "print(os.environ['FAKE_SECRET']); print(os.environ['EXTRA']); "
            "print(sys.stdin.read()); sys.stderr.write('warned')",
            stdin=b"hello", env={"EXTRA": "yes"},
        )
        self.assertEqual([line async for line in proc.lines()], [sandbox.workspace, "s3cret", "yes", "hello"])
        self.assertEqual(await proc.wait(), 0)
        self.assertEqual(await proc.stderr(), "warned")

    async def test_a_long_line_arrives_whole(self) -> None:
        """A report line carries the validated payload whole — megabytes,
        not the 64 KiB an asyncio pipe reads by default."""
        sandbox = await self.create()
        proc = await sandbox.exec(sys.executable, "-c", "print('x' * (2 * 1024 * 1024)); print('end')")
        lines = [line async for line in proc.lines()]
        self.assertEqual([len(lines[0]), lines[1]], [2 * 1024 * 1024, "end"])
        self.assertEqual(await proc.wait(), 0)

    async def test_a_large_stdin_never_deadlocks_a_chatty_child(self) -> None:
        sandbox = await self.create()
        proc = await sandbox.exec(
            sys.executable, "-c",
            "import sys; sys.stdout.write('y' * (1024 * 1024) + '\\n'); sys.stdout.flush(); "
            "print(len(sys.stdin.read()))",
            stdin=b"z" * (1024 * 1024),
        )
        lines = [line async for line in proc.lines()]
        self.assertEqual(lines[1], str(1024 * 1024))
        self.assertEqual(await proc.wait(), 0)

    async def test_find_attach_and_list_recover_a_running_sandbox(self) -> None:
        sandbox = await self.create()
        self.assertIs(await self.executor.find("run-7-scrape"), sandbox)
        self.assertIsNone(await self.executor.find("nobody"))
        self.assertIs(await self.executor.attach(sandbox.id), sandbox)
        self.assertEqual(await self.executor.list({"gtm": "1"}), [sandbox])
        self.assertEqual(await self.executor.list({"gtm": "1", "child": "other"}), [])
        with self.assertRaises(ExecutorGone):
            await self.executor.attach("local-nobody-1")

    async def test_terminate_ends_the_keeper_its_execs_and_every_handle(self) -> None:
        sandbox = await self.create()
        sleeper = await sandbox.exec(sys.executable, "-c", "import time; time.sleep(30)")
        await sandbox.terminate()
        self.assertIsNotNone(await sandbox.poll())
        self.assertNotEqual(await sleeper.wait(), 0)
        self.assertIsNone(await self.executor.find("run-7-scrape"))
        self.assertEqual(await self.executor.list({"gtm": "1"}), [])
        with self.assertRaises(ExecutorGone):
            await self.executor.attach(sandbox.id)
        with self.assertRaises(ExecutorGone):
            await sandbox.exec("true")
        await sandbox.terminate()  # idempotent

    async def test_the_exec_timeout_ends_the_process(self) -> None:
        sandbox = await self.create()
        sleeper = await sandbox.exec(sys.executable, "-c", "import time; time.sleep(30)", timeout=1)
        self.assertEqual([line async for line in sleeper.lines()], [])
        self.assertNotEqual(await sleeper.wait(), 0)

    async def test_the_release_marker_ends_the_keeper_cleanly(self) -> None:
        sandbox = await self.create()
        marker(sandbox.workspace, RELEASE_MARKER).touch()
        await wait_for(self, lambda: ended(sandbox))
        self.assertEqual(await sandbox.poll(), 0)
        self.assertIn("released", self.keeper_log(sandbox))

    async def test_a_name_reopened_after_release_starts_clean(self) -> None:
        first = await self.create()
        marker(first.workspace, RELEASE_MARKER).touch()
        await wait_for(self, lambda: ended(first))
        second = await self.create()  # same name, same workspace on disk
        self.assertIsNone(await second.poll())
        self.assertFalse(marker(second.workspace, RELEASE_MARKER).exists())
        proc = await second.exec("true")
        self.assertEqual(await proc.wait(), 0)
        self.assertIsNone(await second.poll())

    async def test_the_ttl_terminates(self) -> None:
        sandbox = await self.create(name="short", ttl=1)
        await wait_for(self, lambda: ended(sandbox), seconds=10)
        with self.assertRaises(ExecutorGone):
            await sandbox.exec("true")

    async def test_terminate_cancels_the_ttl_timer(self) -> None:
        sandbox = await self.create(name="long", ttl=3600)
        await sandbox.terminate()
        self.assertTrue(sandbox.ttl.cancelled())


if __name__ == "__main__":
    unittest.main()

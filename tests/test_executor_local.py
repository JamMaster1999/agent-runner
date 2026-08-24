#!/usr/bin/env python3
"""The executor adaptor on its local backend: the same lifecycle a Modal
sandbox has — create runs the keeper, exec runs a command in the
workspace's environment, terminate ends everything, and ``ExecutorGone``
is what a dead sandbox answers with.
"""

from __future__ import annotations

import subprocess
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


def wait_for(test: unittest.TestCase, predicate, seconds: float = 15.0) -> None:
    deadline = time.monotonic() + seconds
    while not predicate():
        test.assertLess(time.monotonic(), deadline, "timed out waiting")
        time.sleep(0.05)


class LocalExecutorTest(unittest.TestCase):
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

    def create(self, **kwargs):
        sandbox = self.executor.create(self.spec(**kwargs))
        self.addCleanup(sandbox.terminate)
        wait_for(self, marker(sandbox.workspace, READY_MARKER).exists)
        return sandbox

    def keeper_log(self, sandbox) -> str:
        return (self.tmp / "sandboxes" / sandbox.name / "keeper.log").read_text()

    def test_create_runs_the_keeper_in_a_workspace_and_says_ready(self) -> None:
        sandbox = self.create()
        self.assertEqual(sandbox.workspace, str(self.tmp / "sandboxes" / "run-7-scrape" / "work"))
        self.assertEqual(marker(sandbox.workspace, READY_MARKER).read_text(), "fresh")
        self.assertIn("ready fresh", self.keeper_log(sandbox))
        self.assertIsNone(sandbox.poll())
        self.assertEqual(sandbox.tags, {"gtm": "1", "child": "scrape"})

    def test_exec_sees_the_workspace_env_and_secrets_and_reads_stdin(self) -> None:
        sandbox = self.create()
        proc = sandbox.exec(
            sys.executable, "-c",
            "import os, sys; print(os.environ['AGENT_RUNNER_WORKSPACE']); "
            "print(os.environ['FAKE_SECRET']); print(os.environ['EXTRA']); "
            "print(sys.stdin.read()); sys.stderr.write('warned')",
            stdin=b"hello", env={"EXTRA": "yes"},
        )
        self.assertEqual(list(proc.lines()), [sandbox.workspace, "s3cret", "yes", "hello"])
        self.assertEqual(proc.wait(), 0)
        self.assertEqual(proc.stderr(), "warned")

    def test_find_attach_and_list_recover_a_running_sandbox(self) -> None:
        sandbox = self.create()
        self.assertIs(self.executor.find("run-7-scrape"), sandbox)
        self.assertIsNone(self.executor.find("nobody"))
        self.assertIs(self.executor.attach(sandbox.id), sandbox)
        self.assertEqual(self.executor.list({"gtm": "1"}), [sandbox])
        self.assertEqual(self.executor.list({"gtm": "1", "child": "other"}), [])
        with self.assertRaises(ExecutorGone):
            self.executor.attach("local-nobody-1")

    def test_terminate_ends_the_keeper_its_execs_and_every_handle(self) -> None:
        sandbox = self.create()
        sleeper = sandbox.exec(sys.executable, "-c", "import time; time.sleep(30)")
        sandbox.terminate()
        self.assertIsNotNone(sandbox.poll())
        self.assertNotEqual(sleeper.wait(), 0)
        self.assertIsNone(self.executor.find("run-7-scrape"))
        self.assertEqual(self.executor.list({"gtm": "1"}), [])
        with self.assertRaises(ExecutorGone):
            self.executor.attach(sandbox.id)
        with self.assertRaises(ExecutorGone):
            sandbox.exec("true")
        sandbox.terminate()  # idempotent

    def test_the_release_marker_ends_the_keeper_cleanly(self) -> None:
        sandbox = self.create()
        marker(sandbox.workspace, RELEASE_MARKER).touch()
        wait_for(self, lambda: sandbox.poll() is not None)
        self.assertEqual(sandbox.poll(), 0)
        self.assertIn("released", self.keeper_log(sandbox))

    def test_the_ttl_terminates(self) -> None:
        sandbox = self.create(name="short", ttl=1)
        wait_for(self, lambda: sandbox.poll() is not None, seconds=10)
        with self.assertRaises(ExecutorGone):
            sandbox.exec("true")

    def test_a_spec_never_reaches_a_platform_call(self) -> None:
        # The whole vocabulary a project may use, spelled here: anything
        # else in a project is a platform leak.
        spec = self.spec()
        self.assertEqual(
            sorted(f for f in spec.__dataclass_fields__),
            ["command", "cpu", "env", "memory_limit_mb", "name", "secrets", "tags", "ttl_seconds"],
        )


if __name__ == "__main__":
    unittest.main()

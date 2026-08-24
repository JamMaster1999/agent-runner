#!/usr/bin/env python3
"""Liveness inside the sandbox: silence is not death when files are being
written, and a runaway process tree is ended by the memory fuse.
"""

from __future__ import annotations

import json
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

from agent_runner import attempt as attempt_module  # noqa: E402
from agent_runner import outcomes  # noqa: E402
from agent_runner.attempt import newest_mtime, run_attempt, tree_rss_mb  # noqa: E402
from agent_runner.runtime import Policy, RunSpec, Verdict  # noqa: E402

FAKE_CLI = REPO / "tests" / "fake_cli" / "fake-cli"
LINUX = sys.platform.startswith("linux")


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


def validate(workdir: Path) -> Verdict:
    try:
        return Verdict(valid=True, data=json.loads((Path(workdir) / "out.json").read_text()))
    except (OSError, ValueError):
        return Verdict(valid=False, message="missing", repair_message="REPAIR")


class LivenessCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.workdir = self.tmp / "work"
        self.scenario_path = self.tmp / "scenario.json"
        env = mock.patch.dict(
            _os.environ,
            {
                "AGENT_RUNNER_PROJECT_ROOT": str(self.tmp),
                "RUNNER_CODEX_CLI": str(FAKE_CLI),
                "FAKE_CLI_SCENARIO": str(self.scenario_path),
                "FAKE_CLI_CALLS": str(self.tmp / "calls"),
            },
        )
        env.start()
        self.addCleanup(env.stop)

    def scenario(self, calls: list[dict]) -> None:
        self.scenario_path.write_text(json.dumps(calls))

    def run_with(self, policy: Policy, **kwargs):
        return run_attempt(
            codex_spec(policy=policy), "task", self.workdir, validate=validate,
            poll_seconds=0.1, **kwargs,
        )


class FileWritesAreLifeTest(LivenessCase):
    def test_a_silent_cli_that_keeps_writing_is_alive(self) -> None:
        self.scenario([{
            "keep_writing": {"path": str(self.workdir / "progress.log"), "every": 0.2, "seconds": 2.5},
            "write": [{"path": str(self.workdir / "out.json"), "text": '{"ok": true}'}],
        }])
        report = self.run_with(Policy(stall_seconds=1.0))
        self.assertEqual(report.outcome, outcomes.VALID, report.error)

    def test_a_silent_cli_that_writes_nothing_stalls(self) -> None:
        self.scenario([{"sleep": 20}])
        started = time.monotonic()
        report = self.run_with(Policy(stall_seconds=1.0))
        self.assertEqual(report.outcome, outcomes.STALLED)
        self.assertIn("wrote no file", report.error)
        self.assertLess(time.monotonic() - started, 10)

    def test_writes_count_only_under_watched_folders(self) -> None:
        elsewhere = self.tmp / "checkpoints" / "scrape" / "2026FALL"
        scenario = [{
            "keep_writing": {"path": str(elsewhere / "progress.json"), "every": 0.2, "seconds": 2.5},
            "write": [{"path": str(self.workdir / "out.json"), "text": '{"ok": true}'}],
        }]
        self.scenario(scenario)
        report = self.run_with(Policy(stall_seconds=1.0), watch_dirs=(elsewhere,))
        self.assertEqual(report.outcome, outcomes.VALID, report.error)
        # Unwatched, the same writes are invisible and the attempt stalls.
        (self.tmp / "calls" / "count").unlink()
        report = self.run_with(Policy(stall_seconds=1.0))
        self.assertEqual(report.outcome, outcomes.STALLED)

    def test_newest_mtime(self) -> None:
        self.assertIsNone(newest_mtime([self.tmp / "nowhere"]))
        empty = self.tmp / "empty"
        empty.mkdir()
        self.assertIsNone(newest_mtime([empty]))
        deep = self.tmp / "a" / "b" / "c.txt"
        deep.parent.mkdir(parents=True)
        deep.write_text("x")
        self.assertEqual(newest_mtime([self.tmp / "a"]), max(deep.stat().st_mtime, deep.parent.stat().st_mtime))


class MemoryFuseTest(LivenessCase):
    @unittest.skipIf(LINUX, "/proc is present here")
    def test_tree_rss_is_unknown_without_proc(self) -> None:
        self.assertIsNone(tree_rss_mb(_os.getpid()))

    @unittest.skipUnless(LINUX, "/proc is Linux")
    def test_tree_rss_reads_proc(self) -> None:
        self.assertGreater(tree_rss_mb(_os.getpid()), 1.0)
        self.assertIsNone(tree_rss_mb(2**22 - 1))

    def test_the_fuse_ends_the_attempt_infra(self) -> None:
        self.scenario([{"sleep": 20}])
        started = time.monotonic()
        with (
            mock.patch.object(attempt_module, "RSS_CHECK_SECONDS", 0.1),
            mock.patch.object(attempt_module, "tree_rss_mb", return_value=5000.0),
        ):
            report = self.run_with(Policy(stall_seconds=30, rss_limit_mb=4096))
        self.assertEqual(report.outcome, outcomes.INFRA)
        self.assertIn("memory fuse", report.error)
        self.assertIn("5000 MB", report.error)
        self.assertLess(time.monotonic() - started, 10)

    def test_no_limit_means_no_check(self) -> None:
        self.scenario([{"write": [{"path": str(self.workdir / "out.json"), "text": '{"ok": true}'}]}])
        with mock.patch.object(attempt_module, "tree_rss_mb", side_effect=AssertionError("checked")):
            report = self.run_with(Policy())
        self.assertEqual(report.outcome, outcomes.VALID)


if __name__ == "__main__":
    unittest.main()

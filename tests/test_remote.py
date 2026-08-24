#!/usr/bin/env python3
"""The attempt-in-sandbox protocol: ``serve`` reads one request from
stdin, runs the attempt through the real adapters and the fake CLI, and
writes the event stream a supervisor beats on — the report last.
"""

from __future__ import annotations

import io
import json
import signal
import sys
import tempfile
import threading
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

from agent_runner import outcomes, remote, state  # noqa: E402
from agent_runner.remote import (  # noqa: E402
    AttemptRequest,
    attempt_workdir,
    checkpoint_dir,
    pid_file,
    report_from_json,
    report_to_json,
    serve,
)
from agent_runner.runtime import AttemptReport, Policy, RunSpec, Usage, Verdict  # noqa: E402

FAKE_CLI = REPO / "tests" / "fake_cli" / "fake-cli"
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


def build_validate(payload: dict):
    def validate(workdir: Path) -> Verdict:
        try:
            data = json.loads((Path(workdir) / "out.json").read_text())
        except (OSError, ValueError):
            return Verdict(valid=False, message="missing", repair_message="REPAIR")
        return Verdict(valid=True, data={**data, "validator": payload})

    return validate


class ServeCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.workspace = self.tmp / "work"
        self.scenario_path = self.tmp / "scenario.json"
        env = mock.patch.dict(
            _os.environ,
            {
                "AGENT_RUNNER_PROJECT_ROOT": str(self.tmp),
                "AGENT_RUNNER_WORKSPACE": str(self.workspace),
                "RUNNER_CODEX_CLI": str(FAKE_CLI),
                "FAKE_CLI_SCENARIO": str(self.scenario_path),
                "FAKE_CLI_CALLS": str(self.tmp / "calls"),
            },
        )
        env.start()
        self.addCleanup(env.stop)
        for name in (state.STATE_S3_ENV, "CODEX_HOME", "CLAUDE_CONFIG_DIR"):
            _os.environ.pop(name, None)
        handler = signal.getsignal(signal.SIGTERM)
        self.addCleanup(signal.signal, signal.SIGTERM, handler)

    def scenario(self, calls: list[dict]) -> None:
        self.scenario_path.write_text(json.dumps(calls))

    def workdir(self) -> Path:
        return attempt_workdir(self.workspace, KEY, 1)

    def request(self, **overrides) -> AttemptRequest:
        values = dict(
            spec=codex_spec(),
            task="task",
            workdir=str(self.workdir()),
            validator={"child": "research"},
            run_id="run-1",
            attempt=1,
            pid_file=str(pid_file(self.workspace, KEY)),
        )
        values.update(overrides)
        return AttemptRequest(**values)

    def serve(self, stdin_text: str) -> tuple[int, list[dict]]:
        out = io.StringIO()
        rc = serve(build_validate, io.StringIO(stdin_text), out)
        return rc, [json.loads(line) for line in out.getvalue().splitlines()]


class ServeTest(ServeCase):
    def test_a_valid_attempt_streams_session_usage_then_the_report(self) -> None:
        self.scenario([{
            "emit": [
                {"type": "thread.started", "thread_id": "th_1"},
                {"type": "turn.completed", "usage": {"input_tokens": 10, "cached_input_tokens": 4, "output_tokens": 3}},
            ],
            "write": [{"path": str(self.workdir() / "out.json"), "text": '{"ok": true}'}],
        }])
        rc, events = self.serve(self.request().to_json())
        self.assertEqual(rc, 0)
        kinds = [event["e"] for event in events]
        self.assertEqual(kinds[-1], "report")
        self.assertIn("session", kinds)
        self.assertIn("usage", kinds)
        self.assertEqual(next(e for e in events if e["e"] == "session")["ref"], "th_1")
        report = report_from_json(events[-1])
        self.assertEqual(report.outcome, outcomes.VALID)
        self.assertEqual(report.session_ref, "th_1")
        self.assertEqual(report.data, {"ok": True, "validator": {"child": "research"}})
        self.assertEqual(report.usage, Usage(tok_input=10, tok_cache_read=4, tok_output=3))
        # The pid file names this process; the CLI home landed in the workspace.
        self.assertEqual(pid_file(self.workspace, KEY).read_text(), str(_os.getpid()))
        self.assertEqual(_os.environ["CODEX_HOME"], str(self.workspace / "codex-home"))

    def test_a_failed_attempt_is_still_a_report(self) -> None:
        self.scenario([{"stderr": "boom", "exit": 1}])
        rc, events = self.serve(self.request().to_json())
        self.assertEqual(rc, 0)
        self.assertEqual(events[-1]["e"], "report")
        self.assertEqual(events[-1]["outcome"], outcomes.INFRA)

    def test_a_request_that_cannot_be_read_is_an_error_line_and_exit_1(self) -> None:
        rc, events = self.serve("not a request")
        self.assertEqual(rc, 1)
        self.assertEqual(events[-1]["e"], "error")
        self.assertIn("JSONDecodeError", events[-1]["message"])

    def test_sigterm_cancels_the_attempt_and_says_so(self) -> None:
        self.scenario([{"sleep": 20}])
        threading.Timer(0.5, _os.kill, args=(_os.getpid(), signal.SIGTERM)).start()
        with mock.patch.object(remote, "TICK_SECONDS", 0.1):
            rc, events = self.serve(self.request().to_json())
        self.assertEqual(rc, 0)
        self.assertEqual(events[-1]["e"], "cancelled")
        self.assertIn("tick", [event["e"] for event in events])

    def test_checkpoint_stamps_are_verified_before_the_attempt(self) -> None:
        directory = checkpoint_dir(self.workspace, "scrape", "2026FALL")
        directory.mkdir(parents=True)
        (directory / "progress.json").write_text('{"term": "2025FALL"}')
        (directory / "kept.json").write_text('{"term": "2026FALL"}')
        self.scenario([{"write": [{"path": str(self.workdir() / "out.json"), "text": '{"ok": true}'}]}])
        with mock.patch.object(sys, "stderr", new=io.StringIO()):
            rc, _ = self.serve(
                self.request(checkpoint={"directory": str(directory), "term": "2026FALL"}).to_json()
            )
        self.assertEqual(rc, 0)
        self.assertFalse((directory / "progress.json").exists())
        self.assertTrue((directory / "kept.json").exists())


class WireShapeTest(unittest.TestCase):
    def test_the_request_round_trips(self) -> None:
        request = AttemptRequest(
            spec=codex_spec(policy=Policy(stall_seconds=30, rss_limit_mb=4096)),
            task="do it",
            workdir="/work/attempts/k/attempt-02",
            validator={"child": "scrape", "items": [{"kind": "term", "key": "2026FALL"}]},
            session_ref="th_9",
            session_usage=Usage(tok_input=5).as_dict(),
            run_id="run-1",
            attempt=2,
            timeout_minutes=90.0,
            checkpoint={"directory": "/work/checkpoints/scrape/2026FALL", "term": "2026FALL"},
            resources=("cdp_browser",),
            watch_dirs=("/work/checkpoints/scrape/2026FALL",),
            pid_file="/work/.runner/attempts/k.pid",
        )
        self.assertEqual(AttemptRequest.from_json(request.to_json()), request)

    def test_the_report_round_trips(self) -> None:
        report = AttemptReport(
            outcome=outcomes.RATE_LIMITED,
            session_ref="th_1",
            error="capped",
            detail="tail",
            usage=Usage(tok_input=1, tok_output=2),
            session_usage=Usage(tok_input=3),
            resumed=True,
            repair_rounds_used=1,
        )
        back = report_from_json(report_to_json(report))
        for name in ("outcome", "session_ref", "error", "detail", "usage", "session_usage", "resumed", "repair_rounds_used"):
            self.assertEqual(getattr(back, name), getattr(report, name), name)

    def test_the_paths_are_under_the_workspace_and_key_safe(self) -> None:
        self.assertEqual(
            attempt_workdir("/work", "a/b:c", 3), Path("/work/attempts/a_b_c/attempt-03")
        )
        self.assertEqual(pid_file("/work", "a/b"), Path("/work/.runner/attempts/a_b.pid"))


if __name__ == "__main__":
    unittest.main()

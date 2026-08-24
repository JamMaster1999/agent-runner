#!/usr/bin/env python3
"""The S3 state mirror: sessions and checkpoints follow a run to any worker.

Three contracts, in order of how much damage getting them wrong does:

1. Credential files NEVER travel, in either direction. auth.json and
   .credentials.json sit in the same CLI homes the transcripts do; the
   denylist is what keeps a shared bucket from collecting a login (or
   planting one).
2. Write-through and restore round-trip: what worker A produced, worker B
   finds. Both workers here are the real adapters and the real hook
   functions, against a fake S3 client — no network, no boto3.
3. AGENT_RUNNER_STATE_S3 unset is the behavior this package shipped before
   the mirror existed: no client built, no decision changed.
"""

from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import os as _os

REPO = Path(__file__).resolve().parents[1]
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import sessions, state, workdirs  # noqa: E402
from agent_runner.harness import get_adapter  # noqa: E402

BUCKET_URL = "s3://runner-state/fleet"
ROLLOUT = "sessions/2026/08/20/rollout-2026-08-20T09-15-00-{ref}.jsonl"


class FakeS3:
    """A dict standing in for a bucket: keys to (bytes, LastModified).

    Only the three calls the mirror makes, with boto3's keyword shapes.
    ``fail_on`` makes one call raise, which is how the best-effort posture
    gets proven without a network.
    """

    def __init__(self, fail_on: str = "") -> None:
        self.objects: dict[str, tuple[bytes, datetime]] = {}
        self.fail_on = fail_on
        self.clock = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)

    def _check(self, call: str) -> None:
        if call == self.fail_on:
            raise RuntimeError(f"S3 {call} is down")

    def upload_file(self, *, Filename: str, Bucket: str, Key: str) -> None:
        self._check("upload_file")
        self.objects[Key] = (Path(Filename).read_bytes(), self.clock)

    def download_file(self, *, Bucket: str, Key: str, Filename: str) -> None:
        self._check("download_file")
        Path(Filename).write_bytes(self.objects[Key][0])

    def list_objects_v2(self, *, Bucket: str, Prefix: str, **kwargs) -> dict:
        self._check("list_objects_v2")
        contents = [
            {"Key": key, "LastModified": modified}
            for key, (_, modified) in sorted(self.objects.items())
            if key.startswith(Prefix)
        ]
        return {"Contents": contents, "IsTruncated": False}


class MirrorCase(unittest.TestCase):
    """A configured mirror backed by the fake client, plus a scratch tree."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.s3 = FakeS3()
        env = mock.patch.dict(_os.environ, {state.STATE_S3_ENV: BUCKET_URL})
        env.start()
        self.addCleanup(env.stop)
        client = mock.patch.object(state, "s3_client", lambda: self.s3)
        client.start()
        self.addCleanup(client.stop)

    def codex_home(self, name: str) -> Path:
        """One worker's CODEX_HOME, pinned in the environment the way
        prepare_session_homes pins it."""
        home = self.tmp / name / "codex-home"
        home.mkdir(parents=True, exist_ok=True)
        patcher = mock.patch.dict(_os.environ, {"CODEX_HOME": str(home)})
        patcher.start()
        self.addCleanup(patcher.stop)
        return home

    def write_rollout(self, home: Path, ref: str, text: str = "transcript\n") -> Path:
        path = home / ROLLOUT.format(ref=ref)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path


class CredentialDenylistTest(unittest.TestCase):
    """The exclusion, proven on the two real file names and by shape."""

    def test_the_cli_login_files_are_denied(self) -> None:
        self.assertTrue(state.is_denied("auth.json"))
        self.assertTrue(state.is_denied(".credentials.json"))

    def test_anything_that_smells_like_a_credential_is_denied(self) -> None:
        for name in (
            "credentials.json",
            ".netrc",
            ".env",
            "oauth_token.txt",
            "SECRET.txt",
            "api_key.json",
            "cookies.sqlite",
            "server.pem",
            "id_rsa.key",
        ):
            self.assertTrue(state.is_denied(name), name)

    def test_session_and_checkpoint_files_travel(self) -> None:
        for name in (
            "rollout-2026-08-20T09-15-00-019f146e-7331-7630.jsonl",
            "08565ee8-3763-47ee-a8f8-e693660a8c7a.jsonl",
            "progress.json",
            "history.jsonl",
        ):
            self.assertFalse(state.is_denied(name), name)


class CredentialsNeverUploadTest(MirrorCase):
    """The guarantee end to end: a real CLI home holds the login file right
    beside the transcript, and only the transcript leaves the machine."""

    def test_auth_json_in_the_home_is_never_uploaded(self) -> None:
        home = self.codex_home("worker-a")
        (home / "auth.json").write_text('{"token": "sk-real-credential"}')
        (home / ".credentials.json").write_text('{"oauth": "sk-real-credential"}')
        self.write_rollout(home, "th_abc")
        # Push the whole home, not just the located transcript: even a
        # caller that hands over everything cannot leak the login.
        mirror = state.mirror()
        mirror.push(
            "sessions/codex/th_abc",
            home,
            sorted(p for p in home.rglob("*") if p.is_file()),
        )
        self.assertTrue(self.s3.objects)
        for key, (body, _) in self.s3.objects.items():
            self.assertNotIn("auth.json", key)
            self.assertNotIn("credentials.json", key)
            self.assertNotIn(b"sk-real-credential", body)

    def test_a_credential_in_the_bucket_is_never_downloaded(self) -> None:
        home = self.codex_home("worker-b")
        self.s3.objects["fleet/sessions/codex/th_abc/auth.json"] = (
            b'{"token": "planted"}',
            self.s3.clock,
        )
        state.mirror().pull("sessions/codex/th_abc", home)
        self.assertFalse((home / "auth.json").exists())


class SessionRoundTripTest(MirrorCase):
    """Write-through on worker A, restore on worker B, through the real
    adapter and the real hook functions."""

    def test_transcript_written_on_one_worker_resumes_on_another(self) -> None:
        adapter = get_adapter("codex")
        home_a = self.codex_home("worker-a")
        original = self.write_rollout(home_a, "th_abc", "worker A wrote this\n")
        sessions.push_session(adapter, "th_abc")

        home_b = self.codex_home("worker-b")
        self.assertFalse(adapter.session_state("th_abc")[1])
        self.assertTrue(sessions.ensure_session_local(adapter, "th_abc"))
        restored = home_b / original.relative_to(home_a)
        self.assertEqual(restored.read_text(), "worker A wrote this\n")
        # The CLI finds it where it looks for it: same path under the home.
        self.assertEqual(adapter.session_state("th_abc")[1], [restored])

    def test_a_session_nobody_has_runs_fresh(self) -> None:
        adapter = get_adapter("codex")
        self.codex_home("worker-b")
        self.assertFalse(sessions.ensure_session_local(adapter, "th_missing"))

    def test_a_local_transcript_is_never_fetched_or_clobbered(self) -> None:
        adapter = get_adapter("codex")
        home = self.codex_home("worker-a")
        self.write_rollout(home, "th_abc", "local truth\n")
        self.s3.fail_on = "list_objects_v2"  # any fetch at all would raise
        self.assertTrue(sessions.ensure_session_local(adapter, "th_abc"))

    def test_upload_failure_is_a_warning_not_a_lost_run(self) -> None:
        adapter = get_adapter("codex")
        home = self.codex_home("worker-a")
        self.write_rollout(home, "th_abc")
        self.s3.fail_on = "upload_file"
        sessions.push_session(adapter, "th_abc")  # must not raise
        self.assertFalse(self.s3.objects)

    def test_download_failure_falls_back_to_a_fresh_run(self) -> None:
        adapter = get_adapter("codex")
        self.write_rollout(self.codex_home("worker-a"), "th_abc")
        sessions.push_session(adapter, "th_abc")
        self.codex_home("worker-b")
        self.s3.fail_on = "download_file"
        self.assertFalse(sessions.ensure_session_local(adapter, "th_abc"))

    def test_the_claude_transcript_layout_round_trips_too(self) -> None:
        adapter = get_adapter("claude")
        session_id = "08565ee8-3763-47ee-a8f8-e693660a8c7a"
        home_a = self.tmp / "worker-a" / "claude-home"
        project = home_a / "projects" / "-data-workspace"
        project.mkdir(parents=True)
        (project / f"{session_id}.jsonl").write_text("claude transcript\n")
        with mock.patch.dict(_os.environ, {"CLAUDE_CONFIG_DIR": str(home_a)}):
            sessions.push_session(adapter, session_id)

        home_b = self.tmp / "worker-b" / "claude-home"
        home_b.mkdir(parents=True)
        with mock.patch.dict(_os.environ, {"CLAUDE_CONFIG_DIR": str(home_b)}):
            self.assertTrue(sessions.ensure_session_local(adapter, session_id))
            located = adapter.session_state(session_id)[1]
        self.assertEqual(
            located, [home_b / "projects" / "-data-workspace" / f"{session_id}.jsonl"]
        )


class CheckpointMirrorTest(MirrorCase):
    """Stamps written on one worker are verified on the next."""

    def checkpoint(self, worker: str, term: str = "2026FALL") -> Path:
        return workdirs.checkpoint_dir(self.tmp / worker / "vol", "scrape", term)

    def test_stamps_travel_and_pass_the_term_gate_on_another_worker(self) -> None:
        directory = self.checkpoint("worker-a")
        stamp = directory / "progress.json"
        stamp.write_text(json.dumps({"term": "2026FALL", "pages": 12}))
        workdirs.push_checkpoints(directory)

        # The folder path IS the key, so the next worker — which mounts the
        # volume in the same place and starts with nothing in it — reads
        # worker A's stamps back.
        stamp.unlink()
        workdirs.pull_checkpoints(directory)
        survivors = workdirs.verify_or_discard(directory, "2026FALL")
        self.assertEqual([p.name for p in survivors], ["progress.json"])
        self.assertEqual(json.loads(stamp.read_text())["pages"], 12)

    def test_a_local_stamp_wins_over_an_older_mirrored_one(self) -> None:
        directory = self.checkpoint("worker-a")
        stamp = directory / "progress.json"
        stamp.write_text(json.dumps({"term": "2026FALL", "pages": 1}))
        workdirs.push_checkpoints(directory)
        stamp.write_text(json.dumps({"term": "2026FALL", "pages": 99}))
        _os.utime(stamp, (self.s3.clock.timestamp() + 60,) * 2)
        workdirs.pull_checkpoints(directory)
        self.assertEqual(json.loads(stamp.read_text())["pages"], 99)

    def test_an_empty_folder_pushes_nothing(self) -> None:
        workdirs.push_checkpoints(self.checkpoint("worker-a"))
        self.assertFalse(self.s3.objects)


class KeyLayoutTest(unittest.TestCase):
    def test_s3_urls_parse_and_malformed_ones_raise(self) -> None:
        self.assertEqual(state.parse_s3_url("s3://b/p/q"), ("b", "p/q"))
        self.assertEqual(state.parse_s3_url("s3://b"), ("b", ""))
        self.assertEqual(state.parse_s3_url("s3://b/p/"), ("b", "p"))
        for bad in ("https://b/p", "b/p", "s3:///p"):
            with self.assertRaises(RuntimeError):
                state.parse_s3_url(bad)

    def test_a_session_ref_can_never_steer_the_key(self) -> None:
        adapter = get_adapter("codex")
        self.assertEqual(
            sessions.session_group(adapter, "th_1"), "sessions/codex/th_1"
        )
        self.assertEqual(state.key_segment("a/../b"), "a_.._b")
        self.assertIsNone(sessions.session_group(adapter, "../escape"))
        self.assertIsNone(sessions.session_group(adapter, "a/b"))
        self.assertIsNone(sessions.session_group(adapter, ""))

    def test_a_key_that_walks_out_of_the_root_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "home"
            root.mkdir()
            s3 = FakeS3()
            s3.objects["fleet/sessions/codex/th_1/../escaped.jsonl"] = (
                b"nope",
                s3.clock,
            )
            state.StateMirror("b", "fleet", s3).pull("sessions/codex/th_1", root)
            self.assertFalse((Path(tmp) / "escaped.jsonl").exists())

    def test_the_checkpoint_folder_path_is_its_key(self) -> None:
        group = workdirs.checkpoint_group(Path("/data/checkpoints/mit/run-7/scrape/2026FALL"))
        self.assertEqual(
            group, "checkpoints/data/checkpoints/mit/run-7/scrape/2026FALL"
        )


class AttemptWiringTest(MirrorCase):
    """The two hook points inside run_attempt, driven by the fake-CLI rig:
    the transcript is mirrored when the attempt ends, and a resume the host
    cannot serve becomes a fresh run instead of a doomed spawn."""

    def setUp(self) -> None:
        super().setUp()
        self.calls = self.tmp / "calls"
        self.scenario_path = self.tmp / "scenario.json"
        self.workdir = self.tmp / "work"
        self.home = self.codex_home("worker")
        patcher = mock.patch.dict(
            _os.environ,
            {
                "AGENT_RUNNER_PROJECT_ROOT": str(self.tmp),
                "RUNNER_CODEX_CLI": str(REPO / "tests" / "fake_cli" / "fake-cli"),
                "FAKE_CLI_SCENARIO": str(self.scenario_path),
                "FAKE_CLI_CALLS": str(self.calls),
            },
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def scenario(self, ref: str | None) -> None:
        """The CLI writes its rollout file the way the real one does, then
        exits clean."""
        call: dict = {"exit": 0}
        if ref:
            call["emit"] = [{"type": "thread.started", "thread_id": ref}]
            call["write"] = [
                {"path": str(self.home / ROLLOUT.format(ref=ref)), "text": "turns\n"}
            ]
        self.scenario_path.write_text(json.dumps([call]))

    def run_once(self, session_ref: str | None = None):
        from agent_runner.attempt import run_attempt
        from agent_runner.runtime import RunSpec

        return run_attempt(
            RunSpec(
                key="fixture__research__codex",
                harness="codex",
                agent_ref="fixture-agent",
                agent_config={"model": "fixture-model"},
                required_env=("FAKE_CLI_SCENARIO", "FAKE_CLI_CALLS"),
            ),
            "task",
            self.workdir,
            session_ref=session_ref,
            poll_seconds=0.01,
        )

    def argv(self) -> list[str]:
        return json.loads((self.calls / "call-00.json").read_text())["argv"]

    def test_the_attempts_transcript_is_mirrored_when_it_ends(self) -> None:
        self.scenario("th_new")
        report = self.run_once()
        self.assertEqual(report.session_ref, "th_new")
        self.assertEqual(
            sorted(self.s3.objects),
            [f"fleet/sessions/codex/th_new/{ROLLOUT.format(ref='th_new')}"],
        )

    def test_a_resume_nobody_can_serve_becomes_a_fresh_run(self) -> None:
        self.scenario("th_new")
        report = self.run_once(session_ref="th_gone")
        self.assertFalse(report.resumed)
        self.assertNotIn("resume", self.argv())

    def test_a_resume_the_mirror_can_serve_still_resumes(self) -> None:
        # Worker A's transcript is in the bucket; this host has never seen
        # the session, and resumes it anyway.
        self.s3.objects[f"fleet/sessions/codex/th_old/{ROLLOUT.format(ref='th_old')}"] = (
            b"worker A turns\n",
            self.s3.clock,
        )
        self.scenario(None)
        report = self.run_once(session_ref="th_old")
        self.assertTrue(report.resumed)
        self.assertIn("resume", self.argv())
        self.assertIn("th_old", self.argv())
        self.assertEqual(
            (self.home / ROLLOUT.format(ref="th_old")).read_text(), "worker A turns\n"
        )


class MirrorUnsetTest(unittest.TestCase):
    """Unset AGENT_RUNNER_STATE_S3 and the module is inert: nothing built,
    nothing fetched, no decision changed."""

    def setUp(self) -> None:
        patcher = mock.patch.dict(_os.environ, {}, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        _os.environ.pop(state.STATE_S3_ENV, None)
        exploding = mock.patch.object(
            state, "s3_client", mock.Mock(side_effect=AssertionError("built a client"))
        )
        exploding.start()
        self.addCleanup(exploding.stop)

    def test_no_mirror_and_no_client(self) -> None:
        self.assertIsNone(state.mirror())
        self.assertIsNone(state.active_mirror())

    def test_every_hook_is_a_no_op(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adapter = get_adapter("codex")
            with mock.patch.dict(_os.environ, {"CODEX_HOME": str(tmp)}):
                # Resume decisions are untouched: an absent transcript is
                # still handed to the CLI, exactly as before the mirror.
                self.assertTrue(sessions.ensure_session_local(adapter, "th_gone"))
                sessions.push_session(adapter, "th_gone")
            directory = workdirs.checkpoint_dir(Path(tmp), "scrape", "2026FALL")
            workdirs.pull_checkpoints(directory)
            workdirs.push_checkpoints(directory)


class MissingDependencyTest(unittest.TestCase):
    """The variable is set but the extra is not installed: loud at startup,
    a warning at attempt time."""

    def setUp(self) -> None:
        env = mock.patch.dict(_os.environ, {state.STATE_S3_ENV: BUCKET_URL})
        env.start()
        self.addCleanup(env.stop)
        # sys.modules[name] = None makes `import name` raise ImportError, so
        # this holds whether or not boto3 is installed here.
        absent = mock.patch.dict(sys.modules, {"boto3": None})
        absent.start()
        self.addCleanup(absent.stop)

    def test_startup_refuses_with_install_guidance(self) -> None:
        with self.assertRaises(RuntimeError) as caught:
            state.mirror()
        self.assertIn("boto3", str(caught.exception))
        self.assertIn("agent-runner[s3]", str(caught.exception))

    def test_prepare_session_homes_refuses_to_boot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(RuntimeError):
                sessions.prepare_session_homes(Path(tmp), apply=False)

    def test_an_attempt_warns_instead_of_failing(self) -> None:
        self.assertIsNone(state.active_mirror())


if __name__ == "__main__":
    unittest.main()


class LiveSessionMirrorTest(MirrorCase):
    """A running attempt's transcript reaches the mirror while it grows —
    the same whole-file copy as the final push, made only when the file
    changed, and never before the CLI has named its session."""

    KEY = "fleet/sessions/codex/th_live/" + ROLLOUT.format(ref="th_live")

    def test_each_change_is_pushed_and_an_unchanged_file_is_not(self) -> None:
        adapter = get_adapter("codex")
        home = self.codex_home("worker-a")
        path = self.write_rollout(home, "th_live", "turn 1\n")
        live = sessions.LiveSessionMirror(adapter, lambda: "th_live")
        with mock.patch.object(self.s3, "upload_file", wraps=self.s3.upload_file) as upload:
            live.tick()
            live.tick()
            self.assertEqual(upload.call_count, 1)
            self.assertEqual(self.s3.objects[self.KEY][0], b"turn 1\n")
            path.write_text("turn 1\nturn 2\n")
            live.tick()
            self.assertEqual(upload.call_count, 2)
            self.assertEqual(self.s3.objects[self.KEY][0], b"turn 1\nturn 2\n")

    def test_no_session_yet_pushes_nothing(self) -> None:
        adapter = get_adapter("codex")
        self.codex_home("worker-a")
        sessions.LiveSessionMirror(adapter, lambda: None).tick()
        self.assertFalse(self.s3.objects)

    def test_the_thread_runs_only_with_a_mirror_configured(self) -> None:
        adapter = get_adapter("codex")
        home = self.codex_home("worker-a")
        self.write_rollout(home, "th_live", "turn 1\n")
        live = sessions.LiveSessionMirror(adapter, lambda: "th_live", interval=0.01)
        live.start()
        for _ in range(200):
            if self.KEY in self.s3.objects:
                break
            time.sleep(0.01)
        live.stop()
        self.assertEqual(self.s3.objects[self.KEY][0], b"turn 1\n")

        with mock.patch.dict(_os.environ, {state.STATE_S3_ENV: ""}):
            idle = sessions.LiveSessionMirror(adapter, lambda: "th_live", interval=0.01)
            idle.start()
            self.assertIsNone(idle._thread)
            idle.stop()

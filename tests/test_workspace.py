#!/usr/bin/env python3
"""The workspace: a sandbox's working tree, kept in S3.

Three contracts, in order of how much damage getting them wrong does:

1. Credential files NEVER travel, in either direction. auth.json and
   .credentials.json sit in the same CLI homes the transcripts do; the
   denylist is what keeps a shared bucket from collecting a login (or
   planting one).
2. prepare / checkpoint / release round-trip: what one sandbox pushed the
   next one pulls, and only a COMPLETE push is ever restored — the
   manifest goes last and names what is there.
3. AGENT_RUNNER_STATE_S3 unset means a fresh workspace and no client.
"""

from __future__ import annotations

import io
import json
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

from agent_runner import state  # noqa: E402
from agent_runner.workspace import (  # noqa: E402
    MANIFEST,
    READY_MARKER,
    RELEASE_MARKER,
    Workspace,
    keeper,
    marker,
)

BUCKET_URL = "s3://runner-state/fleet"
GROUP = "mit/run-7/scrape"
ROLLOUT = "codex-home/sessions/2026/08/24/rollout-2026-08-24T09-15-00-th_1.jsonl"
STAMP = "checkpoints/scrape/2026FALL/progress.json"


class _NoSuchKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3:
    """A dict standing in for a bucket. Only the four calls the mirror
    makes, with boto3's keyword shapes; ``fail_on`` makes one of them raise,
    which is how the best-effort posture gets proven without a network."""

    def __init__(self, fail_on: str = "") -> None:
        self.objects: dict[str, bytes] = {}
        self.puts: list[str] = []
        self.gets: list[str] = []
        self.fail_on = fail_on

    def _check(self, call: str) -> None:
        if call == self.fail_on:
            raise RuntimeError(f"S3 {call} is down")

    def upload_file(self, *, Filename: str, Bucket: str, Key: str) -> None:
        self._check("upload_file")
        self.objects[Key] = Path(Filename).read_bytes()
        self.puts.append(Key)

    def download_file(self, *, Bucket: str, Key: str, Filename: str) -> None:
        self._check("download_file")
        self.gets.append(Key)
        Path(Filename).write_bytes(self.objects[Key])

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str = "") -> None:
        self._check("put_object")
        self.objects[Key] = Body
        self.puts.append(Key)

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        if Key not in self.objects:
            raise _NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def manifest(self) -> dict:
        return json.loads(self.objects[f"fleet/{GROUP}/{MANIFEST}"])


def write(root: Path, relative: str, text: str = "x\n") -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


class WorkspaceCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.s3 = FakeS3()
        self.mirror = state.StateMirror("runner-state", "fleet", self.s3)
        self.root = self.tmp / "a"
        self.workspace = Workspace(self.root, GROUP, self.mirror)


class KeyLayoutTest(WorkspaceCase):
    def test_keys_are_prefix_group_relative(self) -> None:
        self.assertEqual(self.workspace.key(MANIFEST), f"fleet/{GROUP}/manifest.json")
        bare = Workspace(self.root, GROUP, state.StateMirror("b", "", self.s3))
        self.assertEqual(bare.key(ROLLOUT), f"{GROUP}/{ROLLOUT}")

    def test_s3_urls_parse_and_malformed_ones_raise(self) -> None:
        self.assertEqual(state.parse_s3_url("s3://b/p/q"), ("b", "p/q"))
        self.assertEqual(state.parse_s3_url("s3://b"), ("b", ""))
        self.assertEqual(state.parse_s3_url("s3://b/p/"), ("b", "p"))
        for bad in ("https://b/p", "b/p", "s3:///p"):
            with self.assertRaises(RuntimeError):
                state.parse_s3_url(bad)

    def test_a_ref_can_never_steer_a_key_segment(self) -> None:
        self.assertEqual(state.key_segment("a/../b"), "a_.._b")
        self.assertEqual(state.key_segment("run-7:scrape"), "run-7_scrape")
        self.assertEqual(state.key_segment(""), "_")


class CredentialDenylistTest(unittest.TestCase):
    """The exclusion, proven on the two real file names and by shape."""

    def test_the_cli_login_files_are_denied(self) -> None:
        self.assertTrue(state.is_denied("auth.json"))
        self.assertTrue(state.is_denied(".credentials.json"))

    def test_anything_that_smells_like_a_credential_is_denied(self) -> None:
        for name in (
            "credentials.json", ".netrc", ".env", "oauth_token.txt", "SECRET.txt",
            "api_key.json", "cookies.sqlite", "server.pem", "id_rsa.key",
        ):
            self.assertTrue(state.is_denied(name), name)

    def test_session_and_checkpoint_files_travel(self) -> None:
        for name in (
            "rollout-2026-08-20T09-15-00-019f146e-7331-7630.jsonl",
            "08565ee8-3763-47ee-a8f8-e693660a8c7a.jsonl",
            "progress.json",
            "history.jsonl",
            "manifest.json",
        ):
            self.assertFalse(state.is_denied(name), name)


class CheckpointTest(WorkspaceCase):
    def test_nothing_anywhere_is_fresh(self) -> None:
        self.assertEqual(self.workspace.prepare(), "fresh")
        self.assertTrue(self.root.is_dir())

    def test_pushes_what_changed_then_the_manifest_last(self) -> None:
        write(self.root, ROLLOUT)
        write(self.root, STAMP, '{"term": "2026FALL"}')
        self.assertEqual(self.workspace.checkpoint(), 2)
        self.assertEqual(self.s3.puts[-1], f"fleet/{GROUP}/{MANIFEST}")
        self.assertEqual(self.s3.manifest(), {"files": sorted([ROLLOUT, STAMP])})
        # Unchanged: nothing re-uploaded, the manifest still restated.
        puts = len(self.s3.puts)
        self.assertEqual(self.workspace.checkpoint(), 0)
        self.assertEqual(self.s3.puts[puts:], [f"fleet/{GROUP}/{MANIFEST}"])
        # One file grew: only it travels again.
        write(self.root, ROLLOUT, "x\ny\n")
        self.assertEqual(self.workspace.checkpoint(), 1)
        self.assertEqual(self.s3.objects[f"fleet/{GROUP}/{ROLLOUT}"], b"x\ny\n")

    def test_a_deleted_file_leaves_the_manifest(self) -> None:
        stamp = write(self.root, STAMP)
        write(self.root, ROLLOUT)
        self.workspace.checkpoint()
        stamp.unlink()
        self.workspace.checkpoint()
        self.assertEqual(self.s3.manifest(), {"files": [ROLLOUT]})

    def test_runner_markers_and_attempt_workdirs_stay_local(self) -> None:
        write(self.root, ".runner/ready", "fresh")
        write(self.root, ".runner/attempts/k.pid", "1")
        write(self.root, "attempts/k/attempt-01/out.json", "{}")
        write(self.root, ROLLOUT)
        self.assertEqual(list(self.workspace.files()), [ROLLOUT])

    def test_symlinks_never_travel(self) -> None:
        write(self.root, ROLLOUT)
        (self.root / "link.jsonl").symlink_to(self.root / ROLLOUT)
        (self.root / "dirlink").symlink_to(self.root / "codex-home")
        self.assertEqual(list(self.workspace.files()), [ROLLOUT])

    def test_an_upload_failure_is_a_warning_not_a_lost_run(self) -> None:
        self.s3.fail_on = "upload_file"
        write(self.root, ROLLOUT)
        with mock.patch.object(sys, "stderr", new=io.StringIO()) as err:
            self.assertEqual(self.workspace.checkpoint(), 0)
        self.assertIn("upload of", err.getvalue())
        # The manifest names only what landed — nothing.
        self.assertEqual(self.s3.manifest(), {"files": []})
        # Next time the upload works, the file goes.
        self.s3.fail_on = ""
        self.assertEqual(self.workspace.checkpoint(), 1)


class RoundTripTest(WorkspaceCase):
    def other(self, name: str = "b") -> Workspace:
        return Workspace(self.tmp / name, GROUP, self.mirror)

    def test_the_next_sandbox_pulls_the_last_complete_push(self) -> None:
        write(self.root, ROLLOUT, "transcript\n")
        write(self.root, STAMP, '{"term": "2026FALL"}')
        self.workspace.checkpoint()
        other = self.other()
        self.assertEqual(other.prepare(), "pulled")
        self.assertEqual((other.root / ROLLOUT).read_text(), "transcript\n")
        self.assertEqual((other.root / STAMP).read_text(), '{"term": "2026FALL"}')
        self.assertEqual(other.files().keys(), self.workspace.files().keys())

    def test_a_local_copy_is_used_and_never_clobbered(self) -> None:
        write(self.root, ROLLOUT, "old\n")
        self.workspace.checkpoint()
        other = self.other()
        write(other.root, ROLLOUT, "newer, local\n")
        self.assertEqual(other.prepare(), "local")
        self.assertEqual((other.root / ROLLOUT).read_text(), "newer, local\n")
        self.assertEqual(self.s3.gets, [])

    def test_a_half_pushed_tree_is_never_restored(self) -> None:
        # Files in the bucket but no manifest: a keeper died mid-push.
        self.s3.objects[f"fleet/{GROUP}/{ROLLOUT}"] = b"partial"
        self.assertEqual(self.other().prepare(), "fresh")

    def test_a_download_failure_warns_and_the_rest_still_lands(self) -> None:
        write(self.root, ROLLOUT)
        write(self.root, STAMP)
        self.workspace.checkpoint()
        self.s3.fail_on = "download_file"
        with mock.patch.object(sys, "stderr", new=io.StringIO()) as err:
            self.assertEqual(self.other().prepare(), "pulled")
        self.assertIn("download of", err.getvalue())


class CredentialsNeverTravelTest(WorkspaceCase):
    def test_a_login_in_the_home_is_never_uploaded(self) -> None:
        write(self.root, "codex-home/auth.json", '{"tokens": {}}')
        write(self.root, "claude-home/.credentials.json", "{}")
        write(self.root, ROLLOUT)
        self.workspace.checkpoint()
        self.assertEqual(
            sorted(self.s3.objects),
            [f"fleet/{GROUP}/{ROLLOUT}", f"fleet/{GROUP}/{MANIFEST}"],
        )

    def test_a_login_in_the_bucket_is_never_downloaded(self) -> None:
        self.s3.objects[f"fleet/{GROUP}/codex-home/auth.json"] = b"planted"
        self.s3.objects[f"fleet/{GROUP}/{ROLLOUT}"] = b"ok"
        self.s3.put_object(
            Bucket="b", Key=f"fleet/{GROUP}/{MANIFEST}",
            Body=json.dumps({"files": ["codex-home/auth.json", ROLLOUT]}).encode(),
        )
        self.assertEqual(self.workspace.prepare(), "pulled")
        self.assertFalse((self.root / "codex-home/auth.json").exists())
        self.assertTrue((self.root / ROLLOUT).exists())

    def test_the_runners_own_folders_and_credential_components_never_restore(self) -> None:
        planted = {
            ".runner/release": b"",
            "attempts/k/attempt-01/out.json": b"{}",
            "codex-home/auth.json/x": b"squat",
            ROLLOUT: b"ok",
        }
        for relative, body in planted.items():
            self.s3.objects[f"fleet/{GROUP}/{relative}"] = body
        self.s3.put_object(
            Bucket="b", Key=f"fleet/{GROUP}/{MANIFEST}", Body=json.dumps({"files": list(planted)}).encode()
        )
        self.assertEqual(self.workspace.prepare(), "pulled")
        self.assertEqual(sorted(self.workspace.files()), [ROLLOUT])
        self.assertFalse((self.root / ".runner").exists())
        self.assertFalse((self.root / "attempts").exists())
        self.assertFalse((self.root / "codex-home" / "auth.json").exists())

    def test_a_manifest_that_walks_out_of_the_root_is_refused(self) -> None:
        self.s3.objects[f"fleet/{GROUP}/../escaped.jsonl"] = b"nope"
        self.s3.put_object(
            Bucket="b", Key=f"fleet/{GROUP}/{MANIFEST}",
            Body=json.dumps({"files": ["../escaped.jsonl"]}).encode(),
        )
        with mock.patch.object(sys, "stderr", new=io.StringIO()):
            self.workspace.prepare()
        self.assertFalse((self.tmp / "escaped.jsonl").exists())


class KeeperTest(WorkspaceCase):
    """The entrypoint loop, driven in a thread against the fake bucket."""

    def setUp(self) -> None:
        super().setUp()
        env = mock.patch.dict(_os.environ, {state.STATE_S3_ENV: BUCKET_URL})
        env.start()
        self.addCleanup(env.stop)
        client = mock.patch.object(state, "s3_client", lambda: self.s3)
        client.start()
        self.addCleanup(client.stop)

    def wait_for(self, predicate, seconds: float = 10.0) -> None:
        deadline = time.monotonic() + seconds
        while not predicate():
            self.assertLess(time.monotonic(), deadline, "timed out waiting")
            time.sleep(0.05)

    def test_ready_then_checkpoints_until_released(self) -> None:
        stop = threading.Event()
        result: list[int] = []
        thread = threading.Thread(
            target=lambda: result.append(keeper(self.root, GROUP, 0.2, stop)), daemon=True
        )
        with mock.patch.object(sys, "stdout", new=io.StringIO()) as out:
            thread.start()
            self.wait_for(marker(self.root, READY_MARKER).exists)
            self.assertEqual(marker(self.root, READY_MARKER).read_text(), "fresh")
            write(self.root, ROLLOUT)
            self.wait_for(lambda: f"fleet/{GROUP}/{ROLLOUT}" in self.s3.objects)
            write(self.root, STAMP)
            marker(self.root, RELEASE_MARKER).touch()
            thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result, [0])
        self.assertEqual(self.s3.manifest(), {"files": sorted([ROLLOUT, STAMP])})
        lines = out.getvalue().splitlines()
        self.assertEqual(lines[0], "ready fresh")
        self.assertTrue(lines[-1].startswith("released "))

    def test_stop_releases_too(self) -> None:
        stop = threading.Event()
        thread = threading.Thread(target=keeper, args=(self.root, GROUP, 60, stop), daemon=True)
        with mock.patch.object(sys, "stdout", new=io.StringIO()):
            thread.start()
            self.wait_for(marker(self.root, READY_MARKER).exists)
            write(self.root, ROLLOUT)
            stop.set()
            thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertIn(f"fleet/{GROUP}/{ROLLOUT}", self.s3.objects)

    def test_the_second_keeper_pulls_what_the_first_pushed(self) -> None:
        write(self.root, ROLLOUT)
        marker(self.root, RELEASE_MARKER).parent.mkdir(parents=True)
        marker(self.root, RELEASE_MARKER).touch()
        with mock.patch.object(sys, "stdout", new=io.StringIO()):
            self.assertEqual(keeper(self.root, GROUP, 60, threading.Event()), 0)
            other = self.tmp / "b"
            marker(other, RELEASE_MARKER).parent.mkdir(parents=True)
            marker(other, RELEASE_MARKER).touch()
            keeper(other, GROUP, 60, threading.Event())
        self.assertEqual(marker(other, READY_MARKER).read_text(), "pulled")
        self.assertTrue((other / ROLLOUT).exists())


class MirrorUnsetTest(unittest.TestCase):
    """Unset AGENT_RUNNER_STATE_S3 and the module is inert: nothing built,
    nothing fetched, a fresh workspace."""

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

    def test_the_workspace_is_fresh_and_pushes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Workspace(Path(tmp) / "w", GROUP, state.mirror())
            self.assertEqual(workspace.prepare(), "fresh")
            write(workspace.root, ROLLOUT)
            self.assertEqual(workspace.checkpoint(), 0)
            self.assertEqual(workspace.checkpoint(), 0)


class MissingDependencyTest(unittest.TestCase):
    """The variable is set but the extra is not installed: loud at startup."""

    def setUp(self) -> None:
        env = mock.patch.dict(_os.environ, {state.STATE_S3_ENV: BUCKET_URL})
        env.start()
        self.addCleanup(env.stop)
        absent = mock.patch.dict(sys.modules, {"boto3": None})
        absent.start()
        self.addCleanup(absent.stop)

    def test_the_keeper_refuses_to_start_with_install_guidance(self) -> None:
        with self.assertRaises(RuntimeError) as caught:
            state.mirror()
        self.assertIn("boto3", str(caught.exception))
        self.assertIn("agent-runner[s3]", str(caught.exception))


if __name__ == "__main__":
    unittest.main()

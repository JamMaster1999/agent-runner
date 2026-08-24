#!/usr/bin/env python3
"""Workdirs and checkpoints: the term-scoped folder builder and the
term-stamp verification gate (matrix B3, adapter half: checkpoints never
cross terms — the path is term-scoped AND every file's stamp is verified
at resume; mismatch discards loudly and runs fresh).
"""

from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

import os as _os

REPO = Path(__file__).resolve().parents[1]
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import workdirs  # noqa: E402


class CheckpointDirTest(unittest.TestCase):
    def test_term_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                workdirs.checkpoint_dir(Path(tmp), "scrape", "")

    def test_path_is_term_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fall = workdirs.checkpoint_dir(Path(tmp), "scrape", "2026FALL")
            spring = workdirs.checkpoint_dir(Path(tmp), "scrape", "2027SPRING")
            self.assertEqual(fall, Path(tmp) / "checkpoints" / "scrape" / "2026FALL")
            self.assertNotEqual(fall, spring)
            self.assertFalse(fall.exists())  # pure: named from outside, created by the attempt side


class TermStampVerificationTest(unittest.TestCase):
    def write_checkpoint(self, directory: Path, name: str, term: str | None) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / name
        payload = {"pages_done": 12}
        if term is not None:
            payload["term"] = term
        path.write_text(json.dumps(payload))
        return path

    def test_matching_stamps_survive_and_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory = workdirs.checkpoint_dir(Path(tmp), "scrape", "2026FALL")
            kept = self.write_checkpoint(directory, "progress.json", "2026FALL")
            survivors = workdirs.verify_or_discard(directory, "2026FALL")
            self.assertEqual(survivors, [kept])
            self.assertTrue(kept.exists())

    def test_cross_term_stamps_are_discarded_loudly(self) -> None:
        # The term-rollover reuse bug class: a Fall checkpoint must never
        # seed a Spring scrape, even if the folder somehow carries it.
        with tempfile.TemporaryDirectory() as tmp:
            directory = workdirs.checkpoint_dir(Path(tmp), "scrape", "2027SPRING")
            stale = self.write_checkpoint(directory, "progress.json", "2026FALL")
            fresh = self.write_checkpoint(directory, "fresh.json", "2027SPRING")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                survivors = workdirs.verify_or_discard(directory, "2027SPRING")
            self.assertEqual(survivors, [fresh])
            self.assertFalse(stale.exists())
            self.assertIn("term-stamp mismatch", stderr.getvalue())
            self.assertIn("progress.json", stderr.getvalue())

    def test_unstamped_or_unparsable_files_count_as_mismatch(self) -> None:
        # Resume must never trust what it cannot prove.
        with tempfile.TemporaryDirectory() as tmp:
            directory = workdirs.checkpoint_dir(Path(tmp), "scrape", "2026FALL")
            unstamped = self.write_checkpoint(directory, "unstamped.json", None)
            garbage = directory / "garbage.json"
            garbage.write_text("not json{")
            with redirect_stderr(io.StringIO()):
                survivors = workdirs.verify_or_discard(directory, "2026FALL")
            self.assertEqual(survivors, [])
            self.assertFalse(unstamped.exists())
            self.assertFalse(garbage.exists())

    def test_missing_directory_verifies_empty(self) -> None:
        matching, mismatched = workdirs.verify_checkpoints(
            Path("/nonexistent/checkpoints"), "2026FALL"
        )
        self.assertEqual((matching, mismatched), ([], []))


if __name__ == "__main__":
    unittest.main()

"""Workdirs: the local folders a model is handed (agent_runner.md).

The model only ever sees folders, never storage: the runner prepares an
attempt's private workspace and, for children that declare one, a
term-scoped checkpoint folder the project renders into its prompt as
``${checkpoint_dir}``. Where those folders live (a worker volume, a Mac
disk) is the caller's choice of root.

Checkpoints (durable_execution.md): one function builds the folder path and
term is a required argument; every checkpoint file carries its term inside;
before any resume the caller verifies each stamp against the run's term —
match resumes, mismatch discards loudly and runs fresh. Any failure costs
time, never correctness.

A folder lives on one worker's disk, so the optional state mirror
(``agent_runner.state``) carries it between hosts: ``pull_checkpoints``
before the stamps are verified, ``push_checkpoints`` after an attempt
stamped them. With no mirror configured both are no-ops.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from agent_runner import state

TERM_STAMP_KEY = "term"


def attempt_workdir(root: Path, name: str, attempt: int) -> Path:
    """The attempt's private workspace: ``{root}/{name}/attempt-NN``,
    created on first touch."""
    path = Path(root) / name / f"attempt-{attempt:02d}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def checkpoint_dir(root: Path, child: str, term: str) -> Path:
    """THE one function that builds a checkpoint folder path; ``term`` is a
    required argument, so a term-less checkpoint path is unrepresentable.
    The folder is created on first touch."""
    if not term:
        raise ValueError("checkpoint_dir requires a non-empty term")
    path = Path(root) / "checkpoints" / child / term
    path.mkdir(parents=True, exist_ok=True)
    return path


def checkpoint_term(path: Path) -> str | None:
    """The term stamp carried inside one checkpoint file (a JSON object's
    top-level ``term`` key). None when the file is unreadable, not JSON, or
    unstamped — all treated as a mismatch by verification, because an
    unprovable stamp must never be resumed across a term boundary."""
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if isinstance(data, dict):
        value = data.get(TERM_STAMP_KEY)
        if isinstance(value, str) and value:
            return value
    return None


def verify_checkpoints(directory: Path, term: str) -> tuple[list[Path], list[Path]]:
    """Every checkpoint file's term stamp checked against the run's term:
    returns ``(matching, mismatched)`` file lists. Non-JSON sidecar files
    count as mismatched — resume must never trust what it cannot prove."""
    directory = Path(directory)
    matching: list[Path] = []
    mismatched: list[Path] = []
    if not directory.is_dir():
        return matching, mismatched
    for path in sorted(directory.iterdir()):
        if not path.is_file():
            continue
        if checkpoint_term(path) == term:
            matching.append(path)
        else:
            mismatched.append(path)
    return matching, mismatched


def verify_or_discard(directory: Path, term: str) -> list[Path]:
    """The pre-resume gate: files whose stamp matches the run's term
    survive; every other file is DISCARDED and the discard is logged loudly
    (stderr) — the run then scrapes fresh for whatever was lost. Returns the
    surviving files."""
    matching, mismatched = verify_checkpoints(directory, term)
    for path in mismatched:
        print(
            f"WARNING: checkpoint term-stamp mismatch: {path} does not carry "
            f"term {term!r}; discarding it and running fresh.",
            file=sys.stderr,
        )
        try:
            path.unlink()
        except OSError as exc:
            print(
                f"WARNING: could not discard mismatched checkpoint {path}: {exc}",
                file=sys.stderr,
            )
    return matching


def checkpoint_group(directory: Path) -> str:
    """One checkpoint folder's key prefix in the state mirror: its own
    absolute path. The path already carries every scope its caller gave the
    folder (run, child, term) and every worker mounts the volume at the same
    place, so the path IS the identity — there is no second naming scheme to
    keep in sync with the first."""
    parts = [
        state.key_segment(part)
        for part in Path(directory).parts
        if part not in (os.sep, "/")
    ]
    return "/".join(["checkpoints", *parts])


def push_checkpoints(directory: Path) -> None:
    """Mirror a checkpoint folder after the attempt that stamped it. Files
    only, one key each, top level only — exactly the set the term-stamp gate
    verifies."""
    mirror = state.active_mirror()
    if mirror is None:
        return
    directory = Path(directory)
    if not directory.is_dir():
        return
    mirror.push(
        checkpoint_group(directory),
        directory,
        [path for path in sorted(directory.iterdir()) if path.is_file()],
    )


def pull_checkpoints(directory: Path) -> None:
    """Bring other workers' stamps here before the term-stamp gate reads
    them, so verification judges the run's whole progress and not just this
    host's share of it."""
    mirror = state.active_mirror()
    if mirror is None:
        return
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    mirror.pull(checkpoint_group(directory), directory)

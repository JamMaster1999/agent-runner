"""Sessions: volume-backed CLI session state, and the resume preamble
(agent_runner.md — core's sessions duty).

Two things depend on a session surviving its worker: a retry resumes the
CLI session instead of restarting, and the transcript is the post-run
record of exactly what the model was sent. Both live wherever the CLI's
home directory points — so this module's job is pointing every adapter's
home at one durable root (the worker volume), via the adapters'
``prepare_home`` hooks. Extracting a live session ref from a captured
stream is the adapters' ``session_ref_from_log``; carrying that ref between
attempts is the caller's (the Temporal layer rides it in heartbeat
details).

A volume makes a session survive its worker; the optional state mirror
(``agent_runner.state``) makes it survive its HOST, so a retry that lands
anywhere can resume. ``push_session`` mirrors a transcript once the attempt
that wrote it ends, and ``ensure_session_local`` fetches one back before a
resume is attempted. With no mirror configured both are no-ops.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from agent_runner import state
from agent_runner.harness import registered_adapters
from agent_runner.harness.base import HarnessAdapter

# Default resume preamble, vocabulary-neutral: it names no project output
# convention. A caller whose contract has its own naming supplies
# policy["resume_preamble"] on the RunSpec (run_attempt prefers it).
RESUME_PREAMBLE = (
    "RESUME: You are resuming your own earlier session for this exact job; "
    "it was interrupted before the output file was written. Reuse the "
    "research already in this conversation — do not redo items you fully "
    "finished — and complete the remaining ones. Evidence you fetched "
    "earlier in this conversation counts as seen this run. The work packet "
    "below is identical to the one you were given; any run identifiers and "
    "the output path in it are NEW and replace the old ones.\n\n"
)


def prepare_session_homes(volume_root: Path, *, apply: bool = True) -> dict[str, str]:
    """Point every registered adapter's CLI home at ``volume_root`` and seed
    credentials there (the Modal model, ruling D1): each adapter creates its
    home directory under the volume, seeds its credential file once from the
    environment when absent, and normalizes tokens on read. Refreshed tokens
    the CLI writes back land on the volume and persist across workers.

    Returns the environment overrides (CLI home + auth variables); with
    ``apply`` True (the default, meant for worker startup) they are also
    written into ``os.environ`` so the adapters' ``env_passthrough`` carries
    them into every agent process.
    """
    # Worker startup is where a broken state mirror must surface: a bucket
    # this worker cannot reach is a deploy error, not a per-attempt warning.
    state.mirror()
    volume_root = Path(volume_root)
    volume_root.mkdir(parents=True, exist_ok=True)
    overrides: dict[str, str] = {}
    for adapter in registered_adapters():
        overrides.update(adapter.prepare_home(volume_root, os.environ))
    if apply:
        os.environ.update(overrides)
    return overrides


def session_group(adapter: HarnessAdapter, session_ref: str) -> str | None:
    """This session's key prefix in the mirror, or None for a ref no key can
    safely be built from. A ref is opaque CLI text and also feeds a filename
    glob, so one carrying a separator is refused rather than escaped."""
    if not session_ref or ".." in session_ref or {"/", "\\"} & set(session_ref):
        return None
    return f"sessions/{adapter.name}/{state.key_segment(session_ref)}"


def push_session(adapter: HarnessAdapter, session_ref: str) -> None:
    """Mirror one session's transcript after the attempt that wrote it. The
    CLI has exited by now, so the file on disk is the complete record of
    what the model was sent."""
    active = state.active_mirror()
    group = session_group(adapter, session_ref)
    if active is None or group is None:
        return
    located = adapter.session_state(session_ref)
    if located is None:
        return
    home, files = located
    if files:
        active.push(group, home, files)


def ensure_session_local(adapter: HarnessAdapter, session_ref: str) -> bool:
    """True when this host can resume ``session_ref``: the transcript is
    already here, or the mirror had it and it is here now.

    False is the mirror's one verdict that changes behavior — the transcript
    exists nowhere, so a resume of it could only fail and the caller runs
    fresh instead. Without a mirror configured this always returns True and
    the resume proceeds exactly as it always did."""
    active = state.active_mirror()
    group = session_group(adapter, session_ref)
    if active is None or group is None:
        return True
    located = adapter.session_state(session_ref)
    if located is None:
        return True
    home, files = located
    if files:
        return True
    active.pull(group, home)
    located = adapter.session_state(session_ref)
    if located is not None and located[1]:
        return True
    print(
        f"WARNING: session {session_ref} is on neither this worker nor the "
        f"state mirror; running fresh.",
        file=sys.stderr,
    )
    return False

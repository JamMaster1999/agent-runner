"""Sessions: the CLI homes and the resume preamble (agent_runner.md —
core's sessions duty).

Two things depend on a session surviving the process that opened it: a
retry resumes the CLI session instead of restarting, and the transcript
is the post-run record of exactly what the model was sent. Both live
wherever the CLI's home directory points — so this module's job is
pointing every adapter's home at one root (the sandbox's workspace, a
worker volume, a laptop folder) via the adapters' ``prepare_home`` hooks.
Extracting a live session ref from a captured stream is the adapters'
``session_ref_from_log``; carrying that ref between attempts is the
caller's (the Temporal layer rides it in heartbeat details); carrying the
home between hosts is ``agent_runner.workspace``'s.
"""

from __future__ import annotations

import os
from pathlib import Path

from agent_runner import state
from agent_runner.harness import registered_adapters

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


def prepare_session_homes(root: Path, *, apply: bool = True) -> dict[str, str]:
    """Point every registered adapter's CLI home at ``root`` and seed
    credentials there (ruling D1): each adapter creates its home directory
    under the root, seeds its credential file once from the environment
    when absent, and normalizes tokens on read.

    Returns the environment overrides (CLI home + auth variables); with
    ``apply`` True (the default, meant for process startup) they are also
    written into ``os.environ`` so the adapters' ``env_passthrough`` carries
    them into every agent process.
    """
    # Startup is where a broken state mirror must surface: a bucket this
    # process cannot name is a deploy error, not a per-attempt warning.
    state.mirror()
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    overrides: dict[str, str] = {}
    for adapter in registered_adapters():
        overrides.update(adapter.prepare_home(root, os.environ))
    if apply:
        os.environ.update(overrides)
    return overrides

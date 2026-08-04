#!/usr/bin/env python3
"""Characterization tests for resume_prompt_fingerprint (D2 template hash).

Two attempts received identical work exactly when their fingerprints match —
the definition of a resumable pair. Under D2 the fingerprint is the sha256 of
the PRE-substitution template: run-varying values (run id, attempt, output
directory, CDP endpoint) exist only as {{RUNNER_*}}/{{RESOURCE:*}} tokens
there, so invariance across runs needs no un-substitution and no preamble
stripping. Attempt rows recorded before the template contract carry
old-style fingerprints and simply never match — those sessions go fresh.

(The real-phase-1-builder identity case stayed in the GTM suite with the
prompt builders it exercises; this file is runner-pure.)
"""

from __future__ import annotations

import hashlib
import sys
import unittest
from pathlib import Path

import os as _os

REPO = Path(__file__).resolve().parents[1]
# Runner-repo test header: point the runner's path constants at this repo,
# then put src/ on sys.path when agent_runner is not already importable (the
# no-pip stdlib run — the same path the GTM bootstrap shim relies on).
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
_os.environ.setdefault("RUNNER_PROJECT_ID", "testproj")
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import attempts  # noqa: E402
from agent_runner.engine import runner_variables  # noqa: E402
from agent_runner.templates import substitute  # noqa: E402


def synthetic_template(packet: str = "alpha") -> str:
    """A template carrying every run-varying slot in token form, mirroring
    the real builders: the tokenized output path, the run-id token (bare in
    _meta, shell-quoted in the progress command), and the _meta attempt."""
    return (
        "Production fixture template.\n"
        "- Write exactly one valid JSON object to `{{RUNNER_OUTPUT_PATH}}/out.json`.\n"
        "- Progress command: python3 core/job_event.py progress 'job-key' "
        "--run-id '{{RUNNER_JOB_KEY}}' --attempt {{RUNNER_ATTEMPT}}\n"
        "_meta:\n"
        "{\n"
        '  "attempt": {{RUNNER_ATTEMPT}},\n'
        '  "run_id": "{{RUNNER_JOB_KEY}}"\n'
        "}\n"
        f"Packet: {packet}\n"
    )


class ResumeFingerprintTest(unittest.TestCase):
    def test_fingerprint_is_the_sha256_of_the_template(self) -> None:
        template = synthetic_template()
        self.assertEqual(
            attempts.resume_prompt_fingerprint(template),
            hashlib.sha256(template.encode()).hexdigest(),
        )

    def test_invariant_across_run_id_attempt_and_output_path(self) -> None:
        # Substitution inputs never reach the fingerprint: the hash is taken
        # before run-a/run-b values exist, so two attempts at different runs,
        # attempt numbers, and output dirs share one fingerprint while the
        # substituted prompts they received differ.
        template = synthetic_template()
        fingerprint = attempts.resume_prompt_fingerprint(template)
        prompt_a = substitute(template, runner_variables("run-a", "job-1", 1, Path("/tmp/run-a")))
        prompt_b = substitute(
            template, runner_variables("run-b", "job-1", 3, Path("/tmp/run-b/attempt-03"))
        )
        self.assertNotEqual(prompt_a, prompt_b)
        self.assertEqual(fingerprint, attempts.resume_prompt_fingerprint(template))

    def test_resume_preamble_changes_the_hash_so_it_must_never_be_hashed(self) -> None:
        # The engine fingerprints the template BEFORE prepending the RESUME
        # preamble to the substituted prompt; nothing is stripped any more.
        template = synthetic_template()
        self.assertNotEqual(
            attempts.resume_prompt_fingerprint(template),
            attempts.resume_prompt_fingerprint(attempts.RESUME_PREAMBLE + template),
        )

    def test_different_packet_content_changes_the_fingerprint(self) -> None:
        self.assertNotEqual(
            attempts.resume_prompt_fingerprint(synthetic_template(packet="alpha")),
            attempts.resume_prompt_fingerprint(synthetic_template(packet="beta")),
        )


if __name__ == "__main__":
    unittest.main()

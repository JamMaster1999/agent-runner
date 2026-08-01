#!/usr/bin/env python3
"""Spawn-site OSError classification (Modal phase 3, step 2 item 4).

A Popen OSError (ENOMEM at fan-out width is the realistic case) means no CLI
ever started — it says nothing about the job. It must surface as a retryable
'spawn_failure' RunnerError routed to POLICY's retry row, not escape to the
unhandled-exception catch-all and terminally block the job.
"""

from __future__ import annotations

import argparse
import errno
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import os as _os

REPO = Path(__file__).resolve().parents[1]
# Runner-repo test header: point the runner's path constants at this repo,
# then put src/ on sys.path when agent_runner is not already importable (the
# no-pip stdlib run — the same path the GTM bootstrap shim relies on).
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import engine, outcomes  # noqa: E402
from agent_runner.engine import POLICY, policy_signal, run_agent_job_once  # noqa: E402
from agent_runner.harness import get_adapter  # noqa: E402
from agent_runner.protocol import SubmitRequest  # noqa: E402
from agent_runner.runtime import RunnerError, RunnerJob  # noqa: E402


def phase5_job(prompt_ref: dict | None = None) -> RunnerJob:
    """The step-5 fixture: the generic job the engine receives, derived from
    a full-data SubmitRequest exactly as the facade derives it."""
    return RunnerJob.from_submit(
        SubmitRequest(
            job_key="999_spawn_fixture__phase5_batch_001__claude",
            group_key="999_spawn_fixture",
            task_type="phase5",
            harness="claude",
            labels={"institution": "Spawn Fixture U", "agent": "prod-phase5-instructor"},
            max_attempts=3,
            agent_ref="prod-phase5-instructor",
            prompt_ref=prompt_ref,
            artifact_contract={
                "attempt_dir_name": "phase5_batch_001",
                "output_filename": "phase5_batch_001.json",
                "canonical_path": "results/999_spawn_fixture/claude/phase5_batch_001.json",
            },
            probe_spec={"probe": "phase_output", "repair_rounds": 0, "expensive": False},
            policy={"attempt_timeout_minutes": 90, "resume": True},
            client_refs={"institution_id": "00000000-0000-0000-0000-000000000999"},
        )
    )


class SpawnFailureTest(unittest.TestCase):
    """Popen raising OSError inside run_agent_job_once."""

    def run_once_with_failing_popen(self, exc: OSError) -> RunnerError:
        args = argparse.Namespace(database_url="postgres://unused", force_rerun=True)
        job = phase5_job()
        with tempfile.TemporaryDirectory() as tmp:
            with (
                # DB writers: pipeline_attempts row and job events.
                mock.patch.object(engine, "record_attempt_start"),
                mock.patch.object(engine, "run_job_event"),
                mock.patch.object(engine.subprocess, "Popen", side_effect=exc),
                self.assertRaises(RunnerError) as caught,
            ):
                run_agent_job_once(
                    get_adapter("claude"),
                    args,
                    job,
                    "run-1",
                    Path(tmp),
                    1,
                    # Step-4 contract: the pre-substitution template arrives
                    # as submit data, never from a builder call in the engine.
                    template="prompt body",
                )
        return caught.exception

    def test_enomem_is_a_retryable_spawn_failure(self) -> None:
        failure = self.run_once_with_failing_popen(
            OSError(errno.ENOMEM, "Cannot allocate memory")
        )
        self.assertIsInstance(failure, RunnerError)
        self.assertNotIsInstance(failure, OSError)
        self.assertEqual(failure.code, "spawn_failure")
        self.assertTrue(failure.retryable)
        self.assertFalse(failure.alert)
        self.assertIn("Cannot allocate memory", str(failure))

    def test_spawn_failure_routes_to_the_policy_retry_row(self) -> None:
        failure = self.run_once_with_failing_popen(
            OSError(errno.ENOMEM, "Cannot allocate memory")
        )
        signal = policy_signal(failure)
        self.assertEqual(signal, outcomes.SPAWN_FAILURE)
        self.assertEqual(POLICY[signal].action, "retry")
        self.assertTrue(POLICY[signal].consumes_attempt)

    def test_missing_binary_is_also_a_spawn_failure(self) -> None:
        # FileNotFoundError is an OSError subclass: same classification.
        failure = self.run_once_with_failing_popen(
            FileNotFoundError(errno.ENOENT, "No such file or directory: 'claude'")
        )
        self.assertEqual(failure.code, "spawn_failure")
        self.assertEqual(policy_signal(failure), outcomes.SPAWN_FAILURE)


class TemplateFromSubmitDataTest(unittest.TestCase):
    """The step-4 D2 substitution contract, engine side: the job's
    PRE-substitution template arrives as data (SubmitRequest.prompt_ref's
    text on the RunnerJob), the engine fingerprints the received bytes and
    substitutes the runner variables at attempt start — and a failed DB
    resume claim means a fresh session (the legacy filesystem matchers are
    deleted, design §7.5; the claim path itself is pinned by
    tests/test_resume_claim_sql.py)."""

    TEMPLATE = (
        "Fixture template.\n"
        "Write to `{{RUNNER_OUTPUT_PATH}}/phase5_batch_001.json` "
        "(run {{RUNNER_JOB_KEY}}, attempt {{RUNNER_ATTEMPT}}).\n"
    )

    def run_once(self, tmp: Path) -> tuple[RunnerError, mock.Mock, mock.Mock]:
        args = argparse.Namespace(
            database_url="postgres://unused",
            force_rerun=False,
        )
        job = phase5_job()
        with (
            mock.patch.object(engine, "record_attempt_start") as record_start,
            mock.patch.object(engine, "run_job_event"),
            # The DB claim finds nothing: previously the engine fell back to
            # the adapter's filesystem matcher here; now nothing exists to
            # fall back to and the attempt starts a fresh session.
            mock.patch.object(engine, "claim_resumable_attempt", return_value=None) as claim,
            mock.patch.object(
                engine.subprocess, "Popen", side_effect=OSError(errno.ENOMEM, "boom")
            ),
            self.assertRaises(RunnerError) as caught,
        ):
            run_agent_job_once(
                get_adapter("claude"),
                args,
                job,
                "run-1",
                tmp,
                1,
                template=self.TEMPLATE,
            )
        return caught.exception, record_start, claim

    def test_engine_fingerprints_the_submitted_template_bytes(self) -> None:
        from agent_runner.attempts import resume_prompt_fingerprint

        with tempfile.TemporaryDirectory() as tmp:
            _, record_start, claim = self.run_once(Path(tmp))
        claim.assert_called_once()
        fingerprint = record_start.call_args.args[4]
        self.assertEqual(fingerprint, resume_prompt_fingerprint(self.TEMPLATE))
        self.assertEqual(fingerprint, claim.call_args.args[4])

    def test_engine_substitutes_the_runner_variables_at_attempt_start(self) -> None:
        from agent_runner.engine import runner_variables
        from agent_runner.templates import substitute

        with tempfile.TemporaryDirectory() as tmp:
            self.run_once(Path(tmp))
            directory = Path(tmp) / "phase5_batch_001" / "attempt-01"
            prompt = (directory / "prompt.md").read_text()
        self.assertEqual(
            prompt, substitute(self.TEMPLATE, runner_variables("run-1", 1, directory, None))
        )
        # Fresh session (no DB claim): no RESUME preamble was prepended.
        self.assertTrue(prompt.startswith("Fixture template."))

    def test_the_legacy_filesystem_fallback_is_gone(self) -> None:
        # Design §7.5: the matchers were deleted, not ported — neither the
        # adapters nor the base class offer consume_legacy_session any more.
        for backend in ("claude", "codex"):
            with self.subTest(backend=backend):
                self.assertFalse(hasattr(get_adapter(backend), "consume_legacy_session"))

    def test_submit_spec_prompt_ref_is_the_template_source(self) -> None:
        from agent_runner.attempts import resume_prompt_fingerprint
        from agent_runner.engine import template_from_submit_spec

        job = phase5_job(
            prompt_ref={
                "template": self.TEMPLATE,
                "sha256": resume_prompt_fingerprint(self.TEMPLATE),
            }
        )
        self.assertEqual(template_from_submit_spec(job), self.TEMPLATE)
        self.assertIsNone(template_from_submit_spec(phase5_job(prompt_ref=None)))

    def test_submit_spec_sha256_mismatch_fails_loudly(self) -> None:
        # prompt_ref["sha256"] must be resume_prompt_fingerprint(template):
        # a wrong digest is a client build bug, surfaced as invalid_submit —
        # never silently re-hashed (that could orphan existing sessions).
        from agent_runner.engine import template_from_submit_spec

        job = phase5_job(prompt_ref={"template": self.TEMPLATE, "sha256": "0" * 64})
        with self.assertRaises(RunnerError) as caught:
            template_from_submit_spec(job)
        self.assertEqual(caught.exception.code, "invalid_submit")
        self.assertFalse(caught.exception.retryable)


if __name__ == "__main__":
    unittest.main()

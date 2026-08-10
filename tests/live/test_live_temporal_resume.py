"""The crown jewel: a REAL CLI session resumed across a Temporal activity retry.

Attempt 1 runs a real claude session that learns a codeword, then the activity raises —
simulating a worker dying after the CLI ran but before the result was recorded. Temporal
retries; attempt 2 finds the session_ref in the prior attempt's heartbeat details, resumes
the REAL session, and must produce the codeword it was only ever told in attempt 1.

This is the durable-execution promise (heartbeat details -> session resume) proven with
tokens, not fakes."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

from agent_runner import outcomes

from .conftest import CLAUDE_AGENT, claude_spec, file_check, require_claude, require_live

temporalio = pytest.importorskip("temporalio")

from temporalio import activity, workflow  # noqa: E402
from temporalio.client import Client  # noqa: E402
from temporalio.common import RetryPolicy  # noqa: E402
from temporalio.exceptions import ApplicationError  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

from agent_runner.temporal import TemporalRunConfig, run_agent_attempt  # noqa: E402

pytestmark = [require_live, require_claude]

CODEWORD = "MANGO-42"


@activity.defn
async def codeword_probe(root: str) -> dict:
    attempt = activity.info().attempt
    workdir = Path(root) / f"temporal-attempt-{attempt}"
    workdir.mkdir(parents=True, exist_ok=True)
    config = TemporalRunConfig(heartbeat_seconds=2.0)

    if attempt == 1:
        await run_agent_attempt(
            claude_spec(),
            f"Remember this codeword: {CODEWORD}. Reply with OK and stop. Do not write any files.",
            workdir,
            agent=CLAUDE_AGENT,
            config=config,
            timeout_minutes=6,
        )
        # Give the throttled heartbeat a beat to flush, then die exactly where
        # a worker crash would: after the CLI ran, before anyone recorded it.
        await asyncio.sleep(3)
        raise ApplicationError("simulated worker crash after the CLI run")

    report = await run_agent_attempt(
        claude_spec(),
        "Write the codeword from earlier in this conversation into the file "
        "{{RUNNER_OUTPUT_PATH}}/codeword.txt, then stop.",
        workdir,
        agent=CLAUDE_AGENT,
        validate=file_check("codeword.txt", CODEWORD),
        config=config,
        timeout_minutes=6,
    )
    return {
        "outcome": report.outcome,
        "resumed": report.resumed,
        "text": (report.data or {}).get("text", ""),
    }


# sandboxed=False: the sandbox re-imports this module, whose conftest chain
# touches shutil.which at import time; determinism is not what's under test.
@workflow.defn(sandboxed=False)
class CodewordResume:
    @workflow.run
    async def run(self, root: str) -> dict:
        return await workflow.execute_activity(
            codeword_probe,
            root,
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                maximum_attempts=2, initial_interval=timedelta(seconds=1)
            ),
        )


@pytest.mark.asyncio
async def test_real_session_resumes_across_activity_retry(project_root: Path) -> None:
    async with await WorkflowEnvironment.start_local() as env:
        client: Client = env.client
        async with Worker(
            client,
            task_queue="live-test",
            workflows=[CodewordResume],
            activities=[codeword_probe],
        ):
            result = await client.execute_workflow(
                CodewordResume.run,
                str(project_root),
                id=f"live-resume-{uuid.uuid4().hex[:8]}",
                task_queue="live-test",
            )
    assert result["outcome"] == outcomes.VALID
    assert result["resumed"] is True, "attempt 2 did not resume the session from heartbeat details"
    assert CODEWORD.lower() in result["text"].lower()

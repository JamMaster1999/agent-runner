"""A durable agent workflow: the worker dies, the retry keeps the agent's memory.

This is the production shape. A Temporal workflow runs an agent step with a
retry policy. On the first attempt the agent does real work, and then the
activity crashes before anyone recorded the result. Temporal retries. The
second attempt finds the session handle in the heartbeat that the first
attempt left behind, resumes the same conversation, and finishes the job.

Needs the temporal extra (pip install 'agent-runner[temporal]'). The first run
downloads a small Temporal dev server binary; no server setup, no docker.

Run it:

    python examples/temporal_pipeline.py
"""

import asyncio
import os
import uuid
from datetime import timedelta
from pathlib import Path

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agent_runner.harness.base import AgentDef
from agent_runner.runtime import RunSpec, Verdict
from agent_runner.temporal import TemporalRunConfig, run_agent_attempt

os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(Path.cwd()))

AGENT = AgentDef(
    name="pipeline-researcher",
    description="a demo agent inside a durable workflow",
    config={"model": "haiku"},
    body="Do exactly what the task says, then stop.\n",
)


def check(directory: Path) -> Verdict:
    path = directory / "answer.txt"
    if not path.is_file():
        return Verdict(valid=False, message="answer.txt missing")
    return Verdict(valid=True, data={"text": path.read_text()})


@activity.defn
async def research_step(root: str) -> dict:
    attempt = activity.info().attempt
    workdir = Path(root) / f"attempt-{attempt}"
    workdir.mkdir(parents=True, exist_ok=True)
    config = TemporalRunConfig(heartbeat_seconds=2.0)
    spec = RunSpec(key="pipeline-demo", harness="claude")

    if attempt == 1:
        print("attempt 1: the agent works, then the worker dies...")
        await run_agent_attempt(
            spec,
            "Remember this codeword: DURABLE-9. Reply with OK and stop. Do not write any files.",
            workdir,
            agent=AGENT,
            config=config,
            timeout_minutes=6,
        )
        await asyncio.sleep(3)  # let the heartbeat carrying the session handle flush
        raise ApplicationError("worker crashed before the result was recorded")

    print("attempt 2: a fresh attempt resumes the session from the heartbeat...")
    report = await run_agent_attempt(
        spec,
        "Write the codeword from earlier in this conversation into the file "
        "{{RUNNER_OUTPUT_PATH}}/answer.txt, then stop.",
        workdir,
        agent=AGENT,
        validate=check,
        config=config,
        timeout_minutes=6,
    )
    return {"resumed": report.resumed, "text": (report.data or {})["text"].strip()}


# sandboxed=False keeps the demo in one file; real projects put workflow
# definitions in their own import-clean module.
@workflow.defn(sandboxed=False)
class ResearchPipeline:
    @workflow.run
    async def run(self, root: str) -> dict:
        return await workflow.execute_activity(
            research_step,
            root,
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=2, initial_interval=timedelta(seconds=1)),
        )


async def main() -> None:
    root = Path("runs/temporal-demo").absolute()
    async with await WorkflowEnvironment.start_local() as env:
        client: Client = env.client
        async with Worker(
            client, task_queue="demo", workflows=[ResearchPipeline], activities=[research_step]
        ):
            result = await client.execute_workflow(
                ResearchPipeline.run,
                str(root),
                id=f"pipeline-demo-{uuid.uuid4().hex[:8]}",
                task_queue="demo",
            )
    print(f"resumed across the crash: {result['resumed']}")
    print(f"attempt 2 never learned the codeword. The session knew it: {result['text']}")


if __name__ == "__main__":
    asyncio.run(main())

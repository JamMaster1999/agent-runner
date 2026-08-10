"""Run a batch of research agents in parallel on one subscription.

Give it a list of topics. It fans out one Claude Code agent per topic, four at
a time, and collects a short summary file for each. There is no API key and no
per-token bill anywhere in this file: every agent runs through the CLI on the
plan you already pay for.

Run it:

    python examples/parallel_fanout.py "solar panels" "wind turbines" "heat pumps"

Swap harness="claude" for "codex" to run the same batch on your ChatGPT plan.
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from agent_runner.attempt import run_attempt
from agent_runner.harness.base import AgentDef
from agent_runner.runtime import RunSpec, Verdict

os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(Path.cwd()))

AGENT = AgentDef(
    name="researcher",
    description="researches one topic and writes a short summary",
    config={"model": "haiku"},
    body="You are a careful researcher. Do exactly what the task says, then stop.\n",
)


def check(workdir: Path) -> Verdict:
    path = workdir / "summary.md"
    if not path.is_file():
        return Verdict(valid=False, message="summary.md missing")
    return Verdict(valid=True, data={"summary": path.read_text()})


def research(topic: str):
    workdir = Path("runs") / topic.replace(" ", "-")
    workdir.mkdir(parents=True, exist_ok=True)
    return topic, run_attempt(
        RunSpec(key="research-" + topic, harness="claude"),
        "Research " + topic + " briefly and write a short summary with three "
        "concrete facts to {{RUNNER_OUTPUT_PATH}}/summary.md, then stop.",
        workdir,
        agent=AGENT,
        validate=check,
        timeout_minutes=6,
    )


def main() -> None:
    topics = sys.argv[1:] or ["solar panels", "wind turbines", "heat pumps"]
    print(f"fanning out {len(topics)} agents, four at a time...")

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(research, topics))

    total_tokens = 0
    for topic, report in results:
        total_tokens += report.usage.tok_output
        print(f"  {topic}: {report.outcome}  (runs/{topic.replace(' ', '-')}/summary.md)")
    print(f"done. {total_tokens} output tokens across the batch, zero API dollars.")


if __name__ == "__main__":
    main()

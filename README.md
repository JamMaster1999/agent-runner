# agent-runner

Use Claude Code and Codex as workers in your automated workflows.

An agentic workflow is a chain of agent steps: research a topic, check the result, save it, move on. Real workflows run many of these agents at once. The cheapest way to run them is through the agent CLIs you already subscribe to. Claude Code and Codex run on your Claude or ChatGPT subscription, so a workflow that fans out fifty research agents needs no API key and no per-token bill.

The problem is that these CLIs were built for a person at a keyboard, not for automation. Left alone, one will retry a dead login for twenty minutes, another reports success after a failed run, and the real error hides inside a JSON stream. agent-runner wraps one CLI run in a loop that always cleans up the process, reads the stream as it happens, and ends with one honest word that says what happened. It also hands you the session, so a retry can pick up where the agent left off instead of paying for the same work twice.

This library grew out of a production pipeline that runs thousands of research agents this way. It contains the runner only. Your prompts, your checks, and your storage stay in your code.

## Installation

```bash
pip install git+https://github.com/JamMaster1999/agent-runner.git

# with the Temporal workflow wrapper:
pip install 'agent-runner[temporal] @ git+https://github.com/JamMaster1999/agent-runner.git'
```

Requires Python 3.13+ and at least one agent CLI, installed and logged in with your subscription:

```bash
npm install -g @anthropic-ai/claude-code   # then: claude login
npm install -g @openai/codex               # then: codex login
```

## Quick start

One agent, one task, one result file.

```python
import os
from pathlib import Path

from agent_runner.attempt import run_attempt
from agent_runner.harness.base import AgentDef
from agent_runner.runtime import RunSpec

# Working folders and agent files are created under this root.
os.environ["AGENT_RUNNER_PROJECT_ROOT"] = str(Path.cwd())

workdir = Path("run-1")
workdir.mkdir(exist_ok=True)

report = run_attempt(
    RunSpec(key="hello", harness="claude"),
    "Write the word PONG into the file {{RUNNER_OUTPUT_PATH}}/out.txt, then stop.",
    workdir,
    agent=AgentDef(
        name="hello-agent",
        description="a minimal demo agent",
        config={"model": "haiku"},
        body="Do exactly what the task says, then stop.\n",
    ),
)

print(report.outcome)           # "valid"
print(report.session_ref)       # the handle a later run can resume
print(report.usage.tok_output)  # token usage, read from the stream
```

`{{RUNNER_OUTPUT_PATH}}` is replaced with the run's working folder at start time. The agent only ever sees a normal local path. To run the same task on Codex, set `harness="codex"` and give the agent a Codex model name in its config, or an empty config for the defaults.

## Outcomes

Every run ends with exactly one word on `report.outcome`:

| Outcome | Meaning |
|---|---|
| `valid` | the result exists and passed your check |
| `invalid_schema` | the agent ran, but the result failed your check |
| `rate_limited` | the provider said slow down, including subscription usage caps |
| `auth` | the login is dead, so fail fast instead of burning retries |
| `timeout` | the run took too long and the process was stopped |
| `spawn_failure` | the CLI never started, for example a missing binary |
| `infra` | it failed, and the CLI's own error output proves nothing more specific |

Two rules keep these words honest:

- **A good result beats the exit code.** If the agent wrote a passing result and the CLI crashed on the way out, the run is still `valid`. If the CLI exited cleanly but its final turn failed, the run is the failure it tried to hide.
- **Errors are read only from the CLI's own error output, never from the agent's transcript.** A research agent may quote a web page that contains "403". That must never be mistaken for a real error.

```python
from agent_runner import outcomes

if report.outcome == outcomes.VALID:
    save(report.data)
elif report.outcome == outcomes.RATE_LIMITED:
    wait_and_retry()               # costs nothing, the work is intact
elif report.outcome == outcomes.AUTH:
    alert_operator(report.error)   # retrying a dead login wastes time
```

## Checking the result

The check is yours: pass a function that returns a `Verdict`. If the check fails and you supply a repair message, that message is sent into the agent's still-open session (on CLIs that support follow-up messages, currently Codex). Fixing a malformed result this way costs seconds, not a fresh run.

```python
import json
from agent_runner.runtime import Verdict

def check(workdir):
    path = workdir / "answer.json"
    if not path.is_file():
        return Verdict(valid=False, message="answer.json missing",
                       repair_message="You did not write answer.json. Write it now, then stop.")
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        return Verdict(valid=False, message=str(e),
                       repair_message=f"answer.json is not valid JSON ({e}). Rewrite it, then stop.")
    return Verdict(valid=True, data=data)

report = run_attempt(
    RunSpec(key="survey", harness="codex", agent_config={}, repair_rounds=2),
    "Answer the question as JSON in {{RUNNER_OUTPUT_PATH}}/answer.json: ...",
    workdir,
    validate=check,
)
report.repair_rounds_used  # how many repair messages it took
```

## Resuming a session

Every run surfaces the CLI's session handle as soon as the stream reveals it. Pass it back on the next run and the CLI reopens that conversation, with all of its context, instead of starting over. In a workflow this is the difference between a crash costing seconds and a crash costing a full research run.

```python
first = run_attempt(spec, "Research the topic and remember your findings.", workdir_1, agent=agent)

second = run_attempt(
    spec,
    "Now write the findings from this conversation to {{RUNNER_OUTPUT_PATH}}/findings.md.",
    workdir_2,
    agent=agent,
    session_ref=first.session_ref,   # resume, do not restart
)
second.resumed  # True
```

## Running agents in parallel

`run_attempt` is a plain blocking function, so scaling out is ordinary Python. Every run gets its own working folder and its own session. The CLI login is shared, which is the point: all of them run on the one subscription.

```python
from concurrent.futures import ThreadPoolExecutor

topics = ["solar panels", "wind turbines", "heat pumps", "geothermal"]

def research(topic):
    workdir = Path("runs") / topic.replace(" ", "-")
    workdir.mkdir(parents=True, exist_ok=True)
    return run_attempt(
        RunSpec(key="research-" + topic, harness="claude"),
        "Research " + topic + " and write a one-page summary to {{RUNNER_OUTPUT_PATH}}/summary.md, then stop.",
        workdir,
        agent=agent,
    )

with ThreadPoolExecutor(max_workers=8) as pool:
    reports = list(pool.map(research, topics))
```

## Workflows that survive crashes

For workflows that must survive worker crashes and run for hours, agent-runner ships a ready-made wrapper for [Temporal](https://temporal.io) as the `[temporal]` extra. Inside a Temporal activity it adds:

- a heartbeat while the CLI runs, so the server knows the agent is alive without guessing from a clock
- the session handle rides the heartbeat, so a retry on any machine resumes the same session
- outcomes become typed retry errors: `rate_limited` waits long, `infra` retries elsewhere, `auth` stops immediately
- a resume budget, so a poisoned session is eventually abandoned for a fresh one

```python
from temporalio import activity
from agent_runner.temporal import run_agent_attempt

@activity.defn
async def research(packet: dict) -> dict:
    report = await run_agent_attempt(
        spec_for(packet), task_for(packet), workdir_for(packet),
        agent=agent_for(packet), validate=check_for(packet),
    )
    return report.data  # failed outcomes raise typed errors for Temporal's retry policy
```

## Using your subscription on servers

On your laptop the CLIs are logged in already. On a server fleet, seed the login once from environment variables onto a shared disk. Each CLI writes its refreshed tokens back to that disk, so the login keeps working across restarts and new machines, and every worker runs on the same subscription.

```python
from pathlib import Path
from agent_runner.auth import prepare_auth

# Reads CODEX_AUTH_JSON and CLAUDE_CREDENTIALS_JSON from the environment,
# writes them to the volume the first time, and returns the environment
# settings that point each CLI at that home.
env = prepare_auth(Path("/data"))
```

## Giving an agent a browser

A run can declare that it needs a browser. The `cdp_browser` resource starts Chrome and hands its address into the task as a template value. Runs that declare nothing carry no browser code.

```python
from agent_runner.resources import cdp_browser

spec = RunSpec(key="scrape", harness="codex", agent_config={},
               resource_specs=({"kind": "cdp_browser"},))
run_attempt(spec, "Connect to the browser at {{RESOURCE:cdp_browser.endpoint}} ...",
            workdir, resources={"cdp_browser": cdp_browser.provider()})
```

## How CLI support works

Everything specific to one CLI lives in one adapter file under [`src/agent_runner/harness/`](src/agent_runner/harness/): command shapes, stream formats, error markers, login files, resume mechanics. The core never mentions a provider by name. Supporting a new CLI means writing one adapter. The `RUNNER_CLAUDE_CLI` and `RUNNER_CODEX_CLI` variables override where the binaries are found, which is also how the test suite swaps in a fake CLI.

## Docker base image

The repo's [`Dockerfile`](Dockerfile) builds a base image for containerized workers: pinned CLI versions, Chrome for the browser resource, and this package with the Temporal extra. The image records exactly which versions went into it, in its labels and in `/etc/agent-runner-provenance.json`. Extend it and add only your own code.

## Testing

```bash
# free: a fake CLI replays scripted output through the real adapters
python -m unittest discover tests

# the live tier: real CLIs, real tokens, run on purpose
RUN_LIVE=1 pytest tests/live
```

The live tier proves what fakes cannot: that resume really recalls context, that repair really lands in the open session, and that auth failures, rate limits, and usage caps end with the right outcome word. Rate limits are reproduced by pointing the real CLI at a local stub server that answers with the provider's error responses, so nothing waits on a real cap and nothing spends tokens. CI runs the suite with and without Temporal installed and fails the build if a Temporal import leaks into the core.

## License

MIT

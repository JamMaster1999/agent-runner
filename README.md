# Agent Runner

A production runtime for AI coding agents. Run Claude Code and Codex headless from Python, on the subscription you already pay for: spawn the CLI agent, stream its work, and get back a single classified outcome. That outcome is the contract a workflow engine needs to treat agents as reliable steps.

## The problem

Building with LLMs has followed a clear progression:

1. **Chat completions**: stateless request/response. Send a prompt, get text back.
2. **Workflows**: deterministic multi-step processes. Reliable, but no reasoning.
3. **Agents**: autonomous LLM-driven processes. Powerful, but they fail unpredictably. They time out, hit rate limits, produce malformed output, lose auth mid-run.
4. **Agentic workflows**: agents as managed steps inside reliable workflows. The agent does the thinking; the infrastructure provides the guarantees.

Step 4 is where things break down. Workflow engines need activities that return a typed result and declare whether a failure is retryable. Agents don't naturally do that. They crash, hang, or silently produce garbage. And the agent CLIs were built for a person at a keyboard, not for automation. Left alone, one will retry a dead login for twenty minutes, another reports success after a failed run, and the real error hides inside a JSON stream.

**agent-runner is the layer between your workflow and your agents.** It manages the full lifecycle of a CLI-based coding agent (Claude Code, Codex) as a subprocess: spawn, stream, validate, classify, repair, and always reap. Every attempt ends with exactly one of eight outcomes, and each outcome maps cleanly onto retry logic. It also hands you the session, so a retry can pick up where the agent left off instead of paying for the same work twice. And because the agents run through the CLIs, everything runs on your existing Claude or ChatGPT plan instead of per-token API billing.

## Installation

```bash
pip install git+https://github.com/JamMaster1999/agent-runner.git

# with the Temporal workflow wrapper, and the S3 state mirror:
pip install 'agent-runner[temporal,s3] @ git+https://github.com/JamMaster1999/agent-runner.git'
```

Requires Python 3.13+ and at least one agent CLI, installed and logged in:

```bash
npm install -g @anthropic-ai/claude-code   # then: claude login
npm install -g @openai/codex               # then: codex login
```



## How it works

Every call to `run_attempt` walks through the same lifecycle:

| Stage        | What happens                                                                                                                                                                            |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Spawn**    | Build the CLI command from a `RunSpec` and an `AgentDef`, start it as a subprocess, and write the task prompt to its stdin. The command shape is provider-specific, but your code never branches on a provider name. |
| **Stream**   | Read the CLI's JSON output line by line as it runs. `StreamEvent`s surface progress, tool calls, token usage, and cost in real time via an `on_event` callback.                          |
| **Validate** | When the CLI exits, call your `validate` function. You decide what a correct result looks like. agent-runner never parses or judges the agent's output.                                  |
| **Classify** | End the attempt with exactly one of eight outcome words (`valid`, `invalid_schema`, `rate_limited`, `infra`, `auth`, `timeout`, `stalled`, `spawn_failure`). Uses only CLI-owned error evidence, never the agent's transcript. |
| **Repair**   | If validation fails, send your repair prompt into the still-open session and re-validate. Fixes a malformed result in seconds without a full re-run. Multiple rounds iterate up to a budget you set. |
| **Resume**   | The CLI's session handle is extracted from the stream as soon as it appears. On the next attempt, pass it back and the agent picks up where it left off with all of its prior context.   |
| **Reap**     | The CLI child process is always terminated and cleaned up, on every exit path. Valid exit, timeout, stall, cancellation, exception: no code path leaves a live agent burning provider budget.    |

Your workflow calls `run_attempt`, gets back an `AttemptReport` with one outcome word, and decides what to do next.

## Quick start

One agent, one task, one result file.

```python
import os
from pathlib import Path

from agent_runner.attempt import run_attempt
from agent_runner.harness.base import AgentDef
from agent_runner.runtime import RunSpec

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

## Examples

Three runnable scripts, each one file:

- [`examples/parallel_fanout.py`](examples/parallel_fanout.py): fan out a batch of parallel AI agents on one subscription, four at a time
- [`examples/kill_and_resume.py`](examples/kill_and_resume.py): kill the process mid-run, then watch a brand new process resume the same session with the agent's memory intact
- [`examples/temporal_pipeline.py`](examples/temporal_pipeline.py): a durable Temporal workflow where the worker crashes after the agent worked, and the retry picks up the same conversation

## The eight outcomes

Every run ends with exactly one word on `report.outcome`. No exceptions, no ambiguity.


| Outcome          | Meaning                                                                | Retry?              |
| ---------------- | ---------------------------------------------------------------------- | ------------------- |
| `valid`          | the result exists and passed your check                                | Done                |
| `invalid_schema` | the agent ran, but the result failed your check                        | Yes                 |
| `rate_limited`   | the provider said slow down, including subscription usage caps         | Yes, long backoff   |
| `auth`           | the login is dead, so fail fast instead of burning retries             | No, alert           |
| `timeout`        | the run took too long and the process was stopped                      | Yes                 |
| `stalled`        | the agent stopped producing output, so the process was stopped         | Yes                 |
| `spawn_failure`  | the CLI never started, for example a missing binary                    | Yes, another worker |
| `infra`          | it failed, and the CLI's own error output proves nothing more specific | Yes, another worker |


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

Validation is yours: pass a function that returns a `Verdict`. agent-runner never parses or judges agent output. It classifies based on your verdict.

If the check fails and you supply a repair message, that message is sent into the agent's still-open session (on CLIs that support follow-up messages, currently Codex and Claude). Fixing a malformed result this way costs seconds, not a fresh run. Multiple rounds iterate up to the spec's `repair_rounds` budget.

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

`run_attempt` is a plain blocking function, so scaling out is ordinary Python. Every run gets its own working folder and its own session. The CLI login is shared, which is the point: all of them run on the one subscription. This is how you batch-process a long list with parallel AI agents and no API key.

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



## Durable workflows that survive crashes

For workflows that must survive worker crashes and run for hours, agent-runner ships a ready-made wrapper for [Temporal](https://temporal.io) as the `[temporal]` extra. Inside a Temporal activity it adds:

- A **heartbeat** while the CLI runs, so the server knows the attempt is alive (whether the agent still is, is the stall watchdog's job)
- The **session handle rides the heartbeat**, so a retry on any machine resumes the same session
- Outcomes become **typed retry errors**: `rate_limited` waits long, `infra` retries elsewhere, `auth` stops immediately
- A **resume budget**, so a poisoned session is eventually abandoned for a fresh one

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
from agent_runner.sessions import prepare_session_homes

# Reads CODEX_AUTH_JSON and CLAUDE_CREDENTIALS_JSON from the environment,
# writes them to the volume the first time, and returns the environment
# settings that point each CLI at that home.
env = prepare_session_homes(Path("/data"))
```



## Workers that can pick up any run

A session transcript and a checkpoint folder are files on one machine's disk. If the retry lands on a different machine, the agent's memory of the run is not there, and the work starts over. Set one variable and those files are written through to S3 as they are produced, so any worker can resume any session and read any checkpoint.

```bash
export AGENT_RUNNER_STATE_S3=s3://my-bucket/agent-state
# AWS credentials and region come from the standard AWS_* variables
```

- **Local disk stays the working layer.** The CLIs read and write their own files exactly as before. Uploads happen after an attempt ends, downloads only when this machine is missing a file a resume needs.
- **Absent everywhere means a fresh run**, which is what a single-machine setup already did.
- **A mirror problem never costs a run.** Failed uploads warn and the local copy stands. A worker does refuse to start when the variable is set but unusable, such as a malformed URL or a missing extra, so a typo surfaces at deploy time instead of at 3am.
- **Logins never travel.** Credential files are excluded in both directions, by name. Every worker seeds its own login from the environment, and a login file on a shared bucket is a login file nobody audits.

Requires the `s3` extra (`pip install 'agent-runner[s3]'`); boto3 is imported only when the variable is set. Unset, this feature does not exist.



## Giving an agent a browser

A run can declare that it needs a browser. The `cdp_browser` resource starts headless Chrome and hands its DevTools (CDP) address into the task as a template value, so a scraping agent can drive a real browser. Runs that declare nothing carry no browser code.

```python
from agent_runner.resources import cdp_browser

spec = RunSpec(key="scrape", harness="codex", agent_config={},
               resource_specs=({"kind": "cdp_browser"},))
run_attempt(spec, "Connect to the browser at {{RESOURCE:cdp_browser.endpoint}} ...",
            workdir, resources={"cdp_browser": cdp_browser.provider()})
```



## Architecture

```
┌─────────────────────────────────────────────┐
│              Your workflow                  │
│         (Temporal, or anything)             │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│            agent-runner core                │
│                                             │
│  run_attempt(spec, task, workdir, ...)      │
│    → spawn → stream → validate → classify  │
│    → repair (optional) → reap (always)     │
│                                             │
│  Returns: AttemptReport with one outcome    │
└──────────────────┬──────────────────────────┘
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
┌─────────────────┐ ┌─────────────────┐
│  Claude Code    │ │     Codex       │
│    adapter      │ │    adapter      │
└─────────────────┘ └─────────────────┘
```

Everything specific to one CLI lives in one adapter file under `[src/agent_runner/harness/](src/agent_runner/harness/)`: command shapes, stream formats, error markers, login files, resume mechanics. The core never mentions a provider by name. Supporting a new CLI means writing one adapter and one `register()` call, with zero changes to core. The `RUNNER_CLAUDE_CLI` and `RUNNER_CODEX_CLI` variables override where the binaries are found, which is also how the test suite swaps in a fake CLI.

**Core** (zero dependencies, stdlib only): the attempt loop, the outcome vocabulary, stream parsing, environment isolation, session management, credential handling, template substitution.

**Optional modules:**

- `agent_runner.temporal`: the Temporal activity wrapper (heartbeat, resume, retry mapping)
- `agent_runner.resources`: resource provisioning (e.g., headless Chrome via CDP)
- `agent_runner.state`: the S3 state mirror (sessions and checkpoints readable from any worker)



## Production guarantees

- **Process hygiene**: the CLI child is always reaped on every exit path (valid, timeout, stall, cancellation, exception). A dead attempt never leaves a live agent burning provider budget.
- **Environment isolation**: agent processes get a filtered environment, a safe baseline plus only what the spec declared. Operator secrets never leak to model-driven shell commands.
- **Fatal early termination**: live stream evidence of dead-end conditions (e.g., auth retry loops) terminates the CLI early instead of waiting out its twenty-minute backoff ladder.
- **Stall watchdog**: alive is not the same as working. A CLI that holds its process open but streams nothing for ten minutes is reaped and the attempt ends `stalled`, so a wedged agent is retried within minutes instead of sitting out the run's multi-hour backstop. `AGENT_RUNNER_STALL_SECONDS` changes the window; `0` switches the watchdog off.
- **Credential normalization**: token whitespace from copy-paste is stripped before it reaches the CLI, preventing silent auth failures from line-break-wrapped pastes.
- **Credentials stay on the worker**: the state mirror carries session transcripts and checkpoints only. Credential files are refused by name on upload and on download, so a shared bucket can neither collect a login nor plant one.



## Docker base image

The repo's `[Dockerfile](Dockerfile)` builds a base image for containerized workers: pinned CLI versions, Chrome for the browser resource, and this package with the Temporal extra. The image records exactly which versions went into it, in its labels and in `/etc/agent-runner-provenance.json`. Extend it and add only your own code.

## Testing

```bash
# free: a fake CLI replays scripted output through the real adapters
python -m unittest discover tests

# the live tier: real CLIs, real tokens, run on purpose
RUN_LIVE=1 pytest tests/live
```

The test suite is token-free: a fake-CLI rig stands in for the real CLIs, so spawn/stream/classify/repair run end to end with zero spend. The live tier proves what fakes cannot: that resume really recalls context, that repair really lands in the open session, and that auth failures and rate limits end with the right outcome word. CI runs the suite with and without Temporal installed and fails the build if a Temporal import leaks into core.

## How this compares

If you searched for a way to run Claude Code programmatically, or to use your Claude subscription instead of an API key for automation, you probably found three kinds of projects. Each solves a different problem.

- **Parallel coding tools** (parallel-code, vibe-kanban, amux) run several coding agents in git worktrees while you review the diffs. Great interactive tools, built for a developer at a screen, not for a headless system.
- **Official SDKs** (claude-agent-sdk, codex-sdk) give you programmatic access to one CLI each. They stop there: no shared outcome vocabulary across providers, no retry mapping, no session resume across machines.
- **Agent frameworks** (LangGraph, CrewAI) orchestrate API calls, billed per token. They own the agent loop and know nothing about CLI subscriptions.

agent-runner sits in the gap between them: AI agent orchestration where the workers are headless coding agents, the outcomes are typed for a workflow engine, and the bill is the flat subscription you already pay. The parallel runners are for your IDE. This is for your infrastructure.

## License

MIT

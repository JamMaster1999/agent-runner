# agent-runner

Run [Claude Code](https://github.com/anthropics/claude-code) and [Codex](https://github.com/openai/codex) CLI agents from Python — spawn the CLI, stream its output live, and end every attempt with exactly one truthful outcome.

Agent CLIs are easy to start and hard to trust: they retry dead credentials for twenty minutes, exit 0 on failed turns, and bury the real error in a JSON stream. agent-runner wraps one CLI attempt in a loop that always reaps the child process, classifies how it actually ended, and hands back the session handle so a retry can resume the conversation instead of paying for it twice.

## Installation

```bash
pip install git+https://github.com/JamMaster1999/agent-runner.git

# with the Temporal activity wrapper:
pip install 'agent-runner[temporal] @ git+https://github.com/JamMaster1999/agent-runner.git'
```

Requires Python 3.13+ and at least one agent CLI installed and authenticated:

```bash
npm install -g @anthropic-ai/claude-code   # then: claude login
npm install -g @openai/codex               # then: codex login
```

## Quick start

```python
import os
from pathlib import Path

from agent_runner.attempt import run_attempt
from agent_runner.harness.base import AgentDef
from agent_runner.runtime import RunSpec

# Attempt directories and agent discovery files are created under this root.
os.environ["AGENT_RUNNER_PROJECT_ROOT"] = str(Path.cwd())

workdir = Path("attempt-1")
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
print(report.session_ref)       # the handle a later attempt can resume
print(report.usage.tok_output)  # token usage, aggregated from the stream
```

`{{RUNNER_OUTPUT_PATH}}` is substituted with the attempt's working directory at spawn time; the CLI only ever sees a concrete local path. Swap `harness="codex"` (and drop the claude-specific `model` config) to run the same attempt on Codex.

## Outcomes

Every attempt ends with exactly one word on `report.outcome`:

| Outcome | Meaning |
|---|---|
| `valid` | the deliverable exists and passed your validator |
| `invalid_schema` | it ran, but the output failed validation (after any repair rounds) |
| `rate_limited` | the provider said slow down — includes subscription usage caps |
| `auth` | the credential is dead; fail fast instead of burning retries |
| `timeout` | the attempt outlived its budget and the CLI child was reaped |
| `spawn_failure` | the CLI never started (missing binary, bad flags) |
| `infra` | it failed, and the CLI's own error text proves nothing more specific |

Two rules keep the classification honest:

- **Output validity beats exit code.** A CLI crash after the deliverable was written still classifies `valid`; a zero exit with a failed final turn classifies as the failure it hid.
- **Evidence comes only from CLI-owned error text** — typed stream error events or stderr — never from the agent transcript, where research content can contain words like "403" incidentally. A stream event proving the attempt can no longer succeed (an auth-dead retry loop) aborts it immediately.

```python
from agent_runner import outcomes

if report.outcome == outcomes.VALID:
    publish(report.data)
elif report.outcome == outcomes.RATE_LIMITED:
    sleep_long_and_retry()          # costs nothing, the work is intact
elif report.outcome == outcomes.AUTH:
    alert_operator(report.error)    # retrying a dead login burns time, not tokens
```

## Validating output

Validation is yours: pass a closure returning a `Verdict`. On a failed verdict with a `repair_message`, the message is sent into the attempt's still-open session — on CLIs that support in-session follow-ups (Codex) — so fixing a malformed output costs seconds, not a fresh run.

```python
import json
from agent_runner.runtime import Verdict

def validate(workdir):
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
    validate=validate,
)
report.repair_rounds_used  # how many repair messages it took
```

## Resuming sessions

Every attempt surfaces the CLI's session handle as soon as the stream reveals it. Pass it back and the CLI reopens that conversation — with all of its context — instead of starting over.

```python
first = run_attempt(spec, "Research the topic and remember your findings.", workdir_1, agent=agent)

second = run_attempt(
    spec,
    "Now write the findings from this conversation to {{RUNNER_OUTPUT_PATH}}/findings.md.",
    workdir_2,
    agent=agent,
    session_ref=first.session_ref,   # resume, don't restart
)
second.resumed  # True
```

## Live telemetry

`on_event` streams progress, tool calls, token usage, and captured provider hook events while the CLI runs. `on_session` fires the moment the session handle appears, so you can persist the resume handle before the attempt ends — that is what makes crash-safe retries possible.

```python
run_attempt(
    spec, task, workdir, agent=agent,
    on_event=lambda e: print(e.kind, e.message),
    on_session=lambda ref: store_resume_handle(ref),
)
```

## Temporal integration

`agent_runner.temporal` (the `[temporal]` extra) wraps `run_attempt` for use inside a [Temporal](https://temporal.io) activity:

- a heartbeat pump runs while the CLI does — liveness is the heartbeat, not a wall clock
- the session handle rides heartbeat details, so an activity retry on any worker resumes the same CLI session
- outcomes map to typed retry errors: `rate_limited` backs off long, `infra` retries elsewhere, `auth` is non-retryable
- a resume budget caps how often one session is resumed before falling back to a fresh one

```python
from temporalio import activity
from agent_runner.temporal import run_agent_attempt

@activity.defn
async def research(packet: dict) -> dict:
    report = await run_agent_attempt(
        spec_for(packet), task_for(packet), workdir_for(packet),
        agent=agent_for(packet), validate=validate_for(packet),
    )
    return report.data  # non-valid outcomes raise typed ApplicationErrors
```

## Browser access

`agent_runner.resources` provisions what a spec declares. `cdp_browser` spawns Chrome and hands its DevTools endpoint into the task as a template value; specs that declare nothing carry no browser dependencies.

```python
from agent_runner.resources import cdp_browser

spec = RunSpec(key="scrape", harness="codex", agent_config={},
               resource_specs=({"kind": "cdp_browser"},))
run_attempt(spec, "Connect to the browser at {{RESOURCE:cdp_browser.endpoint}} ...",
            workdir, resources={"cdp_browser": cdp_browser.provider()})
```

## Harness adapters

Every provider difference — command shapes, stream dialects, failure markers, credential files, resume mechanics — lives in one adapter per CLI under [`src/agent_runner/harness/`](src/agent_runner/harness/). The core never branches on a provider name; adding a CLI means adding an adapter. `RUNNER_CLAUDE_CLI` / `RUNNER_CODEX_CLI` override binary discovery, which is also how the test rig substitutes a fake CLI.

## Worker base image

The repo's [`Dockerfile`](Dockerfile) builds a worker base image for containerized deployments: pinned CLI versions with provenance (OCI labels plus `/etc/agent-runner-provenance.json`), Chrome for the browser resource, and the package with the temporal extra. Extend it and add only your own code.

## Testing

```bash
# token-free: a fake CLI replays scripted streams through the real adapters
python -m unittest discover tests

# the live tier: real CLIs, real tokens, run on purpose
RUN_LIVE=1 pytest tests/live
```

The live tier proves what fakes cannot: that resume really recalls context, that repair really lands in the open session, and that the failure surfaces (auth, rate limit, usage cap) classify as the real binaries render them — rate limits are replicated by pointing the real CLI at a local stub endpoint, so nothing waits on a real cap. CI runs the suite with and without Temporal installed and fails the build on any Temporal import leaking into the core.

## License

MIT

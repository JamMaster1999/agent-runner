# agent-runner

**Job:** run one agent CLI attempt — Claude Code or Codex — and tell the truth about how it ended. A Python library you call from your own code: it spawns the CLI, streams the live output, and ends every attempt with exactly one outcome word. Prompts, validation, retries, and storage stay on your side of the line.

## The shape

```python
from agent_runner.attempt import run_attempt
from agent_runner.harness.base import AgentDef
from agent_runner.runtime import RunSpec, Verdict

report = run_attempt(
    RunSpec(key="survey", harness="claude"),
    "Write the answer as JSON to {{RUNNER_OUTPUT_PATH}}/answer.json, then stop.",
    workdir,
    agent=AgentDef(name="surveyor", description="answers one question", config={"model": "haiku"}, body="You are a careful surveyor...\n"),
    validate=lambda d: Verdict(valid=(d / "answer.json").is_file(), message="answer.json missing"),
)

report.outcome      # one word from the table below
report.session_ref  # the handle a later attempt resumes
report.usage        # tokens and cost, aggregated from the stream
```

The task message is rendered before it arrives; the only substitution performed here is the closed `{{RUNNER_*}}` set bound at attempt start. Validation crosses the boundary as a closure returning a `Verdict` — the library never parses or judges output content itself.

## Outcomes

Every attempt ends with exactly one word. Exit codes are not trusted on their own: a zero exit with a failed final turn classifies as the failure, and a nonzero exit after a valid deliverable classifies `valid` — output validity beats exit code.

| outcome | meaning |
|---|---|
| `valid` | the deliverable exists and passed your validator |
| `invalid_schema` | it ran, but the output failed validation (after any repair rounds) |
| `rate_limited` | the provider said slow down — includes subscription usage caps |
| `auth` | the credential is dead; retrying would burn time, fail fast instead |
| `timeout` | the attempt outlived its budget and the CLI child was reaped |
| `spawn_failure` | the CLI never started (missing binary, bad flags) |
| `infra` | it failed and the CLI's own error text proves nothing more specific |

Failure evidence comes only from CLI-owned error text — typed stream error events or stderr — never from the agent transcript, whose research content can contain words like "403" incidentally. A stream event proving the attempt can no longer succeed (an auth-dead retry loop, say) aborts it immediately instead of waiting out the CLI's own backoff.

## What rides the attempt

- **stream** — `on_event` delivers live telemetry: progress, tool calls, token usage and cost, captured provider hook events
- **repair** — on a failed validation, your `Verdict.repair_message` is sent into the still-open session before the attempt gives up; fixing costs seconds, not a re-run
- **resume** — pass the previous attempt's `session_ref` and the CLI reopens that conversation instead of starting over
- **workdirs** — attempt workspaces and term-stamped checkpoint folders; a stale checkpoint from another term is discarded loudly, never resumed
- **isolation** — the CLI child gets a filtered environment: a safe baseline plus exactly what the spec declared (`required_env`) and what its adapter needs
- **auth** — credential files seeded once from the environment onto a durable volume; tokens the CLI refreshes persist there, and every token is whitespace-normalized on read
- **hygiene** — the CLI child is always reaped, on every exit path

Provider differences — command shapes, stream dialects, failure markers, credential files — live in one adapter per CLI under `agent_runner/harness/`. The core never spells a provider's name.

## Optional modules

- `agent_runner.temporal` (`pip install 'agent-runner[temporal]'`) — a ready-made [Temporal](https://temporal.io) activity wrapper: a heartbeat pump while the CLI runs, `session_ref` riding heartbeat details so an activity retry resumes the session, outcome-aware retry errors (`rate_limited` backs off long, `auth` is non-retryable), and a resume budget with fresh-session fallback
- `agent_runner.resources` — provisioning for a spec's declared resources: `cdp_browser` spawns Chrome and hands its endpoint in as a template value; declare nothing, carry no browser dependencies

## The base image

`Dockerfile` builds a worker base image: pinned CLI versions with provenance (OCI labels plus `/etc/agent-runner-provenance.json`), Chrome, and the package with the temporal extra. What enters the image is an allowlist of three named COPY paths — a state file invented tomorrow stays out by construction. Extend it and add only your own code.

## Install and test

- `pip install -e .` — the core has zero dependencies; `pip install -e '.[temporal]'` adds the activity wrapper
- `python -m unittest discover tests` — token-free: a fake-CLI rig stands in for the real binaries via the `RUNNER_CLAUDE_CLI` / `RUNNER_CODEX_CLI` overrides, so spawn, stream, classify, and repair run end to end with zero spend
- `RUN_LIVE=1 pytest tests/live` — the live tier: real CLIs, real tokens, run on purpose; proves resume really recalls context, repair really lands in the open session, and the failure surfaces (auth, rate limit, usage cap) classify as the real binaries render them
- CI runs the suite with and without Temporal installed and fails the build on any Temporal import leaking into the core

MIT licensed.

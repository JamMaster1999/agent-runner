# Pipeline Event Catalog

> **Post-cutover note (2026-08-04):** the live tables are the runner
> database's `jobs`/`events` (migrations 002/004) and agent-side emit is
> `python3 -m agent_runner emit`; historical references below to
> `pipeline_events`/`pipeline_jobs`/`core/job_event.py` describe the
> pre-cutover bridge era and are kept for provenance.

How job progress becomes data. Established by the Task 5 probe runs
(2026-07-05, Claude Code 2.1.201 + Codex CLI 0.142.5): two instrumented
probe agents, one per CLI, each spawning a subagent, with every supported
hook wired and both stdout streams captured and replayed through the
production parsers.

## Unified event schema

Every event is one row in the runner database's `events` table (migration
004; the pre-cutover `pipeline_events` name survives only in the history
below), written via `agent-runner emit` and the engine lifecycle helpers,
with this shape:

```json
{
  "at": "2026-07-05T22:01:07Z",
  "run_id": "20260705T...__184_el-camino...__ab12cd34",
  "job_stable_id": "184_el-camino...__phase2__codex",
  "phase": "phase2",
  "backend": "codex",
  "attempt": 1,
  "event": "agent_progress",
  "message": "[codex] mapping departments",
  "progress_current": 3,
  "progress_total": 10
}
```

Migration 031 adds six additive columns (extraction plan §4 step 2; all NULL
on pre-031 rows — readers must fall back):

| Column | Meaning |
|---|---|
| `tok_input` | Typed `usage.input_tokens`, mirroring the number the message renders |
| `tok_cache_write` | Claude `cache_creation_input_tokens`; always NULL for Codex (the message never renders it) |
| `tok_cache_read` | Claude `cache_read_input_tokens` / Codex `cached_input_tokens` |
| `tok_output` | Typed `usage.output_tokens` |
| `cost_usd` | Claude `total_cost_usd` at full precision (message renders `%.4f`); always NULL for Codex (no dollars in the stream) |
| `group_key` | Opaque grouping key = institution stable_id, copied from `pipeline_jobs.group_key` at insert |

The five typed-usage columns are set ONLY on `turn_completed` /
`result_success` / `result_error` events and mirror EXACTLY what the message
text renders — the dashboard's message regexes remain the fallback for old
rows, so typed and regex sums must always agree (`tests/test_usage_parity.py`
offline; `db/verify_typed_usage_parity.sql` against a live run).
`pipeline_jobs` also gains `group_key` plus display-only
`labels {"institution", "agent"}`, backfilled by every `ensure_job` upsert.

`event` kinds by source:

| Source | Kinds |
|---|---|
| Orchestrator lifecycle | `start` (claim), `attempt_started`, `progress`, `finish`, `fail`, `retry_waiting`, `blocked`, `reaped`, `cancelled` |
| Stream tail (both CLIs) | `session_started`, `turn_started`, `turn_completed`, `turn_failed`, `agent_message`, `agent_progress`, `command_started`, `command_completed`, `tool_started`, `tool_completed`, `tool_failed`, `web_search`, `file_change`, `subagent_spawning`, `subagent_started`, `subagent_update`, `stream_error`, `result_success`, `result_error` |
| Hook conversion | `hook_session_start`, `hook_subagent_start`, `hook_subagent_stop`, `hook_stop`, `hook_session_end` |
| Phase 3 agent (direct `job_event.py` calls) | `agent_progress` |

Batched progress: the orchestrator drains each poll tick's stream lines and
appends them in ONE psql round trip (`job_event.py progress --batch-json -`).
Stream/hook-derived updates are advisory (`fatal=False`): a failed DB write
logs a warning and never kills an agent attempt. Lifecycle events
(`start`/`finish`/`fail`) still hard-fail.

## Sources, by reliability

1. **Output-file validation** — the only source of truth for success.
   Everything below is progress telemetry.
2. **Stream tail (guaranteed)** — stdout of the process the orchestrator
   owns, parsed by `core/stream_events.py`:
   - Codex: `codex exec --json` JSONL (`thread.*`, `turn.*`, `item.*` with
     item types `agent_message`, `command_execution`, `collab_tool_call`,
     `mcp_tool_call`, `web_search`, `file_change`, `error`).
   - Claude: `--print --output-format stream-json --verbose` JSONL
     (`system/init`, `assistant`/`user` message blocks, `system/task_*`,
     `result`). Subagent activity is tagged via `parent_tool_use_id`.
   Parsers are pure line-in/events-out and replayable offline against the
   captured `codex.stdout.jsonl` / `claude.stdout.log` of any attempt.
3. **Hooks (advisory)** — jsonl capture + conversion to DB events. Both hook
   scripts attribute via the `UFLO_*` env stamped by `agent_env()` (verified
   to propagate into every hook process on both CLIs, including subagent
   fires) and ignore sessions without `UFLO_RUN_ID`. `current_run.json` is
   retired.
   **Phase 5/6 runs ARE captured here** (since 2026-07-19): both phases run
   as orchestrator `pipeline_jobs` fan-out, so every batch agent launches
   with `agent_env()` attribution and its stream and hook events land in
   `pipeline_events` like any other phase (migrations 024/025 add the
   `phase6_import`/`phase5_import` job phases). The old caveat — workflow
   `agent()` calls had no per-agent env, so their hooks lacked `UFLO_RUN_ID`
   and were dropped — applied only to the retired
   `core/build_phase_workflow.py` mechanism.
4. **Subagent self-reporting (Codex phase 3)** — the only signal from inside
   a Codex subagent. Verified 2026-07-05 (probe run 3): a child's own tool
   calls fire NO hooks and emit NO parent-stream items under `exec` v1, and
   a child's printed `PROGRESS:` lines never reach the parent stream. Only
   the spawn/wait boundaries and the child's final message are externally
   visible. The phase 3 registrar agent therefore writes its own progress
   directly to the DB via the `job_event.py` command embedded in its prompt
   — that contract line is load-bearing. The reviewer subagent deliberately
   stays boundary-visible only (spawn/wait + final message); give it the
   same `job_event.py` packet if mid-review visibility is ever needed.

## PROGRESS convention

All agents print `PROGRESS: <n>/<m> <short message>` at each major stage.
The stream parsers lift these lines into `agent_progress` events carrying
`progress_current`/`progress_total` — from Codex `agent_message` items,
Claude assistant text, and even tool output (e.g. an agent `echo`ing
progress inside a shell step). Phases 1/2/4 get the convention from the
orchestrator prompt's "Progress contract"; Phase 3 additionally reports
directly via the `job_event.py` command included in its prompt packet.

## Hook support matrix (verified 2026-07-05)

| Hook event | Claude 2.1.201 headless | Codex 0.142.5 `exec` |
|---|---|---|
| SessionStart | fires | fires |
| UserPromptSubmit | fires | fires |
| PreToolUse / PostToolUse | fires (all tools; PostToolUse carries `tool_response`, `duration_ms`) | fires (all tools incl. collab `spawn_agent`/wait) |
| SubagentStart / SubagentStop | fires (`agent_id`, `agent_type`) | **does NOT fire** under stable multi_agent (v1) |
| Stop | fires | fires (`last_assistant_message`) |
| SessionEnd | fires (`reason`) | not supported |
| Notification / PreCompact | supported, none observed | PreCompact supported, none observed |

Codex subagent lifecycle under `exec` therefore comes from two working
signals instead of Subagent hooks:

- `PostToolUse` on `spawn_agent` (tool_input carries `agent_type`, e.g.
  `prod-phase3-registrar`) and on the wait tool — wired in
  `.codex/config.toml`, converted to `hook_subagent_start`/`_stop`.
- `collab_tool_call` stream items — spawn (receiver thread ids) and wait
  (per-child status **and final message**).

One hook is policy, not telemetry: the Codex `PreToolUse` matcher
`^(Read|mcp__filesystem__read_file)$` runs `.codex/hooks/block_direct_read.py`,
which blocks those **direct Read tools only**. Shell reads (`cat`, `head`, a
python one-liner) are not intercepted — the rest of the no-local-reads
contract rides on the prompt, so treat the hook as a backstop rather than
enforcement. (On Modal the enforcement becomes structural: research agents
simply will not mount the results volume.)

`features.multi_agent_v2=true` does make SubagentStart/Stop fire under
`exec`, but it is flagged "under development" and the probe showed it
breaking named-agent spawning (`agent_type: "default"`, encrypted spawn
message, thinner stream items) — do not enable it for production. The
config keeps catch-all SubagentStart/Stop matchers as best-effort in case a
future CLI restores them on the stable path.

## Failure matrix

| Scenario | Outcome |
|---|---|
| Stream/hook events flow, output valid, exit 0 | Normal: full event trail + `finish`. |
| Events flow, output valid, exit ≠ 0 | Codex path accepts the attempt on output validity (progress note records the exit code). |
| Events flow, output missing/invalid | Attempt fails via `classify_failure` regardless of how healthy the telemetry looked. |
| Stream tail broken (unparseable stdout) | Parsers skip bad lines silently; hooks + lifecycle events still land; validation unaffected. |
| DB unreachable during stream/hook progress | Warning on orchestrator stderr, attempt continues (advisory writes). |
| DB unreachable during start/finish/fail | Hard `PipelineError` — lifecycle bookkeeping must not silently diverge. |
| Hook fires from an interactive (non-run) session | Ignored: no `UFLO_RUN_ID` in env. |

## Raw capture locations

- `.local/runs/<run_id>/<job>/attempt-N/codex.stdout.jsonl` / `claude.stdout.log` — verbatim streams (full fidelity; DB events are the curated view).
- `.local/codex_hooks/events.jsonl`, `.local/claude_hooks/events.jsonl` — hook fires with UFLO attribution.
- `pipeline_events` table — the ordered, unified event trail the frontend reads (id-cursor; `LISTEN pipeline_events` for push). Rows older than 30 days pruned at orchestrator startup; hook jsonl logs rotate per run into `.local/*_hooks/archive/`.

"""Codex harness adapter: every codex-CLI-specific behavior in one module
(design doc §2). Binary fallbacks, the `-c dotted.key=value` agent-config
flattening, exec/resume/followup command shapes, thread extraction, the
exec-v1 subagent hook quirk, the error-report dialect, terminal markers, and
doctor/health commands — moved verbatim from the pre-adapter modules
(phase-2 step 5).

The legacy filesystem resume matcher (packet-membership match over
.local/runs) was DELETED at extraction step 4 (design §7.5), not ported:
every resumable thread is DB-tracked in the attempts store, and resume
rights belong solely to claim_resumable_attempt — pinned by
tests/test_resume_claim_sql.py. This module imports no client modules.

Step-5 retype: builders take the generic ``RunnerJob``; the per-task timeout
and never-resume branches left with the adapter policy slots (both are
submit data, folded by the client's submit policy). Agent configuration is
submit DATA too (``job.agent_config``): the adapter never gates on
``task_type`` and never reads the client's rendered discovery files at
spawn time — the 2026-08-04 phase-name-enumeration bug class is
structurally gone."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, ClassVar

from agent_runner import util
from agent_runner.harness.base import AgentDef, Capabilities, HarnessAdapter, SpawnSpec
from agent_runner.runtime import RunnerError, RunnerJob
from agent_runner.harness.codex_stream import CodexStreamParser


# Standard macOS app-bundle install locations, tried after PATH; the
# RUNNER_CODEX_CLI environment variable overrides both.
CODEX_CLI_FALLBACKS = (
    Path("/Applications/Codex.app/Contents/Resources/codex"),
    Path("/Applications/ChatGPT.app/Contents/Resources/codex"),
)


def codex_command() -> str | None:
    override = os.environ.get("RUNNER_CODEX_CLI")
    if override and Path(override).exists():
        return override
    if found := shutil.which("codex"):
        return found
    for fallback in CODEX_CLI_FALLBACKS:
        if fallback.exists():
            return str(fallback)
    return None


def toml_cli_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(toml_cli_value(item) for item in value) + "]"
    raise RunnerError(
        f"Unsupported Codex agent config value: {value!r}",
        code="invalid_codex_agent_config",
        retryable=False,
        alert=True,
    )


def flattened_codex_config(prefix: str, value: Any) -> list[tuple[str, Any]]:
    if isinstance(value, dict):
        items: list[tuple[str, Any]] = []
        for key, nested in value.items():
            items.extend(flattened_codex_config(f"{prefix}.{key}", nested))
        return items
    return [(prefix, value)]


def codex_agent_config_args(job: RunnerJob) -> list[str]:
    """The `-c dotted.key=value` overrides delivering the job's agent
    configuration to `codex exec`.

    ``job.agent_config`` is submit DATA (the whole table is delivered
    verbatim — the client prunes its own metadata keys before submitting).
    None with a named agent is a LOUD error, never a silent unconfigured
    spawn: the old task_type gate dropped every override — model, tool
    restrictions, developer_instructions — for any task name outside one
    client's phase list, with no error (2026-08-04 incident). An explicit
    {} means a deliberately unconfigured session (a parent session driving
    its own subagents)."""
    if job.agent_config is None:
        if job.agent_ref:
            raise RunnerError(
                f"{job.key}: codex job names agent {job.agent_ref!r} but the "
                "submit carried no agent_config. Submit the agent's config "
                "table (SubmitRequest.agent_config), or an explicit {} for a "
                "deliberately unconfigured session.",
                code="missing_codex_agent_config",
                retryable=False,
                alert=True,
            )
        return []

    args: list[str] = []
    for key, value in job.agent_config.items():
        for dotted_key, dotted_value in flattened_codex_config(key, value):
            args.extend(["-c", f"{dotted_key}={toml_cli_value(dotted_value)}"])
    return args


def toml_file_value(value: Any) -> str:
    """One config value in the rendered agent-FILE dialect (moved verbatim
    from the client's sync_agents at extraction step 7). Distinct from
    ``toml_cli_value``, the `-c` override dialect: this one escapes strings
    by hand and supports inline tables."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(value, list):
        return "[" + ", ".join(toml_file_value(item) for item in value) + "]"
    if isinstance(value, dict):
        inner = ", ".join(f"{k} = {toml_file_value(v)}" for k, v in value.items())
        return "{ " + inner + " }"
    raise RunnerError(
        f"codex agent config: unsupported value: {value!r}",
        code="agent_render",
        retryable=False,
    )


def codex_exec_command(final_message_path: Path, job: RunnerJob) -> list[str]:
    command = codex_command()
    if not command:
        raise RunnerError(
            "Required command not found: codex",
            code="missing_command",
            retryable=False,
            alert=True,
        )
    return [
        command,
        "exec",
        "--json",
        "--cd",
        str(util.project_root()),
        "--dangerously-bypass-approvals-and-sandbox",
        "--dangerously-bypass-hook-trust",
        "--output-last-message",
        str(final_message_path),
        *codex_agent_config_args(job),
        "-",
    ]


def codex_exec_resume_command(
    final_message_path: Path, job: RunnerJob, thread_id: str
) -> list[str]:
    """`codex exec resume` variant of codex_exec_command. `resume` has no
    --cd; the working directory comes from the process cwd (PROJECT_ROOT)."""
    command = codex_command()
    if not command:
        raise RunnerError(
            "Required command not found: codex",
            code="missing_command",
            retryable=False,
            alert=True,
        )
    return [
        command,
        "exec",
        "resume",
        "--json",
        "--dangerously-bypass-approvals-and-sandbox",
        "--dangerously-bypass-hook-trust",
        "--output-last-message",
        str(final_message_path),
        *codex_agent_config_args(job),
        thread_id,
        "-",
    ]


def codex_thread_id(stdout_path: Path) -> str | None:
    """The session id from the attempt's JSON event stream, for `codex exec
    resume` follow-ups."""
    try:
        with stdout_path.open() as fh:
            for line in fh:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("type") == "thread.started":
                    return payload.get("thread_id") or None
    except OSError:
        return None
    return None


class CodexAdapter(HarnessAdapter):
    """The Codex CLI (`codex exec --json`, prompt on stdin)."""

    name: ClassVar[str] = "codex"
    display_name: ClassVar[str] = "Codex"
    start_label: ClassVar[str] = "Codex CLI"
    session_noun: ClassVar[str] = "thread"
    # resume/followup: `codex exec resume <thread>` (session resume and the
    # phase 3.5/6 repair path). hooks: PostToolUse etc. fire under `codex
    # exec`; dedicated Subagent hooks do not (see normalize_hook_event).
    # doctor: `codex doctor --json` is structured. final_message_artifact:
    # --output-last-message.
    capabilities: ClassVar[Capabilities] = Capabilities(
        resume=True,
        followup=True,
        hooks=True,
        doctor=True,
        final_message_artifact=True,
    )
    # Identical to the claude_code table on purpose: the pre-adapter code
    # matched one shared list for both CLIs, and splitting it per dialect
    # would be a behavior change, not a move. Prune per dialect deliberately.
    terminal_markers: ClassVar[tuple[tuple[str, tuple[str, ...]], ...]] = (
        ("health_budget_too_low", ("error_max_budget_usd", "reached maximum budget")),
        (
            "auth",
            (
                "authentication_error",
                "oauth token has expired",
                "token expired",
                "please run /login",
                "not logged in",
                "login required",
                "invalid api key",
                "api key not found",
            ),
        ),
        (
            "billing_or_credits",
            (
                "billing_error",
                "credit balance is too low",
                "insufficient_quota",
                "payment required",
                "out of credit",
            ),
        ),
        (
            "invalid_invocation",
            (
                "unknown option",
                "unknown argument",
                "unexpected argument",
                "unrecognized argument",
                "invalid value for",
                "no such subcommand",
            ),
        ),
    )

    def resolve_binary(self) -> str | None:
        return codex_command()

    def health_checks(self, args: argparse.Namespace) -> None:
        """`codex doctor --json` plus a capped live exec probe."""
        self.run_doctor(args.health_timeout_seconds)
        health_command = [
            codex_command() or "codex",
            "exec",
            "--json",
            "--ephemeral",
            "--sandbox",
            "read-only",
            "--skip-git-repo-check",
        ]
        if args.codex_health_model:
            health_command += ["--model", args.codex_health_model]
        health_command.append("Healthcheck only. Reply with OK.")
        self.run_health_command(health_command, args.health_timeout_seconds)

    def run_doctor(self, timeout: int) -> None:
        command = codex_command()
        if not command:
            raise RunnerError(
                "Required command not found: codex",
                code="missing_command",
                retryable=False,
                alert=True,
            )
        try:
            result = subprocess.run(
                [command, "doctor", "--json"],
                cwd=util.project_root(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            output = ((exc.stdout or "") if isinstance(exc.stdout, str) else "") + "\n" + (
                (exc.stderr or "") if isinstance(exc.stderr, str) else ""
            )
            raise RunnerError(
                "codex doctor timed out",
                code="health_timeout",
                retryable=True,
                details=output,
            ) from exc
        output = (result.stdout or "") + "\n" + (result.stderr or "")
        try:
            data = json.loads(result.stdout or "{}")
        except json.JSONDecodeError:
            if result.returncode != 0:
                failure = self.classify_failure(output)
                failure.details = output
                raise failure
            return

        checks = data.get("checks", data)
        if isinstance(checks, dict):
            items = [item for item in checks.values() if isinstance(item, dict)]
        elif isinstance(checks, list):
            items = [item for item in checks if isinstance(item, dict)]
        else:
            items = []

        # terminal.env: codex doctor flags a non-interactive terminal
        # environment, which is exactly what a headless engine spawn is —
        # never evidence of a broken CLI.
        ignored_failures = {"terminal.env"}
        hard_failures = [
            {
                "id": item.get("id"),
                "category": item.get("category"),
                "status": item.get("status"),
                "summary": item.get("summary"),
                "issues": item.get("issues"),
            }
            for item in items
            if str(item.get("status") or "").lower() in {"fail", "error"}
            and item.get("id") not in ignored_failures
        ]
        if hard_failures:
            raise RunnerError(
                "codex doctor reported failures",
                code="codex_doctor",
                retryable=False,
                alert=True,
                details=json.dumps(hard_failures, indent=2, sort_keys=True),
            )

    def materialize_agent(self, agent: AgentDef, header: str) -> str:
        """The `.codex/agents/<name>.toml` dialect: `# header` first line,
        name/description, the config keys in dict order, then the body as a
        `developer_instructions` TOML literal string (moved verbatim from
        the client's sync_agents at extraction step 7)."""
        if "'''" in agent.body:
            raise RunnerError(
                f"codex agent {agent.name}: body contains ''' which breaks "
                "the developer_instructions TOML literal string",
                code="agent_render",
                retryable=False,
            )
        lines = [f"# {header}"]
        lines.append(f'name = "{agent.name}"')
        lines.append(f"description = {toml_file_value(agent.description)}")
        for key, value in agent.config.items():
            lines.append(f"{key} = {toml_file_value(value)}")
        lines.append("developer_instructions = '''")
        return "\n".join(lines) + "\n" + agent.body + "'''\n"

    def build_spawn(self, job: RunnerJob, directory: Path) -> SpawnSpec:
        return SpawnSpec(
            command=codex_exec_command(directory / "codex.final.txt", job),
            stdout_path=directory / "codex.stdout.jsonl",
            stderr_path=directory / "codex.stderr.log",
        )

    def build_resume(self, job: RunnerJob, directory: Path, session_ref: str) -> SpawnSpec:
        return SpawnSpec(
            command=codex_exec_resume_command(directory / "codex.final.txt", job, session_ref),
            stdout_path=directory / "codex.stdout.jsonl",
            stderr_path=directory / "codex.stderr.log",
        )

    def build_followup(
        self, job: RunnerJob, directory: Path, session_ref: str
    ) -> SpawnSpec | None:
        command = codex_command()
        if not command:
            return None
        # `resume` has no --cd; the working directory comes from the process cwd.
        return SpawnSpec(
            command=[
                command,
                "exec",
                "resume",
                "--json",
                "--dangerously-bypass-approvals-and-sandbox",
                "--dangerously-bypass-hook-trust",
                "--output-last-message",
                str(directory / "codex.repair.final.txt"),
                *codex_agent_config_args(job),
                session_ref,
                "-",
            ],
            stdout_path=directory / "codex.repair.stdout.jsonl",
            stderr_path=directory / "codex.repair.stderr.log",
        )

    def env_passthrough(self) -> tuple[str, ...]:
        # CLI auth/home names the filtered agent env must inherit.
        return ("CODEX_HOME", "OPENAI_API_KEY", "OPENAI_BASE_URL", "RUNNER_CODEX_CLI")

    def session_ref_from_log(self, stdout_path: Path) -> str | None:
        return codex_thread_id(stdout_path)

    def stream_parser(self) -> CodexStreamParser:
        return CodexStreamParser()

    def hook_event_log(self) -> Path:
        return util.state_dir() / "codex_hooks" / "events.jsonl"

    def normalize_hook_event(
        self, event: dict[str, Any], agent_name: str
    ) -> tuple[str, str] | None:
        # Dedicated SubagentStart/Stop hooks do not fire under `codex exec`
        # with the stable multi_agent (v1) feature — subagent lifecycle
        # arrives as PostToolUse fires on the spawn/wait collab tools instead.
        # Both are handled so a future CLI that restores Subagent hooks needs
        # no change.
        hook_name = event.get("hook_event_name")
        tool_name = event.get("tool_name") or ""
        agent_label = event.get("agent_type") or agent_name
        if hook_name == "SubagentStart":
            return "hook_subagent_start", f"Codex subagent started: {agent_label}"
        if hook_name == "SubagentStop":
            return "hook_subagent_stop", f"Codex subagent stopped: {agent_label}"
        if hook_name == "PostToolUse" and tool_name == "spawn_agent":
            return (
                "hook_subagent_start",
                f"Codex spawned subagent: {event.get('spawned_agent_type') or agent_label}",
            )
        if hook_name == "PostToolUse" and tool_name in {"wait_agent", "multi_agent_v1wait_agent"}:
            return "hook_subagent_stop", "Codex subagent wait completed"
        return None

    def stream_error_line(self, payload: dict[str, Any]) -> str | None:
        """codex --json emits `turn.failed` / `error` events."""
        kind = payload.get("type") or ""
        if kind == "turn.failed":
            message = str((payload.get("error") or {}).get("message") or "")
            return f"codex turn.failed: {message}".strip()
        if kind == "error":
            return f"codex error: {payload.get('message') or ''}".strip()
        return None

    def orphan_patterns(self) -> list[str]:
        return ["codex exec"]

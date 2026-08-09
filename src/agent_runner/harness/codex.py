"""Codex harness adapter: every codex-CLI-specific behavior in one module.
Binary fallbacks, the `-c dotted.key=value` agent-config flattening,
exec/resume/followup command shapes, thread extraction, the exec-v1
subagent hook quirk, the error-report dialect, terminal markers, and the
volume-backed credential model — the runner core never spells this CLI's
name.

Agent configuration is DATA (``spec.agent_config``): the adapter never
gates on ``task_type`` and never reads the caller's rendered discovery
files at spawn time. ``prepare_agent`` folds an ``AgentDef`` into the same
data shape, so spawn takes an agent definition and a task message with
nothing read from disk."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any, ClassVar, Mapping

from agent_runner import outcomes, util
from agent_runner.auth import seed_credential_file
from agent_runner.harness.base import AgentDef, Capabilities, HarnessAdapter, SpawnSpec
from agent_runner.runtime import RunnerError, RunSpec
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


def codex_agent_config_args(spec: RunSpec) -> list[str]:
    """The `-c dotted.key=value` overrides delivering the spec's agent
    configuration to `codex exec`.

    ``spec.agent_config`` is DATA (the whole table is delivered verbatim —
    the caller prunes its own metadata keys before handing it over). None
    with a named agent is a LOUD error, never a silent unconfigured spawn
    (the 2026-08-04 incident: a task-name gate silently dropped every
    override). An explicit {} means a deliberately unconfigured session (a
    parent session driving its own subagents)."""
    if spec.agent_config is None:
        if spec.agent_ref:
            raise RunnerError(
                f"{spec.key}: codex run names agent {spec.agent_ref!r} but "
                "carries no agent_config. Supply the agent's config table "
                "(RunSpec.agent_config or the agent= definition), or an "
                "explicit {} for a deliberately unconfigured session.",
                code="missing_codex_agent_config",
                retryable=False,
                alert=True,
            )
        return []

    args: list[str] = []
    for key, value in spec.agent_config.items():
        for dotted_key, dotted_value in flattened_codex_config(key, value):
            args.extend(["-c", f"{dotted_key}={toml_cli_value(dotted_value)}"])
    return args


def toml_file_value(value: Any) -> str:
    """One config value in the rendered agent-FILE dialect. Distinct from
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


def codex_exec_command(final_message_path: Path, spec: RunSpec) -> list[str]:
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
        *codex_agent_config_args(spec),
        "-",
    ]


def codex_exec_resume_command(
    final_message_path: Path, spec: RunSpec, thread_id: str
) -> list[str]:
    """`codex exec resume` variant of codex_exec_command. `resume` has no
    --cd; the working directory comes from the process cwd (project root)."""
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
        *codex_agent_config_args(spec),
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
    # repair path). hooks: PostToolUse etc. fire under `codex exec`;
    # dedicated Subagent hooks do not (see normalize_hook_event).
    # final_message_artifact: --output-last-message.
    capabilities: ClassVar[Capabilities] = Capabilities(
        resume=True,
        followup=True,
        hooks=True,
        doctor=True,
        final_message_artifact=True,
    )
    # Marker codes ARE outcome words. Identical to the claude_code table on
    # purpose: the pre-adapter code matched one shared list for both CLIs;
    # prune per dialect deliberately, never as a side effect. Order matters —
    # first match wins, and a subscription CLI's "usage limit" text must
    # classify rate_limited before the auth/billing sweep sees it.
    terminal_markers: ClassVar[tuple[tuple[str, tuple[str, ...]], ...]] = (
        (
            outcomes.RATE_LIMITED,
            (
                "rate limit",
                "rate_limit",
                "too many requests",
                "overloaded_error",
                "usage limit",
            ),
        ),
        (
            outcomes.AUTH,
            (
                "authentication_error",
                "oauth token has expired",
                "token expired",
                "please run /login",
                "not logged in",
                "login required",
                "invalid api key",
                "api key not found",
                "billing_error",
                "credit balance is too low",
                "insufficient_quota",
                "payment required",
                "out of credit",
            ),
        ),
        (
            outcomes.SPAWN_FAILURE,
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

    def prepare_home(self, volume_root: Path, env: Mapping[str, str]) -> dict[str, str]:
        """CODEX_HOME on the volume; auth.json seeded once from the
        CODEX_AUTH_JSON environment value (a Modal-style secret), mode 0600.
        Refreshed tokens the CLI writes back land on the volume."""
        home = Path(volume_root) / "codex-home"
        home.mkdir(parents=True, exist_ok=True)
        seed = env.get("CODEX_AUTH_JSON")
        if seed:
            seed_credential_file(home / "auth.json", seed)
        return {"CODEX_HOME": str(home)}

    def materialize_agent(self, agent: AgentDef, header: str) -> str:
        """The `.codex/agents/<name>.toml` dialect: `# header` first line,
        name/description, the config keys in dict order, then the body as a
        `developer_instructions` TOML literal string."""
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

    def prepare_agent(self, agent: AgentDef) -> dict[str, Any] | None:
        """Codex agents spawn from config alone: the definition's table plus
        the body as developer_instructions, all delivered as `-c` overrides —
        nothing is read from (or written to) disk."""
        config = dict(agent.config)
        config.setdefault("developer_instructions", agent.body)
        return config

    def build_spawn(self, spec: RunSpec, directory: Path) -> SpawnSpec:
        return SpawnSpec(
            command=codex_exec_command(directory / "codex.final.txt", spec),
            stdout_path=directory / "codex.stdout.jsonl",
            stderr_path=directory / "codex.stderr.log",
        )

    def build_resume(self, spec: RunSpec, directory: Path, session_ref: str) -> SpawnSpec:
        return SpawnSpec(
            command=codex_exec_resume_command(directory / "codex.final.txt", spec, session_ref),
            stdout_path=directory / "codex.stdout.jsonl",
            stderr_path=directory / "codex.stderr.log",
        )

    def build_followup(
        self, spec: RunSpec, directory: Path, session_ref: str
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
                *codex_agent_config_args(spec),
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

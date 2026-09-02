"""Harness adapter contract: Capabilities, SpawnSpec, AgentDef, and
HarnessAdapter.

The adapter is the load-bearing wall: runner core contains zero
harness-name branches, and every provider difference — binary discovery,
spawn/resume/followup command shapes, stream and hook dialects,
terminal-failure marker data, env quirks, credential-file models — lives in
one adapter module registered under the spec's harness name. The base class
carries the judgment-free shared machinery (marker scan, error-report
shell); adapters supply evidence and dialects only. Policy — what a failure
means for the run — stays with the caller (the Temporal layer ships the
ruled mapping).
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Mapping

from agent_runner import outcomes
from agent_runner.harness.stream import iter_jsonl
from agent_runner.pool import KIND_RATE, KIND_SERVER, KIND_USAGE
from agent_runner.runtime import RunnerError, RunSpec
from agent_runner.util import read_tail

# The three limits a CLI reports as text, each by the words the pinned CLIs
# use for it (Claude Code 2.1.220, Codex 0.146.0 — codex-rs protocol/src/
# error.rs and the CLI's own limit strings). Order matters: Claude's
# "Server is temporarily limiting requests (not your usage limit) · Rate
# limited" names all three and is the server's doing. Typed stream events
# (``stream_fatal``) beat this table wherever a CLI emits one.
LIMIT_MARKERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (KIND_SERVER, ("temporarily limiting requests", "overloaded", "high demand", "at capacity")),
    (KIND_USAGE, ("usage limit", "session limit", "weekly limit", "hit your limit")),
    (KIND_RATE, ("too many requests", "429", "rate limit", "rate_limit")),
)

# Terminal-failure markers shared by every CLI dialect so far: the codes ARE
# outcome words, matched only against CLI-owned error text. Order matters —
# first match wins, and a subscription CLI's "usage limit" text must classify
# rate_limited before the auth/billing sweep sees it. An adapter whose CLI
# genuinely diverges overrides its ``terminal_markers`` class variable.
COMMON_TERMINAL_MARKERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (outcomes.RATE_LIMITED, tuple(marker for _, markers in LIMIT_MARKERS for marker in markers)),
    (
        outcomes.AUTH,
        (
            "authentication_error",
            "authentication_failed",
            "401 unauthorized",
            "missing bearer",
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


def limit_kind(lower: str) -> str:
    """Which limit a rate_limited text names (``LIMIT_MARKERS`` order)."""
    return next(kind for kind, markers in LIMIT_MARKERS if any(marker in lower for marker in markers))


@dataclass(frozen=True)
class Capabilities:
    """Proven degradation flags, not speculation: every False path already
    ran in production. No ``resume`` -> every attempt is fresh; no
    ``followup`` -> validation failure goes straight to retry; no ``hooks``
    -> stream telemetry only."""

    resume: bool = False             # can reopen a session and continue
    followup: bool = False           # can inject a message into an existing session (repair)
    hooks: bool = False              # emits lifecycle hooks the runner can capture
    doctor: bool = False             # structured self-diagnosis command
    final_message_artifact: bool = False


@dataclass(frozen=True)
class SpawnSpec:
    """One CLI invocation: the command plus where its streams land. The
    prompt (or follow-up message) always arrives on stdin."""

    command: list[str]
    stdout_path: Path
    stderr_path: Path


@dataclass(frozen=True)
class AgentDef:
    """One agent definition crossing the adapter boundary as DATA: the
    runner never reads the caller's source tree. ``config`` is the agent's
    per-harness frontmatter table; ``body`` is the verbatim prompt — the
    caller guarantees the trailing newline."""

    name: str
    description: str
    config: dict[str, Any]
    body: str

    def __post_init__(self) -> None:
        # The name becomes a discovery-file path component (prepare_agent
        # writes <root>/.claude/agents/<name>.md): a separator or ".." would
        # escape that directory, so reject it at the boundary.
        if not self.name or any(seq in self.name for seq in ("/", "\\", "..")):
            raise RunnerError(
                f"invalid agent name {self.name!r}: names must be non-empty "
                "and contain no path separators or '..'",
                code="agent_render",
                retryable=False,
                alert=True,
            )


class HarnessAdapter(ABC):
    """One agent CLI's contract with the attempt loop.

    ``name`` is the registry key and equals the spec's harness value. The
    display strings feed operator-facing event messages only; nothing ever
    parses them.
    """

    name: ClassVar[str]
    capabilities: ClassVar[Capabilities]
    display_name: ClassVar[str]      # "<display_name> wrote valid output; ..."
    start_label: ClassVar[str]       # "Started <start_label> attempt 2"
    session_noun: ClassVar[str]      # what this CLI calls a resumable session

    # Failure classification (2026-07-28 policy, restated in the stage-3
    # vocabulary): markers are matched ONLY against CLI-owned error text —
    # typed stream error events (error_report) or CLI stderr — never agent
    # transcript tails, whose web-research content can contain tokens like
    # '403' or 'api key' incidentally. Marker codes ARE outcome words
    # (agent_runner.outcomes); anything unmatched classifies ``infra``.
    terminal_markers: ClassVar[tuple[tuple[str, tuple[str, ...]], ...]] = ()

    # -- credentials & homes (the Modal model, ruling D1) ------------------

    def prepare_home(self, root: Path, env: Mapping[str, str]) -> dict[str, str]:
        """Point this CLI's home at a directory under ``root`` (the
        workspace) and seed its credential file there once from ``env``
        when absent — refreshed tokens the CLI writes back stay there.
        Returns environment overrides (home + auth names). Default: no
        credential-file model."""
        return {}

    def bind_credentials(self) -> dict[str, str]:
        """Env for the attempt's credentials, normalized on read (token
        normalization is here so a corrupted operator paste never reaches
        the CLI)."""
        return {}

    # -- agent materialization ---------------------------------------------

    @abstractmethod
    def materialize_agent(self, agent: AgentDef, header: str) -> str:
        """The complete discovery-file text for ``agent`` in this harness's
        dialect. ``header`` is a caller-supplied comment STRING with no
        comment syntax — the adapter wraps it in its own dialect. Render
        constraints raise RunnerError (code='agent_render', retryable=False)
        naming the harness."""

    def prepare_agent(self, agent: AgentDef) -> dict[str, Any] | None:
        """Make ``agent`` spawnable in this harness's dialect and return the
        effective ``agent_config`` for the spawn (None keeps the spec's).
        File-based dialects write their discovery file here; config-based
        dialects fold the body into the returned table. Default: the
        agent's config table, untouched."""
        return dict(agent.config)

    # -- spawn / resume / followup -----------------------------------------

    @abstractmethod
    def build_spawn(self, spec: RunSpec, directory: Path) -> SpawnSpec:
        """Fresh-attempt invocation."""

    @abstractmethod
    def build_resume(self, spec: RunSpec, directory: Path, session_ref: str) -> SpawnSpec:
        """Reopen ``session_ref`` and continue the same task."""

    def build_followup(
        self, spec: RunSpec, directory: Path, session_ref: str
    ) -> SpawnSpec | None:
        """Message injection into an existing session (the repair path);
        None when unsupported (``capabilities.followup`` is False) or the
        binary is missing — the attempt loop falls back to a plain retry."""
        return None

    def env_overrides(self) -> dict[str, str]:
        """Harness-specific env quirks layered over the shared agent env."""
        return {}

    def env_passthrough(self) -> tuple[str, ...]:
        """Environment names this harness's CLI needs inherited from the
        engine when the filtered agent environment is in effect (auth
        tokens, CLI home overrides). Names only — the attempt loop copies
        the values from its own environment when present."""
        return ()

    # -- session resume ----------------------------------------------------

    @abstractmethod
    def session_ref_from_event(self, payload: dict[str, Any]) -> str | None:
        """The opaque session ref carried by one parsed stream event; None
        when this event does not name one. The attempt loop calls this on
        the lines it is already tailing, so no log rescan is ever needed."""

    def session_ref_from_log(self, stdout_path: Path) -> str | None:
        """The first session ref in a captured stdout stream, for one-shot
        callers (the repair path); None until the CLI has emitted it."""
        for payload in iter_jsonl(stdout_path):
            ref = self.session_ref_from_event(payload)
            if ref:
                return ref
        return None

    def session_present(self, session_ref: str) -> bool:
        """Whether this CLI's home holds the transcript of ``session_ref``,
        so a resume has something to reopen. True when the home is not
        configured (nothing to check); False only when the home is known
        and the transcript is not in it."""
        return True

    # -- telemetry ---------------------------------------------------------

    @abstractmethod
    def stream_parser(self) -> Any:
        """A fresh per-attempt stdout parser (``parse_line(line)`` ->
        StreamEvent list)."""

    def hook_event_log(self) -> Path:
        """Where this harness's hook-capture wiring appends events — one
        shared convention: ``<state_dir>/<name>_hooks/events.jsonl``."""
        from agent_runner.harness.hook_capture import event_log_path

        return event_log_path(self.name)

    @abstractmethod
    def normalize_hook_event(
        self, event: dict[str, Any], agent_name: str
    ) -> tuple[str, str] | None:
        """Convert one raw captured hook event to ``(event_name, message)``;
        None drops it."""

    # -- failure: the adapter supplies EVIDENCE, the caller supplies JUDGMENT

    @abstractmethod
    def stream_error_line(self, payload: dict[str, Any]) -> str | None:
        """One typed stream event rendered as CLI-owned error text; None for
        non-error events (the dialect hook under ``error_report``)."""

    def error_report(self, stdout_path: Path, stderr_path: Path) -> str:
        """The CLI's own error report for a failed attempt, for
        ``classify_failure``.

        Prefers the typed error events in the JSON stream on stdout (via the
        adapter's ``stream_error_line`` dialect hook); falls back to the
        stderr tail, which carries only CLI diagnostics. The stdout
        transcript tail is deliberately never used: it holds web-research
        tool output where auth/billing-looking tokens appear incidentally.
        """
        errors = [
            error
            for payload in iter_jsonl(stdout_path)
            if (error := self.stream_error_line(payload)) is not None
        ]
        if errors:
            return "\n".join(errors[-5:])
        return read_tail(stderr_path, 4000).strip()

    def stream_fatal(self, payload: dict[str, Any]) -> RunnerError | None:
        """CLI-owned evidence from one LIVE stream event that the attempt can
        no longer succeed (an auth-dead retry loop, say): the loop terminates
        the CLI and classifies with the returned error instead of waiting out
        the CLI's own backoff ladder. Typed evidence beats marker text — the
        event's own status code decides the outcome word, so a CLI wording
        change can never misroute it. Default: no such evidence."""
        return None

    def terminal_failure(self, stdout_path: Path) -> str | None:
        """CLI-owned evidence, read after a ZERO exit, that the final turn
        failed anyway (a CLI that exits 0 on failed turns). None: the exit
        code stands. Default: exit codes tell the truth."""
        return None

    RESET_PATTERN = re.compile(
        r"try again at\s+([A-Za-z]{3,9} \d{1,2}(?:st|nd|rd|th)?,? \d{4} \d{1,2}:\d{2}\s?[APap]\.?[Mm])"
    )

    @classmethod
    def reset_time_in(cls, text: str) -> datetime | None:
        """The reset moment a subscription CLI's limit text names ("try
        again at Sep 5th, 2026 6:07 PM"), read in this process's local
        timezone — the CLI rendered it there. None when the text names
        none; the caller's default backoff stands."""
        match = cls.RESET_PATTERN.search(text or "")
        if not match:
            return None
        cleaned = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", match.group(1)).replace(".", "")
        for fmt in ("%b %d, %Y %I:%M %p", "%B %d, %Y %I:%M %p"):
            try:
                return datetime.strptime(cleaned, fmt).astimezone()
            except ValueError:
                continue
        return None

    def classify(self, text: str) -> RunnerError | None:
        """Terminal-failure evidence from this harness's marker data; None
        means no proof and the core default (``infra``) applies. Callers
        must never pass agent transcript tails; use ``error_report`` for
        attempt logs."""
        lower = (text or "").lower()
        for code, markers in self.terminal_markers:
            if any(marker in lower for marker in markers):
                terminal = code in outcomes.TERMINAL
                limited = code == outcomes.RATE_LIMITED
                return RunnerError(
                    f"{self.name} {code} failure reported by the CLI",
                    code=code,
                    retryable=not terminal,
                    alert=terminal,
                    details=text,
                    resets_at=self.reset_time_in(text) if limited else None,
                    kind=limit_kind(lower) if limited else None,
                )
        return None

    def classify_failure(self, text: str) -> RunnerError:
        """``classify`` plus the core default: no proof from the CLI's own
        text means ``infra`` — retryable, says nothing about the job."""
        classified = self.classify(text)
        if classified is not None:
            return classified
        return RunnerError(
            f"{self.name} attempt failed",
            code=outcomes.INFRA,
            retryable=True,
            alert=False,
            details=text,
        )

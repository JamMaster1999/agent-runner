"""The runner's run vocabulary: ``RunSpec``, ``AttemptReport``, ``Verdict``,
and ``RunnerError`` (stage-3 carve-out).

agent-runner is a library called from inside a project's activities. There
is no wire protocol and no job store here any more: the caller hands
``run_attempt`` a ``RunSpec`` plus the already-rendered task message, and
gets back an ``AttemptReport`` whose ``outcome`` is exactly one word from
``agent_runner.outcomes``. Everything project-shaped — prompt rendering,
contracts, receipts, workflows — stays on the caller's side of the line;
validation crosses the boundary only as the ``validate`` closure that
returns a ``Verdict``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class RunnerError(Exception):
    """A runner-module failure crossing toward the caller.

    ``code`` is an outcome word (``agent_runner.outcomes``) when the failure
    is attempt-shaped, or a specific module code (``agent_render``,
    ``missing_command``) when it is not. ``retryable`` False is terminal
    proof; ``alert`` marks an operator-worthy fact — the caller maps it onto
    its own alerting (agent-runner itself never notifies anyone).
    """

    def __init__(
        self,
        message: str,
        *,
        code: str = "infra",
        retryable: bool = True,
        alert: bool = False,
        details: str = "",
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.alert = alert
        self.details = details


@dataclass(frozen=True)
class Policy:
    """The per-run knobs the attempt loop and adapters honor. A typed field
    set instead of a string-keyed dict: a typo'd knob is a construction-time
    error, never a silently ignored setting. None everywhere means "the
    caller never said" — the runner's default applies; an explicitly falsy
    value (timeout 0, empty preamble) is honored as given."""

    attempt_timeout_minutes: float | None = None
    stall_seconds: float | None = None
    disallowed_tools: tuple[str, ...] = ()
    setting_sources: tuple[str, ...] | None = None
    effort: str | None = None
    resume_preamble: str | None = None


@dataclass(frozen=True)
class RunSpec:
    """One agent run as the runner sees it: nothing here the caller did not
    say. ``task_type``/``labels`` are opaque caller vocabulary (display and
    attribution, never parsed). ``agent_config`` is the agent's per-harness
    config table as DATA: None means the caller never said (a loud error
    when an ``agent_ref`` is named), {} means a deliberately unconfigured
    session — the runner never reads the caller's source tree to find out.

    ``policy`` is a ``Policy`` — the typed per-run knob set.
    """

    key: str                          # attribution key (env stamps, event labels)
    harness: str                      # adapter registry key
    agent_ref: str = ""
    agent_config: dict[str, Any] | None = None
    task_type: str = ""               # opaque caller vocabulary
    labels: dict[str, Any] = field(default_factory=dict)
    policy: Policy = field(default_factory=Policy)
    repair_rounds: int = 0
    resource_specs: tuple[dict[str, Any], ...] = ()
    required_env: tuple[str, ...] = ()


@dataclass(frozen=True)
class Verdict:
    """The caller's validation closure's answer for one attempt's output.

    agent-runner does no validation of its own — contracts are the
    project's. ``valid`` False classifies the attempt ``invalid_schema``;
    ``repair_message`` is the ready-to-send follow-up (the project's
    auto-generated repair text) the runner messages into the still-open
    session when the adapter supports it and the spec carries repair budget.
    ``data`` carries the parsed output object on a valid verdict.
    """

    valid: bool
    message: str = ""
    repair_message: str | None = None
    data: dict[str, Any] | None = None


@dataclass
class Usage:
    """Token and cost totals aggregated from the attempt's stream events."""

    tok_input: int = 0
    tok_cache_write: int = 0
    tok_cache_read: int = 0
    tok_output: int = 0
    cost_usd: float = 0.0

    def add_event(self, event: Any) -> None:
        for name in ("tok_input", "tok_cache_write", "tok_cache_read", "tok_output"):
            value = getattr(event, name, None)
            if value is not None:
                setattr(self, name, getattr(self, name) + value)
        cost = getattr(event, "cost_usd", None)
        if cost is not None:
            self.cost_usd += cost


@dataclass
class AttemptReport:
    """How one attempt ended: exactly one outcome, plus the evidence.

    ``outcome`` is one word from ``agent_runner.outcomes.OUTCOMES``.
    ``session_ref`` is the CLI session this attempt opened (or resumed) —
    the handle a later attempt resumes. ``error`` is the operator-facing
    message for non-valid outcomes; ``detail`` the CLI-owned error text
    behind it. ``data`` is the validator's parsed output on ``valid``.
    """

    outcome: str
    session_ref: str | None = None
    error: str = ""
    detail: str = ""
    data: dict[str, Any] | None = None
    usage: Usage = field(default_factory=Usage)
    resumed: bool = False
    repair_rounds_used: int = 0
    workdir: Path | None = None

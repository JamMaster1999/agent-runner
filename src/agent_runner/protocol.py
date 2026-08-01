"""Transport-neutral runner protocol: one surface, two bindings (design doc
§5, D1c).

Every operation the pipeline manager may ask of the agent runner is declared
here: the ``RunnerProtocol`` ABC plus the request/response wire dataclasses.
All identifiers crossing the boundary — ``job_key``, ``group_key``,
``task_type``, ``labels``, ``request_identity``, ``outcome_code`` — are
OPAQUE to the runner: plain strings (labels a plain JSON object) the runner
stores and returns but never parses. Calls are synchronous and blocking in
phase 2 (the chain driver keeps its straight-line shape); each operation's
service-mode mapping is §5's table.

The local binding (core/runner/local.py) is the only one that exists
tonight; the HTTP binding slots into the same conformance suite at
extraction (doc §10 step 8). That is why every wire dataclass must
round-trip through JSON and every operation must carry the same signature on
every binding — enforced by tests/test_protocol_conformance.py.
"""

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Sequence, TypeVar

# The §5 operation names, exactly as they appear on RunnerProtocol. The
# conformance suite asserts each exists on the ABC and on every binding with
# a matching signature; a new operation must land here AND in the design doc.
OPERATIONS = (
    "submit",
    "await_outcome",
    "await_all",
    "report_output",
    "send_followup",
    "cancel",
    "requeue",
    "block",
    "interrupt",
    "get_artifacts",
    "acquire_lease",
    "lease_heartbeat",
    "release_lease",
    "track_task",
    "task_heartbeat",
    "finish_task",
    "fail_task",
    "preflight",
    "list_jobs",
    "list_events",
)


@dataclass(frozen=True)
class SubmitRequest:
    """One job submission (§5 op 1, replaces ensure_job).

    ``reset='upsert'`` is today's default: a metadata upsert that requeues
    only terminal rows. ``'force'`` is --force-rerun's full reset. ``labels``
    are display strings only — nothing ever parses them (§4).

    Step-4 growth (extraction plan §4 item 4): the job crosses the surface
    as DATA — agent, prompt template, artifact contract, probe/resource
    specs — so the local binding reconstructs its internal runtime from the
    request instead of a bind registry. ``prompt_ref['sha256']`` MUST equal
    the resume fingerprint digest of the pre-substitution template
    (attempts.resume_prompt_fingerprint), or existing sessions silently stop
    matching. A submit may omit ``prompt_ref`` (late binding: phase2/
    synthesis templates depend on earlier outputs); a second upsert submit
    carrying the template must land before await_outcome. ``client_refs``
    is TRANSITIONAL: {'institution_id': uuid} for the legacy business
    columns and lease resolution — it dies at cutover (plan §3)."""

    job_key: str
    group_key: str
    task_type: str
    harness: str
    labels: dict[str, Any] = field(default_factory=dict)
    reset: str = "upsert"  # 'upsert' | 'force'
    max_attempts: int | None = None
    request_identity: str | None = None
    # step-4 growth (§2): the job as data.
    agent_ref: str = ""
    prompt_ref: dict[str, str] | None = None  # {"template": raw text, "sha256": hex digest}
    artifact_contract: dict[str, Any] | None = None
    # {"attempt_dir_name": spec key, "output_filename": phase output name,
    #  "canonical_path": repo-relative canonical target}
    probe_spec: dict[str, Any] | None = None
    # {"probe": registered callback key, "repair_rounds": int, "expensive": bool}
    resource_specs: list[dict[str, Any]] = field(default_factory=list)
    # e.g. [{"kind": "cdp_browser", "resumable": False}]
    required_env: list[str] = field(default_factory=list)
    policy: dict[str, Any] | None = None
    # {"attempt_timeout_minutes": int, "resume": bool} — success stays a
    # caller act via finish_task (R1); no success_message here.
    client_refs: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class JobHandle:
    job_key: str
    group_key: str = ""


@dataclass(frozen=True)
class ArtifactRef:
    """One stored artifact, addressed by (job_key, attempt, name) (§3).
    ``ref`` is opaque text: a local path today, an object-storage key on
    Modal — the field never changes shape."""

    job_key: str
    attempt: int
    name: str
    ref: str


@dataclass(frozen=True)
class Usage:
    """Typed usage totals (§8): rotation and cost accounting cannot run on
    regex, so usage is typed at the boundary."""

    tok_input: int = 0
    tok_cache_write: int = 0
    tok_cache_read: int = 0
    tok_output: int = 0
    cost_usd: float = 0.0


@dataclass(frozen=True)
class Outcome:
    """A job's terminal answer (§5 op 2): {status, attempts, error_code,
    outcome_code, artifacts, usage, data}. ``error_code`` is RUNNER
    vocabulary (§6); ``outcome_code`` is CALLER vocabulary, opaque.
    ``data`` (step 4, R5) carries the parsed output object so the
    phase-1 → synthesis → phase-2 in-memory flow survives the facade."""

    status: str
    attempts: int
    error_code: str | None = None
    outcome_code: str | None = None
    artifacts: list[ArtifactRef] = field(default_factory=list)
    usage: Usage = field(default_factory=Usage)
    data: dict[str, Any] | None = None

    @classmethod
    def from_wire_dict(cls, payload: dict[str, Any]) -> "Outcome":
        return cls(
            status=payload["status"],
            attempts=payload["attempts"],
            error_code=payload.get("error_code"),
            outcome_code=payload.get("outcome_code"),
            artifacts=[ArtifactRef(**ref) for ref in payload.get("artifacts") or []],
            usage=Usage(**(payload.get("usage") or {})),
            data=payload.get("data"),
        )


@dataclass(frozen=True)
class OutputReport:
    """Wire form of a probe's ProbeReport (§5 op 4, §6). ``verdict`` is the
    client vocabulary (valid | invalid_schema | missing_evidence |
    infrastructure_failure); no retry opinion is expressible — the field
    does not exist. The parsed data object stays client-side."""

    job_key: str
    attempt: int
    verdict: str
    outcome_code: str = ""
    message: str = ""
    repair_message: str | None = None
    details: str = ""


@dataclass(frozen=True)
class LeaseRequest:
    """Generic named exclusivity with stale takeover (§5 op 9). The
    ``lease_key`` is opaque; ``holder`` identifies the acquiring run."""

    lease_key: str
    holder: str
    stale_after_s: int
    labels: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Lease:
    lease_key: str
    holder: str
    lease_ref: str


@dataclass(frozen=True)
class TaskRequest:
    """D9 tracked task (§5 op 10): claim-dedupe + heartbeat + terminal
    record. No retry loop, no accounts — and a tracked task is NEVER killed
    on cancel, only flagged."""

    task_key: str
    labels: dict[str, Any] = field(default_factory=dict)
    stale_after_s: int | None = None


@dataclass(frozen=True)
class TaskHandle:
    task_key: str


@dataclass(frozen=True)
class TaskFailure:
    """Terminal record for a failed tracked task. ``outcome_code`` is the
    caller's vocabulary, opaque to the runner."""

    message: str
    outcome_code: str = "unknown"
    details: str = ""


@dataclass(frozen=True)
class PreflightRequest:
    """§5 op 11: the runner half of preflight — adapter health checks plus
    named-env presence (the runner checks Firecrawl without knowing
    Firecrawl). The client keeps its own preflight for its DB and results/
    probes. ``binaries_only`` (step 4, R7) is --skip-cli-health parity:
    adapter binary presence + required_env only, no live CLI probes."""

    harnesses: list[str] = field(default_factory=list)
    required_env: list[str] = field(default_factory=list)
    binaries_only: bool = False


@dataclass(frozen=True)
class PreflightReport:
    ok: bool
    failures: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class JobQuery:
    """§5 op 12 read filter: by opaque ``group_key`` — never by parsing
    keys."""

    group_key: str | None = None
    status: str | None = None
    limit: int = 100


@dataclass(frozen=True)
class EventQuery:
    """§5 op 12 event read: cursor pagination on the event id."""

    group_key: str | None = None
    after_id: int | None = None
    limit: int = 500


# Every wire dataclass, for the conformance suite's JSON round-trip check.
WIRE_DATACLASSES = (
    SubmitRequest,
    JobHandle,
    ArtifactRef,
    Usage,
    Outcome,
    OutputReport,
    LeaseRequest,
    Lease,
    TaskRequest,
    TaskHandle,
    TaskFailure,
    PreflightRequest,
    PreflightReport,
    JobQuery,
    EventQuery,
)

T = TypeVar("T")


def to_wire(message: Any) -> dict[str, Any]:
    """Wire (JSON-object) form of a protocol dataclass."""
    return dataclasses.asdict(message)


def from_wire(cls: type[T], payload: dict[str, Any]) -> T:
    """Rebuild a protocol dataclass from its wire form."""
    decode = getattr(cls, "from_wire_dict", None)
    if decode is not None:
        return decode(payload)
    return cls(**payload)


class RunnerProtocol(ABC):
    """The one client surface (§5). GTM call sites go through a binding of
    this ABC only; bindings must never require callers to know which one
    they hold."""

    @abstractmethod
    def submit(self, request: SubmitRequest) -> JobHandle:
        """§5 op 1: idempotent upsert on (project, job_key); replaces
        ensure_job. ``reset`` picks upsert-requeue-terminal vs full reset."""

    @abstractmethod
    def await_outcome(self, handle: JobHandle, timeout_s: float | None = None) -> Outcome:
        """§5 op 2: the blocking half of the old run_or_resume — run the job
        to a terminal answer, reusing valid prior output first."""

    @abstractmethod
    def await_all(self, handles: Sequence[JobHandle]) -> dict[str, Outcome]:
        """§5 op 3: collect-don't-abort over many handles (today's
        failure-list semantics on the fan-out phases)."""

    @abstractmethod
    def report_output(self, report: OutputReport) -> None:
        """§5 op 4: probe verdict round-trip. Library mode runs probes
        in-process; service mode reports against attempt.output_written."""

    @abstractmethod
    def send_followup(self, handle: JobHandle, message: str, timeout_s: float | None = None) -> Outcome:
        """§5 op 5: message an existing session (repair). Message opaque;
        capability-gated, degrades to plain retry."""

    @abstractmethod
    def cancel(self, job_key: str) -> None:
        """§5 op 6: contractually eventual (≤ one poll interval). Tracked
        tasks are never killed, only flagged."""

    @abstractmethod
    def requeue(self, job_key: str) -> None:
        """§5 op 7: terminal-only operator recovery — resets the budget,
        preserves attempts rows so the session continues."""

    @abstractmethod
    def block(self, job_key: str, reason: str, outcome_code: str = "unknown", details: str = "") -> None:
        """Step-4 op: record a terminal 'blocked' stop for a job the caller
        drove (e.g. an import failure after a valid agent output). Ownership-
        guarded and event-fenced exactly like the runner's own lifecycle
        writes; ``outcome_code``/``details`` are the caller's opaque
        vocabulary."""

    @abstractmethod
    def interrupt(self, scope: str) -> list[str]:
        """Step-4 op: bulk-flag the scope's (a run id's) still-running jobs
        for stranded-job recovery on shutdown (SIGTERM). Ownership-guarded to
        rows this worker claimed for that run; returns the marked job keys
        for the shutdown log."""

    @abstractmethod
    def get_artifacts(self, job_key: str) -> list[ArtifactRef]:
        """§5 op 8: the runner answers "what exists"; the client validates,
        decides reuse, and promotes — before submitting."""

    @abstractmethod
    def acquire_lease(self, request: LeaseRequest) -> Lease:
        """§5 op 9: generic named exclusivity with stale takeover."""

    @abstractmethod
    def lease_heartbeat(self, lease: Lease) -> None:
        """§5 op 9: keep a held lease fresh (job heartbeats also bump their
        linked lease)."""

    @abstractmethod
    def release_lease(self, lease: Lease, outcome: str) -> None:
        """§5 op 9: release a held lease. ``outcome`` is the caller's run
        outcome vocabulary, opaque here."""

    @abstractmethod
    def track_task(self, request: TaskRequest) -> TaskHandle:
        """§5 op 10 (D9): claim a tracked task — the claim-dedupe is the
        double-import guard. Replaces claim_script_job."""

    @abstractmethod
    def task_heartbeat(self, handle: TaskHandle) -> str | None:
        """§5 op 10 (D9): bump the task's heartbeat against the reaper;
        returns the task's current status (the cancel FLAG — tracked tasks
        are never killed), or None when the probe failed (advisory)."""

    @abstractmethod
    def finish_task(self, handle: TaskHandle, message: str = "") -> None:
        """§5 op 10 (D9): record the task's terminal success."""

    @abstractmethod
    def fail_task(self, handle: TaskHandle, failure: TaskFailure) -> None:
        """§5 op 10 (D9): record the task's terminal failure. Replaces
        fail_script_job; no retry loop — import retry policy stays a
        pipeline-manager concern."""

    @abstractmethod
    def preflight(self, request: PreflightRequest) -> PreflightReport:
        """§5 op 11: adapter health checks + named-env presence."""

    @abstractmethod
    def list_jobs(self, query: JobQuery) -> list[dict[str, Any]]:
        """§5 op 12: job rows filtered by opaque group_key/status."""

    @abstractmethod
    def list_events(self, query: EventQuery) -> list[dict[str, Any]]:
        """§5 op 12: event rows after a cursor id, oldest first."""

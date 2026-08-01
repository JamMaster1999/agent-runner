"""Structured probe outcomes and the runner error-code vocabulary (design
doc §6, phase-2 step 7).

Two vocabularies, deliberately separate:

- The CLIENT (probe) vocabulary is what a probe may say about an attempt's
  output. It carries NO retry opinion — the field does not exist on
  ProbeReport. Probes report what they saw; the engine's POLICY table
  (core/runner/engine.py) is the only place retry decisions live.
- The RUNNER vocabulary (error_code) is the engine's judged failure codes,
  the signal set the POLICY table routes on.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# -- Client (probe) vocabulary: what a probe may say about an output (§6).
VALID = "valid"
INVALID_SCHEMA = "invalid_schema"
MISSING_EVIDENCE = "missing_evidence"
# The probe's own infrastructure broke (validator subprocess timeout, dry-run
# import stall) — it says nothing about the agent's work.
INFRASTRUCTURE_FAILURE = "infrastructure_failure"

PROBE_VERDICTS = (VALID, INVALID_SCHEMA, MISSING_EVIDENCE, INFRASTRUCTURE_FAILURE)

# -- Runner vocabulary (error_code): the engine's judged failure codes (§6).
AUTH = "auth"
BILLING = "billing"
RATE_LIMITED = "rate_limited"
BUDGET = "budget"
INVALID_INVOCATION = "invalid_invocation"
TIMEOUT = "timeout"
SPAWN_FAILURE = "spawn_failure"
PROBE_TIMEOUT = "probe_timeout"
CANCELLED = "cancelled"
UNKNOWN = "unknown"

ERROR_CODES = (
    AUTH,
    BILLING,
    RATE_LIMITED,
    BUDGET,
    INVALID_INVOCATION,
    TIMEOUT,
    SPAWN_FAILURE,
    PROBE_TIMEOUT,
    CANCELLED,
    UNKNOWN,
)


@dataclass(frozen=True)
class ProbeReport:
    """One probe's structured verdict on an attempt's output.

    ``verdict`` is one of PROBE_VERDICTS. ``outcome_code`` is the caller's
    own vocabulary for the specific defect (e.g. 'dead_urls',
    'stale_or_wrong_output') — opaque to the runner, recorded for events and
    attempt history. ``data`` carries the parsed output object on a valid
    verdict. ``repair_message`` is a ready-to-send follow-up for a
    repairable defect; the engine uses it only when the adapter has the
    followup capability and the job has repair budget. No retry opinion is
    expressible here — deliberately (§6).
    """

    verdict: str
    message: str = ""
    outcome_code: str = ""
    details: str = ""
    repair_message: str | None = None
    data: dict[str, Any] | None = None

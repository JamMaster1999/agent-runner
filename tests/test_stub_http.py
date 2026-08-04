#!/usr/bin/env python3
"""Stub-HTTP binding tests (extraction step 7).

``StubHTTPRunner`` is the HTTP transport's serialization skeleton: every
§5 operation must exist with the protocol's exact signature, force each
argument and return value through a real to_wire -> JSON text -> from_wire
hop (the inner binding sees fresh, equal instances — never the caller's
objects), and refuse — loudly, with json.dumps's TypeError — any value
that cannot cross JSON. The dual-binding conformance suite lives
client-side (GTM tests/test_protocol_conformance.py); these tests pin the
wire behavior itself.
"""

from __future__ import annotations

import dataclasses
import inspect
import sys
import unittest
from pathlib import Path

import os as _os

REPO = Path(__file__).resolve().parents[1]
# Runner-repo test header: point the runner's path constants at this repo,
# then put src/ on sys.path when agent_runner is not already importable (the
# no-pip stdlib run — the same path the GTM bootstrap shim relies on).
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
_os.environ.setdefault("RUNNER_PROJECT_ID", "testproj")
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner.protocol import (  # noqa: E402
    OPERATIONS,
    ArtifactRef,
    EventQuery,
    JobHandle,
    JobQuery,
    Lease,
    LeaseRequest,
    Outcome,
    OutputReport,
    PreflightReport,
    PreflightRequest,
    RunnerProtocol,
    SubmitRequest,
    TaskFailure,
    TaskHandle,
    TaskRequest,
    Usage,
)
from agent_runner.stub_http import StubHTTPRunner  # noqa: E402

# Sample wire dataclasses — mirrors the sample-building approach in the
# client-side conformance suite.
HANDLE = JobHandle(job_key="999_conformance__phase5_batch_001__claude", group_key="999_conformance")
ARTIFACT = ArtifactRef(job_key=HANDLE.job_key, attempt=2, name="primary", ref="results/x/claude/phase5.json")
OUTCOME = Outcome(
    status="succeeded",
    attempts=2,
    outcome_code="valid",
    artifacts=[ARTIFACT],
    usage=Usage(tok_input=10, cost_usd=0.05),
    data={"_meta": {"phase": "phase5"}, "instructors": []},
)
LEASE = Lease(lease_key="999_conformance", holder="run-1", lease_ref="run-1")
TASK_HANDLE = TaskHandle(task_key="999_conformance__phase3_import__script")
SUBMIT = SubmitRequest(
    job_key=HANDLE.job_key,
    group_key="999_conformance",
    task_type="phase5",
    harness="claude",
    labels={"institution": "Conformance University", "agent": "prod-phase5-instructor"},
    max_attempts=3,
    agent_ref="prod-phase5-instructor",
    prompt_ref={"template": "Enrich ${RUNNER_JOB_KEY}", "sha256": "ab" * 32},
)

# One sample invocation per §5 operation: (positional arguments).
SAMPLE_CALLS: dict[str, tuple] = {
    "submit": (SUBMIT,),
    "await_outcome": (HANDLE, 30.0),
    "await_all": ([HANDLE],),
    "report_output": (
        OutputReport(job_key=HANDLE.job_key, attempt=1, verdict="valid", outcome_code="valid"),
    ),
    "send_followup": (HANDLE, "Re-find live URLs for ...", 60.0),
    "cancel": (HANDLE.job_key,),
    "requeue": (HANDLE.job_key,),
    "block": (HANDLE.job_key, "import failed", "phase5_import_failed", "trace"),
    "interrupt": ("run-1",),
    "get_artifacts": (HANDLE.job_key,),
    "acquire_lease": (LeaseRequest(lease_key="999_conformance", holder="run-1", stale_after_s=600),),
    "lease_heartbeat": (LEASE,),
    "release_lease": (LEASE, "success"),
    "track_task": (
        TaskRequest(task_key=TASK_HANDLE.task_key, labels={"message": "Started Phase 3 DB import"}),
    ),
    "task_heartbeat": (TASK_HANDLE,),
    "finish_task": (TASK_HANDLE, "imported"),
    "fail_task": (TASK_HANDLE, TaskFailure(message="boom", outcome_code="db_import_failed")),
    "preflight": (PreflightRequest(harnesses=["claude"], required_env=["FIRECRAWL_API_KEY"]),),
    "list_jobs": (JobQuery(group_key="999_conformance", status="running", limit=10),),
    "list_events": (EventQuery(group_key="999_conformance", after_id=5, limit=100),),
}

# Canned inner-binding returns: a wire dataclass wherever the protocol
# returns one, so the response leg of the hop is exercised too; operations
# absent here return None.
CANNED_RETURNS: dict[str, object] = {
    "submit": HANDLE,
    "await_outcome": OUTCOME,
    "await_all": {HANDLE.job_key: OUTCOME},
    "send_followup": OUTCOME,
    "interrupt": ["a__job", "b__job"],
    "get_artifacts": [ARTIFACT],
    "acquire_lease": LEASE,
    "track_task": TASK_HANDLE,
    "task_heartbeat": "running",
    "preflight": PreflightReport(ok=True, failures=[]),
    "list_jobs": [{"job_key": HANDLE.job_key, "status": "running"}],
    "list_events": [],
}


class RecordingRunner(RunnerProtocol):
    """Fake inner binding: records every call's arguments, replays the
    canned returns. Hand-written on purpose — independent of the stub's
    generated dispatch, so a dispatch bug cannot hide inside the fake."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self.returns: dict[str, object] = dict(CANNED_RETURNS)

    def _answer(self, name: str, *args):
        self.calls.append((name, args))
        return self.returns.get(name)

    def submit(self, request):
        return self._answer("submit", request)

    def await_outcome(self, handle, timeout_s=None):
        return self._answer("await_outcome", handle, timeout_s)

    def await_all(self, handles):
        return self._answer("await_all", handles)

    def report_output(self, report):
        return self._answer("report_output", report)

    def send_followup(self, handle, message, timeout_s=None):
        return self._answer("send_followup", handle, message, timeout_s)

    def cancel(self, job_key):
        return self._answer("cancel", job_key)

    def requeue(self, job_key):
        return self._answer("requeue", job_key)

    def block(self, job_key, reason, outcome_code="unknown", details=""):
        return self._answer("block", job_key, reason, outcome_code, details)

    def interrupt(self, scope):
        return self._answer("interrupt", scope)

    def get_artifacts(self, job_key):
        return self._answer("get_artifacts", job_key)

    def acquire_lease(self, request):
        return self._answer("acquire_lease", request)

    def lease_heartbeat(self, lease):
        return self._answer("lease_heartbeat", lease)

    def release_lease(self, lease, outcome):
        return self._answer("release_lease", lease, outcome)

    def track_task(self, request):
        return self._answer("track_task", request)

    def task_heartbeat(self, handle):
        return self._answer("task_heartbeat", handle)

    def finish_task(self, handle, message=""):
        return self._answer("finish_task", handle, message)

    def fail_task(self, handle, failure):
        return self._answer("fail_task", handle, failure)

    def preflight(self, request):
        return self._answer("preflight", request)

    def list_jobs(self, query):
        return self._answer("list_jobs", query)

    def list_events(self, query):
        return self._answer("list_events", query)


class SurfaceTest(unittest.TestCase):
    """Every §5 operation exists on the stub with the protocol's exact
    signature — the same check the client-side conformance suite runs."""

    def test_every_operation_matches_the_protocol_signature(self) -> None:
        for name in OPERATIONS:
            with self.subTest(operation=name):
                implementation = getattr(StubHTTPRunner, name, None)
                self.assertIsNotNone(implementation, f"StubHTTPRunner lacks {name}")
                self.assertFalse(
                    getattr(implementation, "__isabstractmethod__", False),
                    f"StubHTTPRunner.{name} is still abstract",
                )
                self.assertEqual(
                    inspect.signature(implementation),
                    inspect.signature(getattr(RunnerProtocol, name)),
                    f"StubHTTPRunner.{name} signature drifted from the protocol",
                )

    def test_stub_is_instantiable_and_is_a_binding(self) -> None:
        self.assertIsInstance(StubHTTPRunner(RecordingRunner()), RunnerProtocol)

    def test_sample_calls_cover_every_operation(self) -> None:
        self.assertEqual(set(SAMPLE_CALLS), set(OPERATIONS))


class RoundTripTest(unittest.TestCase):
    """Arguments and returns actually cross JSON: the inner binding sees
    equal-but-fresh instances, and the caller gets equal-but-fresh returns."""

    def test_every_operation_round_trips_arguments_and_returns(self) -> None:
        for name in OPERATIONS:
            with self.subTest(operation=name):
                inner = RecordingRunner()
                stub = StubHTTPRunner(inner)
                args = SAMPLE_CALLS[name]
                result = getattr(stub, name)(*args)

                self.assertEqual(len(inner.calls), 1)
                recorded_name, recorded_args = inner.calls[0]
                self.assertEqual(recorded_name, name)
                self.assertEqual(recorded_args, args)
                # Fresh instances, not the caller's objects: the values
                # really crossed JSON text, not a passthrough.
                for original, received in zip(args, recorded_args):
                    if dataclasses.is_dataclass(original):
                        self.assertIsNot(received, original)
                    elif isinstance(original, list):
                        for o_item, r_item in zip(original, received):
                            if dataclasses.is_dataclass(o_item):
                                self.assertIsNot(r_item, o_item)

                expected = CANNED_RETURNS.get(name)
                self.assertEqual(result, expected)
                if dataclasses.is_dataclass(expected):
                    self.assertIsNot(result, expected)

    def test_await_all_return_hops_value_wise(self) -> None:
        # dict[str, Outcome]: each Outcome comes back rebuilt, not shared.
        stub = StubHTTPRunner(RecordingRunner())
        outcomes = stub.await_all([HANDLE])
        self.assertEqual(outcomes, {HANDLE.job_key: OUTCOME})
        self.assertIsNot(outcomes[HANDLE.job_key], OUTCOME)


class DictKeyFidelityTest(unittest.TestCase):
    """JSON object keys are strings: a real transport coerces int/float/
    bool/None keys to their json.dumps renderings and rejects everything
    else — the stub must not let a non-string key cross the seam intact."""

    def test_non_string_dict_keys_cross_as_json_strings(self) -> None:
        inner = RecordingRunner()
        stub = StubHTTPRunner(inner)
        stub.cancel({1: "a", None: "b", 2.5: "c"})
        stub.cancel({True: "d"})  # separate dict: True == 1 would collapse
        self.assertEqual(
            [args for _, args in inner.calls],
            [({"1": "a", "null": "b", "2.5": "c"},), ({"true": "d"},)],
        )

    def test_unserializable_dict_key_raises(self) -> None:
        stub = StubHTTPRunner(RecordingRunner())
        with self.assertRaises(TypeError):
            stub.cancel({("tuple", "key"): "x"})


class SerializationRefusalTest(unittest.TestCase):
    """The stub must never silently bypass serialization: anything JSON
    cannot carry raises json.dumps's TypeError at the seam."""

    def test_smuggled_unserializable_argument_raises(self) -> None:
        stub = StubHTTPRunner(RecordingRunner())
        poisoned = dataclasses.replace(SUBMIT, labels={"socket": object()})
        with self.assertRaises(TypeError):
            stub.submit(poisoned)

    def test_bare_unserializable_argument_raises(self) -> None:
        stub = StubHTTPRunner(RecordingRunner())
        with self.assertRaises(TypeError):
            stub.cancel(object())

    def test_smuggled_unserializable_return_raises(self) -> None:
        inner = RecordingRunner()
        inner.returns["task_heartbeat"] = object()
        stub = StubHTTPRunner(inner)
        with self.assertRaises(TypeError):
            stub.task_heartbeat(TASK_HANDLE)


if __name__ == "__main__":
    unittest.main()

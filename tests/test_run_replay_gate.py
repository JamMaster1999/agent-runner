#!/usr/bin/env python3
"""ensure_job under Modal replay (platform phase 3, step 4).

GTM_RUN_REPLAY marks an orchestrator start as an auto-replay of the same
intended run (Modal replays ungraceful Function deaths). ensure_job's
"fresh start = manual intervention" reset predates auto-replay: without the
gate, every incarnation would requeue terminally failed jobs with a fresh
attempt budget (or, under --force-rerun, wipe the whole run back to zero).
Under replay the upsert must be metadata-only.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

import os as _os

REPO = Path(__file__).resolve().parents[1]
# Runner-repo test header: point the runner's path constants at this repo,
# then put src/ on sys.path when agent_runner is not already importable (the
# no-pip stdlib run — the same path the GTM bootstrap shim relies on).
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner import jobstore  # noqa: E402
from agent_runner.runtime import RunnerJob  # noqa: E402

INSTITUTION = {"id": "5b0f3a1e-0000-0000-0000-000000000001", "stable_id": "test-inst"}


def job(key: str = "phase2", phase: str = "phase2") -> RunnerJob:
    return RunnerJob(
        key=f"test-inst:{key}",
        group_key=INSTITUTION["stable_id"],
        task_type=phase,
        harness="codex",
        agent_ref="prod-phase2-departments",
        labels={"institution": "test-inst", "agent": "prod-phase2-departments"},
        attempt_dir_name=key,
        output_filename=f"{phase}.json",
        canonical_relpath=f"results/999_test/codex/{phase}.json",
        client_refs={"institution_id": INSTITUTION["id"]},
    )


class EnsureJobReplayGateTest(unittest.TestCase):
    def ensure_job_sql(self, *, force: bool, replay: bool, key: str = "phase2") -> str:
        env = {"GTM_RUN_REPLAY": "1"} if replay else {}
        with mock.patch.object(jobstore, "db_rows") as psql, \
                mock.patch.dict(jobstore.os.environ, env, clear=False):
            if not replay:
                jobstore.os.environ.pop("GTM_RUN_REPLAY", None)
            jobstore.ensure_job(
                "postgresql://unused",
                job(key=key, phase=key.split("_")[0]),
                max_attempts=5,
                force=force,
            )
        return psql.call_args[0][1]

    def test_manual_start_still_requeues_terminal_rows(self):
        sql = self.ensure_job_sql(force=False, replay=False)
        self.assertIn("attempt_count = CASE WHEN", sql)
        self.assertIn("'failed'", sql)

    def test_manual_force_still_resets(self):
        sql = self.ensure_job_sql(force=True, replay=False)
        self.assertIn("attempt_count = 0", sql)

    def test_replay_is_metadata_only(self):
        sql = self.ensure_job_sql(force=False, replay=True)
        self.assertNotIn("attempt_count", sql.split("ON CONFLICT")[1])
        self.assertNotIn("status = ", sql.split("ON CONFLICT")[1])

    def test_replay_overrides_force(self):
        sql = self.ensure_job_sql(force=True, replay=True)
        self.assertNotIn("attempt_count = 0", sql)
        self.assertNotIn("status = 'queued'", sql)

    def test_replay_metadata_upsert_is_complete(self):
        # Step-9 schema: the metadata half is the runner-vocabulary columns
        # (what used to be institution_id/agent_name/output_path rides in
        # agent_ref/artifact_contract/labels jsonb now).
        sql = self.ensure_job_sql(force=False, replay=True)
        tail = sql.split("ON CONFLICT")[1]
        for column in ("task_type", "harness", "agent_ref",
                       "artifact_contract", "policy", "labels",
                       "group_key", "max_attempts", "updated_at"):
            self.assertIn(column, tail)

    def test_default_upsert_never_requeues_succeeded(self):
        # The fan-out template machinery is gone (extraction plan §4 step 3):
        # former template keys (phase3_5/phase5/phase6) go through the same
        # default upsert as every other job, and no upsert variant — default,
        # force, or replay — ever puts 'succeeded' in the terminal requeue
        # list. Phase-level state now lives in enrichment_phase_states.
        for key in ("phase2", "phase3_5", "phase5", "phase6"):
            for force in (False, True):
                for replay in (False, True):
                    sql = self.ensure_job_sql(force=force, replay=replay, key=key)
                    self.assertNotIn(
                        "'succeeded'",
                        sql.split("ON CONFLICT")[1],
                        f"key={key} force={force} replay={replay}",
                    )


if __name__ == "__main__":
    unittest.main()

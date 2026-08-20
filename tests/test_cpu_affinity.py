#!/usr/bin/env python3
"""The CPU clamp: AGENT_RUNNER_AGENT_CPUS=N restricts every spawned CLI
child to cores 0..N-1 (Linux), composed with PDEATHSIG in one preexec
hook; unset, invalid, or non-Linux leaves the spawn untouched.
"""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

import os as _os

REPO = Path(__file__).resolve().parents[1]
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner.attempt import _cpu_affinity, _preexec  # noqa: E402

# The child reports both halves of the composed preexec hook: its CPU
# affinity and its PR_GET_PDEATHSIG signal (2 = PR_GET_PDEATHSIG).
CHILD_REPORT = (
    "import ctypes, os\n"
    "sig = ctypes.c_int()\n"
    "ctypes.CDLL('libc.so.6', use_errno=True).prctl(2, ctypes.byref(sig))\n"
    "print(sorted(os.sched_getaffinity(0)), sig.value)\n"
)


class CpuAffinityTest(unittest.TestCase):
    def test_unset_or_invalid_yields_no_hook(self) -> None:
        for value in ("", "0", "-2", "many"):
            with mock.patch.dict(_os.environ, {"AGENT_RUNNER_AGENT_CPUS": value}):
                self.assertIsNone(_cpu_affinity())

    @unittest.skipUnless(sys.platform == "linux", "sched_setaffinity is Linux-only")
    def test_child_gets_clamp_and_pdeathsig(self) -> None:
        with mock.patch.dict(_os.environ, {"AGENT_RUNNER_AGENT_CPUS": "1"}):
            out = subprocess.run(
                [sys.executable, "-c", CHILD_REPORT],
                preexec_fn=_preexec(),
                capture_output=True,
                text=True,
                check=True,
            ).stdout
        self.assertEqual(out.strip(), "[0] 15")  # cores {0}, SIGTERM


if __name__ == "__main__":
    unittest.main()

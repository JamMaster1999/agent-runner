#!/usr/bin/env python3
"""Core is Temporal-free: importing every core module must not pull
temporalio in, and no core source file may reference it (agent_runner.md:
"Core's CI runs with Temporal absent — an import leak from the layers
above fails the build").

Two gates, so the proof holds in both CI jobs: the import sweep catches a
leak when temporalio is absent (the core job), and the source scan catches
it even when the temporal extra is installed (the temporal job).
"""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

import os as _os

REPO = Path(__file__).resolve().parents[1]
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

SRC = REPO / "src" / "agent_runner"
OPTIONAL_DIRS = ("temporal",)


def core_modules() -> list[str]:
    modules = []
    for path in sorted(SRC.rglob("*.py")):
        relative = path.relative_to(SRC.parent)
        if relative.parts[1] in OPTIONAL_DIRS:
            continue
        name = ".".join(relative.with_suffix("").parts)
        if name.endswith(".__init__"):
            name = name[: -len(".__init__")]
        modules.append(name)
    return modules


class TemporalFreeCoreTest(unittest.TestCase):
    def test_importing_every_core_module_never_loads_temporalio(self) -> None:
        if importlib.util.find_spec("temporalio") is not None:
            # Another suite in this process may legitimately have imported
            # the optional layer; the sweep proves core only when Temporal
            # is absent (the core CI job). The source scan below covers the
            # installed case.
            self.skipTest("temporalio installed; the import sweep runs in the core CI job")
        for name in core_modules():
            importlib.import_module(name)
        self.assertNotIn(
            "temporalio",
            sys.modules,
            "a core module imported temporalio — the optional layer leaked "
            "into core",
        )

    def test_no_core_source_references_temporalio(self) -> None:
        offenders = []
        for path in sorted(SRC.rglob("*.py")):
            relative = path.relative_to(SRC)
            if relative.parts[0] in OPTIONAL_DIRS:
                continue
            if "temporalio" in path.read_text():
                offenders.append(str(relative))
        self.assertEqual(offenders, [])

    def test_temporal_package_without_the_extra_fails_with_guidance(self) -> None:
        try:
            import temporalio  # noqa: F401
        except ImportError:
            with self.assertRaises(ImportError) as caught:
                importlib.import_module("agent_runner.temporal")
            self.assertIn("agent-runner[temporal]", str(caught.exception))
        else:
            self.skipTest("temporalio installed; the guidance path needs it absent")


if __name__ == "__main__":
    unittest.main()

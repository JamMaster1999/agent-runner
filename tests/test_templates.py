#!/usr/bin/env python3
"""Contract tests for agent_runner.templates.substitute (D2).

The closed variable set is substituted at attempt start; unknown template
variables and any {{...}} token that survives substitution raise. Resource
values substitute as JSON-encoded scalars so one template renders both the
supplied-resource and agent-managed branches. (The CDP resource-variable
cases stayed in the GTM suite with the CDP provider they exercise.)
"""

from __future__ import annotations

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

from agent_runner.templates import TemplateError, substitute  # noqa: E402


class SubstituteTest(unittest.TestCase):
    def test_substitutes_the_closed_variable_set(self) -> None:
        rendered = substitute(
            'path `{{RUNNER_OUTPUT_PATH}}/out.json` attempt {{RUNNER_ATTEMPT}} '
            'run "{{RUNNER_JOB_KEY}}" endpoint {{RESOURCE:cdp_browser.endpoint}}',
            {
                "RUNNER_OUTPUT_PATH": "/tmp/run/attempt-01",
                "RUNNER_ATTEMPT": "1",
                "RUNNER_JOB_KEY": "run-a",
                "RESOURCE:cdp_browser.endpoint": "null",
            },
        )
        self.assertEqual(
            rendered, 'path `/tmp/run/attempt-01/out.json` attempt 1 run "run-a" endpoint null'
        )

    def test_unknown_template_variable_raises(self) -> None:
        with self.assertRaises(TemplateError):
            substitute("hello {{RUNNER_TYPO}}", {"RUNNER_ATTEMPT": "1"})

    def test_unused_provided_variables_are_allowed(self) -> None:
        # The engine always passes the full closed set; most templates simply
        # never reference the resource variables.
        self.assertEqual(
            substitute("attempt {{RUNNER_ATTEMPT}}", {
                "RUNNER_ATTEMPT": "2",
                "RESOURCE:cdp_browser.endpoint": "null",
            }),
            "attempt 2",
        )

    def test_malformed_leftover_token_raises(self) -> None:
        # Not matched by the variable pattern (space in the name), so it
        # survives substitution and must be caught by the leftover scan.
        with self.assertRaises(TemplateError):
            substitute("hello {{ RUNNER_ATTEMPT }}", {"RUNNER_ATTEMPT": "1"})

    def test_token_smuggled_in_through_a_value_raises(self) -> None:
        with self.assertRaises(TemplateError):
            substitute(
                "attempt {{RUNNER_ATTEMPT}}", {"RUNNER_ATTEMPT": "{{RUNNER_JOB_KEY}}"}
            )

    def test_plain_braces_and_json_blocks_pass_through(self) -> None:
        template = '{\n  "a": {},\n  "attempt": {{RUNNER_ATTEMPT}}\n}'
        self.assertEqual(
            substitute(template, {"RUNNER_ATTEMPT": "3"}),
            '{\n  "a": {},\n  "attempt": 3\n}',
        )


if __name__ == "__main__":
    unittest.main()

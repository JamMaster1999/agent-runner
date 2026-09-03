#!/usr/bin/env python3
"""The cdp_browser resource: template-value encoding and endpoint parsing.

No Chrome is ever spawned here — the launch path is exercised in the live
gauntlet; these pin the pure seams (DevToolsActivePort parsing, the
JSON-encoded template values, the null overlay for agent-managed runs).
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import os as _os

REPO = Path(__file__).resolve().parents[1]
_os.environ.setdefault("AGENT_RUNNER_PROJECT_ROOT", str(REPO))
try:
    import agent_runner  # noqa: F401
except ImportError:
    sys.path.insert(0, str(REPO / "src"))

from agent_runner.resources.cdp_browser import (  # noqa: E402
    CdpBrowserProvider,
    cdp_variables,
    parse_devtools_active_port,
    user_agent,
)
from agent_runner.templates import (  # noqa: E402
    CDP_BROWSER_ENDPOINT,
    CDP_BROWSER_WEBSOCKET_URL,
    substitute,
)


class UserAgentTest(unittest.TestCase):
    def test_headless_token_is_gone_and_the_major_version_is_the_binarys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fake = Path(directory) / "chromium"
            fake.write_text("#!/bin/sh\necho 'Chromium 128.0.6613.137 built on Debian'\n")
            fake.chmod(0o755)
            agent = user_agent(fake)
        self.assertIn("Chrome/128.0.0.0", agent)
        self.assertNotIn("Headless", agent)


class DevToolsActivePortTest(unittest.TestCase):
    def test_port_and_ws_path_parse(self) -> None:
        endpoint, websocket = parse_devtools_active_port(
            "9222\n/devtools/browser/abc-123\n"
        )
        self.assertEqual(endpoint, "http://127.0.0.1:9222")
        self.assertEqual(websocket, "ws://127.0.0.1:9222/devtools/browser/abc-123")

    def test_port_only_gives_empty_websocket(self) -> None:
        endpoint, websocket = parse_devtools_active_port("9222\n")
        self.assertEqual(endpoint, "http://127.0.0.1:9222")
        self.assertEqual(websocket, "")

    def test_empty_file_raises(self) -> None:
        with self.assertRaises(ValueError):
            parse_devtools_active_port("")


class TemplateValueTest(unittest.TestCase):
    def test_null_variables_render_null_into_templates(self) -> None:
        # Projects that declare nothing carry no browser; the same template
        # must render with JSON null in every resource slot.
        provider = CdpBrowserProvider()
        template = (
            '{"endpoint": ' + CDP_BROWSER_ENDPOINT + ", "
            '"ws": ' + CDP_BROWSER_WEBSOCKET_URL + "}"
        )
        rendered = substitute(template, provider.null_variables())
        self.assertEqual(rendered, '{"endpoint": null, "ws": null}')

    def test_live_variables_render_quoted_strings(self) -> None:
        variables = cdp_variables(
            {
                "cdp_browser.endpoint": "http://127.0.0.1:9222",
                "cdp_browser.websocket_url": "ws://127.0.0.1:9222/devtools/browser/x",
                "cdp_browser.profile_dir": "/tmp/profile",
                "cdp_browser.log_path": "/tmp/log",
            }
        )
        rendered = substitute("endpoint: " + CDP_BROWSER_ENDPOINT, variables)
        self.assertEqual(rendered, 'endpoint: "http://127.0.0.1:9222"')

    def test_variable_names_cover_the_closed_token_set(self) -> None:
        provider = CdpBrowserProvider()
        self.assertEqual(
            sorted(provider.null_variables()),
            sorted(
                [
                    "RESOURCE:cdp_browser.endpoint",
                    "RESOURCE:cdp_browser.websocket_url",
                    "RESOURCE:cdp_browser.profile_dir",
                    "RESOURCE:cdp_browser.log_path",
                ]
            ),
        )


if __name__ == "__main__":
    unittest.main()

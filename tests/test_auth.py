#!/usr/bin/env python3
"""Auth: volume-backed CLI credential files, the Modal model (ruling D1).

Seeded once (a refreshed credential the CLI wrote is never clobbered),
private file modes, token normalization on read (the 2026-07-30 wrapped
paste that 401'd a valid token), and the per-adapter home/credential
models behind ``sessions.prepare_session_homes``.
"""

from __future__ import annotations

import json
import sys
import tempfile
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

from agent_runner.auth import normalize_token, seed_credential_file  # noqa: E402
from agent_runner.harness import get_adapter  # noqa: E402
from agent_runner.sessions import prepare_session_homes  # noqa: E402


class NormalizeTokenTest(unittest.TestCase):
    def test_embedded_line_break_is_stripped(self) -> None:
        # The live incident: a terminal-wrapped paste embedded a newline
        # mid-token and the CLI 401'd on an otherwise valid credential.
        self.assertEqual(normalize_token("sk-ant-abc\ndef "), "sk-ant-abcdef")

    def test_all_whitespace_kinds_are_stripped(self) -> None:
        self.assertEqual(normalize_token(" a\tb\r\nc "), "abc")

    def test_clean_token_passes_through(self) -> None:
        self.assertEqual(normalize_token("sk-ant-abcdef"), "sk-ant-abcdef")


class SeedCredentialFileTest(unittest.TestCase):
    def test_seeds_once_with_private_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "codex-home" / "auth.json"
            self.assertTrue(seed_credential_file(path, '{"token": "seed"}'))
            self.assertEqual(path.read_text(), '{"token": "seed"}')
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_never_clobbers_a_refreshed_credential(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "auth.json"
            path.write_text('{"token": "refreshed-by-the-cli"}')
            self.assertFalse(seed_credential_file(path, '{"token": "stale-seed"}'))
            self.assertEqual(path.read_text(), '{"token": "refreshed-by-the-cli"}')


class AdapterHomeTest(unittest.TestCase):
    def test_codex_home_lands_on_the_volume_and_seeds_auth_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            overrides = get_adapter("codex").prepare_home(
                Path(tmp), {"CODEX_AUTH_JSON": '{"token": "t"}'}
            )
            home = Path(overrides["CODEX_HOME"])
            self.assertEqual(home, Path(tmp) / "codex-home")
            self.assertEqual((home / "auth.json").read_text(), '{"token": "t"}')
            self.assertEqual((home / "auth.json").stat().st_mode & 0o777, 0o600)

    def test_codex_home_without_a_seed_still_points_at_the_volume(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            overrides = get_adapter("codex").prepare_home(Path(tmp), {})
            self.assertTrue(Path(overrides["CODEX_HOME"]).is_dir())
            self.assertFalse((Path(overrides["CODEX_HOME"]) / "auth.json").exists())

    def test_claude_home_normalizes_the_token_and_seeds_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            overrides = get_adapter("claude").prepare_home(
                Path(tmp),
                {
                    "CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-ab\ncd",
                    "CLAUDE_CREDENTIALS_JSON": '{"oauth": true}',
                },
            )
            home = Path(overrides["CLAUDE_CONFIG_DIR"])
            self.assertEqual(home, Path(tmp) / "claude-home")
            self.assertEqual(overrides["CLAUDE_CODE_OAUTH_TOKEN"], "sk-ant-abcd")
            self.assertEqual((home / ".credentials.json").read_text(), '{"oauth": true}')

    def test_claude_bind_credentials_normalizes_on_read(self) -> None:
        with mock.patch.dict(
            _os.environ, {"CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-ab\ncd"}
        ):
            bound = get_adapter("claude").bind_credentials()
        self.assertEqual(bound, {"CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-abcd"})


class PrepareSessionHomesTest(unittest.TestCase):
    def test_collects_every_adapter_and_can_apply(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(_os.environ, {}, clear=False):
                _os.environ.pop("CODEX_HOME", None)
                overrides = prepare_session_homes(Path(tmp), apply=False)
                self.assertIn("CODEX_HOME", overrides)
                self.assertIn("CLAUDE_CONFIG_DIR", overrides)
                # apply=False left the process environment alone.
                self.assertNotIn("CODEX_HOME", _os.environ)
                applied = prepare_session_homes(Path(tmp))
                self.assertEqual(_os.environ["CODEX_HOME"], applied["CODEX_HOME"])


class SandboxCredentialTest(unittest.TestCase):
    """The credential a sandbox receives: the operator's login minus the
    one token that could rotate it (spikes 4 and 9)."""

    AUTH = {
        "OPENAI_API_KEY": None,
        "tokens": {
            "id_token": "eyJ.id.token",
            "access_token": "eyJ.access.token",
            "refresh_token": "rt-ROTATES-EVERYONE",
            "account_id": "acct_1",
        },
        "last_refresh": "2026-08-24T00:00:00Z",
    }

    def test_blanks_the_refresh_token_and_ships_the_rest(self) -> None:
        from agent_runner.harness.codex import sandbox_credential

        shipped = json.loads(sandbox_credential(json.dumps(self.AUTH)))
        self.assertEqual(shipped["tokens"]["refresh_token"], "")
        self.assertEqual(shipped["tokens"]["id_token"], "eyJ.id.token")
        self.assertEqual(shipped["tokens"]["access_token"], "eyJ.access.token")
        self.assertEqual(shipped["tokens"]["account_id"], "acct_1")
        self.assertNotIn("ROTATES", sandbox_credential(json.dumps(self.AUTH)))

    def test_rejects_anything_but_a_chatgpt_login(self) -> None:
        from agent_runner.harness.codex import sandbox_credential
        from agent_runner.runtime import RunnerError

        for bad in ("not json", '{"OPENAI_API_KEY": "sk-x"}', '{"tokens": {"id_token": "", "access_token": "a", "account_id": "b"}}'):
            with self.assertRaises(RunnerError) as caught:
                sandbox_credential(bad)
            self.assertEqual(caught.exception.code, "auth")
            self.assertFalse(caught.exception.retryable)


if __name__ == "__main__":
    unittest.main()

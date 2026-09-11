"""A Codex login refreshes on the server: the exchange codex login makes,
the result is the login now, and a refresh token the server rejects is a
login a person must make again."""

from __future__ import annotations

import base64
import io
import json
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from unittest import mock

from agent_runner.harness.codex import CLIENT_ID, REFRESH_URL, login_expires_at, refresh_login
from agent_runner.runtime import RunnerError

EXPIRES = datetime(2026, 9, 13, 21, 21, tzinfo=timezone.utc)


def jwt(exp: datetime) -> str:
    claims = base64.urlsafe_b64encode(json.dumps({"exp": int(exp.timestamp())}).encode()).decode().rstrip("=")
    return f"h.{claims}.s"


LOGIN = json.dumps({
    "auth_mode": "chatgpt",
    "tokens": {"id_token": "old-id", "access_token": jwt(EXPIRES), "refresh_token": "old-refresh", "account_id": "acct"},
    "last_refresh": "2026-09-03T21:21:13Z",
})


def answer(status: int, body: dict):
    if status == 200:
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(body).encode()
        return mock.patch("urllib.request.urlopen", return_value=response)
    error = urllib.error.HTTPError(REFRESH_URL, status, "no", {}, io.BytesIO(json.dumps(body).encode()))
    return mock.patch("urllib.request.urlopen", side_effect=error)


class CodexLoginTest(unittest.TestCase):
    def test_the_access_token_says_when_the_login_runs_out(self) -> None:
        self.assertEqual(login_expires_at(LOGIN), EXPIRES)

    def test_a_refresh_sends_the_refresh_token_and_returns_the_login_with_fresh_tokens(self) -> None:
        fresh = {"id_token": "new-id", "access_token": jwt(EXPIRES + timedelta(days=10)), "refresh_token": "new-refresh"}
        with answer(200, fresh) as urlopen:
            refreshed = json.loads(refresh_login(LOGIN))
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, REFRESH_URL)
        self.assertEqual(json.loads(request.data), {"client_id": CLIENT_ID, "grant_type": "refresh_token", "refresh_token": "old-refresh"})
        self.assertEqual(refreshed["tokens"], {**fresh, "account_id": "acct"}, "the three tokens rotate, the account stays")
        self.assertEqual(refreshed["auth_mode"], "chatgpt")
        self.assertNotEqual(refreshed["last_refresh"], "2026-09-03T21:21:13Z")
        self.assertEqual(login_expires_at(json.dumps(refreshed)), EXPIRES + timedelta(days=10))

    def test_a_rejected_refresh_token_is_a_login_a_person_must_make_again(self) -> None:
        with answer(400, {"error": "invalid_grant", "error_description": "refresh_token_reused"}):
            with self.assertRaises(RunnerError) as caught:
                refresh_login(LOGIN)
        self.assertEqual((caught.exception.code, caught.exception.retryable, caught.exception.alert), ("auth", False, True))
        self.assertIn("refresh_token_reused", str(caught.exception))

    def test_a_server_failure_is_not_a_verdict_on_the_login(self) -> None:
        with answer(503, {"error": "busy"}):
            with self.assertRaises(urllib.error.HTTPError):
                refresh_login(LOGIN)


if __name__ == "__main__":
    unittest.main()

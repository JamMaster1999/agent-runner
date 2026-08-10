"""A local stand-in API endpoint for the failure-surface live tests.

The rate-limit and usage-limit outcomes cannot be triggered on demand against the real
providers without actually exhausting a plan, so these tests point the REAL CLI binary at
this server instead (claude via ANTHROPIC_BASE_URL, codex via OPENAI_BASE_URL). The CLI's
whole client stack — request shaping, retry policy, error rendering — runs for real; only
the far end is ours, answering every request with the provider's documented error shape.
Zero tokens by construction.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ANTHROPIC_RATE_LIMIT = (
    429,
    {"type": "error", "error": {"type": "rate_limit_error", "message": "Rate limit exceeded."}},
)
# The subscription cap: distinct text on purpose — the marker tables classify
# "usage limit" as rate_limited before the auth/billing sweep can see it.
ANTHROPIC_USAGE_LIMIT = (
    429,
    {
        "type": "error",
        "error": {
            "type": "rate_limit_error",
            "message": "You have reached your usage limit. Your limit will reset at 6pm.",
        },
    },
)
OPENAI_RATE_LIMIT = (
    429,
    {
        "error": {
            "message": "Rate limit reached for requests. Please try again later.",
            "type": "rate_limit_exceeded",
            "code": "rate_limit_exceeded",
        }
    },
)
OPENAI_USAGE_LIMIT = (
    429,
    {
        "error": {
            "message": "You've hit your usage limit. Try again later.",
            "type": "usage_limit_reached",
            "code": "usage_limit_reached",
        }
    },
)


class StubAPI:
    """Answers every request with one configured (status, json_body); counts hits."""

    def __init__(self, status: int, body: dict, retry_after: str | None = "1"):
        self.status = status
        self.body = json.dumps(body).encode()
        self.retry_after = retry_after
        self.hits = 0
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def _respond(self) -> None:
                stub.hits += 1
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                self.send_response(stub.status)
                self.send_header("Content-Type", "application/json")
                if stub.retry_after is not None:
                    self.send_header("retry-after", stub.retry_after)
                self.send_header("Content-Length", str(len(stub.body)))
                self.end_headers()
                self.wfile.write(stub.body)

            do_GET = do_POST = do_PUT = do_DELETE = _respond

            def log_message(self, *args) -> None:  # keep pytest output clean
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def __enter__(self) -> "StubAPI":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._server.shutdown()
        self._server.server_close()

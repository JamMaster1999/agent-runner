"""Shared test helpers."""

from __future__ import annotations

import asyncio
import inspect
import time
import unittest


async def wait_for(test: unittest.TestCase, predicate, what: str = "timed out waiting", seconds: float = 15.0) -> None:
    """Until ``predicate()`` (plain or awaitable) is true, else fail with ``what``."""
    deadline = time.monotonic() + seconds
    while True:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        test.assertLess(time.monotonic(), deadline, what)
        await asyncio.sleep(0.05)

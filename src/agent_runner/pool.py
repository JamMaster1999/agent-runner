"""A pool of CLI accounts the attempts of one harness run on, and the
ledger of what each account carries.

An attempt ``lease``s the least-loaded account with room under its cap and
gives it back when the process ends; with every account full or held, the
attempt waits here (heartbeating) instead of failing. The caller decides
what a limit means (``agent_runner.temporal.sandbox``); the pool knows two
verbs for it — ``throttle`` (the account is over its concurrency: halve
the cap to what it carried, once per pause, and pause new starts) and
``hold`` (nothing runs on the account until a moment) — and grows a cap
back by one per valid attempt, up to ``share``: the fleet's slots divided
among the accounts, the most one account is ever asked to carry. Worker
memory only: a restart relearns the caps and holds in one round.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator


def now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Account:
    cap: int
    active: int = 0
    held_until: datetime | None = None

    def held(self, at: datetime) -> bool:
        return self.held_until is not None and self.held_until > at

    def free(self, at: datetime) -> bool:
        return self.active < self.cap and not self.held(at)


class Pool:
    def __init__(self, var: str, credentials: tuple[str, ...], share: int) -> None:
        if not credentials:
            raise ValueError(f"{var}: a pool needs at least one credential")
        if share < 1:
            raise ValueError(f"{var}: an account's share must be at least one attempt")
        self.var = var  # the env var the chosen credential rides the attempt in
        self.credentials = credentials
        self.share = share
        self.accounts = [Account(cap=share) for _ in credentials]
        self._changed = asyncio.Event()

    async def acquire(self) -> int:
        """The least-loaded free account, once there is one."""
        while True:
            self._changed.clear()
            at = now()
            free = [slot for slot, account in enumerate(self.accounts) if account.free(at)]
            if free:
                slot = min(free, key=lambda slot: self.accounts[slot].active)
                self.accounts[slot].active += 1
                return slot
            # A hold lifts by the clock, not by an event: wake for the earliest.
            lift = self._next_lift(at)
            with suppress(TimeoutError):
                async with asyncio.timeout((lift - at).total_seconds() if lift else None):
                    await self._changed.wait()

    def release(self, slot: int) -> None:
        self.accounts[slot].active -= 1
        self._changed.set()

    @asynccontextmanager
    async def lease(self) -> AsyncIterator[int]:
        slot = await self.acquire()
        try:
            yield slot
        finally:
            self.release(slot)

    def env(self, slot: int) -> dict[str, str]:
        return {self.var: self.credentials[slot]}

    def succeeded(self, slot: int) -> None:
        account = self.accounts[slot]
        account.cap = min(self.share, account.cap + 1)
        self._changed.set()

    def throttle(self, slot: int, until: datetime) -> None:
        """The account is over its concurrency: its cap halves to what it
        carried — once per pause, so a burst of reports from the sessions
        already running does not halve it to nothing — and new starts wait
        until ``until``."""
        account = self.accounts[slot]
        if not account.held(now()):
            account.cap = max(1, min(account.cap, account.active) // 2)
        self.hold(slot, until)

    def hold(self, slot: int, until: datetime) -> None:
        """Nothing new runs on the account before ``until``; a hold never shortens one."""
        account = self.accounts[slot]
        account.held_until = max(account.held_until or until, until)

    def next_free(self, at: datetime | None = None) -> datetime:
        """When a retry may run: now while an account is free, else the earliest hold to lift."""
        at = at or now()
        if any(account.free(at) for account in self.accounts):
            return at
        return self._next_lift(at) or at

    def _next_lift(self, at: datetime) -> datetime | None:
        lifts = [account.held_until for account in self.accounts if account.held(at)]
        return min(lifts) if lifts else None

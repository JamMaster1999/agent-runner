"""A pool of CLI accounts the attempts of one harness run on, and the
throttle that keeps each account under what it can carry.

An attempt ``acquire``s the least-loaded account with room under its cap
and ``release``s it when the process ends; with every account full or
held, the attempt waits here (heartbeating) instead of failing. What the
CLI then reports moves the caps:

- ``rate`` (too many requests at once): the account's cap halves to what
  it was actually carrying, and it is held for a short jittered pause so
  the burst drains before the next start lands on it.
- ``usage`` (the subscription window is spent): the account is held until
  the reset the CLI named, or the caller's backoff when it named none.
- ``server`` (the provider is overloaded): not this account's doing;
  nothing here changes — the attempt itself pauses with jitter.

Every valid attempt grows its account's cap by one, back up to ``share``
— the fleet's slots divided among the accounts, the most one account is
ever asked to carry. Worker memory only: a restart relearns the caps and
holds in one round.
"""

from __future__ import annotations

import asyncio
import random
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

KIND_RATE = "rate"
KIND_USAGE = "usage"
KIND_SERVER = "server"
KINDS = (KIND_RATE, KIND_USAGE, KIND_SERVER)

RATE_HOLD = timedelta(seconds=30)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def jitter(base: timedelta, spread: float = 0.5) -> timedelta:
    """``base`` scaled by a random factor in [1 - spread, 1 + spread]: two
    hundred attempts told to wait the same time must not return together."""
    return base * random.uniform(1 - spread, 1 + spread)


@dataclass
class Account:
    cap: int
    active: int = 0
    held_until: datetime | None = None

    def free(self, now: datetime) -> bool:
        return self.active < self.cap and (self.held_until is None or self.held_until <= now)


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
        self._changed = asyncio.Condition()

    async def acquire(self) -> int:
        """The least-loaded free account, once there is one."""
        async with self._changed:
            while True:
                now = _now()
                free = [slot for slot, account in enumerate(self.accounts) if account.free(now)]
                if free:
                    slot = min(free, key=lambda slot: self.accounts[slot].active)
                    self.accounts[slot].active += 1
                    return slot
                # A hold lifts by the clock, not by an event: wake for the earliest.
                lifts = [a.held_until for a in self.accounts if a.held_until and a.held_until > now]
                with suppress(TimeoutError):
                    async with asyncio.timeout((min(lifts) - now).total_seconds() if lifts else None):
                        await self._changed.wait()

    def release(self, slot: int) -> None:
        self.accounts[slot].active -= 1
        self._wake()

    def env(self, slot: int) -> dict[str, str]:
        return {self.var: self.credentials[slot]}

    def succeeded(self, slot: int) -> None:
        account = self.accounts[slot]
        account.cap = min(self.share, account.cap + 1)
        self._wake()

    def limited(self, slot: int, kind: str, until: datetime) -> None:
        """What a limit the CLI reported means for the account it ran on.
        ``until`` is when a usage limit lifts (the CLI's reset, else the
        caller's backoff)."""
        account = self.accounts[slot]
        if kind == KIND_RATE:
            account.cap = max(1, min(account.cap, account.active) // 2)
            account.held_until = _now() + jitter(RATE_HOLD)
        elif kind == KIND_USAGE:
            account.held_until = until

    def next_free(self, now: datetime | None = None) -> datetime:
        """When a retry may run: now while an account is free, else the earliest hold to lift."""
        now = now or _now()
        if any(account.free(now) for account in self.accounts):
            return now
        lifts = [a.held_until for a in self.accounts if a.held_until and a.held_until > now]
        return min(lifts) if lifts else now

    def _wake(self) -> None:
        asyncio.get_running_loop().create_task(self._notify())  # notify needs the lock

    async def _notify(self) -> None:
        async with self._changed:
            self._changed.notify_all()

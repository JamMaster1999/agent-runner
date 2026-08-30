"""A pool of CLI accounts an activity's attempts rotate through: attempt n
runs on slot n-1 (wrapping), a rate-limited slot is held until its reset,
and with every slot held the retry waits for the earliest. Worker memory
only: a restart forgets the holds and relearns each in one attempt.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Pool:
    var: str  # the env var the chosen credential rides the attempt in
    credentials: tuple[str, ...]
    held: dict[int, datetime] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.credentials:
            raise ValueError(f"{self.var}: a pool needs at least one credential")

    def slot(self, attempt: int, now: datetime | None = None) -> int:
        """Slot ``attempt - 1`` wrapping, skipping held slots; all held, the one that frees soonest."""
        now = now or _now()
        order = [(attempt - 1 + i) % len(self.credentials) for i in range(len(self.credentials))]
        free = [slot for slot in order if self.held.get(slot, now) <= now]
        return free[0] if free else min(order, key=self.held.__getitem__)

    def env(self, slot: int) -> dict[str, str]:
        return {self.var: self.credentials[slot]}

    def hold(self, slot: int, until: datetime) -> None:
        self.held[slot] = until

    def next_free(self, now: datetime | None = None) -> datetime:
        """When a retry may run: now while a slot is free, else the earliest hold to lift."""
        now = now or _now()
        lifts = [self.held.get(slot, now) for slot in range(len(self.credentials))]
        return now if any(lift <= now for lift in lifts) else min(lifts)

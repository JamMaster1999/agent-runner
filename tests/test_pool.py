"""A pool rotates attempts across accounts, holds a rate-limited one, and waits for the earliest reset when all are held."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from agent_runner.pool import Pool

NOW = datetime(2026, 8, 26, 19, 44, tzinfo=timezone.utc)


class PoolTest(unittest.TestCase):
    def test_slots_rotate_by_attempt_and_skip_held_accounts(self) -> None:
        pool = Pool("TOKEN", ("a", "b", "c"))
        self.assertEqual([pool.slot(n, NOW) for n in (1, 2, 3, 4)], [0, 1, 2, 0])
        self.assertEqual(pool.env(1), {"TOKEN": "b"})
        pool.hold(0, NOW + timedelta(minutes=6))
        self.assertEqual(pool.slot(1, NOW), 1, "attempt 1's own slot is held: the next account")
        self.assertEqual(pool.next_free(NOW), NOW)
        pool.hold(1, NOW + timedelta(minutes=16))
        pool.hold(2, NOW + timedelta(minutes=26))
        self.assertEqual(pool.slot(4, NOW), 0, "every account held: the one that frees soonest")
        self.assertEqual(pool.next_free(NOW), NOW + timedelta(minutes=6))
        later = NOW + timedelta(minutes=7)
        self.assertEqual(pool.slot(2, later), 0, "a lifted hold frees the account")
        self.assertEqual(pool.next_free(later), later)

    def test_a_single_account_pool_waits_for_its_own_reset(self) -> None:
        pool = Pool("TOKEN", ("only",))
        pool.hold(0, NOW + timedelta(hours=1))
        self.assertEqual(pool.slot(5, NOW), 0)
        self.assertEqual(pool.next_free(NOW), NOW + timedelta(hours=1))

    def test_an_empty_pool_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            Pool("TOKEN", ())


if __name__ == "__main__":
    unittest.main()

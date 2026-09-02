"""A pool hands attempts the least-loaded account, caps what one account
carries, halves that cap on a rate limit, holds a spent account until its
reset, and grows the cap back one valid attempt at a time."""

from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from agent_runner import pool as pool_module
from agent_runner.pool import KIND_RATE, KIND_SERVER, KIND_USAGE, Pool

NOW = datetime(2026, 8, 26, 19, 44, tzinfo=timezone.utc)


class PoolTest(unittest.IsolatedAsyncioTestCase):
    async def test_attempts_spread_over_the_least_loaded_accounts(self) -> None:
        pool = Pool("TOKEN", ("a", "b", "c"), share=2)
        slots = [await pool.acquire() for _ in range(6)]
        self.assertEqual(sorted(slots), [0, 0, 1, 1, 2, 2], "every account carries its share, none more")
        self.assertEqual(slots[:3], [0, 1, 2], "each new attempt lands on the emptiest account")
        self.assertEqual(pool.env(1), {"TOKEN": "b"})

    async def test_a_full_pool_makes_the_next_attempt_wait_for_a_release(self) -> None:
        pool = Pool("TOKEN", ("only",), share=1)
        first = await pool.acquire()
        waiter = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0.05)
        self.assertFalse(waiter.done(), "the account is full: the attempt waits, it does not fail")
        pool.release(first)
        self.assertEqual(await asyncio.wait_for(waiter, 2), 0)

    async def test_a_rate_limit_halves_the_cap_to_what_the_account_carried(self) -> None:
        pool = Pool("TOKEN", ("a",), share=8)
        for _ in range(6):
            await pool.acquire()
        pool.limited(0, KIND_RATE, until=NOW)
        self.assertEqual(pool.accounts[0].cap, 3)
        self.assertGreater(pool.accounts[0].held_until, pool_module._now(), "and pauses new starts briefly")
        self.assertLess(pool.accounts[0].held_until, pool_module._now() + timedelta(seconds=60))
        pool.limited(0, KIND_RATE, until=NOW)
        pool.limited(0, KIND_RATE, until=NOW)
        pool.limited(0, KIND_RATE, until=NOW)
        self.assertEqual(pool.accounts[0].cap, 1, "the cap never drops below one attempt")

    async def test_a_valid_attempt_grows_the_cap_back_up_to_the_share(self) -> None:
        pool = Pool("TOKEN", ("a",), share=3)
        pool.accounts[0].cap = 1
        pool.succeeded(0)
        pool.succeeded(0)
        pool.succeeded(0)
        self.assertEqual(pool.accounts[0].cap, 3)

    async def test_a_usage_limit_holds_the_account_until_its_reset_and_the_others_carry_on(self) -> None:
        pool = Pool("TOKEN", ("a", "b"), share=4)
        reset = pool_module._now() + timedelta(hours=1)
        pool.limited(0, KIND_USAGE, until=reset)
        self.assertEqual(pool.accounts[0].held_until, reset)
        self.assertEqual([await pool.acquire() for _ in range(3)], [1, 1, 1], "nothing lands on the held account")
        self.assertEqual(pool.next_free(NOW), NOW, "an account is free: a retry may run now")
        pool.limited(1, KIND_USAGE, until=reset + timedelta(hours=1))
        self.assertEqual(pool.next_free(NOW), reset, "every account held: the earliest reset")

    async def test_a_server_limit_changes_nothing_on_the_account(self) -> None:
        pool = Pool("TOKEN", ("a",), share=4)
        await pool.acquire()
        pool.limited(0, KIND_SERVER, until=NOW + timedelta(hours=1))
        self.assertEqual((pool.accounts[0].cap, pool.accounts[0].held_until), (4, None))

    async def test_a_hold_lifts_by_the_clock_without_a_release(self) -> None:
        pool = Pool("TOKEN", ("a",), share=1)
        pool.limited(0, KIND_USAGE, until=pool_module._now() + timedelta(seconds=0.3))
        started = asyncio.get_running_loop().time()
        self.assertEqual(await asyncio.wait_for(pool.acquire(), 5), 0)
        self.assertGreaterEqual(asyncio.get_running_loop().time() - started, 0.25)

    def test_an_empty_pool_or_a_zero_share_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            Pool("TOKEN", (), share=1)
        with self.assertRaises(ValueError):
            Pool("TOKEN", ("a",), share=0)

    def test_jitter_spreads_a_wait_around_its_base(self) -> None:
        with mock.patch.object(pool_module.random, "uniform", return_value=1.5):
            self.assertEqual(pool_module.jitter(timedelta(seconds=30)), timedelta(seconds=45))
        with mock.patch.object(pool_module.random, "uniform", return_value=0.5):
            self.assertEqual(pool_module.jitter(timedelta(seconds=30)), timedelta(seconds=15))


if __name__ == "__main__":
    unittest.main()

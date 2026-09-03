"""A pool hands attempts the least-loaded account, caps what one account
carries, halves that cap once per pause on a throttle, holds a spent
account until its reset, and grows the cap back one valid attempt at a time."""

from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from agent_runner import util
from agent_runner.pool import Pool

NOW = datetime(2026, 8, 26, 19, 44, tzinfo=timezone.utc)


def soon(seconds: float) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


class PoolTest(unittest.IsolatedAsyncioTestCase):
    async def test_attempts_spread_over_the_least_loaded_accounts(self) -> None:
        pool = Pool("TOKEN", ("a", "b", "c"), share=2)
        slots = [await pool.acquire() for _ in range(6)]
        self.assertEqual(sorted(slots), [0, 0, 1, 1, 2, 2], "every account carries its share, none more")
        self.assertEqual(slots[:3], [0, 1, 2], "each new attempt lands on the emptiest account")
        self.assertEqual(pool.env(1), {"TOKEN": "b"})

    async def test_a_full_pool_makes_the_next_attempt_wait_for_a_release(self) -> None:
        pool = Pool("TOKEN", ("only",), share=1)
        async with pool.lease() as first:
            waiter = asyncio.create_task(pool.acquire())
            await asyncio.sleep(0.05)
            self.assertFalse(waiter.done(), "the account is full: the attempt waits, it does not fail")
        self.assertEqual(await asyncio.wait_for(waiter, 2), first)

    async def test_a_throttle_halves_the_cap_to_what_the_account_carried_once_per_pause(self) -> None:
        pool = Pool("TOKEN", ("a",), share=8)
        for _ in range(6):
            await pool.acquire()
        pool.throttle(0, until=soon(60))
        self.assertEqual(pool.accounts[0].cap, 3)
        pool.throttle(0, until=soon(60))
        pool.throttle(0, until=soon(60))
        self.assertEqual(pool.accounts[0].cap, 3, "the sessions already running report the same storm: one halving")
        pool.accounts[0].held_until = None  # the pause is over
        pool.throttle(0, until=soon(60))
        self.assertEqual(pool.accounts[0].cap, 1)
        pool.accounts[0].held_until = None
        pool.throttle(0, until=soon(60))
        self.assertEqual(pool.accounts[0].cap, 1, "the cap never drops below one attempt")

    async def test_a_valid_attempt_grows_the_cap_back_up_to_the_share(self) -> None:
        pool = Pool("TOKEN", ("a",), share=3)
        pool.accounts[0].cap = 1
        for _ in range(3):
            pool.succeeded(0)
        self.assertEqual(pool.accounts[0].cap, 3)

    async def test_a_hold_keeps_the_account_out_and_never_shortens(self) -> None:
        pool = Pool("TOKEN", ("a", "b"), share=4)
        reset = soon(3600)
        pool.hold(0, until=reset)
        self.assertEqual([await pool.acquire() for _ in range(3)], [1, 1, 1], "nothing lands on the held account")
        self.assertEqual(pool.next_free(NOW), NOW, "an account is free: a retry may run now")
        pool.throttle(0, until=soon(30))
        self.assertEqual(pool.accounts[0].held_until, reset, "a shorter pause never cuts a longer hold")
        pool.hold(1, until=reset + timedelta(hours=1))
        self.assertEqual(pool.next_free(NOW), reset, "every account held: the earliest reset")

    async def test_a_hold_lifts_by_the_clock_without_a_release(self) -> None:
        pool = Pool("TOKEN", ("a",), share=1)
        pool.hold(0, until=soon(0.3))
        started = asyncio.get_running_loop().time()
        self.assertEqual(await asyncio.wait_for(pool.acquire(), 5), 0)
        self.assertGreaterEqual(asyncio.get_running_loop().time() - started, 0.25)

    def test_an_empty_pool_or_a_zero_share_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            Pool("TOKEN", (), share=1)
        with self.assertRaises(ValueError):
            Pool("TOKEN", ("a",), share=0)

    def test_jitter_spreads_a_wait_around_its_base(self) -> None:
        with mock.patch.object(util.random, "uniform", return_value=1.5):
            self.assertEqual(util.jitter(timedelta(seconds=30)), timedelta(seconds=45))
        with mock.patch.object(util.random, "uniform", return_value=0.5):
            self.assertEqual(util.jitter(timedelta(seconds=30)), timedelta(seconds=15))


if __name__ == "__main__":
    unittest.main()

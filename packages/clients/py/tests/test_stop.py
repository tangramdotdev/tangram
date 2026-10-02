import asyncio
import unittest

from tangram.resolve import resolve
from tangram.stop import Stop


class StopTests(unittest.TestCase):
    def test_promise_is_stable_and_can_be_created_outside_event_loop(self):
        stop = Stop()
        self.assertIs(stop.promise, stop.promise)
        stop.stop()
        stop.stop()
        self.assertIs(stop.promise, stop.promise)


class StopAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_completes_all_waiters_and_can_be_awaited_repeatedly(self):
        stop = Stop()
        promise = stop.promise
        first = asyncio.ensure_future(promise)
        second = asyncio.ensure_future(promise)
        await asyncio.sleep(0)
        self.assertFalse(first.done())
        self.assertFalse(second.done())
        stop.stop()
        self.assertIsNone(await first)
        self.assertIsNone(await second)
        self.assertIsNone(await promise)
        self.assertIsNone(await promise)
        self.assertIs(stop.promise, promise)
        stop.stop()

    async def test_stopping_before_waiting_keeps_promise_resolved(self):
        stop = Stop()
        stop.stop()
        self.assertIsNone(await stop.promise)
        self.assertIsNone(await stop.promise)

    async def test_canceling_one_waiter_does_not_cancel_the_signal(self):
        stop = Stop()
        canceled = asyncio.ensure_future(stop.promise)
        surviving = asyncio.ensure_future(stop.promise)
        await asyncio.sleep(0)
        canceled.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await canceled
        self.assertFalse(surviving.done())
        stop.stop()
        self.assertIsNone(await surviving)
        self.assertIsNone(await stop.promise)

    async def test_promise_works_with_recursive_resolution(self):
        stop = Stop()
        task = asyncio.create_task(resolve([stop.promise, {"same": stop.promise}]))
        await asyncio.sleep(0)
        stop.stop()
        self.assertEqual(await task, [None, {"same": None}])

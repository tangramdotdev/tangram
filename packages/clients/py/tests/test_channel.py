"""Verify the process channel matches the JavaScript channel contract."""

import unittest

from tangram.process.connect.channel import Channel


class ChannelTests(unittest.IsolatedAsyncioTestCase):
    async def test_independent_capacity_and_priority_order(self):
        channel = Channel(2)
        for value in (1, 2):
            self.assertTrue(channel.push(value))
        for value in (3, 4):
            self.assertTrue(channel.push(value, True))
        for priority in (False, True):
            with self.assertRaisesRegex(RuntimeError, "message queue is full"):
                channel.push(5, priority)
        self.assertEqual(
            [(await channel.next())["value"] for _ in range(4)], [3, 4, 1, 2]
        )

    async def test_waiters_are_registered_immediately_and_served_in_order(self):
        channel = Channel(0)
        first, second = channel.next(), channel.next()
        self.assertTrue(channel.push(1))
        self.assertTrue(channel.push(2, True))
        self.assertEqual(await first, {"done": False, "value": 1})
        self.assertEqual(await second, {"done": False, "value": 2})

    async def test_close_wakes_waiters_and_drains_queued_values(self):
        channel = Channel(1)
        pending = channel.next()
        channel.close()
        self.assertEqual(await pending, {"done": True, "value": None})
        self.assertFalse(channel.push(1))
        channel = Channel(1)
        channel.push(1)
        channel.close()
        self.assertEqual(await channel.next(), {"done": False, "value": 1})
        self.assertEqual(await channel.next(), {"done": True, "value": None})

    async def test_error_wakes_waiters_after_draining_queued_values(self):
        error = RuntimeError("closed")
        channel = Channel(1)
        pending = channel.next()
        channel.close(error)
        with self.assertRaises(RuntimeError) as raised:
            await pending
        self.assertIs(raised.exception, error)
        channel = Channel(1)
        channel.push(1)
        channel.close(error)
        self.assertEqual((await channel.next())["value"], 1)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            await channel.next()

    async def test_return_and_python_iterator(self):
        channel = Channel(2)
        channel.push(1)
        channel.push(2)
        self.assertIs(channel.__aiter__(), channel)
        self.assertEqual(await channel.return_(), {"done": True, "value": None})
        self.assertEqual([value async for value in channel], [1, 2])

    async def test_cancelled_await_does_not_cancel_javascript_style_waiter(self):
        channel = Channel(0)
        pending = channel.next()
        pending.cancel()
        self.assertTrue(channel.push(1))
        channel.close()

    async def test_priority_nullish_falls_through_to_ordinary_queue(self):
        channel = Channel(1)
        channel.push(None, True)
        channel.push(None)
        self.assertEqual(await channel.next(), {"done": False, "value": None})

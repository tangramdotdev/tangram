import asyncio
import unittest

from tangram.queue import Queue


class QueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_pump_buffers_values_before_completion(self):
        async def input_():
            for value in range(3):
                yield value

        queue = Queue(input_())
        await queue._task
        for value in range(3):
            self.assertEqual(await queue.next(), {"done": False, "value": value})
        self.assertEqual(await queue.next(), {"done": True, "value": None})
        self.assertEqual(await queue.next(), {"done": True, "value": None})

    async def test_buffered_values_precede_input_error(self):
        error = ValueError("input failed")

        async def input_():
            yield 1
            raise error

        queue = Queue(input_())
        await queue._task
        self.assertEqual(await queue.next(), {"done": False, "value": 1})
        for _ in range(2):
            with self.assertRaises(ValueError) as raised:
                await queue.next()
            self.assertIs(raised.exception, error)

    async def test_waiters_receive_values_and_error_in_order(self):
        ready = asyncio.Event()

        async def input_():
            await ready.wait()
            yield 1
            yield 2
            raise ValueError("failed")

        queue = Queue(input_())
        first, second, third = [queue.next() for _ in range(3)]
        ready.set()
        self.assertEqual((await first)["value"], 1)
        self.assertEqual((await second)["value"], 2)
        with self.assertRaisesRegex(ValueError, "failed"):
            await third

    async def test_stopping_one_waiter_does_not_stop_input(self):
        ready = asyncio.Event()

        async def input_():
            await ready.wait()
            yield 5

        queue = Queue(input_())
        stop = asyncio.get_running_loop().create_future()
        first, second = queue.next(stop), queue.next()
        stop.set_result(None)
        self.assertEqual(await first, {"done": True, "value": None})
        ready.set()
        self.assertEqual(await second, {"done": False, "value": 5})
        self.assertEqual(await queue.next(), {"done": True, "value": None})

    async def test_cancelled_waiter_does_not_consume_value(self):
        ready = asyncio.Event()

        async def input_():
            await ready.wait()
            yield 5

        queue = Queue(input_())
        first = queue.next()
        first.cancel()
        ready.set()
        await queue._task
        self.assertEqual(await queue.next(), {"done": False, "value": 5})

    async def test_failed_stop_does_not_complete_waiter(self):
        ready = asyncio.Event()

        async def input_():
            await ready.wait()
            yield 5

        queue = Queue(input_())
        stop = asyncio.get_running_loop().create_future()
        next_ = queue.next(stop)
        stop.set_exception(ValueError("stop failed"))
        await asyncio.sleep(0)
        self.assertFalse(next_.done())
        ready.set()
        self.assertEqual((await next_)["value"], 5)

    async def test_async_iteration(self):
        async def input_():
            yield 1
            yield 2

        self.assertEqual([value async for value in Queue(input_())], [1, 2])

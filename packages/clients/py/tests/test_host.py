import asyncio
import os
import signal
import unittest

from tangram import host


class Host(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_read_does_not_consume_future_input(self):
        reader, writer = os.pipe()
        stopper = host.stopper_open()
        try:
            pending = asyncio.create_task(host.read(reader, 10, stopper))
            await asyncio.sleep(0)
            await host.stopper_stop(stopper)
            with self.assertRaisesRegex(RuntimeError, "the operation was stopped"):
                await pending
            os.write(writer, b"abc")
            self.assertEqual(await host.read(reader, 10), b"abc")
        finally:
            os.close(reader)
            os.close(writer)

    async def test_spawn_pipe_io_and_wait_by_pid(self):
        child = await host.spawn(
            {
                "executable": "/bin/sh",
                "args": ["-c", "read line; printf '%s' \"$line\""],
                "stdin": "pipe",
                "stdout": "pipe",
                "stderr": "null",
            }
        )
        await host.write(child.stdin, b"hello\n")
        await host.close(child.stdin)
        self.assertEqual(await host.read(child.stdout), b"hello")
        self.assertEqual(await host.wait(child.pid), {"exit": 0})

    async def test_signaled_exit(self):
        child = await host.spawn(
            "/bin/sh", ["-c", "kill -TERM $$"], stdout="null", stderr="null"
        )
        self.assertEqual(await child.wait(), {"exit": 128 + signal.SIGTERM})

    async def test_stopped_wait_kills_child(self):
        child = await host.spawn(
            "/bin/sh", ["-c", "sleep 0.1"], stdout="null", stderr="null"
        )
        stopper = host.stopper_open()
        await host.stopper_stop(stopper)
        self.assertEqual(
            await host.wait(child.pid, stopper), {"exit": 128 + signal.SIGKILL}
        )
        with self.assertRaises(ValueError):
            await host.wait(child.pid)

    async def test_independent_signal_listener_lifetimes(self):
        previous = signal.getsignal(signal.SIGWINCH)
        first = host.listen_signal("sigwinch")
        second = host.listen_signal("sigwinch")
        try:
            os.kill(os.getpid(), signal.SIGWINCH)
            await asyncio.wait_for(anext(first), 1)
            await asyncio.wait_for(anext(second), 1)
            await first.close()
            os.kill(os.getpid(), signal.SIGWINCH)
            await asyncio.wait_for(anext(second), 1)
            pending = [asyncio.create_task(anext(second)) for _ in range(2)]
            await asyncio.sleep(0)
            await second.close()
            results = await asyncio.gather(*pending, return_exceptions=True)
            self.assertTrue(
                all(isinstance(result, StopAsyncIteration) for result in results)
            )
        finally:
            await first.close()
            await second.close()
        self.assertIs(signal.getsignal(signal.SIGWINCH), previous)

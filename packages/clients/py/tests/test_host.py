import asyncio
import os
import signal
import unittest

from tangram import host


class Host(unittest.IsolatedAsyncioTestCase):
    async def test_magic_uses_the_defining_module_and_export_identity(self):
        from tangram.module import Module
        from tangram.referent import Referent

        module = Module("py", Referent("/test/main.tg.py"))
        namespace = {"__tangram_module__": module}
        exec("def original(): return 42\nalias = original", namespace)
        function = namespace["original"]
        self.assertEqual(
            host.magic(function), {"module": module.to_data(), "export": "original"}
        )
        del namespace["original"]
        self.assertEqual(host.magic(function)["export"], "alias")
        del namespace["alias"]
        with self.assertRaisesRegex(ValueError, "find an export"):
            host.magic(function)
        with self.assertRaisesRegex(ValueError, "Tangram module"):
            host.magic(lambda: None)
        with self.assertRaisesRegex(TypeError, "python function"):
            host.magic(object())

    async def test_magic_preserves_the_exported_decorated_callable(self):
        import functools

        from tangram.module import Module
        from tangram.referent import Referent

        namespace = {"__tangram_module__": Module("py", Referent("/test/main.tg.py"))}
        exec("def function(value): return value", namespace)
        original = namespace["function"]
        namespace["function"] = functools.cache(original)
        self.assertEqual(host.magic(namespace["function"])["export"], "function")
        with self.assertRaisesRegex(ValueError, "find an export"):
            host.magic(original)

    async def test_magic_uses_the_exported_wrapper_instead_of_the_wrapped_function(
        self,
    ):
        from tangram.module import Module
        from tangram.referent import Referent

        helper = {"__tangram_module__": Module("py", Referent("/test/helper.tg.py"))}
        exec("def original(): return 42", helper)
        module = Module("py", Referent("/test/main.tg.py"))
        namespace = {
            "__tangram_module__": module,
            "original": helper["original"],
        }
        exec(
            "import functools\n@functools.wraps(original)\n"
            "def wrapped(): return original() + 1",
            namespace,
        )
        self.assertEqual(namespace["wrapped"](), 43)
        self.assertEqual(
            host.magic(namespace["wrapped"]),
            {"module": module.to_data(), "export": "wrapped"},
        )

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

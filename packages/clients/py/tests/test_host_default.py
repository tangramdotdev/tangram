import asyncio
import importlib
import os
import signal
import tempfile
import unittest
from pathlib import Path

from tangram import host
from tangram.host import default


class HostDefaultTests(unittest.IsolatedAsyncioTestCase):
    async def test_source_layout_and_host_replacement(self):
        self.assertEqual(default.spawn.__module__, "tangram.host.default")
        original = host.read_file

        async def replacement(path):
            return path.encode()

        try:
            host.set_host({"read_file": replacement})
            self.assertIs(importlib.import_module("tangram.host"), host)
            self.assertEqual(await host.read_file("replacement"), b"replacement")
            self.assertIs(default.read_file, original)
        finally:
            host.set_host({"read_file": original})

    async def test_read_file_and_force_recursive_remove(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "nested"
            directory.mkdir()
            path = directory / "file"
            path.write_bytes(b"\x00content\xff")
            self.assertEqual(await default.read_file(str(path)), b"\x00content\xff")
            await default.remove(str(directory))
            self.assertFalse(directory.exists())
            await default.remove(str(directory))

    async def test_explicit_path_resolution(self):
        with self.assertRaisesRegex(FileNotFoundError, "failed to find sh in PATH"):
            await default.spawn({"executable": "sh", "env": {}})
        child = await default.spawn(
            {
                "executable": "sh",
                "args": ["-c", "exit 3"],
                "env": {"PATH": "/bin"},
                "stdin": "null",
                "stdout": "null",
                "stderr": "null",
            }
        )
        self.assertEqual(await default.wait(child.pid), {"exit": 3})

    async def test_awaitable_stopper_stops_wait_with_signal_exit(self):
        stopper = await default.stopper_open()
        child = await default.spawn(
            "/bin/sh", ["-c", "sleep 60"], stdout="null", stderr="null"
        )
        pending = asyncio.create_task(default.wait(child.pid, stopper))
        await default.stopper_stop(stopper)
        self.assertEqual(
            await asyncio.wait_for(pending, 2), {"exit": 128 + signal.SIGKILL}
        )
        with self.assertRaises(ValueError):
            await default.wait(child.pid)
        await default.stopper_close(stopper)

    async def test_named_signal(self):
        child = await default.spawn(
            "/bin/sh", ["-c", "sleep 60"], stdout="null", stderr="null"
        )
        await default.signal(child.pid, "TERM")
        self.assertEqual(await default.wait(child.pid), {"exit": 128 + signal.SIGTERM})

    def test_parallelism_is_number(self):
        self.assertIsInstance(default.parallelism, int)
        self.assertGreaterEqual(default.parallelism, 1)
        self.assertGreaterEqual(os.cpu_count() or 1, 1)

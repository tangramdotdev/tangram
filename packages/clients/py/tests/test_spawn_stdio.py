"""Verify source-aligned spawn forwarding and stream ownership."""

import asyncio
import importlib
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tangram.process.spawn import spawn_sandboxed
from tangram.referent import Referent

spawn = importlib.import_module("tangram.process.spawn")
stdio = importlib.import_module("tangram.process.stdio")
connect = importlib.import_module("tangram.process.connect")


class SpawnStdioTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = object()
        self.opened = SimpleNamespace(
            initial=SimpleNamespace(
                output={
                    "process": "prc_test",
                    "outcome": {"exit": {"code": 0}},
                    "tokens": {},
                }
            )
        )

    async def test_inherited_output_shares_one_ordered_read(self):
        task = AsyncMock()
        with (
            patch.object(spawn.host, "is_tty", return_value=False),
            patch.object(
                spawn.host, "is_foreground_controlling_tty", return_value=False
            ),
            patch.object(
                connect.Connection, "open", AsyncMock(return_value=self.opened)
            ) as open_,
            patch.object(stdio, "task", task),
        ):
            process = await spawn_sandboxed(
                {"command": Referent({}), "sandbox": {}},
                mode="run",
                client=self.client,
            )
            await process._stdio_promise
        self.assertEqual(
            open_.call_args.kwargs["reads"],
            [
                {"streams": ["stdout", "stderr"]},
            ],
        )
        self.assertEqual(task.call_args.args[3:7], ("pipe", "pipe", "pipe", False))
        self.assertIs(task.call_args.kwargs["client"], self.client)
        self.assertFalse(process.stdin.available)
        self.assertFalse(process.stdout.available)
        self.assertFalse(process.stderr.available)
        self.assertIs(process.connection, self.opened)

    async def test_explicit_pipes_are_provided_without_forwarding(self):
        task = AsyncMock()
        with (
            patch.object(spawn.host, "is_tty", return_value=False),
            patch.object(
                spawn.host, "is_foreground_controlling_tty", return_value=False
            ),
            patch.object(
                connect.Connection, "open", AsyncMock(return_value=self.opened)
            ) as open_,
            patch.object(stdio, "task", task),
        ):
            process = await spawn_sandboxed(
                {
                    "command": Referent({}),
                    "sandbox": {},
                    "stdin": "pipe",
                    "stdout": "pipe",
                    "stderr": "pipe",
                },
                mode="run",
                client=self.client,
            )
            await asyncio.sleep(0)
        self.assertEqual(
            open_.call_args.kwargs["reads"],
            [
                {"streams": ["stdout"]},
                {"streams": ["stderr"]},
            ],
        )
        task.assert_not_called()
        self.assertIsNone(process._stdio_promise)
        self.assertTrue(process.stdin.available)
        self.assertTrue(process.stdout.available)
        self.assertTrue(process.stderr.available)

    async def test_background_tty_input_is_not_forwarded(self):
        task = AsyncMock()
        with (
            patch.object(spawn.host, "is_tty", return_value=True),
            patch.object(
                spawn.host, "is_foreground_controlling_tty", return_value=False
            ),
            patch.object(
                connect.Connection, "open", AsyncMock(return_value=self.opened)
            ) as open_,
            patch.object(stdio, "task", task),
        ):
            process = await spawn_sandboxed(
                {
                    "command": Referent({}),
                    "sandbox": {},
                    "stdout": "null",
                    "stderr": "null",
                },
                mode="run",
                client=self.client,
            )
        self.assertEqual(open_.call_args.args[0]["stdin"], "null")
        task.assert_not_called()
        self.assertIsNone(process._stdio_promise)

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tangram.process.connect import Connection, connect, spawn


class ConnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_distinguishes_absent_sandbox(self):
        local = AsyncMock(return_value="local")
        sandboxed = AsyncMock(return_value="sandboxed")
        with (
            patch("tangram.process.spawn.spawn_unsandboxed", local),
            patch("tangram.process.spawn.spawn_sandboxed", sandboxed),
        ):
            options = {"tokens": {}}
            self.assertEqual(await spawn({}, options, "run"), "local")
            self.assertEqual(
                await spawn({"sandbox": None}, options, "run"), "sandboxed"
            )
            local.assert_awaited_once_with({}, options, client=None)
            sandboxed.assert_awaited_once_with(
                {"sandbox": None}, options, "run", client=None
            )

    async def test_connect_passes_referrer_options_and_reads(self):
        open = AsyncMock(return_value="process")
        options = {"lease": "lease", "reads": [{"streams": ["stdout"]}]}
        with patch("tangram.process.Process.connect", open):
            self.assertEqual(await connect("prc_test", options), "process")
        open.assert_awaited_once_with("prc_test", client=None, **options)

    async def test_controls_are_not_replayed(self):
        session = SimpleNamespace(
            _closed=False,
            _error=None,
            signal=AsyncMock(side_effect=ConnectionError("disconnected")),
            cancel=AsyncMock(),
            tty=AsyncMock(),
        )
        connection = Connection(session)
        with self.assertRaises(ConnectionError):
            await connection.signal({"signal": "TERM"})
        session.signal.assert_awaited_once_with({"signal": "TERM"})
        await connection.cancel({"lease": "lease"})
        await connection.tty({"size": {"rows": 40, "cols": 80}})
        session.cancel.assert_awaited_once_with({"lease": "lease"})

    async def test_reconnect_rejects_unsandboxed_process(self):
        session = SimpleNamespace(_closed=True, _error=None, output={"process": 10})
        with self.assertRaisesRegex(ValueError, "expected a sandboxed process"):
            await Connection(session).ensure_session()

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import tangram as tg
from tangram.sleep import sleep


class SleepTests(unittest.IsolatedAsyncioTestCase):
    async def test_duration_is_forwarded_in_seconds_without_conversion(self):
        for duration in (0, 0.125, 2):
            with self.subTest(duration=duration):
                delegate = AsyncMock(return_value=None)
                with patch.object(tg.host, "sleep", delegate):
                    self.assertIsNone(await sleep(duration))
                delegate.assert_awaited_once_with(duration)

    async def test_public_export_reads_the_current_host_when_awaited(self):
        original = tg.host.sleep
        first = AsyncMock()
        second = AsyncMock(return_value="host result")
        try:
            tg.set_host({"sleep": first})
            pending = tg.sleep(0.5)
            first.assert_not_called()
            tg.set_host({"sleep": second})
            self.assertEqual(await pending, "host result")
            first.assert_not_called()
            second.assert_awaited_once_with(0.5)
        finally:
            tg.set_host({"sleep": original})

    async def test_host_failure_is_propagated_unchanged(self):
        error = ValueError("invalid duration")
        with patch.object(tg.host, "sleep", AsyncMock(side_effect=error)):
            with self.assertRaises(ValueError) as caught:
                await sleep(-1)
        self.assertIs(caught.exception, error)

    async def test_cancellation_reaches_host_sleep(self):
        started = asyncio.Event()
        canceled = asyncio.Event()

        async def delegate(duration):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                canceled.set()

        with patch.object(tg.host, "sleep", delegate):
            task = asyncio.create_task(sleep(1))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(canceled.is_set())

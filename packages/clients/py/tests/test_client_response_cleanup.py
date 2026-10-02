"""Release completed and missing endpoint responses without draining their bodies."""

import unittest
from unittest.mock import AsyncMock, Mock

from tangram.client.process.cancel import try_cancel_process
from tangram.client.process.get import try_get_process
from tangram.client.process.signal import try_signal_process
from tangram.client.process.tty.put import try_set_process_tty_size
from tangram.client.read import try_read_stream
from tangram.client.sandbox.destroy import try_destroy_sandbox
from tangram.client.sandbox.get import try_get_sandbox
from tangram.http import Response


class EndpointResponseCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_responses_are_released(self):
        operations = [
            (try_cancel_process, {"lease": "lease"}),
            (try_get_process, {}),
            (try_signal_process, {"signal": "sigterm"}),
            (try_set_process_tty_size, {"size": {"cols": 80, "rows": 24}}),
            (try_get_sandbox, {}),
            (try_destroy_sandbox, {}),
            (try_read_stream, {}),
        ]
        for operation, arg in operations:
            with self.subTest(operation=operation.__name__):
                close = Mock()
                response = Response(404, close=close)
                client = Mock(send_with_retry=AsyncMock(return_value=response))
                result = (
                    await operation(client, {"blob": "id"})
                    if operation is try_read_stream
                    else await operation(client, "id", arg)
                )
                self.assertIsNone(result)
                close.assert_called_once_with()

    async def test_bodyless_successes_and_conflicts_are_released(self):
        operations = [
            (try_signal_process, {"signal": "sigterm"}, 204, True),
            (try_set_process_tty_size, {"size": {"cols": 80, "rows": 24}}, 204, True),
            (try_destroy_sandbox, {}, 204, True),
            (try_destroy_sandbox, {}, 409, False),
        ]
        for operation, arg, status, expected in operations:
            with self.subTest(operation=operation.__name__, status=status):
                close = Mock()
                response = Response(status, close=close)
                client = Mock(send_with_retry=AsyncMock(return_value=response))
                self.assertEqual(await operation(client, "id", arg), expected)
                close.assert_called_once_with()

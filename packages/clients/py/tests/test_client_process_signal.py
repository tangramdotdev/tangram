"""Check signal JSON requests and source response handling."""

import unittest
from unittest.mock import AsyncMock, Mock

from tangram.client.process.signal import Signal, signal_process, try_signal_process
from tangram.error import Error
from tangram.http import Body, Response


class ProcessSignalTests(unittest.IsolatedAsyncioTestCase):
    def client(self, response):
        client = Mock()
        client.send_with_retry = AsyncMock(return_value=response)
        return client

    async def test_request_encodes_location_in_json_and_preserves_fields(self):
        response = Response(204)
        response.collect = AsyncMock(side_effect=AssertionError("must not collect"))
        client = self.client(response)
        self.assertTrue(
            await try_signal_process(
                client,
                "id /?",
                {
                    "signal": "TERM",
                    "location": {
                        "components": [{"name": "upstream", "regions": ["west"]}]
                    },
                    "tokens": {"token": True},
                    "extra": True,
                },
            )
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.uri.path, "/processes/id%20%2F%3F/signal")
        self.assertIsNone(request.uri.query)
        self.assertEqual(
            request.headers.to_data(), {"content-type": "application/json"}
        )
        self.assertEqual(
            await request.body.json(),
            {
                "signal": "TERM",
                "location": "remote:upstream(west)",
                "tokens": {"token": True},
                "extra": True,
            },
        )
        response.collect.assert_not_awaited()

    async def test_null_location_is_explicit_and_tokens_are_not_defaulted(self):
        client = self.client(Response(200))
        self.assertIsNone(await signal_process(client, "id", signal="INT"))
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(await request.body.json(), {"signal": "INT", "location": None})
        self.assertEqual(Signal.Arg.__required_keys__, frozenset({"signal"}))

    async def test_missing_and_http_error(self):
        client = self.client(Response(404))
        self.assertIsNone(await try_signal_process(client, "id", signal="TERM"))
        with self.assertRaisesRegex(ValueError, "failed to find the process"):
            await signal_process(client, "id", signal="TERM")
        client = self.client(Response(500, body=Body.json({"message": "failure"})))
        with self.assertRaises(Error) as caught:
            await try_signal_process(client, "id", signal="TERM")
        self.assertEqual(await caught.exception.message(), "failure")

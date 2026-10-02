"""Check cancellation request fields and missing/error semantics."""

import unittest
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs

from tangram.client.process.cancel import cancel_process, try_cancel_process
from tangram.error import Error
from tangram.http import Body, Response


class ProcessCancelTests(unittest.IsolatedAsyncioTestCase):
    def client(self, status=200, output=None):
        client = Mock()
        client.send_with_retry = AsyncMock(
            return_value=Response(status, body=Body.json(output or {"released": False}))
        )
        return client

    async def test_request_percent_encodes_id_and_uses_only_cancel_fields(self):
        client = self.client(output={"released": True})
        output = await cancel_process(
            client,
            "id /?",
            {
                "lease": "lease",
                "location": {"components": [{"name": "upstream", "regions": ["west"]}]},
                "extra": "ignored",
            },
        )
        self.assertEqual(output, {"released": True})
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.uri.path, "/processes/id%20%2F%3F/cancel")
        self.assertEqual(request.headers.to_data(), {})
        self.assertEqual(
            parse_qs(request.uri.query),
            {
                "lease": ["lease"],
                "location": ["remote:upstream(west)"],
            },
        )

    async def test_absent_and_null_location_are_omitted(self):
        for options in [{"lease": "lease"}, {"lease": "lease", "location": None}]:
            client = self.client()
            self.assertEqual(
                await try_cancel_process(client, "id", options), {"released": False}
            )
            self.assertEqual(
                parse_qs(client.send_with_retry.call_args.args[0].uri.query),
                {"lease": ["lease"]},
            )

    async def test_missing_and_http_errors(self):
        client = self.client(404)
        self.assertIsNone(await try_cancel_process(client, "missing", lease="lease"))
        with self.assertRaisesRegex(ValueError, "failed to find the process"):
            await cancel_process(client, "missing", lease="lease")
        for status in [199, 300, 503]:
            client = self.client(status, {"message": "failed"})
            with self.assertRaises(Error) as caught:
                await try_cancel_process(client, "id", lease="lease")
            self.assertEqual(await caught.exception.message(), "failed")

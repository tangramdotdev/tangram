"""Check TTY resize request fields and response handling."""

import unittest
from unittest.mock import AsyncMock, Mock

from tangram.client.process.tty.put import (
    set_process_tty_size,
    try_set_process_tty_size,
)
from tangram.error import Error
from tangram.http import Body, Response


class ProcessTtyPutTests(unittest.IsolatedAsyncioTestCase):
    def client(self, response):
        client = Mock()
        client.send_with_retry = AsyncMock(return_value=response)
        return client

    async def test_json_request_preserves_size_tokens_and_extra_fields(self):
        response = Response(204)
        response.collect = AsyncMock(side_effect=AssertionError("must not collect"))
        client = self.client(response)
        self.assertTrue(
            await try_set_process_tty_size(
                client,
                "id /?",
                {
                    "size": {"cols": 80, "rows": 24},
                    "location": {
                        "components": [{"name": "upstream", "regions": ["west"]}]
                    },
                    "tokens": {"token": True},
                    "extra": True,
                },
            )
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "PUT")
        self.assertEqual(request.uri.path, "/processes/id%20%2F%3F/tty/size")
        self.assertIsNone(request.uri.query)
        self.assertEqual(
            request.headers.to_data(), {"content-type": "application/json"}
        )
        self.assertEqual(
            await request.body.json(),
            {
                "size": {"cols": 80, "rows": 24},
                "location": "remote:upstream(west)",
                "tokens": {"token": True},
                "extra": True,
            },
        )
        response.collect.assert_not_awaited()

    async def test_missing_location_serializes_as_null_and_set_returns_none(self):
        client = self.client(Response(200))
        self.assertIsNone(
            await set_process_tty_size(client, "id", size={"cols": 0, "rows": 0})
        )
        self.assertEqual(
            await client.send_with_retry.call_args.args[0].body.json(),
            {
                "size": {"cols": 0, "rows": 0},
                "location": None,
            },
        )

    async def test_missing_and_non_success_statuses(self):
        client = self.client(Response(404))
        self.assertIsNone(
            await try_set_process_tty_size(client, "id", size={"cols": 80, "rows": 24})
        )
        with self.assertRaisesRegex(ValueError, "failed to find the process"):
            await set_process_tty_size(client, "id", size={"cols": 80, "rows": 24})
        for status in [199, 300, 503]:
            client = self.client(
                Response(status, body=Body.json({"message": "failure"}))
            )
            with self.assertRaises(Error) as caught:
                await try_set_process_tty_size(
                    client, "id", size={"cols": 80, "rows": 24}
                )
            self.assertEqual(await caught.exception.message(), "failure")

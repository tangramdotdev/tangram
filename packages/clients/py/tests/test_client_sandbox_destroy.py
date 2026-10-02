"""Check sandbox destruction request and missing/conflict semantics."""

import unittest
from unittest.mock import AsyncMock, Mock

from tangram.client.sandbox.destroy import destroy_sandbox, try_destroy_sandbox
from tangram.error import Error
from tangram.http import Body, Response


class SandboxDestroyTests(unittest.IsolatedAsyncioTestCase):
    def client(self, response):
        client = Mock()
        client.send_with_retry = AsyncMock(return_value=response)
        return client

    async def test_request_selects_only_location_and_encodes_id(self):
        response = Response(204)
        response.collect = AsyncMock(side_effect=AssertionError("must not collect"))
        client = self.client(response)
        self.assertIsNone(
            await destroy_sandbox(
                client,
                "id /?",
                {
                    "location": {
                        "components": [{"name": "upstream", "regions": ["west"]}]
                    },
                    "extra": True,
                },
            )
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.uri.path, "/sandboxes/id%20%2F%3F/destroy")
        self.assertIsNone(request.uri.query)
        self.assertEqual(
            request.headers.to_data(), {"content-type": "application/json"}
        )
        self.assertEqual(
            await request.body.json(), {"location": "remote:upstream(west)"}
        )
        response.collect.assert_not_awaited()

    async def test_default_and_null_location_emit_null(self):
        for arg in [None, {"location": None}]:
            client = self.client(Response(200))
            self.assertTrue(await try_destroy_sandbox(client, "id", arg))
            self.assertEqual(
                await client.send_with_retry.call_args.args[0].body.json(),
                {"location": None},
            )

    async def test_missing_and_already_destroyed_are_distinct(self):
        client = self.client(Response(404))
        self.assertIsNone(await try_destroy_sandbox(client, "id"))
        with self.assertRaisesRegex(ValueError, "failed to find the sandbox"):
            await destroy_sandbox(client, "id")
        client = self.client(Response(409))
        self.assertFalse(await try_destroy_sandbox(client, "id"))
        with self.assertRaisesRegex(ValueError, "the sandbox was already destroyed"):
            await destroy_sandbox(client, "id")

    async def test_other_non_success_statuses_raise_source_error(self):
        for status in [199, 300, 503]:
            client = self.client(
                Response(status, body=Body.json({"message": "failure"}))
            )
            with self.assertRaises(Error) as caught:
                await try_destroy_sandbox(client, "id")
            self.assertEqual(await caught.exception.message(), "failure")

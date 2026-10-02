"""Check sandbox retrieval request and location handling."""

import unittest
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs

from tangram.client.sandbox.get import get_sandbox, try_get_sandbox
from tangram.error import Error
from tangram.http import Body, Response


class SandboxGetTests(unittest.IsolatedAsyncioTestCase):
    def client(self, status=200, output=None, headers=None):
        client = Mock()
        client.send_with_retry = AsyncMock(
            return_value=Response(status, headers, Body.json(output or {}))
        )
        return client

    async def test_default_request_and_nullish_defaults(self):
        for arg in [None, {"source": None, "tokens": None, "location": None}]:
            client = self.client(output={"id": "sandbox"})
            self.assertEqual(await get_sandbox(client, "id /?", arg), {"id": "sandbox"})
            request = client.send_with_retry.call_args.args[0]
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.uri.path, "/sandboxes/id%20%2F%3F")
            self.assertEqual(request.headers.to_data(), {"accept": "application/json"})
            self.assertEqual(parse_qs(request.uri.query), {"source": ["auto"]})

    async def test_explicit_request_fields_are_selected_and_locations_encoded(self):
        client = self.client()
        await try_get_sandbox(
            client,
            "id",
            {
                "source": "remote",
                "tokens": {"token": True},
                "location": {"components": [{"name": "upstream", "regions": ["west"]}]},
                "extra": True,
            },
        )
        self.assertEqual(
            parse_qs(client.send_with_retry.call_args.args[0].uri.query),
            {
                "source": ["remote"],
                "tokens[token]": ["true"],
                "location": ["remote:upstream(west)"],
            },
        )

    async def test_output_location_decodes_without_metadata_header_override(self):
        client = self.client(
            output={
                "id": "sandbox",
                "location": "remote:upstream(west)",
                "metadata": "body",
            },
            headers={"x-tg-process-metadata": '{"value": "header"}'},
        )
        self.assertEqual(
            await get_sandbox(client, "id"),
            {
                "id": "sandbox",
                "location": {"name": "upstream", "region": "west"},
                "metadata": "body",
            },
        )
        client = self.client(output={"location": None})
        self.assertEqual(await get_sandbox(client, "id"), {"location": None})

    async def test_missing_and_http_error(self):
        client = self.client(404)
        self.assertIsNone(await try_get_sandbox(client, "id"))
        with self.assertRaisesRegex(ValueError, "failed to find the sandbox"):
            await get_sandbox(client, "id")
        for status in [199, 300, 503]:
            client = self.client(status, {"message": "failure"})
            with self.assertRaises(Error) as caught:
                await try_get_sandbox(client, "id")
            self.assertEqual(await caught.exception.message(), "failure")

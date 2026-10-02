"""Check sandbox creation sends source data unchanged and decodes locations."""

import unittest
from unittest.mock import AsyncMock, Mock

from tangram.client.sandbox.create import create_sandbox
from tangram.error import Error
from tangram.http import Body, Response


class SandboxCreateTests(unittest.IsolatedAsyncioTestCase):
    def client(self, status=200, output=None):
        client = Mock()
        client.send_with_retry = AsyncMock(
            return_value=Response(status, body=Body.json(output or {}))
        )
        return client

    async def test_json_arg_is_transmitted_without_rewriting_locations(self):
        client = self.client(output={"id": "sandbox_id", "tokens": {"token": True}})
        arg = {
            "data": {"mounts": [{"source": "file_id", "target": "/mount"}]},
            "location": "remote:upstream(west)",
            "tokens": None,
            "extra": True,
        }
        self.assertEqual(
            await create_sandbox(client, arg),
            {"id": "sandbox_id", "tokens": {"token": True}},
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.uri.path, "/sandboxes")
        self.assertIsNone(request.uri.query)
        self.assertEqual(
            request.headers.to_data(),
            {"accept": "application/json", "content-type": "application/json"},
        )
        self.assertEqual(await request.body.json(), arg)

    async def test_string_locations_decode_and_structured_or_null_locations_preserve(
        self,
    ):
        for input_, expected in [
            ("remote:upstream(west)", {"name": "upstream", "region": "west"}),
            ({"region": "west"}, {"region": "west"}),
            (None, None),
        ]:
            client = self.client(output={"id": "sandbox_id", "location": input_})
            self.assertEqual((await create_sandbox(client, {}))["location"], expected)

    async def test_non_success_including_404_raises_source_error(self):
        for status in [199, 300, 404, 503]:
            client = self.client(status, {"message": "failed"})
            with self.assertRaises(Error) as caught:
                await create_sandbox(client, {})
            self.assertEqual(await caught.exception.message(), "failed")

"""Check process storage JSON and response semantics."""

import unittest
from unittest.mock import AsyncMock, Mock

from tangram.client import Client
from tangram.client.process.put import put_process
from tangram.error import Error
from tangram.http import Body, Response


class ProcessPutTests(unittest.IsolatedAsyncioTestCase):
    def client(self, status=200, output=None):
        client = Mock()
        client.send_with_retry = AsyncMock(
            return_value=Response(status, body=Body.json(output or {}))
        )
        return client

    async def test_source_arg_serializes_location_and_preserves_data_and_fields(self):
        client = self.client(output={"tokens": {"token": True}})
        data = {"command": "command_id", "status": "created"}
        arg = {
            "data": data,
            "location": {"components": [{"name": "upstream", "regions": ["west"]}]},
            "extra": True,
        }
        self.assertEqual(
            await put_process(client, "id /?", arg), {"tokens": {"token": True}}
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "PUT")
        self.assertEqual(request.uri.path, "/processes/id%20%2F%3F")
        self.assertIsNone(request.uri.query)
        self.assertEqual(
            request.headers.to_data(),
            {"accept": "application/json", "content-type": "application/json"},
        )
        self.assertEqual(
            await request.body.json(),
            {"data": data, "location": "remote:upstream(west)", "extra": True},
        )
        self.assertIsInstance(arg["location"], dict)

    async def test_facade_expanded_arg_and_raw_data_have_the_same_body(self):
        data = {"command": "command_id"}
        client = Client()
        client.send_with_retry = AsyncMock(
            return_value=Response(200, body=Body.json({}))
        )
        await client.put_process("id", {"data": data})
        self.assertEqual(
            await client.send_with_retry.call_args.args[0].body.json(),
            {"data": data, "location": None},
        )
        client.send_with_retry.return_value = Response(200, body=Body.json({}))
        await client.put_process("id", data)
        self.assertEqual(
            await client.send_with_retry.call_args.args[0].body.json(),
            {"data": data, "location": None},
        )

    async def test_every_non_success_status_including_404_raises_source_error(self):
        for status in [199, 300, 404, 503]:
            client = self.client(status, {"message": "failure"})
            with self.assertRaises(Error) as caught:
                await put_process(client, "id", {"data": {}})
            self.assertEqual(await caught.exception.message(), "failure")

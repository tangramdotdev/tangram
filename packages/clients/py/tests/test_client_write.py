"""Check the write overloads, streaming, and authorization referents."""

import unittest
from typing import Any, cast
from unittest.mock import AsyncMock

from tangram.client.write import write
from tangram.error import Error
from tangram.http import Body, Response


class ClientWriteTests(unittest.IsolatedAsyncioTestCase):
    def client(self, *, status=200, output=None):
        response = Response(
            status,
            body=Body.json(output or {"blob": "blb_example?tokens[local][0]=secret"}),
        )
        client = AsyncMock()
        client.send.return_value = response
        return client

    async def test_bytes_and_utf8_string_overloads_return_id(self):
        for value, expected in [(b"bytes", b"bytes"), ("héllo", "héllo".encode())]:
            with self.subTest(value=value):
                client = self.client()
                self.assertEqual(await write(client, value), "blb_example")
                request = client.send.call_args.args[0]
                self.assertEqual(str(request.uri), "/write")
                self.assertEqual([chunk async for chunk in request.body], [expected])
                self.assertFalse(request.body.replayable)

    async def test_streaming_output_preserves_referent_and_option(self):
        consumed = []

        async def chunks():
            for value in (b"first", b"", b"second"):
                consumed.append(value)
                yield value

        client = self.client()
        output = await write(client, {"checkoutPointers": False}, chunks())
        self.assertEqual(consumed, [])
        self.assertEqual(output["blob"].node, "blb_example")
        self.assertEqual(output["blob"].options, {"tokens": {"local": ["secret"]}})
        request = client.send.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.uri), "/write?checkout_pointers=false")
        self.assertEqual(request.headers["accept"], "application/json")
        self.assertEqual(request.headers["content-type"], "application/octet-stream")
        self.assertEqual(
            [chunk async for chunk in request.body], [b"first", b"", b"second"]
        )
        self.assertEqual(consumed, [b"first", b"", b"second"])

    async def test_empty_bytes_are_a_single_chunk(self):
        client = self.client()
        await write(client, b"")
        self.assertEqual(
            [chunk async for chunk in client.send.call_args.args[0].body], [b""]
        )

    async def test_missing_input_is_rejected_before_sending(self):
        client = self.client()
        with self.assertRaisesRegex(AssertionError, "failed assertion"):
            await cast(Any, write)(client, {})
        client.send.assert_not_awaited()

    async def test_non_success_raises_tangram_error(self):
        client = self.client(status=409, output={"message": "the write failed"})
        with self.assertRaises(Error) as caught:
            await write(client, b"data")
        self.assertEqual(await caught.exception.message, "the write failed")


if __name__ == "__main__":
    unittest.main()

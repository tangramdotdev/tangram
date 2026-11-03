"""Buffered blob upload retries."""

import unittest

from tangram.client import Client, RequestError
from tangram.http import Body, Response
from tangram.http2 import TransportError

ID = "blb_010000000000000000000000000000000000000000000000000000"
RETRY = {"backoff": 0, "jitter": 0, "max_delay": 0, "max_retries": 2}


class WriteTests(unittest.IsolatedAsyncioTestCase):
    async def test_buffered_uploads_replay_after_a_connection_failure(self):
        for value in ("héllo", "héllo".encode()):
            client = Client(retry=RETRY)
            bodies = []

            async def send(request):
                bodies.append(await request.body.collect())
                if len(bodies) == 1:
                    raise RequestError(TransportError("graceful shutdown"))
                return Response(200, body=Body.json({"blob": ID}))

            client._send = send
            self.assertEqual(await client.write(value), ID)
            self.assertEqual(bodies, ["héllo".encode()] * 2)

    async def test_streaming_uploads_do_not_replay(self):
        client = Client(retry=RETRY)
        bodies = []

        async def send(request):
            bodies.append(await request.body.collect())
            raise RequestError(TransportError("graceful shutdown"))

        async def chunks():
            yield b"hello"

        client._send = send
        with self.assertRaises(TransportError):
            await client.write({}, chunks())
        self.assertEqual(bodies, [b"hello"])

    async def test_buffered_uploads_do_not_retry_terminal_errors(self):
        for error in (
            TimeoutError(),
            TransportError("protocol error", retryable=False),
        ):
            client = Client(retry=RETRY)
            bodies = []

            async def send(request):
                bodies.append(await request.body.collect())
                raise RequestError(error)

            client._send = send
            with self.assertRaises(type(error)):
                await client.write(b"hello")
            self.assertEqual(bodies, [b"hello"])

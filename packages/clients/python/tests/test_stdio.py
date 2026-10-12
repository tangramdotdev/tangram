"""Process stdio stream retries."""

import json
import unittest

from tangram.client import Client
from tangram.http import Response

ID = "pcs_010000000000000000000000000000000000000000000000000000"

STREAM_RETRY = {"backoff": 0, "jitter": 0, "max_delay": 0, "max_retries": 2}


def response(positions):
    """Create a response that sends chunks and ends without a response."""
    body = "".join(
        "event: notification\ndata: "
        + json.dumps(
            {
                "kind": "chunk",
                "value": {
                    "bytes": "YQ==",
                    "combined_position": position,
                    "stream": "stdout",
                    "stream_position": position,
                },
            }
        )
        + "\n\n"
        for position in positions
    )
    return Response(200, {"content-type": "text/event-stream"}, body.encode())


class StdioTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_exhausts_stream_retry(self):
        client = Client(stream_retry=STREAM_RETRY)
        sends = 0

        async def send(request):
            nonlocal sends
            sends += 1
            return response([])

        client.send = send
        output = await client.try_read_process_stdio(ID, streams=["stdout"])
        self.assertIsNotNone(output)
        with self.assertRaisesRegex(ConnectionError, "ended before completion"):
            async for _ in output:
                pass
        self.assertEqual(sends, 1 + STREAM_RETRY["max_retries"])

    async def test_read_progress_resets_attempts(self):
        client = Client(stream_retry=STREAM_RETRY)
        responses = [[0], [], [1], [], []]
        sends = 0

        async def send(request):
            nonlocal sends
            positions = responses[sends] if sends < len(responses) else []
            sends += 1
            return response(positions)

        client.send = send
        output = await client.try_read_process_stdio(ID, streams=["stdout"])
        self.assertIsNotNone(output)
        chunks = []
        with self.assertRaisesRegex(ConnectionError, "ended before completion"):
            async for chunk in output:
                chunks.append(chunk["stream_position"])
        self.assertEqual(chunks, [0, 1])
        self.assertEqual(sends, len(responses))

    async def test_write_exhausts_stream_retry(self):
        client = Client(stream_retry=STREAM_RETRY)
        sends = 0

        async def send(request):
            nonlocal sends
            sends += 1
            return Response(200, {"content-type": "text/event-stream"}, b"")

        async def input():
            return
            yield

        client.send = send
        with self.assertRaisesRegex(ConnectionError, "ended before completion"):
            await client.try_write_process_stdio(ID, {"streams": ["stdin"]}, input())
        self.assertEqual(sends, 1 + STREAM_RETRY["max_retries"])

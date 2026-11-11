"""Process wait retries."""

import unittest

from tangram.client import Client
from tangram.http import Response

ID = "pcs_010000000000000000000000000000000000000000000000000000"

STREAM_RETRY = {"backoff": 0, "jitter": 0, "max_delay": 0, "max_retries": 2}


def response(text, error=None):
    """Create a response whose body sends the text, and then fails with the error."""

    async def body():
        yield text.encode()
        if error is not None:
            raise error

    return Response(200, {"content-type": "text/event-stream"}, body())


class WaitTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_stream_failure_is_sent_again_and_returns_the_outcome(self):
        client = Client(stream_retry=STREAM_RETRY)
        responses = [
            response('event: outcome\ndata: {"exit":0}\n', ConnectionResetError()),
            response('event: outcome\ndata: {"exit":0}\n\nevent: end\ndata: \n\n'),
        ]
        sends = 0

        async def send_with_retry(request):
            nonlocal sends
            sends += 1
            return responses[sends - 1]

        client.send_with_retry = send_with_retry
        outcome = await client.wait_process(ID, {})
        self.assertEqual(outcome["exit"], 0)
        self.assertEqual(sends, 2)

    async def test_wait_server_error_event_fails_without_being_sent_again(self):
        client = Client(stream_retry=STREAM_RETRY)
        sends = 0

        async def send_with_retry(request):
            nonlocal sends
            sends += 1
            return response('event: error\ndata: {"message":"failed"}\n\n')

        client.send_with_retry = send_with_retry
        with self.assertRaises(Exception):
            await client.wait_process(ID, {})
        self.assertEqual(sends, 1)

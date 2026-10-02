"""Check HTTP argument framing and fragmented SSE decoding."""

import json
import unittest

from tangram.http import Body, Request, Response, Stream, query_string


async def fragments(bytes_, size=1):
    for offset in range(0, len(bytes_), size):
        yield bytes_[offset : offset + size]


class HttpTests(unittest.IsolatedAsyncioTestCase):
    def test_query(self):
        self.assertEqual(
            query_string(
                {
                    "metadata": False,
                    "tokens": {"remote:example": ["hello world"]},
                    "location": None,
                }
            ),
            "metadata=false&tokens%5Bremote%3Aexample%5D%5B0%5D=hello%20world",
        )

    async def test_argument_frame(self):
        arg = {"large": "hello" * 1000}
        request = Request("POST", "/example", body=b"payload").arg(arg)
        self.assertEqual(str(request.uri), "/example")
        self.assertEqual(request.headers["x-tg-arg-in-body"], "true")
        body = b"".join([chunk async for chunk in request.body])
        length, shift, index = 0, 0, 0
        while True:
            byte = body[index]
            index += 1
            length |= (byte & 127) << shift
            if not byte & 128:
                break
            shift += 7
        self.assertEqual(json.loads(body[index : index + length]), arg)
        self.assertEqual(body[index + length :], b"payload")
        self.assertTrue(request.body.replayable)

    async def test_sse_fragmentation(self):
        data = (
            b"\xef\xbb\xbf: comment\r\nevent: output\r\n"
            b"data: h\xc3\xa9llo\r\ndata: world\r\n\r\n"
            b"event: ack\rdata: {}\r\r"
        )
        response = Response(200, body=fragments(data))
        self.assertEqual(
            [event async for event in response.sse()],
            [
                {"event": "output", "data": "h��llo\nworld"},
                {"event": "ack", "data": "{}"},
            ],
        )

    async def test_sse_dispatches_remaining_block(self):
        response = Response(200, body=fragments(b"data: unfinished\n"))
        self.assertEqual(
            [event async for event in response.sse()], [{"data": "unfinished"}]
        )

    async def test_close_unstarted_response_stream(self):
        closed = []
        response = Response(
            200, body=fragments(b"data: hello\n\n"), close=lambda: closed.append(True)
        )
        stream = Stream(response.sse(), response.close)
        await stream.aclose()
        self.assertEqual(closed, [True])

    async def test_sse_encoding(self):
        async def events():
            yield {"event": "notification", "data": "hello\nworld"}

        response = Response(200, body=Body.sse(events()))
        self.assertEqual(
            [event async for event in response.sse()],
            [{"event": "notification", "data": "hello\nworld"}],
        )


if __name__ == "__main__":
    unittest.main()

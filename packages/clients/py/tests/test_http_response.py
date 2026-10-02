import asyncio
import unittest

from tangram.http.body import Body
from tangram.http.headers import Headers
from tangram.http.response import Response


class EventStream:
    def __init__(self):
        self.listeners = {}
        self.closed = False

    def once(self, name, callback):
        self.listeners.setdefault(name, []).append((callback, True))

    def on(self, name, callback):
        self.listeners.setdefault(name, []).append((callback, False))

    def emit(self, name, *args):
        callbacks = self.listeners.get(name, [])
        self.listeners[name] = [entry for entry in callbacks if not entry[1]]
        for callback, _ in callbacks:
            callback(*args)

    def close(self):
        self.closed = True


class ResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_constructor_preserves_body_and_headers(self):
        headers = Headers({"content-type": "application/json"})
        body = Body.json({"value": "hello"})
        response = Response(200, headers, body)
        self.assertIs(response.headers, headers)
        self.assertIs(response.body, body)
        self.assertEqual(await response.json(), {"value": "hello"})

    async def test_sse_uses_body_semantics(self):
        response = Response(200, {}, Body.text("event: final\ndata: tail"))
        self.assertEqual(
            [event async for event in response.sse()],
            [{"event": "final", "data": "tail"}],
        )

    async def test_stream_buffers_before_body_is_read(self):
        stream = EventStream()
        task = asyncio.create_task(Response.from_stream(stream))
        await asyncio.sleep(0)
        stream.emit("data", b"hello")
        stream.emit("response", {":status": "200"})
        stream.emit("data", b" world")
        stream.emit("end")
        response = await task
        self.assertEqual(response.status, 200)
        self.assertEqual(await response.collect(), b"hello world")
        self.assertFalse(stream.closed)

    async def test_unconsumed_stream_can_be_closed(self):
        stream = EventStream()
        task = asyncio.create_task(Response.from_stream(stream))
        await asyncio.sleep(0)
        stream.emit("response", {":status": "200"})
        response = await task
        await response.close()
        self.assertTrue(stream.closed)

    async def test_stream_error_before_response(self):
        stream = EventStream()
        task = asyncio.create_task(Response.from_stream(stream))
        await asyncio.sleep(0)
        stream.emit("error", ConnectionError("disconnected"))
        with self.assertRaisesRegex(ConnectionError, "disconnected"):
            await task

    async def test_stream_error_after_queued_chunks(self):
        stream = EventStream()
        task = asyncio.create_task(Response.from_stream(stream))
        await asyncio.sleep(0)
        stream.emit("response", {":status": "200"})
        response = await task
        stream.emit("data", b"prefix")
        stream.emit("trailers", {"x-tg-event": "error"})
        iterator = response.body.__aiter__()
        self.assertEqual(await anext(iterator), b"prefix")
        with self.assertRaisesRegex(ValueError, "missing data"):
            await anext(iterator)

    async def test_invalid_status_rejects_response(self):
        stream = EventStream()
        task = asyncio.create_task(Response.from_stream(stream))
        await asyncio.sleep(0)
        stream.emit("response", {":status": "200.5"})
        with self.assertRaisesRegex(ValueError, "invalid status"):
            await task

    async def test_transport_body_is_closed_on_failed_collection(self):
        closed = []

        async def body():
            try:
                yield b"prefix"
                raise ConnectionError("disconnected")
            finally:
                closed.append("body")

        response = Response(200, {}, body(), close=lambda: closed.append("stream"))
        with self.assertRaisesRegex(ConnectionError, "disconnected"):
            await response.collect()
        self.assertEqual(closed, ["body", "stream"])
        await response.close()
        self.assertEqual(closed, ["body", "stream"])

import unittest

from tangram.http.body import Body


async def chunks(*values):
    for value in values:
        yield value


class BodyTests(unittest.IsolatedAsyncioTestCase):
    async def test_replayable_bodies_preserve_chunks_when_prepended(self):
        body = Body.bytes(b"").prepend(b"prefix")
        self.assertTrue(body.replayable)
        self.assertEqual([chunk async for chunk in body], [b"prefix", b""])
        self.assertEqual([chunk async for chunk in body], [b"prefix", b""])
        self.assertEqual(await Body.empty().collect(), b"")
        self.assertEqual(await Body.text("café").collect(), "café".encode())

    async def test_stream_normalization_and_replayability(self):
        body = Body(chunks("hello", b" ", "world")).prepend(b"prefix ")
        self.assertFalse(body.replayable)
        self.assertEqual(await body.collect(), b"prefix hello world")

    async def test_json_static_and_instance_methods(self):
        value = {"text": "café", "nested": [1, None, True]}
        body = Body.json(value)
        self.assertTrue(body.replayable)
        self.assertEqual(await body.json(), value)
        self.assertEqual(await body.json(), value)

    async def test_sse_static_and_instance_methods(self):
        events = [{"data": "one\ntwo", "event": "update"}, {"data": ""}]
        body = Body.sse(chunks(*events))
        self.assertFalse(body.replayable)
        self.assertEqual([event async for event in body.sse()], events)

    async def test_sse_chunk_boundaries_and_final_event(self):
        body = Body(
            chunks(
                b": comment\n\nid: ignored\nretry: 1\n\n",
                b"event: ready\r\n\r",
                b"\ndata: first\ndata: second\n\n",
                b"event: final\ndata: tail",
            )
        )
        self.assertEqual(
            [event async for event in body.sse()],
            [
                {"event": "ready", "data": "first\nsecond"},
                {"event": "final", "data": "tail"},
            ],
        )

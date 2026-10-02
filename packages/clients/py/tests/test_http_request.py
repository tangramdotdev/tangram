import json
import unittest

from tangram.http.body import Body
from tangram.http.headers import Headers
from tangram.http.request import Request
from tangram.http.uri import Uri


class RequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_constructor_arg_and_preserved_instances(self):
        headers = Headers({"accept": "application/json"})
        uri = Uri("/objects?old=true")
        body = Body.text("tail")
        request = Request(
            {"method": "POST", "uri": uri, "headers": headers, "body": body}
        )
        self.assertEqual(request.method, "POST")
        self.assertIs(request.uri, uri)
        self.assertIs(request.headers, headers)
        self.assertIs(request.body, body)
        self.assertEqual(await request.body.collect(), b"tail")

    async def test_constructor_wraps_body(self):
        async def chunks():
            yield "hello"
            yield b" world"

        request = Request({"method": "POST", "uri": "/write", "body": chunks()})
        self.assertIsInstance(request.body, Body)
        self.assertEqual(await request.body.collect(), b"hello world")

    async def test_query_replaces_query_and_copies_headers(self):
        headers = Headers({"x-tg-arg-in-body": "true", "content-length": "0"})
        request = Request("GET", "/read?old=true", headers)
        output = request.arg({"position": 2, "length": 8})
        self.assertIs(output, request)
        self.assertEqual(str(request.uri), "/read?position=2&length=8")
        self.assertEqual(await request.body.collect(), b"")
        self.assertIsNot(request.headers, headers)
        self.assertIsNone(request.headers.get("x-tg-arg-in-body"))
        self.assertEqual(headers.get("x-tg-arg-in-body"), "true")
        self.assertEqual(request.headers.get("content-length"), "0")

    async def test_query_threshold_is_strict(self):
        request = Request("POST", "/write", body=Body.text("tail"))
        request.arg({"x": "a" * 4094})
        self.assertEqual(len(request.uri.query), 4096)
        self.assertIsNone(request.headers.get("x-tg-arg-in-body"))
        self.assertEqual(await request.body.collect(), b"tail")
        request.arg({"x": "a" * 4095})
        self.assertIsNone(request.uri.query)
        self.assertEqual(request.headers.get("x-tg-arg-in-body"), "true")

    async def test_frame_uses_utf8_byte_length_and_explicit_body(self):
        headers = Headers({"content-length": "4", "cache-control": "max-age=60"})
        request = Request("POST", "/write", headers, Body.text("old"))
        arg = {"x": "é" * 3000, "nested": [True, None, {"y": "✓"}]}
        expected = json.dumps(arg, ensure_ascii=False, separators=(",", ":")).encode()
        request.arg(arg, Body.text("new"))
        frame = await request.body.collect()
        offset = 0
        length = 0
        shift = 0
        while True:
            byte = frame[offset]
            offset += 1
            length |= (byte & 127) << shift
            if byte < 128:
                break
            shift += 7
        self.assertEqual(length, len(expected))
        self.assertEqual(frame[offset : offset + length], expected)
        self.assertEqual(frame[offset + length :], b"new")
        self.assertEqual(str(request.uri), "/write")
        self.assertEqual(request.headers.get("cache-control"), "no-store")
        self.assertIsNone(request.headers.get("content-length"))
        self.assertEqual(headers.get("content-length"), "4")

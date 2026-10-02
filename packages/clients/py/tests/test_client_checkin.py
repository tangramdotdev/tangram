"""Verify checkin's default omission, option renaming, and output proofs."""

import unittest
from unittest.mock import AsyncMock

from tangram.client import Client
from tangram.client.checkin import Checkin, checkin
from tangram.error import Error
from tangram.http import Body, Response


class CheckinCodecTests(unittest.TestCase):
    def to_json(self, options, updates=None):
        return Checkin.Arg.to_json(
            {"path": "/package", "options": options, "updates": updates or []}
        )

    def test_default_options_are_omitted(self):
        self.assertEqual(
            self.to_json(
                {
                    "checkout_pointers": True,
                    "destructive": False,
                    "deterministic": False,
                    "ignore": True,
                    "local_dependencies": True,
                    "lock": "auto",
                    "locked": False,
                    "root": False,
                    "solve": True,
                    "unsolved_dependencies": False,
                    "watch": False,
                }
            ),
            {"options": {}, "path": "/package"},
        )

    def test_enabled_options_have_exact_wire_names(self):
        source = {
            "checkout_pointers": False,
            "destructive": True,
            "deterministic": True,
            "ignore": False,
            "local_dependencies": False,
            "lock": "attr",
            "locked": True,
            "root": True,
            "solve": False,
            "unsolved_dependencies": True,
            "watch": True,
            "ttl": 3.0,
        }
        output = self.to_json(source, ["one", "two"])
        self.assertEqual(
            output,
            {
                "path": "/package",
                "updates": "one,two",
                "options": {
                    "checkout_pointers": False,
                    "destructive": True,
                    "deterministic": True,
                    "ignore": False,
                    "source_dependencies": False,
                    "lock": "attr",
                    "locked": True,
                    "root": True,
                    "solve": False,
                    "unsolved_dependencies": True,
                    "watch": True,
                    "tag_ttl": "3s",
                },
            },
        )
        self.assertEqual(source["ttl"], 3.0)
        self.assertNotIn("source_dependencies", source)

    def test_null_lock_infinite_ttl_and_camel_options(self):
        self.assertEqual(
            self.to_json(
                {
                    "lock": None,
                    "ttl": None,
                    "checkoutPointers": False,
                    "localDependencies": False,
                    "unsolvedDependencies": True,
                }
            )["options"],
            {
                "lock": None,
                "tag_ttl": "infinite",
                "checkout_pointers": False,
                "source_dependencies": False,
                "unsolved_dependencies": True,
            },
        )


class CheckinEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_progress_and_artifact_referent(self):
        async def events():
            yield {"event": "log", "data": '{"message":"checking in"}'}
            yield {
                "event": "output",
                "data": '{"artifact":"fil_test?name=artifact&tokens[local][0]=secret"}',
            }

        client = Client()
        client.send_with_retry = AsyncMock(
            return_value=Response(200, body=Body.sse(events()))
        )
        stream = await checkin(
            client, {"path": "/package", "options": {"ttl": 0}, "updates": []}
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.uri), "/checkin")
        self.assertEqual(request.headers["accept"], "text/event-stream")
        self.assertEqual(
            await request.body.json(),
            {"path": "/package", "options": {"tag_ttl": "0s"}},
        )
        output = [event async for event in stream]
        self.assertEqual(output[1]["value"]["artifact"].node, "fil_test")
        self.assertEqual(
            output[1]["value"]["artifact"].options,
            {"name": "artifact", "tokens": {"local": ["secret"]}},
        )

    async def test_client_path_overload_uses_same_codec(self):
        client = Client()
        client.send_with_retry = AsyncMock(return_value=Response(200, body=b""))
        stream = await client.checkin(
            "/package", options={"ignore": False}, updates=["tag"]
        )
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(
            await request.body.json(),
            {"path": "/package", "updates": "tag", "options": {"ignore": False}},
        )
        await stream.aclose()

    async def test_status_error_is_preserved(self):
        client = Client()
        client.send_with_retry = AsyncMock(
            return_value=Response(
                409, body=Body.json({"message": "failed to check in"})
            )
        )
        with self.assertRaises(Error) as caught:
            await checkin(client, {"path": "/package", "options": {}, "updates": []})
        self.assertEqual(await caught.exception.message, "failed to check in")

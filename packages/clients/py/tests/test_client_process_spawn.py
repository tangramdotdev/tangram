"""Check spawn wire defaults, inline command proofs, and progress decoding."""

import unittest
from unittest.mock import AsyncMock

from tangram.client import Client
from tangram.client.process.spawn import (
    Arg,
    CommandArg,
    Output,
    spawn_process,
    try_spawn_process,
)
from tangram.error import Error
from tangram.http import Body, Response
from tangram.referent import Referent


class SpawnCodecTests(unittest.TestCase):
    def test_optional_fields_preserve_nulls_and_omit_source_defaults(self):
        self.assertEqual(
            Arg.to_json(
                {
                    "command": Referent("cmd_test"),
                    "cached": False,
                    "checksum": None,
                    "cache_location": None,
                    "debug": None,
                    "location": None,
                    "parent": None,
                    "public": False,
                    "retry": False,
                    "sandbox": None,
                    "stderr": "inherit",
                    "stdin": "pipe",
                    "stdout": "inherit",
                    "tty": False,
                    "ignored": True,
                }
            ),
            {
                "command": "cmd_test",
                "cached": False,
                "checksum": None,
                "cache_location": None,
                "debug": None,
                "location": None,
                "parent": None,
                "sandbox": None,
                "stdin": "pipe",
                "tty": False,
            },
        )

    def test_inline_command_serializes_both_nested_referents(self):
        executable = Referent({"artifact": "fil_test", "path": "bin"}, {"name": "exec"})
        stdin = Referent("blb_test", {"tokens": {"local": ["secret"]}})
        command = {"executable": executable, "stdin": stdin, "args": []}
        original = Referent(command, {"name": "command"})
        output = Arg.to_json({"command": original})
        self.assertEqual(
            output["command"],
            {
                "node": {
                    "executable": executable.to_data(),
                    "stdin": stdin.to_data(),
                    "args": [],
                },
                "options": {"name": "command"},
            },
        )
        self.assertIs(command["executable"], executable)
        self.assertIs(command["stdin"], stdin)
        self.assertEqual(
            CommandArg.to_json({"executable": executable, "stdin": None})["stdin"], None
        )

    def test_locations_are_encoded_and_decoded_without_decoding_outcomes(self):
        self.assertEqual(
            Arg.to_json(
                {
                    "command": Referent("cmd_test"),
                    "location": {
                        "components": [{"name": "cloud", "regions": ["east"]}]
                    },
                    "cache_location": {"components": []},
                }
            )["location"],
            "remote:cloud(east)",
        )
        outcome = {"exit": 0, "output": {"kind": "map", "value": {}}}
        output = Output.from_json(
            {"process": "prc_test", "location": "local", "outcome": outcome}
        )
        self.assertEqual(output["location"], {})
        self.assertIs(output["outcome"], outcome)
        self.assertEqual(
            Output.from_json({"process": 123, "location": None, "outcome": None}),
            {"process": 123, "location": None, "outcome": None},
        )


class SpawnEndpointTests(unittest.IsolatedAsyncioTestCase):
    def client(self, events, status=200):
        async def input():
            for event in events:
                yield event

        response = Response(status, body=Body.sse(input()))
        client = Client()
        client.send_with_retry = AsyncMock(return_value=response)
        return client

    async def test_request_and_nullable_progress(self):
        client = self.client(
            [
                {"event": "log", "data": '{"message":"spawning"}'},
                {
                    "event": "output",
                    "data": '{"process":"prc_test","location":"local"}',
                },
                {"event": "output", "data": "null"},
            ]
        )
        stream = await try_spawn_process(client, {"command": Referent("cmd_test")})
        request = client.send_with_retry.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.uri), "/processes/spawn")
        self.assertEqual(request.headers["accept"], "text/event-stream")
        self.assertEqual(request.headers["content-type"], "application/json")
        self.assertEqual(await request.body.json(), {"command": "cmd_test"})
        events = [event async for event in stream]
        self.assertEqual(events[1]["value"]["location"], {})
        self.assertIsNone(events[2]["value"])

    async def test_required_spawn_rejects_null_output(self):
        client = self.client([{"event": "output", "data": "null"}])
        events = await spawn_process(client, {"command": Referent("cmd_test")})
        with self.assertRaisesRegex(ValueError, "expected a process"):
            _ = [event async for event in events]

    async def test_non_success_preserves_tangram_error(self):
        client = Client()
        client.send_with_retry = AsyncMock(
            return_value=Response(
                409, body=Body.json({"message": "the process could not be spawned"})
            )
        )
        with self.assertRaises(Error) as caught:
            await try_spawn_process(client, {"command": Referent("cmd_test")})
        self.assertEqual(
            await caught.exception.message, "the process could not be spawned"
        )

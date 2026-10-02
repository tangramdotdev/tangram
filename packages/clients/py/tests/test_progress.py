"""Check progress.ts decoding and collection semantics."""

import json
import unittest
from unittest.mock import patch

from tangram.error import Error
from tangram.progress import Progress


class Response:
    def __init__(self, events):
        self.events = events

    async def sse(self):
        for event in self.events:
            yield event


def event(kind, value):
    result = {"data": json.dumps(value)}
    if kind is not None:
        result["event"] = kind
    return result


class ProgressTests(unittest.IsolatedAsyncioTestCase):
    async def test_named_events_convert_only_output(self):
        response = Response(
            [
                event("diagnostic", {"message": "diagnostic"}),
                event("indicators", [{"name": "build"}]),
                event("log", {"message": "log"}),
                event("output", 3),
            ]
        )
        converted = []

        def convert(value):
            converted.append(value)
            return value * 2

        self.assertEqual(
            [value async for value in Progress.decode(response, convert)],
            [
                {"kind": "diagnostic", "value": {"message": "diagnostic"}},
                {"kind": "indicators", "value": [{"name": "build"}]},
                {"kind": "log", "value": {"message": "log"}},
                {"kind": "output", "value": 6},
            ],
        )
        self.assertEqual(converted, [3])

    async def test_anonymous_non_output_preserves_the_parsed_event(self):
        parsed = {"kind": "extension", "extra": True}
        response = Response(
            [event(None, parsed), event(None, {"kind": "output", "value": 3})]
        )
        self.assertEqual(
            [
                value
                async for value in Progress.decode(response, lambda value: value + 1)
            ],
            [parsed, {"kind": "output", "value": 4}],
        )

    async def test_error_ids_and_inline_errors_use_the_correct_constructor(self):
        for value, constructor in [
            ("error_id", "with_id"),
            ({"message": "failure"}, "from_data"),
        ]:
            error = Error("failure")
            with patch.object(Error, constructor, return_value=error) as create:
                with self.assertRaises(Error) as caught:
                    _ = [
                        value
                        async for value in Progress.decode(
                            Response([event("error", value)])
                        )
                    ]
                self.assertIs(caught.exception, error)
                create.assert_called_once_with(value)

    async def test_unknown_named_event_rejects_before_parsing_payload(self):
        with self.assertRaisesRegex(ValueError, "invalid progress event"):
            _ = [
                value
                async for value in Progress.decode(
                    Response([{"event": "unknown", "data": "not json"}])
                )
            ]

    async def test_last_output_exhausts_events_and_returns_the_last_value(self):
        seen = []

        async def events():
            for value in [
                {"kind": "output", "value": 1},
                {"kind": "log", "value": {"message": "after"}},
                {"kind": "output", "value": None},
            ]:
                seen.append(value)
                yield value

        self.assertIsNone(await Progress.last_output(events()))
        self.assertEqual(len(seen), 3)

        async def empty():
            if False:
                yield

        self.assertIsNone(await Progress.last_output(empty()))

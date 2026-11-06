"""Shared authorization token decoding cases."""

import gc
import json
import tracemalloc
import unittest
from pathlib import Path

import tangram as tg
from tangram import authorization


def fixtures():
    path = Path(__file__).parents[2] / "rust/fixtures/tokens.json"
    return json.loads(path.read_text())


def token(index):
    id = index.to_bytes(4, "little") + bytes(12)
    return authorization.encode_base32(
        bytes([0, 1, 0b10, 0, 0, 1, 0, *id, 0, *bytes(64)])
    )


def retained(callback):
    # Measure the memory that remains allocated after running the callback.
    gc.collect()
    tracemalloc.start()
    try:
        callback()
        gc.collect()
        current, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    return current


class AuthorizationTests(unittest.TestCase):
    def test_shared_token_decoding(self):
        for fixture in fixtures():
            parsed = authorization.Token.from_data(fixture["token"])
            self.assertEqual(parsed.to_data(), fixture["token"])
            self.assertEqual(parsed.secret_id, fixture["secret_id"])
            self.assertTrue(parsed.signature)
            body = parsed.body
            self.assertEqual(
                body,
                authorization.Body(
                    int(fixture["expires_at"]),
                    tuple(fixture["permissions"]),
                    fixture["resource"],
                ),
            )

    def test_malformed_tokens_are_opaque(self):
        token = fixtures()[0]["token"]
        for malformed in ["", "1", token[:40], f"1{token[1:]}", token.upper()]:
            parsed = authorization.Token.from_data(malformed)
            self.assertIsNone(parsed.body)
            self.assertEqual(parsed.to_data(), malformed)

    def test_replaced_tokens_release_parsed_tokens(self):
        child = tg.File.with_id(f"fil_01{'a' * 52}")
        parent = tg.Directory({"child": child})

        def replace():
            for index in range(10_000):
                child.state.tokens = {"local": [token(index)]}
                parent.state.collect_tokens()

        self.assertLess(retained(replace), 2 * 1024 * 1024)

    def test_cleared_tokens_release_parsed_tokens(self):
        child = tg.File.with_id(f"fil_01{'a' * 52}")
        parent = tg.Directory({"child": child})

        def clear():
            child.state.tokens = {"local": [token(index) for index in range(10_000)]}
            parent.state.collect_tokens()
            child.state.tokens = {}
            parent.state.collect_tokens()

        self.assertLess(retained(clear), 2 * 1024 * 1024)

    def test_structured_entries_round_trip_and_isolate_cloned_entries(self):
        data = {
            "local": [fixture["token"] for fixture in fixtures()],
            "remote": ["opaque-token"],
        }
        tokens = authorization.Tokens.from_data(data)
        self.assertEqual(tokens.to_data(), data)
        entry = tokens.get("local")
        cloned = tokens.clone()
        self.assertIs(cloned.get("local").authorization[0], entry.authorization[0])
        cloned.get("local").authorization.clear()
        self.assertEqual(tokens.to_data(), data)

    def test_handles_expose_structured_tokens_and_collect_current_data(self):
        fixture = fixtures()[0]
        file = tg.File.with_id(fixture["resource"])
        data = {"local": [fixture["token"]]}
        file.state.tokens = data
        self.assertEqual(
            file.state.tokens.get("local").authorization[0].body.resource,
            fixture["resource"],
        )
        self.assertEqual(file.state.collect_tokens().to_data(), data)
        file.state.tokens = {"remote": ["opaque-token"]}
        self.assertEqual(
            file.state.collect_tokens().to_data(), {"remote": ["opaque-token"]}
        )


if __name__ == "__main__":
    unittest.main()

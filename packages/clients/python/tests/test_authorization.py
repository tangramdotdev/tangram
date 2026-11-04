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
            body = authorization.parse(fixture["token"])
            self.assertEqual(
                body,
                {
                    "expires_at": int(fixture["expires_at"]),
                    "permissions": fixture["permissions"],
                    "resource": fixture["resource"],
                },
            )

    def test_malformed_tokens_are_opaque(self):
        token = fixtures()[0]["token"]
        for malformed in ["", "1", token[:40], f"1{token[1:]}", token.upper()]:
            self.assertIsNone(authorization.parse(malformed))

    def test_replaced_tokens_do_not_retain_their_decoded_tokens(self):
        child = tg.File.with_id(f"fil_01{'a' * 52}")
        parent = tg.Directory({"child": child})

        def replace():
            for index in range(10_000):
                child.tokens["local"] = [token(index)]
                parent.state.collect_tokens()

        self.assertLess(retained(replace), 2 * 1024 * 1024)

    def test_cleared_tokens_do_not_retain_their_decoded_tokens(self):
        child = tg.File.with_id(f"fil_01{'a' * 52}")
        parent = tg.Directory({"child": child})

        def clear():
            child.tokens["local"] = [token(index) for index in range(10_000)]
            parent.state.collect_tokens()
            child.tokens["local"].clear()
            parent.state.collect_tokens()

        self.assertLess(retained(clear), 2 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()

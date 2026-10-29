"""Shared authorization token decoding cases."""

import json
import unittest
from pathlib import Path

from tangram import authorization


def fixtures():
    path = Path(__file__).parents[2] / "rust/fixtures/tokens.json"
    return json.loads(path.read_text())


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


if __name__ == "__main__":
    unittest.main()

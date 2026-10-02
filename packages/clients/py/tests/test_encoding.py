import types
import unittest

from tangram import encoding
from tangram.encoding import set_encoding


class EncodingTests(unittest.TestCase):
    def test_binary_codecs(self):
        value = b"\x00\xffhello"
        self.assertEqual(encoding.base64.encode(value), "AP9oZWxsbw==")
        self.assertEqual(encoding.base64.decode("AP9oZWxsbw=="), value)
        self.assertEqual(encoding.hex.encode(value), "00ff68656c6c6f")
        self.assertEqual(encoding.hex.decode("00ff68656c6c6f"), value)
        self.assertEqual(encoding.base64.decode(""), b"")
        self.assertEqual(encoding.hex.decode(""), b"")

    def test_binary_decoders_reject_noncanonical_input(self):
        for value in ["Zg", "Zg==\n", "Zh==", "Zg===", "!"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                encoding.base64.decode(value)
        for value in ["FF", "ff ff", "f", "fg"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                encoding.hex.decode(value)

    def test_structured_codecs(self):
        value = {"name": "héllo", "count": 3, "values": [True, False]}
        for codec in [encoding.json, encoding.toml, encoding.yaml]:
            with self.subTest(codec=codec):
                self.assertEqual(codec.decode(codec.encode(value)), value)
        self.assertEqual(encoding.json.encode({"a": 1}), '{"a":1}')
        for value in ["NaN", "Infinity", "-Infinity"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                encoding.json.decode(value)
        with self.assertRaises(ValueError):
            encoding.json.encode(float("nan"))

    def test_utf8(self):
        self.assertEqual(encoding.utf8.encode("héllo"), b"h\xc3\xa9llo")
        self.assertEqual(encoding.utf8.decode(b"h\xc3\xa9llo"), "héllo")
        self.assertEqual(encoding.utf8.decode(b"\xef\xbb\xbftext"), "\ufefftext")
        with self.assertRaises(UnicodeDecodeError):
            encoding.utf8.decode(b"\xff")

    def test_set_encoding_preserves_identity_and_unspecified_operations(self):
        previous = encoding.json
        previous_hex = encoding.hex
        replacement = types.SimpleNamespace(
            decode=lambda value: [value], encode=lambda value: "replacement"
        )
        alias = encoding.encoding
        try:
            set_encoding({"json": replacement})
            self.assertIs(alias, encoding)
            self.assertEqual(alias.json.encode(None), "replacement")
            self.assertEqual(encoding.json.decode("value"), ["value"])
            self.assertIs(encoding.hex, previous_hex)
            set_encoding(types.SimpleNamespace(json=previous))
            self.assertIs(alias.json, previous)
        finally:
            set_encoding({"json": previous})

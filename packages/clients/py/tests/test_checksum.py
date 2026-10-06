"""Checksum API compatibility with checksum.ts."""

import hashlib
import unittest
from unittest.mock import AsyncMock, patch

import tangram as tg
from tangram import Blob, Checksum, File, host, output
from tangram.checksum import checksum


class ChecksumTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        async def text(file, client=None):
            contents = await file.contents(client)
            return (await contents.load(client))["bytes"].decode()

        self.enterContext(patch.object(File, "text", tg.property(text)))

    async def test_native_strings_and_bytes(self):
        for algorithm in ("sha256", "sha512"):
            expected = f"{algorithm}:{hashlib.new(algorithm, b'hello').hexdigest()}"
            self.assertEqual(await checksum("hello", algorithm), expected)
            self.assertEqual(await Checksum.new(b"hello", algorithm), expected)

    def test_validation(self):
        for value in ("sha256:abc", "sha512-ab+/==", "blake3:XYZ", "sha256:any"):
            self.assertTrue(Checksum.is_(value))
            self.assertIs(Checksum.expect(value), value)
            self.assertIsNone(Checksum.assert_(value))
        for value in (None, 42, "sha256:", "md5:abc", "sha256:ab_cd", "sha256:ab=c"):
            self.assertFalse(Checksum.is_(value))
            with self.assertRaises(AssertionError):
                Checksum.expect(value)
            with self.assertRaises(AssertionError):
                Checksum.assert_(value)

    def test_algorithm_separator_precedence(self):
        self.assertEqual(Checksum.algorithm("sha256-abc"), "sha256")
        self.assertEqual(Checksum.algorithm("sha512:abc-def"), "sha512")
        self.assertEqual(Checksum.algorithm("sha256-abc:def"), "sha256-abc")
        with self.assertRaisesRegex(ValueError, "invalid checksum"):
            Checksum.algorithm("sha256")

    async def test_artifact_builtin_command(self):
        for input in (Blob("hello"), File("hello")):
            with patch("tangram.process.build.build", new_callable=AsyncMock) as build:
                build.return_value = File("sha256:abc")
                self.assertEqual(await Checksum.new(input, "sha256"), "sha256:abc")
                options = build.call_args.kwargs
                self.assertEqual(options["executable"], "tg")
                self.assertEqual(options["host"], host.current)
                self.assertNotIn("name", options)
                args = options["args"]
                self.assertEqual(
                    args[:5],
                    ["builtin", "checksum", "--algorithm", "sha256", "--input"],
                )
                self.assertIsInstance(args[5], File)
                self.assertEqual(await args[5].text, "hello")
                self.assertEqual(args[6:], ["--output", output])

    async def test_artifact_result_validation(self):
        for result in (Blob("sha256:abc"), File("invalid checksum")):
            with patch("tangram.process.build.build", new_callable=AsyncMock) as build:
                build.return_value = result
                with self.assertRaises(AssertionError):
                    await Checksum.new(Blob("hello"), "sha256")

    async def test_future_input(self):
        async def input():
            return "hello"

        self.assertEqual(
            await checksum(input(), "sha256"), host.checksum("hello", "sha256")
        )

"""Builtin command contracts mirror the JavaScript source."""

import importlib
from unittest.mock import patch

from helpers import ObjectTestCase

from tangram.blob import Blob
from tangram.directory import Directory
from tangram.file import File
from tangram.placeholder import output
from tangram.symlink import Symlink

builtin = importlib.import_module("tangram.builtin")


class Build:
    def __init__(self, result):
        self.result = result
        self.options = {}

    def __getattr__(self, name):
        def set(value):
            self.options[name] = value
            return self

        return set

    def __await__(self):
        async def result():
            return self.result

        return result().__await__()


class BuiltinTests(ObjectTestCase):
    async def test_archive_argument_order_and_validation(self):
        blob = Blob("archive")
        builder = Build(File(blob))
        artifact = Directory({"file": File("input"), "link": Symlink("file")})
        with patch.object(builtin, "build", return_value=builder):
            self.assertIs(await builtin.archive(artifact, "tar", "gz"), blob)
        self.assertEqual(
            builder.options["args"],
            [
                "builtin",
                "archive",
                "--compression",
                "gz",
                "--format",
                "tar",
                "--input",
                artifact,
                "--output",
                output,
            ],
        )
        self.assertEqual(builder.options["named"], "archive")
        for artifact, message in [
            (
                File("x", dependencies={"dep": File("dep")}),
                "cannot archive a file with dependencies",
            ),
            (Symlink(artifact=File("x")), "cannot archive a symlink with an artifact"),
            (Symlink(value={}), "cannot archive a symlink without a path"),
        ]:
            with self.assertRaisesRegex(ValueError, message):
                await builtin.archive(artifact, "tar")

    async def test_download_populates_the_callers_checksum_option(self):
        options = {"mode": "raw"}
        builder = Build(File(Blob("download")))
        with patch.object(builtin, "build", return_value=builder):
            await builtin.download("https://example.test", "sha512:any", options)
        self.assertEqual(options, {"mode": "raw", "checksum": "sha512"})

    async def test_download_positional_options_and_defaults(self):
        blob = Blob("download")
        for checksum, options, algorithm, mode in [
            (None, None, "sha512", "raw"),
            (
                "sha512-any",
                {"checksum": "blake3", "mode": "extract"},
                "blake3",
                "extract",
            ),
        ]:
            result = File(blob) if mode == "raw" else Directory({})
            builder = Build(result)
            with patch.object(builtin, "build", return_value=builder):
                value = await builtin.download(
                    "https://example.test", checksum, options
                )
            self.assertIs(value, blob if mode == "raw" else result)
            self.assertEqual(
                builder.options["args"],
                [
                    "builtin",
                    "download",
                    "--checksum",
                    algorithm,
                    "--mode",
                    mode,
                    "--output",
                    output,
                    "https://example.test",
                ],
            )
            self.assertEqual(builder.options["checksum"], checksum or "sha512:none")
            self.assertTrue(builder.options["network"])
            if options:
                self.assertEqual(options["checksum"], "blake3")

    async def test_compression_and_extraction_commands(self):
        input = Blob("input")
        for name in ("compress", "decompress", "extract"):
            result = Directory({}) if name == "extract" else File("result")
            builder = Build(result)
            with patch.object(builtin, "build", return_value=builder):
                await getattr(builtin, name)(
                    input, "gz"
                ) if name == "compress" else await getattr(builtin, name)(input)
            args = builder.options["args"]
            self.assertEqual(args[:2], ["builtin", name])
            self.assertEqual(args[-2:], ["--output", output])
            file = args[args.index("--input") + 1]
            self.assertIs(await file.contents, input)
            self.assertEqual(builder.options["named"], name)

    async def test_bundle_resolves_and_preserves_dependency_free_handle(self):
        artifact = Directory({"file": File("input")})

        async def pending():
            return artifact

        self.assertIs(
            await builtin.bundle(pending(), client=self.object_client), artifact
        )

    async def test_bundle_collects_transitive_dependencies_and_rewrites_symlinks(self):
        leaf = Directory({"data": File("leaf")})
        intermediate = await File.new(
            {"contents": "intermediate", "dependencies": {"leaf": leaf}}
        )
        root = Directory(
            {
                "file": await File.new(
                    {"contents": "root", "dependencies": {"dep": intermediate}}
                ),
                "link": Symlink("data", artifact=leaf),
            }
        )
        result = await builtin.bundle(root, client=self.object_client)
        self.assertEqual(await (await result.get("file")).dependencies, {})
        link = (await result.entries)["link"]
        self.assertEqual(await link.path, f".tangram/store/{leaf.id}/data")
        self.assertIsNone(await link.artifact)
        store = await result.get(".tangram/store")
        self.assertEqual(list(await store.entries), sorted([leaf.id, intermediate.id]))
        self.assertEqual(await (await store.get(intermediate.id)).dependencies, {})

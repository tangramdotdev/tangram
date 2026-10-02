"""Exercise constructors, mutation semantics, graph edges, and local execution."""

import asyncio

from helpers import ObjectTestCase

import tangram as tg
from tangram.value import UNSET


class BuilderTests(ObjectTestCase):
    async def test_directory_merge_and_symlinks(self):
        directory = await tg.directory(
            {"a/one": "one", "a/two": "two", "old": "old"},
            {
                "a/one": None,
                "a/three": "three",
                "old": None,
                "relative": tg.Symlink("a/../a/two"),
                "artifact": tg.Symlink(
                    artifact=await tg.directory({"nested/file": "hello"})
                ),
            },
        )
        self.assertEqual(await (await directory.get("relative")).text(), "two")
        self.assertEqual(
            await (
                await (await directory.get("artifact/nested/file")).get("nested/file")
            ).text(),
            "hello",
        )
        self.assertIsNone(await directory.try_get("old"))
        self.assertEqual(
            [name async for name, _ in directory.walk()],
            ["a", "a/two", "a/three", "relative", "artifact"],
        )
        with self.assertRaises(ValueError):
            await directory.try_get("../escape")

    async def test_graph_preserves_non_edge_integers(self):
        graph = tg.Graph(
            [
                {
                    "kind": "directory",
                    "children": [{"directory": 1, "count": 25, "last": "file"}],
                },
                {"kind": "directory", "entries": {"file": 2}},
                {
                    "kind": "file",
                    "contents": tg.Blob("hello"),
                    "executable": True,
                    "dependencies": {"self": tg.Referent(2)},
                },
            ]
        )
        directory = await graph.get(0)
        file = await directory.get("file")
        self.assertIs(await file.executable(), True)
        self.assertEqual(await file.text(), "hello")
        self.assertEqual(await file.dependency_objects(), [file])
        decoded = tg.Object.from_data(graph.to_data())
        self.assertEqual(decoded.id, graph.id)

    async def test_file_and_command_merge(self):
        dependency = tg.File("dependency")
        file = await tg.file(
            "hello", " world", dependencies={"dep": dependency}, executable=True
        )
        self.assertEqual(await file.length(), 11)
        self.assertEqual(await file.dependency_objects(), [dependency])
        command = (
            await tg.command({"executable": "sh"})
            .arg("-c", "echo hello")
            .env(PATH="/bin")
            .env({"PATH": tg.Mutation.suffix("/usr/bin", ":")})
        )
        self.assertEqual(
            (await command.env("PATH")).value.components, ["/bin:/usr/bin"]
        )
        self.assertEqual(
            [arg.value for arg in await command.args()], ["-c", "echo hello"]
        )

    async def test_local_duplex_and_exit(self):
        async with await tg.spawn(
            {"executable": "/bin/cat"}, stdin="pipe", stdout="pipe", stderr="null"
        ) as process:

            async def read():
                return b"".join([chunk async for chunk in process.read()])

            reader = asyncio.create_task(read())
            data = b"hello\n" * 20000
            await process.write(data)
            await process.end()
            self.assertEqual(await reader, data)
            self.assertEqual((await process.wait())["exit"], 0)
        async with await tg.spawn(
            {"executable": "/bin/sh"},
            args=["-c", "kill -TERM $$"],
            stdin="null",
            stdout="null",
            stderr="null",
        ) as process:
            self.assertEqual((await process.wait())["exit"], 143)
        with self.assertRaisesRegex(tg.Error, "the child process failed"):
            await tg.run({"executable": "/bin/sh"}, args=["-c", "exit 7"])

    async def test_build_rejects_uncacheable(self):
        with self.assertRaisesRegex(tg.Error, "cacheable"):
            await tg.build("sh", network=True)

    async def test_mutations_distinguish_missing_and_null(self):
        mutation = await tg.Mutation.set_if_unset(3)
        self.assertIsNone(await mutation.apply(None))
        self.assertEqual(await mutation.apply(), 3)
        self.assertIs(await tg.Mutation.unset().apply(3), UNSET)
        value = {"remove": 1, "list": [2], "null": None}
        mutation = await tg.Mutation.merge(
            {
                "remove": tg.Mutation.unset(),
                "list": tg.Mutation.prepend([1]),
                "null": tg.Mutation.set_if_unset(4),
            }
        )
        await mutation.apply(value)
        self.assertEqual(value, {"list": [1, 2], "null": None})
        self.assertEqual(
            (await (await tg.Mutation.prefix("", ":")).apply("a")).components, ["a"]
        )
        self.assertEqual(
            (await tg.Template.join(":", None, "a", "", "b")).components, ["a:b"]
        )

    async def test_fluent_objects_and_nested_inputs(self):
        from tangram.directory import DirectoryBuilder
        from tangram.file import FileBuilder
        from tangram.graph import GraphBuilder
        from tangram.symlink import SymlinkBuilder
        from tangram.template import TemplateBuilder

        async def string():
            return "hello"

        value = string()
        builder = (
            FileBuilder(value)
            .contents(value)
            .executable()
            .dependencies({"a": tg.File("a")})
            .dependencies(None)
            .dependency("b", tg.File("b"))
        )
        file = await builder
        self.assertEqual(await file.text(), "hellohello")
        self.assertTrue(await file.executable())
        self.assertEqual(set(await file.dependencies()), {"b"})
        self.assertEqual((await builder).id, file.id)
        directory = await DirectoryBuilder().entry("nested/file", FileBuilder("hello"))
        template = await TemplateBuilder(directory, "/nested/file")
        link = await SymlinkBuilder(template)
        self.assertEqual(await (await link.resolve()).text(), "hello")
        graph = (
            await GraphBuilder()
            .nodes(
                [
                    {"kind": "directory", "entries": {"file": 1}},
                    {"kind": "file", "contents": "first"},
                ]
            )
            .node({"kind": "file", "contents": "second"})
        )
        self.assertEqual(await (await (await graph.get(0)).get("file")).text(), "first")

    async def test_constructor_null_clears(self):
        from tangram.args import Args

        mapped = await Args.apply(
            [
                {"values": [1]},
                {"values": None},
                {"values": [2]},
                {"map": {"a": 1}},
                {"map": None},
                {"map": {"b": 2}},
            ],
            map=lambda value: value,
            reduce={"values": "append", "map": "merge"},
        )
        self.assertEqual(mapped, {"values": [2], "map": {"b": 2}})
        command = await tg.Command.new(
            {"executable": "sh", "args": [1], "env": {"A": None}},
            {"args": None, "env": None},
            {"args": [2], "env": {"B": tg.Command.Value.value({"a": 1})}},
        )
        self.assertEqual([arg.value for arg in await command.args()], [2])
        self.assertEqual(set(await command.env()), {"B"})
        self.assertEqual((await command.env("B")).kind, "value")

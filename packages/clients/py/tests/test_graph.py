import asyncio
import unittest

import tangram as tg
from tangram.graph import Graph, Pointer
from tangram.mutation import UNSET
from tangram.referent import Referent


class GraphTests(unittest.IsolatedAsyncioTestCase):
    async def test_arguments_offsets_reset_and_future_builder(self):
        async def node():
            return {"kind": "file", "contents": "a"}

        graph = (
            await tg.graph()
            .node(node())
            .nodes([{"kind": "directory", "entries": {"file": 0}}])
        )
        self.assertEqual((await graph.nodes)[1]["entries"], {"file": 1})
        args = await Graph.arg(
            {"nodes": [{"kind": "file"}]},
            {
                "nodes": [
                    {"kind": "directory", "entries": {"self": 0}},
                    {"kind": "file", "dependencies": {"self": 0, "null": None}},
                    {"kind": "symlink", "artifact": 0},
                ]
            },
        )
        self.assertEqual(args["nodes"][1]["entries"], {"self": 1})
        self.assertEqual(args["nodes"][2]["dependencies"]["self"].node, 1)
        self.assertEqual(args["nodes"][3]["artifact"], 1)
        reset = await Graph.arg_resolved(args, {"nodes": None}, UNSET)
        self.assertEqual(reset, {"nodes": []})
        future = asyncio.get_running_loop().create_future()
        future.set_result(graph)
        self.assertIs(await Graph.new(future), graph)

    async def test_serialization_roundtrip_and_children(self):
        child = await tg.file("child")
        graph = await tg.graph(
            {
                "nodes": [
                    {"kind": "directory", "entries": {"file": 1, "external": child}},
                    {
                        "kind": "file",
                        "contents": "contents",
                        "dependencies": {
                            "local": 0,
                            "null": None,
                            "missing": {"node": None, "options": {"name": "missing"}},
                            "external": {
                                "node": child,
                                "options": {
                                    "tokens": {"local": ["proof"]},
                                    "path": "a b",
                                },
                            },
                        },
                    },
                    {"kind": "symlink", "artifact": 0},
                ]
            }
        )
        data = graph.to_data()["value"]
        file = data["nodes"][1]
        self.assertEqual(file["dependencies"]["local"], "0")
        self.assertEqual(file["dependencies"]["missing"], "?name=missing")
        self.assertEqual(file["dependencies"]["external"], child.id + "?path=a%20b")
        self.assertNotIn("executable", file)
        self.assertNotIn("module", file)
        self.assertNotIn("path", data["nodes"][2])
        restored = Graph.from_data(data)
        self.assertEqual(restored.to_data()["value"], data)
        self.assertEqual(
            Graph.Data.children(data), [child.id, file["contents"], child.id]
        )
        self.assertEqual(
            [value.id for value in await graph.children], Graph.Data.children(data)
        )

    async def test_cross_graph_pointer_and_inherited_proofs(self):
        graph = await tg.graph().node({"kind": "file", "contents": "a"})
        pointer = {"graph": graph, "index": 0, "kind": "file"}
        outer = await tg.graph().node(
            {"kind": "directory", "entries": {"child": pointer}}
        )
        edge = (await outer.nodes)[0]["entries"]["child"]
        self.assertIsInstance(edge, Pointer)
        self.assertEqual(
            Graph.Edge.from_data(Graph.Edge.to_data(edge)).graph.id, graph.id
        )
        self.assertEqual(Graph.Edge.from_data_string(edge.to_data_string()).index, 0)
        self.assertEqual(Graph.Data.Edge.children(edge.to_data_string()), [graph.id])
        outer.state.location = {"name": "region"}
        outer.state.tokens = {"local": ["proof"]}
        artifact = await outer.get(0)
        self.assertEqual(artifact.state.location, outer.state.location)
        self.assertEqual(artifact.state.tokens, outer.state.tokens)

    async def test_dependency_normalization_and_pointer_validation(self):
        graph = await tg.graph().node({"kind": "directory"})
        dependency = Referent(
            Pointer(graph, 0, "directory"),
            {
                "location": {"name": "local"},
                "tokens": {"local": ["proof"]},
                "path": "a",
            },
        )
        encoded = Graph.Dependency.to_data_string(dependency)
        self.assertNotIn("tokens", encoded)
        cleaned = Graph.Data.Dependency.without_location_and_tokens(encoded)
        self.assertEqual(cleaned, dependency.node.to_data_string() + "?path=a")
        self.assertEqual(Graph.Data.Dependency.children(cleaned), [graph.id])
        with self.assertRaises(ValueError):
            Graph.Dependency.from_data_string("0?tokens[local][0]=x")
        for index in (-1, 0.5, 2**53):
            with self.assertRaises(AssertionError):
                Graph.Edge.from_arg(index, [{}])
        with self.assertRaises(AssertionError):
            Graph.Pointer.from_arg({"graph": graph, "index": 0})
        with self.assertRaises(ValueError):
            Graph.Pointer.from_data_string(
                graph.pointer(0, "directory").to_data_string() + "&extra=x"
            )
        with self.assertRaises(AssertionError):
            await graph.get(-1)

    def test_branch_directory_data(self):
        graph = Graph([])
        pointer = Pointer(graph, 0, "directory")
        directory = {"children": [{"directory": pointer, "count": 3, "last": "z"}]}
        data = Graph.Directory.to_data(directory)
        self.assertTrue(Graph.Data.Directory.is_branch(data))
        self.assertEqual(Graph.Data.Directory.children(data), [graph.id])
        self.assertEqual(
            Graph.Directory.from_data(data)["children"][0]["directory"].graph.id,
            graph.id,
        )

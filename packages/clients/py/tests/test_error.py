import unittest

from tangram.error import Error, error
from tangram.module import Module
from tangram.object import Object
from tangram.referent import Referent


class ErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_arguments_and_fluent_future_fields(self):
        async def message():
            return "failed"

        result = await error().message(message()).values({"a": "1"}).values({"b": "2"})
        self.assertEqual(await result.message, "failed")
        self.assertEqual(await result.values, {"a": "1", "b": "2"})
        self.assertTrue(await result.stack)
        self.assertEqual(
            await Error.arg("first", {"message": "second"}), {"message": "second"}
        )
        self.assertIs(await Error.new({"stack": []}, result), result)

    async def test_sync_defaults_and_source_kind(self):
        source = error.sync("source", {"kind": "unavailable", "stack": None})
        result = error.sync("failed", {"source": Referent(source), "stack": []})
        self.assertEqual(await result.kind, "unavailable")
        self.assertEqual(await result.stack, [])
        self.assertEqual(str(Error.from_data({"message": "readable"})), "readable")
        self.assertEqual(await error.sync().values, {})
        unloaded = Error.with_id(source.id)
        self.assertIsNone(await error.sync({"source": Referent(unloaded)}).kind)

    async def test_inline_source_getter_and_data_roundtrip(self):
        data = {
            "message": "outer",
            "source": {"node": {"message": "inner", "kind": "missing"}, "options": {}},
        }
        result = Error.from_data(data)
        self.assertEqual(result.to_data(), data)
        source = await result.source
        self.assertIsInstance(source.node, Error)
        self.assertEqual(await source.node.message, "inner")
        self.assertEqual(await result.kind, "missing")
        self.assertEqual(Object.to_data(result), {"kind": "error", "value": data})
        self.assertEqual(result.to_data_or_id(), data)
        stored = Error.with_id(result.id)
        self.assertEqual(stored.to_data_or_id(), result.id)

    async def test_location_diagnostic_children_and_proof_removal(self):
        child = error.sync("child", {"stack": None})
        module = Module("object", Referent(child, {"tokens": {}, "location": None}))
        range_ = {
            "start": {"line": 0, "character": 0},
            "end": {"line": 1, "character": 0},
        }
        location = {
            "symbol": None,
            "file": {"kind": "module", "value": module},
            "range": range_,
        }
        diagnostic = {
            "message": "diagnostic",
            "severity": "error",
            "location": {"module": module, "range": range_},
        }
        object_ = {
            "location": location,
            "diagnostics": [diagnostic],
            "stack": [location],
            "source": Referent(child),
        }
        self.assertEqual(Error.Object.children(object_), [child, child, child, child])
        data = Error.Object.to_data(object_)
        self.assertEqual(Error.Data.children(data), [child.id, child.id, child.id])
        stripped = Error.Data.without_location_and_tokens(data)
        self.assertEqual(Error.Data.children(stripped), Error.Data.children(data))
        self.assertIsInstance(
            Error.Object.from_data(data)["location"]["file"]["value"], Module
        )

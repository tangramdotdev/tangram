import asyncio
import unittest

from tangram import encoding, path
from tangram.location import Arg, Location
from tangram.process.spawn import normalize_sandbox
from tangram.reference import Reference
from tangram.sandbox import Mount, Sandbox


class Utilities(unittest.TestCase):
    def test_path_components_preserve_parent_and_initial_dot(self):
        self.assertEqual(path.components("./a/./b//../"), [".", "a", "b", ".."])
        self.assertEqual(path.join("a", "/b", "..", "c"), "/b/../c")
        self.assertEqual(path.parent("/"), "")

    def test_location_multi_component_roundtrip(self):
        data = " local (us-east, eu-west), remote:cloud (ap-south), remote "
        value = Arg.from_data_string(data)
        self.assertEqual(
            Arg.to_data_string(value),
            "local(us-east,eu-west),remote:cloud(ap-south),remote",
        )
        with self.assertRaises(ValueError):
            Location.from_data_string(data)
        self.assertEqual(
            Location.from_data_string("remote:cloud(us-east)"),
            {"name": "cloud", "region": "us-east"},
        )
        for invalid in ("local()", "remote:", "local(a,)", "local(a)z"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                Arg.from_data_string(invalid)

    def test_reference_options_and_token_order(self):
        value = Reference(
            "../source",
            {
                "get": "x/y",
                "source": "tag",
                "location": Arg.from_data_string("local,remote"),
                "tokens": {"remote": ["a", "b"]},
            },
        )
        self.assertEqual(Reference.from_data_string(value.to_data_string()), value)
        self.assertEqual(Reference.from_data(value.to_data()), value)
        self.assertNotIn(
            "tokens",
            Reference.from_data_string(
                Reference.without_tokens(value.to_data_string())
            ).options,
        )
        with self.assertRaises(ValueError):
            Reference.from_data_string("x?tokens[local][1]=a")

    def test_encodings(self):
        for codec, value in (
            (encoding.base64, b"\x00\xff"),
            (encoding.hex, b"\x00\xff"),
            (encoding.utf8, "hi 🌻"),
            (encoding.json, {"a": [True, None, 2]}),
            (encoding.toml, {"a": [1, 2]}),
            (encoding.yaml, {"a": [True, None, 2]}),
        ):
            with self.subTest(codec=codec):
                self.assertEqual(codec.decode(codec.encode(value)), value)

    def test_sandbox_normalization(self):
        self.assertIsNone(normalize_sandbox({"sandbox": False}))
        self.assertEqual(normalize_sandbox({"sandbox": True}), {"ttl": 0})
        self.assertEqual(
            normalize_sandbox(
                {"sandbox": {"network": {"ports": ["80:80"]}}, "ports": ["90:90"]}
            ),
            {"ttl": 0, "network": {"kind": "bridge", "ports": ["80:80", "90:90"]}},
        )
        with self.assertRaises(ValueError):
            normalize_sandbox({"sandbox": "sbx_test", "cpu": 1})
        with self.assertRaises(ValueError):
            normalize_sandbox({"network": False, "ports": ["80:80"]})
        self.assertEqual(
            Mount.from_data_string(
                Mount.to_data_string({"source": "a", "target": "/b", "readonly": True})
            ),
            {"source": "a", "target": "/b", "readonly": True},
        )


class SandboxBuilder(unittest.IsolatedAsyncioTestCase):
    async def test_nested_awaitables_and_append(self):
        class Client:
            async def create_sandbox(self, arg):
                self.arg = arg
                return {"data": {"id": "sbx_test"}, "tokens": {"local": ["opaque"]}}

            async def try_destroy_sandbox(self, id, **options):
                self.destroyed = id

        async def future(value):
            await asyncio.sleep(0)
            return value

        client = Client()
        builder = (
            Sandbox.create({"cpu": future(2)}, client=client)
            .port(future("80:80"))
            .ports([future("90:90")])
            .ttl(future(20))
        )
        sandbox = await builder
        self.assertEqual(client.arg["cpu"], 2)
        self.assertEqual(client.arg["ttl"], 20)
        self.assertEqual(
            client.arg["network"], {"kind": "bridge", "ports": ["80:80", "90:90"]}
        )
        self.assertEqual(sandbox.tokens, {"local": ["opaque"]})
        await sandbox.close()
        self.assertEqual(client.destroyed, "sbx_test")

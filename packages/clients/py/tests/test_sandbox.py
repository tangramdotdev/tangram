"""Sandbox handles and argument conversion follow sandbox.ts."""

import unittest

from tangram.location import Arg as LocationArg
from tangram.mutation import Mutation
from tangram.sandbox import Sandbox


class Client:
    def __init__(self):
        self.calls = []
        self.output: dict = {"data": {"id": "sbx_test", "status": "created"}}

    async def create_sandbox(self, arg):
        self.calls.append(("create", arg))
        return self.output

    async def get_sandbox(self, id, **arg):
        self.calls.append(("get", id, arg))
        return self.output

    async def destroy_sandbox(self, id, **arg):
        self.calls.append(("destroy", id, arg))

    async def try_destroy_sandbox(self, id, **arg):
        self.calls.append(("try_destroy", id, arg))


class SandboxTests(unittest.IsolatedAsyncioTestCase):
    async def test_constructor_clones_and_normalizes_proofs(self):
        tokens = {"local": ["old", "old"]}
        state = {"data": {"id": "sbx_test"}, "tokens": {"local": ["new"]}}
        sandbox = Sandbox({"id": "sbx_test", "tokens": tokens, "state": state})
        self.assertEqual(tokens, {"local": ["old", "old"]})
        self.assertEqual(sandbox.tokens, {"local": ["new", "old"]})
        proofs = sandbox.tokens
        proofs["local"].clear()
        self.assertEqual(sandbox.tokens, {"local": ["new", "old"]})
        self.assertIs(sandbox.state, state)
        self.assertIs(Sandbox.expect(sandbox), sandbox)
        with self.assertRaises(AssertionError):
            Sandbox.assert_("sbx_test")
        self.assertTrue(Sandbox.Id.is_("sbx_test"))
        self.assertFalse(Sandbox.Id.is_(None))

    async def test_load_replaces_location_and_inherits_proofs(self):
        client = Client()
        location = LocationArg.from_data_string("remote:one")
        sandbox = Sandbox(
            "sbx_test", client=client, location=location, tokens={"local": ["old"]}
        )
        client.output = {
            "data": {"id": "sbx_test"},
            "tokens": {"local": ["new"]},
            "location": {"name": "two", "region": "west"},
        }
        await sandbox.load()
        self.assertEqual(client.calls[0][2]["location"], location)
        self.assertEqual(sandbox.tokens, {"local": ["new", "old"]})
        self.assertEqual(
            sandbox.location, LocationArg.from_location(client.output["location"])
        )
        self.assertEqual(client.output["tokens"], {"local": ["new"]})
        client.output = {"data": {"id": "sbx_test"}, "tokens": {"local": []}}
        await sandbox.reload()
        self.assertIsNone(sandbox.location)
        self.assertEqual(sandbox.tokens, {"local": ["new", "old"]})

    async def test_owned_disposal_and_detach(self):
        client = Client()
        async with Sandbox("sbx_test", client=client, owned=True):
            pass
        self.assertEqual(client.calls, [("try_destroy", "sbx_test", {})])
        sandbox = Sandbox("sbx_test", client=client, owned=True)
        sandbox.detach()
        await sandbox.close()
        self.assertEqual(len(client.calls), 1)
        await sandbox.destroy()
        self.assertEqual(client.calls[-1], ("destroy", "sbx_test", {}))
        self.assertFalse(sandbox.owned)

    async def test_builder_resolves_mutations_and_appends_mounts_and_ports(self):
        client = Client()

        async def memory():
            return 512

        mount = {"source": "/a", "target": "/b", "readonly": True}
        builder = Sandbox.create(client=client).memory(memory()).mount(mount)
        builder.mounts([{"source": "/c", "target": "/d"}]).network("bridge")
        builder.port("80:8080").ports(["443:8443"]).ttl(Mutation.set(42))
        sandbox = await builder
        arg = client.calls[0][1]
        self.assertEqual(arg["memory"], 512)
        self.assertEqual(arg["ttl"], 42)
        self.assertEqual(arg["mounts"], ["/a:/b,ro", "/c:/d"])
        self.assertEqual(
            arg["network"], {"kind": "bridge", "ports": ["80:8080", "443:8443"]}
        )
        self.assertTrue(sandbox.owned)
        await builder
        self.assertEqual(client.calls[1][1], arg)

    async def test_argument_and_network_conversions(self):
        self.assertEqual(
            Sandbox.Arg.to_data(
                {"cpu": None, "mounts": None, "network": None, "location": None}
            ),
            {"cpu": None, "location": None},
        )
        self.assertEqual(
            Sandbox.Arg.to_data({"network": {"ports": ["80"]}, "ports": ["81"]}),
            {"network": {"kind": "bridge", "ports": ["80", "81"]}},
        )
        for network in (False, "host"):
            with self.assertRaises(ValueError):
                Sandbox.Arg.to_data({"network": network, "ports": ["80"]})
        self.assertEqual(
            Sandbox.Network.from_data({"kind": "bridge", "ports": []}), "bridge"
        )
        self.assertEqual(
            Sandbox.Network.from_data({"kind": "bridge", "ports": ["80"]}),
            {"kind": "bridge", "ports": ["80"]},
        )
        self.assertEqual(
            Sandbox.Isolation.from_data(Sandbox.Isolation.to_data("vm")), "vm"
        )
        self.assertEqual(
            Sandbox.Mount.from_data_string("/a:/b,rw"),
            {"source": "/a", "target": "/b", "readonly": False},
        )
        for mount in ("/a", "/a:relative", "/a:/b,no"):
            with self.assertRaises(ValueError):
                Sandbox.Mount.from_data_string(mount)

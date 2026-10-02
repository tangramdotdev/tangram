"""Run matching pure-value cases through the JS and Python clients."""

import asyncio
import json
import shutil
import subprocess
import unittest
from pathlib import Path

import tangram as tg


class Strings(list):
    raw = True


class JavaScriptParity(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(shutil.which("bun"), "Bun is required for JS comparison")
    async def test_value_templates_and_mutations(self):
        script = Path(__file__).parent / "parity" / "value.mjs"
        result = await asyncio.to_thread(
            subprocess.run,
            ["bun", str(script)],
            check=True,
            capture_output=True,
            text=True,
        )
        expected = json.loads(result.stdout)
        shared = asyncio.get_running_loop().create_future()
        nested = asyncio.get_running_loop().create_future()
        nested.set_result(42)
        shared.set_result({"nested": [nested]})
        output = {}
        output["resolve"] = tg.Value.to_data(await tg.resolve([shared, shared]))
        output["value"] = tg.Value.to_data(
            {"bytes": bytes([0, 128, 255]), "null": None, "list": [True, 3, "text"]}
        )
        output["template"] = (
            await tg.Template.join(
                ":", None, asyncio.sleep(0, result="before"), await tg.template("after")
            )
        ).to_data()
        strings = Strings(["\n  before ", "\n  after\n"])
        output["tagged"] = (
            await tg.template(strings, asyncio.sleep(0, result="middle"))
        ).to_data()
        output["raw"] = (await tg.Template.raw(strings, "middle")).to_data()
        mutations = [
            tg.Mutation.unset(),
            await tg.Mutation.set(shared),
            await tg.Mutation.set_if_unset(None),
            await tg.Mutation.append([asyncio.sleep(0, result=2)]),
            await tg.Mutation.prepend([1]),
            await tg.Mutation.prefix("before", ":"),
            await tg.Mutation.suffix("after"),
            await tg.Mutation.merge({"nested": asyncio.sleep(0, result=[3])}),
        ]
        output["mutations"] = [mutation.to_data() for mutation in mutations]
        output["append"] = tg.Value.to_data(await mutations[3].apply([1]))
        output["prefix"] = tg.Value.to_data(await mutations[5].apply("body"))
        output["null"] = await mutations[2].apply(None)
        target = {"keep": None, "remove": 2, "list": [1]}
        await (
            await tg.Mutation.merge(
                {"remove": tg.Mutation.unset(), "list": await tg.Mutation.append([2])}
            )
        ).apply(target)
        output["merge"] = tg.Value.to_data(target)
        file = tg.File.with_id(
            "fil_010000000000000000000000000000000000000000000000000000"
        )
        file.location = {}
        file.tokens = {"local": ["proof"]}
        output["artifact"] = tg.Value.to_data(file)
        output["artifactTemplate"] = (
            await tg.template("prefix/", file, "/", tg.output)
        ).to_data()
        referent = tg.Referent(
            "node with spaces",
            {
                "name": "same name",
                "location": {"name": "remote name", "region": "eu"},
                "tokens": {"local": ["proof"]},
            },
        )
        output["referent"] = referent.to_data_string()
        output["location"] = tg.Location.Arg.to_data_string(
            {"components": [{}, {"name": "remote name", "regions": ["us", "eu"]}]}
        )
        for name, value in expected.items():
            with self.subTest(name=name):
                self.assertEqual(output[name], value)

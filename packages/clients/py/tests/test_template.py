import unittest

import tangram as tg
from tangram.referent import Referent
from tangram.template import Template, TemplateBuilder, unindent


class TemplateStrings(list):
    raw = True


class TemplateTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolve_and_components_copy(self):
        async def component():
            return "b"

        template = await TemplateBuilder("a", component(), None, "", "c")
        self.assertEqual(template.components, ["abc"])
        components = template.components
        components.append("changed")
        self.assertEqual(template.components, ["abc"])

    async def test_join_ignores_empty_arguments_and_reuses_future(self):
        async def component():
            return "b"

        future = component()
        builder = Template.join(":", None, "a", "", future)
        self.assertEqual((await builder).components, ["a:b"])
        self.assertEqual((await builder).components, ["a:b"])

    async def test_tagged_strings_unindent_and_raw(self):
        strings = TemplateStrings(["\n    hello ", "\n    world\n"])
        self.assertEqual(
            (await TemplateBuilder(strings, "there")).components,
            ["hello there\nworld\n"],
        )
        self.assertEqual(
            (await Template.raw(strings, "there")).components,
            ["\n    hello there\n    world\n"],
        )
        self.assertEqual(unindent(["\n   ", "\n    tail"]), ["", "\n tail"])

    async def test_data_proofs_and_objects(self):
        artifact = tg.File.with_id("fil_test")
        artifact._location = {}
        artifact.tokens = {"local": ["proof"]}
        template = Template(["prefix", artifact, tg.output])
        data = template.to_data()
        self.assertEqual(Template.Data.children(data), ["fil_test"])
        stripped = Template.Data.without_location_and_tokens(data)
        referent = Referent.from_data_string(stripped["components"][1]["value"])
        self.assertEqual(referent.options, {})
        self.assertIn("tokens", data["components"][1]["value"])
        self.assertEqual(template.objects(), [artifact])
        decoded = Template.from_data(data)
        self.assertEqual(decoded.components[0], "prefix")
        self.assertEqual(decoded.components[1].id, "fil_test")
        self.assertEqual(decoded.components[2].name, "output")

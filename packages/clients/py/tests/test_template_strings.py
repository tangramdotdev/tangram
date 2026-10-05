"""Native t-strings preserve values until the consuming builder resolves them."""

import asyncio
import sys
import unittest

from helpers import ObjectTestCase

import tangram as tg
from tangram.resolve import capture


def literal(source, **values):
    # Evaluate literals at test time so Python 3.12 and 3.13 can import this module.
    return eval(source, {"tg": tg, **values})


@unittest.skipIf(sys.version_info < (3, 14), "t-strings require Python 3.14")
class TemplateStringTests(ObjectTestCase):
    async def test_template_preserves_artifacts_and_placeholders(self):
        file = tg.File.with_id("fil_test")
        file.tokens = {"local": ["proof"]}
        pending = asyncio.sleep(0, result=file)
        value = literal('t"cat {pending} > {tg.output}"', pending=pending)
        builder = tg.template(value)
        for _ in range(2):
            template = await builder
            self.assertEqual(template.components, ["cat ", file, " > ", tg.output])
            self.assertIs(template.components[1], file)
            self.assertIs(template.components[3], tg.output)
            self.assertEqual(template.objects(), [file])
            self.assertIn("proof", template.to_data()["components"][1]["value"])

    async def test_template_unindents_only_literal_strings(self):
        name = "there\n    interpolated"
        value = literal('t"""\n    hello {name}\n    world\n"""', name=name)
        self.assertEqual(
            (await tg.template(value)).components,
            ["hello there\n    interpolated\nworld\n"],
        )
        self.assertEqual(
            (await tg.Template.raw(value)).components,
            ["\n    hello there\n    interpolated\n    world\n"],
        )
        self.assertEqual((await tg.template(literal('t""'))).components, [])
        self.assertEqual(
            (await tg.template(literal('t"literal"'))).components, ["literal"]
        )

    async def test_nested_templates_and_awaitable_arguments(self):
        pending = asyncio.sleep(0, result="value")
        inner = literal('t"<{pending}>"', pending=pending)
        value = literal('t"{inner}{pending}{tg.output}"', inner=inner, pending=pending)
        builder = tg.template(value, pending)
        for _ in range(2):
            self.assertEqual(
                (await builder).components, ["<value>value", tg.output, "value"]
            )
        self.assertEqual(
            (
                await tg.Template.new(asyncio.sleep(0, result=literal('t"hello"')))
            ).components,
            ["hello"],
        )
        self.assertEqual(
            (await tg.Template.join(":", literal('t"a"'), literal('t"b"'))).components,
            ["a:b"],
        )

    async def test_file_resolves_shared_coroutines_and_fluent_options(self):
        name = asyncio.sleep(0, result="world")
        value = literal('t"""\n    Hello, {name}!\n"""', name=name)
        dependency = tg.File("dependency")
        builder = (
            tg.file(value).contents(name).dependency("input", dependency).executable()
        )
        for _ in range(2):
            file = await builder
            self.assertEqual(await file.text(), "Hello, world!\nworld")
            self.assertTrue(await file.executable)
            self.assertIs((await file.dependencies)["input"].node, dependency)

    async def test_file_unindents_after_interpolation(self):
        name = "one\n    two"
        value = literal('t"""\n    {name}\n"""', name=name)
        file = await tg.File.new(asyncio.sleep(0, result=value))
        self.assertEqual(await file.text(), "one\ntwo\n")
        raw = await tg.File.Builder(True, value)
        self.assertEqual(await raw.text(), "\n    one\n    two\n")
        self.assertEqual(await (await tg.file(literal('t""'))).text(), "")
        self.assertEqual(
            await (await tg.file("\n    ordinary\n")).text(), "\n    ordinary\n"
        )

    async def test_rejects_implicit_stringification_and_formatting(self):
        for source in ['t"{tg.output!s}"', 't"{42!r}"', 't"{42:04d}"', 't"{42=}"']:
            for builder in [tg.template, tg.file]:
                with self.subTest(source=source, builder=builder):
                    with self.assertRaisesRegex(ValueError, "conversions and format"):
                        await builder(literal(source))
        for value in [tg.output, tg.File("contents"), 42]:
            with self.subTest(value=value):
                with self.assertRaisesRegex(TypeError, "must resolve to strings"):
                    await tg.file(literal('t"{value}"', value=value))
        with self.assertRaisesRegex(TypeError, "invalid template component"):
            await tg.template(literal('t"{42}"'))

    async def test_resolution_preserves_metadata_and_detects_cycles(self):
        pending = asyncio.sleep(0, result="hello")
        value = literal('t"<{pending!s:>8}>"', pending=pending)
        resolved = await tg.resolve(value)
        self.assertEqual(resolved.strings, value.strings)
        self.assertEqual(resolved.values, ("hello",))
        self.assertEqual(resolved.interpolations[0].expression, "pending")
        self.assertEqual(resolved.interpolations[0].conversion, "s")
        self.assertEqual(resolved.interpolations[0].format_spec, ">8")
        cycle = []
        value = literal('t"{cycle}"', cycle=cycle)
        cycle.append(value)
        with self.assertRaisesRegex(ValueError, "cycle detected"):
            await tg.resolve(capture(value))

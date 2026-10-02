import unittest

import tangram as tg
from tangram.mutation import UNSET


class MutationTests(unittest.IsolatedAsyncioTestCase):
    async def test_builder_resolves_nested_futures_and_is_reusable(self):
        async def value():
            return [42]

        builder = tg.mutation({"kind": "set", "value": {"nested": value()}})
        self.assertEqual(
            (await builder).inner, {"kind": "set", "value": {"nested": [42]}}
        )
        self.assertEqual((await builder).value, {"nested": [42]})

    async def test_inner_constructor_retains_identity(self):
        inner = {"kind": "set", "value": 42}
        mutation = tg.Mutation(inner)
        self.assertIs(mutation.inner, inner)
        inner["value"] = 43
        self.assertEqual(await mutation.apply(None), 43)

    async def test_unset_differs_from_null(self):
        mutation = await tg.Mutation.set_if_unset(42)
        self.assertEqual(await mutation.apply(), 42)
        self.assertIsNone(await mutation.apply(None))
        target = {"key": None}
        await tg.Mutation.unset().apply_to(target, "key")
        self.assertEqual(target, {})

    async def test_merge_mutates_target_and_skips_undefined(self):
        target = {"keep": 1, "remove": 2, "list": [1]}
        mutation = tg.Mutation(
            "merge",
            value={
                "keep": UNSET,
                "remove": tg.Mutation.unset(),
                "list": await tg.Mutation.append([2]),
            },
        )
        self.assertIs(await mutation.apply(target), target)
        self.assertEqual(target, {"keep": 1, "list": [1, 2]})

    async def test_array_and_template_mutations(self):
        self.assertEqual(await (await tg.Mutation.prepend([1])).apply(None), [1])
        self.assertEqual(await (await tg.Mutation.append([2])).apply([1]), [1, 2])
        self.assertEqual(
            (await (await tg.Mutation.prefix("prefix", ":")).apply("body")).components,
            ["prefix:body"],
        )
        self.assertEqual(
            (await (await tg.Mutation.suffix("suffix", ":")).apply()).components,
            ["suffix"],
        )
        with self.assertRaises(TypeError):
            await (await tg.Mutation.prefix("prefix")).apply(42)

    async def test_all_kinds_round_trip(self):
        values = [
            tg.Mutation.unset(),
            await tg.Mutation.set(42),
            await tg.Mutation.set_if_unset(None),
            await tg.Mutation.append([1]),
            await tg.Mutation.prepend([2]),
            await tg.Mutation.prefix("before", ":"),
            await tg.Mutation.suffix("after"),
            await tg.Mutation.merge({"key": "value"}),
        ]
        for mutation in values:
            data = mutation.to_data()
            self.assertEqual(tg.Mutation.from_data(data).to_data(), data)
        with self.assertRaises(ValueError):
            tg.Mutation.from_data({"kind": "unknown"})

    async def test_objects_matches_source_branches(self):
        artifact = await tg.file("hello")
        self.assertEqual((await tg.Mutation.set(artifact)).objects(), [artifact])
        self.assertEqual((await tg.Mutation.append([artifact])).objects(), [artifact])
        self.assertEqual((await tg.Mutation.prefix(artifact)).objects(), [artifact])
        self.assertEqual(
            (await tg.Mutation.merge({"artifact": artifact})).objects(), []
        )

    async def test_data_children_and_normalization(self):
        artifact = tg.File.with_id(
            "fil_010000000000000000000000000000000000000000000000000000"
        )
        artifact.tokens = {"local": ["token"]}
        artifact.location = tg.Location.from_data_string("local")
        referent = tg.Value.to_data(artifact)
        for data in [
            {"kind": "set", "value": referent},
            {"kind": "set_if_unset", "value": referent},
            {"kind": "append", "values": [referent]},
            {"kind": "prepend", "values": [referent]},
            {"kind": "merge", "value": {"key": referent}},
        ]:
            self.assertEqual(tg.Mutation.Data.children(data), [artifact.id])
            normalized = tg.Mutation.Data.without_location_and_tokens(data)
            self.assertEqual(tg.Mutation.Data.children(normalized), [artifact.id])
            self.assertNotIn("token", str(normalized))
        self.assertEqual(tg.Mutation.Data.children({"kind": "unset"}), [])

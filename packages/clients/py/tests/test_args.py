"""Match the ordered argument mapping and reduction contract in args.ts."""

import asyncio
import unittest

from tangram.args import Args
from tangram.mutation import UNSET, Mutation
from tangram.template import Template


class ArgsTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolve_concurrent_inputs_then_map_in_order(self):
        events = []
        started = asyncio.Event()
        count = 0

        async def input_(index):
            nonlocal count
            count += 1
            if count == 2:
                started.set()
            await started.wait()
            return {"items": [asyncio.sleep(0, result=index)]}

        async def map_(arg):
            events.append(arg["items"][0])
            await asyncio.sleep(0)
            return arg

        result = await Args.apply(
            [input_(1), input_(2)], map=map_, reduce={"items": "append"}
        )
        self.assertEqual(result, {"items": [1, 2]})
        self.assertEqual(events, [1, 2])

    async def test_all_mutation_reducers(self):
        cases = [
            ("set", 1, 2, 2),
            ("unset", 1, 2, UNSET),
            ("set_if_unset", 1, 2, 1),
            ("prepend", [1], [2], [2, 1]),
            ("append", [1], [2], [1, 2]),
            ("prefix", "a", "b", Template(["ba"])),
            ("suffix", "a", "b", Template(["ab"])),
            ("merge", {"a": 1}, {"b": 2}, {"a": 1, "b": 2}),
        ]
        for kind, initial, value, expected in cases:
            with self.subTest(kind=kind):
                result = await Args.apply_resolved(
                    [{"value": initial}, {"value": value}],
                    map=lambda arg: arg,
                    reduce={"value": kind},
                )
                if expected is UNSET:
                    self.assertNotIn("value", result)
                elif isinstance(expected, Template):
                    self.assertEqual(result["value"].components, expected.components)
                else:
                    self.assertEqual(result["value"], expected)

    async def test_null_and_unset_bypass_reducers(self):
        result = await Args.apply_resolved(
            [{"value": 1}, {"value": UNSET}, {"clear": None}],
            map=lambda arg: arg,
            reduce={"value": "set", "clear": "invalid"},
        )
        self.assertEqual(result, {"value": 1, "clear": None})
        result = await Args.apply_resolved(
            [{"value": 1}, {"value": None}, {"value": 2}],
            map=lambda arg: arg,
            reduce={"value": "set_if_unset"},
        )
        self.assertEqual(result, {"value": None})

    async def test_explicit_mutations_override_declared_reducer(self):
        result = await Args.apply_resolved(
            [
                {"value": [1]},
                {"value": await Mutation.append([2])},
                {"value": Mutation.unset()},
                {"value": await Mutation.set_if_unset([3])},
            ],
            map=lambda arg: arg,
            reduce={"value": "set"},
        )
        self.assertEqual(result, {"value": [3]})

    async def test_custom_reducer_retains_explicit_unset(self):
        seen = []

        async def reduce_(current, value):
            seen.append((current, value))
            return UNSET

        result = await Args.apply_resolved(
            [{"value": 1}, {"value": 2}],
            map=lambda arg: arg,
            reduce={"value": reduce_},
        )
        self.assertEqual(seen, [(UNSET, 1), (UNSET, 2)])
        self.assertIn("value", result)
        self.assertIs(result["value"], UNSET)

    async def test_custom_reducer_and_no_reducer_allow_non_values(self):
        handle = object()
        result = await Args.apply_resolved(
            [{"handle": handle}, {"value": handle}],
            map=lambda arg: arg,
            reduce={"value": lambda current, value: value},
        )
        self.assertIs(result["handle"], handle)
        self.assertIs(result["value"], handle)
        for reducer in ("set", lambda current, value: value):
            with self.subTest(reducer=reducer):
                with self.assertRaises((AssertionError, TypeError)):
                    await Args.apply_resolved(
                        [{"value": handle}, {"value": Mutation.unset()}],
                        map=lambda arg: arg,
                        reduce={"value": reducer},
                    )

    async def test_invalid_kind_and_invalid_template_reducer(self):
        with self.assertRaisesRegex(ValueError, 'unknown mutation kind "missing"'):
            await Args.apply_resolved(
                [{"value": 1}], map=lambda arg: arg, reduce={"value": "missing"}
            )
        for kind in ("prefix", "suffix"):
            with self.subTest(kind=kind):
                with self.assertRaisesRegex(TypeError, "template argument"):
                    await Args.apply_resolved(
                        [{"value": ["invalid"]}],
                        map=lambda arg: arg,
                        reduce={"value": kind},
                    )

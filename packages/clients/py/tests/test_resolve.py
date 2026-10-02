"""JS resolve semantics for nested awaitables and atomic handles."""

import asyncio

from helpers import ObjectTestCase

from tangram import Blob
from tangram.file import FileBuilder
from tangram.resolve import Resolve, resolve


class ResolveTests(ObjectTestCase):
    async def test_nested_shared_future(self):
        future = asyncio.get_running_loop().create_future()
        future.set_result({"items": [asyncio.sleep(0, result=3)]})
        self.assertEqual(
            await resolve([future, future]), [{"items": [3]}, {"items": [3]}]
        )

    async def test_shared_coroutine_builder_and_reawait(self):
        value = asyncio.sleep(0, result="hi")
        builder = FileBuilder(value).contents(value)
        first = await builder
        second = await builder
        self.assertEqual(first.id, second.id)
        self.assertEqual(await first.text(), "hihi")

    async def test_atomic_marker_presence(self):
        class Atomic:
            pass

        atomic = Atomic()
        setattr(atomic, Resolve.atomic, None)
        self.assertIs(await resolve(atomic), atomic)
        blob = Blob("hello")
        self.assertIs((await resolve({"blob": blob}))["blob"], blob)

    async def test_cycles_and_shared_noncycles(self):
        cyclic = {}
        cyclic["child"] = [cyclic]
        with self.assertRaisesRegex(ValueError, r"\.child\[0\]"):
            await resolve(cyclic)
        child = {"hello": "world"}
        self.assertEqual(await resolve([child, child]), [child, child])

    async def test_readonly_mapping_and_sequence_inputs(self):
        from collections import UserList
        from types import MappingProxyType

        value = MappingProxyType({"values": UserList([asyncio.sleep(0, result=3)])})
        self.assertEqual(await resolve(value), {"values": [3]})
        self.assertEqual(await resolve(memoryview(b"bytes")), memoryview(b"bytes"))

    async def test_builder_captures_readonly_nested_inputs(self):
        from types import MappingProxyType

        import tangram as tg

        value = asyncio.sleep(0, result=42)
        builder = tg.mutation(
            {"kind": "set", "value": MappingProxyType({"value": value})}
        )
        self.assertEqual((await builder).value, {"value": 42})
        self.assertEqual((await builder).value, {"value": 42})

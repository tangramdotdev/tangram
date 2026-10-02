"""The recursive promise and atomic contract from resolve.ts."""

import asyncio
import unittest

from tangram.mutation import UNSET
from tangram.reference import Reference
from tangram.referent import Referent
from tangram.resolve import Deferred, Resolve, capture, resolve


class ResolveNamespaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_deep_shared_promises_and_coroutines(self):
        calls = 0

        async def compute():
            nonlocal calls
            calls += 1
            return {"child": asyncio.sleep(0, result=[3, None])}

        coroutine = compute()
        future = asyncio.get_running_loop().create_future()
        future.set_result(coroutine)
        output = await resolve({"a": future, "b": [future, coroutine]})
        self.assertEqual(
            output,
            {
                "a": {"child": [3, None]},
                "b": [{"child": [3, None]}, {"child": [3, None]}],
            },
        )
        self.assertEqual(calls, 1)

    async def test_instance_fields_are_resolved_into_a_record(self):
        class Record:
            shared_class_attribute = "not an own field"

        record = Record()
        record.value = asyncio.sleep(0, result={"nested": 7})
        self.assertEqual(await resolve(record), {"value": {"nested": 7}})
        record.value = record
        with self.assertRaisesRegex(ValueError, r"cycle detected at \.value$"):
            await resolve(record)

    async def test_atomic_attribute_presence_including_inherited_marker(self):
        class Atomic:
            __tangram_atomic__ = False

        atomic = Atomic()
        atomic.child = atomic
        self.assertIs(await resolve(atomic), atomic)
        self.assertEqual(Resolve.atomic, "__tangram_atomic__")

    async def test_reference_and_referent_keep_their_python_representation(self):
        reference = Reference(
            asyncio.sleep(0, result="package"),
            {
                "name": asyncio.sleep(0, result="input"),
            },
        )
        referent = Referent(
            asyncio.sleep(0, result=reference),
            {
                "tokens": {"local": [asyncio.sleep(0, result="token")]},
            },
        )
        resolved = await resolve(referent)
        self.assertIsInstance(resolved, Referent)
        self.assertIsInstance(resolved.node, Reference)
        self.assertEqual(resolved.node.node, "package")
        self.assertEqual(resolved.node.options, {"name": "input"})
        self.assertEqual(resolved.options, {"tokens": {"local": ["token"]}})

    async def test_capture_preserves_shared_inputs_and_cycle_detection(self):
        coroutine = asyncio.sleep(0, result=5)
        captured = capture([coroutine, {"same": coroutine}])
        self.assertIs(captured[0], captured[1]["same"])
        self.assertIsInstance(captured[0], Deferred)
        self.assertEqual(await resolve(captured), [5, {"same": 5}])
        self.assertEqual(await resolve(captured), [5, {"same": 5}])

        class Record:
            pass

        record = Record()
        record.child = asyncio.sleep(0, result=9)
        captured_record = capture(record)
        self.assertEqual(await resolve(captured_record), {"child": 9})
        self.assertEqual(await resolve(captured_record), {"child": 9})
        cyclic = []
        cyclic.append(cyclic)
        with self.assertRaisesRegex(ValueError, r"cycle detected at \[0\]$"):
            await resolve(capture(cyclic))

    async def test_nested_undefined_and_function_diagnostics(self):
        with self.assertRaisesRegex(
            TypeError,
            r"invalid value to resolve at \.child: "
            r"undefined is not a value, use null instead$",
        ):
            await resolve({"child": UNSET})
        with self.assertRaisesRegex(TypeError, "require the embedded runtime"):
            await resolve(lambda: "function command")

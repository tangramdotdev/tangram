"""Exercise the constructor and read contracts translated from blob.ts."""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from tangram.blob import Blob, BlobBuilder, blob
from tangram.mutation import Mutation


class BlobTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_preserves_identity_and_collapses_children(self):
        existing = Blob("existing")
        self.assertIs(await Blob.new(existing), existing)
        self.assertIs(await blob(existing), existing)
        self.assertEqual(await (await blob()).object(), {"bytes": b""})
        one = await blob("one")
        self.assertEqual(await one.object(), {"bytes": b"one"})
        many = await blob("one", b"two")
        children = (await many.object())["children"]
        self.assertEqual([child["length"] for child in children], [3, 3])
        self.assertEqual(await many.length, 6)

    async def test_arguments_resolve_nested_futures_and_reduce_mutations(self):
        async def deferred(value):
            await asyncio.sleep(0)
            return value

        existing = Blob("existing")
        builder = blob(
            deferred(None),
            deferred("first"),
            {"children": None},
            {"children": [{"blob": deferred(existing), "length": deferred(8)}]},
            {"children": Mutation.append([{"blob": Blob("last"), "length": 4}])},
        )
        first = await builder
        second = await builder
        children = (await first.object())["children"]
        self.assertIs(children[0]["blob"], existing)
        self.assertEqual([child["length"] for child in children], [8, 4])
        self.assertEqual(first.id, second.id)
        self.assertEqual(await Blob.arg(None), {"children": []})

    async def test_forced_leaf_and_branch_forms(self):
        self.assertEqual(await (await Blob.leaf(None)).object(), {"bytes": b""})
        leaf = await Blob.leaf("a", b"b", bytearray(b"c"), memoryview(b"d"))
        self.assertEqual(await leaf.object(), {"bytes": b"abcd"})
        empty_branch = await Blob.branch()
        self.assertEqual(await empty_branch.object(), {"children": []})
        one_branch = await Blob.branch("a")
        self.assertEqual(len((await one_branch.object())["children"]), 1)
        self.assertEqual(await (await Blob.raw("  a\n")).object(), {"bytes": b"  a\n"})
        existing = Blob("existing")
        client = type("Client", (), {"read": AsyncMock(return_value=b"existing")})()
        with patch.object(Blob, "store", new=AsyncMock(return_value=existing.id)):
            leaf = await BlobBuilder.create_leaf(existing, "suffix", client=client)
        self.assertEqual(await leaf.object(), {"bytes": b"existingsuffix"})

    async def test_read_always_stores_and_forwards_options_and_state_tokens(self):
        value = Blob("local", tokens={"local": ["proof"]})
        client = type("Client", (), {"read": AsyncMock(return_value=b"result")})()
        options = {"position": "start+1", "length": 2, "size": 1, "tokens": {}}
        with patch.object(Blob, "store", new=AsyncMock(return_value=value.id)) as store:
            self.assertEqual(await value.read(options, client=client), b"result")
            store.assert_awaited_once_with(client)
        client.read.assert_awaited_once_with(
            value.id, position="start+1", length=2, size=1, tokens=value.tokens
        )
        self.assertEqual(options["tokens"], {})

    async def test_awaitable_bytes_and_text_use_replacement_utf8_decoding(self):
        value = Blob("local")
        client = type("Client", (), {"read": AsyncMock(return_value=b"a\xffb")})()
        with patch.object(Blob, "store", new=AsyncMock(return_value=value.id)):
            self.assertEqual(await value.bytes(client), b"a\xffb")
            self.assertEqual(await value.text(client), "a\ufffdb")
        with patch.object(Blob, "read", new=AsyncMock(return_value=b"text")):
            self.assertEqual(await value.bytes, b"text")
            self.assertEqual(await value.text, "text")

    async def test_tagged_template_builder_matches_js_unindent_and_raw(self):
        class Strings(list):
            raw = True

        strings = Strings(["\n  first ", "\n  last\n"])
        value = await blob(strings, "middle")
        self.assertEqual(await value.object(), {"bytes": b"first middle\nlast\n"})
        raw_value = await Blob.raw(strings, "middle")
        self.assertEqual(
            await raw_value.object(), {"bytes": b"\n  first middle\n  last\n"}
        )

    async def test_object_and_data_helpers_round_trip_children(self):
        leaf = Blob.with_object({"bytes": b"\x00\xff"})
        leaf_data = Blob.Object.to_data(await leaf.object())
        self.assertEqual(leaf_data, {"bytes": "AP8="})
        self.assertEqual(Blob.Object.from_data(leaf_data), {"bytes": b"\x00\xff"})
        self.assertEqual(Blob.Object.children(await leaf.object()), [])
        self.assertEqual(Blob.Data.children(leaf_data), [])
        branch = await Blob.branch(leaf)
        object_ = await branch.object()
        data = Blob.Object.to_data(object_)
        self.assertEqual(Blob.Object.children(object_), [leaf])
        self.assertEqual(Blob.Data.children(data), [leaf.id])
        restored = Blob.from_data(data)
        self.assertEqual(restored.id, branch.id)
        self.assertEqual(Blob.Object.children(await restored.object())[0].id, leaf.id)
        self.assertIs(Blob.expect(leaf), leaf)
        Blob.assert_(leaf)
        with self.assertRaises(TypeError):
            Blob.expect("not a blob")

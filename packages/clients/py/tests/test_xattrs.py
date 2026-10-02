import unittest
from unittest.mock import AsyncMock, patch

from tangram.file import xattrs


class XattrTests(unittest.IsolatedAsyncioTestCase):
    async def read(self, attributes, name="user.tangram.output", names=None):
        with (
            patch.object(
                xattrs.host,
                "getxattr",
                AsyncMock(side_effect=lambda path, key: attributes.get(key)),
            ) as get,
            patch.object(
                xattrs.host,
                "listxattr",
                AsyncMock(return_value=list(attributes) if names is None else names),
            ) as list_,
        ):
            result = await xattrs.read_sharded("file", name)
        self.assertEqual(get.await_args_list[0].args, ("file", name))
        list_.assert_awaited_once_with("file")
        return result

    async def test_absent_and_unsharded_attributes(self):
        self.assertIsNone(await self.read({}))
        self.assertEqual(await self.read({xattrs.OUTPUT_NAME: b"data"}), b"data")
        self.assertEqual(await self.read({xattrs.OUTPUT_NAME: b""}), b"")
        self.assertEqual(
            await self.read({xattrs.OUTPUT_NAME: b"data", "unrelated.00": b"ignored"}),
            b"data",
        )

    async def test_shards_are_sorted_numerically_and_duplicates_are_deduplicated(self):
        name = xattrs.OUTPUT_NAME
        attrs = {f"{name}.{index}": str(index).encode() for index in range(12)}
        names = list(reversed(attrs)) + [f"{name}.0"]
        self.assertEqual(await self.read(attrs, names=names), b"01234567891011")

    async def test_invalid_shard_names_follow_safe_integer_and_canonical_rules(self):
        for suffix in (
            "",
            "01",
            "-1",
            "-0",
            "+1",
            "1.0",
            "1e2",
            " 1",
            "١",
            "9007199254740992",
            "9" * 5000,
        ):
            with self.subTest(suffix=suffix):
                with self.assertRaisesRegex(ValueError, "invalid xattr shard name"):
                    await self.read({xattrs.OUTPUT_NAME + "." + suffix: b"data"})
        # The largest JS safe integer is a valid name but leaves a gap.
        with self.assertRaisesRegex(ValueError, "found a gap in the xattr shards"):
            await self.read({xattrs.OUTPUT_NAME + ".9007199254740991": b"data"})

    async def test_mixed_gap_and_disappeared_shard_errors_are_distinct(self):
        name = xattrs.OUTPUT_NAME
        with self.assertRaisesRegex(
            ValueError, "found both unsharded and sharded xattrs"
        ):
            await self.read({name: b"data", name + ".0": b"shard"})
        with self.assertRaisesRegex(ValueError, "found a gap in the xattr shards"):
            await self.read({name + ".0": b"zero", name + ".2": b"two"})
        with self.assertRaisesRegex(ValueError, "an xattr shard disappeared"):
            await self.read({}, names=[name + ".0"])

    async def test_process_wrappers_read_contents_only_for_empty_attribute(self):
        for reader, name in (
            (xattrs.read_error, xattrs.ERROR_NAME),
            (xattrs.read_outcome, xattrs.OUTCOME_NAME),
            (xattrs.read_output, xattrs.OUTPUT_NAME),
        ):
            for value in (None, b"data", b""):
                with self.subTest(reader=reader.__name__, value=value):
                    with (
                        patch.object(
                            xattrs, "read_sharded", AsyncMock(return_value=value)
                        ) as read,
                        patch.object(
                            xattrs.host,
                            "read_file",
                            AsyncMock(return_value=b"contents"),
                        ) as contents,
                    ):
                        self.assertEqual(
                            await reader("file"), b"contents" if value == b"" else value
                        )
                    read.assert_awaited_once_with("file", name)
                    if value == b"":
                        contents.assert_awaited_once_with("file")
                    else:
                        contents.assert_not_awaited()

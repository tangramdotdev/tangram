"""Directory and blob construction like checkin."""

import unittest

import tangram as tg


class DirectoryTests(unittest.IsolatedAsyncioTestCase):
    def test_with_entries_creates_a_leaf_when_the_entries_fit(self):
        directory = tg.Directory.with_entries(entries(names(1024)))
        self.assertIn("entries", value(directory))

    def test_with_entries_splits_entries_into_leaves_like_checkin(self):
        sorted_ = sorted(names(1025))
        directory = tg.Directory.with_entries(entries(sorted_))
        self.assertEqual(shape(directory), [(1024, sorted_[1023]), (1, sorted_[1024])])

    def test_with_entries_nests_branches_until_the_children_fit(self):
        sorted_ = sorted(names(1024 * 128 + 1))
        directory = tg.Directory.with_entries(entries(sorted_))
        self.assertEqual(
            shape(directory),
            [(1024 * 128, sorted_[1024 * 128 - 1]), (1, sorted_[1024 * 128])],
        )
        child = value(directory)["children"][0]["directory"]
        self.assertEqual(len(shape(child)), 128)

    async def test_new_nests_each_full_chunk_of_blob_children_in_a_branch(self):
        children = [
            {"blob": tg.Blob.with_object({"bytes": bytes([index % 256])}), "length": 1}
            for index in range(1025)
        ]
        blob = await tg.Blob.new({"children": children})
        lengths = [child["length"] for child in value(blob)["children"]]
        self.assertEqual(lengths, [1024, 1])


def names(count):
    return [str(index) for index in range(count)]


def entries(names):
    return {name: tg.Symlink.with_object({"path": name}) for name in names}


def value(object_):
    """Get an object's value without loading it."""
    return object_.state.object["value"]


def shape(directory):
    """Get the count and last name of each child of a branch."""
    return [(child["count"], child["last"]) for child in value(directory)["children"]]


if __name__ == "__main__":
    unittest.main()

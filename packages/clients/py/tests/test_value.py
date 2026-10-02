"""Check value and object compatibility with the canonical Rust serializer."""

import unittest

import tangram as tg


class ValueTests(unittest.TestCase):
    def test_primitives_and_bytes(self):
        value = {"hello": "world", "nested": [True, 42, None], "bytes": b"\x00\xff"}
        self.assertEqual(
            tg.Value.stringify(tg.Value.parse(tg.Value.stringify(value))),
            tg.Value.stringify(value),
        )

    def test_all_mutations(self):
        mutations = [
            tg.Mutation("set", value={"hello": 42}),
            tg.Mutation("set_if_unset", value=b"hello"),
            tg.Mutation("unset"),
            tg.Mutation("append", values=[42, tg.output]),
            tg.Mutation("prepend", values=["hello"]),
            tg.Mutation("prefix", template=tg.Template(["hello"]), separator=":"),
            tg.Mutation("suffix", template=tg.Template([tg.output])),
            tg.Mutation("merge", value={"hello": [42]}),
        ]
        for mutation in mutations:
            with self.subTest(kind=mutation.kind):
                self.assertEqual(
                    tg.Value.stringify(tg.Value.parse(tg.Value.stringify(mutation))),
                    tg.Value.stringify(mutation),
                )

    def test_modules(self):
        for source in ("./hello.ts", "/hello.ts", tg.File("hello")):
            module = tg.Module("ts", tg.Referent(source, {"name": "hello"}))
            parsed = tg.Value.parse(tg.Value.stringify(module))
            self.assertEqual(parsed.kind, module.kind)
            self.assertEqual(parsed.referent.options, module.referent.options)
            self.assertEqual(tg.Value.stringify(parsed), tg.Value.stringify(module))

    def test_template_artifact(self):
        value = tg.Template(["hello", tg.output, tg.File("world")])
        self.assertEqual(
            tg.Value.stringify(tg.Value.parse(tg.Value.stringify(value))),
            tg.Value.stringify(value),
        )

    def test_invalid_values(self):
        for value in (float("inf"), float("nan"), 2**53 + 1, {42: "hello"}, object()):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                tg.Value.stringify(value)

    def test_object_ids(self):
        self.assertEqual(
            tg.Blob("Hello").id,
            "blb_01zby8hmr9wc7c8t2g8c7qt29cyt8hkeg2y6y1yahh585dx6hebf2g",
        )
        values = [
            tg.Blob("hello"),
            tg.File("hello"),
            tg.Directory({"hello": tg.File("hello")}),
            tg.Symlink("hello"),
            tg.Command("sh", host=tg.host.current),
            tg.Graph(
                [
                    {"kind": "directory", "entries": {"hello": 1}},
                    {"kind": "file", "contents": tg.Blob("hello")},
                ]
            ),
            tg.Error("hello"),
        ]
        for value in values:
            with self.subTest(kind=value.kind):
                parsed = tg.Value.parse(tg.Value.stringify(value))
                self.assertEqual(parsed.id, value.id)
                self.assertIsInstance(parsed, type(value))

    def test_pointer(self):
        graph = tg.Graph([{"kind": "directory", "entries": {"self": 0}}])
        pointer = graph.pointer(0, "directory")
        self.assertEqual(
            tg.Pointer.from_data(pointer.to_data()).to_data(), pointer.to_data()
        )
        self.assertEqual(
            tg.Pointer.from_data(pointer.to_data_string()).to_data(), pointer.to_data()
        )
        self.assertTrue(pointer.artifact().id.startswith("dir_"))

    def test_referent(self):
        referent = tg.Referent(
            "hello",
            {
                "location": {"name": "example", "region": "east"},
                "path": "a/b ?c",
                "tokens": {"remote:example": ["one", "two"]},
            },
        )
        self.assertEqual(
            tg.Referent.from_data_string(referent.to_data_string()), referent
        )

    def test_checksum(self):
        self.assertEqual(
            tg.host.checksum(b"", "blake3"),
            "blake3:af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
        )


if __name__ == "__main__":
    unittest.main()

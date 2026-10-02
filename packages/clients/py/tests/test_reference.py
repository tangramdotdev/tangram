import unittest

from tangram.location import Arg
from tangram.reference import Reference


class References(unittest.TestCase):
    def test_namespace_and_complete_option_roundtrip(self):
        self.assertIs(Reference.Object, Reference)
        self.assertIs(Reference.String, str)
        options = Reference.Options(
            artifact="artifact",
            get="key/path",
            id="object",
            location={"components": [{"name": "cloud"}]},
            name="name",
            path="./path",
            source="source",
            tag="tag",
            tokens={"remote:cloud": ["first", "second"], "local": ["third"]},
        )
        value = Reference.Object(42, options)
        data = value.to_data(str)
        self.assertEqual(list(data["options"]), list(options))
        self.assertEqual(
            data["options"]["location"], Arg.to_data_string(options["location"])
        )
        self.assertEqual(Reference.from_data(data, int), value)
        string = value.to_data_string(str)
        self.assertEqual(Reference.from_data_string(string, int), value)

    def test_absent_and_unknown_options(self):
        self.assertEqual(
            Reference("node", None).to_data(), {"node": "node", "options": {}}
        )
        self.assertEqual(Reference("node", None).to_data_string(), "node")
        self.assertEqual(
            Reference("node", {"name": None, "unknown": "ignored"}).to_data(),
            {"node": "node", "options": {}},
        )
        self.assertEqual(Reference.from_data({"node": "node"}), Reference("node"))
        with self.assertRaises(AssertionError):
            Reference.from_data("node")

    def test_query_grammar_matches_javascript_split(self):
        reference = Reference.from_data_string("node?name=one=ignored?path=ignored")
        self.assertEqual(reference, Reference("node", {"name": "one"}))
        self.assertEqual(
            Reference.from_data_string("node?name=first&name=second").options,
            {"name": "second"},
        )
        self.assertEqual(
            Reference.from_data_string("node?name=a+b").options, {"name": "a+b"}
        )
        self.assertEqual(
            Reference("node", {"name": "!~*'() /"}).to_data_string(),
            "node?name=!~*'()%20%2F",
        )
        for string, error in (
            ("node?", "missing value"),
            ("node?name", "missing value"),
            ("node?unknown=%xx", "invalid key"),
            ("node?tokens[local][1]=proof", "invalid token index"),
            ("node?tokens[local][٠]=proof", "invalid key"),
        ):
            with self.subTest(string=string), self.assertRaisesRegex(ValueError, error):
                Reference.from_data_string(string)

    def test_tokens_are_sequential_per_location(self):
        value = Reference.from_data_string(
            "node?tokens[a][00]=x&tokens[b][0]=y&tokens[a][1]=z"
        )
        self.assertEqual(value.options, {"tokens": {"a": ["x", "z"], "b": ["y"]}})

    def test_without_tokens_clones_options(self):
        data = {
            "node": "node",
            "options": {"name": "name", "tokens": {"local": ["proof"]}},
        }
        copy = Reference.Data.without_tokens(data)
        self.assertEqual(copy, {"node": "node", "options": {"name": "name"}})
        self.assertIn("tokens", data["options"])
        self.assertIsNot(copy["options"], data["options"])
        self.assertEqual(
            Reference.Data.without_tokens("node?name=name&tokens[local][0]=proof"),
            "node?name=name",
        )
        self.assertEqual(
            Reference.Data.without_tokens({"node": "node"}), {"node": "node"}
        )

    def test_node_conversion_occurs_before_options(self):
        called = []
        with self.assertRaises(ValueError):
            Reference.from_data_string(
                "node?unknown=ignored", lambda node: called.append(node)
            )
        self.assertEqual(called, ["node"])

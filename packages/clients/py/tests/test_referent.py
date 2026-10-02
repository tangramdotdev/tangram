import unittest

from tangram.location import Location
from tangram.referent import Referent


class Referents(unittest.TestCase):
    def test_namespaces_and_complete_option_roundtrip(self):
        options = Referent.Options(
            artifact="artifact",
            id="object",
            location={"name": "cloud"},
            name="name",
            path="./path",
            tag="tag",
            tokens={"remote:cloud": ["first", "second"], "local": ["third"]},
        )
        value = Referent(42, options)
        data = value.to_data(str)
        self.assertEqual(list(data["options"]), list(options))
        self.assertEqual(
            data["options"]["location"], Location.to_data_string(options["location"])
        )
        self.assertEqual(Referent.from_data(data, int), value)
        self.assertEqual(
            Referent.from_data_string(value.to_data_string(str), int), value
        )
        self.assertEqual(Referent.Data.Options(name="name"), {"name": "name"})

    def test_absent_and_unknown_options(self):
        self.assertEqual(
            Referent("node", None).to_data(), {"node": "node", "options": {}}
        )
        self.assertEqual(Referent("node", None).to_data_string(), "node")
        self.assertEqual(
            Referent("node", {"name": None, "unknown": "ignored"}).to_data(),
            {"node": "node", "options": {}},
        )
        self.assertEqual(Referent.from_data({"node": "node"}), Referent("node"))
        with self.assertRaises(AssertionError):
            Referent.from_data("node")

    def test_query_grammar_and_errors(self):
        value = Referent.from_data_string("node?name=one=ignored?path=ignored")
        self.assertEqual(value, Referent("node", {"name": "one"}))
        self.assertEqual(
            Referent.from_data_string("node?name=first&name=second").options,
            {"name": "second"},
        )
        self.assertEqual(
            Referent.from_data_string("node?name=a+b").options, {"name": "a+b"}
        )
        self.assertEqual(
            Referent("node", {"name": "!~*'() /"}).to_data_string(),
            "node?name=!~*'()%20%2F",
        )
        for string, error in (
            ("node?", "missing value"),
            ("node?name", "missing value"),
            ("node?unknown=%xx", "invalid key"),
            ("node?name=%xx", "invalid URI component"),
            ("node?tokens[local][1]=%xx", "invalid token index"),
            ("node?tokens[local][٠]=proof", "invalid key"),
        ):
            with self.subTest(string=string), self.assertRaisesRegex(ValueError, error):
                Referent.from_data_string(string)

    def test_tokens_and_proof_construction(self):
        value = Referent.from_data_string(
            "node?tokens[a][00]=x&tokens[b][0]=y&tokens[a][1]=z"
        )
        self.assertEqual(value.options, {"tokens": {"a": ["x", "z"], "b": ["y"]}})
        proofs = ["proof"]
        referent = Referent.with_node_and_local_tokens("node", proofs)
        self.assertEqual(referent.options, {"tokens": {"local": proofs}})
        self.assertIsNot(referent.options["tokens"]["local"], proofs)
        self.assertEqual(Referent.with_node_and_local_tokens("node", []).options, {})
        self.assertEqual(
            Referent.with_node_and_tokens("node", {"local": []}).options, {}
        )

    def test_without_proofs_clones_only_options(self):
        proofs = {"local": ["proof"]}
        value = Referent(
            "node", {"name": "name", "tokens": proofs, "location": "local"}
        )
        copy = value.without_token()
        self.assertEqual(copy.options, {"name": "name", "location": "local"})
        self.assertIsNot(copy.options, value.options)
        self.assertIs(value.options["tokens"], proofs)
        self.assertEqual(value.without_location_and_tokens().options, {"name": "name"})
        self.assertIsNone(Referent("node", None).without_token().options)
        self.assertIsNone(Referent("node", None).without_location_and_tokens().options)

    def test_node_conversion_precedes_options(self):
        for operation, data in (
            (Referent.to_data, Referent("node", {"location": 42})),
            (Referent.to_data_string, Referent("node", {"location": 42})),
            (Referent.from_data, {"node": "node", "options": {"location": 42}}),
            (Referent.from_data_string, "node?unknown=ignored"),
        ):
            called = []
            with self.assertRaises((ValueError, TypeError)):
                operation(data, lambda node: called.append(node) or node)
            self.assertEqual(called, ["node"])

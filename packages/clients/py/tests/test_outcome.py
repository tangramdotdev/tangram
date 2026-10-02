"""Check process outcome presence, wire error forms, and inherited proofs."""

import unittest

from tangram.error import Error
from tangram.file import File
from tangram.process.outcome import Outcome
from tangram.referent import Referent
from tangram.template import Template


class OutcomeTests(unittest.TestCase):
    def test_missing_null_and_present_output_round_trip_distinctly(self):
        for source, expected in (
            ({"exit": 0}, {"error": None, "exit": 0}),
            ({"exit": 0, "error": None}, {"error": None, "exit": 0}),
            ({"exit": 0, "output": None}, {"error": None, "exit": 0, "output": None}),
            ({"exit": 7, "output": False}, {"error": None, "exit": 7, "output": False}),
        ):
            with self.subTest(source=source):
                outcome = Outcome.from_data(source)
                self.assertEqual(outcome, expected)
                data = Outcome.to_data(outcome)
                self.assertNotIn("error", data)
                self.assertEqual("output" in data, "output" in source)
                if "output" in source:
                    self.assertEqual(data["output"], source["output"])

    def test_error_id_proofs_are_decoded_and_serialized(self):
        referent = Referent(
            "err_test", {"location": {"name": "cloud"}, "tokens": {"local": ["proof"]}}
        )
        data = {"exit": 9, "error": referent.to_data_string()}
        outcome = Outcome.from_data(data)
        self.assertIsInstance(outcome["error"], Error)
        self.assertEqual(outcome["error"].id, "err_test")
        self.assertEqual(outcome["error"].state.location, {"name": "cloud"})
        self.assertEqual(outcome["error"].tokens, {"local": ["proof"]})
        self.assertEqual(Outcome.to_data(outcome), data)

    def test_inline_error_keeps_error_data_representation(self):
        data = {"exit": 1, "error": {"message": "failed"}}
        outcome = Outcome.from_data(data)
        self.assertIsInstance(outcome["error"], Error)
        self.assertFalse(outcome["error"].state.stored)
        self.assertEqual(Outcome.to_data(outcome), data)

    def test_output_object_data_and_proofs_round_trip(self):
        data = {
            "exit": 0,
            "output": {
                "kind": "map",
                "value": {
                    "file": {
                        "kind": "object",
                        "value": "fil_test?tokens[local][0]=proof",
                    }
                },
            },
        }
        outcome = Outcome.from_data(data)
        self.assertIsInstance(outcome["output"]["file"], File)
        self.assertEqual(outcome["output"]["file"].tokens, {"local": ["proof"]})
        self.assertEqual(Outcome.to_data(outcome), data)

    def test_inherit_location_preserves_existing_child_locations(self):
        error = Error.with_id("err_test")
        file = File.with_id("fil_new")
        existing = File.with_referent(
            Referent("fil_existing", {"location": {"name": "old"}})
        )
        outcome = {
            "error": error,
            "exit": 1,
            "output": {"items": [file, Template([existing])]},
        }
        location = {"name": "cloud"}
        self.assertIsNone(Outcome.inherit_location(outcome, location))
        self.assertEqual(error.state.location, location)
        self.assertEqual(file.location, location)
        self.assertEqual(existing.location, {"name": "old"})

    def test_inherit_tokens_merges_error_and_nested_output_proofs(self):
        error = Error.with_referent(
            Referent("err_test", {"tokens": {"local": ["existing"]}})
        )
        file = File.with_id("fil_test")
        outcome = {
            "error": error,
            "exit": 1,
            "output": {"items": [file, Template([file])]},
        }
        tokens = {"local": ["incoming"]}
        self.assertIsNone(Outcome.inherit_tokens(outcome, tokens))
        self.assertEqual(set(error.tokens["local"]), {"existing", "incoming"})
        self.assertEqual(file.tokens, tokens)
        self.assertEqual(tokens, {"local": ["incoming"]})
        self.assertIsNone(
            Outcome.inherit_tokens({"error": None, "exit": 0, "output": None}, tokens)
        )

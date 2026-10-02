"""Check proofs on inline command data match the JS recursive traversal."""

import unittest
from copy import deepcopy

from tangram.process.command import inherit_options, without_location_and_tokens
from tangram.referent import Referent


class ProcessCommandTests(unittest.TestCase):
    def test_referent_options_preserve_existing_proofs(self):
        command = {
            "executable": {
                "node": {"artifact": "fil_one", "path": "bin"},
                "options": {
                    "location": "remote",
                    "tokens": {"remote": ["old"]},
                    "name": "exe",
                },
            },
            "stdin": {"node": "lef_stdin", "options": {}},
        }
        before = deepcopy(command)
        output = inherit_options(
            command, {"location": {"region": None}, "tokens": {"local": ["new"]}}
        )
        self.assertEqual(command, before)
        self.assertEqual(
            output["executable"]["options"],
            {
                "location": "remote",
                "tokens": {"remote": ["old"], "local": ["new"]},
                "name": "exe",
            },
        )
        self.assertEqual(
            output["stdin"]["options"],
            {"location": "local", "tokens": {"local": ["new"]}},
        )

    def test_all_recursive_value_variants(self):
        artifact = {"kind": "object", "value": "fil_one"}
        template = {
            "components": [
                {"kind": "string", "value": "hello"},
                {"kind": "artifact", "value": "fil_one"},
            ]
        }
        values = [
            [deepcopy(artifact)],
            {"kind": "map", "value": {"a": deepcopy(artifact)}},
            {
                "kind": "module",
                "value": {"kind": "js", "referent": {"node": "fil_one", "options": {}}},
            },
            {"kind": "template", "value": deepcopy(template)},
            *[
                {
                    "kind": "mutation",
                    "value": {"kind": kind, "values": [deepcopy(artifact)]},
                }
                for kind in ("append", "prepend")
            ],
            {
                "kind": "mutation",
                "value": {"kind": "merge", "value": {"a": deepcopy(artifact)}},
            },
            *[
                {
                    "kind": "mutation",
                    "value": {"kind": kind, "value": deepcopy(artifact)},
                }
                for kind in ("set", "set_if_unset")
            ],
            *[
                {
                    "kind": "mutation",
                    "value": {"kind": kind, "template": deepcopy(template)},
                }
                for kind in ("prefix", "suffix")
            ],
            {"kind": "mutation", "value": {"kind": "unset"}},
            {"kind": "bytes", "value": "aGVsbG8="},
            {"kind": "placeholder", "value": "output"},
        ]
        command = {
            "executable": {"node": {"path": "sh"}, "options": {}},
            "args": [{"kind": "value", "value": value} for value in values],
        }
        before = deepcopy(command)
        output = inherit_options(
            command, {"location": {"region": None}, "tokens": {"local": ["proof"]}}
        )
        self.assertEqual(command, before)
        self.assertEqual(
            Referent.from_data_string(output["args"][0]["value"][0]["value"]).options,
            {"location": {}, "tokens": {"local": ["proof"]}},
        )
        self.assertEqual(
            output["args"][2]["value"]["value"]["referent"]["options"],
            {"location": "local", "tokens": {"local": ["proof"]}},
        )
        stripped = without_location_and_tokens(output)
        self.assertEqual(stripped["args"], command["args"])
        self.assertEqual(stripped["executable"], command["executable"])
        self.assertIsNone(stripped["stdin"])
        self.assertEqual(stripped["env"], {})

    def test_env_and_missing_collections(self):
        command = {
            "executable": {"node": {"path": "sh"}, "options": {}},
            "env": {
                "OBJECT": {
                    "kind": "string",
                    "value": {"kind": "object", "value": "fil_one?name=hello"},
                }
            },
        }
        output = inherit_options(command, {"tokens": {"local": ["proof"]}})
        self.assertEqual(
            Referent.from_data_string(
                output["env"]["OBJECT"]["value"]["value"]
            ).options,
            {"name": "hello", "tokens": {"local": ["proof"]}},
        )
        self.assertEqual(without_location_and_tokens(output)["env"], command["env"])
        self.assertEqual(
            without_location_and_tokens({"executable": {"node": "sh", "options": {}}}),
            {
                "executable": {"node": "sh", "options": {}},
                "args": [],
                "env": {},
                "stdin": None,
            },
        )

    def test_null_collections(self):
        command = {
            "executable": {"node": {"path": "sh"}, "options": {}},
            "args": None,
            "env": None,
        }
        output = inherit_options(command, {})
        self.assertIsNone(output["args"])
        self.assertIsNone(output["env"])
        self.assertEqual(without_location_and_tokens(output)["args"], [])
        self.assertEqual(without_location_and_tokens(output)["env"], {})

import unittest
from typing import get_args

from tangram.diagnostic import Diagnostic
from tangram.file import File
from tangram.location import Location
from tangram.module import Module
from tangram.referent import Referent


class DiagnosticTests(unittest.TestCase):
    def test_namespace_types_and_missing_location_conversion(self):
        self.assertEqual(
            get_args(Diagnostic.Severity), ("error", "warning", "info", "hint")
        )
        for severity in get_args(Diagnostic.Severity):
            value = Diagnostic(location=None, message="message", severity=severity)
            data = Diagnostic.to_data(value)
            self.assertEqual(data, {"message": "message", "severity": severity})
            self.assertEqual(Diagnostic.from_data(data), value)
            self.assertEqual(Diagnostic.children(value), [])
            self.assertEqual(Diagnostic.Data.children(data), [])
        data = Diagnostic.Data(message="message", severity="info", location=None)
        self.assertIsNone(Diagnostic.from_data(data)["location"])
        self.assertEqual(
            Diagnostic.children({"message": "message", "severity": "hint"}), []
        )

    def location(self):
        module = Module(
            "file",
            Referent(
                File.with_id("fil_example"),
                {
                    "location": Location.from_data_string("local"),
                    "tokens": {"local": ["secret"]},
                },
            ),
        )
        span = {
            "start": {"line": 1, "character": 2},
            "end": {"line": 1, "character": 5},
        }
        return Module.Location(module=module, range=span)

    def test_location_codec_and_object_children_delegate_to_module_location(self):
        location = self.location()
        diagnostic = {"location": location, "message": "message", "severity": "warning"}
        data = Diagnostic.to_data(diagnostic)
        self.assertEqual(data["location"], Module.Location.to_data(location))
        restored = Diagnostic.from_data(data)
        self.assertEqual(restored["location"]["range"], location["range"])
        self.assertEqual(Diagnostic.to_data(restored), data)
        children = Diagnostic.children(diagnostic)
        self.assertEqual([child.id for child in children], ["fil_example"])
        self.assertEqual(Diagnostic.Data.children(data), ["fil_example"])

    def test_data_proof_stripping_copies_top_level_and_preserves_other_fields(self):
        data = Diagnostic.to_data(
            {"location": self.location(), "message": "message", "severity": "error"}
        )
        data["extra"] = {"retained": True}
        stripped = Diagnostic.Data.without_location_and_tokens(data)
        self.assertIsNot(stripped, data)
        self.assertIs(stripped["extra"], data["extra"])
        self.assertEqual(stripped["location"]["range"], data["location"]["range"])
        options = stripped["location"]["module"]["referent"]["options"]
        self.assertNotIn("location", options)
        self.assertNotIn("tokens", options)
        self.assertIn("location", data["location"]["module"]["referent"]["options"])
        self.assertIn("tokens", data["location"]["module"]["referent"]["options"])

    def test_absent_or_null_location_is_preserved_in_shallow_data_copy(self):
        for data in (
            {"message": "message", "severity": "error"},
            {"location": None, "message": "message", "severity": "hint"},
        ):
            stripped = Diagnostic.Data.without_location_and_tokens(data)
            self.assertEqual(stripped, data)
            self.assertIsNot(stripped, data)

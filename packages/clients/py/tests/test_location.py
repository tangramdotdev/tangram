import unittest

from tangram.location import Location


class LocationTests(unittest.TestCase):
    def test_location_guards_exclude_argument_fields(self):
        for value in ({}, {"region": None}, {"region": "west", "extra": True}):
            self.assertTrue(Location.Local.is_(value))
            self.assertTrue(Location.is_(value))
            self.assertFalse(Location.Remote.is_(value))
        for value in ({"name": "default"}, {"name": "cloud", "region": None}):
            self.assertTrue(Location.Remote.is_(value))
            self.assertTrue(Location.is_(value))
            self.assertFalse(Location.Local.is_(value))
        for value in (
            None,
            [],
            "local",
            {"region": 1},
            {"name": None},
            {"components": []},
            {"regions": None},
            {"remote": False},
        ):
            with self.subTest(value=value):
                self.assertFalse(Location.is_(value))

    def test_component_guards_preserve_remote_field_asymmetry(self):
        arg = Location.Arg
        self.assertTrue(arg.LocalComponent.is_({}))
        self.assertTrue(arg.LocalComponent.is_({"regions": None}))
        self.assertTrue(arg.LocalComponent.is_({"regions": ["", "west"]}))
        self.assertFalse(arg.LocalComponent.is_({"remote": None}))
        # The source deliberately only rejects this field on local components.
        self.assertTrue(arg.RemoteComponent.is_({"name": "cloud", "remote": None}))
        self.assertTrue(arg.Component.is_({"name": "cloud", "remote": None}))
        self.assertTrue(arg.is_({"components": [{"name": "cloud", "remote": None}]}))
        self.assertTrue(arg.is_({"components": []}))
        for value in (
            {"components": None},
            {"components": [None]},
            {"components": [{"region": "west"}]},
            {"components": [{"regions": [1]}]},
            {"components": [{"name": 1}]},
        ):
            self.assertFalse(arg.is_(value))

    def test_location_conversion_preserves_null_and_empty_region_semantics(self):
        arg = Location.Arg
        for value, encoded in (
            ({}, "local"),
            ({"region": None}, "local"),
            ({"region": "west"}, "local(west)"),
            ({"name": "default"}, "remote"),
            ({"name": "cloud", "region": "west"}, "remote:cloud(west)"),
        ):
            self.assertEqual(Location.to_data_string(value), encoded)
            expected = {key: item for key, item in value.items() if item is not None}
            self.assertEqual(Location.from_data_string(encoded), expected)
        self.assertEqual(
            arg.from_location({"region": ""}), {"components": [{"regions": [""]}]}
        )
        self.assertEqual(
            arg.to_location({"components": [{"regions": [""]}]}), {"region": ""}
        )
        for components in ([], [{}, {}], [{"regions": []}], [{"regions": ["a", "b"]}]):
            self.assertIsNone(arg.to_location({"components": components}))
        self.assertEqual(
            arg.to_location({"components": [{"name": "cloud", "regions": None}]}),
            {"name": "cloud"},
        )

    def test_serialization_filters_empty_regions_and_omits_default_name(self):
        self.assertEqual(
            Location.Arg.to_data_string(
                {
                    "components": [
                        {"regions": ["", "a", ""]},
                        {"name": "default", "regions": [""]},
                        {"name": "cloud", "regions": ["b", "c"]},
                    ]
                }
            ),
            "local(a),remote,remote:cloud(b,c)",
        )
        self.assertEqual(Location.Arg.to_data_string({"components": []}), "")
        with self.assertRaises((TypeError, KeyError)):
            Location.Arg.to_data_string({"region": "west"})

    def test_parser_matches_source_whitespace_names_and_trailing_commas(self):
        self.assertEqual(
            Location.Arg.from_data_string("\ufeff local (a, b-2), remote:cloud_3(c), "),
            {
                "components": [
                    {"regions": ["a", "b-2"]},
                    {"name": "cloud_3", "regions": ["c"]},
                ]
            },
        )
        self.assertEqual(Location.Arg.from_data_string(" \t"), {"components": []})
        for data in (
            "local()",
            "remote:",
            "local(a,)",
            "local(a",
            "remote :a",
            "LOCAL",
            "local;remote",
            "remote:é",
            "\x85local",
            "local\x1c",
        ):
            with self.subTest(data=data):
                with self.assertRaisesRegex(ValueError, "invalid location arg"):
                    Location.Arg.from_data_string(data)
        for data in ("", "local,remote", "remote(a,b)"):
            with self.assertRaisesRegex(ValueError, "expected exactly one location"):
                Location.from_data_string(data)

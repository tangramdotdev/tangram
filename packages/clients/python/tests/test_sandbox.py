"""Sandbox resource bounds and billing units."""

import unittest

from tangram.sandbox import normalize_resource


class SandboxResourceTests(unittest.TestCase):
    def test_resource_defaults(self):
        self.assertEqual(normalize_resource(4), {"max": 4, "min": 0})
        self.assertEqual(normalize_resource({"min": 4}), {"max": 4, "min": 4})
        self.assertEqual(
            normalize_resource({"min": 0.25, "max": 1}, cpu=True),
            {"max": 1, "min": 0.25},
        )

    def test_invalid_resource_bounds(self):
        for value in ({}, {"min": 5, "max": 4}, {"max": -1}, {"other": 1}, True):
            with self.assertRaises(ValueError):
                normalize_resource(value)
        with self.assertRaises(ValueError):
            normalize_resource(0.0001, cpu=True)
        with self.assertRaises(ValueError):
            normalize_resource(0.25)

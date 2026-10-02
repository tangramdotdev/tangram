import unittest

from tangram.placeholder import Placeholder, output, placeholder
from tangram.resolve import Resolve, capture, resolve


class PlaceholderTests(unittest.TestCase):
    def test_factory_and_output(self):
        value = placeholder("custom")
        self.assertIsInstance(value, Placeholder)
        self.assertEqual(value.name, "custom")
        self.assertEqual(output.name, "output")
        self.assertIsNot(placeholder("output"), output)

    def test_names_are_read_only_and_instances_use_identity_equality(self):
        value = Placeholder("output")
        self.assertNotEqual(value, Placeholder("output"))
        self.assertEqual(value, value)
        with self.assertRaises(AttributeError):
            value.name = "other"

    def test_assert_and_expect_use_source_assertion(self):
        value = Placeholder("output")
        self.assertIs(Placeholder.expect(value), value)
        self.assertIsNone(Placeholder.assert_(value))
        for invalid in (None, "output", {"name": "output"}):
            with self.subTest(value=invalid):
                with self.assertRaisesRegex(AssertionError, "failed assertion"):
                    Placeholder.expect(invalid)
                with self.assertRaisesRegex(AssertionError, "failed assertion"):
                    Placeholder.assert_(invalid)

    def test_data_namespace_and_conversion(self):
        data = Placeholder.Data(name="custom")
        value = Placeholder.from_data(data)
        self.assertEqual(value.name, "custom")
        self.assertEqual(value.to_data(), data)
        self.assertEqual(Placeholder.to_data(value), data)
        self.assertIsNot(value.to_data(), value.to_data())


class PlaceholderResolveTests(unittest.IsolatedAsyncioTestCase):
    async def test_placeholders_are_atomic_even_with_null_marker(self):
        value = Placeholder("output")
        self.assertTrue(hasattr(value, Resolve.atomic))
        self.assertIsNone(getattr(value, Resolve.atomic))
        self.assertIs(capture(value), value)
        self.assertIs(await resolve(value), value)
        self.assertIs((await resolve({"nested": [value]}))["nested"][0], value)

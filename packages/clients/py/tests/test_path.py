import unittest

from tangram import path


class PathTests(unittest.TestCase):
    def test_components_match_source_without_normalizing_parents(self):
        for value, expected in (
            ("", ["/"]),
            ("/", ["/"]),
            ("///a//./b/../", ["/", "a", "b", ".."]),
            ("./a/./b", [".", "a", "b"]),
            ("a/../../b", ["a", "..", "..", "b"]),
            (r"a\b", [r"a\b"]),
        ):
            with self.subTest(value=value):
                self.assertEqual(path.components(value), expected)

    def test_from_components_is_a_lexical_join(self):
        self.assertEqual(path.from_components([]), "")
        self.assertEqual(path.from_components(["/"]), "/")
        self.assertEqual(path.from_components(["/", ".", "a", ".."]), "/./a/..")
        self.assertEqual(path.from_components(["a", "/", "b"]), "a///b")

    def test_join_absolute_arguments_reset_previous_components(self):
        self.assertEqual(path.join(), "")
        self.assertEqual(path.join(None, "a", None, "b"), "a/b")
        self.assertEqual(path.join("a", "/b", "..", "c"), "/b/../c")
        self.assertEqual(path.join("a", "", "b"), "a///b")

    def test_parent_and_absolute_keep_source_empty_path_semantics(self):
        for value, expected in (("", ""), ("/", ""), ("a", ""), ("a/b", "a")):
            with self.subTest(value=value):
                self.assertEqual(path.parent(value), expected)
        self.assertTrue(path.is_absolute("/a"))
        self.assertFalse(path.is_absolute(""))
        self.assertFalse(path.is_absolute(r"C:\a"))

    def test_component_namespace(self):
        self.assertEqual(path.Component.Current, ".")
        self.assertEqual(path.Component.Parent, "..")
        self.assertEqual(path.Component.Root, "/")
        for component in (".", "..", "/"):
            self.assertFalse(path.Component.is_normal(component))
        self.assertTrue(path.Component.is_normal(""))
        self.assertTrue(path.Component.is_normal("a"))

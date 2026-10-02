import unittest

from tangram.http.headers import Headers


class HeadersTests(unittest.TestCase):
    def test_get_preserves_case_and_converts_values(self):
        headers = Headers({"Name": "value", "array": ["first", "second"], "number": 42})
        self.assertEqual(headers.get("Name"), "value")
        self.assertIsNone(headers.get("name"))
        self.assertEqual(headers.get("array"), "first")
        self.assertEqual(headers.get("number"), "42")
        self.assertIsNone(Headers({"empty": []}).get("empty"))

    def test_raw_mapping_is_retained_and_headers_are_copied(self):
        values = {"name": "before"}
        headers = Headers(values)
        values["name"] = "after"
        self.assertEqual(headers.get("name"), "after")
        clone = Headers(headers)
        headers["name"] = "updated"
        self.assertEqual(values["name"], "updated")
        self.assertEqual(clone.get("name"), "after")

    def test_to_data_returns_a_shallow_copy(self):
        values = ["first", "second"]
        headers = Headers({"name": values})
        data = headers.to_data()
        data["other"] = "value"
        self.assertNotIn("other", headers)
        self.assertIs(data["name"], values)

    def test_mapping_mutation_supports_transport(self):
        headers = Headers()
        headers["authorization"] = "Bearer token"
        self.assertEqual(list(headers.items()), [("authorization", "Bearer token")])
        self.assertEqual(headers.pop("authorization"), "Bearer token")
        self.assertEqual(len(headers), 0)

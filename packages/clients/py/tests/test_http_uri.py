import unittest

from tangram.http.uri import Uri, percent_encode


class UriTests(unittest.TestCase):
    def test_string_and_object_constructors(self):
        self.assertIsNone(Uri("/path").query)
        self.assertEqual(Uri("/path?first?second").query, "first?second")
        self.assertEqual(Uri({"path": "/path"}).query, "")
        self.assertEqual(
            str(Uri({"path": "/path", "query": "raw=true"})), "/path?raw=true"
        )
        self.assertEqual(str(Uri("/path?")), "/path")

    def test_nested_query(self):
        uri = Uri(
            {
                "path": "/path",
                "query": {
                    "array": [True, None, {"key": "a b"}],
                    "object": {"nested": False},
                    "empty": None,
                },
            }
        )
        self.assertEqual(
            str(uri),
            "/path?array%5B0%5D=true&array%5B2%5D%5Bkey%5D=a%20b&object%5Bnested%5D=false",
        )

    def test_numbers(self):
        uri = Uri(
            "/",
            {
                "negative_zero": -0.0,
                "integer": 1.0,
                "small": 1e-6,
                "large": 1e21,
                "tiny": 1e-7,
            },
        )
        self.assertEqual(
            str(uri),
            "/?negative_zero=0&integer=1&small=0.000001&large=1e%2B21&tiny=1e-7",
        )

    def test_percent_encoding(self):
        self.assertEqual(
            percent_encode("AZaz09-._~ !'()/😀"),
            "AZaz09-._~%20%21%27%28%29%2F%F0%9F%98%80",
        )
        self.assertEqual(percent_encode("\ud800"), "%EF%BF%BD")
        self.assertEqual(percent_encode("\ud83d\ude00"), "%F0%9F%98%80")

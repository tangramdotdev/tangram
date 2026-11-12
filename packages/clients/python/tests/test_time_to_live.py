"""Canonical lifetime names and their input aliases."""

import unittest

from tangram.client import Client
from tangram.client.checkin import Arg
from tangram.config import validate_pool_options


class TimeToLiveTests(unittest.TestCase):
    def test_checkin_aliases(self):
        for value in (0, 1.5, None):
            for name in ("time_to_live", "ttl"):
                self.assertEqual(
                    Arg.to_json(
                        {"options": {name: value}, "path": "/source", "updates": []}
                    ),
                    {
                        "options": {
                            "tag_time_to_live": "infinite"
                            if value is None
                            else f"{value}s"
                        },
                        "path": "/source",
                    },
                )
        with self.assertRaisesRegex(ValueError, "may not both be set"):
            Arg.to_json(
                {
                    "options": {"time_to_live": 1, "ttl": 2},
                    "path": "/source",
                    "updates": [],
                }
            )

    def test_pool_aliases(self):
        for name in ("time_to_live", "ttl"):
            client = Client(pool={"max": 1, "min": 0, "shared": 1, name: 1.5})
            self.assertEqual(
                client.pool,
                {
                    "max": 1,
                    "min": 0,
                    "shared": 1,
                    "time_to_live": 1.5,
                },
            )
        with self.assertRaisesRegex(ValueError, "may not both be set"):
            Client(pool={"max": 1, "min": 0, "shared": 1, "time_to_live": 1, "ttl": 2})

    def test_pool_validation_checks_aliases(self):
        for name in ("time_to_live", "ttl"):
            for value in (-1, float("nan"), float("inf")):
                with self.assertRaisesRegex(ValueError, "invalid pool time to live"):
                    validate_pool_options(
                        {"max": 1, "min": 0, "shared": 1, name: value}
                    )
        with self.assertRaisesRegex(ValueError, "may not both be set"):
            validate_pool_options(
                {"max": 1, "min": 0, "shared": 1, "time_to_live": 1, "ttl": 2}
            )

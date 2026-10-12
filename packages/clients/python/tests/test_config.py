"""Client configuration defaults and validation."""

import math
import socket
import unittest

from tangram.client import Client, is_transport_error
from tangram.config import (
    default_config,
    default_http_config,
    validate_http_config,
)


class ConfigTests(unittest.IsolatedAsyncioTestCase):
    async def test_defaults(self):
        config = default_config()
        self.assertEqual(
            config["http"],
            {
                "coalescing_target_size": 16 * 1024,
                "connect_timeout": 1,
                "request_timeout": 60,
            },
        )
        client = Client(config)
        self.assertEqual(client.http["request_timeout"], 60)

    def test_http_timeouts(self):
        for value in (0, -1, math.nan, math.inf, "1"):
            for name in ("connect_timeout", "request_timeout"):
                with self.assertRaisesRegex(ValueError, "timeout"):
                    validate_http_config({**default_http_config(), name: value})
        validate_http_config({**default_http_config(), "connect_timeout": 0.5})

    def test_name_resolution_errors(self):
        self.assertTrue(
            is_transport_error(socket.gaierror(socket.EAI_AGAIN, "temporary failure"))
        )
        self.assertFalse(
            is_transport_error(socket.gaierror(socket.EAI_NONAME, "unknown name"))
        )

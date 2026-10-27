"""Client configuration defaults and validation."""

import math
import socket
import unittest

from tangram.client import Client, is_transport_error
from tangram.config import (
    default_config,
    default_http_config,
    default_process_attach_config,
    default_stream_retry_options,
    validate_http_config,
    validate_process_attach_config,
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
        self.assertEqual(
            config["process"], {"attach": {"max_messages": 128, "max_requests": 64}}
        )
        self.assertEqual(
            config["stream_retry"],
            {"backoff": 0.01, "jitter": 0.01, "max_delay": 1, "max_retries": 2**53 - 1},
        )
        client = Client(config)
        self.assertEqual(client.http["request_timeout"], 60)
        self.assertEqual(client.process["attach"]["max_messages"], 128)
        self.assertEqual(client.stream_retry["max_retries"], 2**53 - 1)

    def test_http_timeouts(self):
        for value in (0, -1, math.nan, math.inf, "1"):
            for name in ("connect_timeout", "request_timeout"):
                with self.assertRaisesRegex(ValueError, "timeout"):
                    validate_http_config({**default_http_config(), name: value})
        validate_http_config({**default_http_config(), "connect_timeout": 0.5})

    def test_process_attach_limits(self):
        for value in (0, -1, 1.5, True):
            for name in ("max_messages", "max_requests"):
                with self.assertRaisesRegex(
                    ValueError, f"invalid process attach {name.replace('_', ' ')}"
                ):
                    validate_process_attach_config(
                        {**default_process_attach_config(), name: value}
                    )

    async def test_stream_retry_validation(self):
        with self.assertRaisesRegex(ValueError, "invalid retry count"):
            Client(stream_retry={**default_stream_retry_options(), "max_retries": -1})
        with self.assertRaisesRegex(ValueError, "invalid retry duration"):
            Client(stream_retry={**default_stream_retry_options(), "backoff": -1})

    def test_name_resolution_errors(self):
        self.assertTrue(
            is_transport_error(socket.gaierror(socket.EAI_AGAIN, "temporary failure"))
        )
        self.assertFalse(
            is_transport_error(socket.gaierror(socket.EAI_NONAME, "unknown name"))
        )

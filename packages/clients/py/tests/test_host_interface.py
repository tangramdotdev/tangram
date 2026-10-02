import types
import unittest

from tangram import host


class HostInterfaceTests(unittest.IsolatedAsyncioTestCase):
    def test_source_operations_are_exposed(self):
        operations = (
            "http2 checksum close current disable_raw_mode enable_raw_mode exec "
            "exists get_tty_size getxattr listxattr is_foreground_controlling_tty "
            "is_tty listen_signal magic mkdtemp object_id parallelism parse_value "
            "read read_file remove signal sleep stringify_value spawn "
            "stopper_close stopper_open stopper_stop wait write write_sync"
        ).split()
        for operation in operations:
            self.assertTrue(hasattr(host, operation), operation)
            self.assertTrue(hasattr(host.default, operation), operation)
        self.assertIsInstance(host.parallelism, int)
        self.assertTrue(callable(host.http2.Session.connect))

    async def test_replacement_retains_module_identity_and_other_operations(self):
        original_read = host.read
        original_write = host.write
        module = host
        calls = []

        async def read(fd, length=None, stopper=None):
            calls.append((fd, length, stopper))
            return b"replacement"

        try:
            host.set_host({"read": read, "_private": "hidden"})
            self.assertIs(module, host)
            self.assertIs(host.write, original_write)
            self.assertEqual(await module.read(2, 7), b"replacement")
            self.assertEqual(calls, [(2, 7, None)])
            self.assertFalse(hasattr(host, "_private"))
            host.set_host(types.SimpleNamespace(read=original_read))
            self.assertIs(module.read, original_read)
        finally:
            host.set_host({"read": original_read})

    def test_source_namespace_types(self):
        self.assertIs(host.Host.SpawnArg, host.SpawnArg)
        self.assertIs(host.Host.SpawnOutput, host.SpawnOutput)
        self.assertIs(host.Host.Outcome, host.Outcome)
        self.assertIs(host.Host.MagicOutput, host.MagicOutput)
        self.assertIs(host.Host.Stopper, host.Stopper)

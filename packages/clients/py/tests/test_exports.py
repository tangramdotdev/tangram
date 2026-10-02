"""Check the Python root exports correspond to index.ts."""

import importlib
import unittest

import tangram as tg


class ExportTests(unittest.TestCase):
    def test_every_source_export_has_a_python_root_export(self):
        source_names = {
            "ArchiveFormat",
            "CompressionFormat",
            "Encoding",
            "Function",
            "Host",
            "MaybeMutation",
            "MaybeMutationMap",
            "MaybePromise",
            "MaybeReferent",
            "MutationMap",
            "Range",
            "Resolved",
            "ResolvedArgs",
            "ResolvedReturnValue",
            "ReturnValue",
            "Sync",
            "Tag",
            "Unresolved",
            "UnresolvedArgs",
            "ValueOrMaybeMutationMap",
            "Authorization",
            "Checkin",
            "Checkout",
            "Read",
            "Signal",
            "Write",
            "Args",
            "Artifact",
            "Blob",
            "Checksum",
            "Command",
            "Diagnostic",
            "Directory",
            "Error",
            "File",
            "Graph",
            "Location",
            "Module",
            "Mutation",
            "Object",
            "Placeholder",
            "Process",
            "Progress",
            "Reference",
            "Referent",
            "Request",
            "Resolve",
            "Response",
            "Sandbox",
            "Symlink",
            "Template",
            "Uri",
            "Value",
            "archive",
            "assert_",
            "blob",
            "build",
            "bundle",
            "checksum",
            "client",
            "command",
            "compress",
            "decompress",
            "directory",
            "download",
            "encoding",
            "error",
            "exec",
            "extract",
            "file",
            "graph",
            "host",
            "mutation",
            "output",
            "path",
            "placeholder",
            "process",
            "resolve",
            "run",
            "set_encoding",
            "set_host",
            "set_process",
            "sleep",
            "spawn",
            "symlink",
            "template",
            "todo",
            "unimplemented",
            "unreachable",
        }
        self.assertTrue(source_names.issubset(set(tg.__all__)))
        self.assertTrue(all(hasattr(tg, name) for name in tg.__all__))
        self.assertEqual(len(tg.__all__), len(set(tg.__all__)))

    def test_operation_aliases_are_the_process_factories(self):
        for name in ["build", "exec", "run", "spawn"]:
            self.assertEqual(getattr(tg, name), getattr(tg.Process, name))
        self.assertIs(tg.set_host, tg.host.set_host)
        self.assertIs(tg.set_encoding, tg.encoding.set_encoding)
        self.assertIs(tg.set_process, tg.process.set_process)

    def test_context_and_endpoint_namespaces_are_the_actual_source_modules(self):
        self.assertIs(tg.process, importlib.import_module("tangram.process"))
        self.assertIs(tg.host, importlib.import_module("tangram.host"))
        self.assertIs(tg.encoding, importlib.import_module("tangram.encoding"))
        for name, module in [
            ("Checkin", "tangram.client.checkin"),
            ("Checkout", "tangram.client.checkout"),
            ("Read", "tangram.client.read"),
            ("Signal", "tangram.client.process.signal"),
            ("Write", "tangram.client.write"),
        ]:
            self.assertIs(
                getattr(tg, name), getattr(importlib.import_module(module), name)
            )

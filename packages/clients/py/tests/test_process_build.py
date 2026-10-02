"""Check the build factory defaults and validation through mode changes."""

import unittest
from unittest.mock import patch

from tangram.error import Error
from tangram.mutation import UNSET
from tangram.process import process_arg_resolved
from tangram.process.build import build, validate_build
from tangram.resolve import resolve


class ProcessBuildTests(unittest.IsolatedAsyncioTestCase):
    async def test_factory_uses_run_and_retains_defaults_on_mode_changes(self):
        initial = build({"executable": "sh"})
        self.assertEqual(initial.operation, "run")
        for builder in (initial, initial.run(), initial.spawn(), initial.exec()):
            arg = await process_arg_resolved(*await resolve(builder.arguments))
            self.assertEqual(arg["sandbox"], True)
            self.assertEqual(arg["stdin"], "null")
            self.assertEqual(arg["stdout"], "log")
            self.assertEqual(arg["stderr"], "log")
            self.assertIs(arg["tty"], False)
            self.assertIs(builder._validate, validate_build)

    async def test_mode_switch_preserves_cacheability_validation(self):
        for builder in (build("true").run(), build("true").spawn()):
            builder.network(True)
            with self.assertRaisesRegex(Error, "a build must be cacheable"):
                await builder

    async def test_explicit_arguments_override_defaults(self):
        builder = build({"stdin": "pipe"}, checksum=None).stdout("pipe")
        arg = await process_arg_resolved(*await resolve(builder.arguments))
        self.assertEqual(arg["stdin"], "pipe")
        self.assertEqual(arg["stdout"], "pipe")
        validate_build(arg)

    def test_checksum_defined_null_allows_uncacheable_build(self):
        validate_build({"sandbox": False, "checksum": None})
        validate_build({"sandbox": False, "checksum": "none"})
        with self.assertRaises(Error):
            validate_build({"sandbox": False, "checksum": UNSET})

    def test_network_and_tty_validation(self):
        base = {"stdin": "null", "stdout": "log", "stderr": "log"}
        validate_build(base)
        validate_build({**base, "sandbox": {"network": {}}, "network": None})
        for extra in (
            {"network": {}},
            {"sandbox": {"network": {}}},
            {"sandbox": {"mounts": [{"source": "/tmp"}]}},
            {"mounts": [{"source": "/tmp"}]},
            {"ports": [{}]},
            {"sandbox": "existing"},
            {"tty": None},
            {"tty": 0},
        ):
            with self.subTest(extra=extra), self.assertRaises(Error):
                validate_build({**base, **extra})

    async def test_tagged_template_list_uses_shell(self):
        class Strings(list):
            raw = ["echo ", ""]

        with patch.dict("tangram.process.env", {"SHELL": "/bin/sh"}):
            builder = build(Strings(["echo ", ""]), "hello")
            arg = await process_arg_resolved(*await resolve(builder.arguments))
        self.assertEqual(arg["executable"], "/bin/sh")
        self.assertEqual(arg["args"][0].value, "-c")
        self.assertEqual(arg["args"][1].value.components, ["echo hello"])

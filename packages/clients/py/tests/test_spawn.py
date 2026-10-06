import importlib
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

from helpers import ObjectTestCase

from tangram.error import Error
from tangram.process.spawn import (
    prepare_unsandboxed_command,
    render_stdio,
    spawn_arg_from_resolved,
    spawn_unsandboxed,
    wait_unsandboxed,
)

spawn_module = importlib.import_module("tangram.process.spawn")


class SpawnTests(ObjectTestCase):
    async def test_missing_shell_keeps_the_command_executable(self):
        from tangram.process import env

        with patch.dict(env, {}, clear=True):
            output = await spawn_arg_from_resolved(
                {"command": {"executable": "tg", "args": ["py"]}},
                client=self.object_client,
            )
        self.assertEqual(
            output["arg"]["command"].node["executable"]["node"]["path"], "tg"
        )

    async def test_host_selection_stays_at_the_destination(self):
        from tangram import host
        from tangram.command import Command

        shell = await Command.arg("echo hello")
        self.assertNotIn("host", shell)
        self.assertEqual(await (await Command.new("echo hello")).host, host.current)
        for sandbox in (True, False):
            for explicit_host in (None, "aarch64-linux"):
                arg = {"executable": "sh", "sandbox": sandbox}
                if explicit_host is not None:
                    arg["host"] = explicit_host
                output = await spawn_arg_from_resolved(arg, client=self.object_client)
                command = output["arg"]["command"].node
                if explicit_host is None:
                    self.assertNotIn("host", command)
                else:
                    self.assertEqual(command["host"], explicit_host)
        command = await Command.new({"executable": "sh", "host": "existing-host"})
        output = await spawn_arg_from_resolved(
            {"command": command}, client=self.object_client
        )
        self.assertEqual(output["arg"]["command"].node["host"], "existing-host")

    async def test_function_builders_keep_futures_and_inline_commands(self):
        import asyncio

        import tangram as tg
        from tangram.process import process_arg_resolved
        from tangram.resolve import resolve

        namespace = {
            "__tangram_module__": tg.Module("py", tg.Referent(tg.File("module")))
        }
        exec("def task(*args): return args", namespace)
        for factory in (tg.build, tg.run, tg.spawn, tg.exec):
            calls = 0

            async def value():
                nonlocal calls
                calls += 1
                await asyncio.sleep(0)
                return 42

            shared = value()
            builder = factory(namespace["task"], shared, client=self.object_client).arg(
                shared
            )
            arg = await process_arg_resolved(
                *await resolve(builder.arguments), client=self.object_client
            )
            self.assertNotIn("host", arg["command"].node)
            output = await spawn_arg_from_resolved(arg, client=self.object_client)
            command = output["arg"]["command"].node
            self.assertNotIn("host", command)
            self.assertEqual(command["executable"]["node"]["path"], "tg")
            self.assertEqual(
                command["args"][4:],
                [
                    {"kind": "string", "value": "-A"},
                    {"kind": "value", "value": 42},
                    {"kind": "string", "value": "-A"},
                    {"kind": "value", "value": 42},
                ],
            )
            self.assertEqual(calls, 1)

    async def test_inline_module_command_builder_encodes_arguments(self):
        import tangram as tg
        from tangram.process import process_arg_resolved
        from tangram.resolve import resolve

        for language in ("js", "py"):
            for executable in ("tg", {"path": "tg"}):
                builder = tg.spawn(
                    {
                        "command": tg.Referent(
                            {"executable": executable, "args": [language]}
                        )
                    }
                ).arg(42)
                arg = await process_arg_resolved(*await resolve(builder.arguments))
                self.assertEqual(
                    [(value.kind, value.value) for value in arg["args"]],
                    [("string", "-A"), ("value", 42)],
                )

    async def test_inline_spawn_command_preparation(self):
        setattr(self.object_client, "arg", lambda: {"url": "http://localhost"})
        value = await spawn_arg_from_resolved(
            {
                "executable": "/bin/sh",
                "args": ["-c", "true"],
                "sandbox": False,
                "env": {"TANGRAM_JS_ENGINE": "bad", "X": 7},
            },
            client=self.object_client,
        )
        self.assertEqual(
            value["arg"]["command"].node["executable"]["node"]["path"], "/bin/sh"
        )
        prepared = await prepare_unsandboxed_command(
            value["arg"], client=self.object_client
        )
        self.addCleanup(
            lambda: importlib.import_module("shutil").rmtree(prepared["temp_path"])
        )
        self.assertEqual(prepared["args"], ["-c", "true"])
        self.assertEqual(prepared["env"]["X"], "7")
        self.assertEqual(prepared["env"]["TANGRAM_ENV_X"], "7")
        self.assertEqual(prepared["env"]["TANGRAM_URL"], "http://localhost")
        self.assertNotEqual(prepared["env"]["TANGRAM_JS_ENGINE"], "bad")

    async def test_local_error_uses_process_output(self):
        setattr(self.object_client, "arg", lambda: {})
        value = await spawn_arg_from_resolved(
            {
                "executable": "/bin/sh",
                "args": ["-c", "exit 3"],
                "sandbox": False,
                "stdin": "null",
                "stdout": "null",
                "stderr": "null",
            },
            client=self.object_client,
        )
        process = await spawn_unsandboxed(
            value["arg"], {"name": "test"}, client=self.object_client
        )
        with self.assertRaises(Error) as caught:
            await process.output()
        self.assertEqual(await caught.exception.message, "the child process failed")
        self.assertFalse(Path(process.temp_path).exists())

    async def test_outcome_xattr_precedes_other_xattrs(self):
        with tempfile.TemporaryDirectory() as temp:
            path = str(Path(temp) / "output")
            stdio = {
                name: type("Stream", (), {"close": AsyncMock()})()
                for name in ("stdin", "stdout", "stderr")
            }
            reader = AsyncMock(return_value=b'{"exit":9,"output":"done"}')
            with (
                patch.object(
                    spawn_module.host, "wait", AsyncMock(return_value={"exit": 0})
                ),
                patch.object(spawn_module.host, "exists", AsyncMock(return_value=True)),
                patch.object(spawn_module.host, "remove", AsyncMock()),
                patch.object(spawn_module, "read_outcome", reader),
            ):
                outcome = await wait_unsandboxed(
                    1, stdio, None, temp, path, client=self.object_client
                )
            self.assertEqual(outcome["exit"], 0)
            self.assertEqual(outcome["output"], "done")
            reader.assert_awaited_once_with(path)
            for stream in stdio.values():
                stream.close.assert_awaited_once()

    def test_stdio_rejections(self):
        self.assertEqual(render_stdio("pipe", "stdin"), "pipe")
        with self.assertRaisesRegex(ValueError, "log stdio"):
            render_stdio("log", "stdout")
        with self.assertRaisesRegex(ValueError, "blob stdin"):
            render_stdio("blb_fake", "stdin")

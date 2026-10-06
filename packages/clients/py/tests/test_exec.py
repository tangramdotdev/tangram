"""Check the exec factory, preparation, and process replacement contract."""

import unittest
from unittest.mock import AsyncMock, patch

from tangram.process import process_arg_resolved
from tangram.process.exec import (
    builder,
    exec_resolved,
    exec_unsandboxed,
    render_stdio,
)
from tangram.resolve import resolve


class ExecTests(unittest.IsolatedAsyncioTestCase):
    async def test_factory_accepts_futures_and_fluent_arguments(self):
        async def arg():
            return {"executable": "sh", "args": ["first"]}

        instance = builder(arg()).arg("second")
        self.assertEqual(instance.operation, "exec")
        arg = await process_arg_resolved(*await resolve(instance.arguments))
        self.assertEqual([value.value for value in arg["args"]], ["first", "second"])

    async def test_template_uses_current_process_environment(self):
        class Strings(list):
            raw = ["echo ", ""]

        for shell in ("/custom/shell", None):
            with patch("tangram.process.env", {"SHELL": shell}):
                instance = builder(Strings(["echo ", ""]), "hello")
                arg = await process_arg_resolved(*await resolve(instance.arguments))
            self.assertEqual(arg["executable"], shell or "sh")
            self.assertEqual(arg["args"][0].value, "-c")
            self.assertEqual(arg["args"][1].value.components, ["echo hello"])

    async def test_prepared_command_and_stdio_are_passed_to_host(self):
        arg = {"command": {}, "stdin": "null", "stderr": "null"}
        prepared = {
            "args": ["one"],
            "cwd": "/cwd",
            "env": {"KEY": "value"},
            "executable": "/bin/command",
        }
        with (
            patch("tangram.process.env", {"TANGRAM_OUTPUT": "/output"}),
            patch(
                "tangram.process.spawn.prepare_unsandboxed_command",
                new_callable=AsyncMock,
                return_value=prepared,
            ) as prepare,
            patch("tangram.host.exec", new_callable=AsyncMock) as host_exec,
        ):
            host_exec.return_value = "mock replacement"
            self.assertEqual(
                await exec_unsandboxed(arg, client="client"), "mock replacement"
            )
        prepare.assert_awaited_once_with(arg, "/output", client="client")
        host_exec.assert_awaited_once_with(
            {**prepared, "stdin": "null", "stdout": "inherit", "stderr": "null"}
        )

    async def test_invalid_sandbox_and_each_stdio_fail_before_preparation(self):
        with patch(
            "tangram.process.spawn.prepare_unsandboxed_command",
            new_callable=AsyncMock,
        ) as prepare:
            with self.assertRaisesRegex(ValueError, "an exec must not be sandboxed"):
                await exec_unsandboxed({"sandbox": {}})
            for stream in ("stdin", "stdout", "stderr"):
                with self.assertRaisesRegex(
                    ValueError, f"{stream} must be inherit or null for an exec"
                ):
                    await exec_unsandboxed({stream: "pipe"})
        prepare.assert_not_awaited()

    async def test_output_must_exist_in_current_process_environment(self):
        for output in (None, 123):
            with patch("tangram.process.env", {"TANGRAM_OUTPUT": output}):
                with self.assertRaisesRegex(AssertionError, "failed assertion"):
                    await exec_unsandboxed({})

    async def test_resolved_exec_preserves_validation_and_spawn_preparation(self):
        instance = builder({"executable": "sh"}).validate(lambda arg: None)
        resolved = await resolve(instance.arguments)
        arg = {"command": {}}
        with (
            patch(
                "tangram.process.spawn.spawn_arg_from_resolved",
                new_callable=AsyncMock,
                return_value={"arg": arg, "options": {}},
            ) as prepare,
            patch(
                "tangram.process.exec.exec_unsandboxed",
                new_callable=AsyncMock,
                return_value="replacement",
            ) as execute,
        ):
            self.assertEqual(await exec_resolved(instance, resolved), "replacement")
        self.assertEqual(prepare.await_args.args[0]["executable"], "sh")
        execute.assert_awaited_once_with(arg, client=instance.client)

    def test_render_stdio_rejects_unsupported_modes(self):
        self.assertEqual(render_stdio("null"), "null")
        self.assertEqual(render_stdio("inherit"), "inherit")
        with self.assertRaisesRegex(ValueError, "stdio must be inherit or null"):
            render_stdio("pipe")

    async def test_function_commands_require_a_tangram_module(self):
        instance = builder(lambda: None)
        with self.assertRaisesRegex(ValueError, "Tangram module"):
            await resolve(instance.arguments)

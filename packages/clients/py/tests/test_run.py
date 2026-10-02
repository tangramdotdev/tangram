"""Check the run factory and canonical process output handling."""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from tangram.process import process_arg_resolved
from tangram.process.run import builder, run, run_resolved
from tangram.resolve import resolve


class RunTests(unittest.IsolatedAsyncioTestCase):
    async def test_factory_retains_future_arguments_and_fluent_setters(self):
        async def arg():
            return {"executable": "sh", "args": ["first"]}

        instance = builder(arg()).arg("second").env({"A": "value"})
        self.assertEqual(instance.operation, "run")
        arg = await process_arg_resolved(*await resolve(instance.arguments))
        self.assertEqual([value.value for value in arg["args"]], ["first", "second"])
        self.assertEqual(arg["env"]["A"].value, "value")

    async def test_tagged_template_uses_process_environment(self):
        class Strings(list):
            raw = ["echo ", ""]

        with patch("tangram.process.env", {"SHELL": "/custom/sh"}):
            instance = builder(Strings(["echo ", ""]), "hello")
            arg = await process_arg_resolved(*await resolve(instance.arguments))
        self.assertEqual(arg["executable"], "/custom/sh")
        self.assertEqual(arg["args"][0].value, "-c")
        self.assertEqual(arg["args"][1].value.components, ["echo hello"])

    async def test_low_level_run_passes_options_and_returns_canonical_output(self):
        process = AsyncMock()
        process.output.return_value = "output"
        arg = {"command": {"node": {}}, "sandbox": "sandbox"}
        options = {"tokens": {"object": "token"}}
        with patch(
            "tangram.process.connect.spawn", create=True, new_callable=AsyncMock
        ) as spawn:
            spawn.return_value = process
            self.assertEqual(await run(arg, options), "output")
        spawn.assert_awaited_once_with(arg, options, "run")
        process.output.assert_awaited_once_with()

    async def test_resolved_run_finishes_forwarders_then_uses_output(self):
        process = AsyncMock()
        process.__aenter__.return_value = process
        process.output.return_value = "canonical"
        events = []

        async def output_forwarder():
            events.append("stdout")

        async def stdin_forwarder():
            try:
                await asyncio.Event().wait()
            finally:
                events.append("stdin")

        process._forwarders = [asyncio.create_task(output_forwarder())]
        process._stdin_forwarder = asyncio.create_task(stdin_forwarder())
        await asyncio.sleep(0)
        with patch(
            "tangram.process.spawn.spawn_resolved", new_callable=AsyncMock
        ) as spawn:
            spawn.return_value = process
            result = await run_resolved(object(), [])
        self.assertEqual(result, "canonical")
        self.assertEqual(events, ["stdout", "stdin"])
        process.wait.assert_awaited_once_with()
        process.output.assert_awaited_once_with()

    async def test_resolved_run_preserves_canonical_output_error(self):
        process = AsyncMock()
        process.__aenter__.return_value = process
        process._forwarders = []
        process._stdin_forwarder = None
        error = RuntimeError("the canonical child error")
        process.output.side_effect = error
        with patch(
            "tangram.process.spawn.spawn_resolved", new_callable=AsyncMock
        ) as spawn:
            spawn.return_value = process
            with self.assertRaises(RuntimeError) as caught:
                await run_resolved(object(), [])
        self.assertIs(caught.exception, error)
        process.__aexit__.assert_awaited_once()

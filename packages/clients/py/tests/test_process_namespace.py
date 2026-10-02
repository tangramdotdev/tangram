import asyncio
import unittest

from tangram.command import Command
from tangram.error import Error
from tangram.process import Process
from tangram.referent import Referent


class ProcessNamespaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_output_nested_exit_errors(self):
        for exit, message in (
            (7, "the child process failed"),
            (143, "the child process exited with signal 15"),
        ):
            process = Process(
                "prc_test",
                outcome={"error": None, "exit": exit},
                options={"name": "example"},
            )
            with self.assertRaises(Error) as caught:
                await process.output()
            error = caught.exception
            self.assertEqual(await error.message, message)
            self.assertEqual(await error.values, {"id": "prc_test", "name": "example"})
            source = await error.source
            self.assertEqual(
                await source.node.message, f"the process exited with code {exit}"
            )
            self.assertEqual(source.options, {"name": "example"})

    async def test_child_error_proof_and_name(self):
        child = await Error.new("failure")
        process = Process(
            "prc_test",
            outcome={"error": child, "exit": 1},
            options={"name": "build"},
            tokens={"local": ["proof"]},
        )
        with self.assertRaises(Error) as caught:
            await process.output()
        source = await caught.exception.source
        self.assertIs(source.node, child)
        self.assertEqual(source.options["tokens"], child.state.tokens)
        self.assertEqual(source.options["name"], "build")

    async def test_arg_recursive_futures_null_arrays(self):
        async def argument():
            return {"args": [asyncio.sleep(0, result="hello")]}

        arg = await Process.arg({"executable": "echo"}, {"args": None}, argument())
        self.assertEqual(arg["executable"], "echo")
        self.assertEqual([value.value for value in arg["args"]], ["hello"])

    async def test_wait_shared_pending_request(self):
        class Client:
            calls = 0

            async def wait_process(self, id, **options):
                self.calls += 1
                await asyncio.sleep(0)
                return {"exit": 0, "error": None, "output": "hello"}

        client = Client()
        process = Process("prc_test", client=client)
        first, second = await asyncio.gather(process.wait(), process.wait())
        self.assertIs(first, second)
        self.assertEqual(client.calls, 1)

    async def test_state_and_child_round_trip(self):
        data = {
            "command": "cmd_test?name=command",
            "created_at": 1,
            "host": "test",
            "status": "created",
            "children": [{"cached": True, "process": "prc_child?name=child"}],
            "output": "output",
        }
        state = Process.State.from_data(data)
        self.assertIsInstance(state["command"], Referent)
        self.assertEqual(state["children"][0]["process"].id, "prc_child")
        self.assertEqual(Process.State.to_data(state), data)
        stripped = Process.Data.without_location_and_tokens(
            {**data, "command": "cmd_test?tokens[local][0]=proof"}
        )
        self.assertEqual(stripped["command"], "cmd_test")

    async def test_tokens_are_cloned(self):
        tokens = {"local": ["proof"]}
        process = Process("prc_test", tokens=tokens)
        tokens["local"].append("other")
        clone = process.tokens
        clone["local"].append("another")
        self.assertEqual(process.tokens, {"local": ["proof"]})
        self.assertEqual(Process.Signal.TERM, "TERM")

    async def test_js_command_builder_encodes_argument_kinds(self):
        command = await Command.new({"executable": "tg", "args": ["js"]})
        builder = Process.spawn(command).arg("hello", {"key": "value"})
        from tangram.resolve import resolve

        args = (await resolve(builder.arguments))[-1]["args"]
        self.assertEqual(
            [value.kind for value in args], ["string", "value", "string", "value"]
        )
        self.assertEqual(args[0].value, "-A")
        self.assertEqual(args[1].value, "hello")
        self.assertEqual(args[2].value, "-A")
        self.assertEqual(args[3].value, {"key": "value"})

    async def test_process_context_shell(self):
        import tangram.process as context

        original = context.env
        try:
            context.set_process({"env": {"SHELL": "/example/shell"}})
            self.assertEqual(
                (await Process.arg("hello"))["executable"], "/example/shell"
            )
        finally:
            context.set_process({"env": original})

    async def test_write_retries_partial_chunks_at_updated_positions(self):
        calls = []

        class Connection:
            async def write(self, data):
                chunk = data["value"]
                calls.append((chunk["stream_position"], chunk["bytes"]))
                return {"length": min(7, len(chunk["bytes"])), "closed": False}

        process = Process("prc_test")
        process.connection = Connection()
        data = b"abcdefghijklmnopqrstuvwxyz"
        self.assertEqual(await process.write(data), len(data))
        self.assertEqual([position for position, _ in calls], [0, 7, 14, 21])
        self.assertEqual(
            [chunk for _, chunk in calls], [data, data[7:], data[14:], data[21:]]
        )
        self.assertEqual(process._write_position, len(data))

    async def test_location_arg_becomes_location_on_outcome_objects(self):
        from tangram.file import File
        from tangram.location import Location

        location = {"name": "remote", "region": "east"}
        arg = Location.Arg.from_location(location)
        file = File.with_id("fil_test")
        process = Process(
            "prc_test", location=arg, outcome={"error": None, "exit": 0, "output": file}
        )
        outcome = await process.wait()
        self.assertIs(outcome["output"], file)
        self.assertEqual(process.location, arg)
        self.assertEqual(file.state.location, location)
        child = {"cached": False, "options": {}, "process": process}
        restored = Process.Child.from_data(Process.Child.to_data(child))
        self.assertEqual(restored["process"].location, arg)


class ProcessLocalControlTests(unittest.IsolatedAsyncioTestCase):
    async def test_numeric_handle_cancel_and_signal_use_the_host(self):
        from unittest.mock import AsyncMock, patch

        process = Process(42, outcome={"error": None, "exit": 0})
        with patch("tangram.host.signal", new=AsyncMock()) as signal:
            await process.signal("USR1")
            await process.cancel()
        self.assertEqual(signal.await_args_list[0].args, (42, "USR1"))
        self.assertEqual(signal.await_args_list[1].args, (42, "TERM"))

    async def test_numeric_cancel_stops_then_waits_for_cleanup(self):
        from unittest.mock import AsyncMock, patch

        cleaned = []

        async def cleanup():
            cleaned.append(True)
            return {"error": None, "exit": 0}

        stopper = object()
        promise = asyncio.create_task(cleanup())
        process = Process(42, stopper=stopper, promise=promise)
        with patch("tangram.host.stopper_stop", new=AsyncMock()) as stop:
            await process.cancel()
        stop.assert_awaited_once_with(stopper)
        self.assertTrue(cleaned)

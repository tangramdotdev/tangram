import asyncio

from helpers import ObjectTestCase

from tangram.command import Command, CommandBuilder, command
from tangram.mutation import Mutation


class CommandTests(ObjectTestCase):
    async def test_python_function_commands_resolve_shared_futures_and_fluent_args(
        self,
    ):
        from tangram import host
        from tangram.file import File
        from tangram.module import Module
        from tangram.referent import Referent

        file = await File.new("module source")
        module = Module("py", Referent(file, {"path": "main.tg.py", "tag": "tools/^1"}))
        namespace = {"__tangram_module__": module}
        exec("def run(*args): return args", namespace)
        calls = 0

        async def value():
            nonlocal calls
            calls += 1
            return {"value": 42}

        shared = value()
        builder = command(namespace["run"], shared).arg(
            shared, Command.Value.string("raw")
        )
        result = await builder
        args = await result.args
        self.assertEqual(args[0].value, "py")
        self.assertEqual(args[1].value, "--export")
        self.assertEqual(args[2].value, "run")
        self.assertIsNone(args[3].value.referent.options.get("path"))
        self.assertIsNone(args[3].value.referent.options.get("tag"))
        self.assertEqual(
            [(arg.kind, arg.value) for arg in args[4:]],
            [
                ("string", "-A"),
                ("value", {"value": 42}),
                ("string", "-A"),
                ("value", {"value": 42}),
                ("string", "-a"),
                ("string", "raw"),
            ],
        )
        self.assertEqual(calls, 1)
        self.assertEqual((await builder).id, result.id)
        referent = await Command.py(namespace["run"], [])
        self.assertEqual(referent.options.get("path"), "main.tg.py")
        self.assertEqual(referent.options.get("tag"), "tools/^1")
        self.assertEqual(
            module.referent.options, {"path": "main.tg.py", "tag": "tools/^1"}
        )
        self.assertEqual(await referent.node.host, host.current)

    async def test_command_modules_exclude_referent_metadata(self):
        from tangram.file import File
        from tangram.module import Module
        from tangram.referent import Referent

        file = await File.new("module source")
        options = {
            "id": "dir_example",
            "name": "alias",
            "path": "task.tg.py",
            "tag": "tools/^1",
        }
        for kind in ("ts", "py"):
            module = Module(kind, Referent(file, options))
            result = await Command.py(Command.function(module, "default"), [])
            stored = (await result.node.args)[3].value
            self.assertIsInstance(stored, Module)
            for field, value in options.items():
                self.assertIsNone(stored.referent.options.get(field))
                self.assertEqual(result.options.get(field), value)
            self.assertEqual(module.referent.options, options)

    async def test_python_function_arguments_are_always_encoded_individually(self):
        from tangram.file import File
        from tangram.module import Module
        from tangram.referent import Referent

        namespace = {
            "__tangram_module__": Module("py", Referent(await File.new("module")))
        }
        exec("def run(*args): return args", namespace)
        for flag, value in [
            ("-a", Command.Value.string("raw")),
            ("-A", Command.Value.value(42)),
        ]:
            args = [Command.Value.string(flag), value]
            referent = await Command.py(namespace["run"], args)
            for result in [
                referent.node,
                await command(namespace["run"], *args),
                await command(namespace["run"]).arg(*args),
            ]:
                self.assertEqual(
                    [(arg.kind, arg.value) for arg in (await result.args)[4:]],
                    [
                        ("string", "-a"),
                        ("string", flag),
                        ("string", flag),
                        (value.kind, value.value),
                    ],
                )

    async def test_builder_resolves_nested_and_shared_futures(self):
        calls = 0

        async def value():
            nonlocal calls
            calls += 1
            return "hello"

        shared = value()
        builder = command({"executable": "echo"}).arg(shared).env(A=shared)
        result = await builder
        self.assertEqual([arg.value for arg in await result.args], ["hello"])
        self.assertEqual((await result.env)["A"].value, "hello")
        self.assertEqual(calls, 1)
        self.assertEqual((await builder.args(["again"])).id, (await builder).id)

    async def test_callable_arguments_and_mapper_capture(self):
        builder = Command.Builder({"executable": "echo"})
        self.assertIsInstance(builder, CommandBuilder)
        builder.env_mapper(lambda value: {"EARLY": value}).env("a")
        builder.env_mapper(lambda value: {"LATE": value}).env("b")
        result = await builder("one").args(["two"], asyncio.sleep(0, result=["three"]))
        self.assertEqual(
            [arg.value for arg in await result.args], ["one", "two", "three"]
        )
        self.assertEqual(
            {key: value.value for key, value in (await result.env).items()},
            {"EARLY": "a", "LATE": "b"},
        )

    async def test_string_shorthand_and_nullable_object(self):
        result = await Command.new("echo hello")
        self.assertEqual(await result.executable, {"artifact": None, "path": "sh"})
        self.assertEqual([arg.value for arg in await result.args], ["-c", "echo hello"])
        object = await result.object()
        self.assertEqual(object["cwd"], None)
        self.assertEqual(object["stdin"], None)
        self.assertEqual(object["user"], None)
        data = result.to_data()["value"]
        self.assertNotIn("cwd", data)
        self.assertNotIn("stdin", data)
        self.assertNotIn("user", data)
        self.assertIs(await Command.new(result), result)

    async def test_value_identity_and_env_mutation_preserve_kind(self):
        typed = Command.Value.value(["a"])
        self.assertIsInstance(typed, Command.Value)
        result = await command({"executable": "echo", "env": {"A": typed}}).env(
            {"A": await Mutation.append(["b"]), "B": None}
        )
        env = await result.env
        self.assertEqual(env["A"].kind, "value")
        self.assertEqual(env["A"].value, ["a", "b"])
        self.assertIsNone(env["B"].value)
        self.assertIs(Command.Arg.Value.to_value(typed), typed)

    async def test_js_command_encodes_arguments_once(self):
        existing = await Command.new({"executable": "tg", "args": ["js"]})
        value = Command.Value.string("text")
        result = await command(existing, {"args": [1]}).arg(value)
        args = await result.args
        self.assertEqual(
            [(arg.kind, arg.value) for arg in args],
            [
                ("string", "js"),
                ("string", "-A"),
                ("value", 1),
                ("string", "-a"),
                ("string", "text"),
            ],
        )
        result = await command(existing).args([Command.Value.string("-a"), value])
        self.assertEqual(
            [(arg.kind, arg.value) for arg in await result.args],
            [
                ("string", "js"),
                ("string", "-a"),
                ("string", "-a"),
                ("string", "-a"),
                ("string", "text"),
            ],
        )

    async def test_namespace_wire_roundtrip(self):
        result = await Command.new(
            {"executable": "echo", "args": [Command.Value.value({"a": 1})]}
        )
        object = await result.object()
        data = Command.Object.to_data(object)
        decoded = Command.Object.from_data(data)
        self.assertEqual(Command.Object.to_data(decoded), data)
        self.assertEqual(Command.Data.children(data), [])
        self.assertEqual(Command.Data.without_location_and_tokens(data), data)
        self.assertEqual(Command.Value.from_data(data["args"][0]).value, {"a": 1})
        self.assertTrue(Command.Arg.Executable.is_({"path": "echo"}))
        self.assertFalse(Command.Arg.Executable.is_({"path": 1}))

    async def test_constructor_state_and_proof_removal(self):
        from tangram.file import File
        from tangram.referent import Referent

        file = await File.new("hello")
        file.tokens = {"local": ["proof"]}
        file.location = {"region": None}
        result = await Command.new({"executable": "echo", "args": [file]})
        data = Command.Object.to_data(await result.object())
        stripped = Command.Data.without_location_and_tokens(data)
        self.assertEqual(stripped["args"][0]["value"]["value"], file.id)
        decoded = Command.Data.children(data)
        self.assertEqual(decoded, [file.id])
        self.assertTrue(
            Referent.from_data_string(data["args"][0]["value"]["value"]).options
        )
        file.tokens = {}
        file.location = None
        clone = Command({"object": await result.object(), "stored": False})
        self.assertEqual(clone.id, result.id)
        self.assertFalse(clone.state.stored)


class CommandShellTemplateTests(ObjectTestCase):
    async def test_tagged_template_uses_process_shell_and_preserves_components(self):
        from unittest.mock import patch

        from tangram.process import env
        from tangram.template import Template

        class Strings(list):
            raw = ["echo ", ""]

        with patch.dict(env, {"SHELL": "custom-shell"}):
            result = await command(Strings(["echo ", ""]), "hello")
        self.assertEqual(
            await result.executable, {"artifact": None, "path": "custom-shell"}
        )
        args = await result.args
        self.assertEqual(args[0].value, "-c")
        self.assertIsInstance(args[1].value, Template)
        self.assertEqual(args[1].value.components, ["echo hello"])

    async def test_tagged_template_invalid_shell_is_rejected(self):
        from unittest.mock import patch

        from tangram.process import env

        class Strings(list):
            raw = ["true"]

        with patch.dict(env, {"SHELL": 123}):
            with self.assertRaises(AssertionError):
                command(Strings(["true"]))

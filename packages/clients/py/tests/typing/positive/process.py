"""Consumer-facing inference without running a process or server."""

from typing import Literal, Never, assert_type

from tangram.command import Command, CommandBuilder, CommandValue
from tangram.process import Builder, Process
from tangram.process.stdio import Reader, Writer
from tangram.value import ValueType


async def typed_results(command: Command[list[str], str], process: Process[str]):
    assert_type(await process.output(), str)
    assert_type(await command.env, dict[str, CommandValue])
    assert_type(await command.env(), dict[str, CommandValue])
    assert_type(await command.env("SHELL"), CommandValue | None)
    assert_type(await process.env(), dict[str, CommandValue])
    assert_type(await process.env("SHELL"), CommandValue | None)
    assert_type(command.run(), Builder[Literal["run"], str])
    assert_type(await command.run(), str)
    assert_type(await command.spawn(), Process[str])
    assert_type(command.exec(), Builder[Literal["exec"], Never])
    assert_type(command.spawn().run().env(A="a").cwd("/"), Builder[Literal["run"], str])
    assert_type(command.spawn().spawn(), Builder[Literal["spawn"], str])


async def fluent_commands(builder: CommandBuilder[str]):
    assert_type(builder("arg").cwd("/").host("host"), CommandBuilder[str])
    assert_type(await builder, Command[list[ValueType], str])
    assert_type(await builder.spawn(), Process[str])


async def streams(reader: Reader, writer: Writer):
    assert_type(await reader.read(), bytes | None)
    assert_type(await reader.read_all(), bytes)
    assert_type(await reader.text(), str)
    assert_type(await writer.write(b"a"), int)
    assert_type(await writer.write_all(b"a"), None)


async def unresolved_fields(command: Command[list[str], str]):
    import asyncio

    from tangram.command import CommandArgObject
    from tangram.process import ArgObject

    arg: CommandArgObject = {
        "executable": asyncio.sleep(0, result="echo"),
        "args": [asyncio.sleep(0, result="hello")],
        "cwd": asyncio.sleep(0, result="/"),
    }
    process_arg: ArgObject = {
        "command": command,
        "checksum": asyncio.sleep(0, result="none"),
        "stdout": asyncio.sleep(0, result="pipe"),
    }
    assert_type(
        command.run(arg, process_arg).cwd(asyncio.sleep(0, result="/")),
        Builder[Literal["run"], str],
    )


async def function_commands():
    import asyncio

    from tangram.command import command
    from tangram.referent import Referent

    def sync(value: str) -> str:
        return value

    async def async_(value: str) -> str:
        return value

    assert_type(command(sync, "hello"), CommandBuilder[str])
    assert_type(command(async_, asyncio.sleep(0, result="hello")), CommandBuilder[str])
    assert_type(await command(async_), Command[list[ValueType], str])
    assert_type(
        await Command.py(async_, ["hello"]), Referent[Command[list[ValueType], str]]
    )


async def function_results():
    from tangram import build, run, spawn
    from tangram.command import CommandArgObject
    from tangram.referent import Referent

    async def task(value: str) -> str:
        return value

    assert_type(build(task, "hello"), Builder[Literal["run"], str])
    assert_type(run(task, "hello"), Builder[Literal["run"], str])
    assert_type(spawn(task, "hello"), Builder[Literal["spawn"], str])
    assert_type(await Command.py_arg(task, ["hello"]), Referent[CommandArgObject])

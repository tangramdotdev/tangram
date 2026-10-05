"""Replace the current process with a command."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Never

if TYPE_CHECKING:
    from ..client import Client

from .. import host
from ..assert_ import assert_
from ..command import Command
from ..resolve import resolve
from ..template import raw
from . import ArgObject, Builder, process_arg_resolved, spawn


def builder(*args, **options) -> Builder[Literal["exec"], Never]:
    if args and callable(args[0]) and not hasattr(args[0], "__await__"):
        from ..resolve import capture

        function_ = args[0]

        async def command():
            return {
                "command": await Command.python_arg(
                    function_, function_args, client=client
                )
            }

        client = options.get("client")
        builder = Builder("exec", command(), **options)
        function_args = capture(args[1:], builder._memo)
        return builder
    if args and isinstance(args[0], list) and hasattr(args[0], "raw"):
        from . import env

        strings, *placeholders = args
        template = raw(strings, *placeholders)
        executable = env.get("SHELL")
        if executable is None:
            executable = "sh"
        assert_(Command.Arg.Executable.is_(executable))
        shell_arg: ArgObject = {"executable": executable, "args": ["-c", template]}
        args = (shell_arg,)
    return Builder("exec", *args, **options)


async def exec_unsandboxed(arg, *, client: Client | None = None):
    if "sandbox" in arg:
        raise ValueError("an exec must not be sandboxed")
    for stream in ("stdin", "stdout", "stderr"):
        validate_stdio("inherit" if arg.get(stream) is None else arg[stream], stream)

    from . import env

    output_path = env.get("TANGRAM_OUTPUT")
    assert_(isinstance(output_path, str))
    prepared = await spawn.prepare_unsandboxed_command(arg, output_path, client=client)
    return await host.exec(
        {
            "args": prepared["args"],
            "cwd": prepared["cwd"],
            "env": prepared["env"],
            "executable": prepared["executable"],
            "stderr": render_stdio(
                "inherit" if arg.get("stderr") is None else arg["stderr"]
            ),
            "stdin": render_stdio(
                "inherit" if arg.get("stdin") is None else arg["stdin"]
            ),
            "stdout": render_stdio(
                "inherit" if arg.get("stdout") is None else arg["stdout"]
            ),
        }
    )


def validate_stdio(stdio, stream):
    if stdio in ("inherit", "null"):
        return
    raise ValueError(f"{stream} must be inherit or null for an exec")


def render_stdio(stdio):
    if stdio in ("inherit", "null"):
        return stdio
    raise ValueError("stdio must be inherit or null for an exec")


async def exec_resolved(builder, resolved):
    arg = await process_arg_resolved(*resolved, client=builder.client)
    if builder._validate is not None:
        result = builder._validate(arg)
        if result is not None:
            await resolve(result)
    output = await spawn.spawn_arg_from_resolved(arg, client=builder.client)
    return await exec_unsandboxed(output["arg"], client=builder.client)


exec = builder

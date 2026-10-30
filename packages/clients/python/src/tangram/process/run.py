"""Run a process and return its output."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, overload

from ..assert_ import assert_
from ..command import Command, CommandArgument
from ..resolve import Unresolved
from ..template import raw

if TYPE_CHECKING:
    from ..value import ValueType

from . import ArgObject, Builder


@overload
def builder[O: ValueType](
    function_: Callable[..., Unresolved[O]], *args: CommandArgument, **options
) -> Builder[Literal["run"], O]: ...


@overload
def builder[A, O: ValueType](
    command: Command[A, O], *args, **options
) -> Builder[Literal["run"], O]: ...


@overload
def builder(*args, **options) -> Builder[Literal["run"], ValueType]: ...


def builder(*args, **options):
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
        builder = Builder("run", command(), **options)
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
    return Builder("run", *args, **options)


async def run(arg, options=None):
    from .attach import spawn

    process = await spawn(arg, options or {}, "run")
    return await process.output()


async def run_resolved(builder, resolved):
    from . import process_arg_resolved
    from .spawn import spawn_resolved

    arg = await process_arg_resolved(*resolved, client=builder.client)
    process = await spawn_resolved(builder, resolved)
    async with process:
        # Echo the logged streams while awaiting the process.
        echoer = (
            asyncio.ensure_future(echo_stdio(process, arg))
            if arg.get("echo") is True
            else None
        )
        await process.wait()
        if getattr(process, "_forwarders", None):
            await asyncio.gather(*process._forwarders)
        forwarder = getattr(process, "_stdin_forwarder", None)
        if forwarder is not None:
            forwarder.cancel()
            await asyncio.gather(forwarder, return_exceptions=True)

        # Finish echoing the logged streams, even if the process fails.
        if echoer is not None:
            await echoer
        return await process.output()


async def echo_stdio(process, arg) -> None:
    """Echo a process's logged stdout and stderr to this process's stdout and stderr.

    The echo reads from the beginning of the process's log until it ends.
    """
    from .. import host

    streams = [stream for stream in ("stdout", "stderr") if arg.get(stream) == "log"]
    if not streams:
        return
    iterator = await process.read_stdio(streams=streams)
    async for chunk in iterator:
        await host.write(1 if chunk["stream"] == "stdout" else 2, chunk["bytes"])

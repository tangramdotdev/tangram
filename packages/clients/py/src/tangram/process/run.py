"""Run a process and return its output."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Literal, overload

from ..assert_ import assert_
from ..command import Command
from ..template import raw

if TYPE_CHECKING:
    from ..value import ValueType

from . import ArgObject, Builder


@overload
def builder[A, O: ValueType](
    command: Command[A, O], *args, **options
) -> Builder[Literal["run"], O]: ...


@overload
def builder(*args, **options) -> Builder[Literal["run"], ValueType]: ...


def builder(*args, **options):
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
    from .connect import spawn

    process = await spawn(arg, options or {}, "run")
    return await process.output()


async def run_resolved(builder, resolved):
    from .spawn import spawn_resolved

    process = await spawn_resolved(builder, resolved)
    async with process:
        await process.wait()
        if getattr(process, "_forwarders", None):
            await asyncio.gather(*process._forwarders)
        forwarder = getattr(process, "_stdin_forwarder", None)
        if forwarder is not None:
            forwarder.cancel()
            await asyncio.gather(forwarder, return_exceptions=True)
        return await process.output()

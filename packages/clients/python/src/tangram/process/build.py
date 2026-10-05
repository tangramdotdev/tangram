"""Build a cacheable process."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, overload

from ..command import Command, CommandArgument
from ..error import Error
from ..mutation import UNSET
from ..resolve import Unresolved, resolve
from ..template import raw

if TYPE_CHECKING:
    from ..value import ValueType

from . import ArgObject, Builder
from .run import run_resolved


@overload
def build[O: ValueType](
    function_: Callable[..., Unresolved[O]], *args: CommandArgument, **options
) -> Builder[Literal["run"], O]: ...


@overload
def build[A, O: ValueType](
    command: Command[A, O], *args, **options
) -> Builder[Literal["run"], O]: ...


@overload
def build(*args, **options) -> Builder[Literal["run"], ValueType]: ...


def build(*args, **options):
    first_arg: ArgObject = {
        "sandbox": True,
        "stderr": "log",
        "stdin": "null",
        "stdout": "log",
        "tty": False,
    }
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
        builder = Builder("run", first_arg, command(), **options).validate(
            validate_build
        )
        function_args = capture(args[1:], builder._memo)
        return builder
    if args and isinstance(args[0], list) and hasattr(args[0], "raw"):
        strings, *placeholders = args
        template = raw(strings, *placeholders)
        from ..assert_ import assert_
        from . import env

        executable = env.get("SHELL")
        if executable is None:
            executable = "sh"
        assert_(Command.Arg.Executable.is_(executable))
        shell_arg: ArgObject = {"executable": executable, "args": ["-c", template]}
        args = (shell_arg,)
    return Builder("run", first_arg, *args, **options).validate(validate_build)


async def build_resolved(builder, resolved):
    # Keep the defaults and validation on the builder when changing its operation.
    builder = build(*resolved, client=builder.client)
    resolved = await resolve(builder.arguments)
    return await run_resolved(builder, resolved)


def validate_build(arg):
    sandbox = arg.get("sandbox", UNSET)
    if sandbox is UNSET:
        sandbox = True
    sandbox_arg = sandbox if isinstance(sandbox, dict) else {}
    network = arg.get("network", UNSET)
    if network is UNSET:
        network = sandbox_arg.get("network", False)
    cacheable = (
        sandbox is not None
        and sandbox is not False
        and not isinstance(sandbox, str)
        and not sandbox_arg.get("mounts")
        and not arg.get("mounts")
        and not arg.get("ports")
        and not is_network_enabled(network)
        and arg.get("stdin") == "null"
        and arg.get("stdout") == "log"
        and arg.get("stderr") == "log"
        and (arg.get("tty", UNSET) is UNSET or arg.get("tty") is False)
    )
    cacheable = cacheable or arg.get("checksum", UNSET) is not UNSET
    if not cacheable:
        raise Error("a build must be cacheable")


def is_network_enabled(value):
    return value is not UNSET and value is not None and value is not False


builder = build

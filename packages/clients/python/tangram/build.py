"""Build builder for Tangram.

This module mirrors the JavaScript packages/clients/js/src/build.ts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Union

if TYPE_CHECKING:
    from tangram.command import Command
    from tangram.template import Template


def build(*args: Any) -> "BuildBuilder":
    """Create a build builder.

    When called with a string or Template as the first argument, creates a shell
    command that executes the string with `sh -c`.
    """
    from tangram.command import Command
    from tangram.handle import get_handle
    from tangram.process import process
    from tangram.template import Template, template as make_template

    if args and callable(args[0]):
        # Handle function case.
        func = args[0]
        remaining_args = list(args[1:]) if len(args) > 1 else []
        return BuildBuilder({
            "host": "python",
            "executable": Command.Executable.from_data(get_handle().magic(func)),
            "args": remaining_args,
        })
    elif args and hasattr(args[0], "raw"):
        # Handle template literal case (TemplateStringsArray-like).
        strings = args[0]
        placeholders = list(args[1:])
        template = make_template(strings, *placeholders)
        executable = process.get("env", {}).get("SHELL", "sh")
        return BuildBuilder({
            "executable": executable,
            "args": ["-c", template],
        })
    elif args and isinstance(args[0], (str, Template)):
        # Handle string/template case.
        from tangram.template import unindent

        shell = process.get("env", {}).get("SHELL", "sh")
        if isinstance(args[0], str):
            script = unindent(args[0])
        else:
            script = args[0]
        return BuildBuilder({
            "executable": shell,
            "args": ["-c", script],
        })
    else:
        return BuildBuilder(*args)


async def _inner(*args: Any) -> Any:
    """Execute the build."""
    import tangram as tg
    from tangram.process import Process, process

    arg = await _arg(
        {"env": {"TANGRAM_HOST": process.get("env", {}).get("TANGRAM_HOST")}},
        *args,
    )

    source_options: Dict[str, Any] = {}
    if "name" in arg:
        source_options["name"] = arg["name"]
    if "executable" in arg and isinstance(arg["executable"], dict) and "module" in arg["executable"]:
        module = arg["executable"]["module"]
        if "referent" in module and "options" in module["referent"]:
            source_options = {**module["referent"]["options"], **source_options}
            module["referent"]["options"] = {}

    command_mounts = arg.get("mounts", [])
    command_stdin = arg.get("stdin")

    command = await tg.command(
        {"args": arg.get("args")} if "args" in arg else None,
        {"cwd": arg.get("cwd")} if "cwd" in arg else None,
        {"env": arg.get("env")} if "env" in arg else None,
        {"executable": arg.get("executable")} if "executable" in arg else None,
        {"host": arg.get("host")} if "host" in arg else None,
        {"user": arg.get("user")} if "user" in arg else None,
        {"mounts": command_mounts} if command_mounts else None,
        {"stdin": command_stdin} if command_stdin else None,
    )

    checksum = arg.get("checksum")
    network = arg.get("network", False)
    if network and checksum is None:
        raise RuntimeError("a checksum is required to build with network enabled")

    command_id = await command.store()
    command_referent = {"item": command_id, "options": source_options}

    from tangram.handle import get_handle

    spawn_output = await get_handle().spawn_process({
        "checksum": checksum,
        "command": command_referent,
        "create": False,
        "mounts": [],
        "network": network,
        "parent": None,
        "remote": None,
        "retry": False,
        "stderr": None,
        "stdin": None,
        "stdout": None,
    })

    process_obj = Process(
        id=spawn_output["process"],
        remote=spawn_output.get("remote"),
    )

    wait = await process_obj.wait()

    if wait.error is not None:
        raise tg.error("the child process failed", {"source": {"item": wait.error, "options": source_options}})
    if 1 <= wait.exit < 128:
        raise tg.error(f"the process exited with code {wait.exit}")
    if wait.exit >= 128:
        raise tg.error(f"the child process exited with signal {wait.exit - 128}")

    return wait.output


async def _arg(*args: Any) -> Dict[str, Any]:
    """Process build arguments using Args.apply."""
    import tangram as tg
    from tangram.command import Command
    from tangram.process import process

    async def map_fn(arg: Any) -> Dict[str, Any]:
        if arg is None:
            return {}
        elif isinstance(arg, str) or _is_artifact(arg) or isinstance(arg, tg.Template):
            host = process.get("env", {}).get("TANGRAM_HOST")
            executable = process.get("env", {}).get("SHELL", "sh")
            return {
                "args": ["-c", arg],
                "executable": executable,
                "host": host,
            }
        elif isinstance(arg, Command):
            obj = await arg.object()
            output: Dict[str, Any] = {
                "args": obj.get("args", []),
                "env": obj.get("env", {}),
                "executable": obj.get("executable"),
                "host": obj.get("host"),
                "mounts": obj.get("mounts", []),
            }
            if obj.get("cwd") is not None:
                output["cwd"] = obj["cwd"]
            if obj.get("stdin") is not None:
                output["stdin"] = obj["stdin"]
            if obj.get("user") is not None:
                output["user"] = obj["user"]
            return output
        elif isinstance(arg, dict):
            return arg
        else:
            return {}

    return await tg.Args.apply(
        args=list(args),
        map_fn=map_fn,
        reduce_spec={
            "args": "append",
            "env": "merge",
        },
    )


def _is_artifact(value: Any) -> bool:
    """Check if value is an Artifact."""
    from tangram.directory import Directory
    from tangram.file import File
    from tangram.symlink import Symlink

    return isinstance(value, (Directory, File, Symlink))


class BuildBuilder:
    """Builder for creating and executing builds with a fluent interface."""

    def __init__(self, *args: Any) -> None:
        """Initialize the build builder."""
        self._args: List[Any] = [arg for arg in args if arg is not None]

    def arg(self, *args: Any) -> "BuildBuilder":
        """Add arguments to the build command."""
        self._args.append({"args": list(args)})
        return self

    def args(self, *args: Any) -> "BuildBuilder":
        """Add multiple argument sets."""
        for arg_set in args:
            self._args.append({"args": arg_set})
        return self

    def checksum(self, checksum: Any) -> "BuildBuilder":
        """Set the expected checksum."""
        self._args.append({"checksum": checksum})
        return self

    def cwd(self, cwd: Any) -> "BuildBuilder":
        """Set the working directory."""
        self._args.append({"cwd": cwd})
        return self

    def env(self, *envs: Any) -> "BuildBuilder":
        """Set environment variables."""
        for env in envs:
            self._args.append({"env": env})
        return self

    def executable(self, executable: Any) -> "BuildBuilder":
        """Set the executable."""
        self._args.append({"executable": executable})
        return self

    def host(self, host: Any) -> "BuildBuilder":
        """Set the host."""
        self._args.append({"host": host})
        return self

    def mount(self, *mounts: Any) -> "BuildBuilder":
        """Add mounts."""
        self._args.append({"mounts": list(mounts)})
        return self

    def mounts(self, *mounts: Any) -> "BuildBuilder":
        """Add multiple mount sets."""
        for mount_set in mounts:
            self._args.append({"mounts": mount_set})
        return self

    def named(self, name: Any) -> "BuildBuilder":
        """Set the name."""
        self._args.append({"name": name})
        return self

    def network(self, network: Any) -> "BuildBuilder":
        """Enable or disable network access."""
        self._args.append({"network": network})
        return self

    async def _execute(self) -> Any:
        """Execute the build."""
        return await _inner(*self._args)

    def __await__(self) -> Generator[Any, None, Any]:
        """Allow awaiting the builder to execute the build."""
        return self._execute().__await__()

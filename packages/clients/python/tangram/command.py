"""Command class and builder for Tangram.

This module mirrors the JavaScript packages/clients/js/src/command.ts.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Union,
)

from tangram.object_ import Object_

if TYPE_CHECKING:
    from tangram.build import BuildBuilder
    from tangram.run import RunBuilder


def command(*args: Any) -> "CommandBuilder":
    """Create a new command builder.

    When called with a string or Template as the first argument, creates a shell
    command that executes the string with `sh -c`.
    """
    from tangram.process import process
    from tangram.template import Template

    if args and callable(args[0]):
        # Handle function case.
        func = args[0]
        remaining_args = list(args[1:]) if len(args) > 1 else []
        from tangram.handle import get_handle

        return CommandBuilder({
            "host": "python",
            "executable": Command.Executable.from_data(get_handle().magic(func)),
            "args": remaining_args,
        })
    elif args and isinstance(args[0], (str, Template)):
        # Handle template string case.
        shell = process.get("env", {}).get("SHELL", "sh")
        from tangram.template import template as make_template, unindent

        if isinstance(args[0], str):
            script = unindent(args[0])
        else:
            script = args[0]
        return CommandBuilder({
            "executable": shell,
            "args": ["-c", script],
        })
    else:
        return CommandBuilder(*args)


class Command:
    """A command specifies how to run a process."""

    _state: Object_.State

    def __init__(
        self,
        *,
        id: Optional[str] = None,
        object: Optional[Dict[str, Any]] = None,
        stored: bool = False,
    ) -> None:
        """Initialize a command."""
        obj = {"kind": "command", "value": object} if object is not None else None
        self._state = Object_.State(id=id, object=obj, stored=stored)

    @property
    def state(self) -> Object_.State:
        """Get the object state."""
        return self._state

    @classmethod
    def with_id(cls, id: str) -> "Command":
        """Create a command with the given ID."""
        return cls(id=id, stored=True)

    @classmethod
    def with_object(cls, object: Dict[str, Any]) -> "Command":
        """Create a command with the given object."""
        return cls(object=object, stored=False)

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "Command":
        """Create a command from serialized data."""
        return cls.with_object(Command.Object.from_data(data))

    @classmethod
    async def new(cls, *args: Any) -> "Command":
        """Create a new command from arguments."""
        import tangram as tg
        from tangram.process import process

        arg = await cls.arg(*args)
        args_ = arg.get("args", [])
        cwd = arg.get("cwd")
        env = arg.get("env", {})

        executable: Optional[Dict[str, Any]] = None
        exec_arg = arg.get("executable")
        if _is_artifact(exec_arg):
            executable = {"artifact": exec_arg, "path": None}
        elif isinstance(exec_arg, str):
            executable = {"path": exec_arg}
        elif isinstance(exec_arg, dict):
            if "artifact" in exec_arg:
                executable = {"artifact": exec_arg["artifact"], "path": exec_arg.get("path")}
            elif "module" in exec_arg:
                executable = {"module": exec_arg["module"], "export": exec_arg.get("export")}
            elif "path" in exec_arg:
                executable = {"path": exec_arg["path"]}

        host = arg.get("host") or process.get("env", {}).get("TANGRAM_HOST")
        mounts = arg.get("mounts", [])

        if executable is None:
            raise RuntimeError("cannot create a command without an executable")
        if host is None:
            raise RuntimeError("cannot create a command without a host")

        stdin = None
        if arg.get("stdin") is not None:
            stdin = await tg.blob(arg["stdin"])

        user = arg.get("user")

        object = {
            "args": args_,
            "cwd": cwd,
            "env": env,
            "executable": executable,
            "host": host,
            "mounts": mounts,
            "stdin": stdin,
            "user": user,
        }
        return cls.with_object(object)

    @classmethod
    async def arg(cls, *args: Any) -> Dict[str, Any]:
        """Process command arguments using Args.apply."""
        import tangram as tg
        from tangram.process import process

        async def map_fn(arg: Any) -> Dict[str, Any]:
            if arg is None:
                return {}
            elif isinstance(arg, str) or _is_artifact(arg) or isinstance(arg, tg.Template):
                host = process.get("env", {}).get("TANGRAM_HOST")
                return {
                    "args": ["-c", arg],
                    "executable": "/bin/sh",
                    "host": host,
                }
            elif isinstance(arg, Command):
                return await arg.object()
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

    @staticmethod
    def expect(value: Any) -> "Command":
        """Assert that value is a Command and return it."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Command))
        return value

    @staticmethod
    def assert_(value: Any) -> None:
        """Assert that value is a Command."""
        from tangram.assert_ import assert_

        assert_(isinstance(value, Command))

    @property
    def id(self) -> str:
        """Get the command ID."""
        from tangram.assert_ import assert_

        id = self._state.id
        assert_(Object_.Id.kind(id) == "command")
        return id

    async def object(self) -> Dict[str, Any]:
        """Get the command object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "command")
        return obj.get("value", obj)

    async def load(self) -> Dict[str, Any]:
        """Load the command object."""
        from tangram.assert_ import assert_

        obj = await self._state.load()
        assert_(obj.get("kind") == "command")
        return obj.get("value", obj)

    def unload(self) -> None:
        """Unload the command data."""
        self._state.unload()

    async def store(self) -> str:
        """Store the command and return its ID."""
        import tangram as tg

        await tg.Value.store(self)
        return self.id

    async def children(self) -> List[Object_]:
        """Get child objects."""
        return await self._state.children()

    async def args(self) -> List[Any]:
        """Get the command arguments."""
        return (await self.object()).get("args", [])

    async def cwd(self) -> Optional[str]:
        """Get the working directory."""
        return (await self.object()).get("cwd")

    async def env(self) -> Dict[str, Any]:
        """Get the environment variables."""
        return (await self.object()).get("env", {})

    async def executable(self) -> Dict[str, Any]:
        """Get the executable."""
        return (await self.object()).get("executable", {})

    async def host(self) -> str:
        """Get the host."""
        return (await self.object()).get("host", "")

    async def stdin(self) -> Optional[Any]:
        """Get the stdin blob."""
        return (await self.object()).get("stdin")

    async def user(self) -> Optional[str]:
        """Get the user."""
        return (await self.object()).get("user")

    async def mounts(self) -> List[Dict[str, Any]]:
        """Get the mounts."""
        return (await self.object()).get("mounts", [])

    def build(self, *args: Any) -> "BuildBuilder":
        """Create a build builder from this command."""
        from tangram.build import BuildBuilder

        return BuildBuilder(self, {"args": list(args)} if args else None)

    def run(self, *args: Any) -> "RunBuilder":
        """Create a run builder from this command."""
        from tangram.run import RunBuilder

        return RunBuilder(self, {"args": list(args)} if args else None)

    # Nested Object namespace.
    class Object:
        """Namespace for Command.Object operations."""

        @staticmethod
        def to_data(obj: Dict[str, Any]) -> Dict[str, Any]:
            """Convert object to data."""
            import tangram as tg

            data: Dict[str, Any] = {}
            args = obj.get("args", [])
            data["args"] = [tg.Value.to_data(a) for a in args]
            env = obj.get("env", {})
            data["env"] = {k: tg.Value.to_data(v) for k, v in env.items()}
            executable = obj.get("executable")
            if executable:
                data["executable"] = Command.Executable.to_data(executable)
            data["host"] = obj.get("host", "")
            cwd = obj.get("cwd")
            if cwd is not None:
                data["cwd"] = cwd
            mounts = obj.get("mounts", [])
            if mounts:
                data["mounts"] = [Command.Mount.to_data(m) for m in mounts]
            stdin = obj.get("stdin")
            if stdin is not None:
                data["stdin"] = stdin.id if hasattr(stdin, "id") else stdin
            user = obj.get("user")
            if user is not None:
                data["user"] = user
            return data

        @staticmethod
        def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
            """Convert data to object."""
            import tangram as tg

            obj: Dict[str, Any] = {}
            args_data = data.get("args", [])
            obj["args"] = [tg.Value.from_data(a) for a in args_data]
            obj["cwd"] = data.get("cwd")
            env_data = data.get("env", {})
            obj["env"] = {k: tg.Value.from_data(v) for k, v in env_data.items()}
            executable_data = data.get("executable")
            if executable_data:
                obj["executable"] = Command.Executable.from_data(executable_data)
            obj["host"] = data.get("host", "")
            mounts_data = data.get("mounts", [])
            obj["mounts"] = [Command.Mount.from_data(m) for m in mounts_data]
            stdin = data.get("stdin")
            if stdin is not None:
                obj["stdin"] = tg.Blob.with_id(stdin)
            else:
                obj["stdin"] = None
            obj["user"] = data.get("user")
            return obj

        @staticmethod
        def children(obj: Optional[Dict[str, Any]]) -> List[Object_]:
            """Get child objects from command object."""
            import tangram as tg

            if obj is None:
                return []
            result: List[Object_] = []
            # Args.
            for arg in obj.get("args", []):
                result.extend(tg.Value.objects(arg))
            # Env.
            for value in obj.get("env", {}).values():
                result.extend(tg.Value.objects(value))
            # Executable.
            executable = obj.get("executable")
            if executable:
                result.extend(Command.Executable.children(executable))
            # Mounts.
            for mount in obj.get("mounts", []):
                source = mount.get("source")
                if isinstance(source, Object_):
                    result.append(source)
            # Stdin.
            stdin = obj.get("stdin")
            if stdin is not None and isinstance(stdin, Object_):
                result.append(stdin)
            return result

    # Nested Executable namespace.
    class Executable:
        """Namespace for Command.Executable operations."""

        @staticmethod
        def to_data(executable: Dict[str, Any]) -> Dict[str, Any]:
            """Convert executable to data."""
            if "artifact" in executable:
                data: Dict[str, Any] = {}
                artifact = executable["artifact"]
                data["artifact"] = artifact.id if hasattr(artifact, "id") else artifact
                if executable.get("path") is not None:
                    data["path"] = executable["path"]
                return data
            elif "module" in executable:
                data = {"module": executable["module"]}
                if executable.get("export") is not None:
                    data["export"] = executable["export"]
                return data
            elif "path" in executable:
                return {"path": executable["path"]}
            else:
                raise RuntimeError("invalid executable")

        @staticmethod
        def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
            """Convert data to executable."""
            if "artifact" in data:
                from tangram.directory import _artifact_with_id

                return {
                    "artifact": _artifact_with_id(data["artifact"]),
                    "path": data.get("path"),
                }
            elif "module" in data:
                return {
                    "module": data["module"],
                    "export": data.get("export"),
                }
            elif "path" in data:
                return {"path": data["path"]}
            else:
                raise RuntimeError("invalid executable")

        @staticmethod
        def children(executable: Dict[str, Any]) -> List[Object_]:
            """Get child objects from executable."""
            if "artifact" in executable:
                artifact = executable["artifact"]
                if isinstance(artifact, Object_):
                    return [artifact]
            return []

    # Nested Mount namespace.
    class Mount:
        """Namespace for Command.Mount operations."""

        @staticmethod
        def to_data(mount: Dict[str, Any]) -> Dict[str, Any]:
            """Convert mount to data."""
            source = mount["source"]
            return {
                "source": source.id if hasattr(source, "id") else source,
                "target": mount["target"],
            }

        @staticmethod
        def from_data(data: Dict[str, Any]) -> Dict[str, Any]:
            """Convert data to mount."""
            from tangram.directory import _artifact_with_id

            return {
                "source": _artifact_with_id(data["source"]),
                "target": data["target"],
            }


def _is_artifact(value: Any) -> bool:
    """Check if value is an Artifact."""
    from tangram.directory import Directory
    from tangram.file import File
    from tangram.symlink import Symlink

    return isinstance(value, (Directory, File, Symlink))


class CommandBuilder:
    """Builder for creating commands with a fluent interface."""

    def __init__(self, *args: Any) -> None:
        """Initialize the command builder."""
        self._args: List[Dict[str, Any]] = list(args)

    def arg(self, *args: Any) -> "CommandBuilder":
        """Add arguments to the command."""
        self._args.append({"args": list(args)})
        return self

    def args(self, *args: Any) -> "CommandBuilder":
        """Add multiple argument sets."""
        for arg_set in args:
            self._args.append({"args": arg_set})
        return self

    def cwd(self, cwd: Any) -> "CommandBuilder":
        """Set the working directory."""
        self._args.append({"cwd": cwd})
        return self

    def env(self, *envs: Any) -> "CommandBuilder":
        """Set environment variables."""
        for env in envs:
            self._args.append({"env": env})
        return self

    def executable(self, executable: Any) -> "CommandBuilder":
        """Set the executable."""
        self._args.append({"executable": executable})
        return self

    def host(self, host: Any) -> "CommandBuilder":
        """Set the host."""
        self._args.append({"host": host})
        return self

    def mount(self, *mounts: Any) -> "CommandBuilder":
        """Add mounts."""
        self._args.append({"mounts": list(mounts)})
        return self

    def mounts(self, *mounts: Any) -> "CommandBuilder":
        """Add multiple mount sets."""
        for mount_set in mounts:
            self._args.append({"mounts": mount_set})
        return self

    async def _build(self) -> Command:
        """Build the command."""
        return await Command.new(*self._args)

    def __await__(self) -> Generator[Any, None, Command]:
        """Allow awaiting the builder to get the command."""
        return self._build().__await__()

    def build(self, *args: Any) -> "BuildBuilder":
        """Create a build builder from this command."""
        from tangram.build import BuildBuilder

        return BuildBuilder(*self._args, {"args": list(args)} if args else None)

    def run(self, *args: Any) -> "RunBuilder":
        """Create a run builder from this command."""
        from tangram.run import RunBuilder

        return RunBuilder(*self._args, {"args": list(args)} if args else None)


# Type aliases.
Command.Id = str
Command.Arg = Union[None, str, Object_, "tg.Template", "Command", Dict[str, Any]]
Command.Data = Dict[str, Any]

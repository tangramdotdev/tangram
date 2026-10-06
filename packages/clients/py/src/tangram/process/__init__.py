"""Process handles, state accessors, and process lifetime management."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator, Callable, Generator, Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    Never,
    NotRequired,
    Self,
    TypedDict,
    Unpack,
    cast,
    overload,
)

from ..args import Args
from ..async_property import async_property
from ..client import client as default_client
from ..client.process.cancel import Cancel as _Cancel
from ..client.process.connect import Connect as _Connect
from ..client.process.connect import TtyArg
from ..client.process.get import Get as _Get
from ..client.process.put import Put as _Put
from ..client.process.signal import Signal as _Signal
from ..client.process.spawn import Spawn as _Spawn
from ..command import (
    Command,
    CommandArgObject,
    CommandArgument,
    CommandValue,
    ExecutableObject,
    FieldInput,
    command_value,
    reduce_args,
)
from ..directory import Directory
from ..error import Error
from ..file import File
from ..location import Arg as LocationArg
from ..location import ArgObject as LocationArgObject
from ..mutation import UNSET, Mutation
from ..object import Object
from ..referent import Referent, ReferentData, ReferentOptions
from ..resolve import Unresolved, capture, resolve
from ..sandbox import Mount, MountValue, NetworkValue, SandboxArg
from ..symlink import Symlink
from ..template import Template
from ..value import Value
from .command import ProcessCommandData
from .connect import Connection
from .connect.session import CHUNK_SIZE
from .outcome import Outcome, ProcessOutcome
from .stdio import ReadArgObject, Stdio, StdioChunk

if TYPE_CHECKING:
    from ..client import Client
    from ..error import ErrorDataObject
    from ..http import Stream as HttpStream
    from ..location import LocationObject
    from ..value import ValueData, ValueType


args = []
cwd = os.getcwd()
env = dict(os.environ)
export = None
module = None


def set_process(context):
    """Install the current process context while preserving the module identity."""
    import sys

    process = sys.modules[__name__]
    values = context if isinstance(context, dict) else vars(context)
    for name, value in values.items():
        setattr(process, name, value)


type Mode = Literal["exec", "run", "spawn"]


class DebugObject(TypedDict, total=False):
    addr: str | None
    mode: Literal["normal", "break", "wait"] | None


class TtyObject(TypedDict):
    size: Tty.Size


class ProcessChildData(TypedDict):
    cached: NotRequired[bool]
    process: str


class ProcessDataObject(TypedDict):
    actual_checksum: NotRequired[str | None]
    cacheable: NotRequired[bool]
    children: NotRequired[list[ProcessChildData] | None]
    command: ReferentData[ProcessCommandData | str] | str
    created_at: int | float
    debug: NotRequired[DebugObject | None]
    error: NotRequired[ErrorDataObject | str | None]
    exit: NotRequired[int | None]
    expected_checksum: NotRequired[str | None]
    finished_at: NotRequired[int | float | None]
    host: str
    log: NotRequired[str | None]
    output: NotRequired[ValueData]
    retry: NotRequired[bool]
    sandbox: NotRequired[str | None]
    started_at: NotRequired[int | float | None]
    status: Literal["started", "finished"]
    stderr: NotRequired[Literal["inherit", "log", "null", "pipe", "tty"]]
    stdin: NotRequired[Literal["inherit", "log", "null", "pipe", "tty"]]
    stdout: NotRequired[Literal["inherit", "log", "null", "pipe", "tty"]]
    tty: NotRequired[TtyObject | None]


class ArgObject(CommandArgObject, total=False):
    cached: FieldInput[bool]
    cache_location: Unresolved[LocationArgObject | Mutation | None]
    checksum: FieldInput[str]
    command: Unresolved[
        Command | CommandArgObject | Referent[Command | CommandArgObject] | None
    ]
    cpu: FieldInput[int | float]
    debug: Unresolved[bool | DebugObject | Mutation | None]
    location: Unresolved[LocationArgObject | Mutation | None]
    memory: FieldInput[int | float]
    mounts: Unresolved[list[Unresolved[MountValue]] | Mutation | None]
    name: FieldInput[str]
    network: Unresolved[bool | NetworkValue | Mutation | None]
    owner: FieldInput[str]
    ports: Unresolved[list[Unresolved[str]] | Mutation | None]
    sandbox: Unresolved[bool | SandboxArg | str | Mutation | None]
    stderr: Unresolved[Literal["inherit", "log", "null", "pipe", "tty"] | None]
    stdout: Unresolved[Literal["inherit", "log", "null", "pipe", "tty"] | None]
    tty: Unresolved[bool | TtyObject | Mutation | None]


type ProcessInput = Unresolved[
    str | Directory | File | Symlink | Template | Command | ArgObject | None
]


class Process[O: ValueType]:
    Cancel: ClassVar[type[_Cancel]]
    Get: ClassVar[type[_Get]]
    Put: ClassVar[type[_Put]]
    Stdio: ClassVar[type[Stdio]]
    Tty: ClassVar[type[Tty]]
    Mount: ClassVar[type[Mount]]
    Status = Literal["started", "finished"]
    Connect: ClassVar[type[_Connect]]
    Spawn: ClassVar[type[_Spawn]]
    Builder: ClassVar[type[Builder]]
    Child: ClassVar[type[Child]]
    Data: ClassVar[type[Data]]
    State: ClassVar[type[State]]
    Signal: ClassVar[type[Signal]]
    Id: ClassVar[type[str]]
    Outcome: ClassVar[type[Outcome]]
    __tangram_atomic__ = True

    def __init__(
        self,
        id: int | str,
        *,
        client: Client | None = None,
        connection=None,
        lease=None,
        location=None,
        tokens=None,
        options: ReferentOptions | None = None,
        state=None,
        outcome=None,
        promise=None,
        stdin=None,
        stdout=None,
        stderr=None,
        stopper=None,
        stdio_promise=None,
    ):
        if isinstance(id, dict):
            self.__init__(**id)
            return
        from .stdio import Reader, Writer

        self.id = id
        self.client = client or default_client
        self.connection = connection
        self.lease = lease
        self.location = location
        self.tokens = tokens or {}
        self.options: ReferentOptions = {**(options or {})}
        self.spawn_output: Mapping[str, object] = {}
        self.state = state
        self._wait_result = outcome
        self._wait_promise = promise
        self._stdio_promise = stdio_promise
        self._stopper = stopper
        self._owned = outcome is None and (
            stopper is not None if isinstance(id, int) else lease is not None
        )
        self._write_position = 0
        self._write_lock = asyncio.Lock()
        self.stdin = stdin if stdin is not None else Writer(self, "stdin")
        self.stdout = stdout if stdout is not None else Reader(self, "stdout")
        self.stderr = stderr if stderr is not None else Reader(self, "stderr")
        if self.state is not None:
            State.inherit_location(
                self.state,
                None
                if self.location is None
                else LocationArg.to_location(self.location),
            )
            State.inherit_tokens(self.state, self.tokens)

    @classmethod
    async def connect(
        cls, process, *, client: Client | None = None, mode="run", reads=None, **options
    ) -> Self:
        process, reads, options = await resolve([process, reads, options])
        connection = await Connection.open(
            process, client=client, mode=mode, reads=reads, **options
        )
        output = connection.initial.output
        location = output.get("location")
        outcome = output.get("outcome")
        result = cls(
            output["process"],
            client=client,
            connection=connection,
            lease=output.get("lease"),
            location=None if location is None else LocationArg.from_location(location),
            tokens=output.get("tokens"),
            outcome=Outcome.from_data(outcome) if outcome is not None else None,
        )
        result.spawn_output = output
        return result

    @overload
    @classmethod
    def spawn[R: ValueType](
        cls,
        function_: Callable[..., Unresolved[R]],
        *args: CommandArgument,
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ) -> _Builder[Literal["spawn"], R]: ...

    @overload
    @classmethod
    def spawn(
        cls,
        *args: ProcessInput,
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ) -> Builder[Literal["spawn"], ValueType]: ...

    @classmethod
    def spawn(
        cls,
        *args,
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ) -> Builder[Literal["spawn"], ValueType]:
        from .spawn import builder as spawn

        return spawn(*args, client=client, **options)

    @overload
    @classmethod
    def run[R: ValueType](
        cls,
        function_: Callable[..., Unresolved[R]],
        *args: CommandArgument,
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ) -> _Builder[Literal["run"], R]: ...

    @overload
    @classmethod
    def run(
        cls,
        *args: ProcessInput,
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ) -> Builder[Literal["run"], ValueType]: ...

    @classmethod
    def run(
        cls,
        *args,
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ) -> Builder[Literal["run"], ValueType]:
        from .run import builder as run

        return run(*args, client=client, **options)

    @overload
    @classmethod
    def build[R: ValueType](
        cls,
        function_: Callable[..., Unresolved[R]],
        *args: CommandArgument,
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ) -> _Builder[Literal["run"], R]: ...

    @overload
    @classmethod
    def build(
        cls,
        *args: ProcessInput,
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ) -> Builder[Literal["run"], ValueType]: ...

    @classmethod
    def build(
        cls,
        *args,
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ) -> Builder[Literal["run"], ValueType]:
        from .build import builder as build

        return build(*args, client=client, **options)

    @classmethod
    def exec(
        cls,
        *args: ProcessInput | Callable[..., Unresolved[ValueType]],
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ) -> Builder[Literal["exec"], Never]:
        from .exec import builder as exec

        return exec(*args, client=client, **options)

    @classmethod
    def expect(cls, value):
        if not isinstance(value, cls):
            raise AssertionError("expected a process")
        return value

    @classmethod
    def assert_(cls, value):
        cls.expect(value)

    @staticmethod
    async def arg(*args, client: Client | None = None):
        return await Process.arg_resolved(*(await resolve(args)), client=client)

    @staticmethod
    async def arg_resolved(*args, client: Client | None = None):
        return await process_arg_resolved(*args, client=client)

    @property
    def tokens(self):
        from ..authorization import Tokens

        return Tokens.clone(self._tokens)

    @tokens.setter
    def tokens(self, tokens):
        from ..authorization import Tokens

        self._tokens = Tokens.clone(tokens)
        Tokens.normalize(self._tokens)

    def inherit_location(self, location):
        if self.location is None:
            self.location = location

    def inherit_tokens(self, tokens):
        from ..authorization import Tokens

        Tokens.inherit(self._tokens, tokens)

    async def load(self):
        if isinstance(self.id, int):
            raise ValueError("loading unsandboxed process state is not supported")
        from .. import authorization

        output = await self.client.get_process(
            self.id, location=self.location, tokens=self.tokens
        )
        self.tokens = authorization.inherit(output.get("tokens") or {}, self.tokens)
        location = output.get("location")
        self.location = (
            None if location is None else LocationArg.from_location(location)
        )
        self.state = State.from_data(output["data"])
        State.inherit_location(self.state, output.get("location"))
        State.inherit_tokens(self.state, self.tokens)
        return self.state

    async def reload(self):
        return await self.load()

    @async_property
    async def command(self) -> Command[list[ValueType], O] | ProcessCommandData:
        from ..command import Command

        state = await self.load()
        referent = state["command"]
        if isinstance(referent.node, str):
            command = Command.with_referent(referent)
            command._inherit_tokens(self.tokens)
            if command.location is None:
                command.location = (
                    None
                    if self.location is None
                    else LocationArg.to_location(self.location)
                )
            return command
        from .command import inherit_options

        return inherit_options(referent.node, referent.options)

    async def _command_field(self, field, default=None):
        from ..command import Command, CommandValue

        command = await self.command()
        if isinstance(command, Command):
            return (await command.load(self.client)).get(field, default)
        value = command.get(field, default)
        if field == "args":
            return [
                CommandValue(item["kind"], Value.from_data(item["value"]))
                for item in value
            ]
        if field == "env":
            return {
                key: CommandValue(item["kind"], Value.from_data(item["value"]))
                for key, item in value.items()
            }
        if field == "executable":
            referent = Referent.from_data(value)
            executable = dict(referent.node)
            if executable.get("artifact") is not None:
                artifact = Object.with_id(executable["artifact"])
                artifact.location = (referent.options or {}).get(
                    "location",
                    None
                    if self.location is None
                    else LocationArg.to_location(self.location),
                )
                artifact._inherit_tokens((referent.options or {}).get("tokens") or {})
                artifact._inherit_tokens(self.tokens)
                executable["artifact"] = artifact
            return executable
        return value

    @async_property
    async def args(self) -> list[CommandValue]:
        return await self._command_field("args", [])

    @async_property
    async def cwd(self) -> str | None:
        return await self._command_field("cwd")

    @overload
    async def env(self, name: None = None) -> dict[str, CommandValue]: ...

    @overload
    async def env(self, name: str) -> CommandValue | None: ...

    async def env(self, name=None) -> dict[str, CommandValue] | CommandValue | None:
        env = await self._command_field("env", {})
        return env if name is None else env.get(name)

    @async_property
    async def executable(self) -> ExecutableObject:
        return await self._command_field("executable")

    @async_property
    async def user(self) -> str | None:
        return await self._command_field("user")

    @async_property
    async def sandbox(self) -> str | None:
        if isinstance(self.id, int):
            return None
        state = await self.load()
        return state.get("sandbox")

    async def _sandbox_data(self):
        sandbox = await self.sandbox()
        return (
            {} if sandbox is None else (await self.client.get_sandbox(sandbox))["data"]
        )

    @async_property
    async def mounts(self) -> list[MountValue]:
        from ..sandbox import Mount

        return [
            Mount.from_data_string(value)
            for value in (await self._sandbox_data()).get("mounts") or []
        ]

    @async_property
    async def network(self) -> bool:
        return (await self._sandbox_data()).get("network") is not None

    @async_property
    async def ports(self) -> list[str]:
        network = (await self._sandbox_data()).get("network") or {}
        return (
            list(network.get("ports") or []) if network.get("kind") == "bridge" else []
        )

    async def try_read_stdio(
        self, options=None, **kwargs
    ) -> AsyncGenerator[StdioChunk, None] | HttpStream[StdioChunk] | None:
        if not isinstance(self.id, str):
            raise ValueError("stdio reads require a sandboxed process")
        options, kwargs = await resolve([options, kwargs])
        arg: ReadArgObject = {"streams": ["stdout"], **(options or {}), **kwargs}
        if arg.get("location") is None:
            arg["location"] = self.location
        arg["tokens"] = self.tokens
        if self.connection is not None and not self.connection.closed:
            return self.connection.read(arg)
        return await self.client.try_read_process_stdio(self.id, arg)

    async def read_stdio(
        self, options=None, **kwargs
    ) -> AsyncGenerator[StdioChunk, None] | HttpStream[StdioChunk]:
        output = await self.try_read_stdio(options, **kwargs)
        if output is None:
            raise ValueError("failed to find process stdio")
        return output

    async def read(self, stream="stdout", **options) -> AsyncGenerator[bytes, None]:
        chunks = await self.read_stdio(streams=[stream], **options)
        async for chunk in chunks:
            yield chunk["bytes"]

    async def write(self, bytes_: bytes | bytearray | memoryview) -> int:
        bytes_ = await resolve(bytes_)
        async with self._write_lock:
            written = 0
            while written < len(bytes_):
                chunk = bytes_[written : written + CHUNK_SIZE]
                data = {
                    "kind": "chunk",
                    "value": {
                        "bytes": chunk,
                        "stream": "stdin",
                        "combined_position": self._write_position,
                        "stream_position": self._write_position,
                    },
                }
                if self.connection is None:
                    raise ConnectionError("the process has no writable connection")
                output = await self.connection.write(data)
                self._write_position += output["length"]
                written += output["length"]
                if output["closed"]:
                    return written
            return written

    async def end(self) -> None:
        async with self._write_lock:
            if self.connection is None:
                raise ConnectionError("the process has no writable connection")
            await self.connection.write(
                {
                    "kind": "end",
                    "value": {
                        "combined_position": self._write_position,
                        "stream_positions": {"stdin": self._write_position},
                    },
                }
            )

    async def wait(self) -> ProcessOutcome[O]:
        if self._wait_promise is None:
            self._wait_promise = asyncio.create_task(self._wait_inner())
        self._wait_result = await asyncio.shield(self._wait_promise)
        from .outcome import Outcome

        Outcome.inherit_location(
            self._wait_result,
            None if self.location is None else LocationArg.to_location(self.location),
        )
        Outcome.inherit_tokens(self._wait_result, self.tokens)
        self._owned = False
        return self._wait_result

    async def _wait_inner(self) -> ProcessOutcome[O]:
        if self._wait_result is None:
            if self.connection is not None and not self.connection.closed:
                self._wait_result = await self.connection.wait()
            else:
                if isinstance(self.id, int):
                    raise ValueError("missing an unsandboxed process outcome")
                self._wait_result = await self.client.wait_process(
                    self.id,
                    lease=self.lease,
                    location=self.location,
                    tokens=self.tokens,
                )
        if self._wait_result is None:
            raise ValueError("failed to find the process")
        if self._stdio_promise is not None:
            await self._stdio_promise
        self._owned = False
        return cast(ProcessOutcome[O], self._wait_result)

    async def output(self) -> O:
        outcome = await self.wait()
        values = {"id": str(self.id)}
        if self.options.get("name") is not None:
            values["name"] = self.options["name"]
        error = outcome.get("error")
        if error is not None:
            options: ReferentOptions = {**self.options, "tokens": error.state.tokens}
            raise await Error.new(
                "the child process failed",
                {"source": Referent(error, options), "values": values},
            )
        exit = outcome["exit"]
        if exit >= 1:
            error = await Error.new(f"the process exited with code {exit}")
            message = (
                f"the child process exited with signal {exit - 128}"
                if exit >= 128
                else "the child process failed"
            )
            raise await Error.new(
                message,
                {"source": Referent(error, self.options), "values": values},
            )
        output = outcome.get("output")
        if "output" in outcome:
            Value.inherit_tokens(output, self.tokens)
        return cast(O, output)

    async def signal(self, signal: str) -> None:
        if isinstance(self.id, int):
            from .. import host

            await host.signal(self.id, signal)
            return
        if self.location is None and self.connection is None:
            await self.load()
        arg: _Signal.Arg = {
            "signal": signal,
            "location": self.location,
            "tokens": self.tokens,
        }
        if self.connection is not None and not self.connection.closed:
            await self.connection.request("signal", arg)
        else:
            await self.client.signal_process(self.id, arg)

    async def cancel(self) -> None:
        if isinstance(self.id, int):
            from .. import host

            if self._stopper is None:
                await host.signal(self.id, Signal.TERM)
            else:
                await host.stopper_stop(self._stopper)
                if self._wait_promise is not None:
                    await self._wait_promise
            return
        if self.lease is None:
            raise ValueError("missing lease")
        arg: _Cancel.Arg = {"lease": self.lease, "location": self.location}
        if self.connection is not None and not self.connection.closed:
            await self.connection.request("cancel", arg)
        else:
            await self.client.cancel_process(self.id, arg)
        self._owned = False

    @overload
    async def set_tty_size(self, size: Tty.Size) -> None: ...

    @overload
    async def set_tty_size(self, *, cols: int, rows: int) -> None: ...

    async def set_tty_size(self, size: Tty.Size | None = None, **kwargs: int) -> None:
        if isinstance(self.id, int):
            raise ValueError("tty resizing is not supported for unsandboxed processes")
        if self.location is None and self.connection is None:
            await self.load()
        arg: TtyArg = {
            "size": size if size is not None else cast("Tty.Size", kwargs),
            "location": self.location,
            "tokens": self.tokens,
        }
        if self.connection is not None and not self.connection.closed:
            await self.connection.request("tty", arg)
        else:
            await self.client.set_process_tty_size(self.id, arg)

    async def detach(self) -> None:
        if self.connection is not None:
            await self.connection.detach()
        await self.close()
        self._owned = False

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()
            self.connection = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        try:
            if self._owned:
                await self.cancel()
        finally:
            await self.close()

    @staticmethod
    async def spawn_arg(*args, **options):
        from .spawn import spawn_arg

        return await spawn_arg(*args, **options)

    @staticmethod
    async def spawn_arg_from_resolved(*args, **options):
        from .spawn import spawn_arg_from_resolved

        return await spawn_arg_from_resolved(*args, **options)

    @staticmethod
    async def exec_unsandboxed(*args, **options):
        from .exec import exec_unsandboxed

        return await exec_unsandboxed(*args, **options)

    @staticmethod
    async def spawn_unsandboxed(*args, **options):
        from .spawn import spawn_unsandboxed

        return await spawn_unsandboxed(*args, **options)

    @staticmethod
    async def wait_unsandboxed(*args, **options):
        from .spawn import wait_unsandboxed

        return await wait_unsandboxed(*args, **options)

    @staticmethod
    async def prepare_unsandboxed_command(*args, **options):
        from .spawn import prepare_unsandboxed_command

        return await prepare_unsandboxed_command(*args, **options)

    @staticmethod
    async def spawn_sandboxed(*args, **options):
        from .spawn import spawn_sandboxed

        return await spawn_sandboxed(*args, **options)


class Builder[M: Mode, O: ValueType]:
    """An awaitable, callable command or process builder."""

    def __init__(
        self,
        operation: M,
        *args: ProcessInput,
        client: Client | None = None,
        **options: Unpack[ArgObject],
    ):
        self.operation = operation
        self.client = client or default_client
        self._memo = {}
        self._originals = capture([*args, options], self._memo)
        self._module = None
        self.arguments = [
            capture(self.builder_arg(arg), self._memo) for arg in self._originals
        ]
        self._env_mapper = lambda env: env
        self._validate = None
        self._connection = "spawn"

    @overload
    def __await__(self: Builder[Literal["run"], O]) -> Generator[Any, None, O]: ...

    @overload
    def __await__(
        self: Builder[Literal["spawn"], O],
    ) -> Generator[Any, None, Process[O]]: ...

    @overload
    def __await__(self: Builder[Literal["exec"], O]) -> Generator[Any, None, Never]: ...

    def __await__(self) -> Generator[Any, None, Any]:
        return self._create().__await__()

    def __call__(self, *args) -> Self:
        return self.arg(*args)

    def with_options(self, **options) -> Self:
        self.arguments.append(
            capture(self.builder_arg(capture(options, self._memo)), self._memo)
        )
        return self

    def arg(self, *args) -> Self:
        return self.args(list(args))

    def args(self, *args) -> Self:
        for value in args:
            value = capture(value, self._memo)
            self.arguments.append(capture(self.args_arg(value), self._memo))
        return self

    def env(self, *envs, **env) -> Self:
        mapper = self._env_mapper
        for value in (*envs, *([env] if env else [])):

            async def mapped(value=capture(value, self._memo), mapper=mapper):
                value = await resolve(value)
                return {"env": None if value is None else await resolve(mapper(value))}

            self.arguments.append(capture(mapped(), self._memo))
        return self

    def env_mapper(self, mapper) -> Self:
        self._env_mapper = mapper
        return self

    def executable(self, value) -> Self:
        return self.with_options(executable=value)

    def host(self, value: Unresolved[str | Mutation | None]) -> Self:
        return self.with_options(host=value)

    def cwd(self, value: Unresolved[str | Mutation | None]) -> Self:
        return self.with_options(cwd=value)

    def stdin(self, value) -> Self:
        return self.with_options(stdin=value)

    def stdout(
        self,
        value: Unresolved[
            Literal["inherit", "log", "null", "pipe", "tty"] | Mutation | None
        ],
    ) -> Self:
        return self.with_options(stdout=value)

    def stderr(
        self,
        value: Unresolved[
            Literal["inherit", "log", "null", "pipe", "tty"] | Mutation | None
        ],
    ) -> Self:
        return self.with_options(stderr=value)

    def stdio(
        self,
        value: Unresolved[
            Literal["inherit", "log", "null", "pipe", "tty"] | Mutation | None
        ],
    ) -> Self:
        return self.with_options(stdin=value, stdout=value, stderr=value)

    def sandbox(
        self, value: Unresolved[bool | SandboxArg | str | Mutation | None] = True
    ) -> Self:
        return self.with_options(sandbox=value)

    def network(
        self, value: Unresolved[bool | NetworkValue | Mutation | None] = True
    ) -> Self:
        return self.with_options(network=value)

    def mount(self, *values: Unresolved[MountValue]) -> Self:
        return self.with_options(mounts=list(values))

    def mounts(self, *values: Unresolved[list[MountValue] | Mutation | None]) -> Self:
        for value in values:
            self.with_options(mounts=value)
        return self

    def port(self, *values: Unresolved[str]) -> Self:
        return self.with_options(ports=list(values))

    def ports(self, *values: Unresolved[list[str] | Mutation | None]) -> Self:
        for value in values:
            self.with_options(ports=value)
        return self

    def cpu(self, value: Unresolved[int | float | Mutation | None]) -> Self:
        return self.with_options(cpu=value)

    def memory(self, value: Unresolved[int | float | Mutation | None]) -> Self:
        return self.with_options(memory=value)

    def location(self, value: Unresolved[LocationArgObject | Mutation | None]) -> Self:
        return self.with_options(location=value)

    def user(self, value: Unresolved[str | Mutation | None]) -> Self:
        return self.with_options(user=value)

    def checksum(self, value: Unresolved[str | Mutation | None]) -> Self:
        return self.with_options(checksum=value)

    def cached(self, value: Unresolved[bool | Mutation | None] = True) -> Self:
        return self.with_options(cached=value)

    def retry(self, value: Unresolved[bool | Mutation | None] = True) -> Self:
        return self.with_options(retry=value)

    def tty(self, value: Unresolved[bool | TtyObject | Mutation | None] = True) -> Self:
        return self.with_options(tty=value)

    def debug(
        self, value: Unresolved[bool | DebugObject | Mutation | None] = True
    ) -> Self:
        return self.with_options(debug=value)

    def named(self, value: Unresolved[str | Mutation | None]) -> Self:
        return self.with_options(name=value)

    def validate(self, validate) -> Self:
        self._validate = validate
        return self

    def connection(self, mode: Literal["run", "spawn"]) -> Self:
        self._connection = mode
        return self

    def _mode(self, operation, args):
        builder = Builder(operation, *self.arguments, client=self.client)
        builder._env_mapper = self._env_mapper
        builder._validate = self._validate
        builder._connection = self._connection
        if args:
            builder.arg(*args)
        return builder

    def run(self, *args) -> Builder[Literal["run"], O]:
        return self._mode("run", args)

    def spawn(self, *args) -> Builder[Literal["spawn"], O]:
        return self._mode("spawn", args)

    def exec(self, *args) -> Builder[Literal["exec"], Never]:
        return self._mode("exec", args)

    async def _is_module(self):
        from ..command import CommandObject

        if self._module is None:

            async def detect():
                for arg in await resolve(self._originals):
                    command = (
                        arg
                        if isinstance(arg, Command)
                        else arg.get("command")
                        if isinstance(arg, dict)
                        else None
                    )
                    if isinstance(command, Referent):
                        command = command.node
                    if isinstance(command, dict) and "node" in command:
                        command = command["node"]
                    if isinstance(command, dict):
                        if Command.Arg.is_js(command) or Command.Arg.is_py(command):
                            return True
                    if isinstance(command, Command):
                        object = await command.load(self.client)
                        if CommandObject.is_js(object) or CommandObject.is_py(object):
                            return True
                return False

            self._module = capture(detect(), self._memo)
        return await resolve(self._module)

    async def builder_arg(self, arg):
        from ..command import encode_module_args

        arg = await resolve(arg)
        if (
            isinstance(arg, dict)
            and isinstance(arg.get("args"), list)
            and await self._is_module()
        ):
            return {**arg, "args": encode_module_args(arg["args"])}
        return arg

    async def args_arg(self, args):
        from ..command import encode_module_args

        args = await resolve(args)
        if args is not None and await self._is_module():
            args = encode_module_args(args)
        return {"args": args}

    async def _create(self):
        from .build import build_resolved
        from .exec import exec_resolved
        from .run import run_resolved
        from .spawn import spawn_resolved

        resolved = await resolve(self.arguments)
        if self.operation == "command":
            return await Command.new(*resolved, client=self.client)
        operation = {
            "build": build_resolved,
            "exec": exec_resolved,
            "run": run_resolved,
            "spawn": spawn_resolved,
        }[self.operation]
        return await operation(self, resolved)


async def process_arg_resolved(*args, client: Client | None = None):
    from ..command import reduce_env

    async def map_(arg):
        if arg is None or arg is UNSET:
            output = {}
        elif isinstance(arg, (str, Directory, File, Symlink, Template)):
            output = {
                "args": ["-c", arg],
                "executable": env.get("SHELL")
                if isinstance(env.get("SHELL"), str)
                else "sh",
            }
        elif isinstance(arg, Command):
            object_ = await arg.load(client)
            output = {
                key: object_[key] for key in ("args", "env", "executable", "host")
            }
            output.update(
                {
                    key: object_[key]
                    for key in ("cwd", "stdin", "user")
                    if object_.get(key) is not None
                }
            )
        else:
            output = dict(arg)
        if output.get("args") is not None and output.get("args", UNSET) is not UNSET:
            if not isinstance(output["args"], list):
                raise TypeError("command args must be an array")
            output["args"] = [command_value(value) for value in output["args"]]
        return output

    return await Args.apply_resolved(
        args=args,
        map=map_,
        reduce={
            "args": reduce_args,
            "env": reduce_env,
            "mounts": "append",
            "ports": "append",
        },
    )


# Avoid the Process.Builder class attribute when resolving generic annotations.
_Builder = Builder
Process.Builder = Builder


class Child:
    @staticmethod
    def to_data(value):
        process = value["process"]
        if not isinstance(process.id, str):
            raise ValueError("expected a sandboxed process id")
        options: ReferentOptions = {
            **value.get("options", {}),
            "tokens": process.tokens,
        }
        if process.location is not None:
            options["location"] = LocationArg.to_location(process.location)
        return {
            "cached": value["cached"],
            "process": Referent(process.id, options).to_data_string(),
        }

    @staticmethod
    def from_data(data):
        referent = Referent.from_data_string(data["process"])
        options = cast(
            ReferentOptions,
            {
                key: value
                for key, value in (referent.options or {}).items()
                if key != "tokens"
            },
        )
        return {
            "cached": data.get("cached", False),
            "options": options,
            "process": Process(
                referent.node,
                location=None
                if (referent.options or {}).get("location") is None
                else LocationArg.from_location(
                    cast("LocationObject", (referent.options or {})["location"])
                ),
                tokens=(referent.options or {}).get("tokens"),
            ),
        }


class State:
    @staticmethod
    def inherit_location(state, location):
        referent = state["command"]
        if (referent.options or {}).get("location") is None:
            referent.options["location"] = location
        for child in state.get("children") or []:
            child["process"].inherit_location(
                None if location is None else LocationArg.from_location(location)
            )
        for key in ("error", "log"):
            if state.get(key) is not None:
                Object.inherit_location(state[key], location)
        if "output" in state:
            Value.inherit_location(state["output"], location)

    @staticmethod
    def inherit_tokens(state, tokens):
        from ..authorization import Tokens

        referent = state["command"]
        if (referent.options or {}).get("tokens") is None:
            referent.options["tokens"] = {}
        Tokens.inherit(
            referent.options["tokens"],
            tokens,
            referent.node if isinstance(referent.node, str) else None,
        )
        for child in state.get("children") or []:
            child["process"].inherit_tokens(tokens)
        for key in ("error", "log"):
            if state.get(key) is not None:
                Object.inherit_tokens(state[key], tokens)
        if "output" in state:
            Value.inherit_tokens(state["output"], tokens)

    @staticmethod
    def from_data(data):
        from ..blob import Blob

        state = {
            key: data.get(key)
            for key in (
                "actual_checksum",
                "debug",
                "exit",
                "expected_checksum",
                "finished_at",
                "sandbox",
                "started_at",
                "tty",
            )
        }
        state.update(
            {key: data[key] for key in ("created_at", "host", "status") if key in data}
        )
        state.update({key: (data.get(key) or False) for key in ("cacheable", "retry")})
        state.update(
            {key: (data.get(key) or "inherit") for key in ("stdin", "stdout", "stderr")}
        )
        state["command"] = (
            Referent.from_data_string(data["command"])
            if isinstance(data["command"], str)
            else Referent.from_data(data["command"])
        )
        state["children"] = (
            None
            if data.get("children") is None
            else [Child.from_data(child) for child in data["children"]]
        )
        state["error"] = (
            Error.from_data(data["error"]) if data.get("error") is not None else None
        )
        state["log"] = (
            Blob.with_referent(Referent.from_data_string(data["log"]))
            if data.get("log") is not None
            else None
        )
        if "output" in data:
            state["output"] = Value.from_data(data["output"])
        return state

    @staticmethod
    def to_data(state):
        command = state["command"]
        data = {
            "command": command.to_data_string()
            if isinstance(command.node, str)
            else command.to_data()
        }
        data.update(
            {
                key: state[key]
                for key in ("created_at", "host", "status")
                if key in state
            }
        )
        for key in (
            "actual_checksum",
            "debug",
            "exit",
            "expected_checksum",
            "finished_at",
            "sandbox",
            "started_at",
            "tty",
        ):
            if state.get(key) is not None:
                data[key] = state[key]
        for key in ("cacheable", "retry"):
            if state.get(key):
                data[key] = state[key]
        for key in ("stdin", "stdout", "stderr"):
            if state.get(key, "inherit") != "inherit":
                data[key] = state[key]
        if state.get("children") is not None:
            data["children"] = [Child.to_data(child) for child in state["children"]]
        if state.get("error") is not None:
            error = state["error"]
            data["error"] = (
                error.to_referent().to_data_string()
                if error.state.stored
                else error.to_data()
            )
        if state.get("log") is not None:
            data["log"] = state["log"].to_referent().to_data_string()
        if "output" in state:
            data["output"] = Value.to_data(state["output"])
        return data


class Data:
    @staticmethod
    def without_location_and_tokens(data):
        from ..object import ObjectData
        from .command import without_location_and_tokens

        output = dict(data)
        if data.get("children") is not None:
            output["children"] = [
                {
                    **child,
                    "process": Referent.from_data_string(child["process"])
                    .without_location_and_tokens()
                    .to_data_string(),
                }
                for child in data["children"]
            ]
        command = (
            Referent.from_data_string(data["command"])
            if isinstance(data["command"], str)
            else Referent.from_data(data["command"])
        ).without_location_and_tokens()
        if not isinstance(command.node, str):
            command.node = without_location_and_tokens(command.node)
        output["command"] = (
            command.to_data_string()
            if isinstance(command.node, str)
            else command.to_data()
        )
        for key in ("error", "log"):
            value = data.get(key)
            if value is not None:
                output[key] = (
                    Referent.from_data_string(value)
                    .without_location_and_tokens()
                    .to_data_string()
                    if isinstance(value, str)
                    else ObjectData.without_location_and_tokens(
                        {"kind": "error", "value": value}
                    )["value"]
                )
        if "output" in data:
            output["output"] = Value.Data.without_location_and_tokens(data["output"])
        return output


class Tty:
    class Size(TypedDict):
        cols: int
        rows: int

    class Put:
        class Arg(TypedDict):
            location: NotRequired[dict | None]
            size: dict
            tokens: NotRequired[dict | None]


class Signal:
    ABRT = "ABRT"
    ALRM = "ALRM"
    FPE = "FPE"
    HUP = "HUP"
    ILL = "ILL"
    INT = "INT"
    KILL = "KILL"
    PIPE = "PIPE"
    QUIT = "QUIT"
    SEGV = "SEGV"
    TERM = "TERM"
    USR1 = "USR1"
    USR2 = "USR2"


Process.Child = Child
Process.Data = Data
Process.State = State
Process.Signal = Signal
Process.Id = str

Process.Outcome = Outcome

Process.Connect = _Connect
Process.Spawn = _Spawn

Process.Cancel = _Cancel
Process.Get = _Get
Process.Put = _Put
Process.Stdio = Stdio
Process.Tty = Tty

Process.Mount = Mount

from __future__ import annotations

from collections.abc import Awaitable, Callable, Generator, Sequence
from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    Never,
    NotRequired,
    Protocol,
    Self,
    TypedDict,
    Unpack,
    cast,
    overload,
)

if TYPE_CHECKING:
    from .blob import Blob
    from .client import Client
    from .directory import Directory
    from .file import File
    from .process import Builder as ProcessBuilder
    from .referent import Referent
    from .symlink import Symlink
    from .value import ValueData, ValueInput, ValueType

import tangram as tg

from .args import Args
from .mutation import UNSET, Mutation
from .object import Object
from .resolve import Unresolved, capture, resolve
from .template import Template, TemplateInput
from .value import Value


class CommandValueWire(TypedDict):
    kind: Literal["string", "value"]
    value: ValueData


class ExecutableDataObject(TypedDict, total=False):
    artifact: str
    path: str


class CommandDataObject(TypedDict):
    args: list[CommandValueWire]
    cwd: NotRequired[str]
    env: dict[str, CommandValueWire]
    executable: ExecutableDataObject
    host: str
    stdin: NotRequired[str]
    user: NotRequired[str]


class ExecutableObject(TypedDict):
    artifact: Directory | File | Symlink | None
    path: str | None


class ExecutableArgObject(TypedDict, total=False):
    artifact: Unresolved[Directory | File | Symlink | None]
    path: Unresolved[str | None]


type FieldInput[T: ValueType] = Unresolved[T | Mutation[T] | None]
type CommandArgument = Unresolved[CommandValue | ValueInput]


class CommandObjectValue(TypedDict):
    args: list[CommandValue]
    cwd: str | None
    env: dict[str, CommandValue]
    executable: ExecutableObject
    host: str
    stdin: Blob | None
    user: str | None


def reduce_args(a: object, b: object) -> list[CommandValue]:
    previous = [] if a is UNSET or a is None else cast("list[CommandValue]", a)
    next_ = [] if b is UNSET or b is None else cast("list[CommandValue]", b)
    return previous + next_


class CommandArgObject(TypedDict, total=False):
    args: Unresolved[
        Mutation
        | list[
            Unresolved[
                CommandValue | str | Template | Directory | File | Symlink | ValueInput
            ]
        ]
        | None
    ]
    cwd: FieldInput[str]
    env: Unresolved[
        dict[str, Unresolved[CommandValue | ValueInput | Mutation]]
        | str
        | Mutation
        | None
    ]
    executable: Unresolved[
        str | Directory | File | Symlink | ExecutableArgObject | Mutation | None
    ]
    host: FieldInput[str]
    stdin: Unresolved[Blob | bytes | str | None]
    user: FieldInput[str]


type CommandInput = Unresolved[
    str
    | Directory
    | File
    | Symlink
    | Template
    | Command
    | CommandArgObject
    | CommandObjectValue
    | None
]


class BoundEnvironment:
    def __init__(
        self,
        getter: Callable[
            [str | None, Client | None],
            Awaitable[dict[str, CommandValue] | CommandValue | None],
        ],
    ) -> None:
        self.getter = getter

    def __await__(self) -> Generator[Any, None, dict[str, CommandValue]]:
        return cast(
            "Awaitable[dict[str, CommandValue]]", self.getter(None, None)
        ).__await__()

    @overload
    def __call__(
        self, name: None = None, client: Client | None = None
    ) -> Awaitable[dict[str, CommandValue]]: ...

    @overload
    def __call__(
        self, name: str, client: Client | None = None
    ) -> Awaitable[CommandValue | None]: ...

    def __call__(
        self, name: str | None = None, client: Client | None = None
    ) -> Awaitable[dict[str, CommandValue] | CommandValue | None]:
        return self.getter(name, client)


class environment_property[S]:
    def __init__(
        self,
        method: Callable[
            [S, str | None, Client | None],
            Awaitable[dict[str, CommandValue] | CommandValue | None],
        ],
    ) -> None:
        self.method = method
        self.__doc__ = method.__doc__

    @overload
    def __get__(self, instance: None, owner: type[S] | None = None) -> Self: ...

    @overload
    def __get__(
        self, instance: S, owner: type[S] | None = None
    ) -> BoundEnvironment: ...

    def __get__(self, instance: S | None, owner: type[S] | None = None):
        if instance is None:
            return self
        return BoundEnvironment(
            lambda name, client: self.method(instance, name, client)
        )


class Command[A, O: ValueType](Object):
    kind = "command"
    Builder: ClassVar[type[CommandBuilder[ValueType]]]
    Value: ClassVar[type[CommandValue]]
    Arg: ClassVar[type[CommandArg]]
    Object: ClassVar[type[CommandObject]]
    Executable: ClassVar[type[CommandExecutable]]
    Data: ClassVar[type[CommandData]]

    def __init__(
        self,
        executable=None,
        *,
        args=None,
        env=None,
        host=None,
        cwd=None,
        user=None,
        stdin=None,
        value=None,
        **options,
    ):
        from .directory import Directory
        from .file import File
        from .symlink import Symlink

        if (
            isinstance(executable, dict)
            and "stored" in executable
            and ("id" in executable or "object" in executable)
        ):
            arg = executable
            super().__init__(
                arg.get("object"), id=arg.get("id"), tokens=arg.get("tokens")
            )
            self._stored = arg["stored"]
            return
        if executable is not None:
            executable_data: Any = (
                {"path": executable}
                if isinstance(executable, str)
                else {"artifact": executable}
                if isinstance(executable, (Directory, File, Symlink))
                else executable
            )
            executable_data = {
                "artifact": executable_data.get("artifact"),
                "path": executable_data.get("path"),
            }
            value = {"executable": executable_data}
            value["args"] = [command_value(arg) for arg in (args or [])]
            value["env"] = {
                name: command_value(value) for name, value in (env or {}).items()
            }
            if host is None:
                from .host import current

                host = current
            for name, child in (
                ("host", host),
                ("cwd", cwd),
                ("stdin", stdin),
                ("user", user),
            ):
                value[name] = child
        super().__init__(value, **options)

    @classmethod
    async def new(
        cls,
        *args: CommandInput,
        client: Client | None = None,
        **options: Unpack[CommandArgObject],
    ) -> Self:
        from .blob import Blob

        args, options = await resolve([args, options])
        if len(args) == 1 and not options and isinstance(args[0], cls):
            return args[0]
        state = await cls.arg_resolved(*args, options, client=client)
        if state.get("stdin") is not None:
            state["stdin"] = await Blob.new(state["stdin"], client=client)
        if state.get("executable") is None:
            raise ValueError("cannot create a command without an executable")
        command = cls(**state)
        if (await command.host(client)) is None:
            raise ValueError("cannot create a command without a host")
        return command

    @classmethod
    async def arg(cls, *args, client: Client | None = None):
        return await cls.arg_resolved(*(await resolve(args)), client=client)

    @classmethod
    async def arg_resolved(cls, *args, client: Client | None = None):
        from .directory import Directory
        from .file import File
        from .symlink import Symlink

        async def map(arg):

            if arg is None or arg is UNSET:
                return {}
            if isinstance(arg, (str, File, Directory, Symlink, Template)):
                arg = {"args": ["-c", arg], "executable": "sh"}
            elif isinstance(arg, cls):
                arg = await arg.load(client)
            arg = dict(arg)
            if arg.get("args") is not None:
                arg["args"] = [command_value(value) for value in arg["args"]]
            return arg

        return await Args.apply_resolved(
            args,
            map=map,
            reduce={
                "args": reduce_args,
                "env": reduce_env,
            },
        )

    @staticmethod
    async def py[R: ValueType](
        function_: Callable[..., Unresolved[R]],
        args: Sequence[CommandArgument],
        *,
        client: Client | None = None,
    ) -> Referent[Command[list[ValueType], R]]:
        from .referent import Referent

        command = await Command.py_arg(function_, args, client=client)
        node = await Command.new(command.node, client=client)
        return Referent(node, command.options)

    @staticmethod
    async def py_arg[R: ValueType](
        function_: Callable[..., Unresolved[R]],
        args: Sequence[CommandArgument],
        *,
        client: Client | None = None,
    ) -> Referent[CommandArgObject]:
        from . import host
        from .module import Module
        from .referent import Referent

        args = await resolve(list(args))
        target = host.magic(function_)
        module = Module.from_data(target["module"])
        if isinstance(module.referent.node, str):
            from .client import client as default_client
            from .client import last_output

            client = client or default_client
            output = await last_output(await client.checkin(module.referent.node))
            if output is None:
                raise ValueError("the checkin stream ended without output")
            artifact = output["artifact"]
            module = Module(
                module.kind, Referent(Object.with_id(artifact.node), artifact.options)
            )
        options = deepcopy(module.referent.options or {})
        module.referent.options = deepcopy(options)
        for name in ("id", "name", "path", "tag"):
            module.referent.options.pop(name, None)
        command_args: list[CommandArgument] = [
            CommandValue.string("py" if module.kind == "py" else "js")
        ]
        export = target.get("export")
        if export is not None:
            command_args.extend(
                [
                    CommandValue.string("--export"),
                    CommandValue.string(export),
                ]
            )
        command_args.append(CommandValue.value(module))
        for arg in args:
            value = arg if isinstance(arg, CommandValue) else CommandValue.value(arg)
            command_args.extend(
                [CommandValue.string("-a" if value.kind == "string" else "-A"), value]
            )
        arg: CommandArgObject = {
            "args": command_args,
            "executable": "tg",
        }
        return Referent(arg, options)

    @tg.property
    async def args(self, client: Client | None = None) -> list[CommandValue]:
        return (await self.load(client)).get("args", [])

    @tg.property
    async def cwd(self, client: Client | None = None) -> str | None:
        return (await self.load(client)).get("cwd")

    @environment_property
    async def env(
        self, name=None, client: Client | None = None
    ) -> dict[str, CommandValue] | CommandValue | None:
        env = (await self.load(client)).get("env", {})
        return env if name is None else env.get(name)

    @tg.property
    async def executable(self, client: Client | None = None) -> ExecutableObject:
        return (await self.load(client))["executable"]

    @tg.property
    async def host(self, client: Client | None = None) -> str:
        return (await self.load(client))["host"]

    @tg.property
    async def stdin(self, client: Client | None = None) -> Blob | None:
        return (await self.load(client)).get("stdin")

    @tg.property
    async def user(self, client: Client | None = None) -> str | None:
        return (await self.load(client)).get("user")

    def build(self, *args, **options) -> ProcessBuilder[Literal["run"], O]:
        from .process.build import builder as build

        return build(self, {"args": list(args)}, **options)

    def run(self, *args, **options) -> ProcessBuilder[Literal["run"], O]:
        from .process.run import builder as run

        return run(self, {"args": list(args)}, **options)

    def spawn(self, *args, **options) -> ProcessBuilder[Literal["spawn"], O]:
        from .process.spawn import builder as spawn

        return spawn(self, {"args": list(args)}, **options)

    def exec(self, *args, **options) -> ProcessBuilder[Literal["exec"], Never]:
        from .process.exec import builder as exec

        return exec(self, {"args": list(args)}, **options)

    def _decode(self, value) -> CommandObjectValue:
        from .blob import Blob
        from .object import edge_from_data

        value = deepcopy(value)
        if "artifact" in value.get("executable", {}):
            value["executable"]["artifact"] = edge_from_data(
                value["executable"]["artifact"]
            )
        if "stdin" in value:
            value["stdin"] = Blob.with_id(value["stdin"])
        value["args"] = [
            CommandValue(arg["kind"], Value.from_data(arg["value"]))
            for arg in value.get("args", [])
        ]
        value["env"] = {
            name: CommandValue(arg["kind"], Value.from_data(arg["value"]))
            for name, arg in value.get("env", {}).items()
        }
        value["cwd"] = value.get("cwd")
        value["user"] = value.get("user")
        value["stdin"] = value.get("stdin")
        value["executable"].setdefault("artifact", None)
        value["executable"].setdefault("path", None)
        return cast(CommandObjectValue, value)

    def _encode(self, value):
        return CommandObject.to_data(value)


class _ValueConstructor(Protocol):
    def __call__[U](self, value: U) -> CommandValue[U]: ...


class _ValueDescriptor:
    @overload
    def __get__(
        self, instance: None, owner: type[CommandValue]
    ) -> _ValueConstructor: ...

    @overload
    def __get__[T](
        self, instance: CommandValue[T], owner: type[CommandValue] | None = None
    ) -> T: ...

    def __get__(self, instance, owner=None):
        if instance is None:
            return lambda value: CommandValue("value", value)
        return instance._value

    def __set__[T](self, instance: CommandValue[T], value: T) -> None:
        instance._value = value


class CommandValue[T]:
    __tangram_atomic__ = True
    Data: ClassVar[type[CommandValueData]]

    def __init__(self, kind: Literal["string", "value"], value: T):
        self.kind = kind
        self._value: T = value

    @staticmethod
    def string[U](value: U) -> CommandValue[U]:
        return CommandValue("string", value)

    value = _ValueDescriptor()

    @staticmethod
    def from_data(data):
        return CommandValue(
            "string" if data["kind"] == "string" else "value",
            Value.from_data(data["value"]),
        )

    @staticmethod
    def children(value):
        return Value.objects(value.value)

    def to_data(self):
        if self.kind not in ("string", "value"):
            raise ValueError("invalid command value kind")
        return {"kind": self.kind, "value": Value.to_data(self.value)}


def command_value(value):
    if isinstance(value, CommandValue):
        return value
    return CommandValue("string", value)


async def reduce_env(a, b):
    if b is None or b is UNSET:
        return b
    output = dict(a) if a is not None and a is not UNSET else {}
    for key, value in b.items():
        if not isinstance(value, Mutation):
            output[key] = command_value(value)
            continue
        current = output.get(key)
        kind = current.kind if current is not None else "string"
        inner = await value.apply(current.value if current is not None else UNSET)
        if inner is UNSET:
            output.pop(key, None)
        else:
            output[key] = CommandValue(kind, inner)
    return output


class CommandValueData:
    @staticmethod
    def children(data):
        return CommandData._value_children(data["value"])

    @staticmethod
    def without_location_and_tokens(data):
        return {**data, "value": CommandData._without_proofs(data["value"])}


class CommandArg:
    @staticmethod
    def is_js(arg):
        return CommandArg._is_module(arg, "js")

    @staticmethod
    def is_py(arg):
        return CommandArg._is_module(arg, "py")

    @staticmethod
    def _is_module(arg, language):
        executable = arg.get("executable")
        args = arg.get("args") or []
        first_arg = args[0] if args else None
        return (
            executable == "tg"
            or isinstance(executable, dict)
            and executable.get("artifact") is None
            and executable.get("path") == "tg"
        ) and (
            first_arg == language
            or isinstance(first_arg, CommandValue)
            and first_arg.kind == "string"
            and first_arg.value == language
        )

    class Value:
        to_value = staticmethod(command_value)

    class Executable:
        @staticmethod
        def is_(value):
            from .artifact import Artifact

            return (
                Artifact.is_(value)
                or isinstance(value, str)
                or isinstance(value, dict)
                and (value.get("artifact") is None or Artifact.is_(value["artifact"]))
                and (value.get("path") is None or isinstance(value["path"], str))
            )

    class Env:
        reduce = staticmethod(reduce_env)


class CommandObject:
    @staticmethod
    def is_js(object):
        executable = object["executable"]
        args = object["args"]
        return (
            executable.get("artifact") is None
            and executable.get("path") == "tg"
            and bool(args)
            and args[0].kind == "string"
            and args[0].value == "js"
        )

    @staticmethod
    def is_py(object):
        executable = object["executable"]
        args = object["args"]
        return (
            executable.get("artifact") is None
            and executable.get("path") == "tg"
            and bool(args)
            and args[0].kind == "string"
            and args[0].value == "py"
        )

    @staticmethod
    def to_data(object):
        output = {
            "args": [value.to_data() for value in object["args"]],
            "env": {key: value.to_data() for key, value in object["env"].items()},
            "executable": CommandExecutable.to_data(object["executable"]),
            "host": object["host"],
        }
        for key in ("cwd", "user"):
            if object.get(key) is not None:
                output[key] = object[key]
        if object.get("stdin") is not None:
            output["stdin"] = object["stdin"].id
        return output

    @staticmethod
    def from_data(data: CommandDataObject) -> CommandObjectValue:
        return Command.with_object({})._decode(data)

    @staticmethod
    def children(object):
        return [
            *[
                child
                for value in object["args"]
                for child in CommandValue.children(value)
            ],
            *[
                child
                for value in object["env"].values()
                for child in CommandValue.children(value)
            ],
            *CommandExecutable.children(object["executable"]),
            *([object["stdin"]] if object.get("stdin") is not None else []),
        ]


class CommandExecutable:
    @staticmethod
    def to_data(value):
        output = {}
        if value.get("artifact") is not None:
            output["artifact"] = value["artifact"].id
        if value.get("path") is not None:
            output["path"] = value["path"]
        return output

    @staticmethod
    def from_data(data):
        from .artifact import Artifact

        return {
            "artifact": Artifact.with_id(data["artifact"])
            if data.get("artifact") is not None
            else None,
            "path": data.get("path"),
        }

    @staticmethod
    def children(value):
        return [value["artifact"]] if value.get("artifact") is not None else []


class CommandData:
    class Executable:
        @staticmethod
        def children(data):
            return [data["artifact"]] if data.get("artifact") is not None else []

        @staticmethod
        def without_location_and_tokens(data):
            return dict(data)

    @staticmethod
    def children(data):
        return [
            *CommandData.Executable.children(data["executable"]),
            *[
                child
                for value in data.get("args", [])
                for child in CommandValueData.children(value)
            ],
            *[
                child
                for value in data.get("env", {}).values()
                for child in CommandValueData.children(value)
            ],
            *([data["stdin"]] if data.get("stdin") is not None else []),
        ]

    @staticmethod
    def without_location_and_tokens(data):
        output = dict(data)
        if "args" in data:
            output["args"] = [
                CommandValueData.without_location_and_tokens(value)
                for value in data["args"]
            ]
        if "env" in data:
            output["env"] = {
                key: CommandValueData.without_location_and_tokens(value)
                for key, value in data["env"].items()
            }
        output["executable"] = CommandData.Executable.without_location_and_tokens(
            data["executable"]
        )
        return output

    @staticmethod
    def _value_children(data):
        from .object import objects

        return [child.id for child in objects(Value.from_data(data))]

    @staticmethod
    def _without_proofs(data):
        from .referent import Referent

        def referent(value):
            if isinstance(value, str):
                return (
                    Referent.from_data_string(value)
                    .without_location_and_tokens()
                    .to_data_string()
                )
            output = dict(value)
            if "options" in output:
                output["options"] = {
                    key: child
                    for key, child in output["options"].items()
                    if key not in ("location", "tokens")
                }
            return output

        if isinstance(data, list):
            return [CommandData._without_proofs(child) for child in data]
        if not isinstance(data, dict):
            return data
        kind = data.get("kind")
        if kind == "object":
            return {**data, "value": referent(data["value"])}
        if kind == "module":
            return {
                **data,
                "value": {
                    **data["value"],
                    "referent": referent(data["value"]["referent"]),
                },
            }
        if kind == "template":
            return {
                **data,
                "value": {
                    **data["value"],
                    "components": [
                        {**component, "value": referent(component["value"])}
                        if component["kind"] == "artifact"
                        else component
                        for component in data["value"]["components"]
                    ],
                },
            }
        if kind == "mutation":
            mutation = dict(data["value"])
            if mutation["kind"] in ("set", "set_if_unset"):
                mutation["value"] = CommandData._without_proofs(mutation["value"])
            elif mutation["kind"] in ("prepend", "append"):
                mutation["values"] = [
                    CommandData._without_proofs(value) for value in mutation["values"]
                ]
            elif mutation["kind"] in ("prefix", "suffix"):
                mutation["template"] = CommandData._without_proofs(
                    {"kind": "template", "value": mutation["template"]}
                )["value"]
            elif mutation["kind"] == "merge":
                mutation["value"] = {
                    key: CommandData._without_proofs(value)
                    for key, value in mutation["value"].items()
                }
            return {**data, "value": mutation}
        if kind == "map":
            return {
                **data,
                "value": {
                    key: CommandData._without_proofs(value)
                    for key, value in data["value"].items()
                },
            }
        return dict(data)


class CommandBuilder[O: ValueType]:
    """A callable, awaitable command builder matching Command.Builder."""

    def __init__(
        self,
        *args: CommandInput,
        client: Client | None = None,
        **options: Unpack[CommandArgObject],
    ):
        self.client = client
        self._memo = {}
        originals = capture([*args, *([options] if options else [])], self._memo)
        self._originals = originals
        self._module = None
        self.arguments = [
            capture(self.builder_arg(arg), self._memo) for arg in originals
        ]
        self._env_mapper = lambda env: env

    def __call__(self, *args) -> Self:
        return self.args(list(args))

    def arg(self, *args) -> Self:
        return self.args(list(args))

    def args(self, *args) -> Self:
        for arg in args:
            self.arguments.append(
                capture(self.args_arg(capture(arg, self._memo)), self._memo)
            )
        return self

    def cwd(self, cwd: Unresolved[str | Mutation | None]) -> Self:
        self.arguments.append(capture({"cwd": cwd}, self._memo))
        return self

    def env(self, *envs, **env) -> Self:
        for value in (*envs, *([env] if env else [])):
            self.arguments.append(self.env_arg(value))
        return self

    def env_mapper(self, mapper) -> Self:
        self._env_mapper = mapper
        return self

    def executable(self, executable) -> Self:
        self.arguments.append(capture({"executable": executable}, self._memo))
        return self

    def host(self, host: Unresolved[str | Mutation | None]) -> Self:
        self.arguments.append(capture({"host": host}, self._memo))
        return self

    def build(self, *args) -> ProcessBuilder[Literal["run"], O]:
        return self._process("build", args)

    def run(self, *args) -> ProcessBuilder[Literal["run"], O]:
        return self._process("run", args)

    def spawn(self, *args) -> ProcessBuilder[Literal["spawn"], O]:
        return self._process("spawn", args)

    def exec(self, *args) -> ProcessBuilder[Literal["exec"], Never]:
        return self._process("exec", args)

    def _process(self, operation, args):
        from .process.build import builder as build
        from .process.exec import builder as exec
        from .process.run import builder as run
        from .process.spawn import builder as spawn

        arg = capture(self.args_arg(capture(list(args), self._memo)), self._memo)
        factory = {"build": build, "exec": exec, "run": run, "spawn": spawn}[operation]
        return factory(*self.arguments, arg, client=self.client).env_mapper(
            self._env_mapper
        )

    def __await__(self) -> Generator[Any, None, Command[list[ValueType], O]]:
        return Command.new(*self.arguments, client=self.client).__await__()

    async def _is_module(self):
        if self._module is None:
            self._module = capture(
                is_module_command_builder_arg(self._originals), self._memo
            )
        return await resolve(self._module)

    async def builder_arg(self, arg):
        arg = await resolve(arg)
        if (
            isinstance(arg, Command)
            or not isinstance(arg, dict)
            or not isinstance(arg.get("args"), list)
            or not await self._is_module()
        ):
            return arg
        return {**arg, "args": encode_module_args(arg["args"])}

    async def args_arg(self, args):
        args = await resolve(args)
        if args is not None and await self._is_module():
            args = encode_module_args(args)
        return {"args": args}

    def env_arg(self, env):
        mapper = self._env_mapper
        env = capture(env, self._memo)

        async def mapped():
            value = await resolve(env)
            return {"env": None if value is None else await resolve(mapper(value))}

        return capture(mapped(), self._memo)


async def is_module_command_builder_arg(args):
    args = await resolve(args)
    for arg in args:
        if isinstance(arg, Command):
            object = await arg.object()
            if CommandObject.is_js(object) or CommandObject.is_py(object):
                return True
    return False


class EncodedArgs(list[CommandValue]):
    # Preserve the encoding through resolution and builder handoffs.
    __tangram_atomic__ = None


def encode_module_args(args):
    if isinstance(args, EncodedArgs):
        return args
    output = EncodedArgs()
    for value in args:
        value = value if isinstance(value, CommandValue) else CommandValue.value(value)
        output.extend(
            [CommandValue.string("-a" if value.kind == "string" else "-A"), value]
        )
    return output


class TemplateStrings(Protocol):
    raw: Sequence[str]

    def __getitem__(self, index: int) -> str: ...

    def __len__(self) -> int: ...


# Python cannot map a callable's parameter tuple to unresolved argument types.
@overload
def command[R: ValueType](
    function_: Callable[..., Unresolved[R]],
    *args: CommandArgument,
    client: Client | None = None,
    **options: Unpack[CommandArgObject],
) -> CommandBuilder[R]: ...


@overload
def command(
    strings: TemplateStrings,
    *placeholders: TemplateInput,
    client: Client | None = None,
    **options: Unpack[CommandArgObject],
) -> CommandBuilder[ValueType]: ...


@overload
def command(
    *args: CommandInput,
    client: Client | None = None,
    **options: Unpack[CommandArgObject],
) -> CommandBuilder[ValueType]: ...


def command(
    *args, client: Client | None = None, **options: Unpack[CommandArgObject]
) -> CommandBuilder[ValueType]:
    if args and callable(args[0]) and not hasattr(args[0], "__await__"):
        function_ = cast("Callable[..., Unresolved[ValueType]]", args[0])

        async def create():
            referent = await Command.py(function_, function_args, client=client)
            return referent.node

        builder = CommandBuilder(create(), client=client, **options)
        function_args = capture(args[1:], builder._memo)
        return builder
    if args and isinstance(args[0], list) and hasattr(args[0], "raw"):
        from .assert_ import assert_
        from .process import env
        from .template import raw

        strings, *placeholders = args
        template = raw(strings, *placeholders)
        executable = env.get("SHELL")
        if executable is None:
            executable = "sh"
        assert_(Command.Arg.Executable.is_(executable))
        shell_arg: CommandArgObject = {
            "executable": executable,
            "args": ["-c", template],
        }
        args = (shell_arg,)
    return CommandBuilder(*args, client=client, **options)


Command.Builder = CommandBuilder
Command.Value = CommandValue
Command.Arg = CommandArg
Command.Object = CommandObject
Command.Executable = CommandExecutable
Command.Data = CommandData
CommandValue.Data = CommandValueData
